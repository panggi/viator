//! Command protection and dangerous command handling.
//!
//! Provides:
//! - Dangerous command blocking/renaming
//! - Command timeouts
//! - Slowlog integration
//! - Memory limits per command
//!
//! # Redis Compatibility
//!
//! Supports Redis security features:
//! - rename-command CONFIG ""
//! - rename-command FLUSHALL ""

use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Commands considered dangerous by default.
pub const DANGEROUS_COMMANDS: &[&str] = &[
    "FLUSHALL",
    "FLUSHDB",
    "DEBUG",
    "SHUTDOWN",
    "BGREWRITEAOF",
    "BGSAVE",
    "CONFIG",
    "SAVE",
    "SLAVEOF",
    "REPLICAOF",
    "CLUSTER",
    "MIGRATE",
    "RESTORE",
    "SORT",      // Can be slow with STORE
    "KEYS",      // Can be slow on large databases
    "EVAL",      // Arbitrary Lua execution
    "EVALSHA",
    "SCRIPT",
    "MODULE",
    "ACL",       // Security configuration
];

/// Commands that modify data (for read-only mode).
pub const WRITE_COMMANDS: &[&str] = &[
    "SET", "SETNX", "SETEX", "PSETEX", "MSET", "MSETNX", "SETRANGE", "APPEND",
    "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
    "DEL", "UNLINK", "RENAME", "RENAMENX", "COPY", "MOVE",
    "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT", "PERSIST",
    "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LINSERT", "LREM", "LTRIM",
    "SADD", "SREM", "SPOP", "SMOVE", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
    "ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZRANGESTORE", "ZUNIONSTORE", "ZINTERSTORE",
    "HSET", "HSETNX", "HMSET", "HDEL", "HINCRBY", "HINCRBYFLOAT",
    "XADD", "XDEL", "XTRIM", "XSETID", "XACK", "XCLAIM",
    "PFADD", "PFMERGE",
    "GEOADD", "GEORADIUS", "GEORADIUSBYMEMBER",
    "SETBIT", "BITOP", "BITFIELD",
    "PUBLISH",
    "FLUSHDB", "FLUSHALL",
    "RESTORE", "MIGRATE",
    "EVAL", "EVALSHA",
];

/// Configuration for command protection.
#[derive(Debug, Clone)]
pub struct CommandProtectionConfig {
    /// Enable dangerous command blocking
    pub block_dangerous: bool,
    /// Set of explicitly blocked commands
    pub blocked_commands: HashSet<String>,
    /// Command renames (original -> new name)
    pub renamed_commands: DashMap<String, String>,
    /// Enable read-only mode (block all writes)
    pub read_only: bool,
    /// Default command timeout in milliseconds
    pub default_timeout_ms: u64,
    /// Per-command timeout overrides
    pub command_timeouts: DashMap<String, u64>,
    /// Maximum arguments per command
    pub max_arguments: usize,
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
}

impl Default for CommandProtectionConfig {
    fn default() -> Self {
        Self {
            block_dangerous: false, // Off by default for compatibility
            blocked_commands: HashSet::new(),
            renamed_commands: DashMap::new(),
            read_only: false,
            default_timeout_ms: 0, // 0 = no timeout
            command_timeouts: DashMap::new(),
            max_arguments: 1_000_000,
            max_key_size: 512 * 1024 * 1024, // 512 MB
            max_value_size: 512 * 1024 * 1024, // 512 MB
        }
    }
}

/// Command protection manager.
#[derive(Debug)]
pub struct CommandProtection {
    /// Configuration
    config: RwLock<CommandProtectionConfig>,
    /// Statistics
    stats: CommandProtectionStats,
}

/// Statistics for command protection.
#[derive(Debug, Default)]
pub struct CommandProtectionStats {
    /// Commands blocked
    pub blocked_count: AtomicU64,
    /// Commands timed out
    pub timeout_count: AtomicU64,
    /// Commands rejected (too large, etc.)
    pub rejected_count: AtomicU64,
}

impl CommandProtection {
    /// Create a new command protection manager.
    #[must_use]
    pub fn new(config: CommandProtectionConfig) -> Self {
        Self {
            config: RwLock::new(config),
            stats: CommandProtectionStats::default(),
        }
    }

    /// Check if a command is allowed to execute.
    pub fn check_command(&self, command: &str, args: &[&[u8]]) -> Result<(), CommandBlockReason> {
        let config = self.config.read();

        // Check if command is blocked
        let cmd_upper = command.to_uppercase();

        if config.blocked_commands.contains(&cmd_upper) {
            self.stats.blocked_count.fetch_add(1, Ordering::Relaxed);
            return Err(CommandBlockReason::ExplicitlyBlocked);
        }

        // Check dangerous commands
        if config.block_dangerous && DANGEROUS_COMMANDS.contains(&cmd_upper.as_str()) {
            self.stats.blocked_count.fetch_add(1, Ordering::Relaxed);
            return Err(CommandBlockReason::Dangerous);
        }

        // Check read-only mode
        if config.read_only && WRITE_COMMANDS.contains(&cmd_upper.as_str()) {
            self.stats.blocked_count.fetch_add(1, Ordering::Relaxed);
            return Err(CommandBlockReason::ReadOnlyMode);
        }

        // Check argument count
        if args.len() > config.max_arguments {
            self.stats.rejected_count.fetch_add(1, Ordering::Relaxed);
            return Err(CommandBlockReason::TooManyArguments {
                count: args.len(),
                max: config.max_arguments,
            });
        }

        // Check argument sizes
        for (i, arg) in args.iter().enumerate() {
            let max_size = if i == 0 {
                config.max_key_size
            } else {
                config.max_value_size
            };

            if arg.len() > max_size {
                self.stats.rejected_count.fetch_add(1, Ordering::Relaxed);
                return Err(CommandBlockReason::ArgumentTooLarge {
                    index: i,
                    size: arg.len(),
                    max: max_size,
                });
            }
        }

        Ok(())
    }

    /// Resolve a command name (handle renames).
    #[must_use]
    pub fn resolve_command_name(&self, command: &str) -> Option<String> {
        let config = self.config.read();
        let cmd_upper = command.to_uppercase();

        // Check if command is renamed
        if let Some(renamed) = config.renamed_commands.get(&cmd_upper) {
            let new_name = renamed.clone();
            if new_name.is_empty() {
                // Renamed to empty = disabled
                return None;
            }
            return Some(new_name);
        }

        Some(cmd_upper)
    }

    /// Get timeout for a command.
    #[must_use]
    pub fn get_timeout(&self, command: &str) -> Option<Duration> {
        let config = self.config.read();
        let cmd_upper = command.to_uppercase();

        // Check for command-specific timeout
        if let Some(ms) = config.command_timeouts.get(&cmd_upper) {
            if *ms > 0 {
                return Some(Duration::from_millis(*ms));
            }
        }

        // Fall back to default
        if config.default_timeout_ms > 0 {
            Some(Duration::from_millis(config.default_timeout_ms))
        } else {
            None
        }
    }

    /// Block a command.
    pub fn block_command(&self, command: &str) {
        self.config.write().blocked_commands.insert(command.to_uppercase());
    }

    /// Unblock a command.
    pub fn unblock_command(&self, command: &str) {
        self.config.write().blocked_commands.remove(&command.to_uppercase());
    }

    /// Rename a command.
    pub fn rename_command(&self, from: &str, to: &str) {
        self.config
            .write()
            .renamed_commands
            .insert(from.to_uppercase(), to.to_uppercase());
    }

    /// Set read-only mode.
    pub fn set_read_only(&self, read_only: bool) {
        self.config.write().read_only = read_only;
    }

    /// Set default timeout.
    pub fn set_default_timeout(&self, timeout_ms: u64) {
        self.config.write().default_timeout_ms = timeout_ms;
    }

    /// Get statistics.
    #[must_use]
    pub fn stats(&self) -> &CommandProtectionStats {
        &self.stats
    }

    /// Update configuration.
    pub fn update_config(&self, config: CommandProtectionConfig) {
        *self.config.write() = config;
    }
}

impl Default for CommandProtection {
    fn default() -> Self {
        Self::new(CommandProtectionConfig::default())
    }
}

/// Reason a command was blocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandBlockReason {
    /// Command is explicitly blocked
    ExplicitlyBlocked,
    /// Command is dangerous
    Dangerous,
    /// Server is in read-only mode
    ReadOnlyMode,
    /// Too many arguments
    TooManyArguments { count: usize, max: usize },
    /// Argument too large
    ArgumentTooLarge { index: usize, size: usize, max: usize },
    /// Command timed out
    TimedOut { elapsed: Duration, limit: Duration },
}

impl std::fmt::Display for CommandBlockReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExplicitlyBlocked => write!(f, "command is blocked"),
            Self::Dangerous => write!(f, "command is considered dangerous"),
            Self::ReadOnlyMode => write!(f, "server is in read-only mode"),
            Self::TooManyArguments { count, max } => {
                write!(f, "too many arguments: {count} (max {max})")
            }
            Self::ArgumentTooLarge { index, size, max } => {
                write!(f, "argument {index} too large: {size} bytes (max {max})")
            }
            Self::TimedOut { elapsed, limit } => {
                write!(f, "command timed out after {elapsed:?} (limit {limit:?})")
            }
        }
    }
}

/// A guard that tracks command execution time.
#[derive(Debug)]
pub struct CommandTimer {
    /// Start time
    start: Instant,
    /// Timeout duration
    timeout: Option<Duration>,
}

impl CommandTimer {
    /// Create a new command timer.
    #[must_use]
    pub fn new(timeout: Option<Duration>) -> Self {
        Self {
            start: Instant::now(),
            timeout,
        }
    }

    /// Check if the command has timed out.
    #[must_use]
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.start.elapsed() > timeout
        } else {
            false
        }
    }

    /// Get elapsed time.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Check timeout and return error if exceeded.
    pub fn check_timeout(&self) -> Result<(), CommandBlockReason> {
        if let Some(timeout) = self.timeout {
            let elapsed = self.start.elapsed();
            if elapsed > timeout {
                return Err(CommandBlockReason::TimedOut {
                    elapsed,
                    limit: timeout,
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_allows_commands() {
        let protection = CommandProtection::default();

        assert!(protection.check_command("GET", &[b"key"]).is_ok());
        assert!(protection.check_command("SET", &[b"key", b"value"]).is_ok());
    }

    #[test]
    fn test_block_dangerous() {
        let config = CommandProtectionConfig {
            block_dangerous: true,
            ..Default::default()
        };
        let protection = CommandProtection::new(config);

        assert!(protection.check_command("GET", &[b"key"]).is_ok());
        assert_eq!(
            protection.check_command("FLUSHALL", &[]),
            Err(CommandBlockReason::Dangerous)
        );
    }

    #[test]
    fn test_explicit_block() {
        let protection = CommandProtection::default();
        protection.block_command("DEBUG");

        assert_eq!(
            protection.check_command("DEBUG", &[b"segfault"]),
            Err(CommandBlockReason::ExplicitlyBlocked)
        );
    }

    #[test]
    fn test_read_only_mode() {
        let config = CommandProtectionConfig {
            read_only: true,
            ..Default::default()
        };
        let protection = CommandProtection::new(config);

        assert!(protection.check_command("GET", &[b"key"]).is_ok());
        assert_eq!(
            protection.check_command("SET", &[b"key", b"value"]),
            Err(CommandBlockReason::ReadOnlyMode)
        );
    }

    #[test]
    fn test_command_rename() {
        let protection = CommandProtection::default();
        protection.rename_command("CONFIG", "MYCONFIG");

        assert_eq!(
            protection.resolve_command_name("CONFIG"),
            Some("MYCONFIG".to_string())
        );

        // Rename to empty = disable
        protection.rename_command("DEBUG", "");
        assert_eq!(protection.resolve_command_name("DEBUG"), None);
    }

    #[test]
    fn test_argument_limits() {
        let config = CommandProtectionConfig {
            max_arguments: 3,
            max_value_size: 10,
            ..Default::default()
        };
        let protection = CommandProtection::new(config);

        // Too many arguments
        assert!(matches!(
            protection.check_command("MSET", &[b"a", b"1", b"b", b"2"]),
            Err(CommandBlockReason::TooManyArguments { .. })
        ));

        // Argument too large
        assert!(matches!(
            protection.check_command("SET", &[b"key", b"very long value here"]),
            Err(CommandBlockReason::ArgumentTooLarge { .. })
        ));
    }

    #[test]
    fn test_command_timer() {
        let timer = CommandTimer::new(Some(Duration::from_millis(100)));
        assert!(!timer.is_timed_out());
        assert!(timer.check_timeout().is_ok());

        // Simulate time passing (we can't really test timeout without sleeping)
        let timer_no_timeout = CommandTimer::new(None);
        assert!(!timer_no_timeout.is_timed_out());
    }
}
