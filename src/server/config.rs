//! Server configuration.

use crate::DEFAULT_PORT;
use std::fs;
use std::path::{Path, PathBuf};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    // === Server ===
    /// Daemonize the server
    pub daemonize: bool,
    /// PID file path
    pub pidfile: Option<PathBuf>,
    /// Log file path (empty for stdout)
    pub logfile: Option<PathBuf>,
    /// Bind address
    pub bind: String,
    /// Port number
    pub port: u16,
    /// Maximum number of clients
    pub max_clients: usize,
    /// TCP keepalive (seconds, 0 to disable)
    pub tcp_keepalive: u32,
    /// Timeout for idle clients (seconds, 0 to disable)
    pub timeout: u32,
    /// Number of databases
    pub databases: u16,
    /// Require password
    pub requirepass: Option<String>,
    /// Working directory
    pub dir: PathBuf,
    /// Log level
    pub loglevel: LogLevel,
    /// Max memory (bytes, 0 for no limit)
    pub maxmemory: usize,
    /// Max memory policy
    pub maxmemory_policy: MaxMemoryPolicy,
    /// Enable AOF persistence
    pub appendonly: bool,
    /// AOF filename
    pub appendfilename: String,
    /// AOF fsync policy
    pub appendfsync: AppendFsync,
    /// VDB filename
    pub dbfilename: String,
    /// Enable VDB save
    pub save: Vec<SaveConfig>,
    /// Keyspace notification events (Redis 8.2+ adds OVERWRITTEN and TYPE_CHANGED)
    pub notify_keyspace_events: KeyspaceEventFlags,
    /// Redis 8.4+ lookahead prefetching depth for command processing
    pub lookahead: u16,
    /// Redis 8.4+ maximum corrupted tail size for automatic AOF repair
    pub aof_load_corrupt_tail_max_size: usize,
    /// Redis 8.4+ Lua JSON array handling
    pub decode_array_with_array_mt: bool,
    /// Redis 8.4+ default scorer for FT.SEARCH text/tag fields
    pub search_default_scorer: SearchScorer,
    /// Redis 8.4+ query behavior on out-of-memory
    pub search_on_oom: SearchOnOom,
    /// Redis 8.4+ communication threads for cluster coordination
    pub search_io_threads: u16,
}

/// Keyspace notification event flags (Redis 8.2+ adds O and T)
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyspaceEventFlags {
    /// K - Keyspace events (__keyspace@<db>__)
    pub keyspace: bool,
    /// E - Keyevent events (__keyevent@<db>__)
    pub keyevent: bool,
    /// g - Generic commands (DEL, EXPIRE, RENAME, etc.)
    pub generic: bool,
    /// $ - String commands
    pub string: bool,
    /// l - List commands
    pub list: bool,
    /// s - Set commands
    pub set: bool,
    /// h - Hash commands
    pub hash: bool,
    /// z - Sorted set commands
    pub zset: bool,
    /// x - Expired events
    pub expired: bool,
    /// e - Evicted events
    pub evicted: bool,
    /// t - Stream commands
    pub stream: bool,
    /// m - Key-miss events
    pub key_miss: bool,
    /// d - Module key type events
    pub module: bool,
    /// n - New key events
    pub new_key: bool,
    /// O - OVERWRITTEN events (Redis 8.2+) - key value completely replaced
    pub overwritten: bool,
    /// T - TYPE_CHANGED events (Redis 8.2+) - key data type changed
    pub type_changed: bool,
    /// A - Alias for "g$lshzxetmdn" (all events)
    pub all: bool,
}

impl KeyspaceEventFlags {
    /// Parse from Redis config string (e.g., "KEA", "Kg$lsh")
    pub fn from_config(s: &str) -> Self {
        let mut flags = Self::default();
        for c in s.chars() {
            match c {
                'K' => flags.keyspace = true,
                'E' => flags.keyevent = true,
                'g' => flags.generic = true,
                '$' => flags.string = true,
                'l' => flags.list = true,
                's' => flags.set = true,
                'h' => flags.hash = true,
                'z' => flags.zset = true,
                'x' => flags.expired = true,
                'e' => flags.evicted = true,
                't' => flags.stream = true,
                'm' => flags.key_miss = true,
                'd' => flags.module = true,
                'n' => flags.new_key = true,
                'O' => flags.overwritten = true, // Redis 8.2+
                'T' => flags.type_changed = true, // Redis 8.2+
                'A' => flags.all = true,
                _ => {}
            }
        }
        flags
    }

    /// Convert to config string
    pub fn to_config(&self) -> String {
        let mut s = String::new();
        if self.keyspace { s.push('K'); }
        if self.keyevent { s.push('E'); }
        if self.generic { s.push('g'); }
        if self.string { s.push('$'); }
        if self.list { s.push('l'); }
        if self.set { s.push('s'); }
        if self.hash { s.push('h'); }
        if self.zset { s.push('z'); }
        if self.expired { s.push('x'); }
        if self.evicted { s.push('e'); }
        if self.stream { s.push('t'); }
        if self.key_miss { s.push('m'); }
        if self.module { s.push('d'); }
        if self.new_key { s.push('n'); }
        if self.overwritten { s.push('O'); }
        if self.type_changed { s.push('T'); }
        if self.all { s.push('A'); }
        s
    }

    /// Check if any notification is enabled
    pub fn is_enabled(&self) -> bool {
        self.keyspace || self.keyevent
    }
}

/// Redis 8.4+ search default scorer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchScorer {
    /// BM25 standard scoring (default in Redis 8.4+)
    #[default]
    Bm25Std,
    /// TF-IDF scoring
    TfIdf,
    /// TFIDF normalized
    TfIdfNorm,
}

/// Redis 8.4+ search behavior on out-of-memory
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchOnOom {
    /// Fail query on OOM
    #[default]
    Fail,
    /// Continue with best-effort results
    Continue,
}

/// Log level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    Debug,
    Verbose,
    #[default]
    Notice,
    Warning,
}

/// Max memory eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxMemoryPolicy {
    #[default]
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    AllKeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileRandom,
    VolatileTtl,
}

/// AOF fsync policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendFsync {
    Always,
    #[default]
    Everysec,
    No,
}

/// VDB save configuration.
#[derive(Debug, Clone, Copy)]
pub struct SaveConfig {
    /// Seconds since last save
    pub seconds: u64,
    /// Minimum changes
    pub changes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            daemonize: false,
            pidfile: None,
            logfile: None,
            bind: "127.0.0.1".to_string(),
            port: DEFAULT_PORT,
            max_clients: 10000,
            tcp_keepalive: 300,
            timeout: 0,
            databases: 16,
            requirepass: None,
            dir: PathBuf::from("."),
            loglevel: LogLevel::default(),
            maxmemory: 0,
            maxmemory_policy: MaxMemoryPolicy::default(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: AppendFsync::default(),
            dbfilename: "dump.vdb".to_string(),
            save: vec![
                SaveConfig {
                    seconds: 3600,
                    changes: 1,
                },
                SaveConfig {
                    seconds: 300,
                    changes: 100,
                },
                SaveConfig {
                    seconds: 60,
                    changes: 10000,
                },
            ],
            notify_keyspace_events: KeyspaceEventFlags::default(),
            lookahead: 16,
            aof_load_corrupt_tail_max_size: 0,
            decode_array_with_array_mt: false,
            search_default_scorer: SearchScorer::default(),
            search_on_oom: SearchOnOom::default(),
            search_io_threads: 1,
        }
    }
}

impl Config {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the bind address.
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.requirepass = Some(password.into());
        self
    }

    /// Set max clients.
    pub fn max_clients(mut self, max: usize) -> Self {
        self.max_clients = max;
        self
    }

    /// Enable AOF persistence.
    pub fn appendonly(mut self, enabled: bool) -> Self {
        self.appendonly = enabled;
        self
    }

    /// Set max memory.
    pub fn maxmemory(mut self, bytes: usize) -> Self {
        self.maxmemory = bytes;
        self
    }

    /// Load configuration from a file (redis.conf compatible format).
    ///
    /// # Format
    /// ```text
    /// # Comment
    /// directive value
    /// directive "value with spaces"
    /// ```
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| ConfigError::IoError(e.to_string()))?;
        Self::parse(&content)
    }

    /// Parse configuration from a string.
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let mut config = Config::default();
        // Clear default save rules - they will be replaced by config file
        config.save.clear();
        let mut includes: Vec<PathBuf> = Vec::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse directive and value
            let (directive, value) = Self::parse_line(line)
                .ok_or_else(|| ConfigError::ParseError {
                    line: line_num + 1,
                    message: "Invalid directive format".to_string(),
                })?;

            // Handle include directive specially
            if directive.eq_ignore_ascii_case("include") {
                includes.push(PathBuf::from(value));
                continue;
            }

            config.apply_directive(&directive.to_lowercase(), value, line_num + 1)?;
        }

        // Process includes after main config
        for include_path in includes {
            if include_path.exists() {
                let included = Self::load_from_file(&include_path)?;
                config.merge(included);
            }
        }

        Ok(config)
    }

    /// Parse a single config line into directive and value.
    fn parse_line(line: &str) -> Option<(&str, &str)> {
        let mut parts = line.splitn(2, |c: char| c.is_whitespace());
        let directive = parts.next()?.trim();
        let value = parts.next().map(|v| v.trim()).unwrap_or("");

        // Handle quoted values
        let value = if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
            &value[1..value.len() - 1]
        } else {
            value
        };

        Some((directive, value))
    }

    /// Apply a single directive to the config.
    fn apply_directive(&mut self, directive: &str, value: &str, line: usize) -> Result<(), ConfigError> {
        match directive {
            // Server
            "daemonize" => self.daemonize = parse_bool(value, line)?,
            "pidfile" => self.pidfile = Some(PathBuf::from(value)),
            "logfile" => {
                if value.is_empty() {
                    self.logfile = None;
                } else {
                    self.logfile = Some(PathBuf::from(value));
                }
            }
            "bind" => self.bind = value.to_string(),
            "port" => self.port = parse_number(value, line)?,
            "timeout" => self.timeout = parse_number(value, line)?,
            "tcp-keepalive" => self.tcp_keepalive = parse_number(value, line)?,
            "loglevel" => {
                self.loglevel = match value.to_lowercase().as_str() {
                    "debug" => LogLevel::Debug,
                    "verbose" => LogLevel::Verbose,
                    "notice" => LogLevel::Notice,
                    "warning" => LogLevel::Warning,
                    _ => return Err(ConfigError::ParseError {
                        line,
                        message: format!("Invalid loglevel: {value}"),
                    }),
                };
            }
            "databases" => self.databases = parse_number(value, line)?,
            "dir" => self.dir = PathBuf::from(value),

            // Security
            "requirepass" => self.requirepass = Some(value.to_string()),

            // Limits
            "maxclients" => self.max_clients = parse_number(value, line)?,
            "maxmemory" => self.maxmemory = parse_memory(value, line)?,
            "maxmemory-policy" => {
                self.maxmemory_policy = match value.to_lowercase().replace('-', "_").as_str() {
                    "noeviction" | "no_eviction" => MaxMemoryPolicy::NoEviction,
                    "allkeys_lru" | "allkeys-lru" => MaxMemoryPolicy::AllKeysLru,
                    "allkeys_lfu" | "allkeys-lfu" => MaxMemoryPolicy::AllKeysLfu,
                    "allkeys_random" | "allkeys-random" => MaxMemoryPolicy::AllKeysRandom,
                    "volatile_lru" | "volatile-lru" => MaxMemoryPolicy::VolatileLru,
                    "volatile_lfu" | "volatile-lfu" => MaxMemoryPolicy::VolatileLfu,
                    "volatile_random" | "volatile-random" => MaxMemoryPolicy::VolatileRandom,
                    "volatile_ttl" | "volatile-ttl" => MaxMemoryPolicy::VolatileTtl,
                    _ => return Err(ConfigError::ParseError {
                        line,
                        message: format!("Invalid maxmemory-policy: {value}"),
                    }),
                };
            }

            // Persistence - AOF
            "appendonly" => self.appendonly = parse_bool(value, line)?,
            "appendfilename" => self.appendfilename = value.to_string(),
            "appendfsync" => {
                self.appendfsync = match value.to_lowercase().as_str() {
                    "always" => AppendFsync::Always,
                    "everysec" => AppendFsync::Everysec,
                    "no" => AppendFsync::No,
                    _ => return Err(ConfigError::ParseError {
                        line,
                        message: format!("Invalid appendfsync: {value}"),
                    }),
                };
            }

            // Persistence - VDB
            "dbfilename" => self.dbfilename = value.to_string(),
            "save" => {
                if value.is_empty() || value == "\"\"" {
                    self.save.clear();
                } else {
                    let parts: Vec<&str> = value.split_whitespace().collect();
                    if parts.len() == 2 {
                        let seconds: u64 = parse_number(parts[0], line)?;
                        let changes: u64 = parse_number(parts[1], line)?;
                        self.save.push(SaveConfig { seconds, changes });
                    }
                }
            }

            // Keyspace notifications
            "notify-keyspace-events" => {
                self.notify_keyspace_events = KeyspaceEventFlags::from_config(value);
            }

            // Redis 8.4+ options
            "lookahead" => self.lookahead = parse_number(value, line)?,
            "aof-load-corrupt-tail-max-size" => {
                self.aof_load_corrupt_tail_max_size = parse_memory(value, line)?;
            }

            // Unknown directive - ignore for forward compatibility
            _ => {
                tracing::warn!("Unknown config directive at line {}: {}", line, directive);
            }
        }

        Ok(())
    }

    /// Merge another config into this one (for includes).
    fn merge(&mut self, other: Config) {
        // Only override non-default values
        if other.daemonize {
            self.daemonize = true;
        }
        if other.pidfile.is_some() {
            self.pidfile = other.pidfile;
        }
        if other.logfile.is_some() {
            self.logfile = other.logfile;
        }
        if other.bind != "127.0.0.1" {
            self.bind = other.bind;
        }
        if other.port != DEFAULT_PORT {
            self.port = other.port;
        }
        if other.requirepass.is_some() {
            self.requirepass = other.requirepass;
        }
        if other.maxmemory > 0 {
            self.maxmemory = other.maxmemory;
        }
        if other.appendonly {
            self.appendonly = true;
        }
    }

    /// Write current configuration to a file.
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let content = self.to_config_string();
        fs::write(path, content).map_err(|e| ConfigError::IoError(e.to_string()))
    }

    /// Convert configuration to config file format.
    pub fn to_config_string(&self) -> String {
        let mut lines = Vec::new();

        lines.push("# Viator configuration file".to_string());
        lines.push("# Generated by Viator".to_string());
        lines.push(String::new());

        lines.push("# Server".to_string());
        lines.push(format!("daemonize {}", if self.daemonize { "yes" } else { "no" }));
        if let Some(ref pidfile) = self.pidfile {
            lines.push(format!("pidfile \"{}\"", pidfile.display()));
        }
        if let Some(ref logfile) = self.logfile {
            lines.push(format!("logfile \"{}\"", logfile.display()));
        }
        lines.push(format!("bind {}", self.bind));
        lines.push(format!("port {}", self.port));
        lines.push(format!("timeout {}", self.timeout));
        lines.push(format!("tcp-keepalive {}", self.tcp_keepalive));
        lines.push(format!("loglevel {:?}", self.loglevel).to_lowercase());
        lines.push(format!("databases {}", self.databases));
        lines.push(String::new());

        lines.push("# Security".to_string());
        if let Some(ref pass) = self.requirepass {
            lines.push(format!("requirepass \"{}\"", pass));
        }
        lines.push(String::new());

        lines.push("# Limits".to_string());
        lines.push(format!("maxclients {}", self.max_clients));
        if self.maxmemory > 0 {
            lines.push(format!("maxmemory {}", self.maxmemory));
        }
        lines.push(String::new());

        lines.push("# Persistence".to_string());
        lines.push(format!("appendonly {}", if self.appendonly { "yes" } else { "no" }));
        lines.push(format!("appendfilename \"{}\"", self.appendfilename));
        lines.push(format!("appendfsync {:?}", self.appendfsync).to_lowercase());
        lines.push(format!("dbfilename \"{}\"", self.dbfilename));
        lines.push(format!("dir \"{}\"", self.dir.display()));
        for save in &self.save {
            lines.push(format!("save {} {}", save.seconds, save.changes));
        }

        lines.join("\n")
    }
}

/// Configuration parsing error.
#[derive(Debug)]
pub enum ConfigError {
    /// I/O error reading config file.
    IoError(String),
    /// Parse error in config file.
    ParseError { line: usize, message: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::IoError(msg) => write!(f, "Config I/O error: {msg}"),
            ConfigError::ParseError { line, message } => {
                write!(f, "Config error at line {line}: {message}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

// Helper functions for parsing

fn parse_bool(value: &str, line: usize) -> Result<bool, ConfigError> {
    match value.to_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(ConfigError::ParseError {
            line,
            message: format!("Invalid boolean: {value}"),
        }),
    }
}

fn parse_number<T: std::str::FromStr>(value: &str, line: usize) -> Result<T, ConfigError> {
    value.parse().map_err(|_| ConfigError::ParseError {
        line,
        message: format!("Invalid number: {value}"),
    })
}

fn parse_memory(value: &str, line: usize) -> Result<usize, ConfigError> {
    let value = value.trim().to_lowercase();

    if value == "0" {
        return Ok(0);
    }

    let (num_str, multiplier) = if value.ends_with("gb") {
        (&value[..value.len() - 2], 1024 * 1024 * 1024)
    } else if value.ends_with("mb") {
        (&value[..value.len() - 2], 1024 * 1024)
    } else if value.ends_with("kb") {
        (&value[..value.len() - 2], 1024)
    } else if value.ends_with('g') {
        (&value[..value.len() - 1], 1024 * 1024 * 1024)
    } else if value.ends_with('m') {
        (&value[..value.len() - 1], 1024 * 1024)
    } else if value.ends_with('k') {
        (&value[..value.len() - 1], 1024)
    } else {
        (value.as_str(), 1)
    };

    let num: usize = num_str.trim().parse().map_err(|_| ConfigError::ParseError {
        line,
        message: format!("Invalid memory value: {value}"),
    })?;

    Ok(num * multiplier)
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let config_str = r#"
# Test config
bind 0.0.0.0
port 6380
timeout 300
maxclients 1000
maxmemory 1gb
appendonly yes
requirepass "secret"
save 900 1
save 300 10
"#;

        let config = Config::parse(config_str).unwrap();
        assert_eq!(config.bind, "0.0.0.0");
        assert_eq!(config.port, 6380);
        assert_eq!(config.timeout, 300);
        assert_eq!(config.max_clients, 1000);
        assert_eq!(config.maxmemory, 1024 * 1024 * 1024);
        assert!(config.appendonly);
        assert_eq!(config.requirepass, Some("secret".to_string()));
        assert_eq!(config.save.len(), 2);
    }

    #[test]
    fn test_parse_memory() {
        assert_eq!(parse_memory("1024", 1).unwrap(), 1024);
        assert_eq!(parse_memory("1kb", 1).unwrap(), 1024);
        assert_eq!(parse_memory("1mb", 1).unwrap(), 1024 * 1024);
        assert_eq!(parse_memory("1gb", 1).unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory("512m", 1).unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("yes", 1).unwrap());
        assert!(parse_bool("true", 1).unwrap());
        assert!(parse_bool("1", 1).unwrap());
        assert!(!parse_bool("no", 1).unwrap());
        assert!(!parse_bool("false", 1).unwrap());
        assert!(!parse_bool("0", 1).unwrap());
    }
}
