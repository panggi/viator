//! Access Control List (ACL) implementation.
//!
//! Redis 6+ compatible ACL system providing:
//! - Per-user authentication
//! - Command-level permissions
//! - Key pattern restrictions
//! - Pub/Sub channel permissions
//!
//! # Redis Compatibility
//!
//! Supports Redis ACL commands:
//! - ACL LIST, ACL USERS, ACL GETUSER
//! - ACL SETUSER, ACL DELUSER
//! - ACL CAT, ACL GENPASS
//! - ACL WHOAMI, ACL LOG

use super::password::{PasswordHash, PasswordVerifier};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;

/// ACL command categories (matching Redis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclCategory {
    /// Administrative commands (CONFIG, DEBUG, etc.)
    Admin,
    /// Bitmap commands (SETBIT, GETBIT, etc.)
    Bitmap,
    /// Blocking commands (BLPOP, BRPOP, etc.)
    Blocking,
    /// Connection commands (AUTH, PING, etc.)
    Connection,
    /// Dangerous commands (FLUSHALL, SHUTDOWN, etc.)
    Dangerous,
    /// Fast commands (O(1) or O(log N))
    Fast,
    /// Geo commands
    Geo,
    /// Hash commands
    Hash,
    /// HyperLogLog commands
    Hyperloglog,
    /// Keyspace commands (DEL, EXISTS, etc.)
    Keyspace,
    /// List commands
    List,
    /// Pub/Sub commands
    Pubsub,
    /// Read commands
    Read,
    /// Scripting commands
    Scripting,
    /// Set commands
    Set,
    /// Slow commands
    Slow,
    /// Sorted set commands
    Sortedset,
    /// Stream commands
    Stream,
    /// String commands
    String,
    /// Transaction commands
    Transaction,
    /// Write commands
    Write,
}

impl AclCategory {
    /// Get category from string name.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Self::Admin),
            "bitmap" => Some(Self::Bitmap),
            "blocking" => Some(Self::Blocking),
            "connection" => Some(Self::Connection),
            "dangerous" => Some(Self::Dangerous),
            "fast" => Some(Self::Fast),
            "geo" => Some(Self::Geo),
            "hash" => Some(Self::Hash),
            "hyperloglog" => Some(Self::Hyperloglog),
            "keyspace" => Some(Self::Keyspace),
            "list" => Some(Self::List),
            "pubsub" => Some(Self::Pubsub),
            "read" => Some(Self::Read),
            "scripting" => Some(Self::Scripting),
            "set" => Some(Self::Set),
            "slow" => Some(Self::Slow),
            "sortedset" => Some(Self::Sortedset),
            "stream" => Some(Self::Stream),
            "string" => Some(Self::String),
            "transaction" => Some(Self::Transaction),
            "write" => Some(Self::Write),
            _ => None,
        }
    }

    /// Get string representation.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Bitmap => "bitmap",
            Self::Blocking => "blocking",
            Self::Connection => "connection",
            Self::Dangerous => "dangerous",
            Self::Fast => "fast",
            Self::Geo => "geo",
            Self::Hash => "hash",
            Self::Hyperloglog => "hyperloglog",
            Self::Keyspace => "keyspace",
            Self::List => "list",
            Self::Pubsub => "pubsub",
            Self::Read => "read",
            Self::Scripting => "scripting",
            Self::Set => "set",
            Self::Slow => "slow",
            Self::Sortedset => "sortedset",
            Self::Stream => "stream",
            Self::String => "string",
            Self::Transaction => "transaction",
            Self::Write => "write",
        }
    }

    /// Get all categories.
    #[must_use]
    pub fn all() -> &'static [Self] {
        &[
            Self::Admin,
            Self::Bitmap,
            Self::Blocking,
            Self::Connection,
            Self::Dangerous,
            Self::Fast,
            Self::Geo,
            Self::Hash,
            Self::Hyperloglog,
            Self::Keyspace,
            Self::List,
            Self::Pubsub,
            Self::Read,
            Self::Scripting,
            Self::Set,
            Self::Slow,
            Self::Sortedset,
            Self::Stream,
            Self::String,
            Self::Transaction,
            Self::Write,
        ]
    }
}

/// Permission for a specific command or category.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    /// Allow all commands
    AllCommands,
    /// Deny all commands
    NoCommands,
    /// Allow specific categories
    AllowCategories(HashSet<AclCategory>),
    /// Deny specific categories
    DenyCategories(HashSet<AclCategory>),
    /// Allow specific commands
    AllowCommands(HashSet<String>),
    /// Deny specific commands
    DenyCommands(HashSet<String>),
}

/// Key pattern for ACL restrictions.
#[derive(Debug, Clone)]
pub struct KeyPattern {
    /// The pattern (glob-style)
    pub pattern: String,
    /// Allow read access
    pub read: bool,
    /// Allow write access
    pub write: bool,
}

impl KeyPattern {
    /// Create a pattern allowing all access.
    #[must_use]
    pub fn all_keys() -> Self {
        Self {
            pattern: "*".to_string(),
            read: true,
            write: true,
        }
    }

    /// Create a read-only pattern.
    #[must_use]
    pub fn read_only(pattern: String) -> Self {
        Self {
            pattern,
            read: true,
            write: false,
        }
    }

    /// Create a write-only pattern.
    #[must_use]
    pub fn write_only(pattern: String) -> Self {
        Self {
            pattern,
            read: false,
            write: true,
        }
    }

    /// Check if a key matches this pattern.
    #[must_use]
    pub fn matches(&self, key: &[u8]) -> bool {
        glob_match(self.pattern.as_bytes(), key)
    }
}

/// ACL user definition.
#[derive(Debug)]
pub struct AclUser {
    /// Username
    pub name: String,
    /// Whether the user is enabled
    pub enabled: bool,
    /// Password hashes (user can have multiple passwords)
    pub passwords: Vec<PasswordHash>,
    /// Whether no password is required
    pub nopass: bool,
    /// Allowed command categories
    pub allowed_categories: HashSet<AclCategory>,
    /// Denied command categories
    pub denied_categories: HashSet<AclCategory>,
    /// Allowed specific commands
    pub allowed_commands: HashSet<String>,
    /// Denied specific commands
    pub denied_commands: HashSet<String>,
    /// Key patterns
    pub key_patterns: Vec<KeyPattern>,
    /// Pub/Sub channel patterns
    pub channel_patterns: Vec<String>,
    /// Allow all channels
    pub all_channels: bool,
    /// Reset channels to none
    pub reset_channels: bool,
}

impl AclUser {
    /// Create a new disabled user with no permissions.
    #[must_use]
    pub fn new(name: String) -> Self {
        Self {
            name,
            enabled: false,
            passwords: Vec::new(),
            nopass: false,
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            key_patterns: Vec::new(),
            channel_patterns: Vec::new(),
            all_channels: false,
            reset_channels: false,
        }
    }

    /// Create the default user with full permissions.
    #[must_use]
    pub fn default_user() -> Self {
        let mut user = Self::new("default".to_string());
        user.enabled = true;
        user.nopass = true;
        // Default user has all permissions
        for cat in AclCategory::all() {
            user.allowed_categories.insert(*cat);
        }
        user.key_patterns.push(KeyPattern::all_keys());
        user.all_channels = true;
        user
    }

    /// Check if user can execute a command.
    #[must_use]
    pub fn can_execute(&self, command: &str, categories: &[AclCategory]) -> bool {
        if !self.enabled {
            return false;
        }

        let cmd_upper = command.to_uppercase();

        // Check explicit command deny
        if self.denied_commands.contains(&cmd_upper) {
            return false;
        }

        // Check explicit command allow
        if self.allowed_commands.contains(&cmd_upper) {
            return true;
        }

        // Check category deny
        for cat in categories {
            if self.denied_categories.contains(cat) {
                return false;
            }
        }

        // Check category allow
        for cat in categories {
            if self.allowed_categories.contains(cat) {
                return true;
            }
        }

        false
    }

    /// Check if user can access a key.
    #[must_use]
    pub fn can_access_key(&self, key: &[u8], write: bool) -> bool {
        if !self.enabled {
            return false;
        }

        for pattern in &self.key_patterns {
            if pattern.matches(key) {
                if write && pattern.write {
                    return true;
                }
                if !write && pattern.read {
                    return true;
                }
            }
        }

        false
    }

    /// Check if user can access a Pub/Sub channel.
    #[must_use]
    pub fn can_access_channel(&self, channel: &str) -> bool {
        if !self.enabled {
            return false;
        }

        if self.all_channels {
            return true;
        }

        for pattern in &self.channel_patterns {
            if glob_match(pattern.as_bytes(), channel.as_bytes()) {
                return true;
            }
        }

        false
    }

    /// Authenticate with password.
    #[must_use]
    pub fn authenticate(&self, password: &str) -> bool {
        if !self.enabled {
            return false;
        }

        if self.nopass {
            return true;
        }

        for hash in &self.passwords {
            if PasswordVerifier::verify(password, hash) {
                return true;
            }
        }

        false
    }

    /// Convert to Redis ACL rule string.
    #[must_use]
    pub fn to_acl_string(&self) -> String {
        let mut parts = Vec::new();

        // User state
        parts.push(format!("user {}", self.name));

        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        // Passwords
        if self.nopass {
            parts.push("nopass".to_string());
        } else {
            for hash in &self.passwords {
                parts.push(hash.to_redis_format());
            }
        }

        // Categories
        if self.allowed_categories.len() == AclCategory::all().len() {
            parts.push("+@all".to_string());
        } else {
            for cat in &self.allowed_categories {
                parts.push(format!("+@{}", cat.as_str()));
            }
        }

        for cat in &self.denied_categories {
            parts.push(format!("-@{}", cat.as_str()));
        }

        // Commands
        for cmd in &self.allowed_commands {
            parts.push(format!("+{cmd}"));
        }

        for cmd in &self.denied_commands {
            parts.push(format!("-{cmd}"));
        }

        // Keys
        for pattern in &self.key_patterns {
            if pattern.read && pattern.write {
                parts.push(format!("~{}", pattern.pattern));
            } else if pattern.read {
                parts.push(format!("%R~{}", pattern.pattern));
            } else if pattern.write {
                parts.push(format!("%W~{}", pattern.pattern));
            }
        }

        // Channels
        if self.all_channels {
            parts.push("&*".to_string());
        } else {
            for channel in &self.channel_patterns {
                parts.push(format!("&{channel}"));
            }
        }

        parts.join(" ")
    }
}

/// The main ACL manager.
#[derive(Debug)]
pub struct Acl {
    /// All users
    users: DashMap<String, Arc<RwLock<AclUser>>>,
    /// ACL log for security events
    log: RwLock<Vec<AclLogEntry>>,
    /// Maximum log entries
    log_max_entries: usize,
}

/// ACL log entry for security events.
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    /// Entry ID
    pub id: u64,
    /// Timestamp (Unix milliseconds)
    pub timestamp: i64,
    /// Reason for the log entry
    pub reason: AclLogReason,
    /// Username involved
    pub username: String,
    /// Client info
    pub client_info: String,
    /// Object (command or key)
    pub object: String,
}

/// Reason for ACL log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclLogReason {
    /// Command denied
    Command,
    /// Key access denied
    Key,
    /// Channel access denied
    Channel,
    /// Authentication failure
    Auth,
}

impl Acl {
    /// Create a new ACL manager with default user.
    #[must_use]
    pub fn new() -> Self {
        let acl = Self {
            users: DashMap::new(),
            log: RwLock::new(Vec::new()),
            log_max_entries: 128,
        };

        // Create default user
        acl.users.insert(
            "default".to_string(),
            Arc::new(RwLock::new(AclUser::default_user())),
        );

        acl
    }

    /// Get a user by name.
    #[must_use]
    pub fn get_user(&self, name: &str) -> Option<Arc<RwLock<AclUser>>> {
        self.users.get(name).map(|r| r.clone())
    }

    /// Create or update a user.
    pub fn set_user(&self, user: AclUser) {
        let name = user.name.clone();
        self.users.insert(name, Arc::new(RwLock::new(user)));
    }

    /// Delete a user (cannot delete "default").
    pub fn delete_user(&self, name: &str) -> bool {
        if name == "default" {
            return false;
        }
        self.users.remove(name).is_some()
    }

    /// List all usernames.
    #[must_use]
    pub fn list_users(&self) -> Vec<String> {
        self.users.iter().map(|r| r.key().clone()).collect()
    }

    /// Authenticate a user.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        if let Some(user) = self.get_user(username) {
            let user = user.read();
            if user.authenticate(password) {
                return true;
            }
        }

        // Log authentication failure
        self.log_event(AclLogEntry {
            id: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            reason: AclLogReason::Auth,
            username: username.to_string(),
            client_info: String::new(),
            object: String::new(),
        });

        false
    }

    /// Check if user can execute command.
    pub fn check_command(
        &self,
        username: &str,
        command: &str,
        categories: &[AclCategory],
        client_info: &str,
    ) -> bool {
        if let Some(user) = self.get_user(username) {
            let user = user.read();
            if user.can_execute(command, categories) {
                return true;
            }
        }

        // Log command denial
        self.log_event(AclLogEntry {
            id: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            reason: AclLogReason::Command,
            username: username.to_string(),
            client_info: client_info.to_string(),
            object: command.to_string(),
        });

        false
    }

    /// Check if user can access key.
    pub fn check_key(
        &self,
        username: &str,
        key: &[u8],
        write: bool,
        client_info: &str,
    ) -> bool {
        if let Some(user) = self.get_user(username) {
            let user = user.read();
            if user.can_access_key(key, write) {
                return true;
            }
        }

        // Log key denial
        self.log_event(AclLogEntry {
            id: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            reason: AclLogReason::Key,
            username: username.to_string(),
            client_info: client_info.to_string(),
            object: String::from_utf8_lossy(key).to_string(),
        });

        false
    }

    /// Log a security event.
    fn log_event(&self, mut entry: AclLogEntry) {
        let mut log = self.log.write();
        entry.id = log.len() as u64;

        log.push(entry);

        // Trim old entries
        if log.len() > self.log_max_entries {
            let excess = log.len() - self.log_max_entries;
            log.drain(0..excess);
        }
    }

    /// Get ACL log entries.
    #[must_use]
    pub fn get_log(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let log = self.log.read();
        let count = count.unwrap_or(log.len()).min(log.len());
        log.iter().rev().take(count).cloned().collect()
    }

    /// Reset ACL log.
    pub fn reset_log(&self) {
        self.log.write().clear();
    }

    /// Generate a secure random password.
    #[must_use]
    pub fn generate_password(bits: u32) -> String {
        use rand::RngCore;
        let bytes = (bits / 8) as usize;
        let mut rng = rand::thread_rng();
        let mut password = vec![0u8; bytes];
        rng.fill_bytes(&mut password);

        // Encode as hex
        password.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Default for Acl {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob pattern matching.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = None;
    let mut star_ti = None;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = Some(pi);
            star_ti = Some(ti);
            pi += 1;
        } else if let (Some(spi), Some(sti)) = (star_pi, star_ti) {
            pi = spi + 1;
            star_ti = Some(sti + 1);
            ti = sti + 1;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_user() {
        let acl = Acl::new();
        assert!(acl.authenticate("default", ""));
        assert!(acl.check_command("default", "GET", &[AclCategory::Read], ""));
        assert!(acl.check_command("default", "SET", &[AclCategory::Write], ""));
    }

    #[test]
    fn test_create_user() {
        let acl = Acl::new();

        let mut user = AclUser::new("testuser".to_string());
        user.enabled = true;
        user.passwords.push(PasswordHash::new_sha256("secret"));
        user.allowed_categories.insert(AclCategory::Read);
        user.key_patterns.push(KeyPattern::all_keys());

        acl.set_user(user);

        assert!(acl.authenticate("testuser", "secret"));
        assert!(!acl.authenticate("testuser", "wrong"));
        assert!(acl.check_command("testuser", "GET", &[AclCategory::Read], ""));
        assert!(!acl.check_command("testuser", "SET", &[AclCategory::Write], ""));
    }

    #[test]
    fn test_key_patterns() {
        let mut user = AclUser::new("test".to_string());
        user.enabled = true;
        user.key_patterns.push(KeyPattern {
            pattern: "cache:*".to_string(),
            read: true,
            write: true,
        });
        user.key_patterns.push(KeyPattern::read_only("readonly:*".to_string()));

        assert!(user.can_access_key(b"cache:foo", false));
        assert!(user.can_access_key(b"cache:foo", true));
        assert!(user.can_access_key(b"readonly:bar", false));
        assert!(!user.can_access_key(b"readonly:bar", true));
        assert!(!user.can_access_key(b"other:key", false));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"foo*", b"foobar"));
        assert!(glob_match(b"*bar", b"foobar"));
        assert!(glob_match(b"foo*bar", b"foobazbar"));
        assert!(glob_match(b"foo?bar", b"fooxbar"));
        assert!(!glob_match(b"foo", b"bar"));
        assert!(!glob_match(b"foo?", b"foo"));
    }

    #[test]
    fn test_acl_log() {
        let acl = Acl::new();

        // Trigger failed auth
        acl.authenticate("nonexistent", "password");

        let log = acl.get_log(Some(10));
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].reason, AclLogReason::Auth);
    }

    #[test]
    fn test_generate_password() {
        let password = Acl::generate_password(256);
        assert_eq!(password.len(), 64); // 256 bits = 32 bytes = 64 hex chars
    }
}
