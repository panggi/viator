//! Audit logging for security events.
//!
//! Provides comprehensive logging of security-relevant events:
//! - Authentication attempts (success/failure)
//! - Authorization denials
//! - Configuration changes
//! - Administrative commands
//!
//! # Redis Compatibility
//!
//! Integrates with ACL LOG command for viewing security events.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, info, warn};

/// Configuration for audit logging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogConfig {
    /// Enable audit logging
    pub enabled: bool,
    /// Log file path (None for memory only)
    pub file_path: Option<PathBuf>,
    /// Maximum entries to keep in memory
    pub max_memory_entries: usize,
    /// Log successful authentications
    pub log_auth_success: bool,
    /// Log failed authentications
    pub log_auth_failure: bool,
    /// Log authorization denials
    pub log_authz_denial: bool,
    /// Log admin commands
    pub log_admin_commands: bool,
    /// Log configuration changes
    pub log_config_changes: bool,
    /// Log connection events
    pub log_connections: bool,
}

impl Default for AuditLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            file_path: None,
            max_memory_entries: 1000,
            log_auth_success: false, // High volume, disabled by default
            log_auth_failure: true,
            log_authz_denial: true,
            log_admin_commands: true,
            log_config_changes: true,
            log_connections: false, // High volume, disabled by default
        }
    }
}

/// Types of audit events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    /// Successful authentication
    AuthSuccess,
    /// Failed authentication
    AuthFailure,
    /// Authorization denied (command)
    AuthzDeniedCommand,
    /// Authorization denied (key access)
    AuthzDeniedKey,
    /// Authorization denied (channel access)
    AuthzDeniedChannel,
    /// Admin command executed
    AdminCommand,
    /// Configuration changed
    ConfigChange,
    /// Client connected
    ClientConnect,
    /// Client disconnected
    ClientDisconnect,
    /// Security alert (e.g., brute force detected)
    SecurityAlert,
    /// ACL modified
    AclChange,
    /// TLS handshake
    TlsHandshake,
}

/// A single audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID
    pub id: u64,
    /// Timestamp (ISO 8601)
    pub timestamp: String,
    /// Unix timestamp in milliseconds
    pub timestamp_ms: i64,
    /// Event type
    pub event_type: AuditEventType,
    /// Severity level
    pub severity: AuditSeverity,
    /// Username involved (if any)
    pub username: Option<String>,
    /// Client address
    pub client_addr: Option<String>,
    /// Client ID
    pub client_id: Option<u64>,
    /// Command that triggered the event
    pub command: Option<String>,
    /// Key involved (if any)
    pub key: Option<String>,
    /// Additional details
    pub details: Option<String>,
    /// Whether the action was successful
    pub success: bool,
}

/// Audit event severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditSeverity {
    /// Informational
    Info,
    /// Warning
    Warning,
    /// Error/security concern
    Error,
    /// Critical security event
    Critical,
}

impl AuditEvent {
    /// Create a new audit event.
    fn new(event_type: AuditEventType, severity: AuditSeverity) -> Self {
        let now = chrono::Utc::now();
        Self {
            id: 0, // Set by AuditLog
            timestamp: now.to_rfc3339(),
            timestamp_ms: now.timestamp_millis(),
            event_type,
            severity,
            username: None,
            client_addr: None,
            client_id: None,
            command: None,
            key: None,
            details: None,
            success: true,
        }
    }

    /// Set username.
    #[must_use]
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set client address.
    #[must_use]
    pub fn with_client_addr(mut self, addr: SocketAddr) -> Self {
        self.client_addr = Some(addr.to_string());
        self
    }

    /// Set client ID.
    #[must_use]
    pub fn with_client_id(mut self, id: u64) -> Self {
        self.client_id = Some(id);
        self
    }

    /// Set command.
    #[must_use]
    pub fn with_command(mut self, command: impl Into<String>) -> Self {
        self.command = Some(command.into());
        self
    }

    /// Set key.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set details.
    #[must_use]
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Set success flag.
    #[must_use]
    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }
}

/// The audit log manager.
pub struct AuditLog {
    /// Configuration
    config: RwLock<AuditLogConfig>,
    /// In-memory log entries
    entries: RwLock<VecDeque<AuditEvent>>,
    /// Event counter
    counter: AtomicU64,
    /// File writer (if configured)
    file_writer: RwLock<Option<BufWriter<File>>>,
}

impl std::fmt::Debug for AuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLog")
            .field("config", &self.config)
            .field("entries_count", &self.entries.read().len())
            .field("counter", &self.counter)
            .finish()
    }
}

impl AuditLog {
    /// Create a new audit log with the given configuration.
    pub fn new(config: AuditLogConfig) -> Self {
        let file_writer = config.file_path.as_ref().and_then(|path| {
            match OpenOptions::new().create(true).append(true).open(path) {
                Ok(file) => Some(BufWriter::new(file)),
                Err(e) => {
                    error!("Failed to open audit log file: {}", e);
                    None
                }
            }
        });

        Self {
            config: RwLock::new(config),
            entries: RwLock::new(VecDeque::new()),
            counter: AtomicU64::new(0),
            file_writer: RwLock::new(file_writer),
        }
    }

    /// Log an event.
    pub fn log(&self, mut event: AuditEvent) {
        let config = self.config.read();

        if !config.enabled {
            return;
        }

        // Check if this event type should be logged
        let should_log = match event.event_type {
            AuditEventType::AuthSuccess => config.log_auth_success,
            AuditEventType::AuthFailure => config.log_auth_failure,
            AuditEventType::AuthzDeniedCommand
            | AuditEventType::AuthzDeniedKey
            | AuditEventType::AuthzDeniedChannel => config.log_authz_denial,
            AuditEventType::AdminCommand => config.log_admin_commands,
            AuditEventType::ConfigChange | AuditEventType::AclChange => config.log_config_changes,
            AuditEventType::ClientConnect | AuditEventType::ClientDisconnect => {
                config.log_connections
            }
            AuditEventType::SecurityAlert | AuditEventType::TlsHandshake => true,
        };

        if !should_log {
            return;
        }

        // Assign ID
        event.id = self.counter.fetch_add(1, Ordering::Relaxed);

        // Log to tracing
        match event.severity {
            AuditSeverity::Critical => {
                error!(
                    target: "audit",
                    event_type = ?event.event_type,
                    username = ?event.username,
                    client = ?event.client_addr,
                    command = ?event.command,
                    "CRITICAL: {:?}", event.details
                );
            }
            AuditSeverity::Error => {
                warn!(
                    target: "audit",
                    event_type = ?event.event_type,
                    username = ?event.username,
                    client = ?event.client_addr,
                    "{:?}", event.details
                );
            }
            AuditSeverity::Warning => {
                warn!(
                    target: "audit",
                    event_type = ?event.event_type,
                    "{:?}", event.details
                );
            }
            AuditSeverity::Info => {
                info!(
                    target: "audit",
                    event_type = ?event.event_type,
                    "{:?}", event.details
                );
            }
        }

        // Write to file
        drop(config);
        if let Some(ref mut writer) = *self.file_writer.write() {
            if let Ok(json) = serde_json::to_string(&event) {
                let _ = writeln!(writer, "{json}");
                let _ = writer.flush();
            }
        }

        // Add to memory buffer
        let config = self.config.read();
        let mut entries = self.entries.write();
        entries.push_back(event);

        // Trim old entries
        while entries.len() > config.max_memory_entries {
            entries.pop_front();
        }
    }

    /// Get recent events.
    #[must_use]
    pub fn get_events(&self, count: Option<usize>) -> Vec<AuditEvent> {
        let entries = self.entries.read();
        let count = count.unwrap_or(entries.len()).min(entries.len());
        entries.iter().rev().take(count).cloned().collect()
    }

    /// Get events by type.
    #[must_use]
    pub fn get_events_by_type(&self, event_type: AuditEventType, count: usize) -> Vec<AuditEvent> {
        let entries = self.entries.read();
        entries
            .iter()
            .rev()
            .filter(|e| e.event_type == event_type)
            .take(count)
            .cloned()
            .collect()
    }

    /// Clear the in-memory log.
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Update configuration.
    pub fn update_config(&self, config: AuditLogConfig) {
        // Handle file path change
        if config.file_path != self.config.read().file_path {
            let file_writer = config.file_path.as_ref().and_then(|path| {
                match OpenOptions::new().create(true).append(true).open(path) {
                    Ok(file) => Some(BufWriter::new(file)),
                    Err(e) => {
                        error!("Failed to open audit log file: {}", e);
                        None
                    }
                }
            });
            *self.file_writer.write() = file_writer;
        }

        *self.config.write() = config;
    }

    // Convenience methods for common events

    /// Log successful authentication.
    pub fn log_auth_success(&self, username: &str, client_addr: SocketAddr) {
        self.log(
            AuditEvent::new(AuditEventType::AuthSuccess, AuditSeverity::Info)
                .with_username(username)
                .with_client_addr(client_addr)
                .with_details("Authentication successful"),
        );
    }

    /// Log failed authentication.
    pub fn log_auth_failure(&self, username: &str, client_addr: SocketAddr, reason: &str) {
        self.log(
            AuditEvent::new(AuditEventType::AuthFailure, AuditSeverity::Warning)
                .with_username(username)
                .with_client_addr(client_addr)
                .with_details(format!("Authentication failed: {reason}"))
                .with_success(false),
        );
    }

    /// Log command authorization denial.
    pub fn log_authz_denied_command(&self, username: &str, client_addr: SocketAddr, command: &str) {
        self.log(
            AuditEvent::new(AuditEventType::AuthzDeniedCommand, AuditSeverity::Warning)
                .with_username(username)
                .with_client_addr(client_addr)
                .with_command(command)
                .with_details(format!("Command not permitted: {command}"))
                .with_success(false),
        );
    }

    /// Log key access denial.
    pub fn log_authz_denied_key(
        &self,
        username: &str,
        client_addr: SocketAddr,
        key: &str,
        write: bool,
    ) {
        let op = if write { "write" } else { "read" };
        self.log(
            AuditEvent::new(AuditEventType::AuthzDeniedKey, AuditSeverity::Warning)
                .with_username(username)
                .with_client_addr(client_addr)
                .with_key(key)
                .with_details(format!("Key {op} not permitted: {key}"))
                .with_success(false),
        );
    }

    /// Log admin command execution.
    pub fn log_admin_command(&self, username: &str, client_addr: SocketAddr, command: &str) {
        self.log(
            AuditEvent::new(AuditEventType::AdminCommand, AuditSeverity::Info)
                .with_username(username)
                .with_client_addr(client_addr)
                .with_command(command)
                .with_details(format!("Admin command executed: {command}")),
        );
    }

    /// Log configuration change.
    pub fn log_config_change(&self, username: &str, parameter: &str, value: &str) {
        self.log(
            AuditEvent::new(AuditEventType::ConfigChange, AuditSeverity::Info)
                .with_username(username)
                .with_details(format!("Config changed: {parameter} = {value}")),
        );
    }

    /// Log ACL change.
    pub fn log_acl_change(&self, username: &str, target_user: &str, action: &str) {
        self.log(
            AuditEvent::new(AuditEventType::AclChange, AuditSeverity::Info)
                .with_username(username)
                .with_details(format!("ACL changed for user '{target_user}': {action}")),
        );
    }

    /// Log security alert.
    pub fn log_security_alert(&self, client_addr: SocketAddr, message: &str) {
        self.log(
            AuditEvent::new(AuditEventType::SecurityAlert, AuditSeverity::Critical)
                .with_client_addr(client_addr)
                .with_details(message),
        );
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(AuditLogConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345)
    }

    #[test]
    fn test_audit_log_auth_failure() {
        let log = AuditLog::default();
        log.log_auth_failure("testuser", test_addr(), "invalid password");

        let events = log.get_events(Some(10));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, AuditEventType::AuthFailure);
        assert_eq!(events[0].username, Some("testuser".to_string()));
    }

    #[test]
    fn test_audit_log_disabled() {
        let config = AuditLogConfig {
            enabled: false,
            ..Default::default()
        };
        let log = AuditLog::new(config);
        log.log_auth_failure("testuser", test_addr(), "invalid password");

        let events = log.get_events(Some(10));
        assert!(events.is_empty());
    }

    #[test]
    fn test_audit_log_max_entries() {
        let config = AuditLogConfig {
            max_memory_entries: 5,
            ..Default::default()
        };
        let log = AuditLog::new(config);

        for i in 0..10 {
            log.log_auth_failure(&format!("user{i}"), test_addr(), "test");
        }

        let events = log.get_events(None);
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn test_event_builder() {
        let event = AuditEvent::new(AuditEventType::AdminCommand, AuditSeverity::Info)
            .with_username("admin")
            .with_client_addr(test_addr())
            .with_command("FLUSHALL")
            .with_details("Database flushed");

        assert_eq!(event.username, Some("admin".to_string()));
        assert_eq!(event.command, Some("FLUSHALL".to_string()));
    }
}
