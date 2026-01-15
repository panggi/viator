//! Security module for Viator.
//!
//! Provides comprehensive security features compatible with Redis 8.4.0:
//! - ACL (Access Control Lists) for per-user permissions
//! - Secure password hashing with Argon2id
//! - Audit logging for security events
//! - TLS configuration helpers
//! - Command protection and sandboxing
//! - Per-connection resource limits
//! - Lua sandbox for secure script execution

pub mod acl;
pub mod audit;
pub mod command_protection;
pub mod connection_limits;
pub mod lua_sandbox;
pub mod password;
pub mod tls_config;

pub use acl::{Acl, AclCategory, AclUser, Permission};
pub use audit::{AuditEvent, AuditLog, AuditLogConfig};
pub use command_protection::{CommandProtection, CommandProtectionConfig, CommandTimer};
pub use connection_limits::{
    ClientType, ConnectionLimits, ConnectionLimitsConfig, ConnectionMemoryTracker, LimitViolation,
    OutputBufferLimits,
};
pub use lua_sandbox::{
    ExecutionLimits, LuaSandboxConfig, ResourceTracker, SafeEnvironmentBuilder, SandboxError,
    ScriptValidator,
};
pub use password::{PasswordHash, PasswordVerifier};
pub use tls_config::{TlsAuthClients, TlsConfig, TlsError, TlsVersion};
