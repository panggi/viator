//! Error types for redis-rs.
//!
//! This module provides a comprehensive error type hierarchy that covers
//! all possible failure modes in the Redis implementation.

use std::io;
use std::net::AddrParseError;
use std::num::{ParseFloatError, ParseIntError};
use thiserror::Error;

/// Result type alias for redis-rs operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for redis-rs.
///
/// This enum represents all possible errors that can occur during
/// Redis operations. It is designed to be both ergonomic and informative,
/// providing detailed error messages for debugging while maintaining
/// type safety.
#[derive(Error, Debug)]
pub enum Error {
    /// Protocol parsing errors
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Command execution errors
    #[error("command error: {0}")]
    Command(#[from] CommandError),

    /// Storage errors
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Authentication errors
    #[error("authentication error: {0}")]
    Auth(#[from] AuthError),

    /// Connection errors
    #[error("connection error: {0}")]
    Connection(String),

    /// Configuration errors
    #[error("configuration error: {0}")]
    Config(String),

    /// Resource limit exceeded
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Internal server error
    #[error("internal error: {0}")]
    Internal(String),

    /// Address parsing error
    #[error("address parse error: {0}")]
    AddrParse(#[from] AddrParseError),
}

/// Protocol-level errors during RESP parsing.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Invalid RESP data type marker
    #[error("invalid type marker: {0:?}")]
    InvalidTypeMarker(u8),

    /// Invalid UTF-8 in simple string
    #[error("invalid UTF-8 in string")]
    InvalidUtf8,

    /// Invalid integer format
    #[error("invalid integer: {0}")]
    InvalidInteger(String),

    /// Invalid float format
    #[error("invalid float: {0}")]
    InvalidFloat(String),

    /// Invalid bulk string length
    #[error("invalid bulk string length: {0}")]
    InvalidBulkLength(i64),

    /// Invalid array length
    #[error("invalid array length: {0}")]
    InvalidArrayLength(i64),

    /// Unexpected end of input
    #[error("unexpected end of input")]
    UnexpectedEof,

    /// Line too long (exceeds inline limit)
    #[error("line too long: {len} bytes (max: {max})")]
    LineTooLong {
        /// Actual line length in bytes
        len: usize,
        /// Maximum allowed length
        max: usize,
    },

    /// Bulk string too large
    #[error("bulk string too large: {len} bytes (max: {max})")]
    BulkTooLarge {
        /// Actual bulk string length in bytes
        len: usize,
        /// Maximum allowed length
        max: usize,
    },

    /// Too many array elements
    #[error("too many array elements: {count} (max: {max})")]
    TooManyElements {
        /// Actual element count
        count: usize,
        /// Maximum allowed count
        max: usize,
    },

    /// Missing CRLF terminator
    #[error("missing CRLF terminator")]
    MissingCrlf,

    /// Protocol version mismatch
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    /// Incomplete frame - need more data
    #[error("incomplete frame, need more data")]
    Incomplete,
}

/// Command execution errors.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    /// Unknown command
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),

    /// Wrong number of arguments
    #[error("ERR wrong number of arguments for '{command}' command")]
    WrongArity {
        /// Command name that received wrong arity
        command: String,
    },

    /// Wrong type for operation
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// Invalid argument
    #[error("ERR invalid argument '{arg}' for '{command}'")]
    InvalidArgument {
        /// Command name
        command: String,
        /// The invalid argument value
        arg: String,
    },

    /// Syntax error
    #[error("ERR syntax error")]
    SyntaxError,

    /// Value out of range
    #[error("ERR value is out of range")]
    OutOfRange,

    /// Not an integer
    #[error("ERR value is not an integer or out of range")]
    NotInteger,

    /// Not a float
    #[error("ERR value is not a valid float")]
    NotFloat,

    /// Invalid cursor
    #[error("ERR invalid cursor")]
    InvalidCursor,

    /// No such key
    #[error("ERR no such key")]
    NoSuchKey,

    /// Key exists
    #[error("ERR key already exists")]
    KeyExists,

    /// Invalid expire time
    #[error("ERR invalid expire time")]
    InvalidExpireTime,

    /// Index out of bounds
    #[error("ERR index out of range")]
    IndexOutOfRange,

    /// Nested MULTI call
    #[error("ERR MULTI calls can not be nested")]
    NestedMulti,

    /// EXEC without MULTI
    #[error("ERR EXEC without MULTI")]
    ExecWithoutMulti,

    /// DISCARD without MULTI
    #[error("ERR DISCARD without MULTI")]
    DiscardWithoutMulti,

    /// Transaction aborted
    #[error("EXECABORT Transaction discarded because of previous errors")]
    TransactionAborted,

    /// WATCH inside MULTI
    #[error("ERR WATCH inside MULTI is not allowed")]
    WatchInsideMulti,

    /// Busy key (for operations like RENAME)
    #[error("ERR target key name is busy")]
    BusyKey,

    /// Operation not permitted
    #[error("NOPERM this user has no permissions to run the '{0}' command")]
    NoPermission(String),

    /// Read-only replica
    #[error("READONLY You can't write against a read only replica")]
    ReadOnlyReplica,

    /// Command disabled
    #[error("ERR command '{0}' is disabled")]
    CommandDisabled(String),

    /// OOM condition
    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    OutOfMemory,

    /// Cluster redirect - key moved to another slot
    #[error("MOVED {slot} {addr}")]
    Moved {
        /// Hash slot number
        slot: u16,
        /// Target node address
        addr: String,
    },

    /// Cluster redirect - key is being migrated
    #[error("ASK {slot} {addr}")]
    Ask {
        /// Hash slot number
        slot: u16,
        /// Target node address
        addr: String,
    },

    /// Cluster is in a down state
    #[error("CLUSTERDOWN The cluster is down")]
    ClusterDown,

    /// Bit value out of range
    #[error("ERR bit is not an integer or out of range")]
    BitValueOutOfRange,

    /// Invalid stream ID
    #[error("ERR Invalid stream ID specified as stream command argument")]
    StreamInvalidId,

    /// Stream ID is equal or smaller than the target stream top item
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    StreamIdTooSmall,

    /// Script not found in cache
    #[error("NOSCRIPT No matching script. Please use EVAL.")]
    NoScript(String),

    /// Script execution error
    #[error("ERR Error running script: {0}")]
    ScriptError(String),

    /// Function not found
    #[error("ERR Function not found")]
    NoFunction(String),
}

/// Storage-level errors.
#[derive(Error, Debug)]
pub enum StorageError {
    /// Persistence error
    #[error("persistence error: {0}")]
    Persistence(String),

    /// Corrupted data
    #[error("corrupted data: {0}")]
    Corrupted(String),

    /// Checksum mismatch
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Expected checksum value
        expected: u32,
        /// Actual computed checksum
        actual: u32,
    },

    /// Version mismatch
    #[error("unsupported VDB version: {0}")]
    UnsupportedVersion(u32),

    /// File not found
    #[error("file not found: {0}")]
    FileNotFound(String),

    /// Disk full
    #[error("disk full or quota exceeded")]
    DiskFull,

    /// Lock acquisition failed
    #[error("failed to acquire lock: {0}")]
    LockFailed(String),

    /// Database index out of range
    #[error("ERR DB index is out of range")]
    DbIndexOutOfRange,

    /// I/O error wrapper
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Authentication errors.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Invalid password
    #[error("WRONGPASS invalid username-password pair")]
    WrongPassword,

    /// Authentication required
    #[error("NOAUTH Authentication required")]
    AuthRequired,

    /// Invalid ACL rule
    #[error("ERR invalid ACL rule: {0}")]
    InvalidAclRule(String),

    /// User not found
    #[error("ERR user '{0}' not found")]
    UserNotFound(String),

    /// Too many authentication failures
    #[error("ERR too many authentication failures")]
    TooManyFailures,
}

impl Error {
    /// Returns true if this error is retryable.
    #[inline]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Io(_) | Error::Connection(_) | Error::ResourceLimit(_)
        )
    }

    /// Returns true if this is a client error (4xx equivalent).
    #[inline]
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Error::Protocol(_) | Error::Command(_) | Error::Auth(_)
        )
    }

    /// Returns true if this is a server error (5xx equivalent).
    #[inline]
    pub fn is_server_error(&self) -> bool {
        matches!(
            self,
            Error::Storage(_) | Error::Internal(_) | Error::Io(_)
        )
    }

    /// Converts the error to a Redis error response string.
    #[must_use]
    pub fn to_viator_error(&self) -> String {
        match self {
            Error::Protocol(e) => format!("ERR {e}"),
            Error::Command(e) => e.to_string(),
            Error::Auth(e) => e.to_string(),
            Error::Storage(e) => format!("ERR {e}"),
            Error::Io(e) => format!("ERR I/O error: {e}"),
            Error::Connection(e) => format!("ERR connection error: {e}"),
            Error::Config(e) => format!("ERR configuration error: {e}"),
            Error::ResourceLimit(e) => format!("ERR {e}"),
            Error::Internal(e) => format!("ERR internal error: {e}"),
            Error::AddrParse(e) => format!("ERR address parse error: {e}"),
        }
    }
}

impl From<ParseIntError> for ProtocolError {
    fn from(e: ParseIntError) -> Self {
        ProtocolError::InvalidInteger(e.to_string())
    }
}

impl From<ParseFloatError> for ProtocolError {
    fn from(e: ParseFloatError) -> Self {
        ProtocolError::InvalidFloat(e.to_string())
    }
}

/// Error context extension trait.
pub trait ErrorContext<T> {
    /// Adds context to an error.
    fn context(self, msg: &str) -> Result<T>;
}

impl<T, E: Into<Error>> ErrorContext<T> for std::result::Result<T, E> {
    fn context(self, msg: &str) -> Result<T> {
        self.map_err(|e| {
            let err: Error = e.into();
            Error::Internal(format!("{msg}: {err}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Command(CommandError::WrongArity {
            command: "GET".to_string(),
        });
        assert_eq!(
            err.to_string(),
            "command error: ERR wrong number of arguments for 'GET' command"
        );
    }

    #[test]
    fn test_protocol_error_display() {
        let err = ProtocolError::InvalidTypeMarker(b'X');
        assert_eq!(err.to_string(), "invalid type marker: 88");
    }

    #[test]
    fn test_error_classification() {
        let client_err = Error::Command(CommandError::SyntaxError);
        assert!(client_err.is_client_error());
        assert!(!client_err.is_server_error());

        let server_err = Error::Internal("test".to_string());
        assert!(!server_err.is_client_error());
        assert!(server_err.is_server_error());
    }
}
