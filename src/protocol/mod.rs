//! Redis Serialization Protocol (RESP) implementation.
//!
//! This module implements RESP2 and RESP3 protocol parsing and serialization.
//! The parser is designed for zero-copy operation where possible.

mod frame;
mod parser;
pub mod responses;

pub use frame::Frame;
pub use parser::{RespParser, parse_frame};
pub use responses::{OK, PONG, NULL_BULK, NULL_ARRAY, EMPTY_BULK, EMPTY_ARRAY, ZERO, ONE};

/// RESP protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RespVersion {
    /// RESP2 (Redis 2.0+)
    #[default]
    Resp2,
    /// RESP3 (Redis 6.0+)
    Resp3,
}

/// CRLF terminator bytes.
pub const CRLF: &[u8] = b"\r\n";

/// Type markers for RESP.
pub mod markers {
    /// Simple string: +
    pub const SIMPLE_STRING: u8 = b'+';
    /// Error: -
    pub const ERROR: u8 = b'-';
    /// Integer: :
    pub const INTEGER: u8 = b':';
    /// Bulk string: $
    pub const BULK_STRING: u8 = b'$';
    /// Array: *
    pub const ARRAY: u8 = b'*';

    // RESP3 additions
    /// Null: _
    pub const NULL: u8 = b'_';
    /// Boolean: #
    pub const BOOLEAN: u8 = b'#';
    /// Double: ,
    pub const DOUBLE: u8 = b',';
    /// Big number: (
    pub const BIG_NUMBER: u8 = b'(';
    /// Bulk error: !
    pub const BULK_ERROR: u8 = b'!';
    /// Verbatim string: =
    pub const VERBATIM_STRING: u8 = b'=';
    /// Map: %
    pub const MAP: u8 = b'%';
    /// Set: ~
    pub const SET: u8 = b'~';
    /// Attribute: |
    pub const ATTRIBUTE: u8 = b'|';
    /// Push: >
    pub const PUSH: u8 = b'>';
}
