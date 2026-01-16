//! RESP frame types.
//!
//! A Frame represents a complete RESP message that can be sent or received.

use super::responses;
use bytes::{BufMut, Bytes, BytesMut};
use std::fmt;

/// A RESP frame representing a complete protocol message.
///
/// # Design
///
/// Frames are designed to be cheap to clone (using `Bytes` for data)
/// and efficient to serialize (direct writes to buffers).
#[derive(Clone, PartialEq)]
pub enum Frame {
    /// Simple string (no newlines allowed)
    Simple(String),

    /// Error message
    Error(String),

    /// 64-bit signed integer
    Integer(i64),

    /// Bulk string (binary-safe)
    Bulk(Bytes),

    /// Null bulk string
    Null,

    /// Array of frames
    Array(Vec<Frame>),
}

impl Frame {
    /// Create a simple string frame.
    #[inline]
    pub fn simple(s: impl Into<String>) -> Self {
        Self::Simple(s.into())
    }

    /// Create an error frame.
    #[inline]
    pub fn error(s: impl Into<String>) -> Self {
        Self::Error(s.into())
    }

    /// Create an integer frame.
    #[inline]
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Create a bulk string frame.
    #[inline]
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Self::Bulk(data.into())
    }

    /// Create a null frame.
    #[inline]
    pub const fn null() -> Self {
        Self::Null
    }

    /// Create an array frame.
    #[inline]
    pub fn array(frames: Vec<Frame>) -> Self {
        Self::Array(frames)
    }

    /// Create an empty array frame.
    #[inline]
    pub fn empty_array() -> Self {
        Self::Array(Vec::new())
    }

    /// Create an OK response.
    #[inline]
    pub fn ok() -> Self {
        Self::Simple("OK".to_string())
    }

    /// Create a PONG response.
    #[inline]
    pub fn pong() -> Self {
        Self::Simple("PONG".to_string())
    }

    /// Create a QUEUED response (for transactions).
    #[inline]
    pub fn queued() -> Self {
        Self::Simple("QUEUED".to_string())
    }

    /// Check if this is a null frame.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Check if this is an error frame.
    #[inline]
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    /// Try to get the frame as a string slice.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Simple(s) | Self::Error(s) => Some(s),
            Self::Bulk(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Try to get the frame as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Simple(s) => Some(s.as_bytes()),
            Self::Bulk(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get the frame as an integer.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            Self::Bulk(b) => std::str::from_utf8(b).ok()?.parse().ok(),
            Self::Simple(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Try to get the frame as an array.
    pub fn as_array(&self) -> Option<&[Frame]> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Convert frame to owned Bytes.
    pub fn to_bytes(&self) -> Option<Bytes> {
        match self {
            Self::Bulk(b) => Some(b.clone()),
            Self::Simple(s) => Some(Bytes::copy_from_slice(s.as_bytes())),
            _ => None,
        }
    }

    /// Serialize the frame to a buffer.
    ///
    /// This is optimized for minimal allocations using pre-computed responses
    /// for common cases like OK, PONG, NULL, and small integers.
    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Self::Simple(s) => {
                // Check for pre-allocated common responses
                match s.as_str() {
                    "OK" => buf.put_slice(responses::OK),
                    "PONG" => buf.put_slice(responses::PONG),
                    "QUEUED" => buf.put_slice(responses::QUEUED),
                    _ => {
                        buf.put_u8(b'+');
                        buf.put_slice(s.as_bytes());
                        buf.put_slice(b"\r\n");
                    }
                }
            }
            Self::Error(s) => {
                buf.put_u8(b'-');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Self::Integer(n) => {
                // Try to use pre-allocated integer response
                if let Some(static_resp) = responses::integer(*n) {
                    buf.put_slice(static_resp);
                } else {
                    buf.put_u8(b':');
                    let mut temp = itoa::Buffer::new();
                    buf.put_slice(temp.format(*n).as_bytes());
                    buf.put_slice(b"\r\n");
                }
            }
            Self::Bulk(data) => {
                // Check for empty bulk string
                if data.is_empty() {
                    buf.put_slice(responses::EMPTY_BULK);
                } else {
                    buf.put_u8(b'$');
                    let mut temp = itoa::Buffer::new();
                    buf.put_slice(temp.format(data.len() as i64).as_bytes());
                    buf.put_slice(b"\r\n");
                    buf.put_slice(data);
                    buf.put_slice(b"\r\n");
                }
            }
            Self::Null => {
                buf.put_slice(responses::NULL_BULK);
            }
            Self::Array(frames) => {
                // Check for empty array
                if frames.is_empty() {
                    buf.put_slice(responses::EMPTY_ARRAY);
                } else {
                    buf.put_u8(b'*');
                    let mut temp = itoa::Buffer::new();
                    buf.put_slice(temp.format(frames.len() as i64).as_bytes());
                    buf.put_slice(b"\r\n");
                    for frame in frames {
                        frame.serialize(buf);
                    }
                }
            }
        }
    }

    /// Calculate the serialized size of this frame.
    pub fn serialized_size(&self) -> usize {
        match self {
            Self::Simple(s) => 1 + s.len() + 2, // +<string>\r\n
            Self::Error(s) => 1 + s.len() + 2,  // -<string>\r\n
            Self::Integer(n) => {
                let digits = if *n == 0 {
                    1
                } else if *n < 0 {
                    (n.abs() as f64).log10().floor() as usize + 2
                } else {
                    (*n as f64).log10().floor() as usize + 1
                };
                1 + digits + 2 // :<integer>\r\n
            }
            Self::Bulk(data) => {
                let len_digits = if data.is_empty() {
                    1
                } else {
                    (data.len() as f64).log10().floor() as usize + 1
                };
                1 + len_digits + 2 + data.len() + 2 // $<len>\r\n<data>\r\n
            }
            Self::Null => 5, // $-1\r\n
            Self::Array(frames) => {
                let len_digits = if frames.is_empty() {
                    1
                } else {
                    (frames.len() as f64).log10().floor() as usize + 1
                };
                let content_size: usize = frames.iter().map(|f| f.serialized_size()).sum();
                1 + len_digits + 2 + content_size // *<len>\r\n<frames>
            }
        }
    }

    /// Convert to a `Vec<u8>` for convenience.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(self.serialized_size());
        self.serialize(&mut buf);
        buf.to_vec()
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple(s) => write!(f, "Simple({s:?})"),
            Self::Error(s) => write!(f, "Error({s:?})"),
            Self::Integer(n) => write!(f, "Integer({n})"),
            Self::Bulk(b) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    write!(f, "Bulk({s:?})")
                } else {
                    write!(f, "Bulk({b:?})")
                }
            }
            Self::Null => write!(f, "Null"),
            Self::Array(arr) => {
                write!(f, "Array[")?;
                for (i, frame) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{frame:?}")?;
                }
                write!(f, "]")
            }
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Simple(s) => write!(f, "+{s}"),
            Self::Error(s) => write!(f, "-{s}"),
            Self::Integer(n) => write!(f, ":{n}"),
            Self::Bulk(b) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    write!(f, "${s}")
                } else {
                    write!(f, "$<{} bytes>", b.len())
                }
            }
            Self::Null => write!(f, "(nil)"),
            Self::Array(arr) => {
                for (i, frame) in arr.iter().enumerate() {
                    if i > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{}) {frame}", i + 1)?;
                }
                Ok(())
            }
        }
    }
}

impl From<&str> for Frame {
    fn from(s: &str) -> Self {
        Self::Bulk(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for Frame {
    fn from(s: String) -> Self {
        Self::Bulk(Bytes::from(s))
    }
}

impl From<Bytes> for Frame {
    fn from(b: Bytes) -> Self {
        Self::Bulk(b)
    }
}

impl From<i64> for Frame {
    fn from(n: i64) -> Self {
        Self::Integer(n)
    }
}

impl From<Vec<Frame>> for Frame {
    fn from(frames: Vec<Frame>) -> Self {
        Self::Array(frames)
    }
}

impl<T: Into<Frame>> FromIterator<T> for Frame {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::Array(iter.into_iter().map(Into::into).collect())
    }
}

/// Fast integer to string conversion.
mod itoa {
    pub struct Buffer {
        bytes: [u8; 20],
    }

    impl Buffer {
        pub fn new() -> Self {
            Self { bytes: [0; 20] }
        }

        pub fn format(&mut self, n: i64) -> &str {
            let mut n = n;
            let negative = n < 0;
            if negative {
                n = -n;
            }

            let mut i = self.bytes.len();
            loop {
                i -= 1;
                self.bytes[i] = b'0' + (n % 10) as u8;
                n /= 10;
                if n == 0 {
                    break;
                }
            }

            if negative {
                i -= 1;
                self.bytes[i] = b'-';
            }

            // We only write ASCII digits and '-', so this is always valid UTF-8
            std::str::from_utf8(&self.bytes[i..]).expect("itoa produces valid UTF-8")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string_serialize() {
        let frame = Frame::simple("OK");
        assert_eq!(frame.to_vec(), b"+OK\r\n");
    }

    #[test]
    fn test_error_serialize() {
        let frame = Frame::error("ERR unknown command");
        assert_eq!(frame.to_vec(), b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_integer_serialize() {
        let frame = Frame::integer(42);
        assert_eq!(frame.to_vec(), b":42\r\n");

        let frame = Frame::integer(-1);
        assert_eq!(frame.to_vec(), b":-1\r\n");

        let frame = Frame::integer(0);
        assert_eq!(frame.to_vec(), b":0\r\n");
    }

    #[test]
    fn test_bulk_string_serialize() {
        let frame = Frame::bulk("hello");
        assert_eq!(frame.to_vec(), b"$5\r\nhello\r\n");

        let frame = Frame::bulk("");
        assert_eq!(frame.to_vec(), b"$0\r\n\r\n");
    }

    #[test]
    fn test_null_serialize() {
        let frame = Frame::null();
        assert_eq!(frame.to_vec(), b"$-1\r\n");
    }

    #[test]
    fn test_array_serialize() {
        let frame = Frame::array(vec![
            Frame::bulk("SET"),
            Frame::bulk("key"),
            Frame::bulk("value"),
        ]);
        assert_eq!(
            frame.to_vec(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn test_empty_array_serialize() {
        let frame = Frame::empty_array();
        assert_eq!(frame.to_vec(), b"*0\r\n");
    }

    #[test]
    fn test_nested_array_serialize() {
        let frame = Frame::array(vec![
            Frame::integer(1),
            Frame::array(vec![Frame::integer(2), Frame::integer(3)]),
        ]);
        assert_eq!(frame.to_vec(), b"*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n");
    }
}
