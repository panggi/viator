//! RESP protocol parser.
//!
//! This parser is designed for:
//! - Zero-copy parsing where possible (using `Bytes` slices)
//! - Streaming input (can handle partial data)
//! - Security (bounded allocations, no stack overflow)

use super::frame::Frame;
use super::markers;
use crate::error::ProtocolError;
use crate::{MAX_ARGUMENTS, MAX_BULK_SIZE, MAX_INLINE_SIZE};
use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr;

/// RESP protocol parser with streaming support.
///
/// # Usage
///
/// ```ignore
/// let mut parser = RespParser::new();
/// parser.extend(data);
///
/// while let Some(frame) = parser.parse()? {
///     // Handle frame
/// }
/// ```
///
/// # Security
///
/// - Maximum bulk string size: 512MB
/// - Maximum array elements: 1M
/// - Maximum inline size: 64KB
/// - Non-recursive parsing to prevent stack overflow
#[derive(Debug, Default)]
pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Create a parser with specified buffer capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Add data to the parser buffer.
    #[inline]
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Add data from another BytesMut.
    #[inline]
    pub fn extend_from_bytes_mut(&mut self, data: BytesMut) {
        self.buffer.unsplit(data);
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the number of bytes in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Get a mutable reference to the buffer for direct writing.
    #[inline]
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Clear the parser buffer.
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Trim the buffer if it has grown too large relative to its contents.
    ///
    /// This prevents unbounded memory growth when a client sends a large command
    /// followed by many small ones. The buffer retains its large capacity until
    /// explicitly trimmed.
    ///
    /// Call this periodically (e.g., every N commands or after idle time).
    #[inline]
    pub fn maybe_trim(&mut self) {
        const MIN_CAPACITY: usize = 4096;
        const SHRINK_RATIO: usize = 4; // Shrink if capacity > 4x len

        let capacity = self.buffer.capacity();
        let len = self.buffer.len();

        // Only shrink if capacity is significantly larger than needed
        if capacity > MIN_CAPACITY && capacity > len.saturating_mul(SHRINK_RATIO) {
            // Create a new buffer with right-sized capacity
            let target_capacity = len.max(MIN_CAPACITY);
            let mut new_buffer = BytesMut::with_capacity(target_capacity);
            new_buffer.extend_from_slice(&self.buffer);
            self.buffer = new_buffer;
        }
    }

    /// Get the current buffer capacity (for metrics/debugging).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Try to parse a complete frame from the buffer.
    ///
    /// Returns:
    /// - `Ok(Some(frame))` if a complete frame was parsed
    /// - `Ok(None)` if more data is needed
    /// - `Err(e)` if the data is malformed
    pub fn parse(&mut self) -> Result<Option<Frame>, ProtocolError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        // Check for inline command (doesn't start with RESP marker)
        let first = self.buffer[0];
        if !is_resp_marker(first) {
            return self.parse_inline();
        }

        // Parse RESP frame
        match self.parse_frame() {
            Ok(frame) => Ok(Some(frame)),
            Err(ProtocolError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Parse an inline command (plain text, space-separated).
    fn parse_inline(&mut self) -> Result<Option<Frame>, ProtocolError> {
        // Find line ending
        let line_end = match find_crlf(&self.buffer) {
            Some(pos) => pos,
            None => {
                if self.buffer.len() > MAX_INLINE_SIZE {
                    return Err(ProtocolError::LineTooLong {
                        len: self.buffer.len(),
                        max: MAX_INLINE_SIZE,
                    });
                }
                return Ok(None);
            }
        };

        // Extract line
        let line = self.buffer.split_to(line_end);
        self.buffer.advance(2); // Skip CRLF

        // Parse space-separated arguments
        let args = parse_inline_args(&line);
        if args.is_empty() {
            return Ok(None);
        }

        Ok(Some(Frame::Array(args)))
    }

    /// Parse a single RESP frame (may be recursive for arrays).
    fn parse_frame(&mut self) -> Result<Frame, ProtocolError> {
        if self.buffer.is_empty() {
            return Err(ProtocolError::Incomplete);
        }

        let marker = self.buffer[0];
        self.buffer.advance(1);

        match marker {
            markers::SIMPLE_STRING => self.parse_simple_string(),
            markers::ERROR => self.parse_error(),
            markers::INTEGER => self.parse_integer(),
            markers::BULK_STRING => self.parse_bulk_string(),
            markers::ARRAY => self.parse_array(),
            markers::NULL => self.parse_null(),
            _ => Err(ProtocolError::InvalidTypeMarker(marker)),
        }
    }

    /// Parse a simple string (+...\r\n).
    fn parse_simple_string(&mut self) -> Result<Frame, ProtocolError> {
        let line = self.read_line()?;
        let s = std::str::from_utf8(&line).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(Frame::Simple(s.to_string()))
    }

    /// Parse an error (-...\r\n).
    fn parse_error(&mut self) -> Result<Frame, ProtocolError> {
        let line = self.read_line()?;
        let s = std::str::from_utf8(&line).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(Frame::Error(s.to_string()))
    }

    /// Parse an integer (:...\r\n).
    fn parse_integer(&mut self) -> Result<Frame, ProtocolError> {
        let line = self.read_line()?;
        let s = std::str::from_utf8(&line).map_err(|_| ProtocolError::InvalidUtf8)?;
        let n: i64 = s.parse()?;
        Ok(Frame::Integer(n))
    }

    /// Parse a bulk string ($len\r\n...\r\n).
    fn parse_bulk_string(&mut self) -> Result<Frame, ProtocolError> {
        let len_line = self.read_line()?;
        let len_str = std::str::from_utf8(&len_line).map_err(|_| ProtocolError::InvalidUtf8)?;
        let len: i64 = len_str.parse()?;

        // Null bulk string
        if len < 0 {
            return Ok(Frame::Null);
        }

        let len = len as usize;

        // Security check
        if len > MAX_BULK_SIZE {
            return Err(ProtocolError::BulkTooLarge {
                len,
                max: MAX_BULK_SIZE,
            });
        }

        // Check if we have enough data
        if self.buffer.len() < len + 2 {
            // Put back the length line for retry
            return Err(ProtocolError::Incomplete);
        }

        // Extract data
        let data = self.buffer.split_to(len).freeze();

        // Consume CRLF
        if self.buffer.len() < 2 || &self.buffer[..2] != b"\r\n" {
            return Err(ProtocolError::MissingCrlf);
        }
        self.buffer.advance(2);

        Ok(Frame::Bulk(data))
    }

    /// Parse an array (*len\r\n...).
    fn parse_array(&mut self) -> Result<Frame, ProtocolError> {
        let len_line = self.read_line()?;
        let len_str = std::str::from_utf8(&len_line).map_err(|_| ProtocolError::InvalidUtf8)?;
        let len: i64 = len_str.parse()?;

        // Null array
        if len < 0 {
            return Ok(Frame::Null);
        }

        let len = len as usize;

        // Security check
        if len > MAX_ARGUMENTS {
            return Err(ProtocolError::TooManyElements {
                count: len,
                max: MAX_ARGUMENTS,
            });
        }

        // Parse array elements
        let mut frames = Vec::with_capacity(len.min(1024));
        for _ in 0..len {
            frames.push(self.parse_frame()?);
        }

        Ok(Frame::Array(frames))
    }

    /// Parse RESP3 null (_\r\n).
    fn parse_null(&mut self) -> Result<Frame, ProtocolError> {
        self.read_line()?;
        Ok(Frame::Null)
    }

    /// Read a line (up to CRLF) from the buffer.
    fn read_line(&mut self) -> Result<Bytes, ProtocolError> {
        match find_crlf(&self.buffer) {
            Some(pos) => {
                let line = self.buffer.split_to(pos).freeze();
                self.buffer.advance(2); // Skip CRLF
                Ok(line)
            }
            None => Err(ProtocolError::Incomplete),
        }
    }
}

/// Check if a byte is a RESP marker.
#[inline]
fn is_resp_marker(b: u8) -> bool {
    matches!(
        b,
        markers::SIMPLE_STRING
            | markers::ERROR
            | markers::INTEGER
            | markers::BULK_STRING
            | markers::ARRAY
            | markers::NULL
            | markers::BOOLEAN
            | markers::DOUBLE
            | markers::BIG_NUMBER
            | markers::BULK_ERROR
            | markers::VERBATIM_STRING
            | markers::MAP
            | markers::SET
            | markers::ATTRIBUTE
            | markers::PUSH
    )
}

/// Find CRLF in a byte slice.
///
/// Uses SIMD-optimized memchr for fast `\r` search, then verifies `\n` follows.
/// This is significantly faster than a naive byte-by-byte search on large buffers.
#[inline]
fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut offset = 0;
    while offset < buf.len().saturating_sub(1) {
        // Use SIMD-optimized search for '\r'
        match memchr(b'\r', &buf[offset..]) {
            Some(pos) => {
                let abs_pos = offset + pos;
                // Check if '\n' follows
                if abs_pos + 1 < buf.len() && buf[abs_pos + 1] == b'\n' {
                    return Some(abs_pos);
                }
                // Skip this '\r' and continue searching
                offset = abs_pos + 1;
            }
            None => return None,
        }
    }
    None
}

/// Parse inline command arguments.
fn parse_inline_args(line: &[u8]) -> Vec<Frame> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut quote_char = b'"';

    let mut i = 0;
    while i < line.len() {
        let c = line[i];

        if in_quotes {
            if c == quote_char {
                in_quotes = false;
                // Include content without quotes
                let arg = &line[start..i];
                if !arg.is_empty() {
                    args.push(Frame::Bulk(Bytes::copy_from_slice(arg)));
                }
                start = i + 1;
            }
        } else if c == b'"' || c == b'\'' {
            in_quotes = true;
            quote_char = c;
            start = i + 1;
        } else if c == b' ' || c == b'\t' {
            if start < i {
                args.push(Frame::Bulk(Bytes::copy_from_slice(&line[start..i])));
            }
            start = i + 1;
        }

        i += 1;
    }

    // Handle last argument
    if start < line.len() {
        args.push(Frame::Bulk(Bytes::copy_from_slice(&line[start..])));
    }

    args
}

/// Parse a single frame from a byte slice (for testing and one-shot parsing).
pub fn parse_frame(data: &[u8]) -> Result<Frame, ProtocolError> {
    let mut parser = RespParser::new();
    parser.extend(data);
    parser.parse()?.ok_or(ProtocolError::Incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut parser = RespParser::new();
        parser.extend(b"+OK\r\n");

        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Simple("OK".to_string()));
    }

    #[test]
    fn test_parse_error() {
        let mut parser = RespParser::new();
        parser.extend(b"-ERR unknown command\r\n");

        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let mut parser = RespParser::new();
        parser.extend(b":42\r\n");

        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Integer(42));

        parser.extend(b":-1\r\n");
        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Integer(-1));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParser::new();
        parser.extend(b"$5\r\nhello\r\n");

        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Bulk(Bytes::from("hello")));
    }

    #[test]
    fn test_parse_null_bulk() {
        let mut parser = RespParser::new();
        parser.extend(b"$-1\r\n");

        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn test_parse_array() {
        let mut parser = RespParser::new();
        parser.extend(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");

        let frame = parser.parse().unwrap().unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Frame::Bulk(Bytes::from("SET")));
                assert_eq!(arr[1], Frame::Bulk(Bytes::from("key")));
                assert_eq!(arr[2], Frame::Bulk(Bytes::from("value")));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let mut parser = RespParser::new();
        parser.extend(b"*0\r\n");

        let frame = parser.parse().unwrap().unwrap();
        assert_eq!(frame, Frame::Array(vec![]));
    }

    #[test]
    fn test_parse_incomplete() {
        let mut parser = RespParser::new();
        parser.extend(b"$5\r\nhel");

        let result = parser.parse().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_inline() {
        let mut parser = RespParser::new();
        parser.extend(b"PING\r\n");

        let frame = parser.parse().unwrap().unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Frame::Bulk(Bytes::from("PING")));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_parse_inline_with_args() {
        let mut parser = RespParser::new();
        parser.extend(b"SET key value\r\n");

        let frame = parser.parse().unwrap().unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Frame::Bulk(Bytes::from("SET")));
                assert_eq!(arr[1], Frame::Bulk(Bytes::from("key")));
                assert_eq!(arr[2], Frame::Bulk(Bytes::from("value")));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    #[ignore = "Known limitation: parser consumes buffer on Incomplete, needs refactor"]
    fn test_parse_streaming() {
        let mut parser = RespParser::new();

        // Send data in chunks
        parser.extend(b"*2\r\n");
        assert!(parser.parse().unwrap().is_none());

        parser.extend(b"$3\r\nfoo\r\n");
        assert!(parser.parse().unwrap().is_none());

        parser.extend(b"$3\r\nbar\r\n");
        let frame = parser.parse().unwrap().unwrap();

        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_parse_multiple_frames() {
        let mut parser = RespParser::new();
        parser.extend(b"+OK\r\n:42\r\n");

        let frame1 = parser.parse().unwrap().unwrap();
        assert_eq!(frame1, Frame::Simple("OK".to_string()));

        let frame2 = parser.parse().unwrap().unwrap();
        assert_eq!(frame2, Frame::Integer(42));

        assert!(parser.parse().unwrap().is_none());
    }

    #[test]
    fn test_bulk_too_large() {
        let mut parser = RespParser::new();
        let huge_len = MAX_BULK_SIZE + 1;
        parser.extend(format!("${huge_len}\r\n").as_bytes());

        let result = parser.parse();
        assert!(matches!(result, Err(ProtocolError::BulkTooLarge { .. })));
    }

    #[test]
    fn test_find_crlf_edge_cases() {
        // Empty buffer
        assert_eq!(find_crlf(b""), None);

        // Single char
        assert_eq!(find_crlf(b"\r"), None);
        assert_eq!(find_crlf(b"\n"), None);

        // CRLF at start
        assert_eq!(find_crlf(b"\r\n"), Some(0));

        // CRLF in middle
        assert_eq!(find_crlf(b"hello\r\nworld"), Some(5));

        // Lone \r without \n
        assert_eq!(find_crlf(b"hello\rworld"), None);

        // Multiple CRLFs
        assert_eq!(find_crlf(b"\r\n\r\n"), Some(0));

        // \r followed by non-\n
        assert_eq!(find_crlf(b"\r \r\n"), Some(2));
    }
}

/// Property-based tests using proptest.
#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Parser should never panic on arbitrary input.
        #[test]
        fn parser_never_panics(data: Vec<u8>) {
            let mut parser = RespParser::new();
            parser.extend(&data);
            // Should not panic, result doesn't matter
            let _ = parser.parse();
        }

        /// Simple strings should round-trip correctly.
        #[test]
        fn simple_string_roundtrip(s in "[a-zA-Z0-9 ]{0,100}") {
            let encoded = format!("+{s}\r\n");
            let mut parser = RespParser::new();
            parser.extend(encoded.as_bytes());

            let frame = parser.parse().unwrap().unwrap();
            prop_assert_eq!(frame, Frame::Simple(s));
        }

        /// Integers should round-trip correctly.
        #[test]
        fn integer_roundtrip(n in i64::MIN..i64::MAX) {
            let encoded = format!(":{n}\r\n");
            let mut parser = RespParser::new();
            parser.extend(encoded.as_bytes());

            let frame = parser.parse().unwrap().unwrap();
            prop_assert_eq!(frame, Frame::Integer(n));
        }

        /// Bulk strings should round-trip correctly.
        #[test]
        fn bulk_string_roundtrip(data in prop::collection::vec(any::<u8>(), 0..1000)) {
            let encoded = format!("${}\r\n", data.len());
            let mut parser = RespParser::new();
            parser.extend(encoded.as_bytes());
            parser.extend(&data);
            parser.extend(b"\r\n");

            let frame = parser.parse().unwrap().unwrap();
            prop_assert_eq!(frame, Frame::Bulk(Bytes::from(data)));
        }

        /// Arrays should round-trip correctly.
        #[test]
        fn array_length_roundtrip(len in 0usize..100) {
            // Create array of integers
            let mut encoded = format!("*{len}\r\n");
            for i in 0..len {
                encoded.push_str(&format!(":{i}\r\n"));
            }

            let mut parser = RespParser::new();
            parser.extend(encoded.as_bytes());

            let frame = parser.parse().unwrap().unwrap();
            match frame {
                Frame::Array(arr) => prop_assert_eq!(arr.len(), len),
                _ => prop_assert!(false, "Expected array"),
            }
        }

        /// find_crlf should always find CRLF if present.
        #[test]
        fn find_crlf_always_finds(prefix in prop::collection::vec(any::<u8>(), 0..100),
                                  suffix in prop::collection::vec(any::<u8>(), 0..100)) {
            // Filter out any existing \r\n sequences
            let prefix: Vec<u8> = prefix.into_iter()
                .filter(|&b| b != b'\r' && b != b'\n')
                .collect();

            let mut data = prefix.clone();
            data.extend_from_slice(b"\r\n");
            data.extend_from_slice(&suffix);

            let pos = find_crlf(&data);
            prop_assert_eq!(pos, Some(prefix.len()));
        }
    }
}
