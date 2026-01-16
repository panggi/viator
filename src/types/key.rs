//! Redis key type.
//!
//! This module provides a memory-efficient key type that handles both
//! small keys inline and large keys via heap allocation.

use bytes::Bytes;
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Maximum size for inline key storage (small string optimization).
const INLINE_CAPACITY: usize = 23;

/// A Redis key with small string optimization.
///
/// Keys smaller than 24 bytes are stored inline without heap allocation.
/// Larger keys use `Bytes` for efficient reference counting and zero-copy.
///
/// # Security Considerations
///
/// - Keys are treated as binary data (no encoding assumptions)
/// - Maximum key size is enforced at parsing layer
/// - Hash implementation is constant-time for same-length keys
#[derive(Clone)]
pub struct Key {
    inner: KeyInner,
}

#[derive(Clone)]
enum KeyInner {
    /// Small keys stored inline (no heap allocation)
    Inline {
        data: [u8; INLINE_CAPACITY],
        len: u8,
    },
    /// Larger keys stored as Bytes
    Heap(Bytes),
}

impl Key {
    /// Create a new key from bytes.
    #[inline]
    pub fn new(data: impl Into<Bytes>) -> Self {
        let bytes: Bytes = data.into();
        Self::from_bytes(bytes)
    }

    /// Create a key from a Bytes instance.
    #[inline]
    pub fn from_bytes(bytes: Bytes) -> Self {
        if bytes.len() <= INLINE_CAPACITY {
            let mut data = [0u8; INLINE_CAPACITY];
            data[..bytes.len()].copy_from_slice(&bytes);
            Self {
                inner: KeyInner::Inline {
                    data,
                    len: bytes.len() as u8,
                },
            }
        } else {
            Self {
                inner: KeyInner::Heap(bytes),
            }
        }
    }

    /// Create a key from a static string.
    #[inline]
    pub const fn from_static(s: &'static str) -> Self {
        Self::from_static_bytes(s.as_bytes())
    }

    /// Create a key from static bytes.
    #[inline]
    pub const fn from_static_bytes(bytes: &'static [u8]) -> Self {
        if bytes.len() <= INLINE_CAPACITY {
            let mut data = [0u8; INLINE_CAPACITY];
            let mut i = 0;
            while i < bytes.len() {
                data[i] = bytes[i];
                i += 1;
            }
            Self {
                inner: KeyInner::Inline {
                    data,
                    len: bytes.len() as u8,
                },
            }
        } else {
            Self {
                inner: KeyInner::Heap(Bytes::from_static(bytes)),
            }
        }
    }

    /// Returns the key as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.inner {
            KeyInner::Inline { data, len } => &data[..*len as usize],
            KeyInner::Heap(bytes) => bytes,
        }
    }

    /// Returns the length of the key in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            KeyInner::Inline { len, .. } => *len as usize,
            KeyInner::Heap(bytes) => bytes.len(),
        }
    }

    /// Returns true if the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the key is stored inline.
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(self.inner, KeyInner::Inline { .. })
    }

    /// Convert to Bytes, potentially cloning if inline.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        match &self.inner {
            KeyInner::Inline { data, len } => Bytes::copy_from_slice(&data[..*len as usize]),
            KeyInner::Heap(bytes) => bytes.clone(),
        }
    }

    /// Try to interpret the key as a UTF-8 string.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_bytes()).ok()
    }

    /// Check if this key matches a pattern (glob-style).
    ///
    /// Supports:
    /// - `*` matches any sequence of characters
    /// - `?` matches any single character
    /// - `[abc]` matches one of the characters in brackets
    /// - `[^abc]` matches any character not in brackets
    /// - `[a-z]` matches any character in the range
    /// - `\` escapes the next character
    pub fn matches_pattern(&self, pattern: &[u8]) -> bool {
        glob_match(pattern, self.as_bytes())
    }
}

impl Deref for Key {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Key {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for Key {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Hash for Key {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialEq for Key {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(s) = self.as_str() {
            write!(f, "Key({s:?})")
        } else {
            write!(f, "Key({:?})", self.as_bytes())
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(s) = self.as_str() {
            f.write_str(s)
        } else {
            // Display as hex for binary keys
            for byte in self.as_bytes() {
                write!(f, "{byte:02x}")?;
            }
            Ok(())
        }
    }
}

impl From<&str> for Key {
    #[inline]
    fn from(s: &str) -> Self {
        Self::new(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for Key {
    #[inline]
    fn from(s: String) -> Self {
        Self::new(Bytes::from(s))
    }
}

impl From<&[u8]> for Key {
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        Self::new(Bytes::copy_from_slice(bytes))
    }
}

impl From<Vec<u8>> for Key {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        Self::new(Bytes::from(vec))
    }
}

impl From<Bytes> for Key {
    #[inline]
    fn from(bytes: Bytes) -> Self {
        Self::from_bytes(bytes)
    }
}

/// Glob pattern matching implementation.
///
/// This is a non-recursive implementation to prevent stack overflow
/// attacks with deeply nested patterns.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut px = 0; // Pattern index
    let mut tx = 0; // Text index
    let mut next_px = 0;
    let mut next_tx = 0;

    while tx < text.len() || px < pattern.len() {
        if px < pattern.len() {
            match pattern[px] {
                b'*' => {
                    // Try to match star with empty string first
                    next_px = px;
                    next_tx = tx + 1;
                    px += 1;
                    continue;
                }
                b'?' if tx < text.len() => {
                    px += 1;
                    tx += 1;
                    continue;
                }
                b'[' if tx < text.len() => {
                    if let Some((matched, end)) = match_bracket(&pattern[px..], text[tx]) {
                        if matched {
                            px += end;
                            tx += 1;
                            continue;
                        }
                    }
                }
                b'\\' if px + 1 < pattern.len() && tx < text.len() => {
                    if pattern[px + 1] == text[tx] {
                        px += 2;
                        tx += 1;
                        continue;
                    }
                }
                c if tx < text.len() && c == text[tx] => {
                    px += 1;
                    tx += 1;
                    continue;
                }
                _ => {}
            }
        }

        // No match, try backtracking to last star
        if next_tx > 0 && next_tx <= text.len() {
            px = next_px;
            tx = next_tx;
            next_tx += 1;
            continue;
        }

        return false;
    }

    true
}

/// Match a bracket expression like [abc] or [^abc] or [a-z].
/// Returns (matched, end_index) where end_index is position after ']'.
fn match_bracket(pattern: &[u8], ch: u8) -> Option<(bool, usize)> {
    if pattern.is_empty() || pattern[0] != b'[' {
        return None;
    }

    let mut i = 1;
    let negate = if i < pattern.len() && pattern[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;

    while i < pattern.len() && pattern[i] != b']' {
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            // Range like a-z
            let start = pattern[i];
            let end = pattern[i + 2];
            if ch >= start && ch <= end {
                matched = true;
            }
            i += 3;
        } else {
            if pattern[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }

    if i < pattern.len() && pattern[i] == b']' {
        Some((matched != negate, i + 1))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_key() {
        let key = Key::from("hello");
        assert!(key.is_inline());
        assert_eq!(key.len(), 5);
        assert_eq!(key.as_bytes(), b"hello");
    }

    #[test]
    fn test_heap_key() {
        let long_key = "a".repeat(100);
        let key = Key::from(long_key.as_str());
        assert!(!key.is_inline());
        assert_eq!(key.len(), 100);
    }

    #[test]
    fn test_key_equality() {
        let k1 = Key::from("test");
        let k2 = Key::from("test");
        let k3 = Key::from("other");
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_glob_simple() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
    }

    #[test]
    fn test_glob_star() {
        assert!(glob_match(b"h*o", b"hello"));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"h*", b"hello"));
        assert!(glob_match(b"*o", b"hello"));
    }

    #[test]
    fn test_glob_question() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn test_glob_bracket() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn test_glob_bracket_range() {
        assert!(glob_match(b"h[a-z]llo", b"hello"));
        assert!(!glob_match(b"h[a-z]llo", b"h1llo"));
    }

    #[test]
    fn test_glob_bracket_negate() {
        assert!(glob_match(b"h[^0-9]llo", b"hello"));
        assert!(!glob_match(b"h[^0-9]llo", b"h5llo"));
    }

    #[test]
    fn test_key_const() {
        const K: Key = Key::from_static("constant");
        assert_eq!(K.as_bytes(), b"constant");
    }
}
