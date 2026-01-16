//! Compact value encodings for memory efficiency.
//!
//! This module provides Redis-compatible compact encodings:
//! - `IntSet`: Memory-efficient set of integers
//! - `Listpack`: Compact encoding for small lists/hashes
//! - Embedded strings for small strings

use bytes::{Bytes, BytesMut};
use std::cmp::Ordering;
use std::mem::size_of;

/// Maximum size for embedded string (stored inline).
pub const EMBSTR_MAX_SIZE: usize = 44;

/// Maximum elements before promoting IntSet to HashSet.
pub const INTSET_MAX_ENTRIES: usize = 512;

/// Maximum bytes before promoting Listpack to standard encoding.
pub const LISTPACK_MAX_BYTES: usize = 8192;

/// Maximum entries in a listpack before promotion.
pub const LISTPACK_MAX_ENTRIES: usize = 512;

/// Integer encoding type for IntSet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IntEncoding {
    /// 16-bit integers
    Int16 = 2,
    /// 32-bit integers
    Int32 = 4,
    /// 64-bit integers
    Int64 = 8,
}

impl IntEncoding {
    /// Get the byte width of this encoding.
    #[inline]
    pub const fn width(self) -> usize {
        self as usize
    }

    /// Get the minimum encoding that can hold a value.
    #[inline]
    pub fn for_value(value: i64) -> Self {
        if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
            Self::Int16
        } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            Self::Int32
        } else {
            Self::Int64
        }
    }
}

/// A memory-efficient set of integers.
///
/// IntSet stores integers in a sorted array with a uniform encoding.
/// This is much more memory-efficient than a HashSet for small sets
/// of integers.
#[derive(Debug, Clone)]
pub struct IntSet {
    /// The encoding used for all integers
    encoding: IntEncoding,
    /// Raw byte storage
    data: Vec<u8>,
}

impl IntSet {
    /// Create a new empty IntSet.
    pub fn new() -> Self {
        Self {
            encoding: IntEncoding::Int16,
            data: Vec::new(),
        }
    }

    /// Get the number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() / self.encoding.width()
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Check if the set should be promoted to a HashSet.
    #[inline]
    pub fn should_promote(&self) -> bool {
        self.len() >= INTSET_MAX_ENTRIES
    }

    /// Get a value at index.
    fn get_at(&self, index: usize) -> i64 {
        let offset = index * self.encoding.width();
        match self.encoding {
            IntEncoding::Int16 => {
                let bytes: [u8; 2] = self.data[offset..offset + 2].try_into().unwrap();
                i16::from_le_bytes(bytes) as i64
            }
            IntEncoding::Int32 => {
                let bytes: [u8; 4] = self.data[offset..offset + 4].try_into().unwrap();
                i32::from_le_bytes(bytes) as i64
            }
            IntEncoding::Int64 => {
                let bytes: [u8; 8] = self.data[offset..offset + 8].try_into().unwrap();
                i64::from_le_bytes(bytes)
            }
        }
    }

    /// Set a value at index.
    fn set_at(&mut self, index: usize, value: i64) {
        let offset = index * self.encoding.width();
        match self.encoding {
            IntEncoding::Int16 => {
                let bytes = (value as i16).to_le_bytes();
                self.data[offset..offset + 2].copy_from_slice(&bytes);
            }
            IntEncoding::Int32 => {
                let bytes = (value as i32).to_le_bytes();
                self.data[offset..offset + 4].copy_from_slice(&bytes);
            }
            IntEncoding::Int64 => {
                let bytes = value.to_le_bytes();
                self.data[offset..offset + 8].copy_from_slice(&bytes);
            }
        }
    }

    /// Binary search for a value.
    fn search(&self, value: i64) -> Result<usize, usize> {
        let len = self.len();
        if len == 0 {
            return Err(0);
        }

        let mut low = 0;
        let mut high = len;

        while low < high {
            let mid = low + (high - low) / 2;
            match self.get_at(mid).cmp(&value) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Ok(mid),
            }
        }

        Err(low)
    }

    /// Check if a value exists.
    pub fn contains(&self, value: i64) -> bool {
        self.search(value).is_ok()
    }

    /// Upgrade the encoding if needed.
    fn upgrade_encoding(&mut self, required: IntEncoding) {
        if required.width() <= self.encoding.width() {
            return;
        }

        let old_len = self.len();
        let old_encoding = self.encoding;
        let old_data = std::mem::take(&mut self.data);

        self.encoding = required;
        self.data = vec![0u8; old_len * required.width()];

        // Copy values with new encoding
        for i in 0..old_len {
            let value = match old_encoding {
                IntEncoding::Int16 => {
                    let offset = i * 2;
                    let bytes: [u8; 2] = old_data[offset..offset + 2].try_into().unwrap();
                    i16::from_le_bytes(bytes) as i64
                }
                IntEncoding::Int32 => {
                    let offset = i * 4;
                    let bytes: [u8; 4] = old_data[offset..offset + 4].try_into().unwrap();
                    i32::from_le_bytes(bytes) as i64
                }
                IntEncoding::Int64 => {
                    let offset = i * 8;
                    let bytes: [u8; 8] = old_data[offset..offset + 8].try_into().unwrap();
                    i64::from_le_bytes(bytes)
                }
            };
            self.set_at(i, value);
        }
    }

    /// Add a value to the set.
    pub fn add(&mut self, value: i64) -> bool {
        // Check if we need to upgrade encoding
        let required = IntEncoding::for_value(value);
        if required.width() > self.encoding.width() {
            self.upgrade_encoding(required);
        }

        // Find insertion point
        match self.search(value) {
            Ok(_) => false, // Already exists
            Err(pos) => {
                // Insert at position
                let width = self.encoding.width();
                let offset = pos * width;
                let old_len = self.data.len();

                // Make room
                self.data.resize(old_len + width, 0);

                // Shift elements right
                if offset < old_len {
                    self.data.copy_within(offset..old_len, offset + width);
                }

                // Write value
                self.set_at(pos, value);
                true
            }
        }
    }

    /// Remove a value from the set.
    pub fn remove(&mut self, value: i64) -> bool {
        match self.search(value) {
            Ok(pos) => {
                let width = self.encoding.width();
                let offset = pos * width;
                let end = self.data.len();

                // Shift elements left
                if offset + width < end {
                    self.data.copy_within(offset + width..end, offset);
                }

                // Shrink
                self.data.truncate(end - width);
                true
            }
            Err(_) => false,
        }
    }

    /// Get an iterator over values.
    pub fn iter(&self) -> IntSetIter<'_> {
        IntSetIter {
            intset: self,
            index: 0,
        }
    }

    /// Estimate memory usage.
    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.data.capacity()
    }
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over IntSet values.
pub struct IntSetIter<'a> {
    intset: &'a IntSet,
    index: usize,
}

impl Iterator for IntSetIter<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.intset.len() {
            None
        } else {
            let value = self.intset.get_at(self.index);
            self.index += 1;
            Some(value)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.intset.len() - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for IntSetIter<'_> {}

/// Entry type marker for listpack entries.
const LP_INT: u8 = 0x00;
const LP_STR: u8 = 0x80;

/// A compact list-like structure stored in contiguous memory.
///
/// Listpack is a memory-efficient encoding for small lists, storing
/// all elements in a single contiguous buffer. Much more cache-friendly
/// than linked lists for small sizes.
///
/// Format: Each entry is [type_byte][length?][data]
/// - Integers: type_byte=0x00, followed by varint encoding
/// - Strings: type_byte=0x80, followed by length varint, then data
#[derive(Debug, Clone)]
pub struct Listpack {
    /// Raw byte storage
    data: BytesMut,
    /// Number of elements
    len: usize,
}

impl Listpack {
    /// Create a new empty listpack.
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
            len: 0,
        }
    }

    /// Get the number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the total byte size.
    #[inline]
    pub fn bytes(&self) -> usize {
        self.data.len()
    }

    /// Check if the listpack should be promoted to standard encoding.
    #[inline]
    pub fn should_promote(&self) -> bool {
        self.len >= LISTPACK_MAX_ENTRIES || self.data.len() >= LISTPACK_MAX_BYTES
    }

    /// Write a varint to the data buffer.
    fn write_varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.data.extend_from_slice(&[(value as u8) | 0x80]);
            value >>= 7;
        }
        self.data.extend_from_slice(&[value as u8]);
    }

    /// Read a varint from the data buffer at the given offset.
    /// Returns (value, bytes_consumed).
    fn read_varint(data: &[u8], mut offset: usize) -> (u64, usize) {
        let start = offset;
        let mut value: u64 = 0;
        let mut shift = 0;

        loop {
            let byte = data[offset];
            value |= ((byte & 0x7F) as u64) << shift;
            offset += 1;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        (value, offset - start)
    }

    /// Push a bytes value to the end.
    pub fn push_bytes(&mut self, value: &[u8]) {
        // Try to parse as integer first
        if let Ok(s) = std::str::from_utf8(value) {
            if let Ok(n) = s.parse::<i64>() {
                // Encode as integer
                self.data.extend_from_slice(&[LP_INT]);
                // Use zigzag encoding for signed integers
                let zigzag = ((n << 1) ^ (n >> 63)) as u64;
                self.write_varint(zigzag);
                self.len += 1;
                return;
            }
        }

        // Encode as string
        self.data.extend_from_slice(&[LP_STR]);
        self.write_varint(value.len() as u64);
        self.data.extend_from_slice(value);
        self.len += 1;
    }

    /// Push a Bytes value to the end.
    pub fn push(&mut self, value: Bytes) {
        self.push_bytes(&value);
    }

    /// Get all entries as a vector of Bytes.
    pub fn to_vec(&self) -> Vec<Bytes> {
        let mut result = Vec::with_capacity(self.len);
        let mut offset = 0;

        while offset < self.data.len() {
            let entry_type = self.data[offset];
            offset += 1;

            if entry_type == LP_INT {
                // Integer entry
                let (zigzag, consumed) = Self::read_varint(&self.data, offset);
                offset += consumed;
                // Decode zigzag
                let value = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
                result.push(Bytes::from(value.to_string()));
            } else {
                // String entry
                let (str_len, consumed) = Self::read_varint(&self.data, offset);
                offset += consumed;
                let str_len = str_len as usize;
                result.push(Bytes::copy_from_slice(&self.data[offset..offset + str_len]));
                offset += str_len;
            }
        }

        result
    }

    /// Estimate memory usage.
    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.data.capacity()
    }
}

impl Default for Listpack {
    fn default() -> Self {
        Self::new()
    }
}

/// Try to encode a string as an integer.
/// Returns Some(i64) if the string represents a valid integer.
#[inline]
pub fn try_encode_int(value: &[u8]) -> Option<i64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// Check if a bytes value can be stored as an embedded string.
#[inline]
pub fn can_embed_string(value: &[u8]) -> bool {
    value.len() <= EMBSTR_MAX_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intset_basic() {
        let mut set = IntSet::new();

        assert!(set.add(5));
        assert!(set.add(10));
        assert!(set.add(1));
        assert!(!set.add(5)); // Duplicate

        assert_eq!(set.len(), 3);
        assert!(set.contains(5));
        assert!(set.contains(10));
        assert!(set.contains(1));
        assert!(!set.contains(7));

        // Check sorted order
        let values: Vec<i64> = set.iter().collect();
        assert_eq!(values, vec![1, 5, 10]);
    }

    #[test]
    fn test_intset_upgrade() {
        let mut set = IntSet::new();

        // Start with small values
        set.add(100);
        assert_eq!(set.encoding, IntEncoding::Int16);

        // Add a value that requires upgrade
        set.add(100_000);
        assert_eq!(set.encoding, IntEncoding::Int32);

        // Verify both values are still there
        assert!(set.contains(100));
        assert!(set.contains(100_000));
    }

    #[test]
    fn test_intset_remove() {
        let mut set = IntSet::new();
        set.add(1);
        set.add(2);
        set.add(3);

        assert!(set.remove(2));
        assert!(!set.remove(2)); // Already removed

        assert_eq!(set.len(), 2);
        assert!(set.contains(1));
        assert!(!set.contains(2));
        assert!(set.contains(3));
    }

    #[test]
    fn test_listpack_basic() {
        let mut lp = Listpack::new();

        lp.push_bytes(b"hello");
        lp.push_bytes(b"42");
        lp.push_bytes(b"world");

        assert_eq!(lp.len(), 3);

        let values = lp.to_vec();
        assert_eq!(values.len(), 3);
        assert_eq!(&values[0][..], b"hello");
        assert_eq!(&values[1][..], b"42");
        assert_eq!(&values[2][..], b"world");
    }

    #[test]
    fn test_listpack_integers() {
        let mut lp = Listpack::new();

        lp.push_bytes(b"127"); // Small int
        lp.push_bytes(b"1000"); // 13-bit int
        lp.push_bytes(b"50000"); // 16-bit int

        let values = lp.to_vec();
        assert_eq!(&values[0][..], b"127");
        assert_eq!(&values[1][..], b"1000");
        assert_eq!(&values[2][..], b"50000");
    }

    #[test]
    fn test_can_embed_string() {
        assert!(can_embed_string(b"short string"));
        assert!(can_embed_string(&[0u8; 44]));
        assert!(!can_embed_string(&[0u8; 45]));
    }

    #[test]
    fn test_try_encode_int() {
        assert_eq!(try_encode_int(b"123"), Some(123));
        assert_eq!(try_encode_int(b"-456"), Some(-456));
        assert_eq!(try_encode_int(b"hello"), None);
        assert_eq!(try_encode_int(b"12.34"), None);
    }
}
