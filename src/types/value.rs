//! Redis value type.
//!
//! This module provides the polymorphic value type that represents
//! all Redis data types.

use super::{
    Expiry, SortedSet, ValueType, VectorSet, ViatorHash, ViatorList, ViatorSet, ViatorStream,
};
use bytes::Bytes;
use std::sync::Arc;

/// A Redis value that can hold any of the supported data types.
///
/// # Design Rationale
///
/// - `Arc` is used for collections to enable cheap cloning for replication
/// - `parking_lot::RwLock` provides better performance than std locks
/// - Each variant is sized to minimize enum overhead
///
/// # Memory Layout
///
/// The enum is optimized for the common case (strings) while supporting
/// all Redis data types efficiently.
#[derive(Debug, Clone)]
pub enum ViatorValue {
    /// String value (binary-safe bytes)
    String(Bytes),

    /// List value (doubly-linked list semantics)
    List(Arc<parking_lot::RwLock<ViatorList>>),

    /// Set value (unordered unique elements)
    Set(Arc<parking_lot::RwLock<ViatorSet>>),

    /// Sorted set value (elements with scores)
    ZSet(Arc<parking_lot::RwLock<SortedSet>>),

    /// Hash value (field -> value mapping)
    Hash(Arc<parking_lot::RwLock<ViatorHash>>),

    /// Stream value (append-only log)
    Stream(Arc<parking_lot::RwLock<ViatorStream>>),

    /// Vector set value (Redis 8.0+)
    VectorSet(Arc<parking_lot::RwLock<VectorSet>>),
}

impl ViatorValue {
    /// Create a new string value.
    #[inline]
    pub fn string(data: impl Into<Bytes>) -> Self {
        Self::String(data.into())
    }

    /// Create a new empty list.
    #[inline]
    pub fn new_list() -> Self {
        Self::List(Arc::new(parking_lot::RwLock::new(ViatorList::new())))
    }

    /// Create a new empty set.
    #[inline]
    pub fn new_set() -> Self {
        Self::Set(Arc::new(parking_lot::RwLock::new(ViatorSet::new())))
    }

    /// Create a new empty sorted set.
    #[inline]
    pub fn new_zset() -> Self {
        Self::ZSet(Arc::new(parking_lot::RwLock::new(SortedSet::new())))
    }

    /// Create a new empty hash.
    #[inline]
    pub fn new_hash() -> Self {
        Self::Hash(Arc::new(parking_lot::RwLock::new(ViatorHash::new())))
    }

    /// Create a new empty stream.
    #[inline]
    pub fn new_stream() -> Self {
        Self::Stream(Arc::new(parking_lot::RwLock::new(ViatorStream::new())))
    }

    /// Create a new empty vector set.
    #[inline]
    pub fn new_vectorset() -> Self {
        Self::VectorSet(Arc::new(parking_lot::RwLock::new(VectorSet::new())))
    }

    /// Create a vector set value from an existing VectorSet.
    #[inline]
    pub fn from_vectorset(vs: VectorSet) -> Self {
        Self::VectorSet(Arc::new(parking_lot::RwLock::new(vs)))
    }

    /// Create a stream value from an existing ViatorStream.
    #[inline]
    pub fn from_stream(stream: ViatorStream) -> Self {
        Self::Stream(Arc::new(parking_lot::RwLock::new(stream)))
    }

    /// Returns the type of this value.
    #[inline]
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::String(_) => ValueType::String,
            Self::List(_) => ValueType::List,
            Self::Set(_) => ValueType::Set,
            Self::ZSet(_) => ValueType::ZSet,
            Self::Hash(_) => ValueType::Hash,
            Self::Stream(_) => ValueType::Stream,
            Self::VectorSet(_) => ValueType::VectorSet,
        }
    }

    /// Returns true if this is a string value.
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }

    /// Returns true if this is a list value.
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, Self::List(_))
    }

    /// Returns true if this is a set value.
    #[inline]
    pub fn is_set(&self) -> bool {
        matches!(self, Self::Set(_))
    }

    /// Returns true if this is a sorted set value.
    #[inline]
    pub fn is_zset(&self) -> bool {
        matches!(self, Self::ZSet(_))
    }

    /// Returns true if this is a hash value.
    #[inline]
    pub fn is_hash(&self) -> bool {
        matches!(self, Self::Hash(_))
    }

    /// Returns true if this is a stream value.
    #[inline]
    pub fn is_stream(&self) -> bool {
        matches!(self, Self::Stream(_))
    }

    /// Returns true if this is a vector set value.
    #[inline]
    pub fn is_vectorset(&self) -> bool {
        matches!(self, Self::VectorSet(_))
    }

    /// Try to get a reference to the string value.
    #[inline]
    pub fn as_string(&self) -> Option<&Bytes> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get a reference to the list value.
    #[inline]
    pub fn as_list(&self) -> Option<&Arc<parking_lot::RwLock<ViatorList>>> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get a reference to the set value.
    #[inline]
    pub fn as_set(&self) -> Option<&Arc<parking_lot::RwLock<ViatorSet>>> {
        match self {
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get a reference to the sorted set value.
    #[inline]
    pub fn as_zset(&self) -> Option<&Arc<parking_lot::RwLock<SortedSet>>> {
        match self {
            Self::ZSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get a reference to the hash value.
    #[inline]
    pub fn as_hash(&self) -> Option<&Arc<parking_lot::RwLock<ViatorHash>>> {
        match self {
            Self::Hash(h) => Some(h),
            _ => None,
        }
    }

    /// Try to get a reference to the stream value.
    #[inline]
    pub fn as_stream(&self) -> Option<&Arc<parking_lot::RwLock<ViatorStream>>> {
        match self {
            Self::Stream(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get a reference to the vector set value.
    #[inline]
    pub fn as_vectorset(&self) -> Option<&Arc<parking_lot::RwLock<VectorSet>>> {
        match self {
            Self::VectorSet(vs) => Some(vs),
            _ => None,
        }
    }

    /// Returns the number of elements in this value.
    ///
    /// For strings, returns the length in bytes.
    /// For collections, returns the number of elements.
    pub fn len(&self) -> usize {
        match self {
            Self::String(s) => s.len(),
            Self::List(l) => l.read().len(),
            Self::Set(s) => s.read().len(),
            Self::ZSet(z) => z.read().len(),
            Self::Hash(h) => h.read().len(),
            Self::Stream(st) => st.read().len(),
            Self::VectorSet(vs) => vs.read().len(),
        }
    }

    /// Returns true if this value is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Try to parse the string value as an integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::String(s) => std::str::from_utf8(s).ok()?.parse().ok(),
            _ => None,
        }
    }

    /// Try to parse the string value as a float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::String(s) => std::str::from_utf8(s).ok()?.parse().ok(),
            _ => None,
        }
    }

    /// Estimate the memory usage of this value in bytes.
    pub fn memory_usage(&self) -> usize {
        let base = size_of::<Self>();
        match self {
            Self::String(s) => base + s.len(),
            Self::List(l) => base + l.read().memory_usage(),
            Self::Set(s) => {
                let set = s.read();
                base + set.iter().map(|e| e.len()).sum::<usize>()
            }
            Self::ZSet(z) => base + z.read().memory_usage(),
            Self::Hash(h) => {
                let hash = h.read();
                base + hash.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
            }
            Self::Stream(st) => base + st.read().memory_usage(),
            Self::VectorSet(vs) => base + vs.read().memory_usage(),
        }
    }
}

impl Default for ViatorValue {
    fn default() -> Self {
        Self::String(Bytes::new())
    }
}

impl From<&str> for ViatorValue {
    fn from(s: &str) -> Self {
        Self::String(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for ViatorValue {
    fn from(s: String) -> Self {
        Self::String(Bytes::from(s))
    }
}

impl From<Bytes> for ViatorValue {
    fn from(b: Bytes) -> Self {
        Self::String(b)
    }
}

impl From<i64> for ViatorValue {
    fn from(n: i64) -> Self {
        Self::String(Bytes::from(n.to_string()))
    }
}

impl From<f64> for ViatorValue {
    fn from(n: f64) -> Self {
        Self::String(Bytes::from(n.to_string()))
    }
}

impl From<Vec<u8>> for ViatorValue {
    fn from(v: Vec<u8>) -> Self {
        Self::String(Bytes::from(v))
    }
}

/// A stored value with optional expiration.
#[derive(Debug, Clone)]
pub struct StoredValue {
    /// The actual value
    pub value: ViatorValue,
    /// Optional expiration time
    pub expiry: Expiry,
}

impl StoredValue {
    /// Create a new stored value with no expiration.
    #[inline]
    pub fn new(value: ViatorValue) -> Self {
        Self {
            value,
            expiry: Expiry::Never,
        }
    }

    /// Create a new stored value with expiration.
    #[inline]
    pub fn with_expiry(value: ViatorValue, expiry: Expiry) -> Self {
        Self { value, expiry }
    }

    /// Check if this value has expired.
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expiry.is_expired()
    }

    /// Set the expiration time.
    #[inline]
    pub fn set_expiry(&mut self, expiry: Expiry) {
        self.expiry = expiry;
    }

    /// Clear the expiration time.
    #[inline]
    pub fn persist(&mut self) {
        self.expiry = Expiry::Never;
    }

    /// Get the TTL in seconds.
    #[inline]
    pub fn ttl(&self) -> Option<i64> {
        self.expiry.ttl_seconds()
    }

    /// Get the TTL in milliseconds.
    #[inline]
    pub fn pttl(&self) -> Option<i64> {
        self.expiry.ttl_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_value() {
        let v = ViatorValue::string("hello");
        assert!(v.is_string());
        assert_eq!(v.as_string(), Some(&Bytes::from("hello")));
        assert_eq!(v.len(), 5);
    }

    #[test]
    fn test_list_value() {
        let v = ViatorValue::new_list();
        assert!(v.is_list());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn test_value_type() {
        assert_eq!(ViatorValue::string("x").value_type(), ValueType::String);
        assert_eq!(ViatorValue::new_list().value_type(), ValueType::List);
        assert_eq!(ViatorValue::new_set().value_type(), ValueType::Set);
        assert_eq!(ViatorValue::new_zset().value_type(), ValueType::ZSet);
        assert_eq!(ViatorValue::new_hash().value_type(), ValueType::Hash);
    }

    #[test]
    fn test_integer_parsing() {
        let v = ViatorValue::string("42");
        assert_eq!(v.as_i64(), Some(42));

        let v = ViatorValue::string("-100");
        assert_eq!(v.as_i64(), Some(-100));

        let v = ViatorValue::string("not a number");
        assert_eq!(v.as_i64(), None);
    }

    #[test]
    fn test_float_parsing() {
        let v = ViatorValue::string("3.14");
        assert!((v.as_f64().unwrap() - 3.14).abs() < f64::EPSILON);

        let v = ViatorValue::string("-2.5e10");
        assert_eq!(v.as_f64(), Some(-2.5e10));
    }

    #[test]
    fn test_stored_value_expiry() {
        let mut sv = StoredValue::new(ViatorValue::string("test"));
        assert!(!sv.is_expired());
        assert!(sv.ttl().is_none());

        sv.set_expiry(Expiry::from_seconds(10));
        assert!(!sv.is_expired());
        assert!(sv.ttl().is_some());

        sv.persist();
        assert!(sv.ttl().is_none());
    }
}
