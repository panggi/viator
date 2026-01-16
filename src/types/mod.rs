//! Core Viator types.
//!
//! This module defines the fundamental types used throughout Viator,
//! a high-performance key-value store implementation.

mod key;
mod list;
mod set;
pub mod sorted_set;
mod stream;
mod value;

// Probabilistic data structures (compatible with Redis Stack)
pub mod bloom;
pub mod cms;
pub mod cuckoo;
pub mod tdigest;
pub mod timeseries;
pub mod topk;

// Advanced data structures
pub mod graph;
pub mod search;
pub mod vectorset;

pub use key::Key;
pub use list::ViatorList;
pub use set::ViatorSet;
pub use sorted_set::{ScoreBound, SortedSet, SortedSetEntry};
pub use stream::{StreamEntry, StreamId, StreamIdParsed, ViatorStream};
pub use value::{StoredValue, ViatorValue};
pub use vectorset::{DistanceMetric, VectorElement, VectorSet, VectorSetInfo};

// Probabilistic data structure exports
pub use bloom::{BloomFilter, BloomInfo, ScalingBloomFilter};
pub use cms::{CmsInfo, CountMinSketch};
pub use cuckoo::{CuckooFilter, CuckooInfo};
pub use tdigest::{TDigest, TDigestInfo};
pub use timeseries::{Aggregation, DuplicatePolicy, Sample, TimeSeries, TimeSeriesInfo};
pub use topk::{TopK, TopKInfo};

use bytes::Bytes;
use std::collections::HashMap;

/// Type alias for hash (field -> value mapping), compatible with Redis HASH.
pub type ViatorHash = HashMap<Bytes, Bytes>;

/// Type alias for string (binary-safe bytes), compatible with Redis STRING.
pub type ViatorString = Bytes;

/// Database index type.
pub type DbIndex = u16;

/// Maximum database index.
pub const MAX_DB_INDEX: DbIndex = 15;

/// Score type for sorted sets (IEEE 754 double).
pub type Score = f64;

/// Timestamp in milliseconds since Unix epoch.
pub type Timestamp = i64;

/// Get current timestamp in milliseconds.
#[inline]
#[must_use]
pub fn current_timestamp_ms() -> Timestamp {
    chrono::Utc::now().timestamp_millis()
}

/// Expiration time representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expiry {
    /// No expiration
    #[default]
    Never,
    /// Expire at specific timestamp (milliseconds since epoch)
    At(Timestamp),
}

impl Expiry {
    /// Create expiry from TTL in seconds.
    /// A TTL of 0 causes immediate expiration.
    #[must_use]
    pub fn from_seconds(seconds: i64) -> Self {
        if seconds < 0 {
            // Negative values should be validated by the caller
            Self::Never
        } else if seconds == 0 {
            // TTL of 0 means immediate expiration (set expiry to current time)
            Self::At(current_timestamp_ms())
        } else {
            let ms = seconds.saturating_mul(1000);
            Self::At(current_timestamp_ms().saturating_add(ms))
        }
    }

    /// Create expiry from TTL in milliseconds.
    /// A TTL of 0 causes immediate expiration.
    #[must_use]
    pub fn from_millis(millis: i64) -> Self {
        if millis < 0 {
            // Negative values should be validated by the caller
            Self::Never
        } else if millis == 0 {
            // TTL of 0 means immediate expiration (set expiry to current time)
            Self::At(current_timestamp_ms())
        } else {
            Self::At(current_timestamp_ms().saturating_add(millis))
        }
    }

    /// Create expiry from Unix timestamp in seconds.
    #[must_use]
    pub fn at_seconds(timestamp: i64) -> Self {
        Self::At(timestamp.saturating_mul(1000))
    }

    /// Create expiry from Unix timestamp in milliseconds.
    #[must_use]
    pub fn at_millis(timestamp: i64) -> Self {
        Self::At(timestamp)
    }

    /// Check if this expiry has passed.
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        match self {
            Self::Never => false,
            Self::At(ts) => current_timestamp_ms() >= *ts,
        }
    }

    /// Get remaining TTL in milliseconds, or None if no expiry.
    #[must_use]
    pub fn ttl_millis(&self) -> Option<i64> {
        match self {
            Self::Never => None,
            Self::At(ts) => {
                let remaining = ts.saturating_sub(current_timestamp_ms());
                Some(remaining.max(0))
            }
        }
    }

    /// Get remaining TTL in seconds, or None if no expiry.
    #[must_use]
    pub fn ttl_seconds(&self) -> Option<i64> {
        self.ttl_millis().map(|ms| ms / 1000)
    }
}

/// Represents the type of a Redis value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueType {
    /// String type
    String = 0,
    /// List type
    List = 1,
    /// Set type
    Set = 2,
    /// Sorted set type
    ZSet = 3,
    /// Hash type
    Hash = 4,
    /// Stream type
    Stream = 5,
    /// Vector set type (Redis 8.0+)
    VectorSet = 6,
}

impl ValueType {
    /// Returns the type name as used in Redis TYPE command.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
            Self::Hash => "hash",
            Self::Stream => "stream",
            Self::VectorSet => "vectorset",
        }
    }
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expiry_from_seconds() {
        let expiry = Expiry::from_seconds(10);
        assert!(!expiry.is_expired());
        assert!(expiry.ttl_seconds().unwrap() <= 10);
        assert!(expiry.ttl_seconds().unwrap() >= 9);
    }

    #[test]
    fn test_expiry_never() {
        let expiry = Expiry::Never;
        assert!(!expiry.is_expired());
        assert!(expiry.ttl_millis().is_none());
    }

    #[test]
    fn test_expiry_past() {
        let expiry = Expiry::At(0); // Unix epoch
        assert!(expiry.is_expired());
        assert_eq!(expiry.ttl_millis(), Some(0));
    }
}
