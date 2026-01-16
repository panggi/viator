//! Sharded storage for reduced lock contention.
//!
//! This module provides a sharded key-value store that distributes keys
//! across multiple independent shards based on key hash. This reduces
//! lock contention compared to a single shared data structure.

use crate::types::{Key, StoredValue};
use dashmap::DashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Number of shards. Should be a power of 2 for efficient modulo.
/// 64 shards provides good parallelism while keeping overhead low.
const NUM_SHARDS: usize = 64;
const SHARD_MASK: usize = NUM_SHARDS - 1;

/// A single shard containing a portion of the keyspace.
#[derive(Debug)]
pub struct Shard {
    /// Data storage for this shard
    data: DashMap<Key, StoredValue>,
    /// Expiry times for keys in this shard
    expires: DashMap<Key, u64>,
    /// Operation counter for this shard
    ops: AtomicU64,
}

impl Shard {
    /// Create a new empty shard.
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            expires: DashMap::new(),
            ops: AtomicU64::new(0),
        }
    }

    /// Get a value from this shard.
    #[inline]
    pub fn get(&self, key: &Key) -> Option<StoredValue> {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.data.get(key).map(|v| v.clone())
    }

    /// Set a value in this shard.
    #[inline]
    pub fn set(&self, key: Key, value: StoredValue) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.data.insert(key, value);
    }

    /// Delete a key from this shard.
    #[inline]
    pub fn delete(&self, key: &Key) -> bool {
        self.ops.fetch_add(1, Ordering::Relaxed);
        let removed = self.data.remove(key).is_some();
        self.expires.remove(key);
        removed
    }

    /// Check if a key exists in this shard.
    #[inline]
    pub fn exists(&self, key: &Key) -> bool {
        self.data.contains_key(key)
    }

    /// Get the number of keys in this shard.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if this shard is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Set expiry time for a key.
    #[inline]
    pub fn set_expire(&self, key: Key, expire_at: u64) {
        self.expires.insert(key, expire_at);
    }

    /// Get expiry time for a key.
    #[inline]
    pub fn get_expire(&self, key: &Key) -> Option<u64> {
        self.expires.get(key).map(|v| *v)
    }

    /// Remove expiry for a key.
    #[inline]
    pub fn remove_expire(&self, key: &Key) {
        self.expires.remove(key);
    }

    /// Get operation count for this shard.
    #[inline]
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }

    /// Get underlying data map for iteration.
    #[inline]
    pub fn data(&self) -> &DashMap<Key, StoredValue> {
        &self.data
    }

    /// Get underlying expires map for iteration.
    #[inline]
    pub fn expires_map(&self) -> &DashMap<Key, u64> {
        &self.expires
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute shard index for a key using FNV-1a hash.
/// This is faster than the default hasher for short keys.
#[inline]
pub fn shard_index(key: &[u8]) -> usize {
    let mut hasher = fnv::FnvHasher::default();
    key.hash(&mut hasher);
    hasher.finish() as usize & SHARD_MASK
}

/// Sharded key-value store.
///
/// Distributes keys across multiple shards to reduce lock contention.
/// Each shard is an independent DashMap, so operations on different
/// shards can proceed in parallel without contention.
#[derive(Debug)]
pub struct ShardedStore {
    /// The shards
    shards: Box<[Shard; NUM_SHARDS]>,
}

impl ShardedStore {
    /// Create a new sharded store.
    pub fn new() -> Self {
        // Initialize all shards
        // INVARIANT: Vec is created with exactly NUM_SHARDS elements
        let shards: Vec<Shard> = (0..NUM_SHARDS).map(|_| Shard::new()).collect();
        let shards: Box<[Shard; NUM_SHARDS]> = shards
            .try_into()
            .unwrap_or_else(|_| unreachable!("Vec created with exactly NUM_SHARDS elements"));
        Self { shards }
    }

    /// Get the shard for a key.
    #[inline]
    pub fn shard(&self, key: &Key) -> &Shard {
        let idx = shard_index(key.as_bytes());
        &self.shards[idx]
    }

    /// Get a value.
    #[inline]
    pub fn get(&self, key: &Key) -> Option<StoredValue> {
        self.shard(key).get(key)
    }

    /// Set a value.
    #[inline]
    pub fn set(&self, key: Key, value: StoredValue) {
        let shard = self.shard(&key);
        shard.set(key, value);
    }

    /// Delete a key.
    #[inline]
    pub fn delete(&self, key: &Key) -> bool {
        self.shard(key).delete(key)
    }

    /// Check if a key exists.
    #[inline]
    pub fn exists(&self, key: &Key) -> bool {
        self.shard(key).exists(key)
    }

    /// Get total number of keys across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Check if store is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Get total operations across all shards.
    pub fn total_ops(&self) -> u64 {
        self.shards.iter().map(|s| s.ops()).sum()
    }

    /// Iterate over all shards.
    pub fn shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.iter()
    }

    /// Get number of shards.
    #[inline]
    pub const fn num_shards() -> usize {
        NUM_SHARDS
    }
}

impl Default for ShardedStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ViatorValue;
    use bytes::Bytes;

    #[test]
    fn test_shard_index_distribution() {
        // Test that keys are distributed across shards
        let mut shard_counts = vec![0usize; NUM_SHARDS];
        for i in 0..10000 {
            let key = format!("key:{}", i);
            let idx = shard_index(key.as_bytes());
            shard_counts[idx] += 1;
        }

        // Each shard should have roughly 10000/64 = 156 keys
        // Allow for some variance (50-250)
        for count in &shard_counts {
            assert!(*count > 50, "Shard has too few keys: {}", count);
            assert!(*count < 300, "Shard has too many keys: {}", count);
        }
    }

    #[test]
    fn test_sharded_store_basic() {
        let store = ShardedStore::new();

        let key1 = Key::from_static("key1");
        let key2 = Key::from_static("key2");

        let value1 = StoredValue::new(ViatorValue::String(Bytes::from_static(b"value1")));
        let value2 = StoredValue::new(ViatorValue::String(Bytes::from_static(b"value2")));

        store.set(key1.clone(), value1);
        store.set(key2.clone(), value2);

        assert!(store.exists(&key1));
        assert!(store.exists(&key2));
        assert_eq!(store.len(), 2);

        assert!(store.delete(&key1));
        assert!(!store.exists(&key1));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_shard_consistency() {
        let store = ShardedStore::new();
        let key = Key::from_static("testkey");

        // Same key should always go to the same shard
        let shard1: *const Shard = store.shard(&key);
        let shard2: *const Shard = store.shard(&key);
        assert_eq!(shard1, shard2);
    }
}
