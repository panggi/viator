//! Cuckoo filter implementation.
//!
//! A space-efficient probabilistic data structure that supports deletion.

use rand::Rng;

/// Maximum number of kicks before giving up on insertion.
const MAX_KICKS: usize = 500;

/// A Cuckoo filter for probabilistic membership testing with deletion support.
#[derive(Debug, Clone)]
pub struct CuckooFilter {
    /// Buckets containing fingerprints
    buckets: Vec<Bucket>,
    /// Number of buckets
    num_buckets: usize,
    /// Bucket size (number of entries per bucket)
    bucket_size: usize,
    /// Fingerprint size in bits
    fingerprint_size: u8,
    /// Number of items stored
    num_items: usize,
    /// Maximum iterations for insertion
    max_iterations: usize,
    /// Expansion rate for scaling
    expansion_rate: u16,
}

/// A bucket in the cuckoo filter.
#[derive(Debug, Clone)]
struct Bucket {
    /// Fingerprints stored in this bucket (0 = empty)
    fingerprints: Vec<u32>,
}

impl Bucket {
    fn new(size: usize) -> Self {
        Self {
            fingerprints: vec![0; size],
        }
    }

    fn insert(&mut self, fp: u32) -> bool {
        for slot in &mut self.fingerprints {
            if *slot == 0 {
                *slot = fp;
                return true;
            }
        }
        false
    }

    fn contains(&self, fp: u32) -> bool {
        self.fingerprints.contains(&fp)
    }

    fn remove(&mut self, fp: u32) -> bool {
        for slot in &mut self.fingerprints {
            if *slot == fp {
                *slot = 0;
                return true;
            }
        }
        false
    }

    fn is_full(&self) -> bool {
        !self.fingerprints.contains(&0)
    }

    fn swap_random(&mut self, fp: u32) -> u32 {
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..self.fingerprints.len());
        let old = self.fingerprints[idx];
        self.fingerprints[idx] = fp;
        old
    }
}

impl CuckooFilter {
    /// Create a new Cuckoo filter with specified capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self::with_params(capacity, 4, 16)
    }

    /// Create a Cuckoo filter with specific parameters.
    #[must_use]
    pub fn with_params(capacity: usize, bucket_size: usize, fingerprint_bits: u8) -> Self {
        // Calculate number of buckets (power of 2 for fast modulo)
        let num_buckets = ((capacity / bucket_size) + 1).next_power_of_two();
        let buckets = (0..num_buckets).map(|_| Bucket::new(bucket_size)).collect();

        Self {
            buckets,
            num_buckets,
            bucket_size,
            fingerprint_size: fingerprint_bits,
            num_items: 0,
            max_iterations: MAX_KICKS,
            expansion_rate: 1,
        }
    }

    /// Create from Redis BF.RESERVE parameters.
    #[must_use]
    pub fn reserve(capacity: usize, bucket_size: Option<usize>, max_iterations: Option<usize>, expansion: Option<u16>) -> Self {
        let bucket_size = bucket_size.unwrap_or(2);
        let num_buckets = ((capacity / bucket_size) + 1).next_power_of_two();
        let buckets = (0..num_buckets).map(|_| Bucket::new(bucket_size)).collect();

        Self {
            buckets,
            num_buckets,
            bucket_size,
            fingerprint_size: 16,
            num_items: 0,
            max_iterations: max_iterations.unwrap_or(MAX_KICKS),
            expansion_rate: expansion.unwrap_or(1),
        }
    }

    /// Generate fingerprint from item.
    fn fingerprint(&self, item: &[u8]) -> u32 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in item {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        // Ensure fingerprint is non-zero
        let mask = (1u32 << self.fingerprint_size) - 1;
        let fp = (hash as u32) & mask;
        if fp == 0 { 1 } else { fp }
    }

    /// Get primary bucket index.
    fn index1(&self, item: &[u8]) -> usize {
        let mut hash: u64 = 0x84222325cbf29ce4;
        for &byte in item {
            hash = hash.wrapping_mul(0x100000001b3);
            hash ^= u64::from(byte);
        }
        (hash as usize) % self.num_buckets
    }

    /// Get alternate bucket index using fingerprint.
    fn index2(&self, i1: usize, fp: u32) -> usize {
        // XOR with hash of fingerprint
        let fp_hash = (fp as usize).wrapping_mul(0x5bd1e995);
        (i1 ^ fp_hash) % self.num_buckets
    }

    /// Add an item to the filter.
    pub fn add(&mut self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let i1 = self.index1(item);
        let i2 = self.index2(i1, fp);

        // Try to insert in either bucket
        if self.buckets[i1].insert(fp) {
            self.num_items += 1;
            return true;
        }
        if self.buckets[i2].insert(fp) {
            self.num_items += 1;
            return true;
        }

        // Both buckets full, need to relocate
        let mut rng = rand::thread_rng();
        let mut idx = if rng.gen_bool(0.5) { i1 } else { i2 };
        let mut fp = fp;

        for _ in 0..self.max_iterations {
            fp = self.buckets[idx].swap_random(fp);
            idx = self.index2(idx, fp);

            if self.buckets[idx].insert(fp) {
                self.num_items += 1;
                return true;
            }
        }

        // Filter is full
        false
    }

    /// Add item only if it doesn't exist.
    pub fn add_nx(&mut self, item: &[u8]) -> bool {
        if self.contains(item) {
            return false;
        }
        self.add(item)
    }

    /// Check if an item might be in the filter.
    #[must_use]
    pub fn contains(&self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let i1 = self.index1(item);
        let i2 = self.index2(i1, fp);

        self.buckets[i1].contains(fp) || self.buckets[i2].contains(fp)
    }

    /// Remove an item from the filter.
    /// Note: May remove a different item with the same fingerprint (false deletion).
    pub fn remove(&mut self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let i1 = self.index1(item);
        let i2 = self.index2(i1, fp);

        if self.buckets[i1].remove(fp) {
            self.num_items = self.num_items.saturating_sub(1);
            return true;
        }
        if self.buckets[i2].remove(fp) {
            self.num_items = self.num_items.saturating_sub(1);
            return true;
        }
        false
    }

    /// Get the count (approximate) for an item.
    /// Cuckoo filters don't support counting, returns 1 if exists, 0 otherwise.
    #[must_use]
    pub fn count(&self, item: &[u8]) -> usize {
        if self.contains(item) { 1 } else { 0 }
    }

    /// Get the number of items stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_items
    }

    /// Check if filter is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Get filter info.
    #[must_use]
    pub fn info(&self) -> CuckooInfo {
        let size = self.num_buckets * self.bucket_size * 4; // 4 bytes per fingerprint
        let capacity = self.num_buckets * self.bucket_size;
        CuckooInfo {
            size,
            num_buckets: self.num_buckets,
            num_filters: 1,
            num_items: self.num_items,
            num_items_deleted: 0, // Not tracked
            bucket_size: self.bucket_size,
            expansion_rate: self.expansion_rate,
            max_iterations: self.max_iterations,
            capacity,
        }
    }
}

/// Cuckoo filter information.
#[derive(Debug, Clone)]
pub struct CuckooInfo {
    /// Size in bytes
    pub size: usize,
    /// Number of buckets
    pub num_buckets: usize,
    /// Number of sub-filters
    pub num_filters: usize,
    /// Number of items inserted
    pub num_items: usize,
    /// Number of items deleted
    pub num_items_deleted: usize,
    /// Bucket size
    pub bucket_size: usize,
    /// Expansion rate
    pub expansion_rate: u16,
    /// Max iterations for insertion
    pub max_iterations: usize,
    /// Total capacity
    pub capacity: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuckoo_filter_basic() {
        let mut cf = CuckooFilter::new(1000);

        assert!(cf.add(b"hello"));
        assert!(cf.add(b"world"));

        assert!(cf.contains(b"hello"));
        assert!(cf.contains(b"world"));
        assert!(!cf.contains(b"foo"));
    }

    #[test]
    fn test_cuckoo_filter_delete() {
        let mut cf = CuckooFilter::new(1000);

        cf.add(b"hello");
        assert!(cf.contains(b"hello"));

        cf.remove(b"hello");
        assert!(!cf.contains(b"hello"));
    }

    #[test]
    fn test_cuckoo_filter_add_nx() {
        let mut cf = CuckooFilter::new(1000);

        assert!(cf.add_nx(b"hello"));
        assert!(!cf.add_nx(b"hello")); // Already exists
        assert_eq!(cf.len(), 1);
    }
}
