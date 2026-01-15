//! Bloom filter implementation.
//!
//! A space-efficient probabilistic data structure for membership testing.


/// A Bloom filter for probabilistic membership testing.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of bits
    num_bits: usize,
    /// Number of hash functions
    num_hashes: u32,
    /// Number of items added
    num_items: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with specified capacity and error rate.
    #[must_use]
    pub fn new(capacity: usize, error_rate: f64) -> Self {
        let num_bits = Self::optimal_num_bits(capacity, error_rate);
        let num_hashes = Self::optimal_num_hashes(num_bits, capacity);
        let num_words = (num_bits + 63) / 64;

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            num_items: 0,
        }
    }

    /// Create a Bloom filter with specific parameters.
    #[must_use]
    pub fn with_params(num_bits: usize, num_hashes: u32) -> Self {
        let num_words = (num_bits + 63) / 64;
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
            num_items: 0,
        }
    }

    /// Calculate optimal number of bits for given capacity and error rate.
    fn optimal_num_bits(capacity: usize, error_rate: f64) -> usize {
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let n = capacity as f64;
        let m = -(n * error_rate.ln()) / ln2_squared;
        (m.ceil() as usize).max(64)
    }

    /// Calculate optimal number of hash functions.
    fn optimal_num_hashes(num_bits: usize, capacity: usize) -> u32 {
        let k = (num_bits as f64 / capacity as f64) * std::f64::consts::LN_2;
        (k.ceil() as u32).max(1).min(16)
    }

    /// Add an item to the filter.
    pub fn add(&mut self, item: &[u8]) {
        let (h1, h2) = self.hash_pair(item);
        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            self.bits[word_idx] |= 1u64 << bit_idx;
        }
        self.num_items += 1;
    }

    /// Check if an item might be in the filter.
    /// Returns true if the item might exist, false if it definitely doesn't.
    #[must_use]
    pub fn contains(&self, item: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(item);
        for i in 0..self.num_hashes {
            let idx = self.get_index(h1, h2, i);
            let word_idx = idx / 64;
            let bit_idx = idx % 64;
            if (self.bits[word_idx] & (1u64 << bit_idx)) == 0 {
                return false;
            }
        }
        true
    }

    /// Get hash pair using FNV-1a and a variant.
    fn hash_pair(&self, item: &[u8]) -> (u64, u64) {
        // FNV-1a hash
        let mut h1: u64 = 0xcbf29ce484222325;
        for &byte in item {
            h1 ^= u64::from(byte);
            h1 = h1.wrapping_mul(0x100000001b3);
        }

        // Second hash (different seed)
        let mut h2: u64 = 0x84222325cbf29ce4;
        for &byte in item {
            h2 = h2.wrapping_mul(0x100000001b3);
            h2 ^= u64::from(byte);
        }

        (h1, h2)
    }

    /// Get bit index using double hashing.
    fn get_index(&self, h1: u64, h2: u64, i: u32) -> usize {
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash as usize) % self.num_bits
    }

    /// Get the number of items added.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_items
    }

    /// Check if filter is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.num_items == 0
    }

    /// Get the capacity (number of bits).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.num_bits
    }

    /// Get the number of hash functions.
    #[must_use]
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Get filter info for debugging.
    #[must_use]
    pub fn info(&self) -> BloomInfo {
        BloomInfo {
            capacity: self.num_bits,
            size: self.bits.len() * 8,
            num_filters: 1,
            num_items: self.num_items,
            expansion_rate: 0,
        }
    }
}

/// Bloom filter information.
#[derive(Debug, Clone)]
pub struct BloomInfo {
    /// Number of bits
    pub capacity: usize,
    /// Size in bytes
    pub size: usize,
    /// Number of sub-filters (for scaling bloom)
    pub num_filters: usize,
    /// Number of items inserted
    pub num_items: usize,
    /// Expansion rate for scaling bloom
    pub expansion_rate: u32,
}

/// A scaling Bloom filter that grows as items are added.
#[derive(Debug, Clone)]
pub struct ScalingBloomFilter {
    /// Individual bloom filters
    filters: Vec<BloomFilter>,
    /// Initial capacity
    initial_capacity: usize,
    /// Error rate
    error_rate: f64,
    /// Expansion rate (multiply capacity by this for new filters)
    expansion_rate: u32,
    /// Total items added
    total_items: usize,
}

impl ScalingBloomFilter {
    /// Create a new scaling Bloom filter.
    #[must_use]
    pub fn new(capacity: usize, error_rate: f64, expansion_rate: u32) -> Self {
        let mut filters = Vec::new();
        filters.push(BloomFilter::new(capacity, error_rate));
        Self {
            filters,
            initial_capacity: capacity,
            error_rate,
            expansion_rate: expansion_rate.max(2),
            total_items: 0,
        }
    }

    /// Add an item to the filter.
    pub fn add(&mut self, item: &[u8]) -> bool {
        // Check if already exists
        if self.contains(item) {
            return false;
        }

        // Check if we need a new filter
        let last = self.filters.last().unwrap();
        if last.len() >= self.current_capacity() {
            let new_capacity = self.current_capacity() * self.expansion_rate as usize;
            // Reduce error rate for subsequent filters
            let new_error_rate = self.error_rate / (self.filters.len() as f64 + 1.0);
            self.filters.push(BloomFilter::new(new_capacity, new_error_rate));
        }

        // Add to the last filter
        self.filters.last_mut().unwrap().add(item);
        self.total_items += 1;
        true
    }

    /// Check if an item might be in the filter.
    #[must_use]
    pub fn contains(&self, item: &[u8]) -> bool {
        self.filters.iter().any(|f| f.contains(item))
    }

    /// Get current capacity.
    fn current_capacity(&self) -> usize {
        self.initial_capacity * self.expansion_rate.pow(self.filters.len() as u32 - 1) as usize
    }

    /// Get total number of items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.total_items
    }

    /// Check if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_items == 0
    }

    /// Get filter info.
    #[must_use]
    pub fn info(&self) -> BloomInfo {
        let total_capacity: usize = self.filters.iter().map(|f| f.capacity()).sum();
        let total_size: usize = self.filters.iter().map(|f| f.bits.len() * 8).sum();
        BloomInfo {
            capacity: total_capacity,
            size: total_size,
            num_filters: self.filters.len(),
            num_items: self.total_items,
            expansion_rate: self.expansion_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);

        bf.add(b"hello");
        bf.add(b"world");

        assert!(bf.contains(b"hello"));
        assert!(bf.contains(b"world"));
        assert!(!bf.contains(b"foo")); // Might have false positive but unlikely
    }

    #[test]
    fn test_scaling_bloom() {
        let mut sbf = ScalingBloomFilter::new(100, 0.01, 2);

        for i in 0..500 {
            sbf.add(format!("item{i}").as_bytes());
        }

        assert!(sbf.contains(b"item0"));
        assert!(sbf.contains(b"item499"));
        assert!(sbf.info().num_filters > 1);
    }
}
