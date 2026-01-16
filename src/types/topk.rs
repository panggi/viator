//! Top-K Heavy Hitters implementation.
//!
//! A probabilistic data structure for finding the most frequent items.

use bytes::Bytes;
use std::collections::HashMap;

/// A Top-K data structure using the Heavy Keeper algorithm.
#[derive(Debug, Clone)]
pub struct TopK {
    /// Maximum number of items to track
    k: usize,
    /// Width of the heavy keeper array
    width: usize,
    /// Depth (number of hash functions)
    depth: usize,
    /// Decay factor for heavy keeper
    decay: f64,
    /// Heavy keeper buckets [depth][width]
    buckets: Vec<Vec<HeavyBucket>>,
    /// Min-heap for top-k items
    heap: Vec<HeapItem>,
    /// Map from item to heap index
    item_to_heap: HashMap<Bytes, usize>,
}

/// A bucket in the heavy keeper array.
#[derive(Debug, Clone)]
struct HeavyBucket {
    fingerprint: u32,
    count: u64,
}

/// An item in the min-heap.
#[derive(Debug, Clone)]
struct HeapItem {
    item: Bytes,
    count: u64,
}

impl TopK {
    /// Create a new Top-K tracker.
    #[must_use]
    pub fn new(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        let buckets = vec![
            vec![
                HeavyBucket {
                    fingerprint: 0,
                    count: 0
                };
                width
            ];
            depth
        ];
        Self {
            k,
            width,
            depth,
            decay,
            buckets,
            heap: Vec::with_capacity(k),
            item_to_heap: HashMap::with_capacity(k),
        }
    }

    /// Create with default parameters.
    #[must_use]
    pub fn with_k(k: usize) -> Self {
        // Default: width = 8*k, depth = 7, decay = 0.9
        Self::new(k, k * 8, 7, 0.9)
    }

    /// Hash function for fingerprint.
    fn fingerprint(&self, item: &[u8]) -> u32 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in item {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let fp = (hash as u32) & 0xFFFFFFFF;
        if fp == 0 { 1 } else { fp }
    }

    /// Hash function for bucket index.
    fn hash(&self, item: &[u8], seed: usize) -> usize {
        let mut hash: u64 = 0x84222325cbf29ce4 ^ (seed as u64).wrapping_mul(0x9e3779b97f4a7c15);
        for &byte in item {
            hash = hash.wrapping_mul(0x100000001b3);
            hash ^= u64::from(byte);
        }
        (hash as usize) % self.width
    }

    /// Add an item with increment.
    pub fn add(&mut self, item: &[u8], increment: u64) -> Option<Bytes> {
        let fp = self.fingerprint(item);
        let item_bytes = Bytes::copy_from_slice(item);
        let mut max_count: u64 = 0;

        // Update heavy keeper buckets
        for d in 0..self.depth {
            let idx = self.hash(item, d);
            let bucket = &mut self.buckets[d][idx];

            if bucket.fingerprint == fp {
                // Same item, increment count
                bucket.count = bucket.count.saturating_add(increment);
                max_count = max_count.max(bucket.count);
            } else if bucket.count == 0 {
                // Empty bucket, take it
                bucket.fingerprint = fp;
                bucket.count = increment;
                max_count = max_count.max(increment);
            } else {
                // Different item, probabilistically decay
                let decay_prob = self.decay.powi(bucket.count as i32);
                if rand::random::<f64>() < decay_prob {
                    bucket.count = bucket.count.saturating_sub(1);
                    if bucket.count == 0 {
                        bucket.fingerprint = fp;
                        bucket.count = increment;
                        max_count = max_count.max(increment);
                    }
                }
            }
        }

        // Update top-k heap
        let dropped = self.update_heap(item_bytes, max_count);
        dropped
    }

    /// Update the min-heap with item count.
    fn update_heap(&mut self, item: Bytes, count: u64) -> Option<Bytes> {
        if let Some(&idx) = self.item_to_heap.get(&item) {
            // Item already in heap, update count
            self.heap[idx].count = count;
            self.heapify_down(idx);
            self.heapify_up(idx);
            return None;
        }

        if self.heap.len() < self.k {
            // Heap not full, just add
            let idx = self.heap.len();
            self.heap.push(HeapItem {
                item: item.clone(),
                count,
            });
            self.item_to_heap.insert(item, idx);
            self.heapify_up(idx);
            None
        } else if count > self.heap[0].count {
            // New item has higher count than min, replace
            let dropped = self.heap[0].item.clone();
            self.item_to_heap.remove(&dropped);
            self.heap[0] = HeapItem {
                item: item.clone(),
                count,
            };
            self.item_to_heap.insert(item, 0);
            self.heapify_down(0);
            Some(dropped)
        } else {
            None
        }
    }

    /// Heapify up from index.
    fn heapify_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.heap[idx].count < self.heap[parent].count {
                // Update index map
                self.item_to_heap
                    .insert(self.heap[idx].item.clone(), parent);
                self.item_to_heap
                    .insert(self.heap[parent].item.clone(), idx);
                self.heap.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    /// Heapify down from index.
    fn heapify_down(&mut self, mut idx: usize) {
        loop {
            let mut smallest = idx;
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;

            if left < self.heap.len() && self.heap[left].count < self.heap[smallest].count {
                smallest = left;
            }
            if right < self.heap.len() && self.heap[right].count < self.heap[smallest].count {
                smallest = right;
            }

            if smallest != idx {
                // Update index map
                self.item_to_heap
                    .insert(self.heap[idx].item.clone(), smallest);
                self.item_to_heap
                    .insert(self.heap[smallest].item.clone(), idx);
                self.heap.swap(idx, smallest);
                idx = smallest;
            } else {
                break;
            }
        }
    }

    /// Increment an item and return if it was dropped.
    pub fn increment(&mut self, item: &[u8]) -> Option<Bytes> {
        self.add(item, 1)
    }

    /// Query the count of an item.
    #[must_use]
    pub fn query(&self, item: &[u8]) -> u64 {
        let item_bytes = Bytes::copy_from_slice(item);
        if let Some(&idx) = self.item_to_heap.get(&item_bytes) {
            return self.heap[idx].count;
        }

        // Check heavy keeper buckets
        let fp = self.fingerprint(item);
        let mut max_count: u64 = 0;
        for d in 0..self.depth {
            let idx = self.hash(item, d);
            let bucket = &self.buckets[d][idx];
            if bucket.fingerprint == fp {
                max_count = max_count.max(bucket.count);
            }
        }
        max_count
    }

    /// Get the top-k items sorted by count (descending).
    #[must_use]
    pub fn list(&self) -> Vec<(Bytes, u64)> {
        let mut items: Vec<_> = self
            .heap
            .iter()
            .map(|h| (h.item.clone(), h.count))
            .collect();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items
    }

    /// Get the top-k items with full counts.
    #[must_use]
    pub fn list_with_count(&self) -> Vec<(Bytes, u64)> {
        self.list()
    }

    /// Get info about the Top-K tracker.
    #[must_use]
    pub fn info(&self) -> TopKInfo {
        TopKInfo {
            k: self.k,
            width: self.width,
            depth: self.depth,
            decay: self.decay,
        }
    }

    /// Get k value.
    #[must_use]
    pub fn k(&self) -> usize {
        self.k
    }
}

/// Top-K information.
#[derive(Debug, Clone)]
pub struct TopKInfo {
    /// Maximum items to track
    pub k: usize,
    /// Width of the array
    pub width: usize,
    /// Depth of the array
    pub depth: usize,
    /// Decay factor
    pub decay: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topk_basic() {
        let mut topk = TopK::with_k(3);

        // Add items with different frequencies
        for _ in 0..100 {
            topk.increment(b"a");
        }
        for _ in 0..50 {
            topk.increment(b"b");
        }
        for _ in 0..25 {
            topk.increment(b"c");
        }
        for _ in 0..10 {
            topk.increment(b"d");
        }

        let list = topk.list();
        assert_eq!(list.len(), 3);
        // "a" should be at the top
        assert_eq!(list[0].0.as_ref(), b"a");
    }

    #[test]
    fn test_topk_query() {
        let mut topk = TopK::with_k(5);

        for _ in 0..100 {
            topk.increment(b"hello");
        }

        assert!(topk.query(b"hello") >= 90); // Allow some decay
    }
}
