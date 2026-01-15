//! Count-Min Sketch implementation.
//!
//! A probabilistic data structure for frequency estimation.

/// A Count-Min Sketch for frequency estimation.
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    /// 2D array of counters [depth][width]
    counters: Vec<Vec<u64>>,
    /// Width of the sketch
    width: usize,
    /// Depth (number of hash functions)
    depth: usize,
    /// Total count
    total_count: u64,
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch with specified width and depth.
    #[must_use]
    pub fn new(width: usize, depth: usize) -> Self {
        Self {
            counters: vec![vec![0u64; width]; depth],
            width,
            depth,
            total_count: 0,
        }
    }

    /// Create from error rate and probability parameters.
    /// error_rate: desired error rate (e.g., 0.01 for 1%)
    /// probability: probability of achieving error rate (e.g., 0.99)
    #[must_use]
    pub fn from_error_rate(error_rate: f64, probability: f64) -> Self {
        let width = (std::f64::consts::E / error_rate).ceil() as usize;
        let depth = (1.0 / (1.0 - probability)).ln().ceil() as usize;
        Self::new(width.max(1), depth.max(1))
    }

    /// Hash function for row i.
    fn hash(&self, item: &[u8], seed: usize) -> usize {
        let mut hash: u64 = 0xcbf29ce484222325 ^ (seed as u64);
        for &byte in item {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % self.width
    }

    /// Increment the count for an item.
    pub fn increment(&mut self, item: &[u8], count: u64) {
        for i in 0..self.depth {
            let j = self.hash(item, i);
            self.counters[i][j] = self.counters[i][j].saturating_add(count);
        }
        self.total_count = self.total_count.saturating_add(count);
    }

    /// Increment by 1.
    pub fn add(&mut self, item: &[u8]) {
        self.increment(item, 1);
    }

    /// Query the estimated count for an item.
    #[must_use]
    pub fn query(&self, item: &[u8]) -> u64 {
        let mut min = u64::MAX;
        for i in 0..self.depth {
            let j = self.hash(item, i);
            min = min.min(self.counters[i][j]);
        }
        min
    }

    /// Merge another CMS into this one.
    pub fn merge(&mut self, other: &CountMinSketch) -> Result<(), &'static str> {
        if self.width != other.width || self.depth != other.depth {
            return Err("Dimensions must match for merge");
        }
        for i in 0..self.depth {
            for j in 0..self.width {
                self.counters[i][j] = self.counters[i][j].saturating_add(other.counters[i][j]);
            }
        }
        self.total_count = self.total_count.saturating_add(other.total_count);
        Ok(())
    }

    /// Get the width.
    #[must_use]
    pub fn width(&self) -> usize {
        self.width
    }

    /// Get the depth.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Get the total count.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Get info about the sketch.
    #[must_use]
    pub fn info(&self) -> CmsInfo {
        CmsInfo {
            width: self.width,
            depth: self.depth,
            count: self.total_count,
        }
    }
}

/// Count-Min Sketch information.
#[derive(Debug, Clone)]
pub struct CmsInfo {
    /// Width of the sketch
    pub width: usize,
    /// Depth of the sketch
    pub depth: usize,
    /// Total count
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cms_basic() {
        let mut cms = CountMinSketch::new(1000, 5);

        cms.add(b"hello");
        cms.add(b"hello");
        cms.add(b"world");

        assert!(cms.query(b"hello") >= 2);
        assert!(cms.query(b"world") >= 1);
        assert_eq!(cms.query(b"nonexistent"), 0);
    }

    #[test]
    fn test_cms_increment() {
        let mut cms = CountMinSketch::new(1000, 5);

        cms.increment(b"hello", 100);
        assert!(cms.query(b"hello") >= 100);
    }

    #[test]
    fn test_cms_merge() {
        let mut cms1 = CountMinSketch::new(100, 5);
        let mut cms2 = CountMinSketch::new(100, 5);

        cms1.increment(b"hello", 10);
        cms2.increment(b"hello", 20);

        cms1.merge(&cms2).unwrap();
        assert!(cms1.query(b"hello") >= 30);
    }
}
