//! T-Digest implementation.
//!
//! A data structure for accurate estimation of quantiles and percentiles.

use std::cmp::Ordering;
use std::mem::size_of;

/// A T-Digest for quantile estimation.
#[derive(Debug, Clone)]
pub struct TDigest {
    /// Compression factor
    compression: f64,
    /// Centroids (mean, weight)
    centroids: Vec<Centroid>,
    /// Total weight
    total_weight: f64,
    /// Minimum value seen
    min: f64,
    /// Maximum value seen
    max: f64,
    /// Buffer for batch additions
    buffer: Vec<f64>,
    /// Buffer size before compression
    buffer_size: usize,
}

/// A centroid in the T-Digest.
#[derive(Debug, Clone, Copy)]
struct Centroid {
    mean: f64,
    weight: f64,
}

impl TDigest {
    /// Create a new T-Digest with specified compression.
    #[must_use]
    pub fn new(compression: f64) -> Self {
        Self {
            compression: compression.max(10.0),
            centroids: Vec::new(),
            total_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            buffer: Vec::new(),
            buffer_size: ((compression * 5.0) as usize).max(100),
        }
    }

    /// Create with default compression (100).
    #[must_use]
    pub fn default_compression() -> Self {
        Self::new(100.0)
    }

    /// Add a single value.
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

    /// Add a value with weight.
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        if value.is_nan() || weight <= 0.0 {
            return;
        }

        self.min = self.min.min(value);
        self.max = self.max.max(value);

        self.buffer.push(value);
        self.total_weight += weight;

        if self.buffer.len() >= self.buffer_size {
            self.compress();
        }
    }

    /// Compress the digest.
    fn compress(&mut self) {
        if self.buffer.is_empty() && self.centroids.is_empty() {
            return;
        }

        // Add buffer to centroids
        for &value in &self.buffer {
            self.centroids.push(Centroid { mean: value, weight: 1.0 });
        }
        self.buffer.clear();

        // Sort by mean
        self.centroids.sort_by(|a, b| {
            a.mean.partial_cmp(&b.mean).unwrap_or(Ordering::Equal)
        });

        // Merge centroids
        if self.centroids.is_empty() {
            return;
        }

        let mut result = Vec::new();
        let mut weight_so_far = 0.0;
        let total = self.total_weight;

        let mut current = self.centroids[0];

        for centroid in self.centroids.iter().skip(1) {
            let proposed_weight = current.weight + centroid.weight;
            let q0 = weight_so_far / total;
            let q2 = (weight_so_far + proposed_weight) / total;

            // Size limit based on compression
            let size_limit = self.size_limit(q0, q2);

            if proposed_weight <= size_limit {
                // Merge
                current.mean = (current.mean * current.weight + centroid.mean * centroid.weight)
                    / proposed_weight;
                current.weight = proposed_weight;
            } else {
                // Start new centroid
                result.push(current);
                weight_so_far += current.weight;
                current = *centroid;
            }
        }
        result.push(current);

        self.centroids = result;
    }

    /// Calculate size limit for merging.
    fn size_limit(&self, q0: f64, q2: f64) -> f64 {
        let scale = (self.compression / 2.0)
            * (q0 * (1.0 - q0)).sqrt().min((q2 * (1.0 - q2)).sqrt());
        scale.max(1.0)
    }

    /// Query a quantile (0.0 to 1.0).
    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() && self.buffer.is_empty() {
            return f64::NAN;
        }

        // Need to ensure we're compressed
        let centroids = if !self.buffer.is_empty() {
            let mut temp = self.clone();
            temp.compress();
            temp.centroids
        } else {
            self.centroids.clone()
        };

        if centroids.is_empty() {
            return f64::NAN;
        }

        if q <= 0.0 {
            return self.min;
        }
        if q >= 1.0 {
            return self.max;
        }

        let target = q * self.total_weight;
        let mut weight_so_far = 0.0;

        for i in 0..centroids.len() {
            let c = &centroids[i];
            let next_weight = weight_so_far + c.weight;

            if next_weight > target {
                // Interpolate within this centroid
                if i == 0 {
                    // First centroid, interpolate from min
                    let delta = (target - weight_so_far) / c.weight;
                    return self.min + delta * (c.mean - self.min);
                }

                let prev = &centroids[i - 1];
                let delta = (target - weight_so_far) / c.weight;
                return prev.mean + delta * (c.mean - prev.mean);
            }
            weight_so_far = next_weight;
        }

        self.max
    }

    /// Get CDF value (rank) for a value.
    #[must_use]
    pub fn cdf(&self, value: f64) -> f64 {
        if self.centroids.is_empty() && self.buffer.is_empty() {
            return 0.0;
        }

        let centroids = if !self.buffer.is_empty() {
            let mut temp = self.clone();
            temp.compress();
            temp.centroids
        } else {
            self.centroids.clone()
        };

        if centroids.is_empty() {
            return 0.0;
        }

        if value < self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }

        let mut weight_so_far = 0.0;

        for i in 0..centroids.len() {
            let c = &centroids[i];
            if c.mean > value {
                if i == 0 {
                    return weight_so_far / self.total_weight;
                }
                let prev = &centroids[i - 1];
                let delta = (value - prev.mean) / (c.mean - prev.mean);
                return (weight_so_far + delta * c.weight) / self.total_weight;
            }
            weight_so_far += c.weight;
        }

        1.0
    }

    /// Merge another T-Digest into this one.
    pub fn merge(&mut self, other: &TDigest) {
        for c in &other.centroids {
            self.centroids.push(*c);
        }
        for &v in &other.buffer {
            self.buffer.push(v);
        }
        self.total_weight += other.total_weight;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);

        if self.buffer.len() + self.centroids.len() > self.buffer_size {
            self.compress();
        }
    }

    /// Reset the digest.
    pub fn reset(&mut self) {
        self.centroids.clear();
        self.buffer.clear();
        self.total_weight = 0.0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
    }

    /// Get the minimum value.
    #[must_use]
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Get the maximum value.
    #[must_use]
    pub fn max(&self) -> f64 {
        self.max
    }

    /// Get the total count.
    #[must_use]
    pub fn count(&self) -> f64 {
        self.total_weight
    }

    /// Get info about the T-Digest.
    #[must_use]
    pub fn info(&self) -> TDigestInfo {
        TDigestInfo {
            compression: self.compression,
            capacity: (self.compression * 2.0) as usize,
            merged_nodes: self.centroids.len(),
            unmerged_nodes: self.buffer.len(),
            merged_weight: self.centroids.iter().map(|c| c.weight).sum(),
            unmerged_weight: self.buffer.len() as f64,
            total_compressions: 0, // Not tracked
            memory_usage: size_of::<Self>()
                + self.centroids.len() * size_of::<Centroid>()
                + self.buffer.len() * size_of::<f64>(),
        }
    }

    /// Get trimmed mean (average excluding outliers).
    #[must_use]
    pub fn trimmed_mean(&self, low_percentile: f64, high_percentile: f64) -> f64 {
        let low = self.quantile(low_percentile);
        let high = self.quantile(high_percentile);

        if low >= high {
            return self.quantile(0.5);
        }

        let centroids = if !self.buffer.is_empty() {
            let mut temp = self.clone();
            temp.compress();
            temp.centroids
        } else {
            self.centroids.clone()
        };

        let mut sum = 0.0;
        let mut weight = 0.0;

        for c in &centroids {
            if c.mean >= low && c.mean <= high {
                sum += c.mean * c.weight;
                weight += c.weight;
            }
        }

        if weight > 0.0 {
            sum / weight
        } else {
            self.quantile(0.5)
        }
    }
}

/// T-Digest information.
#[derive(Debug, Clone)]
pub struct TDigestInfo {
    /// Compression factor
    pub compression: f64,
    /// Maximum capacity
    pub capacity: usize,
    /// Number of merged nodes
    pub merged_nodes: usize,
    /// Number of unmerged nodes
    pub unmerged_nodes: usize,
    /// Weight of merged nodes
    pub merged_weight: f64,
    /// Weight of unmerged nodes
    pub unmerged_weight: f64,
    /// Total compressions performed
    pub total_compressions: usize,
    /// Memory usage in bytes
    pub memory_usage: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tdigest_basic() {
        let mut td = TDigest::new(100.0);

        // Add 1000 values for better statistical accuracy
        for i in 1..=1000 {
            td.add(i as f64);
        }

        // Median should be around 500 (T-Digest is approximate)
        let median = td.quantile(0.5);
        assert!(
            (median - 500.0).abs() < 50.0,
            "median {} should be near 500",
            median
        );

        // 90th percentile should be around 900
        let p90 = td.quantile(0.9);
        assert!(
            (p90 - 900.0).abs() < 50.0,
            "p90 {} should be near 900",
            p90
        );
    }

    #[test]
    fn test_tdigest_cdf() {
        let mut td = TDigest::new(100.0);

        for i in 1..=1000 {
            td.add(i as f64);
        }

        // CDF of 500 should be around 0.5
        let cdf500 = td.cdf(500.0);
        assert!(
            (cdf500 - 0.5).abs() < 0.15,
            "cdf(500) {} should be near 0.5",
            cdf500
        );
    }

    #[test]
    fn test_tdigest_merge() {
        let mut td1 = TDigest::new(100.0);
        let mut td2 = TDigest::new(100.0);

        for i in 1..=500 {
            td1.add(i as f64);
        }
        for i in 501..=1000 {
            td2.add(i as f64);
        }

        td1.merge(&td2);

        let median = td1.quantile(0.5);
        assert!(
            (median - 500.0).abs() < 50.0,
            "merged median {} should be near 500",
            median
        );
    }
}
