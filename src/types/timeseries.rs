//! Time Series implementation.
//!
//! A data structure for storing and querying time-series data.

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

/// Aggregation type for time series queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    /// Average of values
    Avg,
    /// Sum of values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Range (max - min)
    Range,
    /// Count of samples
    Count,
    /// First value
    First,
    /// Last value
    Last,
    /// Standard deviation (population)
    StdP,
    /// Standard deviation (sample)
    StdS,
    /// Variance (population)
    VarP,
    /// Variance (sample)
    VarS,
    /// Time-weighted average
    Twa,
}

impl Aggregation {
    /// Parse aggregation type from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "AVG" => Some(Self::Avg),
            "SUM" => Some(Self::Sum),
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            "RANGE" => Some(Self::Range),
            "COUNT" => Some(Self::Count),
            "FIRST" => Some(Self::First),
            "LAST" => Some(Self::Last),
            "STD.P" => Some(Self::StdP),
            "STD.S" => Some(Self::StdS),
            "VAR.P" => Some(Self::VarP),
            "VAR.S" => Some(Self::VarS),
            "TWA" => Some(Self::Twa),
            _ => None,
        }
    }
}

/// Duplicate policy for handling duplicate timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicatePolicy {
    /// Block duplicates (error)
    Block,
    /// Keep first value
    First,
    /// Keep last value
    Last,
    /// Keep minimum value
    Min,
    /// Keep maximum value
    Max,
    /// Sum values
    Sum,
}

impl DuplicatePolicy {
    /// Parse duplicate policy from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BLOCK" => Some(Self::Block),
            "FIRST" => Some(Self::First),
            "LAST" => Some(Self::Last),
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            "SUM" => Some(Self::Sum),
            _ => None,
        }
    }
}

/// A sample in the time series.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Timestamp in milliseconds
    pub timestamp: u64,
    /// Value
    pub value: f64,
}

/// A time series.
#[derive(Debug, Clone)]
pub struct TimeSeries {
    /// Samples stored by timestamp
    samples: BTreeMap<u64, f64>,
    /// Labels (key-value metadata)
    labels: HashMap<Bytes, Bytes>,
    /// Retention period in milliseconds (0 = forever)
    retention: u64,
    /// Chunk size for compression (not used in this implementation)
    chunk_size: usize,
    /// Duplicate policy
    duplicate_policy: DuplicatePolicy,
    /// Last timestamp
    last_timestamp: u64,
    /// Total samples ever added
    total_samples: u64,
    /// Source key for aggregation rules
    source_key: Option<Bytes>,
}

impl TimeSeries {
    /// Create a new time series.
    #[must_use]
    pub fn new() -> Self {
        Self {
            samples: BTreeMap::new(),
            labels: HashMap::new(),
            retention: 0,
            chunk_size: 4096,
            duplicate_policy: DuplicatePolicy::Block,
            last_timestamp: 0,
            total_samples: 0,
            source_key: None,
        }
    }

    /// Create with retention period.
    #[must_use]
    pub fn with_retention(retention_ms: u64) -> Self {
        let mut ts = Self::new();
        ts.retention = retention_ms;
        ts
    }

    /// Set labels.
    pub fn set_labels(&mut self, labels: HashMap<Bytes, Bytes>) {
        self.labels = labels;
    }

    /// Add a label.
    pub fn add_label(&mut self, key: Bytes, value: Bytes) {
        self.labels.insert(key, value);
    }

    /// Get labels.
    #[must_use]
    pub fn labels(&self) -> &HashMap<Bytes, Bytes> {
        &self.labels
    }

    /// Set duplicate policy.
    pub fn set_duplicate_policy(&mut self, policy: DuplicatePolicy) {
        self.duplicate_policy = policy;
    }

    /// Set retention.
    pub fn set_retention(&mut self, retention_ms: u64) {
        self.retention = retention_ms;
    }

    /// Add a sample.
    pub fn add(&mut self, timestamp: u64, value: f64) -> Result<u64, &'static str> {
        // Use current time if timestamp is 0 or *
        let ts = if timestamp == 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        } else {
            timestamp
        };

        // Check for duplicates
        if let Some(&existing) = self.samples.get(&ts) {
            match self.duplicate_policy {
                DuplicatePolicy::Block => return Err("TSDB: duplicate timestamp"),
                DuplicatePolicy::First => return Ok(ts),
                DuplicatePolicy::Last => {
                    self.samples.insert(ts, value);
                }
                DuplicatePolicy::Min => {
                    self.samples.insert(ts, existing.min(value));
                }
                DuplicatePolicy::Max => {
                    self.samples.insert(ts, existing.max(value));
                }
                DuplicatePolicy::Sum => {
                    self.samples.insert(ts, existing + value);
                }
            }
        } else {
            self.samples.insert(ts, value);
        }

        self.last_timestamp = ts;
        self.total_samples += 1;

        // Apply retention
        if self.retention > 0 {
            self.apply_retention(ts);
        }

        Ok(ts)
    }

    /// Apply retention policy.
    fn apply_retention(&mut self, current_ts: u64) {
        let cutoff = current_ts.saturating_sub(self.retention);
        // Remove all samples older than cutoff
        self.samples = self.samples.split_off(&cutoff);
    }

    /// Get the latest sample.
    #[must_use]
    pub fn get(&self) -> Option<Sample> {
        self.samples.iter().next_back().map(|(&ts, &value)| Sample {
            timestamp: ts,
            value,
        })
    }

    /// Get sample at specific timestamp.
    #[must_use]
    pub fn get_at(&self, timestamp: u64) -> Option<Sample> {
        self.samples
            .get(&timestamp)
            .map(|&value| Sample { timestamp, value })
    }

    /// Get samples in a range.
    #[must_use]
    pub fn range(&self, from: u64, to: u64, count: Option<usize>) -> Vec<Sample> {
        let iter = self.samples.range(from..=to);
        let samples: Vec<_> = iter
            .map(|(&ts, &value)| Sample {
                timestamp: ts,
                value,
            })
            .collect();

        if let Some(c) = count {
            samples.into_iter().take(c).collect()
        } else {
            samples
        }
    }

    /// Get samples in a range with reverse order.
    #[must_use]
    pub fn rev_range(&self, from: u64, to: u64, count: Option<usize>) -> Vec<Sample> {
        let iter = self.samples.range(from..=to).rev();
        let samples: Vec<_> = iter
            .map(|(&ts, &value)| Sample {
                timestamp: ts,
                value,
            })
            .collect();

        if let Some(c) = count {
            samples.into_iter().take(c).collect()
        } else {
            samples
        }
    }

    /// Get aggregated samples in a range.
    pub fn range_aggregated(
        &self,
        from: u64,
        to: u64,
        bucket_duration: u64,
        aggregation: Aggregation,
    ) -> Vec<Sample> {
        if bucket_duration == 0 {
            return self.range(from, to, None);
        }

        let mut result = Vec::new();
        let mut bucket_start = from;

        while bucket_start <= to {
            let bucket_end = (bucket_start + bucket_duration - 1).min(to);
            let samples: Vec<_> = self
                .samples
                .range(bucket_start..=bucket_end)
                .map(|(&ts, &value)| Sample {
                    timestamp: ts,
                    value,
                })
                .collect();

            if !samples.is_empty() {
                let value = Self::aggregate(&samples, aggregation);
                result.push(Sample {
                    timestamp: bucket_start,
                    value,
                });
            }

            bucket_start += bucket_duration;
        }

        result
    }

    /// Aggregate samples.
    fn aggregate(samples: &[Sample], aggregation: Aggregation) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        match aggregation {
            Aggregation::Avg => {
                let sum: f64 = samples.iter().map(|s| s.value).sum();
                sum / samples.len() as f64
            }
            Aggregation::Sum => samples.iter().map(|s| s.value).sum(),
            Aggregation::Min => samples
                .iter()
                .map(|s| s.value)
                .fold(f64::INFINITY, f64::min),
            Aggregation::Max => samples
                .iter()
                .map(|s| s.value)
                .fold(f64::NEG_INFINITY, f64::max),
            Aggregation::Range => {
                let min = samples
                    .iter()
                    .map(|s| s.value)
                    .fold(f64::INFINITY, f64::min);
                let max = samples
                    .iter()
                    .map(|s| s.value)
                    .fold(f64::NEG_INFINITY, f64::max);
                max - min
            }
            Aggregation::Count => samples.len() as f64,
            Aggregation::First => samples.first().map(|s| s.value).unwrap_or(0.0),
            Aggregation::Last => samples.last().map(|s| s.value).unwrap_or(0.0),
            Aggregation::StdP | Aggregation::StdS => {
                let n = samples.len() as f64;
                if n < 2.0 {
                    return 0.0;
                }
                let mean: f64 = samples.iter().map(|s| s.value).sum::<f64>() / n;
                let variance: f64 = samples
                    .iter()
                    .map(|s| (s.value - mean).powi(2))
                    .sum::<f64>();
                let divisor = if matches!(aggregation, Aggregation::StdP) {
                    n
                } else {
                    n - 1.0
                };
                (variance / divisor).sqrt()
            }
            Aggregation::VarP | Aggregation::VarS => {
                let n = samples.len() as f64;
                if n < 2.0 {
                    return 0.0;
                }
                let mean: f64 = samples.iter().map(|s| s.value).sum::<f64>() / n;
                let variance: f64 = samples
                    .iter()
                    .map(|s| (s.value - mean).powi(2))
                    .sum::<f64>();
                let divisor = if matches!(aggregation, Aggregation::VarP) {
                    n
                } else {
                    n - 1.0
                };
                variance / divisor
            }
            Aggregation::Twa => {
                // Time-weighted average
                if samples.len() < 2 {
                    return samples.first().map(|s| s.value).unwrap_or(0.0);
                }
                let mut weighted_sum = 0.0;
                let mut total_time = 0.0;
                for i in 1..samples.len() {
                    let dt = (samples[i].timestamp - samples[i - 1].timestamp) as f64;
                    let avg = (samples[i].value + samples[i - 1].value) / 2.0;
                    weighted_sum += avg * dt;
                    total_time += dt;
                }
                if total_time > 0.0 {
                    weighted_sum / total_time
                } else {
                    samples.first().map(|s| s.value).unwrap_or(0.0)
                }
            }
        }
    }

    /// Increment the latest value.
    pub fn incr(&mut self, value: f64) -> Result<u64, &'static str> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let current = self
            .samples
            .iter()
            .next_back()
            .map(|(_, &v)| v)
            .unwrap_or(0.0);
        self.samples.insert(ts, current + value);
        self.last_timestamp = ts;
        self.total_samples += 1;

        Ok(ts)
    }

    /// Decrement the latest value.
    pub fn decr(&mut self, value: f64) -> Result<u64, &'static str> {
        self.incr(-value)
    }

    /// Delete samples in a range.
    pub fn del(&mut self, from: u64, to: u64) -> usize {
        let keys: Vec<u64> = self.samples.range(from..=to).map(|(&k, _)| k).collect();
        let count = keys.len();
        for key in keys {
            self.samples.remove(&key);
        }
        count
    }

    /// Get the number of samples.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Check if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Get info about the time series.
    #[must_use]
    pub fn info(&self) -> TimeSeriesInfo {
        let (first_ts, last_ts) = if self.samples.is_empty() {
            (0, 0)
        } else {
            let first = self.samples.keys().next().copied().unwrap_or(0);
            let last = self.samples.keys().next_back().copied().unwrap_or(0);
            (first, last)
        };

        TimeSeriesInfo {
            total_samples: self.samples.len(),
            memory_usage: size_of::<Self>()
                + self.samples.len() * (8 + 8)  // u64 key + f64 value
                + self.labels.len() * 32,
            first_timestamp: first_ts,
            last_timestamp: last_ts,
            retention: self.retention,
            chunk_count: 1,
            chunk_size: self.chunk_size,
            labels: self.labels.clone(),
            duplicate_policy: self.duplicate_policy,
        }
    }
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

/// Time series information.
#[derive(Debug, Clone)]
pub struct TimeSeriesInfo {
    /// Total samples
    pub total_samples: usize,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// First timestamp
    pub first_timestamp: u64,
    /// Last timestamp
    pub last_timestamp: u64,
    /// Retention in milliseconds
    pub retention: u64,
    /// Number of chunks
    pub chunk_count: usize,
    /// Chunk size
    pub chunk_size: usize,
    /// Labels
    pub labels: HashMap<Bytes, Bytes>,
    /// Duplicate policy
    pub duplicate_policy: DuplicatePolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeseries_basic() {
        let mut ts = TimeSeries::new();

        ts.add(1000, 10.0).unwrap();
        ts.add(2000, 20.0).unwrap();
        ts.add(3000, 30.0).unwrap();

        assert_eq!(ts.len(), 3);

        let latest = ts.get().unwrap();
        assert_eq!(latest.timestamp, 3000);
        assert_eq!(latest.value, 30.0);
    }

    #[test]
    fn test_timeseries_range() {
        let mut ts = TimeSeries::new();

        for i in 1..=10 {
            ts.add(i * 1000, i as f64 * 10.0).unwrap();
        }

        let range = ts.range(2000, 5000, None);
        assert_eq!(range.len(), 4);
    }

    #[test]
    fn test_timeseries_aggregation() {
        let mut ts = TimeSeries::new();

        for i in 1..=100 {
            ts.add(i * 100, i as f64).unwrap();
        }

        // Aggregate into 10 buckets of 1000ms each
        let agg = ts.range_aggregated(100, 10000, 1000, Aggregation::Avg);
        assert_eq!(agg.len(), 10);
    }

    #[test]
    fn test_timeseries_retention() {
        let mut ts = TimeSeries::with_retention(5000);

        ts.add(1000, 10.0).unwrap();
        ts.add(2000, 20.0).unwrap();
        ts.add(10000, 100.0).unwrap(); // This triggers retention

        // Samples older than 5000ms from 10000 should be removed
        assert!(ts.len() <= 2);
    }
}
