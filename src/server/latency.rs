//! Latency monitoring infrastructure.
//!
//! Provides Redis-compatible latency monitoring with:
//! - Per-event-type latency tracking
//! - Latency history with timestamps
//! - Spike detection
//! - Automated diagnosis (LATENCY DOCTOR)

use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Maximum number of samples to keep per event type.
const LATENCY_HISTORY_SIZE: usize = 160;

/// Default latency threshold in milliseconds.
/// Events faster than this are not recorded.
const DEFAULT_LATENCY_THRESHOLD_MS: u64 = 0;

/// Latency event types (similar to Redis's latency events).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LatencyEvent {
    /// Overall command execution
    Command,
    /// Fast command execution (simpler commands)
    FastCommand,
    /// Network read operations
    NetworkRead,
    /// Network write operations
    NetworkWrite,
    /// AOF write/sync operations
    AofWrite,
    /// VDB/RDB save operations
    VdbSave,
    /// VDB/RDB load operations
    VdbLoad,
    /// Expiry processing
    ExpireCycle,
    /// Eviction processing
    EvictionCycle,
    /// Lua script execution
    LuaScript,
    /// Pub/Sub message delivery
    PubsubDelivery,
    /// Fork operations (for background save)
    Fork,
    /// Active defragmentation
    ActiveDefrag,
    /// Module operations
    Module,
}

impl LatencyEvent {
    /// Get the string name for this event type.
    pub fn as_str(&self) -> &'static str {
        match self {
            LatencyEvent::Command => "command",
            LatencyEvent::FastCommand => "fast-command",
            LatencyEvent::NetworkRead => "network-read",
            LatencyEvent::NetworkWrite => "network-write",
            LatencyEvent::AofWrite => "aof-write",
            LatencyEvent::VdbSave => "vdb-save",
            LatencyEvent::VdbLoad => "vdb-load",
            LatencyEvent::ExpireCycle => "expire-cycle",
            LatencyEvent::EvictionCycle => "eviction-cycle",
            LatencyEvent::LuaScript => "lua-script",
            LatencyEvent::PubsubDelivery => "pubsub-delivery",
            LatencyEvent::Fork => "fork",
            LatencyEvent::ActiveDefrag => "active-defrag",
            LatencyEvent::Module => "module",
        }
    }

    /// Get all event types.
    pub fn all() -> &'static [LatencyEvent] {
        &[
            LatencyEvent::Command,
            LatencyEvent::FastCommand,
            LatencyEvent::NetworkRead,
            LatencyEvent::NetworkWrite,
            LatencyEvent::AofWrite,
            LatencyEvent::VdbSave,
            LatencyEvent::VdbLoad,
            LatencyEvent::ExpireCycle,
            LatencyEvent::EvictionCycle,
            LatencyEvent::LuaScript,
            LatencyEvent::PubsubDelivery,
            LatencyEvent::Fork,
            LatencyEvent::ActiveDefrag,
            LatencyEvent::Module,
        ]
    }

    /// Parse event type from string.
    pub fn from_str(s: &str) -> Option<LatencyEvent> {
        match s.to_lowercase().as_str() {
            "command" => Some(LatencyEvent::Command),
            "fast-command" => Some(LatencyEvent::FastCommand),
            "network-read" => Some(LatencyEvent::NetworkRead),
            "network-write" => Some(LatencyEvent::NetworkWrite),
            "aof-write" => Some(LatencyEvent::AofWrite),
            "vdb-save" | "rdb-save" => Some(LatencyEvent::VdbSave),
            "vdb-load" | "rdb-load" => Some(LatencyEvent::VdbLoad),
            "expire-cycle" => Some(LatencyEvent::ExpireCycle),
            "eviction-cycle" => Some(LatencyEvent::EvictionCycle),
            "lua-script" => Some(LatencyEvent::LuaScript),
            "pubsub-delivery" => Some(LatencyEvent::PubsubDelivery),
            "fork" => Some(LatencyEvent::Fork),
            "active-defrag" => Some(LatencyEvent::ActiveDefrag),
            "module" => Some(LatencyEvent::Module),
            _ => None,
        }
    }
}

/// A single latency sample with timestamp.
#[derive(Debug, Clone, Copy)]
pub struct LatencySample {
    /// Unix timestamp when the sample was recorded.
    pub timestamp: u64,
    /// Latency in milliseconds.
    pub latency_ms: u64,
}

/// History of latency samples for a single event type.
#[derive(Debug)]
struct EventHistory {
    /// Ring buffer of samples.
    samples: Vec<LatencySample>,
    /// Current write position in the ring buffer.
    position: usize,
    /// Number of samples recorded.
    count: usize,
    /// Maximum latency ever recorded.
    max_latency_ms: u64,
    /// Total number of events (for stats).
    total_events: u64,
}

impl EventHistory {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(LATENCY_HISTORY_SIZE),
            position: 0,
            count: 0,
            max_latency_ms: 0,
            total_events: 0,
        }
    }

    fn record(&mut self, latency_ms: u64) {
        self.total_events += 1;

        if latency_ms > self.max_latency_ms {
            self.max_latency_ms = latency_ms;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let sample = LatencySample {
            timestamp,
            latency_ms,
        };

        if self.samples.len() < LATENCY_HISTORY_SIZE {
            self.samples.push(sample);
        } else {
            self.samples[self.position] = sample;
        }

        self.position = (self.position + 1) % LATENCY_HISTORY_SIZE;
        self.count = self.count.saturating_add(1).min(LATENCY_HISTORY_SIZE);
    }

    fn get_history(&self) -> Vec<LatencySample> {
        if self.samples.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.count);

        // Return samples in chronological order
        if self.count < LATENCY_HISTORY_SIZE || self.samples.len() < LATENCY_HISTORY_SIZE {
            // Buffer not yet full
            result.extend_from_slice(&self.samples);
        } else {
            // Buffer is full, need to unwrap the ring
            result.extend_from_slice(&self.samples[self.position..]);
            result.extend_from_slice(&self.samples[..self.position]);
        }

        result
    }

    fn latest(&self) -> Option<LatencySample> {
        if self.samples.is_empty() {
            return None;
        }

        let idx = if self.position == 0 {
            self.samples.len() - 1
        } else {
            self.position - 1
        };

        self.samples.get(idx).copied()
    }

    fn reset(&mut self) {
        self.samples.clear();
        self.position = 0;
        self.count = 0;
        self.max_latency_ms = 0;
        self.total_events = 0;
    }
}

/// Latency monitor for tracking performance issues.
#[derive(Debug)]
pub struct LatencyMonitor {
    /// Latency threshold in milliseconds (events below this are not recorded).
    threshold_ms: u64,
    /// History for each event type.
    histories: RwLock<HashMap<LatencyEvent, EventHistory>>,
    /// Whether monitoring is enabled.
    enabled: bool,
    /// Start time for uptime calculation.
    start_time: Instant,
}

impl LatencyMonitor {
    /// Create a new latency monitor.
    pub fn new() -> Self {
        Self {
            threshold_ms: DEFAULT_LATENCY_THRESHOLD_MS,
            histories: RwLock::new(HashMap::new()),
            enabled: true,
            start_time: Instant::now(),
        }
    }

    /// Create a new latency monitor with a specific threshold.
    pub fn with_threshold(threshold_ms: u64) -> Self {
        Self {
            threshold_ms,
            histories: RwLock::new(HashMap::new()),
            enabled: true,
            start_time: Instant::now(),
        }
    }

    /// Set the latency threshold.
    pub fn set_threshold(&mut self, threshold_ms: u64) {
        self.threshold_ms = threshold_ms;
    }

    /// Get the latency threshold.
    pub fn threshold(&self) -> u64 {
        self.threshold_ms
    }

    /// Enable or disable monitoring.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if monitoring is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Record a latency event.
    #[inline]
    pub fn record(&self, event: LatencyEvent, latency: Duration) {
        if !self.enabled {
            return;
        }

        let latency_ms = latency.as_millis() as u64;

        // Only record if above threshold
        if latency_ms < self.threshold_ms {
            return;
        }

        let mut histories = self.histories.write();
        histories
            .entry(event)
            .or_insert_with(EventHistory::new)
            .record(latency_ms);
    }

    /// Record a latency event with start time.
    #[inline]
    pub fn record_since(&self, event: LatencyEvent, start: Instant) {
        self.record(event, start.elapsed());
    }

    /// Get history for a specific event type.
    pub fn get_history(&self, event: LatencyEvent) -> Vec<LatencySample> {
        let histories = self.histories.read();
        histories
            .get(&event)
            .map(|h| h.get_history())
            .unwrap_or_default()
    }

    /// Get the latest sample for a specific event type.
    pub fn get_latest(&self, event: LatencyEvent) -> Option<LatencySample> {
        let histories = self.histories.read();
        histories.get(&event).and_then(|h| h.latest())
    }

    /// Get the latest samples for all event types.
    pub fn get_all_latest(&self) -> HashMap<LatencyEvent, LatencySample> {
        let histories = self.histories.read();
        let mut result = HashMap::new();

        for (&event, history) in histories.iter() {
            if let Some(sample) = history.latest() {
                result.insert(event, sample);
            }
        }

        result
    }

    /// Reset history for a specific event type.
    pub fn reset(&self, event: LatencyEvent) {
        let mut histories = self.histories.write();
        if let Some(history) = histories.get_mut(&event) {
            history.reset();
        }
    }

    /// Reset all history.
    pub fn reset_all(&self) {
        let mut histories = self.histories.write();
        histories.clear();
    }

    /// Generate LATENCY DOCTOR output.
    pub fn doctor(&self) -> String {
        let histories = self.histories.read();
        let mut output = String::new();

        if histories.is_empty() {
            output.push_str("I have no latency reports to analyze.\n");
            output.push_str("Either the server is very fast or the latency monitor is disabled.\n");
            output.push_str(
                "Use CONFIG SET latency-monitor-threshold <milliseconds> to enable it.\n",
            );
            return output;
        }

        // Analyze each event type
        let mut issues = Vec::new();

        for event in LatencyEvent::all() {
            if let Some(history) = histories.get(event) {
                if history.max_latency_ms > 0 {
                    let avg = if history.total_events > 0 {
                        history.samples.iter().map(|s| s.latency_ms).sum::<u64>()
                            / history.samples.len() as u64
                    } else {
                        0
                    };

                    if history.max_latency_ms > 100 {
                        issues.push(format!(
                            "- {} had a max latency of {}ms (avg {}ms over {} samples)",
                            event.as_str(),
                            history.max_latency_ms,
                            avg,
                            history.samples.len()
                        ));
                    }
                }
            }
        }

        if issues.is_empty() {
            output.push_str("No significant latency issues detected.\n");
        } else {
            output.push_str("Latency issues detected:\n\n");
            for issue in issues {
                output.push_str(&issue);
                output.push('\n');
            }
            output.push('\n');
            output.push_str("Recommendations:\n");
            output.push_str("- Check if VDB save is configured too aggressively\n");
            output.push_str("- Consider using BGSAVE instead of SAVE\n");
            output.push_str("- Check for slow Lua scripts\n");
            output.push_str("- Verify network latency between clients and server\n");
            output.push_str("- Consider increasing maxmemory if eviction is frequent\n");
        }

        output
    }

    /// Generate LATENCY GRAPH output for an event type.
    pub fn graph(&self, event: LatencyEvent) -> String {
        let history = self.get_history(event);

        if history.is_empty() {
            return format!("No data for event type: {}\n", event.as_str());
        }

        // Find max for scaling
        let max_latency = history.iter().map(|s| s.latency_ms).max().unwrap_or(1);

        // ASCII art graph (like Redis)
        let width = 80;
        let height = 20;
        let scale = max_latency as f64 / height as f64;

        let mut output = String::new();
        output.push_str(&format!(
            "Latency graph for '{}' (max: {}ms)\n",
            event.as_str(),
            max_latency
        ));
        output.push_str(&"-".repeat(width));
        output.push('\n');

        // Sample points to display (we show up to `width` samples)
        let samples: Vec<_> = if history.len() <= width {
            history.clone()
        } else {
            let step = history.len() / width;
            history.iter().step_by(step).copied().collect()
        };

        // Draw rows from top to bottom
        for row in (0..height).rev() {
            let threshold = (row as f64 * scale) as u64;
            let mut line = String::new();

            for sample in &samples {
                if sample.latency_ms >= threshold {
                    line.push('#');
                } else {
                    line.push(' ');
                }
            }

            // Right-align latency label
            output.push_str(&format!("{:>6}ms |{}\n", threshold, line));
        }

        output.push_str(&format!("       +-{}\n", "-".repeat(samples.len())));
        output
    }

    /// Generate a summary for INFO output.
    pub fn info_string(&self) -> String {
        let histories = self.histories.read();
        let mut output = String::new();

        output.push_str(&format!(
            "latency_monitor_threshold:{}\n",
            self.threshold_ms
        ));

        let mut total_events = 0u64;
        let mut max_overall = 0u64;

        for history in histories.values() {
            total_events += history.total_events;
            max_overall = max_overall.max(history.max_latency_ms);
        }

        output.push_str(&format!("latency_events_total:{}\n", total_events));
        output.push_str(&format!("latency_max_ms:{}\n", max_overall));

        output
    }
}

impl Default for LatencyMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for measuring latency.
/// Records latency when dropped.
pub struct LatencyGuard<'a> {
    monitor: &'a LatencyMonitor,
    event: LatencyEvent,
    start: Instant,
}

impl<'a> LatencyGuard<'a> {
    /// Create a new latency guard.
    pub fn new(monitor: &'a LatencyMonitor, event: LatencyEvent) -> Self {
        Self {
            monitor,
            event,
            start: Instant::now(),
        }
    }
}

impl Drop for LatencyGuard<'_> {
    fn drop(&mut self) {
        self.monitor.record_since(self.event, self.start);
    }
}

/// Shared latency monitor type.
pub type SharedLatencyMonitor = std::sync::Arc<LatencyMonitor>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_history() {
        let monitor = LatencyMonitor::with_threshold(0);

        monitor.record(LatencyEvent::Command, Duration::from_millis(5));
        monitor.record(LatencyEvent::Command, Duration::from_millis(10));
        monitor.record(LatencyEvent::Command, Duration::from_millis(15));

        let history = monitor.get_history(LatencyEvent::Command);
        assert_eq!(history.len(), 3);

        // Check latencies
        assert_eq!(history[0].latency_ms, 5);
        assert_eq!(history[1].latency_ms, 10);
        assert_eq!(history[2].latency_ms, 15);
    }

    #[test]
    fn test_threshold() {
        let monitor = LatencyMonitor::with_threshold(10);

        // Below threshold - should not be recorded
        monitor.record(LatencyEvent::Command, Duration::from_millis(5));
        assert!(monitor.get_history(LatencyEvent::Command).is_empty());

        // At/above threshold - should be recorded
        monitor.record(LatencyEvent::Command, Duration::from_millis(10));
        assert_eq!(monitor.get_history(LatencyEvent::Command).len(), 1);
    }

    #[test]
    fn test_latest() {
        let monitor = LatencyMonitor::with_threshold(0);

        assert!(monitor.get_latest(LatencyEvent::Command).is_none());

        monitor.record(LatencyEvent::Command, Duration::from_millis(5));
        let latest = monitor.get_latest(LatencyEvent::Command).unwrap();
        assert_eq!(latest.latency_ms, 5);

        monitor.record(LatencyEvent::Command, Duration::from_millis(10));
        let latest = monitor.get_latest(LatencyEvent::Command).unwrap();
        assert_eq!(latest.latency_ms, 10);
    }

    #[test]
    fn test_reset() {
        let monitor = LatencyMonitor::with_threshold(0);

        monitor.record(LatencyEvent::Command, Duration::from_millis(5));
        monitor.record(LatencyEvent::FastCommand, Duration::from_millis(3));

        monitor.reset(LatencyEvent::Command);

        assert!(monitor.get_history(LatencyEvent::Command).is_empty());
        assert_eq!(monitor.get_history(LatencyEvent::FastCommand).len(), 1);

        monitor.reset_all();
        assert!(monitor.get_history(LatencyEvent::FastCommand).is_empty());
    }

    #[test]
    fn test_ring_buffer() {
        let monitor = LatencyMonitor::with_threshold(0);

        // Record more samples than the buffer size
        for i in 0..LATENCY_HISTORY_SIZE + 50 {
            monitor.record(LatencyEvent::Command, Duration::from_millis(i as u64));
        }

        let history = monitor.get_history(LatencyEvent::Command);

        // Should only keep LATENCY_HISTORY_SIZE samples
        assert_eq!(history.len(), LATENCY_HISTORY_SIZE);

        // First sample should be 50 (oldest remaining)
        assert_eq!(history[0].latency_ms, 50);

        // Last sample should be LATENCY_HISTORY_SIZE + 49
        assert_eq!(
            history[LATENCY_HISTORY_SIZE - 1].latency_ms,
            (LATENCY_HISTORY_SIZE + 49) as u64
        );
    }

    #[test]
    fn test_latency_guard() {
        let monitor = LatencyMonitor::with_threshold(0);

        {
            let _guard = LatencyGuard::new(&monitor, LatencyEvent::Command);
            std::thread::sleep(Duration::from_millis(5));
        }

        let history = monitor.get_history(LatencyEvent::Command);
        assert_eq!(history.len(), 1);
        assert!(history[0].latency_ms >= 5);
    }

    #[test]
    fn test_doctor_empty() {
        let monitor = LatencyMonitor::new();
        let doctor = monitor.doctor();
        assert!(doctor.contains("no latency reports"));
    }

    #[test]
    fn test_doctor_with_data() {
        let monitor = LatencyMonitor::with_threshold(0);

        // Record a high latency event
        monitor.record(LatencyEvent::VdbSave, Duration::from_millis(500));

        let doctor = monitor.doctor();
        assert!(doctor.contains("vdb-save") || doctor.contains("issues"));
    }
}
