//! Per-connection resource limits.
//!
//! Provides protection against resource exhaustion attacks by limiting:
//! - Memory usage per connection
//! - Output buffer size
//! - Query buffer size
//! - Number of pending commands
//!
//! # Redis Compatibility
//!
//! Compatible with Redis configuration options:
//! - client-output-buffer-limit
//! - client-query-buffer-limit
//! - maxmemory-clients

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default query buffer limit (1 GB like Redis)
pub const DEFAULT_QUERY_BUFFER_LIMIT: usize = 1024 * 1024 * 1024;

/// Default output buffer soft limit (256 MB)
pub const DEFAULT_OUTPUT_BUFFER_SOFT_LIMIT: usize = 256 * 1024 * 1024;

/// Default output buffer hard limit (512 MB)
pub const DEFAULT_OUTPUT_BUFFER_HARD_LIMIT: usize = 512 * 1024 * 1024;

/// Default soft limit timeout (60 seconds)
pub const DEFAULT_SOFT_LIMIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Client type for different buffer limit profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientType {
    /// Normal clients
    Normal,
    /// Replica/slave connections
    Replica,
    /// Pub/Sub subscribers
    PubSub,
    /// Master connection (when this server is a replica)
    Master,
}

impl Default for ClientType {
    fn default() -> Self {
        Self::Normal
    }
}

/// Output buffer limits configuration.
#[derive(Debug, Clone)]
pub struct OutputBufferLimits {
    /// Hard limit - client is immediately disconnected
    pub hard_limit: usize,
    /// Soft limit - client is disconnected if exceeded for soft_timeout
    pub soft_limit: usize,
    /// Time allowed at soft limit before disconnect
    pub soft_timeout: Duration,
}

impl Default for OutputBufferLimits {
    fn default() -> Self {
        Self {
            hard_limit: DEFAULT_OUTPUT_BUFFER_HARD_LIMIT,
            soft_limit: DEFAULT_OUTPUT_BUFFER_SOFT_LIMIT,
            soft_timeout: DEFAULT_SOFT_LIMIT_TIMEOUT,
        }
    }
}

/// Limits for pub/sub clients (more permissive).
impl OutputBufferLimits {
    /// Create limits for pub/sub clients.
    #[must_use]
    pub fn pubsub() -> Self {
        Self {
            hard_limit: 32 * 1024 * 1024,  // 32 MB
            soft_limit: 8 * 1024 * 1024,   // 8 MB
            soft_timeout: Duration::from_secs(60),
        }
    }

    /// Create limits for replica clients.
    #[must_use]
    pub fn replica() -> Self {
        Self {
            hard_limit: 256 * 1024 * 1024, // 256 MB
            soft_limit: 64 * 1024 * 1024,  // 64 MB
            soft_timeout: Duration::from_secs(60),
        }
    }
}

/// Configuration for connection limits.
#[derive(Debug, Clone)]
pub struct ConnectionLimitsConfig {
    /// Maximum total memory for all clients
    pub maxmemory_clients: usize,
    /// Query buffer limit per client
    pub query_buffer_limit: usize,
    /// Output buffer limits by client type
    pub output_buffer_limits: HashMap<ClientType, OutputBufferLimits>,
    /// Maximum pending commands per client
    pub max_pending_commands: usize,
    /// Enable eviction of clients when maxmemory_clients is reached
    pub evict_clients: bool,
}

impl Default for ConnectionLimitsConfig {
    fn default() -> Self {
        let mut output_buffer_limits = HashMap::new();
        output_buffer_limits.insert(ClientType::Normal, OutputBufferLimits::default());
        output_buffer_limits.insert(ClientType::PubSub, OutputBufferLimits::pubsub());
        output_buffer_limits.insert(ClientType::Replica, OutputBufferLimits::replica());
        output_buffer_limits.insert(ClientType::Master, OutputBufferLimits::default());

        Self {
            maxmemory_clients: 0, // 0 = no limit
            query_buffer_limit: DEFAULT_QUERY_BUFFER_LIMIT,
            output_buffer_limits,
            max_pending_commands: 10000,
            evict_clients: true,
        }
    }
}

/// Per-connection memory tracker.
#[derive(Debug)]
pub struct ConnectionMemoryTracker {
    /// Connection ID
    pub id: u64,
    /// Client type
    pub client_type: ClientType,
    /// Current query buffer size
    query_buffer_size: AtomicUsize,
    /// Current output buffer size
    output_buffer_size: AtomicUsize,
    /// Pending command count
    pending_commands: AtomicUsize,
    /// Time when soft limit was first exceeded
    soft_limit_exceeded_at: Mutex<Option<Instant>>,
    /// Peak memory usage
    peak_memory: AtomicUsize,
    /// Total bytes read
    bytes_read: AtomicU64,
    /// Total bytes written
    bytes_written: AtomicU64,
}

impl ConnectionMemoryTracker {
    /// Create a new connection memory tracker.
    #[must_use]
    pub fn new(id: u64, client_type: ClientType) -> Self {
        Self {
            id,
            client_type,
            query_buffer_size: AtomicUsize::new(0),
            output_buffer_size: AtomicUsize::new(0),
            pending_commands: AtomicUsize::new(0),
            soft_limit_exceeded_at: Mutex::new(None),
            peak_memory: AtomicUsize::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    /// Get current total memory usage.
    #[must_use]
    pub fn total_memory(&self) -> usize {
        self.query_buffer_size.load(Ordering::Relaxed)
            + self.output_buffer_size.load(Ordering::Relaxed)
    }

    /// Update query buffer size and return the new total.
    pub fn set_query_buffer_size(&self, size: usize) -> usize {
        self.query_buffer_size.store(size, Ordering::Relaxed);
        self.update_peak();
        self.total_memory()
    }

    /// Update output buffer size and return the new total.
    pub fn set_output_buffer_size(&self, size: usize) -> usize {
        self.output_buffer_size.store(size, Ordering::Relaxed);
        self.update_peak();
        self.total_memory()
    }

    /// Add to output buffer size.
    pub fn add_output_buffer(&self, bytes: usize) -> usize {
        let new_size = self.output_buffer_size.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.update_peak();
        new_size
    }

    /// Subtract from output buffer size.
    pub fn drain_output_buffer(&self, bytes: usize) -> usize {
        let current = self.output_buffer_size.load(Ordering::Relaxed);
        let new_size = current.saturating_sub(bytes);
        self.output_buffer_size.store(new_size, Ordering::Relaxed);
        new_size
    }

    /// Increment pending commands.
    pub fn increment_pending(&self) -> usize {
        self.pending_commands.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Decrement pending commands.
    pub fn decrement_pending(&self) -> usize {
        let current = self.pending_commands.load(Ordering::Relaxed);
        if current > 0 {
            self.pending_commands.fetch_sub(1, Ordering::Relaxed) - 1
        } else {
            0
        }
    }

    /// Get pending command count.
    #[must_use]
    pub fn pending_commands(&self) -> usize {
        self.pending_commands.load(Ordering::Relaxed)
    }

    /// Record bytes read.
    pub fn record_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes written.
    pub fn record_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get total bytes read.
    #[must_use]
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Get total bytes written.
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get peak memory usage.
    #[must_use]
    pub fn peak_memory(&self) -> usize {
        self.peak_memory.load(Ordering::Relaxed)
    }

    /// Update peak memory if current is higher.
    fn update_peak(&self) {
        let current = self.total_memory();
        let mut peak = self.peak_memory.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_memory.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    /// Mark soft limit exceeded.
    pub fn mark_soft_limit_exceeded(&self) {
        let mut guard = self.soft_limit_exceeded_at.lock();
        if guard.is_none() {
            *guard = Some(Instant::now());
        }
    }

    /// Clear soft limit exceeded marker.
    pub fn clear_soft_limit_exceeded(&self) {
        *self.soft_limit_exceeded_at.lock() = None;
    }

    /// Check if soft limit timeout has been exceeded.
    #[must_use]
    pub fn is_soft_limit_timeout_exceeded(&self, timeout: Duration) -> bool {
        if let Some(exceeded_at) = *self.soft_limit_exceeded_at.lock() {
            exceeded_at.elapsed() > timeout
        } else {
            false
        }
    }
}

/// Reason for connection limit violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitViolation {
    /// Query buffer too large
    QueryBufferExceeded { size: usize, limit: usize },
    /// Output buffer hard limit exceeded
    OutputBufferHardLimit { size: usize, limit: usize },
    /// Output buffer soft limit timeout exceeded
    OutputBufferSoftTimeout { size: usize, limit: usize, duration: Duration },
    /// Too many pending commands
    TooManyPendingCommands { count: usize, limit: usize },
    /// Total client memory exceeded
    TotalClientMemoryExceeded { total: usize, limit: usize },
}

impl std::fmt::Display for LimitViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueryBufferExceeded { size, limit } => {
                write!(f, "query buffer limit exceeded: {size} > {limit}")
            }
            Self::OutputBufferHardLimit { size, limit } => {
                write!(f, "output buffer hard limit exceeded: {size} > {limit}")
            }
            Self::OutputBufferSoftTimeout { size, limit, duration } => {
                write!(
                    f,
                    "output buffer soft limit {size} > {limit} exceeded for {duration:?}"
                )
            }
            Self::TooManyPendingCommands { count, limit } => {
                write!(f, "too many pending commands: {count} > {limit}")
            }
            Self::TotalClientMemoryExceeded { total, limit } => {
                write!(f, "total client memory exceeded: {total} > {limit}")
            }
        }
    }
}

/// Global connection limits manager.
#[derive(Debug)]
pub struct ConnectionLimits {
    /// Configuration
    config: parking_lot::RwLock<ConnectionLimitsConfig>,
    /// Total memory used by all connections
    total_memory: AtomicUsize,
    /// Active connection trackers
    connections: parking_lot::RwLock<HashMap<u64, Arc<ConnectionMemoryTracker>>>,
    /// Next connection ID
    next_id: AtomicU64,
    /// Statistics
    stats: ConnectionLimitsStats,
}

/// Statistics for connection limits.
#[derive(Debug, Default)]
pub struct ConnectionLimitsStats {
    /// Connections killed for exceeding limits
    pub connections_killed: AtomicU64,
    /// Query buffer limit violations
    pub query_buffer_violations: AtomicU64,
    /// Output buffer violations
    pub output_buffer_violations: AtomicU64,
    /// Pending command violations
    pub pending_command_violations: AtomicU64,
}

impl ConnectionLimits {
    /// Create a new connection limits manager.
    #[must_use]
    pub fn new(config: ConnectionLimitsConfig) -> Self {
        Self {
            config: parking_lot::RwLock::new(config),
            total_memory: AtomicUsize::new(0),
            connections: parking_lot::RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            stats: ConnectionLimitsStats::default(),
        }
    }

    /// Register a new connection and return its tracker.
    pub fn register_connection(&self, client_type: ClientType) -> Arc<ConnectionMemoryTracker> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let tracker = Arc::new(ConnectionMemoryTracker::new(id, client_type));
        self.connections.write().insert(id, Arc::clone(&tracker));
        tracker
    }

    /// Unregister a connection.
    pub fn unregister_connection(&self, id: u64) {
        if let Some(tracker) = self.connections.write().remove(&id) {
            // Subtract this connection's memory from total
            self.total_memory
                .fetch_sub(tracker.total_memory(), Ordering::Relaxed);
        }
    }

    /// Check if a connection violates any limits.
    pub fn check_limits(&self, tracker: &ConnectionMemoryTracker) -> Result<(), LimitViolation> {
        let config = self.config.read();

        // Check query buffer limit
        let query_size = tracker.query_buffer_size.load(Ordering::Relaxed);
        if query_size > config.query_buffer_limit {
            self.stats
                .query_buffer_violations
                .fetch_add(1, Ordering::Relaxed);
            return Err(LimitViolation::QueryBufferExceeded {
                size: query_size,
                limit: config.query_buffer_limit,
            });
        }

        // Check output buffer limits
        let output_size = tracker.output_buffer_size.load(Ordering::Relaxed);
        if let Some(limits) = config.output_buffer_limits.get(&tracker.client_type) {
            // Hard limit check
            if output_size > limits.hard_limit {
                self.stats
                    .output_buffer_violations
                    .fetch_add(1, Ordering::Relaxed);
                return Err(LimitViolation::OutputBufferHardLimit {
                    size: output_size,
                    limit: limits.hard_limit,
                });
            }

            // Soft limit check
            if output_size > limits.soft_limit {
                tracker.mark_soft_limit_exceeded();
                if tracker.is_soft_limit_timeout_exceeded(limits.soft_timeout) {
                    self.stats
                        .output_buffer_violations
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(LimitViolation::OutputBufferSoftTimeout {
                        size: output_size,
                        limit: limits.soft_limit,
                        duration: limits.soft_timeout,
                    });
                }
            } else {
                tracker.clear_soft_limit_exceeded();
            }
        }

        // Check pending commands
        let pending = tracker.pending_commands();
        if pending > config.max_pending_commands {
            self.stats
                .pending_command_violations
                .fetch_add(1, Ordering::Relaxed);
            return Err(LimitViolation::TooManyPendingCommands {
                count: pending,
                limit: config.max_pending_commands,
            });
        }

        // Check total client memory
        if config.maxmemory_clients > 0 {
            let total = self.total_memory.load(Ordering::Relaxed);
            if total > config.maxmemory_clients {
                return Err(LimitViolation::TotalClientMemoryExceeded {
                    total,
                    limit: config.maxmemory_clients,
                });
            }
        }

        Ok(())
    }

    /// Update total memory tracking when a connection's memory changes.
    pub fn update_total_memory(&self, delta: isize) {
        if delta >= 0 {
            self.total_memory
                .fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.total_memory
                .fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }

    /// Get total memory used by all connections.
    #[must_use]
    pub fn total_memory(&self) -> usize {
        self.total_memory.load(Ordering::Relaxed)
    }

    /// Get number of active connections.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }

    /// Find connections that should be evicted to free memory.
    ///
    /// Returns connection IDs sorted by memory usage (highest first).
    pub fn find_eviction_candidates(&self, bytes_needed: usize) -> Vec<u64> {
        let config = self.config.read();
        if !config.evict_clients {
            return Vec::new();
        }

        let connections = self.connections.read();
        let mut candidates: Vec<_> = connections
            .iter()
            .filter(|(_, tracker)| tracker.client_type == ClientType::Normal)
            .map(|(id, tracker)| (*id, tracker.total_memory()))
            .collect();

        // Sort by memory usage descending
        candidates.sort_by(|a, b| b.1.cmp(&a.1));

        // Return enough candidates to free the needed memory
        let mut freed = 0;
        let mut result = Vec::new();
        for (id, mem) in candidates {
            if freed >= bytes_needed {
                break;
            }
            result.push(id);
            freed += mem;
        }

        result
    }

    /// Get statistics.
    #[must_use]
    pub fn stats(&self) -> &ConnectionLimitsStats {
        &self.stats
    }

    /// Update configuration.
    pub fn update_config(&self, config: ConnectionLimitsConfig) {
        *self.config.write() = config;
    }

    /// Get a connection tracker by ID.
    #[must_use]
    pub fn get_tracker(&self, id: u64) -> Option<Arc<ConnectionMemoryTracker>> {
        self.connections.read().get(&id).cloned()
    }

    /// Record a killed connection.
    pub fn record_kill(&self) {
        self.stats.connections_killed.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self::new(ConnectionLimitsConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_tracker() {
        let tracker = ConnectionMemoryTracker::new(1, ClientType::Normal);

        tracker.set_query_buffer_size(1000);
        tracker.set_output_buffer_size(2000);

        assert_eq!(tracker.total_memory(), 3000);
        assert_eq!(tracker.peak_memory(), 3000);

        // Add more output
        tracker.add_output_buffer(500);
        assert_eq!(tracker.total_memory(), 3500);

        // Drain some
        tracker.drain_output_buffer(1000);
        assert_eq!(tracker.total_memory(), 2500);

        // Peak should still be 3500
        assert_eq!(tracker.peak_memory(), 3500);
    }

    #[test]
    fn test_pending_commands() {
        let tracker = ConnectionMemoryTracker::new(1, ClientType::Normal);

        assert_eq!(tracker.pending_commands(), 0);
        assert_eq!(tracker.increment_pending(), 1);
        assert_eq!(tracker.increment_pending(), 2);
        assert_eq!(tracker.decrement_pending(), 1);
        assert_eq!(tracker.decrement_pending(), 0);
        // Should not go negative
        assert_eq!(tracker.decrement_pending(), 0);
    }

    #[test]
    fn test_connection_limits() {
        let limits = ConnectionLimits::default();

        let tracker = limits.register_connection(ClientType::Normal);
        assert_eq!(limits.connection_count(), 1);

        // Should be within limits initially
        assert!(limits.check_limits(&tracker).is_ok());

        // Exceed query buffer limit
        tracker.set_query_buffer_size(2 * 1024 * 1024 * 1024); // 2 GB
        let result = limits.check_limits(&tracker);
        assert!(matches!(
            result,
            Err(LimitViolation::QueryBufferExceeded { .. })
        ));
    }

    #[test]
    fn test_output_buffer_limits() {
        let config = ConnectionLimitsConfig {
            output_buffer_limits: {
                let mut map = HashMap::new();
                map.insert(
                    ClientType::Normal,
                    OutputBufferLimits {
                        hard_limit: 1000,
                        soft_limit: 500,
                        soft_timeout: Duration::from_millis(100),
                    },
                );
                map
            },
            ..Default::default()
        };

        let limits = ConnectionLimits::new(config);
        let tracker = limits.register_connection(ClientType::Normal);

        // Under soft limit
        tracker.set_output_buffer_size(400);
        assert!(limits.check_limits(&tracker).is_ok());

        // Over soft limit but under hard limit
        tracker.set_output_buffer_size(600);
        assert!(limits.check_limits(&tracker).is_ok()); // Not timed out yet

        // Over hard limit
        tracker.set_output_buffer_size(1500);
        assert!(matches!(
            limits.check_limits(&tracker),
            Err(LimitViolation::OutputBufferHardLimit { .. })
        ));
    }

    #[test]
    fn test_eviction_candidates() {
        let limits = ConnectionLimits::default();

        // Create some connections with different memory usage
        let t1 = limits.register_connection(ClientType::Normal);
        t1.set_output_buffer_size(1000);

        let t2 = limits.register_connection(ClientType::Normal);
        t2.set_output_buffer_size(5000);

        let t3 = limits.register_connection(ClientType::Normal);
        t3.set_output_buffer_size(2000);

        // Verify memory is tracked correctly
        assert_eq!(t1.total_memory(), 1000);
        assert_eq!(t2.total_memory(), 5000);
        assert_eq!(t3.total_memory(), 2000);

        // Request eviction to free 6000 bytes - needs both t2 (5000) and t3 (2000)
        let candidates = limits.find_eviction_candidates(6000);

        // Should be ordered by memory (highest first): t2 (5000), t3 (2000)
        // Total freed: 5000 + 2000 = 7000 >= 6000
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], t2.id);
        assert_eq!(candidates[1], t3.id);

        // Request eviction to free 3000 bytes - only needs t2 (5000)
        let candidates_small = limits.find_eviction_candidates(3000);
        assert_eq!(candidates_small.len(), 1);
        assert_eq!(candidates_small[0], t2.id);
    }

    #[test]
    fn test_bytes_tracking() {
        let tracker = ConnectionMemoryTracker::new(1, ClientType::Normal);

        tracker.record_read(100);
        tracker.record_read(200);
        tracker.record_written(50);

        assert_eq!(tracker.bytes_read(), 300);
        assert_eq!(tracker.bytes_written(), 50);
    }
}
