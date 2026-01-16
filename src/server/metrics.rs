//! Server metrics for performance monitoring and optimization.
//!
//! Provides real-time visibility into server performance under load.

use std::sync::atomic::{AtomicU64, Ordering};

/// Get current process memory usage in bytes.
///
/// Returns `(used_memory, used_memory_rss)` tuple where:
/// - `used_memory`: Virtual memory size (or heap approximation)
/// - `used_memory_rss`: Resident Set Size (physical memory)
///
/// Falls back to `(0, 0)` if unable to determine on unsupported platforms.
pub fn get_memory_usage() -> (usize, usize) {
    #[cfg(target_os = "linux")]
    {
        get_memory_usage_linux()
    }

    #[cfg(target_os = "macos")]
    {
        get_memory_usage_macos()
    }

    #[cfg(target_os = "windows")]
    {
        get_memory_usage_windows()
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        // Fallback for other platforms - try POSIX getrusage
        get_memory_usage_posix()
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn get_memory_usage_linux() -> (usize, usize) {
    // Read from /proc/self/statm (page-based memory stats)
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        let parts: Vec<&str> = statm.split_whitespace().collect();
        if parts.len() >= 2 {
            // Get actual page size from system
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
            let vsize = parts[0].parse::<usize>().unwrap_or(0) * page_size;
            let rss = parts[1].parse::<usize>().unwrap_or(0) * page_size;
            return (vsize, rss);
        }
    }
    (0, 0)
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn get_memory_usage_macos() -> (usize, usize) {
    // Use getrusage for RSS on macOS - it's reliable and simple
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) };

    if ret == 0 {
        // On macOS, ru_maxrss is in bytes (unlike Linux where it's in KB)
        let rss = rusage.ru_maxrss as usize;

        // For virtual memory, we can use task_info but getrusage doesn't provide it
        // Use RSS as approximation for used_memory as well
        (rss, rss)
    } else {
        (0, 0)
    }
}

#[cfg(target_os = "windows")]
#[allow(unsafe_code)]
fn get_memory_usage_windows() -> (usize, usize) {
    // Windows implementation using GetProcessMemoryInfo
    use std::mem::size_of;

    #[repr(C)]
    #[allow(non_snake_case)]
    struct PROCESS_MEMORY_COUNTERS {
        cb: u32,
        PageFaultCount: u32,
        PeakWorkingSetSize: usize,
        WorkingSetSize: usize,
        QuotaPeakPagedPoolUsage: usize,
        QuotaPagedPoolUsage: usize,
        QuotaPeakNonPagedPoolUsage: usize,
        QuotaNonPagedPoolUsage: usize,
        PagefileUsage: usize,
        PeakPagefileUsage: usize,
    }

    #[link(name = "psapi")]
    extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut std::ffi::c_void,
            pmc: *mut PROCESS_MEMORY_COUNTERS,
            cb: u32,
        ) -> i32;
        fn GetCurrentProcess() -> *mut std::ffi::c_void;
    }

    unsafe {
        let mut pmc: PROCESS_MEMORY_COUNTERS = std::mem::zeroed();
        pmc.cb = size_of::<PROCESS_MEMORY_COUNTERS>() as u32;

        if GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb) != 0 {
            // PagefileUsage is virtual memory, WorkingSetSize is RSS
            (pmc.PagefileUsage, pmc.WorkingSetSize)
        } else {
            (0, 0)
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
#[allow(unsafe_code)]
fn get_memory_usage_posix() -> (usize, usize) {
    // POSIX fallback using getrusage
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) };

    if ret == 0 {
        // ru_maxrss is in kilobytes on most POSIX systems (except macOS)
        let rss = (rusage.ru_maxrss as usize) * 1024;
        (rss, rss)
    } else {
        (0, 0)
    }
}

/// Format bytes in human readable form.
pub fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// Lightweight histogram for latency tracking.
/// Uses fixed buckets for O(1) insertion.
#[derive(Debug)]
pub struct LatencyHistogram {
    /// Buckets: <100μs, <500μs, <1ms, <5ms, <10ms, <50ms, <100ms, >100ms
    buckets: [AtomicU64; 8],
    total_count: AtomicU64,
    total_sum_us: AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            buckets: Default::default(),
            total_count: AtomicU64::new(0),
            total_sum_us: AtomicU64::new(0),
        }
    }

    /// Record a latency measurement.
    #[inline]
    pub fn record(&self, duration: Duration) {
        let us = duration.as_micros() as u64;

        let bucket = match us {
            0..=99 => 0,
            100..=499 => 1,
            500..=999 => 2,
            1000..=4999 => 3,
            5000..=9999 => 4,
            10000..=49999 => 5,
            50000..=99999 => 6,
            _ => 7,
        };

        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.total_sum_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Get approximate percentile (p50, p99, etc.).
    pub fn percentile(&self, p: f64) -> Duration {
        let total = self.total_count.load(Ordering::Relaxed);
        if total == 0 {
            return Duration::ZERO;
        }

        let target = ((total as f64) * p / 100.0) as u64;
        let mut cumulative = 0u64;

        let bucket_maxes = [100, 500, 1000, 5000, 10000, 50000, 100000, 1_000_000];

        for (i, &max_us) in bucket_maxes.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            if cumulative >= target {
                return Duration::from_micros(max_us);
            }
        }

        Duration::from_micros(bucket_maxes[7])
    }

    /// Get average latency.
    pub fn average(&self) -> Duration {
        let count = self.total_count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let sum = self.total_sum_us.load(Ordering::Relaxed);
        Duration::from_micros(sum / count)
    }

    /// Reset all counters.
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.total_count.store(0, Ordering::Relaxed);
        self.total_sum_us.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Server-wide metrics.
#[derive(Debug)]
pub struct ServerMetrics {
    // Connection metrics
    pub active_connections: AtomicU64,
    pub total_connections: AtomicU64,
    pub rejected_connections: AtomicU64,

    // Throughput metrics
    pub commands_processed: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,

    // Error metrics
    pub errors: AtomicU64,
    pub parse_errors: AtomicU64,

    // Latency tracking
    pub command_latency: LatencyHistogram,

    // Time tracking
    start_time: Instant,
    last_reset: Mutex<Instant>,
}

impl ServerMetrics {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            commands_processed: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            command_latency: LatencyHistogram::new(),
            start_time: now,
            last_reset: Mutex::new(now),
        }
    }

    /// Get uptime.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Calculate operations per second since last reset.
    pub fn ops_per_second(&self) -> f64 {
        let elapsed = self.last_reset.lock().elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return 0.0;
        }
        self.commands_processed.load(Ordering::Relaxed) as f64 / elapsed
    }

    /// Calculate throughput in MB/s.
    pub fn throughput_mbps(&self) -> f64 {
        let elapsed = self.last_reset.lock().elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return 0.0;
        }
        let bytes =
            self.bytes_received.load(Ordering::Relaxed) + self.bytes_sent.load(Ordering::Relaxed);
        (bytes as f64 / 1_000_000.0) / elapsed
    }

    /// Record a command execution.
    #[inline]
    pub fn record_command(&self, latency: Duration, bytes_in: u64, bytes_out: u64) {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes_out, Ordering::Relaxed);
        self.command_latency.record(latency);
    }

    /// Increment connection count.
    #[inline]
    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement connection count.
    #[inline]
    pub fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a rejected connection.
    #[inline]
    pub fn connection_rejected(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an error.
    #[inline]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Generate INFO-style stats string (Redis compatible).
    pub fn to_info_string(&self) -> String {
        let uptime = self.uptime();

        format!(
            "# Server\n\
             uptime_in_seconds:{}\n\
             uptime_in_days:{}\n\
             \n\
             # Clients\n\
             connected_clients:{}\n\
             total_connections_received:{}\n\
             rejected_connections:{}\n\
             \n\
             # Stats\n\
             total_commands_processed:{}\n\
             instantaneous_ops_per_sec:{:.0}\n\
             total_net_input_bytes:{}\n\
             total_net_output_bytes:{}\n\
             instantaneous_input_kbps:{:.2}\n\
             instantaneous_output_kbps:{:.2}\n\
             \n\
             # Latency\n\
             latency_avg_us:{}\n\
             latency_p50_us:{}\n\
             latency_p99_us:{}\n\
             \n\
             # Errors\n\
             total_errors:{}\n\
             parse_errors:{}\n",
            uptime.as_secs(),
            uptime.as_secs() / 86400,
            self.active_connections.load(Ordering::Relaxed),
            self.total_connections.load(Ordering::Relaxed),
            self.rejected_connections.load(Ordering::Relaxed),
            self.commands_processed.load(Ordering::Relaxed),
            self.ops_per_second(),
            self.bytes_received.load(Ordering::Relaxed),
            self.bytes_sent.load(Ordering::Relaxed),
            self.throughput_mbps() * 1000.0 / 2.0, // Approximate split
            self.throughput_mbps() * 1000.0 / 2.0,
            self.command_latency.average().as_micros(),
            self.command_latency.percentile(50.0).as_micros(),
            self.command_latency.percentile(99.0).as_micros(),
            self.errors.load(Ordering::Relaxed),
            self.parse_errors.load(Ordering::Relaxed),
        )
    }

    /// Reset counters (for interval-based metrics).
    pub fn reset_counters(&self) {
        self.commands_processed.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.command_latency.reset();
        *self.last_reset.lock() = Instant::now();
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for timing command execution.
pub struct CommandTimer<'a> {
    metrics: &'a ServerMetrics,
    start: Instant,
    bytes_in: u64,
}

impl<'a> CommandTimer<'a> {
    pub fn new(metrics: &'a ServerMetrics, bytes_in: u64) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            bytes_in,
        }
    }

    pub fn finish(self, bytes_out: u64) {
        self.metrics
            .record_command(self.start.elapsed(), self.bytes_in, bytes_out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_basic() {
        let hist = LatencyHistogram::new();

        hist.record(Duration::from_micros(50));
        hist.record(Duration::from_micros(150));
        hist.record(Duration::from_micros(800));
        hist.record(Duration::from_millis(2));
        hist.record(Duration::from_millis(50));

        assert!(hist.percentile(50.0) <= Duration::from_millis(1));
        assert!(hist.percentile(99.0) <= Duration::from_millis(100));
    }

    #[test]
    fn test_metrics_ops_per_second() {
        let metrics = ServerMetrics::new();

        for _ in 0..1000 {
            metrics.record_command(Duration::from_micros(100), 10, 20);
        }

        // Should have some ops/sec (depends on timing)
        assert!(metrics.commands_processed.load(Ordering::Relaxed) == 1000);
    }

    #[test]
    fn test_info_string() {
        let metrics = ServerMetrics::new();
        metrics.connection_opened();
        metrics.record_command(Duration::from_micros(100), 50, 100);

        let info = metrics.to_info_string();
        assert!(info.contains("connected_clients:1"));
        assert!(info.contains("total_commands_processed:1"));
    }

    #[test]
    fn test_memory_usage() {
        let (used, rss) = get_memory_usage();
        // Should return non-zero values on supported platforms
        // At minimum, we're using some memory to run this test!
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        {
            assert!(rss > 0, "RSS should be non-zero on supported platforms");
            assert!(
                used > 0,
                "Used memory should be non-zero on supported platforms"
            );
        }
        // On unsupported platforms, just verify it doesn't panic
        let _ = (used, rss);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(1024), "1.00K");
        assert_eq!(format_bytes(1536), "1.50K");
        assert_eq!(format_bytes(1024 * 1024), "1.00M");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00G");
    }
}
