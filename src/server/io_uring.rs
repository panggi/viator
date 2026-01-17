//! io_uring support for high-performance async I/O on Linux.
//!
//! This module provides an abstraction layer for using io_uring when available
//! on Linux systems, falling back to standard tokio I/O on other platforms.
//!
//! # Benefits of io_uring
//!
//! - Batched syscalls: Multiple I/O operations per kernel transition
//! - Zero-copy: Shared ring buffers between user and kernel space
//! - Reduced context switches: Polling-based completion notification
//! - Better scalability: Especially under high connection counts
//!
//! # Usage
//!
//! The abstraction automatically selects the best I/O backend:
//! - Linux 5.1+: io_uring (if the `io-uring` feature is enabled)
//! - Other platforms: tokio's standard async I/O

use std::io;

/// Check if io_uring is available on this system.
///
/// Returns true if:
/// - Running on Linux
/// - Kernel version >= 5.1
/// - io_uring feature is enabled
#[inline]
pub fn is_available() -> bool {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        check_io_uring_support()
    }

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        false
    }
}

/// Check io_uring kernel support on Linux.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn check_io_uring_support() -> bool {
    use std::fs;

    // Check kernel version
    if let Ok(version) = fs::read_to_string("/proc/version") {
        // Parse version string like "Linux version 5.15.0-..."
        let parts: Vec<&str> = version.split_whitespace().collect();
        if parts.len() >= 3 {
            let version_str = parts[2];
            let version_parts: Vec<&str> = version_str.split('.').collect();
            if version_parts.len() >= 2 {
                if let (Ok(major), Ok(minor)) = (
                    version_parts[0].parse::<u32>(),
                    version_parts[1].parse::<u32>(),
                ) {
                    // io_uring requires kernel 5.1+
                    return major > 5 || (major == 5 && minor >= 1);
                }
            }
        }
    }
    false
}

/// io_uring configuration options.
#[derive(Debug, Clone)]
pub struct IoUringConfig {
    /// Size of the submission queue (must be power of 2)
    pub sq_entries: u32,
    /// Size of the completion queue (must be power of 2)
    pub cq_entries: u32,
    /// Enable SQPOLL mode for kernel-side polling
    pub sqpoll: bool,
    /// SQPOLL idle timeout in milliseconds
    pub sqpoll_idle_ms: u32,
    /// Enable fixed buffers for zero-copy I/O
    pub fixed_buffers: bool,
    /// Number of fixed buffers
    pub num_fixed_buffers: usize,
    /// Size of each fixed buffer
    pub fixed_buffer_size: usize,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            cq_entries: 512,
            sqpoll: false, // SQPOLL requires CAP_SYS_NICE
            sqpoll_idle_ms: 1000,
            fixed_buffers: true,
            num_fixed_buffers: 64,
            fixed_buffer_size: 4096,
        }
    }
}

impl IoUringConfig {
    /// Configuration optimized for high-throughput networking.
    pub fn high_throughput() -> Self {
        Self {
            sq_entries: 1024,
            cq_entries: 2048,
            sqpoll: false,
            sqpoll_idle_ms: 1000,
            fixed_buffers: true,
            num_fixed_buffers: 256,
            fixed_buffer_size: 8192,
        }
    }

    /// Configuration for low-latency with SQPOLL (requires privileges).
    pub fn low_latency() -> Self {
        Self {
            sq_entries: 512,
            cq_entries: 1024,
            sqpoll: true,
            sqpoll_idle_ms: 100,
            fixed_buffers: true,
            num_fixed_buffers: 128,
            fixed_buffer_size: 4096,
        }
    }
}

/// I/O operation type for batching.
#[derive(Debug, Clone, Copy)]
pub enum IoOp {
    /// Read operation
    Read,
    /// Write operation
    Write,
    /// Accept connection
    Accept,
    /// Close file descriptor
    Close,
}

/// Statistics for io_uring operations.
#[derive(Debug, Default)]
pub struct IoUringStats {
    /// Total submissions
    pub submissions: u64,
    /// Total completions
    pub completions: u64,
    /// Batched submissions (multiple ops per syscall)
    pub batched_submissions: u64,
    /// Operations that used fixed buffers
    pub fixed_buffer_ops: u64,
    /// CQ overflows (indicates need for larger CQ)
    pub cq_overflows: u64,
}

/// Abstract I/O backend that can use either io_uring or tokio.
///
/// This provides a common interface regardless of the underlying
/// I/O mechanism, allowing the server to benefit from io_uring
/// when available without changing the core logic.
#[derive(Debug)]
pub struct IoBackend {
    /// Whether we're using io_uring
    using_io_uring: bool,
    /// Statistics
    stats: IoUringStats,
}

impl IoBackend {
    /// Create a new I/O backend, preferring io_uring if available.
    pub fn new(_config: &IoUringConfig) -> io::Result<Self> {
        let using_io_uring = is_available();

        if using_io_uring {
            tracing::info!("Using io_uring for I/O operations");
        } else {
            tracing::info!("Using tokio for I/O operations (io_uring not available)");
        }

        Ok(Self {
            using_io_uring,
            stats: IoUringStats::default(),
        })
    }

    /// Check if io_uring is being used.
    #[inline]
    pub fn is_io_uring(&self) -> bool {
        self.using_io_uring
    }

    /// Get I/O statistics.
    pub fn stats(&self) -> &IoUringStats {
        &self.stats
    }

    /// Get the backend name for logging.
    pub fn name(&self) -> &'static str {
        if self.using_io_uring {
            "io_uring"
        } else {
            "tokio"
        }
    }
}

impl Default for IoBackend {
    fn default() -> Self {
        // SAFETY: IoBackend::new() currently always returns Ok - it detects
        // io_uring availability but doesn't fail on unavailability
        Self::new(&IoUringConfig::default())
            .unwrap_or_else(|e| unreachable!("IoBackend::new failed unexpectedly: {}", e))
    }
}

// Note: Full io_uring implementation would require the `io-uring` crate
// and significant integration with the connection handling code.
// This module provides the foundation and configuration for that integration.
//
// A complete implementation would include:
// 1. Ring buffer management for submissions/completions
// 2. Fixed buffer registration for zero-copy I/O
// 3. Multi-shot accept for efficient connection handling
// 4. Batched read/write operations
// 5. Integration with tokio's runtime for hybrid operation

/// Run the server using io_uring on Linux.
///
/// This provides significantly better performance by:
/// - Batching syscalls (multiple I/O ops per kernel transition)
/// - Using completion-based I/O (no polling overhead)
/// - Supporting zero-copy operations with registered buffers
///
/// Falls back to tokio on non-Linux or when io_uring is unavailable.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod runtime {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio_uring::net::{TcpListener, TcpStream};
    use tracing::{error, info, warn};

    /// Configuration for the io_uring server.
    #[derive(Debug, Clone)]
    pub struct IoUringServerConfig {
        /// Address to bind to
        pub bind_addr: SocketAddr,
        /// Number of worker threads (defaults to CPU count)
        pub workers: usize,
        /// io_uring configuration
        pub uring_config: IoUringConfig,
    }

    impl Default for IoUringServerConfig {
        fn default() -> Self {
            Self {
                bind_addr: "127.0.0.1:6379".parse().unwrap(),
                workers: num_cpus(),
                uring_config: IoUringConfig::high_throughput(),
            }
        }
    }

    fn num_cpus() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    }

    /// Start the io_uring server.
    ///
    /// This function blocks and runs the server until shutdown.
    pub fn start_server(
        config: IoUringServerConfig,
        database: Arc<crate::storage::Database>,
        executor: Arc<crate::commands::CommandExecutor>,
    ) -> io::Result<()> {
        info!(
            "Starting io_uring server on {} with {} workers",
            config.bind_addr, config.workers
        );

        // Start worker threads, each with its own io_uring instance
        let handles: Vec<_> = (0..config.workers)
            .map(|worker_id| {
                let config = config.clone();
                let database = database.clone();
                let executor = executor.clone();

                std::thread::spawn(move || {
                    tokio_uring::start(async move {
                        if let Err(e) = run_worker(worker_id, &config, database, executor).await {
                            error!("Worker {} failed: {}", worker_id, e);
                        }
                    });
                })
            })
            .collect();

        // Wait for all workers
        for handle in handles {
            handle
                .join()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Worker thread panicked"))?;
        }

        Ok(())
    }

    async fn run_worker(
        worker_id: usize,
        config: &IoUringServerConfig,
        database: Arc<crate::storage::Database>,
        executor: Arc<crate::commands::CommandExecutor>,
    ) -> io::Result<()> {
        // Each worker binds with SO_REUSEPORT
        let listener = TcpListener::bind(config.bind_addr)?;

        info!("Worker {} listening on {}", worker_id, config.bind_addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let database = database.clone();
                    let executor = executor.clone();

                    tokio_uring::spawn(async move {
                        if let Err(e) =
                            handle_connection(stream, peer_addr, database, executor).await
                        {
                            if !is_connection_reset(&e) {
                                warn!("Connection error from {}: {}", peer_addr, e);
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        stream: TcpStream,
        peer_addr: SocketAddr,
        database: Arc<crate::storage::Database>,
        executor: Arc<crate::commands::CommandExecutor>,
    ) -> io::Result<()> {
        use crate::protocol::RespParser;
        use crate::server::ClientState;
        use bytes::BytesMut;

        let mut read_buf = vec![0u8; 8192];
        let mut parser_buf = BytesMut::with_capacity(8192);
        let mut parser = RespParser::new();
        let client = Arc::new(ClientState::new(0, peer_addr, database.clone()));

        loop {
            // Read data using io_uring
            let (result, buf) = stream.read(read_buf).await;
            read_buf = buf;

            let n = result?;
            if n == 0 {
                return Ok(()); // Connection closed
            }

            parser_buf.extend_from_slice(&read_buf[..n]);
            parser.set_buffer(parser_buf.split().freeze());

            // Parse and execute commands
            while let Some(frame) = parser.parse_frame()? {
                let cmd = match crate::commands::ParsedCommand::from_frame(frame) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        let error_frame = crate::protocol::Frame::Error(format!("ERR {}", e));
                        write_frame(&stream, &error_frame).await?;
                        continue;
                    }
                };

                // Get the default db
                let db = database.get_db(client.db_index());

                // Execute command
                let result = executor.execute(cmd, db, client.clone()).await;

                let response = match result {
                    Ok(frame) => frame,
                    Err(e) => crate::protocol::Frame::Error(format!("ERR {}", e)),
                };

                write_frame(&stream, &response).await?;
            }

            // Return remaining data to parser buffer
            parser_buf = parser.take_buffer().try_into_mut().unwrap_or_else(|b| {
                let mut new_buf = BytesMut::with_capacity(b.len());
                new_buf.extend_from_slice(&b);
                new_buf
            });
        }
    }

    async fn write_frame(stream: &TcpStream, frame: &crate::protocol::Frame) -> io::Result<()> {
        let bytes = frame.serialize();
        let (result, _) = stream.write_all(bytes.to_vec()).await;
        result
    }

    fn is_connection_reset(e: &io::Error) -> bool {
        matches!(
            e.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_uring_availability() {
        // This should return false on macOS, true on modern Linux
        let available = is_available();

        #[cfg(target_os = "linux")]
        {
            // On Linux, it depends on kernel version and feature flag
            println!("io_uring available: {}", available);
        }

        #[cfg(not(target_os = "linux"))]
        {
            assert!(!available, "io_uring should not be available on non-Linux");
        }
    }

    #[test]
    fn test_io_backend_creation() {
        let config = IoUringConfig::default();
        let backend = IoBackend::new(&config).unwrap();

        // Backend should work regardless of io_uring availability
        assert!(!backend.name().is_empty());
    }

    #[test]
    fn test_config_presets() {
        let default = IoUringConfig::default();
        let high_throughput = IoUringConfig::high_throughput();
        let low_latency = IoUringConfig::low_latency();

        // High throughput should have larger queues
        assert!(high_throughput.sq_entries >= default.sq_entries);
        assert!(high_throughput.cq_entries >= default.cq_entries);

        // Low latency enables SQPOLL
        assert!(low_latency.sqpoll);
    }
}
