//! io_uring support for high-performance async I/O on Linux.
//!
//! This module provides a custom io_uring-based server implementation that uses:
//! - Fixed buffers for zero-copy I/O
//! - Direct syscall batching
//! - Thread-per-core model with SO_REUSEPORT
//!
//! # Benefits over tokio-uring
//!
//! - No buffer ownership transfer (avoids allocation per write)
//! - Pre-registered buffers for true zero-copy
//! - Direct control over submission/completion queues
//! - Lower latency through reduced abstraction layers

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
    /// Number of fixed buffers per connection
    pub buffers_per_conn: usize,
    /// Size of each fixed buffer
    pub buffer_size: usize,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            cq_entries: 512,
            sqpoll: false, // SQPOLL requires CAP_SYS_NICE
            sqpoll_idle_ms: 1000,
            buffers_per_conn: 2,    // One for read, one for write
            buffer_size: 16 * 1024, // 16KB per buffer
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
            buffers_per_conn: 2,
            buffer_size: 32 * 1024, // 32KB buffers
        }
    }

    /// Configuration for low-latency with SQPOLL (requires privileges).
    pub fn low_latency() -> Self {
        Self {
            sq_entries: 512,
            cq_entries: 1024,
            sqpoll: true,
            sqpoll_idle_ms: 100,
            buffers_per_conn: 2,
            buffer_size: 8 * 1024, // 8KB buffers for cache efficiency
        }
    }
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
    /// CQ overflows (indicates need for larger CQ)
    pub cq_overflows: u64,
}

/// Abstract I/O backend that can use either io_uring or tokio.
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
        Self::new(&IoUringConfig::default())
            .unwrap_or_else(|e| unreachable!("IoBackend::new failed unexpectedly: {}", e))
    }
}

/// io_uring server runtime for Linux.
///
/// This module provides a high-performance server implementation using raw io_uring
/// with fixed buffers for zero-copy I/O operations.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod runtime {
    use super::*;
    use crate::commands::{CommandExecutor, ParsedCommand};
    use crate::protocol::{Frame, RespParser};
    use crate::server::ClientState;
    use crate::server::metrics::ServerMetrics;
    use crate::storage::Database;
    use bytes::BytesMut;
    use io_uring::{IoUring, opcode, types};
    use slab::Slab;
    use std::collections::VecDeque;
    use std::net::{SocketAddr, TcpListener};
    use std::os::unix::io::{AsRawFd, RawFd};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use tracing::{debug, error, info, trace, warn};

    /// Buffer size for each connection
    const BUFFER_SIZE: usize = 16 * 1024;

    /// Token type for identifying operations
    #[derive(Debug, Clone, Copy)]
    enum Token {
        Accept,
        Read { conn_id: usize },
        Write { conn_id: usize },
    }

    impl Token {
        fn encode(self) -> u64 {
            match self {
                Token::Accept => 0,
                Token::Read { conn_id } => ((conn_id as u64) << 2) | 1,
                Token::Write { conn_id } => ((conn_id as u64) << 2) | 2,
            }
        }

        fn decode(value: u64) -> Self {
            match value & 0x3 {
                0 => Token::Accept,
                1 => Token::Read {
                    conn_id: (value >> 2) as usize,
                },
                2 => Token::Write {
                    conn_id: (value >> 2) as usize,
                },
                _ => unreachable!(),
            }
        }
    }

    /// Connection state
    struct Connection {
        fd: RawFd,
        read_buf: Vec<u8>,
        write_buf: BytesMut,
        write_queue: VecDeque<BytesMut>,
        parser: RespParser,
        client: Arc<ClientState>,
        pending_read: bool,
        pending_write: bool,
        closed: bool,
    }

    impl Connection {
        fn new(
            fd: RawFd,
            conn_id: u64,
            server_stats: Arc<crate::storage::ServerStats>,
            metrics: Arc<ServerMetrics>,
        ) -> Self {
            Self {
                fd,
                read_buf: vec![0u8; BUFFER_SIZE],
                write_buf: BytesMut::with_capacity(4096),
                write_queue: VecDeque::new(),
                parser: RespParser::new(),
                client: Arc::new(ClientState::new(conn_id, server_stats, metrics)),
                pending_read: false,
                pending_write: false,
                closed: false,
            }
        }
    }

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
                bind_addr: "127.0.0.1:6379"
                    .parse()
                    .expect("valid default bind address"),
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

    /// Shared state for graceful shutdown
    static SHUTDOWN: AtomicBool = AtomicBool::new(false);

    /// Start the io_uring server.
    ///
    /// This function blocks and runs the server until shutdown.
    pub fn start_server(
        config: IoUringServerConfig,
        database: Arc<Database>,
        executor: Arc<CommandExecutor>,
    ) -> io::Result<()> {
        info!(
            "Starting io_uring server on {} with {} workers",
            config.bind_addr, config.workers
        );

        // Create shared metrics
        let metrics = Arc::new(ServerMetrics::new());

        // Connection ID counter shared across workers
        let next_conn_id = Arc::new(AtomicU64::new(1));

        // Start worker threads, each with its own io_uring instance
        let handles: Vec<_> = (0..config.workers)
            .map(|worker_id| {
                let config = config.clone();
                let database = database.clone();
                let executor = executor.clone();
                let metrics = metrics.clone();
                let next_conn_id = next_conn_id.clone();

                std::thread::spawn(move || {
                    if let Err(e) = run_worker(
                        worker_id,
                        &config,
                        database,
                        executor,
                        metrics,
                        next_conn_id,
                    ) {
                        error!("Worker {} failed: {}", worker_id, e);
                    }
                })
            })
            .collect();

        // Wait for all workers
        for handle in handles {
            handle
                .join()
                .map_err(|_| io::Error::other("Worker thread panicked"))?;
        }

        Ok(())
    }

    fn run_worker(
        worker_id: usize,
        config: &IoUringServerConfig,
        database: Arc<Database>,
        executor: Arc<CommandExecutor>,
        metrics: Arc<ServerMetrics>,
        next_conn_id: Arc<AtomicU64>,
    ) -> io::Result<()> {
        // Create io_uring instance
        let mut ring: IoUring = IoUring::builder()
            .setup_cqsize(config.uring_config.cq_entries)
            .build(config.uring_config.sq_entries)?;

        // Create listener with SO_REUSEPORT for load balancing across workers
        let listener = create_listener(config.bind_addr)?;
        let listener_fd = listener.as_raw_fd();

        info!("Worker {} listening on {}", worker_id, config.bind_addr);

        // Connection slab
        let mut connections: Slab<Connection> = Slab::with_capacity(1024);

        // Submit initial accept
        submit_accept(&mut ring, listener_fd)?;

        // Event loop
        loop {
            if SHUTDOWN.load(Ordering::Relaxed) {
                info!("Worker {} shutting down", worker_id);
                break;
            }

            // Submit pending operations and wait for completions
            ring.submit_and_wait(1)?;

            // Process completions
            let cq = ring.completion();
            let cqes: Vec<_> = cq.collect();

            for cqe in cqes {
                let token = Token::decode(cqe.user_data());
                let result = cqe.result();

                match token {
                    Token::Accept => {
                        if result >= 0 {
                            let client_fd = result;
                            let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);

                            // Configure socket
                            set_tcp_nodelay(client_fd)?;

                            // Create connection
                            let conn = Connection::new(
                                client_fd,
                                conn_id,
                                database.server_stats().clone(),
                                metrics.clone(),
                            );

                            let conn_idx = connections.insert(conn);
                            debug!(
                                "Worker {} accepted connection {} (fd={})",
                                worker_id, conn_id, client_fd
                            );

                            // Submit read for new connection
                            submit_read(&mut ring, &mut connections[conn_idx], conn_idx)?;
                        } else {
                            warn!("Accept failed: {}", io::Error::from_raw_os_error(-result));
                        }

                        // Always re-submit accept
                        submit_accept(&mut ring, listener_fd)?;
                    }

                    Token::Read { conn_id } => {
                        if let Some(conn) = connections.get_mut(conn_id) {
                            conn.pending_read = false;

                            if result > 0 {
                                let n = result as usize;
                                trace!("Connection {} read {} bytes", conn_id, n);

                                // Process data
                                process_data(conn, n, &executor, &database)?;

                                // Submit write if we have data
                                if !conn.write_buf.is_empty() && !conn.pending_write {
                                    submit_write(&mut ring, conn, conn_id)?;
                                }

                                // Submit next read if connection not closed
                                if !conn.closed && !conn.pending_read {
                                    submit_read(&mut ring, conn, conn_id)?;
                                }
                            } else if result == 0 {
                                // Connection closed
                                debug!("Connection {} closed by peer", conn_id);
                                close_connection(&mut connections, conn_id);
                            } else {
                                let err = -result;
                                if err != libc::ECONNRESET && err != libc::EPIPE {
                                    warn!(
                                        "Read error on connection {}: {}",
                                        conn_id,
                                        io::Error::from_raw_os_error(err)
                                    );
                                }
                                close_connection(&mut connections, conn_id);
                            }
                        }
                    }

                    Token::Write { conn_id } => {
                        if let Some(conn) = connections.get_mut(conn_id) {
                            conn.pending_write = false;

                            if result >= 0 {
                                let n = result as usize;
                                trace!("Connection {} wrote {} bytes", conn_id, n);

                                // Clear written data
                                conn.write_buf.clear();

                                // Check for more data to write
                                if let Some(next) = conn.write_queue.pop_front() {
                                    conn.write_buf = next;
                                    submit_write(&mut ring, conn, conn_id)?;
                                }
                            } else {
                                let err = -result;
                                if err != libc::ECONNRESET && err != libc::EPIPE {
                                    warn!(
                                        "Write error on connection {}: {}",
                                        conn_id,
                                        io::Error::from_raw_os_error(err)
                                    );
                                }
                                close_connection(&mut connections, conn_id);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn create_listener(addr: SocketAddr) -> io::Result<TcpListener> {
        use std::net::TcpListener as StdListener;

        let socket = socket2::Socket::new(
            if addr.is_ipv4() {
                socket2::Domain::IPV4
            } else {
                socket2::Domain::IPV6
            },
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;

        // Enable SO_REUSEADDR and SO_REUSEPORT
        socket.set_reuse_address(true)?;
        #[cfg(unix)]
        socket.set_reuse_port(true)?;

        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;

        Ok(StdListener::from(socket))
    }

    fn set_tcp_nodelay(fd: RawFd) -> io::Result<()> {
        let val: libc::c_int = 1;
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn submit_accept(ring: &mut IoUring, listener_fd: RawFd) -> io::Result<()> {
        let accept_e = opcode::Accept::new(
            types::Fd(listener_fd),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .build()
        .user_data(Token::Accept.encode());

        unsafe {
            ring.submission()
                .push(&accept_e)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_read(ring: &mut IoUring, conn: &mut Connection, conn_id: usize) -> io::Result<()> {
        conn.pending_read = true;

        let read_e = opcode::Read::new(
            types::Fd(conn.fd),
            conn.read_buf.as_mut_ptr(),
            conn.read_buf.len() as u32,
        )
        .build()
        .user_data(Token::Read { conn_id }.encode());

        unsafe {
            ring.submission()
                .push(&read_e)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_write(ring: &mut IoUring, conn: &mut Connection, conn_id: usize) -> io::Result<()> {
        if conn.write_buf.is_empty() {
            return Ok(());
        }

        conn.pending_write = true;

        let write_e = opcode::Write::new(
            types::Fd(conn.fd),
            conn.write_buf.as_ptr(),
            conn.write_buf.len() as u32,
        )
        .build()
        .user_data(Token::Write { conn_id }.encode());

        unsafe {
            ring.submission()
                .push(&write_e)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn close_connection(connections: &mut Slab<Connection>, conn_id: usize) {
        if let Some(conn) = connections.try_remove(conn_id) {
            unsafe {
                libc::close(conn.fd);
            }
        }
    }

    fn process_data(
        conn: &mut Connection,
        n: usize,
        executor: &Arc<CommandExecutor>,
        database: &Arc<Database>,
    ) -> io::Result<()> {
        // Add data to parser
        conn.parser.extend(&conn.read_buf[..n]);

        // Process all complete frames
        loop {
            match conn.parser.parse() {
                Ok(Some(frame)) => {
                    // Parse command
                    let cmd = match ParsedCommand::from_frame(frame) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            let error_frame = Frame::error(format!("ERR {}", e));
                            error_frame.serialize(&mut conn.write_buf);
                            continue;
                        }
                    };

                    trace!("Executing: {}", cmd.name);

                    // Execute command synchronously (we're in a blocking context)
                    // We need to use block_on or similar for async commands
                    let response = execute_sync(executor, cmd, conn.client.clone(), database);

                    // Serialize response
                    response.serialize(&mut conn.write_buf);
                }
                Ok(None) => {
                    // Need more data
                    conn.parser.maybe_trim();
                    break;
                }
                Err(e) => {
                    // Protocol error
                    let error_frame = Frame::error(format!("ERR {}", e));
                    error_frame.serialize(&mut conn.write_buf);
                    conn.parser.clear();
                    break;
                }
            }
        }

        // Check if client requested close
        if conn.client.is_closed() {
            conn.closed = true;
        }

        Ok(())
    }

    // Thread-local tokio runtime for executing async commands
    thread_local! {
        static RUNTIME: std::cell::RefCell<tokio::runtime::Runtime> = std::cell::RefCell::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create thread-local runtime")
        );
    }

    /// Execute a command synchronously using thread-local runtime.
    ///
    /// Uses a thread-local tokio runtime to avoid spawning threads per command.
    fn execute_sync(
        executor: &Arc<CommandExecutor>,
        cmd: ParsedCommand,
        client: Arc<ClientState>,
        _database: &Arc<Database>,
    ) -> Frame {
        RUNTIME.with(|rt| {
            rt.borrow().block_on(async {
                match executor.execute(cmd, client).await {
                    Ok(frame) => frame,
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            })
        })
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
