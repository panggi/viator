//! Viator server implementation.
//!
//! This module provides the async TCP server, connection handling,
//! and configuration.

pub mod cluster;
pub mod cluster_bus;
pub mod config;
mod connection;
pub mod io_uring;
pub mod metrics;
pub mod pubsub;
pub mod rate_limit;
pub mod replication;
pub mod sentinel;
mod state;
pub mod watch;

pub use cluster::{ClusterManager, ClusterState, SharedClusterManager, Slot, SlotRange};
pub use cluster_bus::{ClusterBus, ClusterBusConfig, SharedClusterBus};
pub use config::{Config, ConfigError, LogLevel};
pub use connection::Connection;
pub use metrics::ServerMetrics;
pub use pubsub::{PubSubHub, SharedPubSubHub};
pub use rate_limit::{ConnectionRateLimiter, RateLimitConfig, RateLimitStats};
pub use replication::{ReplicationManager, ReplicationRole, SharedReplicationManager};
pub use sentinel::SentinelMonitor;
pub use state::ClientState;
pub use watch::{SharedWatchRegistry, WatchRegistry};

use crate::Result;
use crate::commands::CommandExecutor;
use crate::persistence::VdbSaver;
use crate::pool::BufferPool;
use crate::storage::{Database, ExpiryManager};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore};
use tracing::{error, info, warn};

/// The main Redis server.
#[derive(Debug)]
pub struct Server {
    /// Server configuration
    config: Config,
    /// Database
    database: Arc<Database>,
    /// Command executor
    executor: Arc<CommandExecutor>,
    /// Expiry manager
    expiry_manager: Arc<ExpiryManager>,
    /// Pub/Sub hub
    pubsub: SharedPubSubHub,
    /// Server metrics for performance monitoring
    metrics: Arc<ServerMetrics>,
    /// Running flag
    running: AtomicBool,
    /// Shutdown notification
    shutdown: Arc<Notify>,
    /// Connection counter
    connection_count: AtomicU64,
    /// Total connections
    total_connections: AtomicU64,
    /// Rate limiter for connections
    rate_limiter: ConnectionRateLimiter,
    /// Buffer pool for connection buffers
    buffer_pool: Arc<BufferPool>,
    /// Rate limit statistics
    rate_limit_stats: Arc<RateLimitStats>,
    /// Connection semaphore for backpressure (limits concurrent connections)
    connection_semaphore: Arc<Semaphore>,
}

impl Server {
    /// Create a new server with the given configuration.
    pub fn new(config: Config) -> Self {
        let database = Arc::new(Database::with_config(
            config.requirepass.clone(),
            config.maxmemory,
            config.maxmemory_policy,
        ));
        let pubsub = Arc::new(PubSubHub::new());
        let executor = Arc::new(CommandExecutor::with_pubsub(
            database.clone(),
            pubsub.clone(),
        ));
        let expiry_manager = Arc::new(ExpiryManager::new(database.clone()));
        let metrics = Arc::new(ServerMetrics::new());
        let rate_limiter = ConnectionRateLimiter::new(RateLimitConfig::default());
        let buffer_pool = Arc::new(BufferPool::new());
        let rate_limit_stats = Arc::new(RateLimitStats::default());
        let connection_semaphore = Arc::new(Semaphore::new(config.max_clients));

        Self {
            config,
            database,
            executor,
            expiry_manager,
            pubsub,
            metrics,
            running: AtomicBool::new(false),
            shutdown: Arc::new(Notify::new()),
            connection_count: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            rate_limiter,
            buffer_pool,
            rate_limit_stats,
            connection_semaphore,
        }
    }

    /// Run the server.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let addr: SocketAddr = format!("{}:{}", self.config.bind, self.config.port).parse()?;

        let listener = TcpListener::bind(addr).await?;
        info!("Viator server listening on {}", addr);

        self.running.store(true, Ordering::SeqCst);

        // Start expiry manager
        let expiry_handle = self.expiry_manager.clone().start();

        // Start rate limiter cleanup task
        let rate_cleanup_server = self.clone();
        let rate_cleanup_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if !rate_cleanup_server.running.load(Ordering::Relaxed) {
                    break;
                }
                rate_cleanup_server.rate_limiter.cleanup();
            }
        });

        // Interval for checking SHUTDOWN command request
        let mut shutdown_check_interval = tokio::time::interval(Duration::from_millis(100));
        shutdown_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, peer_addr)) => {
                            // Configure socket for low-latency
                            // TCP_NODELAY disables Nagle's algorithm for immediate sends
                            if let Err(e) = socket.set_nodelay(true) {
                                warn!("Failed to set TCP_NODELAY: {}", e);
                            }

                            // Check rate limit first
                            if !self.rate_limiter.allow_connection(peer_addr.ip()) {
                                warn!("Rate limited connection from {}", peer_addr);
                                self.rate_limit_stats.record_connection_rejected();
                                self.metrics.connection_rejected();
                                continue;
                            }
                            self.rate_limit_stats.record_connection_allowed();

                            // Acquire connection permit (provides backpressure)
                            let permit = match self.connection_semaphore.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    warn!("Max clients reached, rejecting connection from {}", peer_addr);
                                    self.metrics.connection_rejected();
                                    continue;
                                }
                            };

                            self.connection_count.fetch_add(1, Ordering::Relaxed);
                            self.total_connections.fetch_add(1, Ordering::Relaxed);
                            self.metrics.connection_opened();
                            self.database.connection_opened(); // Track in Database for INFO command

                            let server = self.clone();
                            let conn_id = self.total_connections.load(Ordering::Relaxed);

                            tokio::spawn(async move {
                                // permit is held for the duration of the connection
                                let _permit = permit;

                                let mut connection = Connection::new(
                                    socket,
                                    peer_addr,
                                    conn_id,
                                    server.executor.clone(),
                                    server.database.clone(),
                                    server.metrics.clone(),
                                    server.buffer_pool.clone(),
                                    server.config.timeout,
                                );

                                if let Err(e) = connection.run().await {
                                    error!("Connection error from {}: {}", peer_addr, e);
                                    server.metrics.record_error();
                                }

                                server.connection_count.fetch_sub(1, Ordering::Relaxed);
                                server.metrics.connection_closed();
                                server.database.connection_closed(); // Track in Database for INFO command
                                // _permit is dropped here, releasing the semaphore slot
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = self.shutdown.notified() => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = shutdown_check_interval.tick() => {
                    // Check if SHUTDOWN command was issued
                    if self.database.is_shutdown_requested() {
                        info!("SHUTDOWN command received");
                        break;
                    }
                }
            }
        }

        // Graceful shutdown: wait for connections with timeout
        info!(
            "Waiting for {} active connections to close...",
            self.connection_count.load(Ordering::Relaxed)
        );

        let shutdown_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();
        while self.connection_count.load(Ordering::Relaxed) > 0 {
            if start.elapsed() > shutdown_timeout {
                warn!(
                    "Shutdown timeout reached, {} connections still active",
                    self.connection_count.load(Ordering::Relaxed)
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Stop background tasks
        self.expiry_manager.stop();
        expiry_handle.await.ok();
        rate_cleanup_handle.abort();

        // Save final VDB snapshot before exiting (unless SHUTDOWN NOSAVE was used)
        let should_save = !self.config.save.is_empty() && self.database.should_save_on_shutdown();
        if should_save {
            info!("Saving the final VDB snapshot before exiting (do not interrupt!)");
            let vdb_path = std::path::Path::new(&self.config.dir).join(&self.config.dbfilename);

            // Spawn blocking task to ensure save completes even if signals arrive
            let db_clone = self.database.clone();
            let save_result = tokio::task::spawn_blocking(move || match VdbSaver::new(&vdb_path) {
                Ok(saver) => saver.save(&db_clone),
                Err(e) => Err(e),
            })
            .await;

            match save_result {
                Ok(Ok(())) => {
                    info!("DB saved on disk");
                }
                Ok(Err(e)) => {
                    error!("Failed to save VDB on shutdown: {}", e);
                }
                Err(e) => {
                    error!("VDB save task panicked: {}", e);
                }
            }
        } else if !self.config.save.is_empty() {
            info!("Skipping VDB save (SHUTDOWN NOSAVE)");
        }

        self.running.store(false, Ordering::SeqCst);
        info!("Server shutdown complete");

        Ok(())
    }

    /// Signal the server to shutdown.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Check if the server is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get the current connection count.
    pub fn connection_count(&self) -> u64 {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// Get the total connections since startup.
    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Get the database.
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }

    /// Get the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the server metrics.
    pub fn metrics(&self) -> &Arc<ServerMetrics> {
        &self.metrics
    }
}
