//! Viator server implementation.
//!
//! This module provides the async TCP server, connection handling,
//! and configuration.

pub mod cluster;
pub mod cluster_bus;
pub mod config;
mod connection;
pub mod latency;
pub mod metrics;
pub mod pubsub;
pub mod rate_limit;
pub mod replication;
pub mod sentinel;
mod state;
pub mod tracking;
pub mod watch;

pub use cluster::{ClusterManager, ClusterState, SharedClusterManager, Slot, SlotRange};
pub use cluster_bus::{ClusterBus, ClusterBusConfig, SharedClusterBus};
pub use config::{Config, ConfigError, LogLevel};
pub use connection::Connection;
pub use latency::{LatencyEvent, LatencyGuard, LatencyMonitor, LatencySample, SharedLatencyMonitor};
pub use metrics::ServerMetrics;
pub use pubsub::{PubSubHub, SharedPubSubHub};
pub use rate_limit::{ConnectionRateLimiter, RateLimitConfig, RateLimitStats};
pub use replication::{ReplicationManager, ReplicationRole, SharedReplicationManager};
pub use sentinel::SentinelMonitor;
pub use state::{ClientState, TrackingState};
pub use tracking::{SharedTrackingRegistry, TrackingRegistry};
pub use watch::{SharedWatchRegistry, WatchRegistry};

use crate::Result;
use crate::commands::CommandExecutor;
use crate::persistence::{VdbLoadResult, VdbLoader, VdbSaveResult, VdbSaver};
use crate::pool::BufferPool;
use crate::storage::{Database, ExpiryManager};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
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

    /// Log VDB save result in Redis-style format.
    fn log_vdb_save_result(result: &VdbSaveResult) {
        info!(
            "SAVE done, {} keys saved, {} keys skipped, {} bytes written.",
            result.keys_saved, result.keys_skipped, result.bytes_written
        );
        info!("DB saved on disk");
    }

    /// Log VDB load result in Redis-style format.
    fn log_vdb_load_result(result: &VdbLoadResult, duration: Duration) {
        let version = if result.viator_version.is_empty() {
            "unknown".to_string()
        } else {
            result.viator_version.clone()
        };
        info!("Loading VDB produced by version {}", version);

        // Calculate VDB age
        if result.ctime > 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let age = now - result.ctime;
            info!("VDB age {} seconds", age);
        }

        info!(
            "Done loading VDB, keys loaded: {}, keys expired: {}.",
            result.keys_loaded, result.keys_expired
        );
        info!("DB loaded from disk: {:.3} seconds", duration.as_secs_f64());
    }

    /// Run the server.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Server initialized");

        // Load VDB on startup if it exists
        let vdb_path = std::path::Path::new(&self.config.dir).join(&self.config.dbfilename);
        if vdb_path.exists() {
            let load_start = Instant::now();
            match VdbLoader::new(&vdb_path) {
                Ok(loader) => match loader.load_into(&self.database) {
                    Ok(result) => {
                        let load_duration = load_start.elapsed();
                        Self::log_vdb_load_result(&result, load_duration);
                    }
                    Err(e) => error!("Failed to load VDB: {}", e),
                },
                Err(e) => error!("Failed to open VDB file: {}", e),
            }
        }

        let addr: SocketAddr = format!("{}:{}", self.config.bind, self.config.port).parse()?;

        let listener = TcpListener::bind(addr).await?;
        info!("Ready to accept connections tcp");

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

        // Start background save check task (like Redis serverCron)
        let save_check_server = self.clone();
        let save_check_handle = tokio::spawn(async move {
            // Check save conditions every second (Redis uses 100ms in serverCron)
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                if !save_check_server.running.load(Ordering::Relaxed) {
                    break;
                }

                // Skip if save is disabled or already in progress
                if save_check_server.config.save.is_empty() {
                    continue;
                }
                if save_check_server
                    .database
                    .server_stats()
                    .vdb_bgsave_in_progress
                    .load(Ordering::Relaxed)
                {
                    continue;
                }

                // Get current stats
                let changes = save_check_server
                    .database
                    .server_stats()
                    .vdb_changes_since_save
                    .load(Ordering::Relaxed);
                let last_save = save_check_server
                    .database
                    .server_stats()
                    .last_vdb_save_time
                    .load(Ordering::Relaxed);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let seconds_since_save = now.saturating_sub(last_save);

                // Check each save point (already sorted by most urgent in config)
                let mut should_save = false;
                let mut trigger_seconds = 0u64;
                let mut trigger_changes = 0u64;

                for save_config in &save_check_server.config.save {
                    if changes >= save_config.changes && seconds_since_save >= save_config.seconds {
                        should_save = true;
                        trigger_seconds = save_config.seconds;
                        trigger_changes = save_config.changes;
                        break;
                    }
                }

                if should_save {
                    info!(
                        "{} changes in {} seconds. Saving...",
                        trigger_changes, trigger_seconds
                    );

                    // Mark save in progress
                    save_check_server.database.server_stats().vdb_save_started();
                    info!("Background saving started");

                    // Spawn the actual save in a blocking task
                    let db_for_save = save_check_server.database.clone();
                    let db_for_stats = save_check_server.database.clone();
                    let vdb_path = std::path::Path::new(&save_check_server.config.dir)
                        .join(&save_check_server.config.dbfilename);

                    tokio::spawn(async move {
                        let save_result =
                            tokio::task::spawn_blocking(move || match VdbSaver::new(&vdb_path) {
                                Ok(saver) => saver.save(&db_for_save),
                                Err(e) => Err(e),
                            })
                            .await;

                        match save_result {
                            Ok(Ok(result)) => {
                                db_for_stats.server_stats().vdb_save_completed();
                                info!("Background saving terminated with success");
                                info!(
                                    "BGSAVE done, {} keys saved, {} keys skipped, {} bytes written.",
                                    result.keys_saved, result.keys_skipped, result.bytes_written
                                );
                            }
                            Ok(Err(e)) => {
                                // Mark save as no longer in progress even on error
                                db_for_stats
                                    .server_stats()
                                    .vdb_bgsave_in_progress
                                    .store(false, Ordering::Relaxed);
                                error!("Background saving terminated with error: {}", e);
                            }
                            Err(e) => {
                                db_for_stats
                                    .server_stats()
                                    .vdb_bgsave_in_progress
                                    .store(false, Ordering::Relaxed);
                                error!("Background save task panicked: {}", e);
                            }
                        }
                    });
                }
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
        let start = Instant::now();
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
        save_check_handle.abort();

        // Save final VDB snapshot before exiting (unless SHUTDOWN NOSAVE was used)
        let should_save = !self.config.save.is_empty() && self.database.should_save_on_shutdown();
        if should_save {
            info!("User requested shutdown...");
            info!("Saving the final VDB snapshot before exiting.");
            let vdb_path = std::path::Path::new(&self.config.dir).join(&self.config.dbfilename);

            // Spawn blocking task to ensure save completes even if signals arrive
            let db_clone = self.database.clone();
            let save_result = tokio::task::spawn_blocking(move || match VdbSaver::new(&vdb_path) {
                Ok(saver) => saver.save(&db_clone),
                Err(e) => Err(e),
            })
            .await;

            match save_result {
                Ok(Ok(result)) => {
                    Self::log_vdb_save_result(&result);
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
        info!("Viator is now ready to exit, bye bye...");

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
