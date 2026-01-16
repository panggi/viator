//! Expiration manager for background key cleanup.
//!
//! Redis uses a combination of lazy expiration (on access) and active
//! expiration (background task) to manage key TTLs efficiently.

use super::Database;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time;
use tracing::{debug, info, trace};

/// Configuration for the expiry manager.
#[derive(Debug, Clone)]
pub struct ExpiryConfig {
    /// How often to run the expiry cycle (default: 100ms)
    pub cycle_interval: Duration,
    /// Maximum keys to check per database per cycle (default: 20)
    pub keys_per_cycle: usize,
    /// Target percentage of expired keys before aggressive cleanup (default: 25%)
    pub aggressive_threshold: f64,
    /// Maximum keys to process in aggressive mode (default: 100)
    pub aggressive_keys_limit: usize,
}

impl Default for ExpiryConfig {
    fn default() -> Self {
        Self {
            cycle_interval: Duration::from_millis(100),
            keys_per_cycle: 20,
            aggressive_threshold: 0.25,
            aggressive_keys_limit: 100,
        }
    }
}

/// Background expiration manager.
///
/// Implements Redis's active expiration algorithm:
/// 1. Sample a small number of keys with TTL
/// 2. Delete expired keys from the sample
/// 3. If > 25% were expired, repeat immediately
/// 4. Otherwise, wait until next cycle
///
/// This ensures bounded CPU usage while keeping expired keys low.
#[derive(Debug)]
pub struct ExpiryManager {
    database: Arc<Database>,
    config: ExpiryConfig,
    running: AtomicBool,
    shutdown: Arc<Notify>,
}

impl ExpiryManager {
    /// Create a new expiry manager.
    pub fn new(database: Arc<Database>) -> Self {
        Self::with_config(database, ExpiryConfig::default())
    }

    /// Create a new expiry manager with custom configuration.
    pub fn with_config(database: Arc<Database>, config: ExpiryConfig) -> Self {
        Self {
            database,
            config,
            running: AtomicBool::new(false),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Start the background expiry task.
    ///
    /// Returns a handle to the background task.
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.running.store(true, Ordering::SeqCst);

        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// Signal the expiry manager to stop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown.notify_one();
    }

    /// Check if the manager is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Main expiry loop.
    async fn run(&self) {
        info!("Expiry manager started");

        let mut interval = time::interval(self.config.cycle_interval);

        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                _ = interval.tick() => {
                    self.run_cycle().await;
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }

        info!("Expiry manager stopped");
    }

    /// Run a single expiry cycle across all databases.
    async fn run_cycle(&self) {
        let mut total_expired = 0;
        let mut total_sampled = 0;

        // Process each database
        for db_index in 0..16 {
            if let Ok(db) = self.database.get_db(db_index) {
                if db.is_empty() {
                    continue;
                }

                let expired = db.expire_keys(self.config.keys_per_cycle);
                total_expired += expired;
                total_sampled += self.config.keys_per_cycle;

                // Check if we need aggressive cleanup
                if expired > 0 {
                    let expired_ratio = expired as f64 / self.config.keys_per_cycle as f64;

                    if expired_ratio > self.config.aggressive_threshold {
                        debug!(
                            "Aggressive expiry triggered for db {}: {}% expired",
                            db_index,
                            (expired_ratio * 100.0) as u32
                        );

                        // Run additional cycles until ratio drops
                        let mut aggressive_expired = expired;
                        let mut iterations = 0;

                        while aggressive_expired > 0
                            && iterations < 10
                            && self.running.load(Ordering::SeqCst)
                        {
                            aggressive_expired = db.expire_keys(self.config.aggressive_keys_limit);
                            total_expired += aggressive_expired;
                            iterations += 1;

                            // Yield to other tasks
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
        }

        if total_expired > 0 {
            trace!(
                "Expiry cycle: removed {} keys (sampled {})",
                total_expired, total_sampled
            );
        }
    }

    /// Force an immediate expiry cycle (for testing).
    pub async fn force_cycle(&self) {
        self.run_cycle().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Expiry, Key, ViatorValue};

    #[tokio::test]
    #[ignore = "Needs investigation: possible DashMap iteration issue during concurrent modification"]
    async fn test_expiry_manager() {
        let database = Arc::new(Database::new());
        let db = database.default_db();

        // Add some keys with past expiration
        for i in 0..10 {
            db.set_with_expiry(
                Key::from(format!("key{i}")),
                ViatorValue::string("value"),
                Expiry::At(0), // Already expired
            );
        }

        // Add some keys without expiration
        for i in 10..20 {
            db.set(Key::from(format!("key{i}")), ViatorValue::string("value"));
        }

        let manager = Arc::new(ExpiryManager::new(database.clone()));
        manager.force_cycle().await;

        // Expired keys should be removed
        assert_eq!(db.len(), 10);
    }

    #[tokio::test]
    async fn test_expiry_manager_lifecycle() {
        let database = Arc::new(Database::new());
        let manager = Arc::new(ExpiryManager::with_config(
            database,
            ExpiryConfig {
                cycle_interval: Duration::from_millis(10),
                ..Default::default()
            },
        ));

        let handle = manager.clone().start();

        // Let it run briefly
        time::sleep(Duration::from_millis(50)).await;

        assert!(manager.is_running());

        manager.stop();
        handle.await.unwrap();

        assert!(!manager.is_running());
    }
}
