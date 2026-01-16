//! Soak testing infrastructure for long-running stability tests.
//!
//! Soak tests run for extended periods (hours/days) to catch:
//! - Memory leaks
//! - Connection leaks
//! - Gradual performance degradation
//! - Resource exhaustion
//!
//! # Running
//!
//! ```bash
//! # Start Viator with persistence
//! ./target/release/viator --port 6379 &
//!
//! # Run 1-hour soak test
//! SOAK_DURATION_SECS=3600 cargo test --test soak_test -- --ignored --nocapture
//!
//! # Run overnight (8 hours)
//! SOAK_DURATION_SECS=28800 cargo test --test soak_test -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Soak test configuration.
#[derive(Debug, Clone)]
pub struct SoakConfig {
    /// Server host
    pub host: String,
    /// Server port
    pub port: u16,
    /// Test duration
    pub duration: Duration,
    /// Number of concurrent workers
    pub workers: usize,
    /// Operations per worker per second (rate limiting)
    pub ops_per_second_per_worker: u32,
    /// Progress reporting interval
    pub report_interval: Duration,
}

impl Default for SoakConfig {
    fn default() -> Self {
        let duration_secs = std::env::var("SOAK_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60); // Default 1 minute for CI

        Self {
            host: std::env::var("SOAK_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("SOAK_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(6379),
            duration: Duration::from_secs(duration_secs),
            workers: std::env::var("SOAK_WORKERS")
                .ok()
                .and_then(|w| w.parse().ok())
                .unwrap_or(10),
            ops_per_second_per_worker: std::env::var("SOAK_OPS_PER_SEC")
                .ok()
                .and_then(|o| o.parse().ok())
                .unwrap_or(100),
            report_interval: Duration::from_secs(10),
        }
    }
}

/// Soak test metrics.
#[derive(Debug, Default)]
pub struct SoakMetrics {
    /// Total operations completed
    pub total_ops: AtomicU64,
    /// Successful operations
    pub success: AtomicU64,
    /// Failed operations
    pub failures: AtomicU64,
    /// Connection errors
    pub connection_errors: AtomicU64,
    /// Timeout errors
    pub timeouts: AtomicU64,
    /// Running flag
    pub running: AtomicBool,
}

impl SoakMetrics {
    pub fn new() -> Self {
        Self {
            running: AtomicBool::new(true),
            ..Default::default()
        }
    }

    pub fn record_success(&self) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn report(&self) -> String {
        let total = self.total_ops.load(Ordering::Relaxed);
        let success = self.success.load(Ordering::Relaxed);
        let failures = self.failures.load(Ordering::Relaxed);
        let conn_errors = self.connection_errors.load(Ordering::Relaxed);
        let timeouts = self.timeouts.load(Ordering::Relaxed);

        format!(
            "ops={} success={} failures={} conn_errors={} timeouts={}",
            total, success, failures, conn_errors, timeouts
        )
    }
}

/// Simple RESP client for soak testing.
pub struct SoakClient {
    stream: TcpStream,
    read_buf: Vec<u8>,
}

impl SoakClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self, std::io::Error> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr).await?;
        Ok(Self {
            stream,
            read_buf: vec![0u8; 4096],
        })
    }

    /// Send SET and verify OK response.
    pub async fn set(&mut self, key: &str, value: &str) -> Result<bool, std::io::Error> {
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            value.len(),
            value
        );

        self.stream.write_all(cmd.as_bytes()).await?;
        self.stream.flush().await?;

        // Read response
        let n = self.stream.read(&mut self.read_buf).await?;
        Ok(n > 0 && self.read_buf[0] == b'+')
    }

    /// Send GET and verify response.
    pub async fn get(&mut self, key: &str) -> Result<bool, std::io::Error> {
        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);

        self.stream.write_all(cmd.as_bytes()).await?;
        self.stream.flush().await?;

        // Read response (we just check we got something)
        let n = self.stream.read(&mut self.read_buf).await?;
        Ok(n > 0)
    }

    /// Send PING and verify PONG.
    pub async fn ping(&mut self) -> Result<bool, std::io::Error> {
        let cmd = b"*1\r\n$4\r\nPING\r\n";

        self.stream.write_all(cmd).await?;
        self.stream.flush().await?;

        let n = self.stream.read(&mut self.read_buf).await?;
        Ok(n > 0 && self.read_buf.starts_with(b"+PONG"))
    }

    /// Send INCR and verify response.
    pub async fn incr(&mut self, key: &str) -> Result<bool, std::io::Error> {
        let cmd = format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key);

        self.stream.write_all(cmd.as_bytes()).await?;
        self.stream.flush().await?;

        let n = self.stream.read(&mut self.read_buf).await?;
        Ok(n > 0 && self.read_buf[0] == b':')
    }
}

/// Run a single soak test worker.
async fn run_worker(worker_id: usize, config: Arc<SoakConfig>, metrics: Arc<SoakMetrics>) {
    let delay = Duration::from_micros(1_000_000 / config.ops_per_second_per_worker as u64);

    while metrics.is_running() {
        // Connect (or reconnect)
        let mut client = match SoakClient::connect(&config.host, config.port).await {
            Ok(c) => c,
            Err(_) => {
                metrics.record_connection_error();
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        // Run operations until error or stop
        let mut local_ops = 0u64;
        while metrics.is_running() {
            // Vary the operation type
            let op = local_ops % 4;
            let key = format!("soak:worker{}:key{}", worker_id, local_ops % 100);

            let result = match op {
                0 => client.set(&key, "test_value_12345678").await,
                1 => client.get(&key).await,
                2 => client.incr(&format!("soak:counter:{}", worker_id)).await,
                _ => client.ping().await,
            };

            match result {
                Ok(true) => metrics.record_success(),
                Ok(false) => metrics.record_failure(),
                Err(_) => {
                    metrics.record_failure();
                    break; // Reconnect
                }
            }

            local_ops += 1;
            tokio::time::sleep(delay).await;
        }
    }
}

/// Run a complete soak test.
pub async fn run_soak_test(config: SoakConfig) -> Arc<SoakMetrics> {
    let config = Arc::new(config);
    let metrics = Arc::new(SoakMetrics::new());
    let start = Instant::now();

    println!("Starting soak test:");
    println!("  Duration: {:?}", config.duration);
    println!("  Workers: {}", config.workers);
    println!("  Ops/sec/worker: {}", config.ops_per_second_per_worker);
    println!();

    // Spawn workers
    let mut handles = Vec::new();
    for worker_id in 0..config.workers {
        let config = config.clone();
        let metrics = metrics.clone();
        handles.push(tokio::spawn(async move {
            run_worker(worker_id, config, metrics).await;
        }));
    }

    // Progress reporter
    let report_metrics = metrics.clone();
    let report_config = config.clone();
    let reporter = tokio::spawn(async move {
        let mut interval = tokio::time::interval(report_config.report_interval);
        let start = Instant::now();

        while report_metrics.is_running() {
            interval.tick().await;
            let elapsed = start.elapsed();
            let total_ops = report_metrics.total_ops.load(Ordering::Relaxed);
            let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
            println!(
                "[{:?}] {} ops/sec={:.0}",
                elapsed,
                report_metrics.report(),
                ops_per_sec
            );
        }
    });

    // Wait for duration
    tokio::time::sleep(config.duration).await;

    // Stop all workers
    metrics.stop();

    // Wait for workers to finish
    for handle in handles {
        let _ = handle.await;
    }
    reporter.abort();

    let elapsed = start.elapsed();
    let total_ops = metrics.total_ops.load(Ordering::Relaxed);
    let success = metrics.success.load(Ordering::Relaxed);
    let failures = metrics.failures.load(Ordering::Relaxed);

    println!();
    println!("Soak test completed:");
    println!("  Duration: {:?}", elapsed);
    println!("  Total operations: {}", total_ops);
    println!(
        "  Successful: {} ({:.2}%)",
        success,
        100.0 * success as f64 / total_ops.max(1) as f64
    );
    println!(
        "  Failed: {} ({:.2}%)",
        failures,
        100.0 * failures as f64 / total_ops.max(1) as f64
    );
    println!(
        "  Ops/second: {:.0}",
        total_ops as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  Connection errors: {}",
        metrics.connection_errors.load(Ordering::Relaxed)
    );
    println!("  Timeouts: {}", metrics.timeouts.load(Ordering::Relaxed));

    metrics
}

// ==== Test Cases ====
// These tests are ignored by default because they require a running server.

#[tokio::test]
#[ignore]
async fn test_soak_basic() {
    let config = SoakConfig::default();
    let metrics = run_soak_test(config).await;

    // Basic success threshold - at least 99% success rate
    let success = metrics.success.load(Ordering::Relaxed);
    let total = metrics.total_ops.load(Ordering::Relaxed);

    assert!(total > 0, "No operations completed");
    let success_rate = success as f64 / total as f64;
    assert!(
        success_rate >= 0.99,
        "Success rate too low: {:.2}%",
        success_rate * 100.0
    );
}

#[tokio::test]
#[ignore]
async fn test_soak_high_concurrency() {
    let config = SoakConfig {
        workers: 50,
        duration: Duration::from_secs(30),
        ..SoakConfig::default()
    };

    let metrics = run_soak_test(config).await;

    let success = metrics.success.load(Ordering::Relaxed);
    let total = metrics.total_ops.load(Ordering::Relaxed);

    assert!(total > 0, "No operations completed");
    let success_rate = success as f64 / total as f64;
    assert!(
        success_rate >= 0.99,
        "Success rate too low: {:.2}%",
        success_rate * 100.0
    );
}

#[tokio::test]
#[ignore]
async fn test_soak_memory_stability() {
    // This test runs for longer to check memory doesn't grow unboundedly
    let config = SoakConfig {
        workers: 20,
        duration: Duration::from_secs(120), // 2 minutes
        ..SoakConfig::default()
    };

    // TODO: Add memory monitoring via INFO command
    // let initial_memory = get_server_memory();

    let metrics = run_soak_test(config).await;

    // let final_memory = get_server_memory();
    // assert!(final_memory < initial_memory * 2, "Memory grew too much");

    let success = metrics.success.load(Ordering::Relaxed);
    let total = metrics.total_ops.load(Ordering::Relaxed);
    let success_rate = success as f64 / total as f64;
    assert!(
        success_rate >= 0.99,
        "Success rate too low: {:.2}%",
        success_rate * 100.0
    );
}
