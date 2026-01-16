//! High-load integration tests for redis-rs.
//!
//! These tests verify behavior under extreme conditions.

#![allow(dead_code, clippy::unused_io_amount)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;

/// Simple RESP protocol helpers for testing
mod resp {

    pub fn set_cmd(key: &str, value: &str) -> Vec<u8> {
        format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            value.len(),
            value
        )
        .into_bytes()
    }

    pub fn get_cmd(key: &str) -> Vec<u8> {
        format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
    }

    pub fn ping_cmd() -> Vec<u8> {
        b"*1\r\n$4\r\nPING\r\n".to_vec()
    }

    pub fn pipeline(cmds: &[Vec<u8>]) -> Vec<u8> {
        cmds.concat()
    }
}

/// Test configuration
struct LoadTestConfig {
    host: String,
    port: u16,
    num_connections: usize,
    requests_per_conn: usize,
    pipeline_depth: usize,
    value_size: usize,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: std::env::var("REDIS_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(6379),
            num_connections: 100,
            requests_per_conn: 1000,
            pipeline_depth: 1,
            value_size: 64,
        }
    }
}

/// Results from a load test
#[derive(Debug, Default)]
struct LoadTestResults {
    total_requests: u64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_bytes_sent: AtomicU64,
    total_bytes_received: AtomicU64,
    duration: Duration,
}

impl LoadTestResults {
    fn ops_per_second(&self) -> f64 {
        let successful = self.successful_requests.load(Ordering::Relaxed);
        successful as f64 / self.duration.as_secs_f64()
    }

    fn throughput_mbps(&self) -> f64 {
        let bytes = self.total_bytes_sent.load(Ordering::Relaxed)
            + self.total_bytes_received.load(Ordering::Relaxed);
        (bytes as f64 / 1_000_000.0) / self.duration.as_secs_f64()
    }
}

/// Run a single connection workload
async fn run_connection_workload(
    config: &LoadTestConfig,
    conn_id: usize,
    barrier: Arc<Barrier>,
    results: Arc<LoadTestResults>,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.host, config.port);
    let mut stream = TcpStream::connect(&addr).await?;

    // Wait for all connections to be ready
    barrier.wait().await;

    let value = "x".repeat(config.value_size);
    let mut read_buf = vec![0u8; 4096];

    for i in 0..config.requests_per_conn {
        let key = format!("key:{conn_id}:{i}");

        // Build pipeline
        let mut cmds = Vec::with_capacity(config.pipeline_depth * 2);
        for j in 0..config.pipeline_depth {
            let pkey = format!("{key}:{j}");
            cmds.push(resp::set_cmd(&pkey, &value));
            cmds.push(resp::get_cmd(&pkey));
        }
        let pipeline = resp::pipeline(&cmds);

        // Send
        results
            .total_bytes_sent
            .fetch_add(pipeline.len() as u64, Ordering::Relaxed);
        stream.write_all(&pipeline).await?;

        // Read responses (simplified - just drain the buffer)
        let n = stream.read(&mut read_buf).await?;
        results
            .total_bytes_received
            .fetch_add(n as u64, Ordering::Relaxed);
        results
            .successful_requests
            .fetch_add((config.pipeline_depth * 2) as u64, Ordering::Relaxed);
    }

    Ok(())
}

/// High-throughput test with many concurrent connections
#[tokio::test]
#[ignore] // Run manually with: cargo test high_throughput -- --ignored
async fn high_throughput_test() {
    let config = LoadTestConfig {
        num_connections: 100,
        requests_per_conn: 10_000,
        pipeline_depth: 16,
        value_size: 64,
        ..Default::default()
    };

    let results = Arc::new(LoadTestResults {
        total_requests: (config.num_connections
            * config.requests_per_conn
            * config.pipeline_depth
            * 2) as u64,
        ..Default::default()
    });

    let barrier = Arc::new(Barrier::new(config.num_connections));
    let start = Instant::now();

    let config = Arc::new(config);
    let handles: Vec<_> = (0..config.num_connections)
        .map(|conn_id| {
            let config_ref = Arc::clone(&config);
            let barrier = barrier.clone();
            let results = results.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    run_connection_workload(&config_ref, conn_id, barrier, results).await
                {
                    eprintln!("Connection {conn_id} error: {e}");
                }
            })
        })
        .collect();

    // Wait for all to complete
    for handle in handles {
        handle.await.ok();
    }

    let duration = start.elapsed();

    println!("\n=== High Throughput Test Results ===");
    println!("Connections: {}", config.num_connections);
    println!("Requests/conn: {}", config.requests_per_conn);
    println!("Pipeline depth: {}", config.pipeline_depth);
    println!("Duration: {duration:?}");
    println!("Ops/sec: {:.0}", results.ops_per_second());
    println!("Throughput: {:.2} MB/s", results.throughput_mbps());
    println!(
        "Success rate: {:.2}%",
        results.successful_requests.load(Ordering::Relaxed) as f64 / results.total_requests as f64
            * 100.0
    );
}

/// Connection storm test - many connections opening/closing rapidly
#[tokio::test]
#[ignore]
async fn connection_storm_test() {
    let host = "127.0.0.1";
    let port: u16 = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);

    let num_iterations = 1000;
    let concurrent_connections = 100;

    let successful = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    for _ in 0..num_iterations {
        let handles: Vec<_> = (0..concurrent_connections)
            .map(|_| {
                let successful = successful.clone();
                let failed = failed.clone();
                tokio::spawn(async move {
                    let addr = format!("{host}:{port}");
                    match TcpStream::connect(&addr).await {
                        Ok(mut stream) => {
                            // Send PING, expect PONG
                            if stream.write_all(&resp::ping_cmd()).await.is_ok() {
                                let mut buf = [0u8; 32];
                                if stream.read(&mut buf).await.is_ok() {
                                    successful.fetch_add(1, Ordering::Relaxed);
                                    return;
                                }
                            }
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.ok();
        }
    }

    let duration = start.elapsed();
    let total = num_iterations * concurrent_connections;
    let success = successful.load(Ordering::Relaxed);

    println!("\n=== Connection Storm Test Results ===");
    println!("Total connections: {total}");
    println!("Successful: {success}");
    println!("Failed: {}", failed.load(Ordering::Relaxed));
    println!("Duration: {duration:?}");
    println!(
        "Connections/sec: {:.0}",
        f64::from(total) / duration.as_secs_f64()
    );
    println!(
        "Success rate: {:.2}%",
        success as f64 / f64::from(total) * 100.0
    );

    assert!(
        success as f64 / f64::from(total) > 0.99,
        "Success rate too low!"
    );
}

/// Large value test - test with MB-sized values
#[tokio::test]
#[ignore]
async fn large_value_test() {
    let host = "127.0.0.1";
    let port: u16 = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);

    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr).await.expect("Failed to connect");

    // Test with increasingly large values
    let sizes = [1024, 10 * 1024, 100 * 1024, 1024 * 1024]; // 1KB, 10KB, 100KB, 1MB

    for size in sizes {
        let key = format!("large_key_{size}");
        let value = "x".repeat(size);

        let start = Instant::now();

        // SET
        let cmd = resp::set_cmd(&key, &value);
        stream.write_all(&cmd).await.expect("Write failed");

        let mut buf = vec![0u8; 32];
        stream.read(&mut buf).await.expect("Read failed");

        // GET
        let cmd = resp::get_cmd(&key);
        stream.write_all(&cmd).await.expect("Write failed");

        let mut response = Vec::new();
        let mut total_read = 0;
        let expected_min = size + 10; // Value + RESP overhead

        while total_read < expected_min {
            let mut chunk = vec![0u8; 64 * 1024];
            let n = stream.read(&mut chunk).await.expect("Read failed");
            if n == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..n]);
            total_read += n;
        }

        let duration = start.elapsed();

        println!(
            "Size: {} KB, Round-trip: {:?}, Throughput: {:.2} MB/s",
            size / 1024,
            duration,
            (size * 2) as f64 / 1_000_000.0 / duration.as_secs_f64()
        );
    }
}

/// Memory pressure test - fill memory with keys and measure performance
#[tokio::test]
#[ignore]
async fn memory_pressure_test() {
    let host = "127.0.0.1";
    let port: u16 = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);

    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr).await.expect("Failed to connect");

    let num_keys = 100_000;
    let value_size = 256;
    let value = "x".repeat(value_size);

    println!("Inserting {num_keys} keys with {value_size} byte values...");

    let start = Instant::now();
    let mut read_buf = vec![0u8; 4096];

    // Use pipelining for efficiency
    let pipeline_size = 100;
    for batch in 0..(num_keys / pipeline_size) {
        let mut cmds = Vec::with_capacity(pipeline_size);
        for i in 0..pipeline_size {
            let key = format!("memtest:{batch}:{i}");
            cmds.push(resp::set_cmd(&key, &value));
        }
        let pipeline = resp::pipeline(&cmds);
        stream.write_all(&pipeline).await.expect("Write failed");

        // Drain responses
        for _ in 0..pipeline_size {
            stream.read(&mut read_buf).await.ok();
        }

        if batch % 100 == 0 {
            println!("Progress: {}/{} batches", batch, num_keys / pipeline_size);
        }
    }

    let insert_duration = start.elapsed();

    // Now measure read performance under memory pressure
    let read_start = Instant::now();
    let num_reads = 10_000;

    for i in 0..num_reads {
        let batch = i % (num_keys / pipeline_size);
        let idx = i % pipeline_size;
        let key = format!("memtest:{batch}:{idx}");
        let cmd = resp::get_cmd(&key);
        stream.write_all(&cmd).await.expect("Write failed");
        stream.read(&mut read_buf).await.ok();
    }

    let read_duration = read_start.elapsed();

    println!("\n=== Memory Pressure Test Results ===");
    println!("Keys inserted: {num_keys}");
    println!(
        "Estimated memory: {} MB",
        (num_keys * (value_size + 32)) / 1_000_000
    );
    println!("Insert duration: {insert_duration:?}");
    println!(
        "Insert rate: {:.0} keys/sec",
        num_keys as f64 / insert_duration.as_secs_f64()
    );
    println!("Read duration ({num_reads}): {read_duration:?}");
    println!(
        "Read rate: {:.0} ops/sec",
        num_reads as f64 / read_duration.as_secs_f64()
    );
}
