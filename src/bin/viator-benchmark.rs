//! Viator Benchmark - Performance benchmarking tool
//!
//! A tool for benchmarking Viator/Redis server performance.
//! Compatible with redis-benchmark command-line options.

use bytes::{Buf, BytesMut};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Benchmark configuration
struct Config {
    host: String,
    port: u16,
    clients: usize,
    requests: usize,
    data_size: usize,
    keepalive: bool,
    quiet: bool,
    tests: Vec<String>,
    password: Option<String>,
    dbnum: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            clients: 50,
            requests: 100_000,
            data_size: 3,
            keepalive: true,
            quiet: false,
            tests: vec![],
            password: None,
            dbnum: 0,
        }
    }
}

/// Statistics for a benchmark run
#[derive(Default)]
struct Stats {
    completed: AtomicU64,
    errors: AtomicU64,
    latencies: parking_lot::Mutex<Vec<Duration>>,
}

impl Stats {
    fn record(&self, latency: Duration, success: bool) {
        if success {
            self.completed.fetch_add(1, Ordering::Relaxed);
            self.latencies.lock().push(latency);
        } else {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn summary(&self, test_name: &str, total_time: Duration, config: &Config) {
        let completed = self.completed.load(Ordering::Relaxed);
        let mut latencies = self.latencies.lock();

        let rps = completed as f64 / total_time.as_secs_f64();

        // Sort latencies for percentile calculation
        latencies.sort();

        println!("====== {test_name} ======");
        println!(
            "  {} requests completed in {:.2} seconds",
            completed,
            total_time.as_secs_f64()
        );
        println!("  {} parallel clients", config.clients);
        println!("  {} bytes payload", config.data_size);
        println!("  keep alive: {}\n", if config.keepalive { 1 } else { 0 });

        // Print percentiles in redis-benchmark format
        let percentiles = [50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 100.0];
        for pct in percentiles {
            let idx = ((latencies.len() as f64 * pct / 100.0) as usize).saturating_sub(1);
            let idx = idx.min(latencies.len().saturating_sub(1));
            if let Some(lat) = latencies.get(idx) {
                let ms = lat.as_secs_f64() * 1000.0;
                println!("{:.2}% <= {:.0} milliseconds", pct, ms.ceil());
            }
        }

        println!("{:.2} requests per second\n", rps);
    }
}

/// RESP protocol encoder/decoder
fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn read_response(stream: &mut TcpStream, buf: &mut BytesMut) -> Result<(), std::io::Error> {
    // Read until we have a complete response
    let mut temp = [0u8; 4096];
    loop {
        // Check if we have a complete response
        if let Some(end) = find_response_end(buf) {
            buf.advance(end);
            return Ok(());
        }

        let n = stream.read(&mut temp)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        buf.extend_from_slice(&temp[..n]);
    }
}

fn find_response_end(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }

    match buf[0] {
        b'+' | b'-' | b':' => {
            // Simple string, error, or integer - find \r\n
            for i in 0..buf.len().saturating_sub(1) {
                if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                    return Some(i + 2);
                }
            }
            None
        }
        b'$' => {
            // Bulk string - find length, then data
            let mut i = 1;
            while i < buf.len() && buf[i] != b'\r' {
                i += 1;
            }
            if i + 1 >= buf.len() || buf[i + 1] != b'\n' {
                return None;
            }

            let len_str = std::str::from_utf8(&buf[1..i]).ok()?;
            let len: i64 = len_str.parse().ok()?;

            if len < 0 {
                // Null bulk string
                return Some(i + 2);
            }

            let total = i + 2 + len as usize + 2;
            if buf.len() >= total {
                Some(total)
            } else {
                None
            }
        }
        b'*' => {
            // Array - simplified: just find enough \r\n sequences
            let mut count = 0;
            let mut i = 0;
            while i < buf.len().saturating_sub(1) {
                if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                    count += 1;
                    if count >= 2 {
                        return Some(i + 2);
                    }
                }
                i += 1;
            }
            None
        }
        _ => Some(1), // Unknown, skip
    }
}

/// Run a single client's benchmark
fn run_client(
    config: &Config,
    _test_name: &str,
    command_fn: impl Fn(usize) -> Vec<u8>,
    requests_per_client: usize,
    stats: &Stats,
) {
    let addr = format!("{}:{}", config.host, config.port);
    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {addr}: {e}");
            return;
        }
    };

    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let mut buf = BytesMut::with_capacity(4096);

    // AUTH if password is set
    if let Some(ref password) = config.password {
        let cmd = encode_command(&["AUTH", password]);
        if stream.write_all(&cmd).is_err() {
            return;
        }
        if read_response(&mut stream, &mut buf).is_err() {
            return;
        }
    }

    // SELECT database
    if config.dbnum > 0 {
        let db_str = config.dbnum.to_string();
        let cmd = encode_command(&["SELECT", &db_str]);
        if stream.write_all(&cmd).is_err() {
            return;
        }
        if read_response(&mut stream, &mut buf).is_err() {
            return;
        }
    }

    for i in 0..requests_per_client {
        let cmd = command_fn(i);
        let start = Instant::now();

        let success =
            stream.write_all(&cmd).is_ok() && read_response(&mut stream, &mut buf).is_ok();

        let latency = start.elapsed();
        stats.record(latency, success);

        if !success && !config.keepalive {
            // Reconnect
            stream = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(_) => return,
            };
            stream.set_nodelay(true).ok();
        }
    }
}

/// Run a benchmark test
fn run_benchmark(
    config: &Config,
    test_name: &str,
    command_fn: impl Fn(usize) -> Vec<u8> + Send + Sync + Clone,
) {
    let stats = Arc::new(Stats::default());
    let requests_per_client = config.requests / config.clients;

    let start = Instant::now();

    std::thread::scope(|s| {
        for _ in 0..config.clients {
            let stats = stats.clone();
            let cmd_fn = command_fn.clone();
            s.spawn(move || {
                run_client(config, test_name, cmd_fn, requests_per_client, &stats);
            });
        }
    });

    let total_time = start.elapsed();
    stats.summary(test_name, total_time, config);
}

fn print_usage() {
    println!(
        "Usage: viator-benchmark [OPTIONS]

Options:
  -h <hostname>      Server hostname (default: 127.0.0.1)
  -p <port>          Server port (default: 6379)
  -c <clients>       Number of parallel connections (default: 50)
  -n <requests>      Total number of requests (default: 100000)
  -d <size>          Data size of SET/GET value in bytes (default: 3)
  -k <boolean>       1=keep alive, 0=reconnect (default: 1)
  -a <password>      Password for AUTH
  -t <tests>         Only run the comma-separated list of tests
  -q                 Quiet mode, only show query/sec values
  --help             Show this help message

Available tests:
  PING_INLINE, PING_MBULK, SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP,
  SADD, HSET, SPOP, ZADD, ZPOPMIN, LRANGE_100, LRANGE_300, LRANGE_500,
  LRANGE_600, MSET
"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" => {
                i += 1;
                if i < args.len() {
                    config.host = args[i].clone();
                }
            }
            "-p" => {
                i += 1;
                if i < args.len() {
                    config.port = args[i].parse().unwrap_or(6379);
                }
            }
            "-c" => {
                i += 1;
                if i < args.len() {
                    config.clients = args[i].parse().unwrap_or(50);
                }
            }
            "-n" => {
                i += 1;
                if i < args.len() {
                    config.requests = args[i].parse().unwrap_or(100_000);
                }
            }
            "-d" => {
                i += 1;
                if i < args.len() {
                    config.data_size = args[i].parse().unwrap_or(3);
                }
            }
            "-k" => {
                i += 1;
                if i < args.len() {
                    config.keepalive = args[i] != "0";
                }
            }
            "-a" => {
                i += 1;
                if i < args.len() {
                    config.password = Some(args[i].clone());
                }
            }
            "-t" => {
                i += 1;
                if i < args.len() {
                    config.tests = args[i].split(',').map(str::to_uppercase).collect();
                }
            }
            "-q" => {
                config.quiet = true;
            }
            "--help" | "-?" => {
                print_usage();
                return;
            }
            _ => {}
        }
        i += 1;
    }

    // Test connection first
    let addr = format!("{}:{}", config.host, config.port);
    if TcpStream::connect(&addr).is_err() {
        eprintln!("Could not connect to Viator at {addr}");
        eprintln!("PING: Connection refused");
        std::process::exit(1);
    }

    // Print configuration summary (like redis-benchmark)
    if !config.quiet {
        println!(
            "viator-benchmark: {} requests, {} clients, {} bytes payload",
            config.requests, config.clients, config.data_size
        );
        if !config.keepalive {
            println!("WARNING: keepalive disabled, you may see reduced performance");
        }
        println!();
    }

    let data = "x".repeat(config.data_size);
    let data_ref = data.as_str();

    let all_tests = vec![
        "PING_INLINE",
        "PING_MBULK",
        "SET",
        "GET",
        "INCR",
        "LPUSH",
        "RPUSH",
        "LPOP",
        "RPOP",
        "SADD",
        "HSET",
        "SPOP",
        "ZADD",
        "ZPOPMIN",
        "MSET",
    ];

    let tests_to_run: Vec<&str> = if config.tests.is_empty() {
        all_tests
    } else {
        all_tests
            .into_iter()
            .filter(|t| config.tests.iter().any(|ct| ct == *t))
            .collect()
    };

    for test in tests_to_run {
        match test {
            "PING_INLINE" => {
                run_benchmark(&config, "PING_INLINE", |_| b"PING\r\n".to_vec());
            }
            "PING_MBULK" => {
                run_benchmark(&config, "PING_MBULK", |_| encode_command(&["PING"]));
            }
            "SET" => {
                let data = data_ref.to_string();
                run_benchmark(&config, "SET", move |i| {
                    let key = format!("key:{i:012}");
                    encode_command(&["SET", &key, &data])
                });
            }
            "GET" => {
                run_benchmark(&config, "GET", |i| {
                    let key = format!("key:{i:012}");
                    encode_command(&["GET", &key])
                });
            }
            "INCR" => {
                run_benchmark(&config, "INCR", |_| encode_command(&["INCR", "counter"]));
            }
            "LPUSH" => {
                let data = data_ref.to_string();
                run_benchmark(&config, "LPUSH", move |_| {
                    encode_command(&["LPUSH", "mylist", &data])
                });
            }
            "RPUSH" => {
                let data = data_ref.to_string();
                run_benchmark(&config, "RPUSH", move |_| {
                    encode_command(&["RPUSH", "mylist", &data])
                });
            }
            "LPOP" => {
                run_benchmark(&config, "LPOP", |_| encode_command(&["LPOP", "mylist"]));
            }
            "RPOP" => {
                run_benchmark(&config, "RPOP", |_| encode_command(&["RPOP", "mylist"]));
            }
            "SADD" => {
                run_benchmark(&config, "SADD", |i| {
                    let member = format!("member:{i}");
                    encode_command(&["SADD", "myset", &member])
                });
            }
            "HSET" => {
                let data = data_ref.to_string();
                run_benchmark(&config, "HSET", move |i| {
                    let field = format!("field:{i}");
                    encode_command(&["HSET", "myhash", &field, &data])
                });
            }
            "SPOP" => {
                run_benchmark(&config, "SPOP", |_| encode_command(&["SPOP", "myset"]));
            }
            "ZADD" => {
                run_benchmark(&config, "ZADD", |i| {
                    let score = (i % 1000).to_string();
                    let member = format!("member:{i}");
                    encode_command(&["ZADD", "myzset", &score, &member])
                });
            }
            "ZPOPMIN" => {
                run_benchmark(&config, "ZPOPMIN", |_| {
                    encode_command(&["ZPOPMIN", "myzset"])
                });
            }
            "MSET" => {
                let data = data_ref.to_string();
                run_benchmark(&config, "MSET", move |i| {
                    let k1 = format!("key:{:012}", i * 10);
                    let k2 = format!("key:{:012}", i * 10 + 1);
                    let k3 = format!("key:{:012}", i * 10 + 2);
                    encode_command(&["MSET", &k1, &data, &k2, &data, &k3, &data])
                });
            }
            _ => {}
        }
    }
}
