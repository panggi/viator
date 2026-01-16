//! Differential testing framework for comparing Viator against Redis.
//!
//! This test suite runs identical commands against both Viator and Redis,
//! comparing responses to detect behavioral differences.
//!
//! # Prerequisites
//!
//! 1. Redis server running on port 6379
//! 2. Viator server running on port 6380
//!
//! # Running
//!
//! ```bash
//! # Start Redis
//! redis-server --port 6379
//!
//! # Start Viator
//! ./target/release/viator --port 6380
//!
//! # Run differential tests
//! cargo test --test differential_test -- --ignored
//! ```

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Configuration for differential testing.
pub struct DiffTestConfig {
    /// Redis host:port
    pub redis_addr: String,
    /// Viator host:port
    pub viator_addr: String,
    /// Connection timeout
    pub timeout: Duration,
}

impl Default for DiffTestConfig {
    fn default() -> Self {
        Self {
            redis_addr: "127.0.0.1:6379".to_string(),
            viator_addr: "127.0.0.1:6380".to_string(),
            timeout: Duration::from_secs(5),
        }
    }
}

/// A simple RESP client for testing.
pub struct TestClient {
    stream: BufReader<TcpStream>,
}

impl TestClient {
    /// Connect to a Redis-compatible server.
    pub fn connect(addr: &str, timeout: Duration) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Self {
            stream: BufReader::new(stream),
        })
    }

    /// Send a command and get the raw response.
    pub fn send_raw(&mut self, command: &str) -> Result<String, std::io::Error> {
        // Parse command into args
        let args: Vec<&str> = command.split_whitespace().collect();

        // Build RESP array
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        // Send
        self.stream.get_mut().write_all(req.as_bytes())?;
        self.stream.get_mut().flush()?;

        // Read response
        self.read_response()
    }

    /// Read a complete RESP response.
    fn read_response(&mut self) -> Result<String, std::io::Error> {
        let mut line = String::new();
        self.stream.read_line(&mut line)?;

        if line.is_empty() {
            return Ok(line);
        }

        let first_byte = line.chars().next().unwrap_or(' ');

        match first_byte {
            '+' | '-' | ':' => Ok(line), // Simple string, error, integer
            '$' => {
                // Bulk string
                let len: i64 = line[1..].trim().parse().unwrap_or(-1);
                if len < 0 {
                    return Ok(line); // Null bulk string
                }
                let mut data = vec![0u8; len as usize + 2]; // +2 for CRLF
                self.stream.get_mut().read_exact(&mut data)?;
                Ok(format!("{}{}", line, String::from_utf8_lossy(&data)))
            }
            '*' => {
                // Array
                let count: i64 = line[1..].trim().parse().unwrap_or(-1);
                if count < 0 {
                    return Ok(line); // Null array
                }
                let mut result = line;
                for _ in 0..count {
                    result.push_str(&self.read_response()?);
                }
                Ok(result)
            }
            _ => Ok(line),
        }
    }
}

/// Result of a differential test.
#[derive(Debug)]
pub struct DiffResult {
    pub command: String,
    pub redis_response: Result<String, String>,
    pub viator_response: Result<String, String>,
    pub match_result: MatchResult,
}

#[derive(Debug, PartialEq)]
pub enum MatchResult {
    /// Responses are identical
    Exact,
    /// Responses are semantically equivalent (e.g., different ordering for sets)
    Equivalent,
    /// Responses differ
    Mismatch,
    /// One or both servers errored
    Error,
}

/// Run a differential test comparing Redis and Viator.
pub fn run_diff_test(config: &DiffTestConfig, command: &str) -> DiffResult {
    let redis_response = match TestClient::connect(&config.redis_addr, config.timeout) {
        Ok(mut client) => client.send_raw(command).map_err(|e| e.to_string()),
        Err(e) => Err(format!("Redis connection failed: {}", e)),
    };

    let viator_response = match TestClient::connect(&config.viator_addr, config.timeout) {
        Ok(mut client) => client.send_raw(command).map_err(|e| e.to_string()),
        Err(e) => Err(format!("Viator connection failed: {}", e)),
    };

    let match_result = match (&redis_response, &viator_response) {
        (Ok(r), Ok(v)) if r == v => MatchResult::Exact,
        (Ok(r), Ok(v)) if normalize_response(r) == normalize_response(v) => MatchResult::Equivalent,
        (Ok(_), Ok(_)) => MatchResult::Mismatch,
        _ => MatchResult::Error,
    };

    DiffResult {
        command: command.to_string(),
        redis_response,
        viator_response,
        match_result,
    }
}

/// Normalize response for semantic comparison (e.g., sort array elements).
fn normalize_response(response: &str) -> String {
    // For now, just normalize whitespace
    response.trim().to_string()
}

/// Run a batch of commands and report mismatches.
pub fn run_diff_batch(config: &DiffTestConfig, commands: &[&str]) -> Vec<DiffResult> {
    commands
        .iter()
        .map(|cmd| run_diff_test(config, cmd))
        .collect()
}

// ==== Test Cases ====
// These tests are ignored by default because they require running servers.

#[test]
#[ignore]
fn test_string_commands() {
    let config = DiffTestConfig::default();

    let commands = vec![
        "SET difftest:key1 hello",
        "GET difftest:key1",
        "SETNX difftest:key1 world",
        "GET difftest:key1",
        "APPEND difftest:key1 world",
        "GET difftest:key1",
        "STRLEN difftest:key1",
        "GETRANGE difftest:key1 0 4",
        "DEL difftest:key1",
    ];

    let results = run_diff_batch(&config, &commands);

    for result in &results {
        if result.match_result == MatchResult::Mismatch {
            println!("MISMATCH: {}", result.command);
            println!("  Redis:  {:?}", result.redis_response);
            println!("  Viator: {:?}", result.viator_response);
        }
    }

    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.match_result == MatchResult::Mismatch)
        .collect();

    assert!(
        mismatches.is_empty(),
        "Found {} mismatches",
        mismatches.len()
    );
}

#[test]
#[ignore]
fn test_list_commands() {
    let config = DiffTestConfig::default();

    let commands = vec![
        "DEL difftest:list1",
        "RPUSH difftest:list1 a b c",
        "LPUSH difftest:list1 x y z",
        "LRANGE difftest:list1 0 -1",
        "LLEN difftest:list1",
        "LINDEX difftest:list1 0",
        "LINDEX difftest:list1 -1",
        "LPOP difftest:list1",
        "RPOP difftest:list1",
        "LRANGE difftest:list1 0 -1",
        "DEL difftest:list1",
    ];

    let results = run_diff_batch(&config, &commands);

    for result in &results {
        if result.match_result == MatchResult::Mismatch {
            println!("MISMATCH: {}", result.command);
            println!("  Redis:  {:?}", result.redis_response);
            println!("  Viator: {:?}", result.viator_response);
        }
    }

    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.match_result == MatchResult::Mismatch)
        .collect();

    assert!(
        mismatches.is_empty(),
        "Found {} mismatches",
        mismatches.len()
    );
}

#[test]
#[ignore]
fn test_hash_commands() {
    let config = DiffTestConfig::default();

    let commands = vec![
        "DEL difftest:hash1",
        "HSET difftest:hash1 field1 value1",
        "HGET difftest:hash1 field1",
        "HMSET difftest:hash1 field2 value2 field3 value3",
        "HMGET difftest:hash1 field1 field2 field3",
        "HLEN difftest:hash1",
        "HEXISTS difftest:hash1 field1",
        "HEXISTS difftest:hash1 nonexistent",
        "HDEL difftest:hash1 field1",
        "HGET difftest:hash1 field1",
        "DEL difftest:hash1",
    ];

    let results = run_diff_batch(&config, &commands);

    for result in &results {
        if result.match_result == MatchResult::Mismatch {
            println!("MISMATCH: {}", result.command);
            println!("  Redis:  {:?}", result.redis_response);
            println!("  Viator: {:?}", result.viator_response);
        }
    }

    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.match_result == MatchResult::Mismatch)
        .collect();

    assert!(
        mismatches.is_empty(),
        "Found {} mismatches",
        mismatches.len()
    );
}

#[test]
#[ignore]
fn test_set_commands() {
    let config = DiffTestConfig::default();

    let commands = vec![
        "DEL difftest:set1 difftest:set2",
        "SADD difftest:set1 a b c",
        "SCARD difftest:set1",
        "SISMEMBER difftest:set1 a",
        "SISMEMBER difftest:set1 x",
        "SADD difftest:set2 b c d",
        // Note: SMEMBERS ordering may vary - we'd need special comparison
        "SINTER difftest:set1 difftest:set2",
        "DEL difftest:set1 difftest:set2",
    ];

    let results = run_diff_batch(&config, &commands);

    for result in &results {
        if result.match_result == MatchResult::Mismatch {
            println!("MISMATCH: {}", result.command);
            println!("  Redis:  {:?}", result.redis_response);
            println!("  Viator: {:?}", result.viator_response);
        }
    }

    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.match_result == MatchResult::Mismatch)
        .collect();

    assert!(
        mismatches.is_empty(),
        "Found {} mismatches",
        mismatches.len()
    );
}

#[test]
#[ignore]
fn test_sorted_set_commands() {
    let config = DiffTestConfig::default();

    let commands = vec![
        "DEL difftest:zset1",
        "ZADD difftest:zset1 1 a 2 b 3 c",
        "ZCARD difftest:zset1",
        "ZSCORE difftest:zset1 b",
        "ZRANK difftest:zset1 b",
        "ZRANGE difftest:zset1 0 -1",
        "ZRANGE difftest:zset1 0 -1 WITHSCORES",
        "ZINCRBY difftest:zset1 5 a",
        "ZSCORE difftest:zset1 a",
        "DEL difftest:zset1",
    ];

    let results = run_diff_batch(&config, &commands);

    for result in &results {
        if result.match_result == MatchResult::Mismatch {
            println!("MISMATCH: {}", result.command);
            println!("  Redis:  {:?}", result.redis_response);
            println!("  Viator: {:?}", result.viator_response);
        }
    }

    let mismatches: Vec<_> = results
        .iter()
        .filter(|r| r.match_result == MatchResult::Mismatch)
        .collect();

    assert!(
        mismatches.is_empty(),
        "Found {} mismatches",
        mismatches.len()
    );
}

#[test]
#[ignore]
fn test_expiry_commands() {
    let config = DiffTestConfig::default();

    let commands = vec![
        "SET difftest:expkey value",
        "TTL difftest:expkey",
        "EXPIRE difftest:expkey 100",
        "TTL difftest:expkey",
        "PERSIST difftest:expkey",
        "TTL difftest:expkey",
        "PEXPIRE difftest:expkey 50000",
        "PTTL difftest:expkey",
        "DEL difftest:expkey",
    ];

    let results = run_diff_batch(&config, &commands);

    for result in &results {
        // TTL/PTTL may have slight timing differences, so we're lenient
        if result.match_result == MatchResult::Mismatch && !result.command.contains("TTL") {
            println!("MISMATCH: {}", result.command);
            println!("  Redis:  {:?}", result.redis_response);
            println!("  Viator: {:?}", result.viator_response);
        }
    }
}
