//! Viator Shake - Data migration and synchronization tool
//!
//! A tool for migrating data between Viator/Redis instances.
//! Supports full sync using SCAN+DUMP/RESTORE or key-by-key copy.

#![allow(unsafe_code)] // Required for signal handling

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Configuration for the migration
struct Config {
    // Source
    source_host: String,
    source_port: u16,
    source_password: Option<String>,
    source_db: i32,

    // Destination
    dest_host: String,
    dest_port: u16,
    dest_password: Option<String>,
    dest_db: i32,

    // Options
    pattern: String,
    batch_size: usize,
    replace: bool,
    ttl_offset: i64,
    no_ttl: bool,
    parallel: usize,
    dry_run: bool,
    verbose: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            source_host: "127.0.0.1".to_string(),
            source_port: 6379,
            source_password: None,
            source_db: -1, // -1 means all databases

            dest_host: "127.0.0.1".to_string(),
            dest_port: 6380,
            dest_password: None,
            dest_db: -1, // -1 means same as source

            pattern: "*".to_string(),
            batch_size: 100,
            replace: false,
            ttl_offset: 0,
            no_ttl: false,
            parallel: 1,
            dry_run: false,
            verbose: false,
        }
    }
}

/// Statistics for the migration
struct MigrationStats {
    keys_scanned: AtomicU64,
    keys_migrated: AtomicU64,
    keys_failed: AtomicU64,
    keys_skipped: AtomicU64,
    bytes_transferred: AtomicU64,
    start_time: Instant,
}

impl MigrationStats {
    fn new() -> Self {
        Self {
            keys_scanned: AtomicU64::new(0),
            keys_migrated: AtomicU64::new(0),
            keys_failed: AtomicU64::new(0),
            keys_skipped: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn print_progress(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let migrated = self.keys_migrated.load(Ordering::Relaxed);
        let rate = if elapsed > 0.0 {
            migrated as f64 / elapsed
        } else {
            0.0
        };

        eprint!(
            "\rScanned: {}, Migrated: {}, Failed: {}, Skipped: {} ({:.0} keys/sec)    ",
            self.keys_scanned.load(Ordering::Relaxed),
            migrated,
            self.keys_failed.load(Ordering::Relaxed),
            self.keys_skipped.load(Ordering::Relaxed),
            rate
        );
    }

    fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        println!("\n\nMigration Summary:");
        println!("  Duration: {:.2} seconds", elapsed.as_secs_f64());
        println!(
            "  Keys scanned: {}",
            self.keys_scanned.load(Ordering::Relaxed)
        );
        println!(
            "  Keys migrated: {}",
            self.keys_migrated.load(Ordering::Relaxed)
        );
        println!(
            "  Keys failed: {}",
            self.keys_failed.load(Ordering::Relaxed)
        );
        println!(
            "  Keys skipped: {}",
            self.keys_skipped.load(Ordering::Relaxed)
        );
        println!(
            "  Bytes transferred: {} bytes",
            self.bytes_transferred.load(Ordering::Relaxed)
        );

        let migrated = self.keys_migrated.load(Ordering::Relaxed);
        let elapsed_secs = elapsed.as_secs_f64();
        if elapsed_secs > 0.0 {
            println!(
                "  Average rate: {:.0} keys/sec",
                migrated as f64 / elapsed_secs
            );
        }
    }
}

/// RESP value type
#[derive(Debug, Clone)]
enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

#[allow(dead_code)]
impl RespValue {
    fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(Some(data)) => Some(data),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::BulkString(Some(data)) => std::str::from_utf8(data).ok(),
            RespValue::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            RespValue::BulkString(Some(data)) => std::str::from_utf8(data).ok()?.parse().ok(),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&Vec<RespValue>> {
        match self {
            RespValue::Array(Some(arr)) => Some(arr),
            _ => None,
        }
    }

    fn is_nil(&self) -> bool {
        matches!(self, RespValue::BulkString(None) | RespValue::Array(None))
    }
}

/// Redis connection wrapper
struct RedisConn {
    stream: TcpStream,
    buffer: Vec<u8>,
    pos: usize,
}

impl RedisConn {
    fn connect(host: &str, port: u16) -> Result<Self, String> {
        let addr = format!("{host}:{port}");
        let stream =
            TcpStream::connect(&addr).map_err(|e| format!("Failed to connect to {addr}: {e}"))?;
        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(30))).ok();

        Ok(Self {
            stream,
            buffer: Vec::with_capacity(64 * 1024),
            pos: 0,
        })
    }

    fn auth(&mut self, password: &str) -> Result<(), String> {
        let resp = self.command(&[b"AUTH", password.as_bytes()])?;
        match resp {
            RespValue::SimpleString(_) => Ok(()),
            RespValue::Error(e) => Err(e),
            _ => Err("Unexpected AUTH response".to_string()),
        }
    }

    fn select(&mut self, db: i32) -> Result<(), String> {
        let db_str = db.to_string();
        let resp = self.command(&[b"SELECT", db_str.as_bytes()])?;
        match resp {
            RespValue::SimpleString(_) => Ok(()),
            RespValue::Error(e) => Err(e),
            _ => Err("Unexpected SELECT response".to_string()),
        }
    }

    fn command(&mut self, args: &[&[u8]]) -> Result<RespValue, String> {
        self.send_command(args)?;
        self.read_response()
    }

    fn send_command(&mut self, args: &[&[u8]]) -> Result<(), String> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.stream
            .write_all(&buf)
            .map_err(|e| format!("Write error: {e}"))?;
        Ok(())
    }

    fn read_response(&mut self) -> Result<RespValue, String> {
        // Ensure we have data
        self.fill_buffer()?;

        let type_byte = self.read_byte()?;
        match type_byte {
            b'+' => {
                let line = self.read_line()?;
                Ok(RespValue::SimpleString(line))
            }
            b'-' => {
                let line = self.read_line()?;
                Ok(RespValue::Error(line))
            }
            b':' => {
                let line = self.read_line()?;
                let num: i64 = line.parse().map_err(|_| "Invalid integer")?;
                Ok(RespValue::Integer(num))
            }
            b'$' => {
                let line = self.read_line()?;
                let len: i64 = line.parse().map_err(|_| "Invalid bulk length")?;
                if len < 0 {
                    return Ok(RespValue::BulkString(None));
                }
                let data = self.read_exact(len as usize)?;
                self.skip_crlf()?;
                Ok(RespValue::BulkString(Some(data)))
            }
            b'*' => {
                let line = self.read_line()?;
                let count: i64 = line.parse().map_err(|_| "Invalid array length")?;
                if count < 0 {
                    return Ok(RespValue::Array(None));
                }
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(self.read_response()?);
                }
                Ok(RespValue::Array(Some(items)))
            }
            _ => Err(format!("Unknown RESP type: {}", type_byte as char)),
        }
    }

    fn fill_buffer(&mut self) -> Result<(), String> {
        if self.pos < self.buffer.len() {
            return Ok(());
        }

        self.buffer.clear();
        self.pos = 0;

        let mut temp = [0u8; 8192];
        let n = self
            .stream
            .read(&mut temp)
            .map_err(|e| format!("Read error: {e}"))?;
        if n == 0 {
            return Err("Connection closed".to_string());
        }
        self.buffer.extend_from_slice(&temp[..n]);
        Ok(())
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        while self.pos >= self.buffer.len() {
            self.buffer.clear();
            self.pos = 0;
            let mut temp = [0u8; 8192];
            let n = self
                .stream
                .read(&mut temp)
                .map_err(|e| format!("Read error: {e}"))?;
            if n == 0 {
                return Err("Connection closed".to_string());
            }
            self.buffer.extend_from_slice(&temp[..n]);
        }
        let byte = self.buffer[self.pos];
        self.pos += 1;
        Ok(byte)
    }

    fn read_line(&mut self) -> Result<String, String> {
        let mut line = Vec::new();
        loop {
            let byte = self.read_byte()?;
            if byte == b'\r' {
                let next = self.read_byte()?;
                if next == b'\n' {
                    break;
                }
                line.push(byte);
                line.push(next);
            } else {
                line.push(byte);
            }
        }
        String::from_utf8(line).map_err(|_| "Invalid UTF-8".to_string())
    }

    fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, String> {
        let mut data = Vec::with_capacity(len);
        let mut remaining = len;

        while remaining > 0 {
            while self.pos >= self.buffer.len() {
                self.buffer.clear();
                self.pos = 0;
                let mut temp = [0u8; 8192];
                let n = self
                    .stream
                    .read(&mut temp)
                    .map_err(|e| format!("Read error: {e}"))?;
                if n == 0 {
                    return Err("Connection closed".to_string());
                }
                self.buffer.extend_from_slice(&temp[..n]);
            }

            let available = self.buffer.len() - self.pos;
            let to_copy = remaining.min(available);
            data.extend_from_slice(&self.buffer[self.pos..self.pos + to_copy]);
            self.pos += to_copy;
            remaining -= to_copy;
        }

        Ok(data)
    }

    fn skip_crlf(&mut self) -> Result<(), String> {
        let cr = self.read_byte()?;
        let lf = self.read_byte()?;
        if cr != b'\r' || lf != b'\n' {
            return Err("Expected CRLF".to_string());
        }
        Ok(())
    }
}

/// Migrate a single key using DUMP/RESTORE
fn migrate_key(
    source: &mut RedisConn,
    dest: &mut RedisConn,
    key: &[u8],
    config: &Config,
    stats: &MigrationStats,
) -> Result<bool, String> {
    // Get TTL
    let ttl_ms: i64 = if config.no_ttl {
        0
    } else {
        let resp = source.command(&[b"PTTL", key])?;
        match resp {
            RespValue::Integer(t) if t > 0 => (t + config.ttl_offset).max(0),
            _ => 0, // No expiry or key doesn't exist
        }
    };

    // DUMP the key
    let dump_resp = source.command(&[b"DUMP", key])?;
    let dump_data = match dump_resp {
        RespValue::BulkString(Some(data)) => data,
        RespValue::BulkString(None) => {
            // Key doesn't exist (might have expired)
            stats.keys_skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }
        RespValue::Error(e) => return Err(e),
        _ => return Err("Unexpected DUMP response".to_string()),
    };

    stats
        .bytes_transferred
        .fetch_add(dump_data.len() as u64, Ordering::Relaxed);

    if config.dry_run {
        stats.keys_migrated.fetch_add(1, Ordering::Relaxed);
        return Ok(true);
    }

    // RESTORE on destination
    let ttl_str = ttl_ms.to_string();
    let restore_args: Vec<&[u8]> = if config.replace {
        vec![b"RESTORE", key, ttl_str.as_bytes(), &dump_data, b"REPLACE"]
    } else {
        vec![b"RESTORE", key, ttl_str.as_bytes(), &dump_data]
    };

    let restore_resp = dest.command(&restore_args)?;
    match restore_resp {
        RespValue::SimpleString(_) => {
            stats.keys_migrated.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        }
        RespValue::Error(e) => {
            if e.contains("BUSYKEY") && !config.replace {
                stats.keys_skipped.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            } else {
                Err(e)
            }
        }
        _ => Err("Unexpected RESTORE response".to_string()),
    }
}

/// Migrate a single database
fn migrate_database(
    source: &mut RedisConn,
    dest: &mut RedisConn,
    db: i32,
    config: &Config,
    stats: &MigrationStats,
    running: &AtomicBool,
) -> Result<(), String> {
    // Select database on both connections
    source.select(db)?;

    let dest_db = if config.dest_db >= 0 {
        config.dest_db
    } else {
        db
    };
    dest.select(dest_db)?;

    if config.verbose {
        println!("\nMigrating database {} -> {}", db, dest_db);
    }

    // Use SCAN to iterate keys
    let mut cursor = "0".to_string();
    let count_str = config.batch_size.to_string();

    loop {
        if !running.load(Ordering::Relaxed) {
            println!("\nMigration interrupted");
            break;
        }

        // SCAN cursor MATCH pattern COUNT batch_size
        let scan_resp = source.command(&[
            b"SCAN",
            cursor.as_bytes(),
            b"MATCH",
            config.pattern.as_bytes(),
            b"COUNT",
            count_str.as_bytes(),
        ])?;

        let arr = scan_resp.as_array().ok_or("Invalid SCAN response")?;

        if arr.len() < 2 {
            return Err("Invalid SCAN response structure".to_string());
        }

        cursor = arr[0].as_str().ok_or("Invalid cursor")?.to_string();

        let keys = arr[1].as_array().ok_or("Invalid keys array")?;

        for key_val in keys {
            let key = key_val.as_bytes().ok_or("Invalid key")?;
            stats.keys_scanned.fetch_add(1, Ordering::Relaxed);

            match migrate_key(source, dest, key, config, stats) {
                Ok(_) => {}
                Err(e) => {
                    stats.keys_failed.fetch_add(1, Ordering::Relaxed);
                    if config.verbose {
                        let key_str = String::from_utf8_lossy(key);
                        eprintln!("\nError migrating '{}': {}", key_str, e);
                    }
                }
            }
        }

        if !config.verbose {
            stats.print_progress();
        }

        if cursor == "0" {
            break;
        }
    }

    Ok(())
}

/// Migrate a single database (dry-run mode - no destination connection)
fn migrate_database_dry_run(
    source: &mut RedisConn,
    db: i32,
    config: &Config,
    stats: &MigrationStats,
    running: &AtomicBool,
) -> Result<(), String> {
    source.select(db)?;

    let dest_db = if config.dest_db >= 0 {
        config.dest_db
    } else {
        db
    };

    if config.verbose {
        println!("\n[DRY-RUN] Scanning database {} -> {}", db, dest_db);
    }

    let mut cursor = "0".to_string();
    let count_str = config.batch_size.to_string();

    loop {
        if !running.load(Ordering::Relaxed) {
            println!("\nMigration interrupted");
            break;
        }

        let scan_resp = source.command(&[
            b"SCAN",
            cursor.as_bytes(),
            b"MATCH",
            config.pattern.as_bytes(),
            b"COUNT",
            count_str.as_bytes(),
        ])?;

        let arr = scan_resp.as_array().ok_or("Invalid SCAN response")?;

        if arr.len() < 2 {
            return Err("Invalid SCAN response structure".to_string());
        }

        cursor = arr[0].as_str().ok_or("Invalid cursor")?.to_string();

        let keys = arr[1].as_array().ok_or("Invalid keys array")?;

        for key_val in keys {
            let key = key_val.as_bytes().ok_or("Invalid key")?;
            stats.keys_scanned.fetch_add(1, Ordering::Relaxed);

            // In dry-run, just DUMP to check key exists and get size
            let dump_resp = source.command(&[b"DUMP", key])?;
            match dump_resp {
                RespValue::BulkString(Some(data)) => {
                    stats
                        .bytes_transferred
                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                    stats.keys_migrated.fetch_add(1, Ordering::Relaxed);

                    if config.verbose {
                        let key_str = String::from_utf8_lossy(key);
                        println!(
                            "  [DRY-RUN] Would migrate: {} ({} bytes)",
                            key_str,
                            data.len()
                        );
                    }
                }
                RespValue::BulkString(None) => {
                    stats.keys_skipped.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    stats.keys_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if !config.verbose {
            stats.print_progress();
        }

        if cursor == "0" {
            break;
        }
    }

    Ok(())
}

/// Get list of databases that have keys
fn get_databases_with_keys(conn: &mut RedisConn) -> Result<Vec<i32>, String> {
    // Try INFO keyspace to find databases with keys
    let resp = conn.command(&[b"INFO", b"keyspace"])?;
    let info = match resp {
        RespValue::BulkString(Some(data)) => String::from_utf8_lossy(&data).to_string(),
        _ => return Ok(vec![0]), // Default to db 0
    };

    let mut dbs = HashSet::new();
    for line in info.lines() {
        if line.starts_with("db") {
            if let Some(num_str) = line.strip_prefix("db").and_then(|s| s.split(':').next()) {
                if let Ok(num) = num_str.parse::<i32>() {
                    dbs.insert(num);
                }
            }
        }
    }

    if dbs.is_empty() {
        Ok(vec![0])
    } else {
        let mut sorted: Vec<_> = dbs.into_iter().collect();
        sorted.sort();
        Ok(sorted)
    }
}

fn print_usage() {
    println!(
        "Usage: viator-shake [OPTIONS]

Migrate data between Viator/Redis instances.

Source Options:
  --source-host <host>       Source hostname (default: 127.0.0.1)
  --source-port <port>       Source port (default: 6379)
  --source-password <pass>   Source password
  --source-db <db>           Source database (-1 for all, default: -1)

Destination Options:
  --dest-host <host>         Destination hostname (default: 127.0.0.1)
  --dest-port <port>         Destination port (default: 6380)
  --dest-password <pass>     Destination password
  --dest-db <db>             Destination database (-1 to match source)

Migration Options:
  --pattern <pattern>        Key pattern to migrate (default: *)
  --batch-size <n>           SCAN batch size (default: 100)
  --replace                  Replace existing keys at destination
  --ttl-offset <ms>          Add milliseconds to all TTLs
  --no-ttl                   Don't preserve TTLs
  --parallel <n>             Number of parallel workers (default: 1)
  --dry-run                  Show what would be migrated
  --verbose                  Show detailed progress
  --help                     Show this help message

Examples:
  viator-shake --source-port 6379 --dest-port 6380
  viator-shake --pattern 'user:*' --replace
  viator-shake --source-db 0 --dest-db 1 --dest-host other-host
"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--source-host" => {
                i += 1;
                if i < args.len() {
                    config.source_host = args[i].clone();
                }
            }
            "--source-port" => {
                i += 1;
                if i < args.len() {
                    config.source_port = args[i].parse().unwrap_or(6379);
                }
            }
            "--source-password" => {
                i += 1;
                if i < args.len() {
                    config.source_password = Some(args[i].clone());
                }
            }
            "--source-db" => {
                i += 1;
                if i < args.len() {
                    config.source_db = args[i].parse().unwrap_or(-1);
                }
            }
            "--dest-host" => {
                i += 1;
                if i < args.len() {
                    config.dest_host = args[i].clone();
                }
            }
            "--dest-port" => {
                i += 1;
                if i < args.len() {
                    config.dest_port = args[i].parse().unwrap_or(6380);
                }
            }
            "--dest-password" => {
                i += 1;
                if i < args.len() {
                    config.dest_password = Some(args[i].clone());
                }
            }
            "--dest-db" => {
                i += 1;
                if i < args.len() {
                    config.dest_db = args[i].parse().unwrap_or(-1);
                }
            }
            "--pattern" => {
                i += 1;
                if i < args.len() {
                    config.pattern = args[i].clone();
                }
            }
            "--batch-size" => {
                i += 1;
                if i < args.len() {
                    config.batch_size = args[i].parse().unwrap_or(100);
                }
            }
            "--replace" => config.replace = true,
            "--ttl-offset" => {
                i += 1;
                if i < args.len() {
                    config.ttl_offset = args[i].parse().unwrap_or(0);
                }
            }
            "--no-ttl" => config.no_ttl = true,
            "--parallel" => {
                i += 1;
                if i < args.len() {
                    config.parallel = args[i].parse().unwrap_or(1);
                }
            }
            "--dry-run" => config.dry_run = true,
            "--verbose" | "-v" => config.verbose = true,
            "--help" => {
                print_usage();
                return;
            }
            other => {
                eprintln!("Unknown option: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Connect to source
    println!(
        "Connecting to source {}:{}...",
        config.source_host, config.source_port
    );
    let mut source = match RedisConn::connect(&config.source_host, config.source_port) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    if let Some(ref pass) = config.source_password {
        if let Err(e) = source.auth(pass) {
            eprintln!("Source authentication failed: {e}");
            std::process::exit(1);
        }
    }

    // Connect to destination (unless dry-run)
    let mut dest = if config.dry_run {
        println!("Dry run mode - no changes will be made to destination");
        None
    } else {
        println!(
            "Connecting to destination {}:{}...",
            config.dest_host, config.dest_port
        );
        let mut d = match RedisConn::connect(&config.dest_host, config.dest_port) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        };

        if let Some(ref pass) = config.dest_password {
            if let Err(e) = d.auth(pass) {
                eprintln!("Destination authentication failed: {e}");
                std::process::exit(1);
            }
        }

        Some(d)
    };

    // Setup signal handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc_handler(move || {
        r.store(false, Ordering::SeqCst);
    });

    // Get databases to migrate
    let databases = if config.source_db >= 0 {
        vec![config.source_db]
    } else {
        match get_databases_with_keys(&mut source) {
            Ok(dbs) => dbs,
            Err(e) => {
                eprintln!("Error getting database list: {e}");
                vec![0]
            }
        }
    };

    println!("\nStarting migration of {} database(s)...", databases.len());
    println!("Pattern: {}", config.pattern);
    if config.replace {
        println!("Mode: REPLACE existing keys");
    }
    println!();

    let stats = MigrationStats::new();

    // Migrate each database
    for db in databases {
        if !running.load(Ordering::Relaxed) {
            break;
        }

        // For dry-run mode, we still need a destination connection
        // Create a separate dummy connection or use a different approach
        if config.dry_run {
            // In dry-run mode, we simulate migration without actual destination
            if let Err(e) = migrate_database_dry_run(&mut source, db, &config, &stats, &running) {
                eprintln!("\nError during dry-run for database {}: {}", db, e);
            }
        } else if let Some(ref mut dest_conn) = dest {
            if let Err(e) = migrate_database(&mut source, dest_conn, db, &config, &stats, &running)
            {
                eprintln!("\nError migrating database {}: {}", db, e);
            }
        }
    }

    stats.print_summary();

    if stats.keys_failed.load(Ordering::Relaxed) > 0 {
        std::process::exit(1);
    }
}

/// Simple Ctrl-C handler (cross-platform)
fn ctrlc_handler<F: FnMut() + Send + 'static>(mut handler: F) {
    std::thread::spawn(move || {
        // Platform-specific signal handling
        #[cfg(unix)]
        {
            // Create a simple signal pipe
            let (read_fd, write_fd) = unsafe {
                let mut fds = [0i32; 2];
                if libc::pipe(fds.as_mut_ptr()) == 0 {
                    (fds[0], fds[1])
                } else {
                    return;
                }
            };

            // Set up signal handler that writes to pipe
            static mut SIGNAL_WRITE_FD: i32 = -1;
            unsafe {
                SIGNAL_WRITE_FD = write_fd;
                libc::signal(libc::SIGINT, signal_handler as usize);
            }

            extern "C" fn signal_handler(_: i32) {
                unsafe {
                    if SIGNAL_WRITE_FD >= 0 {
                        libc::write(SIGNAL_WRITE_FD, b"X".as_ptr() as *const _, 1);
                    }
                }
            }

            // Wait for signal
            let mut buf = [0u8; 1];
            unsafe {
                libc::read(read_fd, buf.as_mut_ptr() as *mut _, 1);
            }
            handler();
        }

        #[cfg(not(unix))]
        {
            // On non-Unix, just loop and check periodically
            loop {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    });
}
