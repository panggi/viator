//! Viator Load - JSON data import tool
//!
//! A tool for importing data from JSON format into Viator/Redis.
//! Complementary to viator-dump for data migration and recovery.

use base64::Engine;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Configuration for the load operation
struct Config {
    host: String,
    port: u16,
    password: Option<String>,
    input: Option<PathBuf>,
    database: Option<u32>,
    replace: bool,
    no_ttl: bool,
    pipe_mode: bool,
    dry_run: bool,
    verbose: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            password: None,
            input: None,
            database: None,
            replace: false,
            no_ttl: false,
            pipe_mode: false,
            dry_run: false,
            verbose: false,
        }
    }
}

/// Entry from JSON dump
#[derive(Debug, Deserialize)]
struct DumpEntry {
    db: u32,
    key: String,
    #[serde(rename = "type")]
    data_type: String,
    value: serde_json::Value,
    #[serde(default)]
    ttl: Option<i64>,
    #[serde(default)]
    encoding: Option<String>,
}

/// RESP protocol encoder
fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Read a RESP response
fn read_response(stream: &mut TcpStream) -> Result<String, String> {
    let mut buf = [0u8; 1];
    stream
        .read_exact(&mut buf)
        .map_err(|e| format!("Read error: {e}"))?;

    let mut line = String::new();
    loop {
        let mut byte = [0u8; 1];
        stream
            .read_exact(&mut byte)
            .map_err(|e| format!("Read error: {e}"))?;
        if byte[0] == b'\r' {
            let mut lf = [0u8; 1];
            stream
                .read_exact(&mut lf)
                .map_err(|e| format!("Read error: {e}"))?;
            break;
        }
        line.push(byte[0] as char);
    }

    match buf[0] {
        b'+' => Ok(line),
        b'-' => Err(line),
        b':' => Ok(line),
        b'$' => {
            let len: i64 = line.parse().map_err(|_| "Invalid bulk length")?;
            if len < 0 {
                return Ok("(nil)".to_string());
            }
            let mut data = vec![0u8; len as usize];
            stream
                .read_exact(&mut data)
                .map_err(|e| format!("Read error: {e}"))?;
            // Read trailing CRLF
            let mut crlf = [0u8; 2];
            stream
                .read_exact(&mut crlf)
                .map_err(|e| format!("Read error: {e}"))?;
            String::from_utf8_lossy(&data).to_string().pipe(Ok)
        }
        b'*' => {
            let count: i64 = line.parse().map_err(|_| "Invalid array length")?;
            if count < 0 {
                return Ok("(nil)".to_string());
            }
            // Read array elements (simplified - just consume them)
            for _ in 0..count {
                read_response(stream)?;
            }
            Ok(format!("(array of {count})"))
        }
        _ => Err(format!("Unknown response type: {}", buf[0] as char)),
    }
}

trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}

impl<T> Pipe for T {}

/// Send a command and read response
fn send_command(stream: &mut TcpStream, args: &[&[u8]]) -> Result<String, String> {
    let cmd = encode_command(args);
    stream
        .write_all(&cmd)
        .map_err(|e| format!("Write error: {e}"))?;
    stream.flush().map_err(|e| format!("Flush error: {e}"))?;
    read_response(stream)
}

/// Import a string value
fn import_string(
    stream: &mut TcpStream,
    key: &str,
    value: &serde_json::Value,
    encoding: Option<&str>,
    replace: bool,
    dry_run: bool,
) -> Result<(), String> {
    let data = match encoding {
        Some("base64") => {
            let b64 = value.as_str().ok_or("Expected base64 string")?;
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| format!("Base64 decode error: {e}"))?
        }
        _ => value
            .as_str()
            .ok_or("Expected string value")?
            .as_bytes()
            .to_vec(),
    };

    if dry_run {
        println!("  SET {} ({} bytes)", key, data.len());
        return Ok(());
    }

    if replace {
        send_command(stream, &[b"DEL", key.as_bytes()])?;
    }

    send_command(stream, &[b"SET", key.as_bytes(), &data])?;
    Ok(())
}

/// Import a list value
fn import_list(
    stream: &mut TcpStream,
    key: &str,
    value: &serde_json::Value,
    replace: bool,
    dry_run: bool,
) -> Result<(), String> {
    let items = value.as_array().ok_or("Expected array for list")?;

    if dry_run {
        println!("  RPUSH {} ({} items)", key, items.len());
        return Ok(());
    }

    if replace {
        send_command(stream, &[b"DEL", key.as_bytes()])?;
    }

    for item in items {
        let val = item.as_str().ok_or("Expected string in list")?.as_bytes();
        send_command(stream, &[b"RPUSH", key.as_bytes(), val])?;
    }
    Ok(())
}

/// Import a set value
fn import_set(
    stream: &mut TcpStream,
    key: &str,
    value: &serde_json::Value,
    replace: bool,
    dry_run: bool,
) -> Result<(), String> {
    let members = value.as_array().ok_or("Expected array for set")?;

    if dry_run {
        println!("  SADD {} ({} members)", key, members.len());
        return Ok(());
    }

    if replace {
        send_command(stream, &[b"DEL", key.as_bytes()])?;
    }

    for member in members {
        let val = member.as_str().ok_or("Expected string in set")?.as_bytes();
        send_command(stream, &[b"SADD", key.as_bytes(), val])?;
    }
    Ok(())
}

/// Import a hash value
fn import_hash(
    stream: &mut TcpStream,
    key: &str,
    value: &serde_json::Value,
    replace: bool,
    dry_run: bool,
) -> Result<(), String> {
    let fields = value.as_object().ok_or("Expected object for hash")?;

    if dry_run {
        println!("  HSET {} ({} fields)", key, fields.len());
        return Ok(());
    }

    if replace {
        send_command(stream, &[b"DEL", key.as_bytes()])?;
    }

    for (field, val) in fields {
        let value_str = val.as_str().ok_or("Expected string value in hash")?;
        send_command(
            stream,
            &[
                b"HSET",
                key.as_bytes(),
                field.as_bytes(),
                value_str.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

/// Import a sorted set value
fn import_zset(
    stream: &mut TcpStream,
    key: &str,
    value: &serde_json::Value,
    replace: bool,
    dry_run: bool,
) -> Result<(), String> {
    let members = value.as_array().ok_or("Expected array for zset")?;

    if dry_run {
        println!("  ZADD {} ({} members)", key, members.len());
        return Ok(());
    }

    if replace {
        send_command(stream, &[b"DEL", key.as_bytes()])?;
    }

    for member in members {
        let obj = member.as_object().ok_or("Expected object in zset")?;
        let score = obj
            .get("score")
            .and_then(|v| v.as_f64())
            .ok_or("Expected score in zset member")?;
        let value = obj
            .get("member")
            .and_then(|v| v.as_str())
            .ok_or("Expected member in zset")?;
        let score_str = score.to_string();
        send_command(
            stream,
            &[
                b"ZADD",
                key.as_bytes(),
                score_str.as_bytes(),
                value.as_bytes(),
            ],
        )?;
    }
    Ok(())
}

/// Import a stream value
fn import_stream(
    stream: &mut TcpStream,
    key: &str,
    value: &serde_json::Value,
    replace: bool,
    dry_run: bool,
) -> Result<(), String> {
    let entries = value.as_array().ok_or("Expected array for stream")?;

    if dry_run {
        println!("  XADD {} ({} entries)", key, entries.len());
        return Ok(());
    }

    if replace {
        send_command(stream, &[b"DEL", key.as_bytes()])?;
    }

    for entry in entries {
        let obj = entry.as_object().ok_or("Expected object in stream")?;
        let id = obj
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or("Expected id in stream entry")?;
        let fields = obj
            .get("fields")
            .and_then(|v| v.as_object())
            .ok_or("Expected fields in stream entry")?;

        let mut args: Vec<&[u8]> = vec![b"XADD", key.as_bytes(), id.as_bytes()];
        let field_strings: Vec<String> = fields
            .iter()
            .flat_map(|(k, v)| vec![k.clone(), v.as_str().unwrap_or_default().to_string()])
            .collect();
        let field_bytes: Vec<&[u8]> = field_strings.iter().map(|s| s.as_bytes()).collect();
        args.extend(field_bytes);

        let owned_args: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
        let arg_refs: Vec<&[u8]> = owned_args.iter().map(|a| a.as_slice()).collect();
        send_command(stream, &arg_refs)?;
    }
    Ok(())
}

/// Set TTL on a key
fn set_ttl(stream: &mut TcpStream, key: &str, ttl_ms: i64, dry_run: bool) -> Result<(), String> {
    if ttl_ms <= 0 {
        return Ok(());
    }

    // Calculate expiration time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let expire_at = now + ttl_ms;

    if dry_run {
        println!("  PEXPIREAT {} {}", key, expire_at);
        return Ok(());
    }

    let expire_str = expire_at.to_string();
    send_command(
        stream,
        &[b"PEXPIREAT", key.as_bytes(), expire_str.as_bytes()],
    )?;
    Ok(())
}

/// Import a single entry
fn import_entry(
    stream: &mut TcpStream,
    entry: &DumpEntry,
    config: &Config,
    current_db: &mut u32,
) -> Result<(), String> {
    // Switch database if needed
    if entry.db != *current_db {
        if !config.dry_run {
            let db_str = entry.db.to_string();
            send_command(stream, &[b"SELECT", db_str.as_bytes()])?;
        } else {
            println!("SELECT {}", entry.db);
        }
        *current_db = entry.db;
    }

    // Import based on type
    match entry.data_type.as_str() {
        "string" => import_string(
            stream,
            &entry.key,
            &entry.value,
            entry.encoding.as_deref(),
            config.replace,
            config.dry_run,
        )?,
        "list" => import_list(
            stream,
            &entry.key,
            &entry.value,
            config.replace,
            config.dry_run,
        )?,
        "set" => import_set(
            stream,
            &entry.key,
            &entry.value,
            config.replace,
            config.dry_run,
        )?,
        "hash" => import_hash(
            stream,
            &entry.key,
            &entry.value,
            config.replace,
            config.dry_run,
        )?,
        "zset" => import_zset(
            stream,
            &entry.key,
            &entry.value,
            config.replace,
            config.dry_run,
        )?,
        "stream" => import_stream(
            stream,
            &entry.key,
            &entry.value,
            config.replace,
            config.dry_run,
        )?,
        other => {
            eprintln!(
                "Warning: Unknown type '{}' for key '{}', skipping",
                other, entry.key
            );
            return Ok(());
        }
    }

    // Set TTL if present and not disabled
    if !config.no_ttl {
        if let Some(ttl) = entry.ttl {
            set_ttl(stream, &entry.key, ttl, config.dry_run)?;
        }
    }

    Ok(())
}

/// Statistics for the import
#[derive(Default)]
struct ImportStats {
    keys_imported: u64,
    keys_failed: u64,
    keys_skipped: u64,
    by_type: HashMap<String, u64>,
}

fn print_usage() {
    println!(
        "Usage: viator-load [OPTIONS] [input-file]

Load data from JSON format into Viator/Redis server.

Options:
  -h <hostname>    Server hostname (default: 127.0.0.1)
  -p <port>        Server port (default: 6379)
  -a <password>    Password for AUTH
  -n <database>    Target database number (overrides db in JSON)
  --replace        Delete existing keys before setting
  --no-ttl         Don't restore TTLs
  --pipe           Use pipe mode for faster loading
  --dry-run        Show what would be imported without connecting
  --verbose        Show detailed progress
  --help           Show this help message

Input Format:
  JSON Lines format (one JSON object per line), as produced by viator-dump:
  {{\"db\":0,\"key\":\"foo\",\"type\":\"string\",\"value\":\"bar\",\"ttl\":1000}}

Examples:
  viator-load dump.json
  viator-load -h redis.example.com -p 6380 dump.json
  viator-load --replace --no-ttl dump.json
  cat dump.json | viator-load -
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
            "-a" => {
                i += 1;
                if i < args.len() {
                    config.password = Some(args[i].clone());
                }
            }
            "-n" => {
                i += 1;
                if i < args.len() {
                    config.database = Some(args[i].parse().unwrap_or(0));
                }
            }
            "--replace" => config.replace = true,
            "--no-ttl" => config.no_ttl = true,
            "--pipe" => config.pipe_mode = true,
            "--dry-run" => config.dry_run = true,
            "--verbose" | "-v" => config.verbose = true,
            "--help" => {
                print_usage();
                return;
            }
            arg if !arg.starts_with('-') => {
                if arg == "-" {
                    config.input = None; // stdin
                } else {
                    config.input = Some(PathBuf::from(arg));
                }
            }
            other => {
                eprintln!("Unknown option: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Create reader (from file or stdin)
    let reader: Box<dyn BufRead> = if let Some(ref path) = config.input {
        if !path.exists() {
            eprintln!("Error: File not found: {}", path.display());
            std::process::exit(1);
        }
        Box::new(BufReader::new(
            File::open(path).expect("Failed to open input file"),
        ))
    } else {
        Box::new(BufReader::new(std::io::stdin()))
    };

    // Connect to server (unless dry-run)
    let mut stream: Option<TcpStream> = if config.dry_run {
        println!("Dry run mode - no changes will be made\n");
        None
    } else {
        let addr = format!("{}:{}", config.host, config.port);
        let s = match TcpStream::connect(&addr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error: Could not connect to {addr}: {e}");
                std::process::exit(1);
            }
        };
        s.set_nodelay(true).ok();
        s.set_read_timeout(Some(Duration::from_secs(30))).ok();
        s.set_write_timeout(Some(Duration::from_secs(30))).ok();

        // Authenticate if password is set
        if let Some(ref pass) = config.password {
            match send_command(&mut s.try_clone().unwrap(), &[b"AUTH", pass.as_bytes()]) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Authentication failed: {e}");
                    std::process::exit(1);
                }
            }
        }

        Some(s)
    };

    let mut stats = ImportStats::default();
    let mut current_db = 0u32;

    // Select initial database if specified
    if let Some(db) = config.database {
        current_db = db;
        if let Some(ref mut s) = stream {
            let db_str = db.to_string();
            if let Err(e) = send_command(s, &[b"SELECT", db_str.as_bytes()]) {
                eprintln!("Failed to select database {db}: {e}");
                std::process::exit(1);
            }
        }
    }

    println!("Loading data into {}:{}...", config.host, config.port);

    // Process each line
    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading line {}: {e}", line_num + 1);
                stats.keys_failed += 1;
                continue;
            }
        };

        // Skip empty lines and comments
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Parse JSON entry
        let entry: DumpEntry = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Error parsing line {}: {e}", line_num + 1);
                stats.keys_failed += 1;
                continue;
            }
        };

        // Override database if specified
        let mut entry = entry;
        if let Some(db) = config.database {
            entry.db = db;
        }

        if config.verbose {
            println!("[{}] {} ({})", entry.db, entry.key, entry.data_type);
        }

        // Import the entry
        match stream.as_mut() {
            Some(s) => match import_entry(s, &entry, &config, &mut current_db) {
                Ok(()) => {
                    stats.keys_imported += 1;
                    *stats.by_type.entry(entry.data_type.clone()).or_insert(0) += 1;
                }
                Err(e) => {
                    eprintln!("Error importing '{}': {e}", entry.key);
                    stats.keys_failed += 1;
                }
            },
            None => {
                // Dry run - just count
                stats.keys_imported += 1;
                *stats.by_type.entry(entry.data_type.clone()).or_insert(0) += 1;
            }
        }

        // Progress indicator every 1000 keys
        if !config.verbose && (stats.keys_imported + stats.keys_failed) % 1000 == 0 {
            eprint!(
                "\rProcessed {} keys...",
                stats.keys_imported + stats.keys_failed
            );
        }
    }

    // Clear progress line
    if !config.verbose {
        eprint!("\r                                        \r");
    }

    // Print summary
    println!("\nImport complete:");
    println!("  Keys imported: {}", stats.keys_imported);
    if stats.keys_failed > 0 {
        println!("  Keys failed: {}", stats.keys_failed);
    }
    if stats.keys_skipped > 0 {
        println!("  Keys skipped: {}", stats.keys_skipped);
    }

    println!("\nBy type:");
    for (data_type, count) in &stats.by_type {
        println!("  {}: {}", data_type, count);
    }

    if stats.keys_failed > 0 {
        std::process::exit(1);
    }
}
