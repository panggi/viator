//! Viator Dump - Export data to JSON
//!
//! A tool for exporting Viator/Redis data to JSON format.
//! Supports filtering by key patterns and database selection.

#![allow(dead_code)] // Some enum variants used for parsing only

use bytes::BytesMut;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Export configuration
struct Config {
    host: String,
    port: u16,
    password: Option<String>,
    database: u32,
    pattern: String,
    output: Option<String>,
    pretty: bool,
    include_ttl: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            password: None,
            database: 0,
            pattern: "*".to_string(),
            output: None,
            pretty: false,
            include_ttl: true,
        }
    }
}

/// Connection to Viator/Redis server
struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    fn connect(config: &Config) -> Result<Self, String> {
        let addr = format!("{}:{}", config.host, config.port);
        let stream = TcpStream::connect(&addr)
            .map_err(|e| format!("Could not connect to Viator at {}: {}", addr, e))?;

        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(30))).ok();

        let mut conn = Self {
            stream,
            buffer: BytesMut::with_capacity(65536),
        };

        // AUTH if password set
        if let Some(ref password) = config.password {
            let response = conn.execute(&["AUTH", password])?;
            if response.starts_with("-") {
                return Err(format!("AUTH failed: {}", response.trim()));
            }
        }

        // SELECT database
        if config.database > 0 {
            let db_str = config.database.to_string();
            let response = conn.execute(&["SELECT", &db_str])?;
            if response.starts_with("-") {
                return Err(format!("SELECT failed: {}", response.trim()));
            }
        }

        Ok(conn)
    }

    fn execute(&mut self, args: &[&str]) -> Result<String, String> {
        let cmd = encode_command(args);
        self.stream
            .write_all(&cmd)
            .map_err(|e| format!("Write error: {}", e))?;
        self.read_response()
    }

    fn read_response(&mut self) -> Result<String, String> {
        self.buffer.clear();
        let mut temp = [0u8; 8192];

        loop {
            let n = self
                .stream
                .read(&mut temp)
                .map_err(|e| format!("Read error: {}", e))?;

            if n == 0 {
                return Err("Connection closed".to_string());
            }

            self.buffer.extend_from_slice(&temp[..n]);

            if let Some(response) = parse_response(&self.buffer)? {
                return Ok(response);
            }
        }
    }

    fn execute_raw(&mut self, args: &[&str]) -> Result<RespValue, String> {
        let cmd = encode_command(args);
        self.stream
            .write_all(&cmd)
            .map_err(|e| format!("Write error: {}", e))?;
        self.read_response_raw()
    }

    fn read_response_raw(&mut self) -> Result<RespValue, String> {
        self.buffer.clear();
        let mut temp = [0u8; 65536];

        loop {
            let n = self
                .stream
                .read(&mut temp)
                .map_err(|e| format!("Read error: {}", e))?;

            if n == 0 {
                return Err("Connection closed".to_string());
            }

            self.buffer.extend_from_slice(&temp[..n]);

            if let Some((value, _)) = parse_resp_value(&self.buffer, 0)? {
                return Ok(value);
            }
        }
    }
}

/// RESP protocol value
#[derive(Debug, Clone)]
enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    fn as_string(&self) -> Option<String> {
        match self {
            RespValue::Simple(s) => Some(s.clone()),
            RespValue::Bulk(Some(b)) => String::from_utf8(b.clone()).ok(),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<Vec<u8>> {
        match self {
            RespValue::Bulk(Some(b)) => Some(b.clone()),
            RespValue::Simple(s) => Some(s.as_bytes().to_vec()),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&Vec<RespValue>> {
        match self {
            RespValue::Array(Some(arr)) => Some(arr),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            RespValue::Bulk(Some(b)) => String::from_utf8(b.clone()).ok()?.parse().ok(),
            _ => None,
        }
    }
}

fn parse_resp_value(buf: &[u8], offset: usize) -> Result<Option<(RespValue, usize)>, String> {
    if offset >= buf.len() {
        return Ok(None);
    }

    match buf[offset] {
        b'+' => {
            if let Some(end) = find_crlf(buf, offset) {
                let s = String::from_utf8_lossy(&buf[offset + 1..end]).to_string();
                return Ok(Some((RespValue::Simple(s), end + 2)));
            }
            Ok(None)
        }
        b'-' => {
            if let Some(end) = find_crlf(buf, offset) {
                let s = String::from_utf8_lossy(&buf[offset + 1..end]).to_string();
                return Ok(Some((RespValue::Error(s), end + 2)));
            }
            Ok(None)
        }
        b':' => {
            if let Some(end) = find_crlf(buf, offset) {
                let s = String::from_utf8_lossy(&buf[offset + 1..end]);
                let n: i64 = s.parse().map_err(|_| "Invalid integer")?;
                return Ok(Some((RespValue::Integer(n), end + 2)));
            }
            Ok(None)
        }
        b'$' => {
            if let Some(len_end) = find_crlf(buf, offset) {
                let len_str = String::from_utf8_lossy(&buf[offset + 1..len_end]);
                let len: i64 = len_str.parse().map_err(|_| "Invalid bulk length")?;

                if len < 0 {
                    return Ok(Some((RespValue::Bulk(None), len_end + 2)));
                }

                let data_start = len_end + 2;
                let data_end = data_start + len as usize;
                let total = data_end + 2;

                if buf.len() >= total {
                    let data = buf[data_start..data_end].to_vec();
                    return Ok(Some((RespValue::Bulk(Some(data)), total)));
                }
            }
            Ok(None)
        }
        b'*' => {
            if let Some(len_end) = find_crlf(buf, offset) {
                let len_str = String::from_utf8_lossy(&buf[offset + 1..len_end]);
                let len: i64 = len_str.parse().map_err(|_| "Invalid array length")?;

                if len < 0 {
                    return Ok(Some((RespValue::Array(None), len_end + 2)));
                }

                let mut pos = len_end + 2;
                let mut elements = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    match parse_resp_value(buf, pos)? {
                        Some((value, next_pos)) => {
                            elements.push(value);
                            pos = next_pos;
                        }
                        None => return Ok(None),
                    }
                }

                return Ok(Some((RespValue::Array(Some(elements)), pos)));
            }
            Ok(None)
        }
        _ => Err(format!("Unknown RESP type: {}", buf[offset] as char)),
    }
}

fn find_crlf(buf: &[u8], offset: usize) -> Option<usize> {
    (offset..buf.len().saturating_sub(1)).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

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

fn parse_response(buf: &[u8]) -> Result<Option<String>, String> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' | b'-' => {
            if let Some(end) = find_crlf(buf, 0) {
                let s = String::from_utf8_lossy(&buf[1..end]).to_string();
                return Ok(Some(s));
            }
        }
        b':' => {
            if let Some(end) = find_crlf(buf, 0) {
                let s = String::from_utf8_lossy(&buf[1..end]).to_string();
                return Ok(Some(s));
            }
        }
        b'$' => {
            if let Some(len_end) = find_crlf(buf, 0) {
                let len_str = String::from_utf8_lossy(&buf[1..len_end]);
                let len: i64 = len_str.parse().map_err(|_| "Invalid bulk length")?;

                if len < 0 {
                    return Ok(Some("(nil)".to_string()));
                }

                let data_start = len_end + 2;
                let data_end = data_start + len as usize;
                let total = data_end + 2;

                if buf.len() >= total {
                    let data = String::from_utf8_lossy(&buf[data_start..data_end]).to_string();
                    return Ok(Some(data));
                }
            }
        }
        _ => {}
    }

    Ok(None)
}

/// Get all keys matching pattern using SCAN
fn scan_keys(conn: &mut Connection, pattern: &str) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut cursor = "0".to_string();

    loop {
        let response = conn.execute_raw(&["SCAN", &cursor, "MATCH", pattern, "COUNT", "1000"])?;

        if let Some(arr) = response.as_array() {
            if arr.len() >= 2 {
                // First element is new cursor
                if let Some(new_cursor) = arr[0].as_string() {
                    cursor = new_cursor;
                }

                // Second element is array of keys
                if let Some(key_arr) = arr[1].as_array() {
                    for key in key_arr {
                        if let Some(k) = key.as_string() {
                            keys.push(k);
                        }
                    }
                }
            }
        }

        if cursor == "0" {
            break;
        }
    }

    Ok(keys)
}

/// Get key type
fn get_type(conn: &mut Connection, key: &str) -> Result<String, String> {
    conn.execute(&["TYPE", key])
}

/// Get TTL for key
fn get_ttl(conn: &mut Connection, key: &str) -> Result<i64, String> {
    let response = conn.execute(&["PTTL", key])?;
    response.parse().map_err(|_| "Invalid TTL".to_string())
}

/// Export a string value
fn export_string(conn: &mut Connection, key: &str) -> Result<Value, String> {
    let response = conn.execute_raw(&["GET", key])?;
    match response.as_bytes() {
        Some(bytes) => {
            // Try to parse as UTF-8, otherwise base64 encode
            match String::from_utf8(bytes.clone()) {
                Ok(s) => Ok(Value::String(s)),
                Err(_) => {
                    use base64::Engine;
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
                    let mut obj = Map::new();
                    obj.insert("_encoding".to_string(), Value::String("base64".to_string()));
                    obj.insert("data".to_string(), Value::String(encoded));
                    Ok(Value::Object(obj))
                }
            }
        }
        None => Ok(Value::Null),
    }
}

/// Export a list value
fn export_list(conn: &mut Connection, key: &str) -> Result<Value, String> {
    let response = conn.execute_raw(&["LRANGE", key, "0", "-1"])?;
    match response.as_array() {
        Some(arr) => {
            let values: Vec<Value> = arr
                .iter()
                .filter_map(|v| v.as_string().map(Value::String))
                .collect();
            Ok(Value::Array(values))
        }
        None => Ok(Value::Array(vec![])),
    }
}

/// Export a set value
fn export_set(conn: &mut Connection, key: &str) -> Result<Value, String> {
    let response = conn.execute_raw(&["SMEMBERS", key])?;
    match response.as_array() {
        Some(arr) => {
            let values: Vec<Value> = arr
                .iter()
                .filter_map(|v| v.as_string().map(Value::String))
                .collect();
            Ok(Value::Array(values))
        }
        None => Ok(Value::Array(vec![])),
    }
}

/// Export a hash value
fn export_hash(conn: &mut Connection, key: &str) -> Result<Value, String> {
    let response = conn.execute_raw(&["HGETALL", key])?;
    match response.as_array() {
        Some(arr) => {
            let mut map = Map::new();
            let mut i = 0;
            while i + 1 < arr.len() {
                if let (Some(field), Some(value)) = (arr[i].as_string(), arr[i + 1].as_string()) {
                    map.insert(field, Value::String(value));
                }
                i += 2;
            }
            Ok(Value::Object(map))
        }
        None => Ok(Value::Object(Map::new())),
    }
}

/// Export a sorted set value
fn export_zset(conn: &mut Connection, key: &str) -> Result<Value, String> {
    let response = conn.execute_raw(&["ZRANGE", key, "0", "-1", "WITHSCORES"])?;
    match response.as_array() {
        Some(arr) => {
            let mut members = Vec::new();
            let mut i = 0;
            while i + 1 < arr.len() {
                if let (Some(member), Some(score_str)) =
                    (arr[i].as_string(), arr[i + 1].as_string())
                {
                    let score: f64 = score_str.parse().unwrap_or(0.0);
                    let mut obj = Map::new();
                    obj.insert("member".to_string(), Value::String(member));
                    obj.insert("score".to_string(), json_number(score));
                    members.push(Value::Object(obj));
                }
                i += 2;
            }
            Ok(Value::Array(members))
        }
        None => Ok(Value::Array(vec![])),
    }
}

/// Export a stream value
fn export_stream(conn: &mut Connection, key: &str) -> Result<Value, String> {
    let response = conn.execute_raw(&["XRANGE", key, "-", "+"])?;
    match response.as_array() {
        Some(entries) => {
            let mut result = Vec::new();
            for entry in entries {
                if let Some(entry_arr) = entry.as_array() {
                    if entry_arr.len() >= 2 {
                        let id = entry_arr[0].as_string().unwrap_or_default();
                        let mut entry_obj = Map::new();
                        entry_obj.insert("id".to_string(), Value::String(id));

                        if let Some(fields) = entry_arr[1].as_array() {
                            let mut fields_map = Map::new();
                            let mut i = 0;
                            while i + 1 < fields.len() {
                                if let (Some(f), Some(v)) =
                                    (fields[i].as_string(), fields[i + 1].as_string())
                                {
                                    fields_map.insert(f, Value::String(v));
                                }
                                i += 2;
                            }
                            entry_obj.insert("fields".to_string(), Value::Object(fields_map));
                        }
                        result.push(Value::Object(entry_obj));
                    }
                }
            }
            Ok(Value::Array(result))
        }
        None => Ok(Value::Array(vec![])),
    }
}

fn json_number(n: f64) -> Value {
    serde_json::Number::from_f64(n)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Export a key with its value and metadata
fn export_key(conn: &mut Connection, key: &str, include_ttl: bool) -> Result<Value, String> {
    let key_type = get_type(conn, key)?;

    let value = match key_type.as_str() {
        "string" => export_string(conn, key)?,
        "list" => export_list(conn, key)?,
        "set" => export_set(conn, key)?,
        "hash" => export_hash(conn, key)?,
        "zset" => export_zset(conn, key)?,
        "stream" => export_stream(conn, key)?,
        "none" => return Ok(Value::Null),
        _ => {
            // Unknown type, try DUMP
            Value::Null
        }
    };

    let mut obj = Map::new();
    obj.insert("type".to_string(), Value::String(key_type));
    obj.insert("value".to_string(), value);

    if include_ttl {
        let ttl = get_ttl(conn, key)?;
        if ttl > 0 {
            obj.insert("ttl".to_string(), Value::Number(ttl.into()));
        }
    }

    Ok(Value::Object(obj))
}

fn print_usage() {
    println!(
        r#"Usage: viator-dump [OPTIONS]

Export Viator/Redis data to JSON format.

Options:
  -h <hostname>      Server hostname (default: 127.0.0.1)
  -p <port>          Server port (default: 6379)
  -a <password>      Password for AUTH
  -n <database>      Database number (default: 0)
  -k <pattern>       Key pattern to export (default: *)
  -o <file>          Output file (default: stdout)
  --pretty           Pretty-print JSON output
  --no-ttl           Don't include TTL information
  --help             Show this help message

Examples:
  viator-dump                           Export all keys to stdout
  viator-dump -o dump.json              Export to file
  viator-dump -k "user:*" --pretty      Export user keys with formatting
  viator-dump -n 1 -o db1.json          Export database 1

Output Format:
  {{
    "key1": {{ "type": "string", "value": "...", "ttl": 3600 }},
    "key2": {{ "type": "hash", "value": {{ "field": "value" }} }},
    ...
  }}
"#
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
                    config.database = args[i].parse().unwrap_or(0);
                }
            }
            "-k" => {
                i += 1;
                if i < args.len() {
                    config.pattern = args[i].clone();
                }
            }
            "-o" => {
                i += 1;
                if i < args.len() {
                    config.output = Some(args[i].clone());
                }
            }
            "--pretty" => {
                config.pretty = true;
            }
            "--no-ttl" => {
                config.include_ttl = false;
            }
            "--help" | "-?" => {
                print_usage();
                return;
            }
            _ => {}
        }
        i += 1;
    }

    // Connect to server
    let mut conn = match Connection::connect(&config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    eprintln!(
        "Connected to {}:{}, database {}",
        config.host, config.port, config.database
    );

    // Scan all keys
    eprintln!("Scanning keys matching '{}'...", config.pattern);
    let keys = match scan_keys(&mut conn, &config.pattern) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error scanning keys: {}", e);
            std::process::exit(1);
        }
    };

    eprintln!("Found {} keys", keys.len());

    // Export all keys
    let mut data: HashMap<String, Value> = HashMap::new();
    let mut exported = 0;
    let mut errors = 0;

    for key in &keys {
        match export_key(&mut conn, key, config.include_ttl) {
            Ok(value) => {
                if !value.is_null() {
                    data.insert(key.clone(), value);
                    exported += 1;
                }
            }
            Err(e) => {
                eprintln!("Warning: Failed to export '{}': {}", key, e);
                errors += 1;
            }
        }

        if (exported + errors) % 1000 == 0 {
            eprint!("\rExported {} keys...", exported);
        }
    }
    eprintln!("\rExported {} keys ({} errors)", exported, errors);

    // Write output
    let json_output = if config.pretty {
        serde_json::to_string_pretty(&data)
    } else {
        serde_json::to_string(&data)
    };

    let json_str = match json_output {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error serializing JSON: {}", e);
            std::process::exit(1);
        }
    };

    match &config.output {
        Some(path) => {
            let file = match File::create(path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Error creating file '{}': {}", path, e);
                    std::process::exit(1);
                }
            };
            let mut writer = BufWriter::new(file);
            if let Err(e) = writer.write_all(json_str.as_bytes()) {
                eprintln!("Error writing to file: {}", e);
                std::process::exit(1);
            }
            eprintln!("Written to {}", path);
        }
        None => {
            println!("{}", json_str);
        }
    }
}
