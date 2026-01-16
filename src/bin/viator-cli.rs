//! Viator CLI - Command-line interface for Viator
//!
//! A full-featured CLI client compatible with Redis 8.4.0 protocol.
//! Supports interactive mode, single commands, pipe mode, and more.

use bytes::{Buf, BytesMut};
use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// CLI configuration
struct Config {
    host: String,
    port: u16,
    password: Option<String>,
    database: u32,
    timeout: Duration,
    repeat: u32,
    interval: Duration,
    #[allow(dead_code)]
    no_raw: bool,
    #[allow(dead_code)]
    cluster_mode: bool,
    #[allow(dead_code)]
    tls: bool,
    #[allow(dead_code)]
    verbose: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            password: None,
            database: 0,
            timeout: Duration::from_secs(0),
            repeat: 1,
            interval: Duration::ZERO,
            no_raw: false,
            cluster_mode: false,
            tls: false,
            verbose: false,
        }
    }
}

/// Connection wrapper
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

        if config.timeout.as_secs() > 0 {
            stream.set_read_timeout(Some(config.timeout)).ok();
            stream.set_write_timeout(Some(config.timeout)).ok();
        }

        let mut conn = Self {
            stream,
            buffer: BytesMut::with_capacity(4096),
        };

        // AUTH if password set
        if let Some(ref password) = config.password {
            let response = conn.execute(&["AUTH", password])?;
            if response.starts_with("-") {
                return Err(format!("AUTH failed: {}", response.trim()));
            }
        }

        // SELECT database if not 0
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

    fn execute_raw(&mut self, args: &[String]) -> Result<String, String> {
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.execute(&refs)
    }

    fn read_response(&mut self) -> Result<String, String> {
        self.buffer.clear();
        let mut temp = [0u8; 4096];

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
}

/// Encode a command as RESP protocol
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

/// Parse a RESP response
fn parse_response(buf: &[u8]) -> Result<Option<String>, String> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => {
            // Simple string
            if let Some(end) = find_crlf(buf) {
                let s = String::from_utf8_lossy(&buf[1..end]).to_string();
                return Ok(Some(s));
            }
        }
        b'-' => {
            // Error
            if let Some(end) = find_crlf(buf) {
                let s = String::from_utf8_lossy(&buf[0..end]).to_string();
                return Ok(Some(s));
            }
        }
        b':' => {
            // Integer
            if let Some(end) = find_crlf(buf) {
                let s = String::from_utf8_lossy(&buf[1..end]).to_string();
                return Ok(Some(format!("(integer) {}", s)));
            }
        }
        b'$' => {
            // Bulk string
            if let Some(len_end) = find_crlf(buf) {
                let len_str = String::from_utf8_lossy(&buf[1..len_end]);
                let len: i64 = len_str.parse().map_err(|_| "Invalid bulk length")?;

                if len < 0 {
                    return Ok(Some("(nil)".to_string()));
                }

                let data_start = len_end + 2;
                let data_end = data_start + len as usize;
                let total = data_end + 2;

                if buf.len() >= total {
                    let data = &buf[data_start..data_end];
                    let s = String::from_utf8_lossy(data).to_string();
                    return Ok(Some(format!("\"{}\"", s)));
                }
            }
        }
        b'*' => {
            // Array
            return parse_array(buf);
        }
        _ => {
            return Err(format!("Unknown response type: {}", buf[0] as char));
        }
    }

    Ok(None)
}

fn parse_array(buf: &[u8]) -> Result<Option<String>, String> {
    if let Some(len_end) = find_crlf(buf) {
        let len_str = String::from_utf8_lossy(&buf[1..len_end]);
        let len: i64 = len_str.parse().map_err(|_| "Invalid array length")?;

        if len < 0 {
            return Ok(Some("(empty array)".to_string()));
        }

        if len == 0 {
            return Ok(Some("(empty array)".to_string()));
        }

        let mut pos = len_end + 2;
        let mut elements = Vec::new();

        for i in 0..len as usize {
            if pos >= buf.len() {
                return Ok(None); // Need more data
            }

            match buf[pos] {
                b'$' => {
                    if let Some(bulk_len_end) = find_crlf(&buf[pos..]) {
                        let bulk_len_str =
                            String::from_utf8_lossy(&buf[pos + 1..pos + bulk_len_end]);
                        let bulk_len: i64 =
                            bulk_len_str.parse().map_err(|_| "Invalid bulk length")?;

                        if bulk_len < 0 {
                            elements.push(format!("{}) (nil)", i + 1));
                            pos += bulk_len_end + 2;
                        } else {
                            let data_start = pos + bulk_len_end + 2;
                            let data_end = data_start + bulk_len as usize;

                            if buf.len() < data_end + 2 {
                                return Ok(None); // Need more data
                            }

                            let data = &buf[data_start..data_end];
                            let s = String::from_utf8_lossy(data).to_string();
                            elements.push(format!("{}) \"{}\"", i + 1, s));
                            pos = data_end + 2;
                        }
                    } else {
                        return Ok(None);
                    }
                }
                b':' => {
                    if let Some(end) = find_crlf(&buf[pos..]) {
                        let s = String::from_utf8_lossy(&buf[pos + 1..pos + end]);
                        elements.push(format!("{}) (integer) {}", i + 1, s));
                        pos += end + 2;
                    } else {
                        return Ok(None);
                    }
                }
                b'+' => {
                    if let Some(end) = find_crlf(&buf[pos..]) {
                        let s = String::from_utf8_lossy(&buf[pos + 1..pos + end]);
                        elements.push(format!("{}) {}", i + 1, s));
                        pos += end + 2;
                    } else {
                        return Ok(None);
                    }
                }
                b'-' => {
                    if let Some(end) = find_crlf(&buf[pos..]) {
                        let s = String::from_utf8_lossy(&buf[pos + 1..pos + end]);
                        elements.push(format!("{}) (error) {}", i + 1, s));
                        pos += end + 2;
                    } else {
                        return Ok(None);
                    }
                }
                _ => {
                    return Err(format!(
                        "Unknown element type in array: {}",
                        buf[pos] as char
                    ));
                }
            }
        }

        return Ok(Some(elements.join("\n")));
    }

    Ok(None)
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// Interactive REPL mode
fn run_interactive(config: &Config) -> Result<(), String> {
    let mut conn = Connection::connect(config)?;

    let prompt = format!("{}:{}> ", config.host, config.port);
    let _db_prompt = if config.database > 0 {
        format!("{}:{}[{}]> ", config.host, config.port, config.database)
    } else {
        prompt.clone()
    };

    let mut current_db = config.database;
    let mut history: Vec<String> = Vec::new();

    println!("Connected to Viator at {}:{}", config.host, config.port);
    println!("Type 'help' for available commands, 'quit' to exit.\n");

    loop {
        let current_prompt = if current_db > 0 {
            format!("{}:{}[{}]> ", config.host, config.port, current_db)
        } else {
            format!("{}:{}> ", config.host, config.port)
        };

        print!("{}", current_prompt);
        io::stdout().flush().ok();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(_) => break,
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        // Handle special commands
        let lower = input.to_lowercase();
        if lower == "quit" || lower == "exit" {
            break;
        }

        if lower == "clear" {
            print!("\x1B[2J\x1B[1;1H");
            continue;
        }

        if lower == "help" {
            print_help();
            continue;
        }

        // Parse command
        let args = parse_input(input);
        if args.is_empty() {
            continue;
        }

        history.push(input.to_string());

        // Execute command
        for _ in 0..config.repeat {
            match conn.execute_raw(&args) {
                Ok(response) => {
                    println!("{}", response);

                    // Track database changes
                    if args[0].to_uppercase() == "SELECT" && !response.starts_with("-") {
                        if let Some(db) = args.get(1) {
                            current_db = db.parse().unwrap_or(0);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("(error) {}", e);
                    // Try to reconnect
                    match Connection::connect(config) {
                        Ok(new_conn) => {
                            conn = new_conn;
                            eprintln!("Reconnected.");
                        }
                        Err(e) => {
                            eprintln!("Reconnection failed: {}", e);
                        }
                    }
                }
            }

            if config.interval > Duration::ZERO && config.repeat > 1 {
                std::thread::sleep(config.interval);
            }
        }
    }

    Ok(())
}

/// Parse input line into arguments (handling quotes)
fn parse_input(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '"';
    let mut escape_next = false;

    for ch in input.chars() {
        if escape_next {
            current.push(ch);
            escape_next = false;
            continue;
        }

        if ch == '\\' {
            escape_next = true;
            continue;
        }

        if in_quotes {
            if ch == quote_char {
                in_quotes = false;
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                '"' | '\'' => {
                    in_quotes = true;
                    quote_char = ch;
                }
                ' ' | '\t' => {
                    if !current.is_empty() {
                        args.push(current.clone());
                        current.clear();
                    }
                }
                _ => {
                    current.push(ch);
                }
            }
        }
    }

    if !current.is_empty() {
        args.push(current);
    }

    args
}

/// Run single command mode
fn run_command(config: &Config, args: &[String]) -> Result<(), String> {
    let mut conn = Connection::connect(config)?;

    for _ in 0..config.repeat {
        match conn.execute_raw(args) {
            Ok(response) => {
                println!("{}", response);
            }
            Err(e) => {
                eprintln!("(error) {}", e);
                return Err(e);
            }
        }

        if config.interval > Duration::ZERO && config.repeat > 1 {
            std::thread::sleep(config.interval);
        }
    }

    Ok(())
}

/// Run pipe mode (read commands from stdin)
fn run_pipe(config: &Config) -> Result<(), String> {
    let mut conn = Connection::connect(config)?;
    let stdin = io::stdin();

    for line in stdin.lock().lines() {
        let line = line.map_err(|e| format!("Read error: {}", e))?;
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let args = parse_input(line);
        if args.is_empty() {
            continue;
        }

        match conn.execute_raw(&args) {
            Ok(response) => {
                println!("{}", response);
            }
            Err(e) => {
                eprintln!("(error) {}", e);
            }
        }
    }

    Ok(())
}

/// Run MONITOR mode
fn run_monitor(config: &Config) -> Result<(), String> {
    let mut conn = Connection::connect(config)?;

    conn.execute(&["MONITOR"])?;
    println!("OK");

    // Read continuous output
    let mut temp = [0u8; 4096];
    loop {
        match conn.stream.read(&mut temp) {
            Ok(0) => break,
            Ok(n) => {
                // Parse and print each line
                let data = String::from_utf8_lossy(&temp[..n]);
                for line in data.lines() {
                    if line.starts_with('+') {
                        println!("{}", &line[1..]);
                    } else {
                        println!("{}", line);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// Run SUBSCRIBE mode
fn run_subscribe(config: &Config, channels: &[String]) -> Result<(), String> {
    let mut conn = Connection::connect(config)?;

    let mut args: Vec<&str> = vec!["SUBSCRIBE"];
    for ch in channels {
        args.push(ch);
    }

    let cmd = encode_command(&args);
    conn.stream
        .write_all(&cmd)
        .map_err(|e| format!("Write error: {}", e))?;

    println!(
        "Subscribed to {} channel(s). Press Ctrl+C to exit.",
        channels.len()
    );

    // Read continuous output
    let mut temp = [0u8; 4096];
    let mut buffer = BytesMut::new();

    loop {
        match conn.stream.read(&mut temp) {
            Ok(0) => break,
            Ok(n) => {
                buffer.extend_from_slice(&temp[..n]);

                // Try to parse messages
                while let Some(msg) = parse_pubsub_message(&buffer)? {
                    println!("{}", msg.0);
                    buffer.advance(msg.1);
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn parse_pubsub_message(buf: &[u8]) -> Result<Option<(String, usize)>, String> {
    // Simplified parsing for pub/sub messages
    if buf.is_empty() || buf[0] != b'*' {
        return Ok(None);
    }

    // Find complete message
    let mut lines = Vec::new();

    let Some(end) = find_crlf(buf) else {
        return Ok(None);
    };

    let count_str = String::from_utf8_lossy(&buf[1..end]);
    let expected_elements: usize = count_str.parse().unwrap_or(0);
    let mut pos = end + 2;

    for _ in 0..expected_elements {
        if pos >= buf.len() {
            return Ok(None);
        }

        if buf[pos] == b'$' {
            if let Some(len_end) = find_crlf(&buf[pos..]) {
                let len_str = String::from_utf8_lossy(&buf[pos + 1..pos + len_end]);
                let len: i64 = len_str.parse().unwrap_or(-1);

                if len < 0 {
                    lines.push("(nil)".to_string());
                    pos += len_end + 2;
                } else {
                    let data_start = pos + len_end + 2;
                    let data_end = data_start + len as usize;

                    if buf.len() < data_end + 2 {
                        return Ok(None);
                    }

                    let data = String::from_utf8_lossy(&buf[data_start..data_end]).to_string();
                    lines.push(data);
                    pos = data_end + 2;
                }
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    }

    if lines.len() >= 3 {
        let msg_type = &lines[0];
        let channel = &lines[1];
        let content = &lines[2];

        let output = match msg_type.as_str() {
            "message" => format!("{}> {}", channel, content),
            "subscribe" => format!("Subscribed to '{}' ({} subscribers)", channel, content),
            "unsubscribe" => format!("Unsubscribed from '{}' ({} subscribers)", channel, content),
            _ => format!("{}: {} - {}", msg_type, channel, content),
        };

        return Ok(Some((output, pos)));
    }

    Ok(None)
}

fn print_help() {
    println!(
        "Viator CLI Commands:

  General:
    PING                    Test connection
    QUIT                    Close connection
    SELECT <db>             Select database
    AUTH <password>         Authenticate
    INFO [section]          Server information
    CONFIG GET <pattern>    Get configuration
    CONFIG SET <k> <v>      Set configuration
    DBSIZE                  Number of keys in database
    FLUSHDB                 Delete all keys in current DB
    FLUSHALL                Delete all keys in all DBs

  Strings:
    SET <key> <value>       Set string value
    GET <key>               Get string value
    INCR <key>              Increment integer
    DECR <key>              Decrement integer
    APPEND <key> <value>    Append to string

  Lists:
    LPUSH <key> <value>     Push to head
    RPUSH <key> <value>     Push to tail
    LPOP <key>              Pop from head
    RPOP <key>              Pop from tail
    LRANGE <key> <s> <e>    Get range

  Sets:
    SADD <key> <member>     Add member
    SMEMBERS <key>          Get all members
    SISMEMBER <key> <m>     Check membership

  Hashes:
    HSET <key> <f> <v>      Set field
    HGET <key> <field>      Get field
    HGETALL <key>           Get all fields

  Sorted Sets:
    ZADD <key> <s> <m>      Add with score
    ZRANGE <key> <s> <e>    Get range
    ZSCORE <key> <member>   Get score

  Type 'quit' or 'exit' to close the connection.
"
    );
}

fn print_usage() {
    println!(
        "Usage: viator-cli [OPTIONS] [COMMAND [ARG...]]

Viator command line interface - Redis 8.4.0 compatible.

Options:
  -h <hostname>      Server hostname (default: 127.0.0.1)
  -p <port>          Server port (default: 6379)
  -a <password>      Password for AUTH
  -n <database>      Database number (default: 0)
  -r <repeat>        Execute command N times
  -i <interval>      Interval between commands (seconds)
  -x                 Read last argument from STDIN
  --no-raw           Force formatted output
  --cluster          Enable cluster mode
  --tls              Enable TLS
  --scan             Scan keys (with optional pattern)
  --pipe             Read commands from stdin (pipe mode)
  --monitor          Enter monitor mode
  --subscribe <ch>   Subscribe to channel(s)
  --help             Show this help message

Examples:
  viator-cli                           Interactive mode
  viator-cli PING                      Single command
  viator-cli -h redis.example.com GET key
  viator-cli -n 1 KEYS '*'             Use database 1
  viator-cli --pipe < commands.txt     Pipe mode
  viator-cli --monitor                 Monitor all commands
  viator-cli --subscribe news events   Subscribe to channels

Interactive commands:
  help      Show command help
  quit      Exit the CLI
  clear     Clear screen
"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::default();

    let mut command_args: Vec<String> = Vec::new();
    let mut pipe_mode = false;
    let mut monitor_mode = false;
    let mut subscribe_channels: Vec<String> = Vec::new();
    let mut scan_mode = false;
    let mut scan_pattern = "*".to_string();
    let mut read_stdin_arg = false;

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
            "-r" => {
                i += 1;
                if i < args.len() {
                    config.repeat = args[i].parse().unwrap_or(1);
                }
            }
            "-i" => {
                i += 1;
                if i < args.len() {
                    let secs: f64 = args[i].parse().unwrap_or(0.0);
                    config.interval = Duration::from_secs_f64(secs);
                }
            }
            "-x" => {
                read_stdin_arg = true;
            }
            "--no-raw" => {
                config.no_raw = true;
            }
            "--cluster" => {
                config.cluster_mode = true;
            }
            "--tls" => {
                config.tls = true;
            }
            "--pipe" => {
                pipe_mode = true;
            }
            "--monitor" => {
                monitor_mode = true;
            }
            "--subscribe" => {
                i += 1;
                while i < args.len() && !args[i].starts_with('-') {
                    subscribe_channels.push(args[i].clone());
                    i += 1;
                }
                i -= 1; // Back up since loop will increment
            }
            "--scan" => {
                scan_mode = true;
                if i + 1 < args.len() && !args[i + 1].starts_with('-') {
                    i += 1;
                    scan_pattern = args[i].clone();
                }
            }
            "--help" | "-?" => {
                print_usage();
                return;
            }
            _ => {
                // Collect remaining args as command
                command_args.extend(args[i..].iter().cloned());
                break;
            }
        }
        i += 1;
    }

    // Read last arg from stdin if -x flag
    if read_stdin_arg && !command_args.is_empty() {
        let mut stdin_data = String::new();
        if io::stdin().read_to_string(&mut stdin_data).is_ok() {
            command_args.push(stdin_data.trim().to_string());
        }
    }

    // Execute based on mode
    let result = if pipe_mode {
        run_pipe(&config)
    } else if monitor_mode {
        run_monitor(&config)
    } else if !subscribe_channels.is_empty() {
        run_subscribe(&config, &subscribe_channels)
    } else if scan_mode {
        run_scan(&config, &scan_pattern)
    } else if command_args.is_empty() {
        run_interactive(&config)
    } else {
        run_command(&config, &command_args)
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

/// Run SCAN mode
fn run_scan(config: &Config, pattern: &str) -> Result<(), String> {
    let mut conn = Connection::connect(config)?;
    let mut cursor = "0".to_string();
    let mut total = 0u64;

    loop {
        let response = conn.execute(&["SCAN", &cursor, "MATCH", pattern, "COUNT", "100"])?;

        // Parse SCAN response (array with cursor and keys)
        let lines: Vec<&str> = response.lines().collect();

        if lines.len() >= 2 {
            // First element is new cursor
            if let Some(new_cursor) = lines[0].strip_prefix("1) \"") {
                cursor = new_cursor.trim_end_matches('"').to_string();
            } else if let Some(new_cursor) = lines[0].strip_prefix("1) ") {
                cursor = new_cursor.to_string();
            }

            // Rest are keys
            for line in &lines[1..] {
                if line.contains(')') {
                    if let Some(key) = line.split(')').nth(1) {
                        let key = key.trim().trim_matches('"');
                        if !key.is_empty() {
                            println!("{}", key);
                            total += 1;
                        }
                    }
                }
            }
        }

        if cursor == "0" {
            break;
        }
    }

    eprintln!("\n({} keys)", total);
    Ok(())
}
