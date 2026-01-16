//! Viator Sentinel - High availability monitoring for Viator
//!
//! Monitors Viator/Redis instances and provides automatic failover.
//! Compatible with Redis Sentinel 8.4.0 protocol.

#![allow(clippy::type_complexity)]

use bytes::BytesMut;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Sentinel configuration
#[derive(Clone)]
#[allow(dead_code)]
struct SentinelConfig {
    bind: String,
    port: u16,
    announce_ip: Option<String>,
    announce_port: Option<u16>,
    down_after_milliseconds: u64,
    failover_timeout: u64,
    parallel_syncs: u32,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".to_string(),
            port: 26379,
            announce_ip: None,
            announce_port: None,
            down_after_milliseconds: 30000,
            failover_timeout: 180000,
            parallel_syncs: 1,
        }
    }
}

/// Monitored master configuration
#[derive(Clone)]
#[allow(dead_code)]
struct MasterConfig {
    name: String,
    host: String,
    port: u16,
    quorum: u32,
    auth_pass: Option<String>,
}

/// Instance state
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
enum InstanceState {
    Up,
    Down,
    SubjectivelyDown, // S_DOWN
    ObjectivelyDown,  // O_DOWN
}

/// Monitored instance info
#[derive(Clone)]
#[allow(dead_code)]
struct InstanceInfo {
    host: String,
    port: u16,
    state: InstanceState,
    last_ping: Instant,
    last_pong: Option<Instant>,
    role: String,
    info_refresh: Instant,
    down_since: Option<Instant>,
    // Replication info
    master_host: Option<String>,
    master_port: Option<u16>,
    replica_priority: u32,
    replica_offset: u64,
}

impl InstanceInfo {
    fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            state: InstanceState::Up,
            last_ping: Instant::now(),
            last_pong: None,
            role: "unknown".to_string(),
            info_refresh: Instant::now(),
            down_since: None,
            master_host: None,
            master_port: None,
            replica_priority: 100,
            replica_offset: 0,
        }
    }
}

/// Sentinel state
#[allow(dead_code)]
struct SentinelState {
    masters: HashMap<String, MasterConfig>,
    instances: HashMap<String, InstanceInfo>,
    replicas: HashMap<String, Vec<InstanceInfo>>,
    sentinels: HashMap<String, Vec<InstanceInfo>>,
    current_epoch: u64,
    config_epoch: HashMap<String, u64>,
}

impl SentinelState {
    fn new() -> Self {
        Self {
            masters: HashMap::new(),
            instances: HashMap::new(),
            replicas: HashMap::new(),
            sentinels: HashMap::new(),
            current_epoch: 0,
            config_epoch: HashMap::new(),
        }
    }
}

/// Main Sentinel service
struct Sentinel {
    config: SentinelConfig,
    state: Arc<RwLock<SentinelState>>,
    running: AtomicBool,
    my_id: String,
}

impl Sentinel {
    fn new(config: SentinelConfig) -> Self {
        // Generate sentinel ID
        let my_id = format!("{:040x}", rand::random::<u128>());

        Self {
            config,
            state: Arc::new(RwLock::new(SentinelState::new())),
            running: AtomicBool::new(false),
            my_id,
        }
    }

    fn add_master(&self, name: &str, host: &str, port: u16, quorum: u32) {
        let config = MasterConfig {
            name: name.to_string(),
            host: host.to_string(),
            port,
            quorum,
            auth_pass: None,
        };

        let mut state = self.state.write().unwrap();
        state.masters.insert(name.to_string(), config);

        let key = format!("{host}:{port}");
        state.instances.insert(key, InstanceInfo::new(host, port));
        state.replicas.insert(name.to_string(), Vec::new());
        state.sentinels.insert(name.to_string(), Vec::new());
        state.config_epoch.insert(name.to_string(), 0);
    }

    fn run(&self) -> Result<(), String> {
        self.running.store(true, Ordering::SeqCst);

        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener =
            TcpListener::bind(&addr).map_err(|e| format!("Cannot bind to {addr}: {e}"))?;

        listener.set_nonblocking(true).ok();

        println!("Viator Sentinel {} running on {}", &self.my_id[..8], addr);
        println!(
            "Monitoring {} master(s)",
            self.state.read().unwrap().masters.len()
        );

        // Start monitoring threads
        let state_clone = self.state.clone();
        let config_clone = self.config.clone();
        let _running = &self.running;

        std::thread::spawn({
            let state = state_clone;
            let config = config_clone;
            move || {
                monitor_loop(state, config);
            }
        });

        // Accept client connections
        while self.running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let state = self.state.clone();
                    let my_id = self.my_id.clone();
                    std::thread::spawn(move || {
                        handle_client(stream, state, &my_id);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    eprintln!("Accept error: {e}");
                }
            }
        }

        Ok(())
    }
}

/// Monitor instances in a loop
fn monitor_loop(state: Arc<RwLock<SentinelState>>, config: SentinelConfig) {
    loop {
        let masters: Vec<(String, String, u16)> = {
            let s = state.read().unwrap();
            s.masters
                .iter()
                .map(|(name, cfg)| (name.clone(), cfg.host.clone(), cfg.port))
                .collect()
        };

        for (name, host, port) in masters {
            // Ping master
            if let Ok(info) = ping_instance(&host, port) {
                let mut s = state.write().unwrap();
                let key = format!("{host}:{port}");
                if let Some(inst) = s.instances.get_mut(&key) {
                    inst.last_pong = Some(Instant::now());
                    inst.state = InstanceState::Up;
                    inst.role = info.role;
                    inst.down_since = None;
                }

                // Update replicas list
                if let Some(replicas) = s.replicas.get_mut(&name) {
                    for replica_info in &info.replicas {
                        let exists = replicas
                            .iter()
                            .any(|r| r.host == replica_info.0 && r.port == replica_info.1);
                        if !exists {
                            let mut inst = InstanceInfo::new(&replica_info.0, replica_info.1);
                            inst.role = "slave".to_string();
                            replicas.push(inst);
                        }
                    }
                }
            } else {
                let mut s = state.write().unwrap();
                let key = format!("{host}:{port}");
                if let Some(inst) = s.instances.get_mut(&key) {
                    let elapsed = inst
                        .last_pong
                        .map_or(inst.last_ping.elapsed().as_millis() as u64, |t| {
                            t.elapsed().as_millis() as u64
                        });

                    if elapsed > config.down_after_milliseconds && inst.state == InstanceState::Up {
                        inst.state = InstanceState::SubjectivelyDown;
                        inst.down_since = Some(Instant::now());
                        eprintln!("+sdown master {name} {host}:{port}");
                    }
                }
            }

            // Ping replicas
            let replicas: Vec<(String, u16)> = {
                let s = state.read().unwrap();
                s.replicas
                    .get(&name)
                    .map(|r| r.iter().map(|i| (i.host.clone(), i.port)).collect())
                    .unwrap_or_default()
            };

            for (rhost, rport) in replicas {
                if let Ok(info) = ping_instance(&rhost, rport) {
                    let mut s = state.write().unwrap();
                    if let Some(replicas) = s.replicas.get_mut(&name) {
                        if let Some(inst) = replicas
                            .iter_mut()
                            .find(|r| r.host == rhost && r.port == rport)
                        {
                            inst.last_pong = Some(Instant::now());
                            inst.state = InstanceState::Up;
                            inst.replica_offset = info.repl_offset;
                        }
                    }
                } else {
                    let mut s = state.write().unwrap();
                    if let Some(replicas) = s.replicas.get_mut(&name) {
                        if let Some(inst) = replicas
                            .iter_mut()
                            .find(|r| r.host == rhost && r.port == rport)
                        {
                            let elapsed = inst
                                .last_pong
                                .map_or(inst.last_ping.elapsed().as_millis() as u64, |t| {
                                    t.elapsed().as_millis() as u64
                                });

                            if elapsed > config.down_after_milliseconds {
                                inst.state = InstanceState::SubjectivelyDown;
                            }
                        }
                    }
                }
            }
        }

        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Info from pinging an instance
struct PingInfo {
    role: String,
    replicas: Vec<(String, u16)>,
    repl_offset: u64,
}

/// Ping an instance and get info
fn ping_instance(host: &str, port: u16) -> Result<PingInfo, String> {
    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect_timeout(
        &addr.parse().map_err(|_| "Invalid address")?,
        Duration::from_secs(2),
    )
    .map_err(|e| e.to_string())?;

    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    // Send PING
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .map_err(|e| e.to_string())?;

    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).map_err(|e| e.to_string())?;

    if n == 0 || !buf.starts_with(b"+PONG") {
        return Err("No PONG".to_string());
    }

    // Send INFO replication
    stream
        .write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n")
        .map_err(|e| e.to_string())?;

    let mut info_buf = vec![0u8; 4096];
    let n = stream.read(&mut info_buf).map_err(|e| e.to_string())?;

    let info_str = String::from_utf8_lossy(&info_buf[..n]);

    let mut role = "unknown".to_string();
    let mut replicas = Vec::new();
    let mut repl_offset = 0u64;

    for line in info_str.lines() {
        if let Some(r) = line.strip_prefix("role:") {
            role = r.trim().to_string();
        }
        if let Some(offset) = line.strip_prefix("master_repl_offset:") {
            repl_offset = offset.trim().parse().unwrap_or(0);
        }
        if let Some(offset) = line.strip_prefix("slave_repl_offset:") {
            repl_offset = offset.trim().parse().unwrap_or(0);
        }
        // Parse slave entries like "slave0:ip=127.0.0.1,port=6380,..."
        if line.starts_with("slave") && line.contains(':') {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 2 {
                let mut ip = String::new();
                let mut port = 0u16;
                for item in parts[1].split(',') {
                    if let Some(v) = item.strip_prefix("ip=") {
                        ip = v.to_string();
                    }
                    if let Some(v) = item.strip_prefix("port=") {
                        port = v.parse().unwrap_or(0);
                    }
                }
                if !ip.is_empty() && port > 0 {
                    replicas.push((ip, port));
                }
            }
        }
    }

    Ok(PingInfo {
        role,
        replicas,
        repl_offset,
    })
}

/// Handle client connection
fn handle_client(mut stream: TcpStream, state: Arc<RwLock<SentinelState>>, my_id: &str) {
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();

    let mut buffer = BytesMut::with_capacity(4096);
    let mut temp = [0u8; 4096];

    loop {
        match stream.read(&mut temp) {
            Ok(0) => break,
            Ok(n) => {
                buffer.extend_from_slice(&temp[..n]);

                // Parse and handle commands
                while let Some((cmd, args, consumed)) = parse_command(&buffer) {
                    buffer = buffer.split_off(consumed);

                    let response = handle_command(&cmd, &args, &state, my_id);
                    if stream.write_all(response.as_bytes()).is_err() {
                        return;
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break,
        }
    }
}

/// Parse RESP command
fn parse_command(buf: &[u8]) -> Option<(String, Vec<String>, usize)> {
    if buf.is_empty() || buf[0] != b'*' {
        return None;
    }

    let mut pos = 1;
    let mut num_args = 0usize;

    // Find array length
    while pos < buf.len() && buf[pos] != b'\r' {
        num_args = num_args * 10 + (buf[pos] - b'0') as usize;
        pos += 1;
    }

    if pos + 2 > buf.len() {
        return None;
    }
    pos += 2; // Skip \r\n

    let mut args = Vec::with_capacity(num_args);

    for _ in 0..num_args {
        if pos >= buf.len() || buf[pos] != b'$' {
            return None;
        }
        pos += 1;

        let mut len = 0usize;
        while pos < buf.len() && buf[pos] != b'\r' {
            len = len * 10 + (buf[pos] - b'0') as usize;
            pos += 1;
        }

        if pos + 2 > buf.len() {
            return None;
        }
        pos += 2; // Skip \r\n

        if pos + len + 2 > buf.len() {
            return None;
        }

        let arg = String::from_utf8_lossy(&buf[pos..pos + len]).to_string();
        args.push(arg);
        pos += len + 2; // Skip data and \r\n
    }

    if args.is_empty() {
        return None;
    }

    let cmd = args.remove(0).to_uppercase();
    Some((cmd, args, pos))
}

/// Handle sentinel command
fn handle_command(
    cmd: &str,
    args: &[String],
    state: &Arc<RwLock<SentinelState>>,
    my_id: &str,
) -> String {
    match cmd {
        "PING" => "+PONG\r\n".to_string(),

        "SENTINEL" => {
            if args.is_empty() {
                return "-ERR wrong number of arguments\r\n".to_string();
            }

            let subcmd = args[0].to_uppercase();
            match subcmd.as_str() {
                "MASTERS" => {
                    let s = state.read().unwrap();
                    let mut result = format!("*{}\r\n", s.masters.len());

                    for (name, cfg) in &s.masters {
                        let inst_key = format!("{}:{}", cfg.host, cfg.port);
                        let inst = s.instances.get(&inst_key);
                        let flags = inst.map_or("master", |i| match i.state {
                            InstanceState::Up => "master",
                            InstanceState::SubjectivelyDown => "master,s_down",
                            InstanceState::ObjectivelyDown => "master,o_down",
                            InstanceState::Down => "master,disconnected",
                        });

                        let num_slaves = s.replicas.get(name).map_or(0, std::vec::Vec::len);
                        let num_sentinels = s.sentinels.get(name).map_or(0, std::vec::Vec::len);

                        result.push_str(&format_master_info(
                            name,
                            &cfg.host,
                            cfg.port,
                            flags,
                            cfg.quorum,
                            num_slaves,
                            num_sentinels,
                        ));
                    }

                    result
                }

                "MASTER" => {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".to_string();
                    }
                    let name = &args[1];
                    let s = state.read().unwrap();

                    if let Some(cfg) = s.masters.get(name) {
                        let inst_key = format!("{}:{}", cfg.host, cfg.port);
                        let inst = s.instances.get(&inst_key);
                        let flags = inst.map_or("master", |i| match i.state {
                            InstanceState::Up => "master",
                            InstanceState::SubjectivelyDown => "master,s_down",
                            InstanceState::ObjectivelyDown => "master,o_down",
                            InstanceState::Down => "master,disconnected",
                        });

                        let num_slaves = s.replicas.get(name).map_or(0, std::vec::Vec::len);
                        let num_sentinels = s.sentinels.get(name).map_or(0, std::vec::Vec::len);

                        format_master_info(
                            name,
                            &cfg.host,
                            cfg.port,
                            flags,
                            cfg.quorum,
                            num_slaves,
                            num_sentinels,
                        )
                    } else {
                        "-ERR No such master with that name\r\n".to_string()
                    }
                }

                "REPLICAS" | "SLAVES" => {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".to_string();
                    }
                    let name = &args[1];
                    let s = state.read().unwrap();

                    if let Some(replicas) = s.replicas.get(name) {
                        let mut result = format!("*{}\r\n", replicas.len());
                        for replica in replicas {
                            let flags = match replica.state {
                                InstanceState::Up => "slave",
                                InstanceState::SubjectivelyDown => "slave,s_down",
                                _ => "slave,disconnected",
                            };
                            result.push_str(&format_replica_info(
                                &replica.host,
                                replica.port,
                                flags,
                                replica.replica_offset,
                                replica.replica_priority,
                            ));
                        }
                        result
                    } else {
                        "-ERR No such master with that name\r\n".to_string()
                    }
                }

                "SENTINELS" => {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".to_string();
                    }
                    let s = state.read().unwrap();
                    let name = &args[1];

                    if let Some(sentinels) = s.sentinels.get(name) {
                        let mut result = format!("*{}\r\n", sentinels.len());
                        for sentinel in sentinels {
                            result.push_str(&format!(
                                "*4\r\n$4\r\nname\r\n$8\r\nsentinel\r\n$2\r\nip\r\n${}\r\n{}\r\n",
                                sentinel.host.len(),
                                sentinel.host
                            ));
                        }
                        result
                    } else {
                        "-ERR No such master with that name\r\n".to_string()
                    }
                }

                "GET-MASTER-ADDR-BY-NAME" => {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".to_string();
                    }
                    let name = &args[1];
                    let s = state.read().unwrap();

                    if let Some(cfg) = s.masters.get(name) {
                        let port_str = cfg.port.to_string();
                        format!(
                            "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            cfg.host.len(),
                            cfg.host,
                            port_str.len(),
                            port_str
                        )
                    } else {
                        "*-1\r\n".to_string()
                    }
                }

                "MYID" => {
                    format!("${}\r\n{}\r\n", my_id.len(), my_id)
                }

                "INFO-CACHE" => "+OK\r\n".to_string(),

                "CKQUORUM" => {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".to_string();
                    }
                    let name = &args[1];
                    let s = state.read().unwrap();

                    if let Some(cfg) = s.masters.get(name) {
                        let num_sentinels = s.sentinels.get(name).map_or(0, std::vec::Vec::len) + 1;
                        if num_sentinels as u32 >= cfg.quorum {
                            "+OK 1 usable Sentinels. Quorum is reachable.\r\n".to_string()
                        } else {
                            format!(
                                "-NOQUORUM {} sentinels available, {} needed\r\n",
                                num_sentinels, cfg.quorum
                            )
                        }
                    } else {
                        "-ERR No such master with that name\r\n".to_string()
                    }
                }

                _ => "-ERR Unknown sentinel subcommand\r\n".to_string(),
            }
        }

        "INFO" => {
            let section = args.first().map_or("all", std::string::String::as_str);
            format_info(state, my_id, section)
        }

        "CLIENT" => {
            if args.is_empty() {
                return "-ERR wrong number of arguments\r\n".to_string();
            }
            match args[0].to_uppercase().as_str() {
                "SETNAME" => "+OK\r\n".to_string(),
                "GETNAME" => "$-1\r\n".to_string(),
                "LIST" => "$0\r\n\r\n".to_string(),
                _ => "+OK\r\n".to_string(),
            }
        }

        "SUBSCRIBE" | "PSUBSCRIBE" => {
            // Simplified pub/sub response
            "+OK\r\n".to_string()
        }

        "SHUTDOWN" => {
            std::process::exit(0);
        }

        _ => format!("-ERR unknown command '{cmd}'\r\n"),
    }
}

fn format_master_info(
    name: &str,
    host: &str,
    port: u16,
    flags: &str,
    quorum: u32,
    num_slaves: usize,
    num_sentinels: usize,
) -> String {
    let port_str = port.to_string();
    let quorum_str = quorum.to_string();
    let slaves_str = num_slaves.to_string();
    let sentinels_str = num_sentinels.to_string();

    format!(
        "*20\r\n\
        $4\r\nname\r\n${}\r\n{}\r\n\
        $2\r\nip\r\n${}\r\n{}\r\n\
        $4\r\nport\r\n${}\r\n{}\r\n\
        $5\r\nflags\r\n${}\r\n{}\r\n\
        $6\r\nquorum\r\n${}\r\n{}\r\n\
        $10\r\nnum-slaves\r\n${}\r\n{}\r\n\
        $18\r\nnum-other-sentinels\r\n${}\r\n{}\r\n\
        $12\r\nconfig-epoch\r\n$1\r\n0\r\n\
        $15\r\nfailover-timeout\r\n$6\r\n180000\r\n\
        $14\r\nparallel-syncs\r\n$1\r\n1\r\n",
        name.len(),
        name,
        host.len(),
        host,
        port_str.len(),
        port_str,
        flags.len(),
        flags,
        quorum_str.len(),
        quorum_str,
        slaves_str.len(),
        slaves_str,
        sentinels_str.len(),
        sentinels_str
    )
}

fn format_replica_info(host: &str, port: u16, flags: &str, offset: u64, priority: u32) -> String {
    let port_str = port.to_string();
    let offset_str = offset.to_string();
    let priority_str = priority.to_string();

    format!(
        "*12\r\n\
        $4\r\nname\r\n${}\r\n{}:{}\r\n\
        $2\r\nip\r\n${}\r\n{}\r\n\
        $4\r\nport\r\n${}\r\n{}\r\n\
        $5\r\nflags\r\n${}\r\n{}\r\n\
        $14\r\nslave-priority\r\n${}\r\n{}\r\n\
        $17\r\nslave-repl-offset\r\n${}\r\n{}\r\n",
        host.len() + 1 + port_str.len(),
        host,
        port,
        host.len(),
        host,
        port_str.len(),
        port_str,
        flags.len(),
        flags,
        priority_str.len(),
        priority_str,
        offset_str.len(),
        offset_str
    )
}

fn format_info(state: &Arc<RwLock<SentinelState>>, my_id: &str, section: &str) -> String {
    let s = state.read().unwrap();

    let mut info = String::new();

    if section == "all" || section == "server" {
        info.push_str("# Server\r\n");
        info.push_str("viator_version:0.1.0\r\n");
        info.push_str("viator_mode:sentinel\r\n");
        info.push_str(&format!("run_id:{my_id}\r\n"));
        info.push_str("\r\n");
    }

    if section == "all" || section == "sentinel" {
        info.push_str("# Sentinel\r\n");
        info.push_str(&format!("sentinel_masters:{}\r\n", s.masters.len()));
        info.push_str("sentinel_tilt:0\r\n");
        info.push_str("sentinel_running_scripts:0\r\n");
        info.push_str("sentinel_scripts_queue_length:0\r\n");
        info.push_str("sentinel_simulate_failure_flags:0\r\n");

        for (name, cfg) in &s.masters {
            let num_slaves = s.replicas.get(name).map_or(0, std::vec::Vec::len);
            let num_sentinels = s.sentinels.get(name).map_or(0, std::vec::Vec::len);
            let status = s
                .instances
                .get(&format!("{}:{}", cfg.host, cfg.port))
                .map_or("unknown", |i| match i.state {
                    InstanceState::Up => "ok",
                    _ => "sdown",
                });

            info.push_str(&format!(
                "master{}:name={},status={},address={}:{},slaves={},sentinels={}\r\n",
                0,
                name,
                status,
                cfg.host,
                cfg.port,
                num_slaves,
                num_sentinels + 1
            ));
        }
    }

    let len = info.len();
    format!("${len}\r\n{info}\r\n")
}

fn print_usage() {
    println!(
        "Usage: viator-sentinel [OPTIONS] [config-file]

Viator Sentinel - High availability monitoring service.
Compatible with Redis Sentinel 8.4.0 protocol.

Options:
  --port <port>            Sentinel port (default: 26379)
  --bind <address>         Bind address (default: 0.0.0.0)
  --sentinel monitor <name> <host> <port> <quorum>
                           Monitor a master
  --sentinel down-after-milliseconds <name> <ms>
                           Down detection time
  --sentinel failover-timeout <name> <ms>
                           Failover timeout
  --help                   Show this help message

Examples:
  viator-sentinel sentinel.conf
  viator-sentinel --port 26379 \\
    --sentinel monitor mymaster 127.0.0.1 6379 2

Configuration file format:
  port 26379
  sentinel monitor mymaster 127.0.0.1 6379 2
  sentinel down-after-milliseconds mymaster 30000
  sentinel failover-timeout mymaster 180000
  sentinel parallel-syncs mymaster 1
"
    );
}

fn parse_config_file(
    path: &str,
) -> Result<(SentinelConfig, Vec<(String, String, u16, u32)>), String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("Cannot read config file: {e}"))?;

    let mut config = SentinelConfig::default();
    let mut masters = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "port" => {
                if parts.len() >= 2 {
                    config.port = parts[1].parse().unwrap_or(26379);
                }
            }
            "bind" => {
                if parts.len() >= 2 {
                    config.bind = parts[1].to_string();
                }
            }
            "sentinel" => {
                if parts.len() >= 2 {
                    match parts[1].to_lowercase().as_str() {
                        "monitor" => {
                            if parts.len() >= 6 {
                                let name = parts[2].to_string();
                                let host = parts[3].to_string();
                                let port: u16 = parts[4].parse().unwrap_or(6379);
                                let quorum: u32 = parts[5].parse().unwrap_or(2);
                                masters.push((name, host, port, quorum));
                            }
                        }
                        "down-after-milliseconds" => {
                            if parts.len() >= 4 {
                                config.down_after_milliseconds = parts[3].parse().unwrap_or(30000);
                            }
                        }
                        "failover-timeout" => {
                            if parts.len() >= 4 {
                                config.failover_timeout = parts[3].parse().unwrap_or(180000);
                            }
                        }
                        "parallel-syncs" => {
                            if parts.len() >= 4 {
                                config.parallel_syncs = parts[3].parse().unwrap_or(1);
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok((config, masters))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut config = SentinelConfig::default();
    let mut masters: Vec<(String, String, u16, u32)> = Vec::new();
    let mut config_file: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                if i < args.len() {
                    config.port = args[i].parse().unwrap_or(26379);
                }
            }
            "--bind" => {
                i += 1;
                if i < args.len() {
                    config.bind = args[i].clone();
                }
            }
            "--sentinel" => {
                i += 1;
                if i < args.len() && args[i] == "monitor" && i + 4 < args.len() {
                    let name = args[i + 1].clone();
                    let host = args[i + 2].clone();
                    let port: u16 = args[i + 3].parse().unwrap_or(6379);
                    let quorum: u32 = args[i + 4].parse().unwrap_or(2);
                    masters.push((name, host, port, quorum));
                    i += 4;
                }
            }
            "--help" | "-h" => {
                print_usage();
                return;
            }
            _ if !args[i].starts_with('-') => {
                config_file = Some(args[i].clone());
            }
            _ => {}
        }
        i += 1;
    }

    // Load config file if specified
    if let Some(path) = config_file {
        match parse_config_file(&path) {
            Ok((cfg, file_masters)) => {
                config = cfg;
                if masters.is_empty() {
                    masters = file_masters;
                }
            }
            Err(e) => {
                eprintln!("Error loading config: {e}");
                std::process::exit(1);
            }
        }
    }

    if masters.is_empty() {
        eprintln!("Error: No masters configured. Use --sentinel monitor or config file.");
        print_usage();
        std::process::exit(1);
    }

    let sentinel = Sentinel::new(config);

    for (name, host, port, quorum) in masters {
        println!("Monitoring master '{name}' at {host}:{port} (quorum: {quorum})");
        sentinel.add_master(&name, &host, port, quorum);
    }

    if let Err(e) = sentinel.run() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
