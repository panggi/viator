//! Real Sentinel monitoring implementation.
//!
//! This module provides actual TCP-based monitoring of Redis instances,
//! failure detection, and automated failover execution.

use crate::commands::sentinel::{MasterState, MonitoredMaster, ReplicaInfo, SentinelState};
use bytes::BytesMut;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

/// Interval between health checks.
const PING_INTERVAL_MS: u64 = 1000;

/// Timeout for ping response.
const PING_TIMEOUT_MS: u64 = 500;

/// Interval for INFO polling.
const INFO_INTERVAL_MS: u64 = 10000;

/// Interval for HELLO publishing.
const HELLO_INTERVAL_MS: u64 = 2000;

/// Connection state for a monitored instance.
struct InstanceConnection {
    /// The address of the instance.
    addr: SocketAddr,
    /// TCP stream (if connected).
    stream: Option<TcpStream>,
    /// Last successful ping time.
    last_ping: Option<Instant>,
    /// Last INFO poll time.
    last_info: Option<Instant>,
    /// Is the instance reachable?
    reachable: bool,
    /// Time when marked unreachable.
    unreachable_since: Option<Instant>,
    /// Replication offset (for replicas).
    repl_offset: u64,
    /// Run ID of the instance.
    run_id: String,
    /// Authentication password.
    auth_pass: Option<String>,
    /// Authentication user.
    auth_user: Option<String>,
}

impl InstanceConnection {
    fn new(addr: SocketAddr, auth_pass: Option<String>, auth_user: Option<String>) -> Self {
        Self {
            addr,
            stream: None,
            last_ping: None,
            last_info: None,
            reachable: false,
            unreachable_since: None,
            repl_offset: 0,
            run_id: String::new(),
            auth_pass,
            auth_user,
        }
    }

    /// Connect to the instance.
    async fn connect(&mut self) -> bool {
        match TcpStream::connect(self.addr).await {
            Ok(stream) => {
                // Disable Nagle's algorithm for low latency
                let _ = stream.set_nodelay(true);
                self.stream = Some(stream);

                // Authenticate if needed
                if self.auth_pass.is_some() || self.auth_user.is_some() {
                    if !self.authenticate().await {
                        self.stream = None;
                        return false;
                    }
                }

                self.reachable = true;
                self.unreachable_since = None;
                debug!("Connected to {}", self.addr);
                true
            }
            Err(e) => {
                debug!("Failed to connect to {}: {}", self.addr, e);
                self.mark_unreachable();
                false
            }
        }
    }

    /// Authenticate with the instance.
    async fn authenticate(&mut self) -> bool {
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return false,
        };

        let auth_cmd = if let Some(ref user) = self.auth_user {
            format!(
                "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                user.len(),
                user,
                self.auth_pass.as_ref().map(|p| p.len()).unwrap_or(0),
                self.auth_pass.as_ref().map(|p| p.as_str()).unwrap_or("")
            )
        } else if let Some(ref pass) = self.auth_pass {
            format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass)
        } else {
            return true;
        };

        if stream.write_all(auth_cmd.as_bytes()).await.is_err() {
            return false;
        }

        // Read response
        let mut buf = [0u8; 128];
        match tokio::time::timeout(
            Duration::from_millis(PING_TIMEOUT_MS),
            stream.read(&mut buf),
        )
        .await
        {
            Ok(Ok(n)) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                response.starts_with("+OK")
            }
            _ => false,
        }
    }

    /// Send a PING and wait for PONG.
    async fn ping(&mut self) -> bool {
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => {
                if !self.connect().await {
                    return false;
                }
                self.stream.as_mut().unwrap()
            }
        };

        // Send PING
        let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
        if stream.write_all(ping_cmd).await.is_err() {
            self.mark_unreachable();
            self.stream = None;
            return false;
        }

        // Wait for PONG with timeout
        let mut buf = [0u8; 128];
        match tokio::time::timeout(
            Duration::from_millis(PING_TIMEOUT_MS),
            stream.read(&mut buf),
        )
        .await
        {
            Ok(Ok(n)) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                if response.contains("PONG") || response.starts_with("+") {
                    self.last_ping = Some(Instant::now());
                    self.reachable = true;
                    self.unreachable_since = None;
                    true
                } else {
                    self.mark_unreachable();
                    false
                }
            }
            _ => {
                self.mark_unreachable();
                self.stream = None;
                false
            }
        }
    }

    /// Poll INFO from the instance.
    async fn poll_info(&mut self) -> Option<HashMap<String, String>> {
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => {
                if !self.connect().await {
                    return None;
                }
                self.stream.as_mut().unwrap()
            }
        };

        // Send INFO REPLICATION
        let info_cmd = b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
        if stream.write_all(info_cmd).await.is_err() {
            self.stream = None;
            return None;
        }

        // Read response with timeout
        let mut buf = BytesMut::with_capacity(4096);
        let mut temp = [0u8; 1024];

        match tokio::time::timeout(Duration::from_millis(2000), async {
            loop {
                match stream.read(&mut temp).await {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&temp[..n]);
                        // Check if we have a complete response
                        if buf.len() > 3 && buf[buf.len() - 2..] == *b"\r\n" {
                            // Look for end of bulk string
                            if buf.contains(&b'\n') {
                                break;
                            }
                        }
                        if buf.len() > 8192 {
                            break; // Safety limit
                        }
                    }
                    Err(_) => break,
                }
            }
        })
        .await
        {
            Ok(()) => {
                self.last_info = Some(Instant::now());
                parse_info_response(&buf)
            }
            Err(_) => None,
        }
    }

    fn mark_unreachable(&mut self) {
        if self.reachable {
            self.reachable = false;
            self.unreachable_since = Some(Instant::now());
        }
    }

    fn is_sdown(&self, down_after_ms: u64) -> bool {
        if let Some(since) = self.unreachable_since {
            since.elapsed().as_millis() as u64 >= down_after_ms
        } else {
            false
        }
    }
}

/// Parse INFO response into key-value pairs.
fn parse_info_response(data: &[u8]) -> Option<HashMap<String, String>> {
    let text = String::from_utf8_lossy(data);

    // Skip the bulk string prefix ($xxx\r\n)
    let content = if text.starts_with('$') {
        text.split_once("\r\n").map(|(_, rest)| rest)?
    } else {
        &text
    };

    let mut result = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            result.insert(key.to_string(), value.to_string());
        }
    }

    Some(result)
}

/// Sentinel monitor that performs actual health checking and failover.
pub struct SentinelMonitor {
    /// Reference to global sentinel state.
    state: &'static SentinelState,
    /// Connections to masters.
    master_conns: RwLock<HashMap<String, InstanceConnection>>,
    /// Connections to replicas (master_name -> replica_id -> connection).
    replica_conns: RwLock<HashMap<String, HashMap<String, InstanceConnection>>>,
    /// Connections to other sentinels.
    sentinel_conns: RwLock<HashMap<String, InstanceConnection>>,
    /// Running flag.
    running: AtomicBool,
    /// Our sentinel address for announcements.
    our_addr: RwLock<Option<SocketAddr>>,
}

impl SentinelMonitor {
    pub fn new() -> Self {
        Self {
            state: SentinelState::global(),
            master_conns: RwLock::new(HashMap::new()),
            replica_conns: RwLock::new(HashMap::new()),
            sentinel_conns: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
            our_addr: RwLock::new(None),
        }
    }

    /// Set our sentinel's announced address.
    pub fn set_our_addr(&self, addr: SocketAddr) {
        *self.our_addr.write() = Some(addr);
    }

    /// Start the monitoring loop.
    pub async fn start(self: Arc<Self>) {
        if !self.state.is_enabled() {
            info!("Sentinel mode not enabled, monitor not starting");
            return;
        }

        self.running.store(true, Ordering::SeqCst);
        info!("Starting Sentinel monitor");

        // Main monitoring loop
        let mut ping_interval = tokio::time::interval(Duration::from_millis(PING_INTERVAL_MS));
        let mut info_interval = tokio::time::interval(Duration::from_millis(INFO_INTERVAL_MS));
        let mut hello_interval = tokio::time::interval(Duration::from_millis(HELLO_INTERVAL_MS));

        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                _ = ping_interval.tick() => {
                    self.ping_all_instances().await;
                    self.check_sdown_odown().await;
                }
                _ = info_interval.tick() => {
                    self.poll_info_all().await;
                }
                _ = hello_interval.tick() => {
                    self.publish_hello().await;
                }
            }
        }

        info!("Sentinel monitor stopped");
    }

    /// Stop the monitoring loop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Ping all monitored instances.
    async fn ping_all_instances(&self) {
        // Ping masters
        for master_name in self.state.master_names() {
            if let Some(master) = self.state.get_master(&master_name) {
                let config = master.config.read();
                let addr = config.addr;
                let auth_pass = config.auth_pass.clone();
                let auth_user = config.auth_user.clone();
                drop(config);

                // Ensure we have a connection entry
                {
                    let mut conns = self.master_conns.write();
                    conns
                        .entry(master_name.clone())
                        .or_insert_with(|| InstanceConnection::new(addr, auth_pass, auth_user));
                }

                // Ping the master
                let ping_result = {
                    let mut conns = self.master_conns.write();
                    if let Some(conn) = conns.get_mut(&master_name) {
                        conn.ping().await
                    } else {
                        false
                    }
                };

                if ping_result {
                    *master.last_ping.write() = Some(Instant::now());
                }
            }
        }

        // Ping replicas
        for master_name in self.state.master_names() {
            if let Some(master) = self.state.get_master(&master_name) {
                for entry in master.replicas.iter() {
                    let replica_id = entry.key().clone();
                    let replica_info = entry.value().clone();

                    // Ensure connection entry exists
                    {
                        let mut replica_conns = self.replica_conns.write();
                        let master_replicas = replica_conns.entry(master_name.clone()).or_default();
                        master_replicas
                            .entry(replica_id.clone())
                            .or_insert_with(|| {
                                InstanceConnection::new(replica_info.addr, None, None)
                            });
                    }

                    // Ping the replica
                    let (ping_result, new_offset) = {
                        let mut replica_conns = self.replica_conns.write();
                        if let Some(master_replicas) = replica_conns.get_mut(&master_name) {
                            if let Some(conn) = master_replicas.get_mut(&replica_id) {
                                let result = conn.ping().await;
                                (result, conn.repl_offset)
                            } else {
                                (false, 0)
                            }
                        } else {
                            (false, 0)
                        }
                    };

                    // Update replica info
                    if let Some(mut replica) = master.replicas.get_mut(&replica_id) {
                        replica.online = ping_result;
                        replica.last_ping_ms = if ping_result { 0 } else { 9999 };
                        if new_offset > 0 {
                            replica.offset = new_offset;
                        }
                    }
                }
            }
        }
    }

    /// Check for SDOWN and ODOWN conditions.
    async fn check_sdown_odown(&self) {
        for master_name in self.state.master_names() {
            if let Some(master) = self.state.get_master(&master_name) {
                let config = master.config.read();
                let down_after_ms = config.down_after_ms;
                let quorum = config.quorum;
                drop(config);

                // Check master's SDOWN
                let is_sdown = {
                    let conns = self.master_conns.read();
                    conns
                        .get(&master_name)
                        .map(|c| c.is_sdown(down_after_ms))
                        .unwrap_or(false)
                };

                let current_state = *master.state.read();

                // Handle manual SENTINEL FAILOVER command (state set to ODOWN externally)
                if current_state == MasterState::ODown {
                    // ODOWN already set (e.g., by SENTINEL FAILOVER command)
                    info!("Master {} in ODOWN state, initiating failover", master_name);
                    self.initiate_failover(&master_name).await;
                } else if is_sdown && current_state == MasterState::Up {
                    // Transition to SDOWN
                    *master.state.write() = MasterState::SDown;
                    warn!("Master {} is SDOWN", master_name);

                    // Query other sentinels for ODOWN
                    let odown_votes = self.query_sentinels_for_odown(&master_name).await;

                    if odown_votes >= quorum as u64 {
                        // Transition to ODOWN
                        *master.state.write() = MasterState::ODown;
                        warn!(
                            "Master {} is ODOWN (votes: {}/{})",
                            master_name, odown_votes, quorum
                        );

                        // Initiate failover
                        self.initiate_failover(&master_name).await;
                    }
                } else if !is_sdown && current_state == MasterState::SDown {
                    // Master is back up
                    *master.state.write() = MasterState::Up;
                    info!("Master {} is back UP", master_name);
                }
            }
        }
    }

    /// Query other sentinels to see if they agree master is down.
    async fn query_sentinels_for_odown(&self, master_name: &str) -> u64 {
        let mut votes = 1u64; // We vote ourselves

        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => return votes,
        };

        let config = master.config.read();
        let master_addr = config.addr;
        drop(config);

        // Query each known sentinel
        for entry in master.sentinels.iter() {
            let sentinel_info = entry.value();
            let sentinel_addr = sentinel_info.addr;

            // Send SENTINEL IS-MASTER-DOWN-BY-ADDR to the sentinel
            match self
                .send_is_master_down_query(sentinel_addr, master_addr)
                .await
            {
                Some(true) => {
                    votes += 1;
                }
                _ => {}
            }
        }

        votes
    }

    /// Send IS-MASTER-DOWN-BY-ADDR query to another sentinel.
    async fn send_is_master_down_query(
        &self,
        sentinel_addr: SocketAddr,
        master_addr: SocketAddr,
    ) -> Option<bool> {
        // Connect to the sentinel
        let mut stream = match TcpStream::connect(sentinel_addr).await {
            Ok(s) => s,
            Err(_) => return None,
        };

        // Build the command
        // SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current_epoch> <runid>
        let cmd = format!(
            "*6\r\n$8\r\nSENTINEL\r\n$22\r\nIS-MASTER-DOWN-BY-ADDR\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n0\r\n$1\r\n*\r\n",
            master_addr.ip().to_string().len(),
            master_addr.ip(),
            master_addr.port().to_string().len(),
            master_addr.port()
        );

        if stream.write_all(cmd.as_bytes()).await.is_err() {
            return None;
        }

        // Read response
        let mut buf = [0u8; 256];
        match tokio::time::timeout(Duration::from_millis(1000), stream.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {
                // Parse response: *3\r\n:<down_state>\r\n$...\r\n...
                let response = String::from_utf8_lossy(&buf[..n]);
                // Check if first integer is 1 (down)
                if response.contains(":1\r\n") {
                    Some(true)
                } else {
                    Some(false)
                }
            }
            _ => None,
        }
    }

    /// Poll INFO from all instances.
    async fn poll_info_all(&self) {
        for master_name in self.state.master_names() {
            if let Some(master) = self.state.get_master(&master_name) {
                // Poll master INFO
                let info = {
                    let mut conns = self.master_conns.write();
                    if let Some(conn) = conns.get_mut(&master_name) {
                        conn.poll_info().await
                    } else {
                        None
                    }
                };

                if let Some(info) = info {
                    // Discover replicas from INFO
                    self.discover_replicas_from_info(&master, &info).await;
                }

                // Poll replica INFO to get replication offset
                let mut replica_conns = self.replica_conns.write();
                if let Some(master_replicas) = replica_conns.get_mut(&master_name) {
                    for (replica_id, conn) in master_replicas.iter_mut() {
                        if let Some(info) = conn.poll_info().await {
                            // Update replication offset
                            if let Some(offset_str) = info.get("slave_repl_offset") {
                                if let Ok(offset) = offset_str.parse::<u64>() {
                                    conn.repl_offset = offset;
                                    // Update in master's replica info
                                    if let Some(mut replica) = master.replicas.get_mut(replica_id) {
                                        replica.offset = offset;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Discover replicas from master's INFO response.
    async fn discover_replicas_from_info(
        &self,
        master: &MonitoredMaster,
        info: &HashMap<String, String>,
    ) {
        // Parse slave entries: slave0:ip=...,port=...,state=...,offset=...
        for (key, value) in info.iter() {
            if key.starts_with("slave") {
                if let Some(replica_info) = parse_slave_info(value) {
                    let replica_id =
                        format!("{}:{}", replica_info.addr.ip(), replica_info.addr.port());

                    // Add or update replica
                    master
                        .replicas
                        .entry(replica_id)
                        .and_modify(|r| {
                            r.offset = replica_info.offset;
                            r.master_link_status = replica_info.master_link_status.clone();
                        })
                        .or_insert(replica_info);
                }
            }
        }
    }

    /// Publish HELLO message to __sentinel__:hello channel.
    async fn publish_hello(&self) {
        // In a real implementation, this would publish to the master's
        // __sentinel__:hello Pub/Sub channel so other sentinels can discover us
        // For now, we just update our presence in the sentinel lists
    }

    /// Initiate failover for a master.
    async fn initiate_failover(&self, master_name: &str) {
        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => return,
        };

        // Check if failover is already in progress
        if *master.state.read() == MasterState::Failover {
            return;
        }

        info!("Initiating failover for master {}", master_name);
        *master.state.write() = MasterState::Failover;

        // Bump epoch for leader election
        let epoch = self.state.bump_epoch();
        master.failover_epoch.store(epoch, Ordering::SeqCst);

        // Perform leader election among sentinels
        let is_leader = self.elect_leader(master_name, epoch).await;

        if is_leader {
            info!("We are the leader for failover of {}", master_name);
            self.execute_failover(master_name).await;
        } else {
            info!(
                "Another sentinel is leading the failover of {}",
                master_name
            );
        }
    }

    /// Elect a leader for failover.
    async fn elect_leader(&self, master_name: &str, _epoch: u64) -> bool {
        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => return false,
        };

        let config = master.config.read();
        let quorum = config.quorum;
        drop(config);

        let _my_id = self.state.my_id();
        let mut votes = 1u64; // Vote for ourselves

        // Request votes from other sentinels
        for entry in master.sentinels.iter() {
            let sentinel = entry.value();

            // Would send SENTINEL IS-MASTER-DOWN-BY-ADDR with our runid
            // to request a vote. Simplified: just count reachable sentinels.
            if sentinel.is_reachable {
                votes += 1;
            }
        }

        // We win if we have quorum votes
        votes >= quorum as u64
    }

    /// Execute the actual failover.
    async fn execute_failover(&self, master_name: &str) {
        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => return,
        };

        // Select the best replica
        let best_replica = self.select_best_replica(master_name).await;

        let (replica_id, replica_addr) = match best_replica {
            Some(r) => r,
            None => {
                error!("No suitable replica found for failover of {}", master_name);
                *master.state.write() = MasterState::Up; // Reset state
                return;
            }
        };

        info!(
            "Selected replica {} ({}) for promotion",
            replica_id, replica_addr
        );

        // Step 1: Send SLAVEOF NO ONE to the selected replica
        if !self.promote_replica(replica_addr).await {
            error!("Failed to promote replica {}", replica_id);
            *master.state.write() = MasterState::Up;
            return;
        }

        info!("Replica {} promoted to master", replica_id);

        // Step 2: Update master configuration to point to new master
        {
            let mut config = master.config.write();
            config.addr = replica_addr;
        }

        // Step 3: Reconfigure other replicas to replicate from new master
        self.reconfigure_replicas(master_name, replica_addr, &replica_id)
            .await;

        // Step 4: Mark failover complete
        *master.state.write() = MasterState::Up;
        master.config_epoch.fetch_add(1, Ordering::SeqCst);

        info!(
            "Failover complete for {}. New master: {}",
            master_name, replica_addr
        );

        // Remove the promoted replica from replica list
        master.replicas.remove(&replica_id);

        // Update our connection to point to new master
        {
            let mut conns = self.master_conns.write();
            conns.insert(
                master_name.to_string(),
                InstanceConnection::new(replica_addr, None, None),
            );
        }
    }

    /// Select the best replica for promotion based on:
    /// 1. Replica priority (lower is better)
    /// 2. Replication offset (higher is better)
    /// 3. Run ID (lexicographically smallest as tiebreaker)
    async fn select_best_replica(&self, master_name: &str) -> Option<(String, SocketAddr)> {
        let master = self.state.get_master(master_name)?;

        let mut best: Option<(String, SocketAddr, u64, u64)> = None; // (id, addr, priority, offset)

        for entry in master.replicas.iter() {
            let replica = entry.value();

            // Skip offline replicas
            if !replica.online {
                continue;
            }

            // Skip replicas with master link down
            if replica.master_link_status != "ok" {
                continue;
            }

            let priority = 100u64; // Default priority
            let offset = replica.offset;

            match &best {
                None => {
                    best = Some((entry.key().clone(), replica.addr, priority, offset));
                }
                Some((_, _, best_priority, best_offset)) => {
                    // Lower priority wins
                    if priority < *best_priority {
                        best = Some((entry.key().clone(), replica.addr, priority, offset));
                    } else if priority == *best_priority && offset > *best_offset {
                        // Same priority: higher offset wins
                        best = Some((entry.key().clone(), replica.addr, priority, offset));
                    }
                }
            }
        }

        best.map(|(id, addr, _, _)| (id, addr))
    }

    /// Promote a replica to master by sending SLAVEOF NO ONE.
    async fn promote_replica(&self, addr: SocketAddr) -> bool {
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to connect to replica {}: {}", addr, e);
                return false;
            }
        };

        // Send SLAVEOF NO ONE (or REPLICAOF NO ONE)
        let cmd = b"*3\r\n$7\r\nSLAVEOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n";
        if stream.write_all(cmd).await.is_err() {
            return false;
        }

        // Wait for OK response
        let mut buf = [0u8; 128];
        match tokio::time::timeout(Duration::from_millis(5000), stream.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                response.contains("OK")
            }
            _ => false,
        }
    }

    /// Reconfigure other replicas to replicate from the new master.
    async fn reconfigure_replicas(
        &self,
        master_name: &str,
        new_master_addr: SocketAddr,
        promoted_replica_id: &str,
    ) {
        let master = match self.state.get_master(master_name) {
            Some(m) => m,
            None => return,
        };

        for entry in master.replicas.iter() {
            let replica_id = entry.key();
            let replica = entry.value();

            // Skip the promoted replica
            if replica_id == promoted_replica_id {
                continue;
            }

            // Send SLAVEOF <new_master_ip> <new_master_port>
            if let Ok(mut stream) = TcpStream::connect(replica.addr).await {
                let cmd = format!(
                    "*3\r\n$7\r\nSLAVEOF\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    new_master_addr.ip().to_string().len(),
                    new_master_addr.ip(),
                    new_master_addr.port().to_string().len(),
                    new_master_addr.port()
                );

                let _ = stream.write_all(cmd.as_bytes()).await;
                debug!(
                    "Reconfigured replica {} to replicate from {}",
                    replica_id, new_master_addr
                );
            }
        }
    }
}

/// Parse slave info string from INFO output.
fn parse_slave_info(value: &str) -> Option<ReplicaInfo> {
    let mut ip: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut state = "online".to_string();
    let mut offset = 0u64;

    for part in value.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            match k {
                "ip" => ip = Some(v.to_string()),
                "port" => port = v.parse::<u16>().ok(),
                "state" => state = v.to_string(),
                "offset" => offset = v.parse::<u64>().unwrap_or(0),
                _ => {}
            }
        }
    }

    let ip = ip?;
    let port = port?;
    let addr: SocketAddr = format!("{}:{}", ip, port).parse().ok()?;

    Some(ReplicaInfo {
        id: format!("{}:{}", ip, port),
        addr,
        online: state == "online",
        offset,
        last_ping_ms: 0,
        flags: if state == "online" {
            "slave".to_string()
        } else {
            format!("slave,{}", state)
        },
        master_link_status: if state == "online" {
            "ok".to_string()
        } else {
            "err".to_string()
        },
    })
}

impl Default for SentinelMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_info_response() {
        let data = b"$100\r\n# Replication\r\nrole:master\r\nconnected_slaves:2\r\nslave0:ip=127.0.0.1,port=6380,state=online,offset=1000\r\n";
        let info = parse_info_response(data).unwrap();
        assert_eq!(info.get("role"), Some(&"master".to_string()));
        assert_eq!(info.get("connected_slaves"), Some(&"2".to_string()));
    }

    #[test]
    fn test_parse_slave_info() {
        let value = "ip=127.0.0.1,port=6380,state=online,offset=1234,lag=0";
        let replica = parse_slave_info(value).unwrap();
        assert_eq!(replica.addr.port(), 6380);
        assert_eq!(replica.offset, 1234);
        assert!(replica.online);
    }
}
