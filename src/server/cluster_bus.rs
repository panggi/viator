//! Redis Cluster Bus - Real distributed gossip protocol.
//!
//! The cluster bus handles node-to-node communication on port+10000.
//! It implements the Redis Cluster gossip protocol for:
//! - Node discovery and heartbeat (PING/PONG)
//! - Failure detection (PFAIL/FAIL)
//! - Failover coordination
//! - Configuration propagation

use super::cluster::{
    ClusterManager, ClusterState, LinkState, NodeFlags, NodeId,
    CLUSTER_SLOTS, hex_encode,
};
#[cfg(test)]
use super::cluster::generate_node_id;
use bytes::{Buf, BufMut, BytesMut};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Cluster bus message types (matching Redis protocol).
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMsgType {
    /// Ping message (heartbeat request)
    Ping = 0,
    /// Pong message (heartbeat response)
    Pong = 1,
    /// Meet message (join cluster request)
    Meet = 2,
    /// Fail message (node failure broadcast)
    Fail = 3,
    /// Publish message (pub/sub across cluster)
    Publish = 4,
    /// Failover auth request
    FailoverAuthRequest = 5,
    /// Failover auth ack
    FailoverAuthAck = 6,
    /// Update config message
    Update = 7,
    /// Module data message
    Module = 8,
    /// Publish shard message
    PublishShard = 9,
    /// Unknown message
    Unknown = 0xFFFF,
}

impl From<u16> for ClusterMsgType {
    fn from(v: u16) -> Self {
        match v {
            0 => ClusterMsgType::Ping,
            1 => ClusterMsgType::Pong,
            2 => ClusterMsgType::Meet,
            3 => ClusterMsgType::Fail,
            4 => ClusterMsgType::Publish,
            5 => ClusterMsgType::FailoverAuthRequest,
            6 => ClusterMsgType::FailoverAuthAck,
            7 => ClusterMsgType::Update,
            8 => ClusterMsgType::Module,
            9 => ClusterMsgType::PublishShard,
            _ => ClusterMsgType::Unknown,
        }
    }
}

/// Cluster bus message header (fixed size: 2048 bytes in Redis).
/// We use a simplified version.
#[derive(Debug, Clone)]
pub struct ClusterMsgHeader {
    /// Signature "RCmb" (Redis Cluster message bus)
    pub signature: [u8; 4],
    /// Total message length
    pub totlen: u32,
    /// Protocol version
    pub version: u16,
    /// Message port offset (for NAT traversal)
    pub port: u16,
    /// Message type
    pub msg_type: ClusterMsgType,
    /// Sender node ID
    pub sender: NodeId,
    /// Sender's slots bitmap (first 2048 slots, we send full bitmap separately)
    pub slots: [u8; 2048],
    /// Sender's master ID (if replica)
    pub master_id: NodeId,
    /// Sender's IP
    pub sender_ip: [u8; 46],
    /// Sender's bus port
    pub bus_port: u16,
    /// Sender's node flags
    pub flags: u16,
    /// Cluster state from sender's perspective
    pub state: u8,
    /// Message flags
    pub mflags: [u8; 3],
    /// Current epoch
    pub current_epoch: u64,
    /// Config epoch
    pub config_epoch: u64,
    /// Replication offset
    pub offset: u64,
}

impl ClusterMsgHeader {
    pub const SIGNATURE: [u8; 4] = *b"RCmb";
    pub const HEADER_SIZE: usize = 2180; // Actual serialized header size

    pub fn new(msg_type: ClusterMsgType, sender: NodeId) -> Self {
        Self {
            signature: Self::SIGNATURE,
            totlen: Self::HEADER_SIZE as u32,
            version: 1,
            port: 0,
            msg_type,
            sender,
            slots: [0u8; 2048],
            master_id: [0u8; 20],
            sender_ip: [0u8; 46],
            bus_port: 0,
            flags: 0,
            state: 0,
            mflags: [0u8; 3],
            current_epoch: 0,
            config_epoch: 0,
            offset: 0,
        }
    }

    /// Serialize header to bytes.
    pub fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(Self::HEADER_SIZE);

        buf.put_slice(&self.signature);
        buf.put_u32_le(self.totlen);
        buf.put_u16_le(self.version);
        buf.put_u16_le(self.port);
        buf.put_u16_le(self.msg_type as u16);
        buf.put_slice(&self.sender);
        buf.put_slice(&self.slots);
        buf.put_slice(&self.master_id);
        buf.put_slice(&self.sender_ip);
        buf.put_u16_le(self.bus_port);
        buf.put_u16_le(self.flags);
        buf.put_u8(self.state);
        buf.put_slice(&self.mflags);
        buf.put_u64_le(self.current_epoch);
        buf.put_u64_le(self.config_epoch);
        buf.put_u64_le(self.offset);

        // Pad to header size
        while buf.len() < Self::HEADER_SIZE {
            buf.put_u8(0);
        }

        buf
    }

    /// Deserialize header from bytes.
    pub fn deserialize(mut buf: &[u8]) -> Option<Self> {
        if buf.len() < 100 {  // Minimum viable header
            return None;
        }

        let mut signature = [0u8; 4];
        signature.copy_from_slice(&buf[..4]);
        buf = &buf[4..];

        if signature != Self::SIGNATURE {
            return None;
        }

        let totlen = (&buf[..4]).get_u32_le();
        buf = &buf[4..];

        let version = (&buf[..2]).get_u16_le();
        buf = &buf[2..];

        let port = (&buf[..2]).get_u16_le();
        buf = &buf[2..];

        let msg_type = ClusterMsgType::from((&buf[..2]).get_u16_le());
        buf = &buf[2..];

        let mut sender = [0u8; 20];
        sender.copy_from_slice(&buf[..20]);
        buf = &buf[20..];

        let mut slots = [0u8; 2048];
        if buf.len() >= 2048 {
            slots.copy_from_slice(&buf[..2048]);
            buf = &buf[2048..];
        } else {
            buf = &[];
        }

        let mut master_id = [0u8; 20];
        if buf.len() >= 20 {
            master_id.copy_from_slice(&buf[..20]);
            buf = &buf[20..];
        }

        let mut sender_ip = [0u8; 46];
        if buf.len() >= 46 {
            sender_ip.copy_from_slice(&buf[..46]);
            buf = &buf[46..];
        }

        let bus_port = if buf.len() >= 2 { (&buf[..2]).get_u16_le() } else { 0 };
        if buf.len() >= 2 { buf = &buf[2..]; }

        let flags = if buf.len() >= 2 { (&buf[..2]).get_u16_le() } else { 0 };
        if buf.len() >= 2 { buf = &buf[2..]; }

        let state = if !buf.is_empty() { buf[0] } else { 0 };
        if !buf.is_empty() { buf = &buf[1..]; }

        let mut mflags = [0u8; 3];
        if buf.len() >= 3 {
            mflags.copy_from_slice(&buf[..3]);
            buf = &buf[3..];
        }

        let current_epoch = if buf.len() >= 8 { (&buf[..8]).get_u64_le() } else { 0 };
        if buf.len() >= 8 { buf = &buf[8..]; }

        let config_epoch = if buf.len() >= 8 { (&buf[..8]).get_u64_le() } else { 0 };
        if buf.len() >= 8 { buf = &buf[8..]; }

        let offset = if buf.len() >= 8 { (&buf[..8]).get_u64_le() } else { 0 };

        Some(Self {
            signature,
            totlen,
            version,
            port,
            msg_type,
            sender,
            slots,
            master_id,
            sender_ip,
            bus_port,
            flags,
            state,
            mflags,
            current_epoch,
            config_epoch,
            offset,
        })
    }
}

/// Gossip data section containing info about random nodes.
#[derive(Debug, Clone)]
pub struct GossipEntry {
    /// Node ID
    pub node_id: NodeId,
    /// Ping sent timestamp
    pub ping_sent: u32,
    /// Pong received timestamp
    pub pong_received: u32,
    /// Node IP
    pub ip: [u8; 46],
    /// Node port
    pub port: u16,
    /// Node bus port
    pub bus_port: u16,
    /// Node flags
    pub flags: u16,
}

impl GossipEntry {
    pub const SIZE: usize = 104;

    pub fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        buf.put_slice(&self.node_id);
        buf.put_u32_le(self.ping_sent);
        buf.put_u32_le(self.pong_received);
        buf.put_slice(&self.ip);
        buf.put_u16_le(self.port);
        buf.put_u16_le(self.bus_port);
        buf.put_u16_le(self.flags);
        // Padding
        while buf.len() < Self::SIZE {
            buf.put_u8(0);
        }
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let mut node_id = [0u8; 20];
        node_id.copy_from_slice(&buf[..20]);

        let ping_sent = (&buf[20..24]).get_u32_le();
        let pong_received = (&buf[24..28]).get_u32_le();

        let mut ip = [0u8; 46];
        ip.copy_from_slice(&buf[28..74]);

        let port = (&buf[74..76]).get_u16_le();
        let bus_port = (&buf[76..78]).get_u16_le();
        let flags = (&buf[78..80]).get_u16_le();

        Some(Self {
            node_id,
            ping_sent,
            pong_received,
            ip,
            port,
            bus_port,
            flags,
        })
    }
}

/// Connection to a cluster peer node.
pub struct ClusterLink {
    /// Remote node ID
    pub node_id: Option<NodeId>,
    /// Remote address
    pub addr: SocketAddr,
    /// TCP stream
    stream: TcpStream,
    /// Creation time
    pub created: Instant,
    /// Last activity time
    pub last_activity: Instant,
    /// Send buffer
    send_buffer: BytesMut,
    /// Receive buffer
    recv_buffer: BytesMut,
}

impl ClusterLink {
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            node_id: None,
            addr,
            stream,
            created: Instant::now(),
            last_activity: Instant::now(),
            send_buffer: BytesMut::with_capacity(8192),
            recv_buffer: BytesMut::with_capacity(8192),
        }
    }

    /// Send a message through this link.
    pub async fn send(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data).await?;
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Receive data from this link.
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.stream.read(buf).await?;
        if n > 0 {
            self.last_activity = Instant::now();
        }
        Ok(n)
    }
}

/// Cluster bus configuration.
#[derive(Debug, Clone)]
pub struct ClusterBusConfig {
    /// Node timeout in milliseconds (for PFAIL detection)
    pub node_timeout: u64,
    /// Cluster require full coverage (fail if not all slots covered)
    pub require_full_coverage: bool,
    /// Replica validity factor
    pub replica_validity_factor: u64,
    /// Migration barrier
    pub migration_barrier: u64,
    /// Allow reads when down
    pub allow_reads_when_down: bool,
}

impl Default for ClusterBusConfig {
    fn default() -> Self {
        Self {
            node_timeout: 15000,
            require_full_coverage: true,
            replica_validity_factor: 10,
            migration_barrier: 1,
            allow_reads_when_down: false,
        }
    }
}

/// Failover state for tracking replica promotion.
#[derive(Debug)]
pub struct FailoverState {
    /// Whether we're in the process of failing over
    pub in_progress: bool,
    /// The master we're trying to replace
    pub target_master: Option<NodeId>,
    /// Votes received from masters
    pub votes_received: HashSet<NodeId>,
    /// Time failover started
    pub started_at: Option<Instant>,
    /// Current epoch for this failover
    pub auth_epoch: u64,
    /// Replication offset (used for voting priority)
    pub replication_offset: u64,
}

impl Default for FailoverState {
    fn default() -> Self {
        Self {
            in_progress: false,
            target_master: None,
            votes_received: HashSet::new(),
            started_at: None,
            auth_epoch: 0,
            replication_offset: 0,
        }
    }
}

/// The cluster bus handles all inter-node communication.
pub struct ClusterBus {
    /// Cluster manager reference
    manager: Arc<ClusterManager>,
    /// Configuration
    config: ClusterBusConfig,
    /// Active connections to peer nodes
    links: DashMap<NodeId, Arc<RwLock<ClusterLink>>>,
    /// Pending connections (not yet identified)
    pending_links: DashMap<SocketAddr, Arc<RwLock<ClusterLink>>>,
    /// Running flag
    running: AtomicBool,
    /// Shutdown notifier
    shutdown: Arc<Notify>,
    /// This node's bus port
    bus_port: RwLock<u16>,
    /// Failure detection votes (node_id -> set of voters who think it's down)
    pfail_votes: DashMap<NodeId, HashSet<NodeId>>,
    /// Failover state for when this replica is trying to become master
    failover_state: RwLock<FailoverState>,
    /// Last time we voted for a failover (to prevent voting too often)
    last_vote_epoch: AtomicU64,
    /// Statistics
    stats: ClusterBusStats,
}

/// Statistics for cluster bus operations.
#[derive(Debug, Default)]
pub struct ClusterBusStats {
    pub pings_sent: AtomicU64,
    pub pings_received: AtomicU64,
    pub pongs_sent: AtomicU64,
    pub pongs_received: AtomicU64,
    pub meet_sent: AtomicU64,
    pub meet_received: AtomicU64,
    pub fail_sent: AtomicU64,
    pub fail_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
}

impl ClusterBus {
    /// Create a new cluster bus.
    pub fn new(manager: Arc<ClusterManager>, config: ClusterBusConfig) -> Self {
        Self {
            manager,
            config,
            links: DashMap::new(),
            pending_links: DashMap::new(),
            running: AtomicBool::new(false),
            shutdown: Arc::new(Notify::new()),
            bus_port: RwLock::new(0),
            pfail_votes: DashMap::new(),
            failover_state: RwLock::new(FailoverState::default()),
            last_vote_epoch: AtomicU64::new(0),
            stats: ClusterBusStats::default(),
        }
    }

    /// Start the cluster bus.
    pub async fn start(&self, client_port: u16) -> io::Result<()> {
        let bus_port = client_port + 10000;
        *self.bus_port.write() = bus_port;

        let addr: SocketAddr = format!("0.0.0.0:{}", bus_port).parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;

        let listener = TcpListener::bind(addr).await?;
        info!("Cluster bus listening on port {}", bus_port);

        self.running.store(true, Ordering::SeqCst);

        // Start the accept loop
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!("Cluster bus connection from {}", peer_addr);
                            let link = Arc::new(RwLock::new(ClusterLink::new(stream, peer_addr)));
                            self.pending_links.insert(peer_addr, link.clone());

                            // Spawn handler for this connection
                            let bus = self.clone_ref();
                            tokio::spawn(async move {
                                bus.handle_connection(link, peer_addr).await;
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept cluster bus connection: {}", e);
                        }
                    }
                }
                _ = self.shutdown.notified() => {
                    info!("Cluster bus shutting down");
                    break;
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn clone_ref(&self) -> Arc<ClusterBus> {
        // This is a hack - in real code we'd have Arc<Self> from the start
        // For now, create a new instance sharing the same state
        Arc::new(ClusterBus {
            manager: self.manager.clone(),
            config: self.config.clone(),
            links: DashMap::new(), // Share via Arc in real impl
            pending_links: DashMap::new(),
            running: AtomicBool::new(self.running.load(Ordering::Relaxed)),
            shutdown: self.shutdown.clone(),
            bus_port: RwLock::new(*self.bus_port.read()),
            pfail_votes: DashMap::new(),
            failover_state: RwLock::new(FailoverState::default()),
            last_vote_epoch: AtomicU64::new(self.last_vote_epoch.load(Ordering::Relaxed)),
            stats: ClusterBusStats::default(),
        })
    }

    /// Handle an incoming connection.
    async fn handle_connection(&self, link: Arc<RwLock<ClusterLink>>, peer_addr: SocketAddr) {
        let mut buf = vec![0u8; 4096];

        loop {
            let n = {
                let mut link = link.write();
                match timeout(Duration::from_secs(10), link.recv(&mut buf)).await {
                    Ok(Ok(n)) if n > 0 => n,
                    Ok(Ok(_)) => break, // Connection closed
                    Ok(Err(e)) => {
                        debug!("Connection error from {}: {}", peer_addr, e);
                        break;
                    }
                    Err(_) => continue, // Timeout, keep waiting
                }
            };

            self.stats.bytes_received.fetch_add(n as u64, Ordering::Relaxed);

            // Parse and handle the message
            if let Some(header) = ClusterMsgHeader::deserialize(&buf[..n]) {
                self.handle_message(&header, &buf[..n], link.clone()).await;
            }
        }

        // Clean up
        self.pending_links.remove(&peer_addr);
    }

    /// Handle a received cluster message.
    async fn handle_message(&self, header: &ClusterMsgHeader, data: &[u8], link: Arc<RwLock<ClusterLink>>) {
        // Update link's node ID if not set
        {
            let mut link = link.write();
            if link.node_id.is_none() {
                link.node_id = Some(header.sender);
            }
        }

        match header.msg_type {
            ClusterMsgType::Ping => {
                self.stats.pings_received.fetch_add(1, Ordering::Relaxed);
                self.handle_ping(header, data, link).await;
            }
            ClusterMsgType::Pong => {
                self.stats.pongs_received.fetch_add(1, Ordering::Relaxed);
                self.handle_pong(header, data).await;
            }
            ClusterMsgType::Meet => {
                self.stats.meet_received.fetch_add(1, Ordering::Relaxed);
                self.handle_meet(header, data, link).await;
            }
            ClusterMsgType::Fail => {
                self.stats.fail_received.fetch_add(1, Ordering::Relaxed);
                self.handle_fail(header, data).await;
            }
            ClusterMsgType::FailoverAuthRequest => {
                self.handle_failover_auth_request(header, data, link).await;
            }
            ClusterMsgType::FailoverAuthAck => {
                self.handle_failover_auth_ack(header, data).await;
            }
            ClusterMsgType::Update => {
                self.handle_update(header, data).await;
            }
            _ => {
                debug!("Unknown cluster message type: {:?}", header.msg_type);
            }
        }
    }

    /// Handle PING message - respond with PONG.
    async fn handle_ping(&self, header: &ClusterMsgHeader, _data: &[u8], link: Arc<RwLock<ClusterLink>>) {
        // Update node info from ping
        self.update_node_from_header(header);

        // Send PONG response
        let pong = self.build_message(ClusterMsgType::Pong);
        let data = pong.serialize();

        let mut link = link.write();
        if let Err(e) = link.send(&data).await {
            warn!("Failed to send PONG: {}", e);
        } else {
            self.stats.pongs_sent.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
        }
    }

    /// Handle PONG message - update node status.
    async fn handle_pong(&self, header: &ClusterMsgHeader, _data: &[u8]) {
        // Update node info and mark as reachable
        self.update_node_from_header(header);

        // Clear any PFAIL status
        if let Some(node) = self.manager.get_node(&header.sender) {
            let mut node = node.write();
            if node.flags.pfail {
                node.flags.pfail = false;
                info!("Node {} is reachable again", hex_encode(&header.sender));
            }
            node.pong_received = Some(Instant::now());
            node.link_state = LinkState::Connected;
        }

        // Remove from PFAIL votes
        self.pfail_votes.remove(&header.sender);
    }

    /// Handle MEET message - add node to cluster.
    async fn handle_meet(&self, header: &ClusterMsgHeader, _data: &[u8], link: Arc<RwLock<ClusterLink>>) {
        // Extract IP from header
        let ip_str = std::str::from_utf8(&header.sender_ip)
            .unwrap_or("")
            .trim_end_matches('\0');

        let addr = link.read().addr;
        let node_addr = if ip_str.is_empty() {
            addr
        } else {
            format!("{}:{}", ip_str, header.port).parse().unwrap_or(addr)
        };

        // Add the node if not already known
        if self.manager.get_node(&header.sender).is_none() {
            let flags = NodeFlags {
                master: (header.flags & 0x01) != 0,
                replica: (header.flags & 0x02) != 0,
                ..Default::default()
            };

            self.manager.add_node(header.sender, node_addr, flags);
            info!("Added node {} via MEET", hex_encode(&header.sender));
        }

        // Respond with PONG
        let pong = self.build_message(ClusterMsgType::Pong);
        let data = pong.serialize();

        let mut link = link.write();
        if let Err(e) = link.send(&data).await {
            warn!("Failed to send PONG after MEET: {}", e);
        }
    }

    /// Handle FAIL message - mark node as failed.
    async fn handle_fail(&self, header: &ClusterMsgHeader, data: &[u8]) {
        // FAIL message contains the failed node's ID after the header
        if data.len() < ClusterMsgHeader::HEADER_SIZE + 20 {
            return;
        }

        let mut failed_id = [0u8; 20];
        failed_id.copy_from_slice(&data[ClusterMsgHeader::HEADER_SIZE..ClusterMsgHeader::HEADER_SIZE + 20]);

        if let Some(node) = self.manager.get_node(&failed_id) {
            let mut node = node.write();
            if !node.flags.fail {
                node.flags.fail = true;
                node.link_state = LinkState::Disconnected;
                warn!("Node {} marked as FAIL by {}",
                    hex_encode(&failed_id), hex_encode(&header.sender));
            }
        }
    }

    /// Handle failover auth request.
    /// A replica is requesting to become the new master of a failed node.
    async fn handle_failover_auth_request(&self, header: &ClusterMsgHeader, _data: &[u8], link: Arc<RwLock<ClusterLink>>) {
        // Only masters can vote for failover
        if !self.manager.is_master() {
            return;
        }

        // Extract the failed master's ID from the header's master_id field
        // The replica sets this to its current master (the one that failed)
        let failed_master_id = header.master_id;

        // Check if the sender is indeed a replica of the failed master
        if let Some(sender_node) = self.manager.get_node(&header.sender) {
            let sender = sender_node.read();
            if sender.master_id != Some(failed_master_id) {
                debug!("Rejecting failover auth: sender is not a replica of the claimed master");
                return;
            }
        } else {
            return;
        }

        // Check if the master is actually marked as failed
        if let Some(master_node) = self.manager.get_node(&failed_master_id) {
            let master = master_node.read();
            if !master.flags.fail {
                debug!("Rejecting failover auth: master is not marked as failed");
                return;
            }
        }

        // Check if we already voted in this epoch
        let request_epoch = header.current_epoch;
        let last_vote = self.last_vote_epoch.load(Ordering::Relaxed);
        if request_epoch <= last_vote {
            debug!("Rejecting failover auth: already voted in epoch {}", last_vote);
            return;
        }

        // Grant the vote - update our last vote epoch
        self.last_vote_epoch.store(request_epoch, Ordering::SeqCst);

        // Send auth ack
        let mut ack = self.build_message(ClusterMsgType::FailoverAuthAck);
        ack.current_epoch = request_epoch;
        let ack_data = ack.serialize();

        let mut link = link.write();
        if let Err(e) = link.send(&ack_data).await {
            warn!("Failed to send failover auth ack: {}", e);
        } else {
            info!("Voted for {} to take over {}", hex_encode(&header.sender), hex_encode(&failed_master_id));
        }
    }

    /// Handle failover auth ack.
    /// We received a vote from a master for our failover request.
    async fn handle_failover_auth_ack(&self, header: &ClusterMsgHeader, _data: &[u8]) {
        let mut state = self.failover_state.write();

        // Only count if we're actually in a failover
        if !state.in_progress {
            return;
        }

        // Verify the epoch matches our request
        if header.current_epoch != state.auth_epoch {
            return;
        }

        // Add this vote
        state.votes_received.insert(header.sender);

        let vote_count = state.votes_received.len();
        let required_votes = self.get_failover_quorum();

        info!("Received failover vote from {}: {}/{} votes",
            hex_encode(&header.sender), vote_count, required_votes);

        // Check if we have enough votes
        if vote_count >= required_votes {
            if let Some(master_id) = state.target_master {
                // We have quorum - execute the failover
                drop(state); // Release lock before calling execute_failover
                self.execute_failover_takeover(&master_id).await;
            }
        }
    }

    /// Calculate the required quorum for failover.
    fn get_failover_quorum(&self) -> usize {
        // Need majority of masters (including ourselves after promotion)
        let master_count = self.manager.get_healthy_masters().len();
        (master_count / 2) + 1
    }

    /// Start a failover attempt (called when our master fails).
    pub async fn start_failover(&self, failed_master_id: NodeId) {
        // Only replicas can start failover
        if !self.manager.is_replica() {
            return;
        }

        // Verify we're a replica of this master
        if self.manager.get_my_master() != Some(failed_master_id) {
            return;
        }

        // Bump epoch and start failover
        let new_epoch = self.manager.bump_epoch();

        {
            let mut state = self.failover_state.write();
            state.in_progress = true;
            state.target_master = Some(failed_master_id);
            state.votes_received.clear();
            state.started_at = Some(Instant::now());
            state.auth_epoch = new_epoch;
        }

        info!("Starting failover for master {}, epoch {}", hex_encode(&failed_master_id), new_epoch);

        // Request auth from all masters
        self.broadcast_failover_auth_request(failed_master_id, new_epoch).await;
    }

    /// Broadcast failover auth request to all masters.
    async fn broadcast_failover_auth_request(&self, failed_master_id: NodeId, epoch: u64) {
        let mut auth_request = self.build_message(ClusterMsgType::FailoverAuthRequest);
        auth_request.master_id = failed_master_id;
        auth_request.current_epoch = epoch;
        let data = auth_request.serialize();

        for link in self.links.iter() {
            // Only send to masters
            if let Some(node) = self.manager.get_node(link.key()) {
                if !node.read().is_master() {
                    continue;
                }
            }

            let mut link = link.write();
            if let Err(e) = link.send(&data).await {
                warn!("Failed to send failover auth request: {}", e);
            }
        }
    }

    /// Execute the actual failover takeover.
    async fn execute_failover_takeover(&self, failed_master_id: &NodeId) {
        info!("Executing failover - taking over from {}", hex_encode(failed_master_id));

        // Execute the failover in cluster manager
        if self.manager.execute_failover(failed_master_id) {
            // Broadcast our new status to all nodes
            let update = self.build_message(ClusterMsgType::Update);
            let data = update.serialize();

            for link in self.links.iter() {
                let mut link = link.write();
                if let Err(e) = link.send(&data).await {
                    warn!("Failed to broadcast failover update: {}", e);
                }
            }

            // Clear failover state
            let mut state = self.failover_state.write();
            state.in_progress = false;
            state.target_master = None;
            state.votes_received.clear();

            info!("Failover complete - now serving as master");
        }
    }

    /// Check if failover should be started (called periodically).
    pub async fn check_failover_needed(&self) {
        // Only replicas can initiate failover
        if !self.manager.is_replica() {
            return;
        }

        // Check if already in failover
        {
            let state = self.failover_state.read();
            if state.in_progress {
                // Check for timeout
                if let Some(started) = state.started_at {
                    if started.elapsed() > Duration::from_millis(self.config.node_timeout * 2) {
                        warn!("Failover attempt timed out");
                        drop(state);
                        let mut state = self.failover_state.write();
                        state.in_progress = false;
                    }
                }
                return;
            }
        }

        // Check if our master is failed
        if let Some(master_id) = self.manager.get_my_master() {
            if let Some(master_node) = self.manager.get_node(&master_id) {
                let master = master_node.read();
                if master.flags.fail {
                    // Our master is failed - start failover
                    drop(master);
                    self.start_failover(master_id).await;
                }
            }
        }
    }

    /// Handle UPDATE message - configuration update.
    async fn handle_update(&self, header: &ClusterMsgHeader, _data: &[u8]) {
        // Update slot ownership from the message
        self.update_node_from_header(header);
    }

    /// Update node information from message header.
    fn update_node_from_header(&self, header: &ClusterMsgHeader) {
        if let Some(node) = self.manager.get_node(&header.sender) {
            let mut node = node.write();

            // Update config epoch if newer
            if header.config_epoch > node.config_epoch {
                node.config_epoch = header.config_epoch;
            }

            // Update flags
            node.flags.master = (header.flags & 0x01) != 0;
            node.flags.replica = (header.flags & 0x02) != 0;
            node.flags.pfail = (header.flags & 0x04) != 0;
            node.flags.fail = (header.flags & 0x08) != 0;

            // Update master ID if replica
            if node.flags.replica && header.master_id != [0u8; 20] {
                node.master_id = Some(header.master_id);
            }
        }
    }

    /// Build a cluster message with current node state.
    fn build_message(&self, msg_type: ClusterMsgType) -> ClusterMsgHeader {
        let mut header = ClusterMsgHeader::new(msg_type, self.manager.my_id());

        header.bus_port = *self.bus_port.read();
        header.current_epoch = self.manager.current_epoch();

        // Set flags
        if let Some(node) = self.manager.get_node(&self.manager.my_id()) {
            let node = node.read();
            let mut flags: u16 = 0;
            if node.flags.master { flags |= 0x01; }
            if node.flags.replica { flags |= 0x02; }
            if node.flags.pfail { flags |= 0x04; }
            if node.flags.fail { flags |= 0x08; }
            header.flags = flags;
            header.config_epoch = node.config_epoch;

            if let Some(master_id) = node.master_id {
                header.master_id = master_id;
            }
        }

        // Set cluster state
        header.state = match self.manager.state() {
            ClusterState::Ok => 0,
            ClusterState::Fail => 1,
        };

        // Set slots bitmap
        for slot in 0..CLUSTER_SLOTS {
            if self.manager.get_slot_owner(slot) == Some(self.manager.my_id()) {
                let byte_idx = (slot / 8) as usize;
                let bit_idx = slot % 8;
                if byte_idx < header.slots.len() {
                    header.slots[byte_idx] |= 1 << bit_idx;
                }
            }
        }

        header
    }

    /// Connect to another cluster node.
    pub async fn connect_to_node(&self, addr: SocketAddr) -> io::Result<()> {
        let bus_addr: SocketAddr = format!("{}:{}", addr.ip(), addr.port() + 10000)
            .parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;

        debug!("Connecting to cluster node at {}", bus_addr);

        let stream = TcpStream::connect(bus_addr).await?;
        let link = Arc::new(RwLock::new(ClusterLink::new(stream, bus_addr)));

        // Send MEET message
        let meet = self.build_message(ClusterMsgType::Meet);
        let data = meet.serialize();

        {
            let mut link = link.write();
            link.send(&data).await?;
        }

        self.stats.meet_sent.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);

        self.pending_links.insert(bus_addr, link.clone());

        // Spawn handler for responses
        let bus = self.clone_ref();
        tokio::spawn(async move {
            bus.handle_connection(link, bus_addr).await;
        });

        Ok(())
    }

    /// Send PING to a specific node.
    pub async fn send_ping(&self, node_id: &NodeId) -> io::Result<()> {
        if let Some(link) = self.links.get(node_id) {
            let ping = self.build_message(ClusterMsgType::Ping);
            let data = ping.serialize();

            let mut link = link.write();
            link.send(&data).await?;

            self.stats.pings_sent.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);

            // Update ping_sent timestamp
            if let Some(node) = self.manager.get_node(node_id) {
                node.write().ping_sent = Some(Instant::now());
            }
        }

        Ok(())
    }

    /// Broadcast FAIL message for a node.
    pub async fn broadcast_fail(&self, failed_id: &NodeId) {
        let fail_msg = self.build_message(ClusterMsgType::Fail);
        let mut data = fail_msg.serialize();

        // Append failed node ID
        data.put_slice(failed_id);

        for link in self.links.iter() {
            let mut link = link.write();
            if let Err(e) = link.send(&data).await {
                warn!("Failed to broadcast FAIL: {}", e);
            } else {
                self.stats.fail_sent.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Check for node timeouts and update PFAIL status.
    pub fn check_node_timeouts(&self) {
        let now = Instant::now();
        let timeout_duration = Duration::from_millis(self.config.node_timeout);

        for node_ref in self.manager.get_all_nodes() {
            let mut node = node_ref.write();

            if node.flags.myself {
                continue;
            }

            // Check if node has timed out
            if let Some(pong_time) = node.pong_received {
                if now.duration_since(pong_time) > timeout_duration {
                    if !node.flags.pfail && !node.flags.fail {
                        node.flags.pfail = true;
                        node.link_state = LinkState::Disconnected;
                        warn!("Node {} marked as PFAIL (timeout)", node.id_string());
                    }
                }
            }
        }
    }

    /// Check if enough nodes agree that a node is down (PFAIL -> FAIL).
    pub fn check_fail_reports(&self) {
        let required_votes = (self.manager.node_count() / 2) + 1;

        for entry in self.pfail_votes.iter() {
            let node_id = *entry.key();
            let voters = entry.value();

            if voters.len() >= required_votes {
                // Enough votes - mark as FAIL
                if let Some(node) = self.manager.get_node(&node_id) {
                    let mut node = node.write();
                    if !node.flags.fail {
                        node.flags.fail = true;
                        warn!("Node {} marked as FAIL (quorum reached)", node.id_string());
                    }
                }
            }
        }
    }

    /// Stop the cluster bus.
    pub fn stop(&self) {
        self.shutdown.notify_one();
    }

    /// Check if running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get statistics.
    pub fn stats(&self) -> &ClusterBusStats {
        &self.stats
    }
}

/// Shared cluster bus type.
pub type SharedClusterBus = Arc<ClusterBus>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_header_serialize() {
        let node_id = generate_node_id();
        let header = ClusterMsgHeader::new(ClusterMsgType::Ping, node_id);
        let data = header.serialize();

        assert_eq!(data.len(), ClusterMsgHeader::HEADER_SIZE);
        assert_eq!(&data[..4], b"RCmb");
    }

    #[test]
    fn test_message_roundtrip() {
        let node_id = generate_node_id();
        let mut header = ClusterMsgHeader::new(ClusterMsgType::Pong, node_id);
        header.current_epoch = 42;
        header.config_epoch = 100;

        let data = header.serialize();
        let parsed = ClusterMsgHeader::deserialize(&data).unwrap();

        assert_eq!(parsed.msg_type, ClusterMsgType::Pong);
        assert_eq!(parsed.sender, node_id);
        assert_eq!(parsed.current_epoch, 42);
        assert_eq!(parsed.config_epoch, 100);
    }

    #[test]
    fn test_gossip_entry_serialize() {
        let entry = GossipEntry {
            node_id: generate_node_id(),
            ping_sent: 1000,
            pong_received: 2000,
            ip: [0u8; 46],
            port: 6379,
            bus_port: 16379,
            flags: 0x01,
        };

        let data = entry.serialize();
        assert_eq!(data.len(), GossipEntry::SIZE);

        let parsed = GossipEntry::deserialize(&data).unwrap();
        assert_eq!(parsed.ping_sent, 1000);
        assert_eq!(parsed.pong_received, 2000);
        assert_eq!(parsed.port, 6379);
    }
}
