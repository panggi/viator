//! Replication state machine for master-replica synchronization.
//!
//! Implements full replication protocol:
//! - Initial full sync (VDB transfer)
//! - Partial resync (backlog-based)
//! - Replication offset tracking
//! - Replica failover support
//!
//! ## Protocol Flow
//!
//! 1. Replica sends REPLCONF to configure replication
//! 2. Replica sends PSYNC to request sync
//! 3. Master responds with FULLRESYNC or CONTINUE
//! 4. For full sync, master sends VDB snapshot
//! 5. Master streams commands to replicas

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Replication role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// This server is a master
    Master,
    /// This server is a replica
    Replica,
}

impl Default for ReplicationRole {
    fn default() -> Self {
        Self::Master
    }
}

/// Replication state for a replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    /// Initial state
    None,
    /// Connecting to master
    Connecting,
    /// Handshake in progress
    Handshake,
    /// Waiting for BGSAVE
    WaitBgsave,
    /// Receiving VDB bulk transfer
    ReceivingRdb,
    /// Online and receiving commands
    Online,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::None
    }
}

/// Replication backlog for partial resync.
#[derive(Debug)]
pub struct ReplicationBacklog {
    /// Circular buffer for commands
    buffer: RwLock<VecDeque<BacklogEntry>>,
    /// Maximum backlog size in bytes
    max_size: usize,
    /// Current size in bytes
    current_size: AtomicU64,
    /// First offset in backlog
    first_offset: AtomicI64,
    /// Current (next) offset
    current_offset: AtomicI64,
    /// Replication ID
    repl_id: RwLock<String>,
    /// Secondary replication ID (for failover)
    repl_id2: RwLock<String>,
    /// Offset for secondary ID
    repl_id2_offset: AtomicI64,
}

/// Entry in the replication backlog.
#[derive(Debug, Clone)]
struct BacklogEntry {
    /// Command data
    data: Bytes,
    /// Offset of this entry
    offset: i64,
}

impl ReplicationBacklog {
    /// Create a new backlog.
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: RwLock::new(VecDeque::with_capacity(1024)),
            max_size,
            current_size: AtomicU64::new(0),
            first_offset: AtomicI64::new(0),
            current_offset: AtomicI64::new(0),
            repl_id: RwLock::new(generate_repl_id()),
            repl_id2: RwLock::new(String::new()),
            repl_id2_offset: AtomicI64::new(-1),
        }
    }

    /// Add a command to the backlog.
    pub fn add(&self, data: Bytes) {
        let size = data.len() as u64;
        let offset = self.current_offset.fetch_add(data.len() as i64, Ordering::SeqCst);

        let mut buffer = self.buffer.write();
        buffer.push_back(BacklogEntry {
            data,
            offset,
        });

        let mut current = self.current_size.fetch_add(size, Ordering::Relaxed) + size;

        // Trim if over capacity
        while current > self.max_size as u64 && !buffer.is_empty() {
            if let Some(entry) = buffer.pop_front() {
                current -= entry.data.len() as u64;
                self.current_size.fetch_sub(entry.data.len() as u64, Ordering::Relaxed);
                self.first_offset.store(entry.offset + entry.data.len() as i64, Ordering::Relaxed);
            }
        }
    }

    /// Get data from a given offset.
    /// Returns None if the offset is not in the backlog.
    pub fn get_from_offset(&self, offset: i64) -> Option<Vec<Bytes>> {
        let first = self.first_offset.load(Ordering::Relaxed);
        let current = self.current_offset.load(Ordering::Relaxed);

        if offset < first || offset > current {
            return None;
        }

        let buffer = self.buffer.read();
        let mut result = Vec::new();

        for entry in buffer.iter() {
            if entry.offset >= offset {
                result.push(entry.data.clone());
            }
        }

        Some(result)
    }

    /// Check if an offset is valid for partial resync.
    pub fn can_partial_resync(&self, repl_id: &str, offset: i64) -> bool {
        let my_id = self.repl_id.read();
        let my_id2 = self.repl_id2.read();
        let id2_offset = self.repl_id2_offset.load(Ordering::Relaxed);

        // Check primary ID
        if *my_id == repl_id {
            let first = self.first_offset.load(Ordering::Relaxed);
            let current = self.current_offset.load(Ordering::Relaxed);
            return offset >= first && offset <= current;
        }

        // Check secondary ID (for failover)
        if !my_id2.is_empty() && *my_id2 == repl_id && offset <= id2_offset {
            let first = self.first_offset.load(Ordering::Relaxed);
            return offset >= first;
        }

        false
    }

    /// Get current replication ID.
    pub fn repl_id(&self) -> String {
        self.repl_id.read().clone()
    }

    /// Get current offset.
    pub fn current_offset(&self) -> i64 {
        self.current_offset.load(Ordering::Relaxed)
    }

    /// Set replication ID (for when promoted from replica).
    pub fn set_repl_id(&self, id: String) {
        *self.repl_id.write() = id;
    }

    /// Set secondary ID for failover.
    pub fn set_repl_id2(&self, id: String, offset: i64) {
        *self.repl_id2.write() = id;
        self.repl_id2_offset.store(offset, Ordering::Relaxed);
    }

    /// Reset the backlog (generate new ID).
    pub fn reset(&self) {
        *self.repl_id.write() = generate_repl_id();
        *self.repl_id2.write() = String::new();
        self.repl_id2_offset.store(-1, Ordering::Relaxed);
        self.buffer.write().clear();
        self.current_size.store(0, Ordering::Relaxed);
        self.first_offset.store(0, Ordering::Relaxed);
        self.current_offset.store(0, Ordering::Relaxed);
    }
}

impl Default for ReplicationBacklog {
    fn default() -> Self {
        // Default 1MB backlog
        Self::new(1024 * 1024)
    }
}

/// Connected replica information.
#[derive(Debug)]
pub struct ConnectedReplica {
    /// Replica ID (from REPLCONF)
    pub id: String,
    /// Replica address
    pub addr: SocketAddr,
    /// Current state
    pub state: AtomicU8Wrapper,
    /// Replication offset
    pub offset: AtomicI64,
    /// Last interaction time
    pub last_interaction: RwLock<Instant>,
    /// Capabilities
    pub capabilities: RwLock<ReplicaCapabilities>,
    /// Replica listening port
    pub listening_port: u16,
}

/// Wrapper for atomic ReplicaState.
#[derive(Debug)]
pub struct AtomicU8Wrapper(std::sync::atomic::AtomicU8);

impl AtomicU8Wrapper {
    pub fn new(state: ReplicaState) -> Self {
        Self(std::sync::atomic::AtomicU8::new(state as u8))
    }

    pub fn load(&self) -> ReplicaState {
        match self.0.load(Ordering::Relaxed) {
            0 => ReplicaState::None,
            1 => ReplicaState::Connecting,
            2 => ReplicaState::Handshake,
            3 => ReplicaState::WaitBgsave,
            4 => ReplicaState::ReceivingRdb,
            _ => ReplicaState::Online,
        }
    }

    pub fn store(&self, state: ReplicaState) {
        self.0.store(state as u8, Ordering::Relaxed);
    }
}

/// Replica capabilities from REPLCONF.
#[derive(Debug, Clone, Default)]
pub struct ReplicaCapabilities {
    /// Supports EOF marker
    pub eof: bool,
    /// Supports PSYNC2
    pub psync2: bool,
}

impl ConnectedReplica {
    /// Create a new connected replica.
    pub fn new(id: String, addr: SocketAddr, listening_port: u16) -> Self {
        Self {
            id,
            addr,
            state: AtomicU8Wrapper::new(ReplicaState::Connecting),
            offset: AtomicI64::new(-1),
            last_interaction: RwLock::new(Instant::now()),
            capabilities: RwLock::new(ReplicaCapabilities::default()),
            listening_port,
        }
    }

    /// Update last interaction time.
    pub fn touch(&self) {
        *self.last_interaction.write() = Instant::now();
    }

    /// Check if replica is stale.
    pub fn is_stale(&self, timeout: Duration) -> bool {
        self.last_interaction.read().elapsed() > timeout
    }
}

/// Master connection info (when this server is a replica).
#[derive(Debug)]
pub struct MasterInfo {
    /// Master host
    pub host: String,
    /// Master port
    pub port: u16,
    /// Connection state
    pub state: RwLock<ReplicaState>,
    /// Master replication ID
    pub repl_id: RwLock<String>,
    /// Current offset
    pub offset: AtomicI64,
    /// Last sync time
    pub last_sync: RwLock<Option<Instant>>,
    /// Link status
    pub link_status: AtomicBool,
}

impl MasterInfo {
    /// Create new master info.
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            state: RwLock::new(ReplicaState::None),
            repl_id: RwLock::new(String::new()),
            offset: AtomicI64::new(-1),
            last_sync: RwLock::new(None),
            link_status: AtomicBool::new(false),
        }
    }
}

/// Replication manager.
#[derive(Debug)]
pub struct ReplicationManager {
    /// Current role
    role: RwLock<ReplicationRole>,
    /// Replication backlog
    backlog: Arc<ReplicationBacklog>,
    /// Connected replicas (when master)
    replicas: DashMap<String, Arc<ConnectedReplica>>,
    /// Master info (when replica)
    master: RwLock<Option<MasterInfo>>,
    /// Minimum replicas for WAIT
    min_replicas_to_write: AtomicU64,
    /// Timeout for good replicas
    good_replicas_timeout: Duration,
    /// Statistics
    stats: ReplicationStats,
}

/// Replication statistics.
#[derive(Debug, Default)]
pub struct ReplicationStats {
    /// Total commands replicated
    pub commands_replicated: AtomicU64,
    /// Total bytes replicated
    pub bytes_replicated: AtomicU64,
    /// Full syncs performed
    pub full_syncs: AtomicU64,
    /// Partial syncs performed
    pub partial_syncs: AtomicU64,
    /// Failed syncs
    pub failed_syncs: AtomicU64,
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new() -> Self {
        Self {
            role: RwLock::new(ReplicationRole::Master),
            backlog: Arc::new(ReplicationBacklog::default()),
            replicas: DashMap::new(),
            master: RwLock::new(None),
            min_replicas_to_write: AtomicU64::new(0),
            good_replicas_timeout: Duration::from_secs(10),
            stats: ReplicationStats::default(),
        }
    }

    /// Create with custom backlog size.
    pub fn with_backlog_size(size: usize) -> Self {
        Self {
            role: RwLock::new(ReplicationRole::Master),
            backlog: Arc::new(ReplicationBacklog::new(size)),
            replicas: DashMap::new(),
            master: RwLock::new(None),
            min_replicas_to_write: AtomicU64::new(0),
            good_replicas_timeout: Duration::from_secs(10),
            stats: ReplicationStats::default(),
        }
    }

    /// Get current role.
    pub fn role(&self) -> ReplicationRole {
        *self.role.read()
    }

    /// Set role to master.
    pub fn set_master(&self) {
        *self.role.write() = ReplicationRole::Master;
        *self.master.write() = None;
        self.backlog.reset();
    }

    /// Set role to replica of given master.
    pub fn set_replica_of(&self, host: String, port: u16) {
        *self.role.write() = ReplicationRole::Replica;
        *self.master.write() = Some(MasterInfo::new(host, port));
        // Clear our replicas since we're now a replica
        self.replicas.clear();
    }

    /// Get replication ID.
    pub fn repl_id(&self) -> String {
        self.backlog.repl_id()
    }

    /// Get current replication offset.
    pub fn repl_offset(&self) -> i64 {
        self.backlog.current_offset()
    }

    /// Feed a command to the backlog (for propagation to replicas).
    pub fn feed_command(&self, cmd: Bytes) {
        if *self.role.read() == ReplicationRole::Master {
            self.backlog.add(cmd.clone());
            self.stats.commands_replicated.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes_replicated.fetch_add(cmd.len() as u64, Ordering::Relaxed);
        }
    }

    /// Handle PSYNC request from replica.
    /// Returns (response, data_to_send).
    pub fn handle_psync(&self, repl_id: &str, offset: i64) -> PsyncResponse {
        // Check if we can do partial resync
        if self.backlog.can_partial_resync(repl_id, offset) {
            // Partial resync possible
            if let Some(data) = self.backlog.get_from_offset(offset) {
                self.stats.partial_syncs.fetch_add(1, Ordering::Relaxed);
                return PsyncResponse::Continue {
                    offset: self.backlog.current_offset(),
                    data,
                };
            }
        }

        // Full resync required
        self.stats.full_syncs.fetch_add(1, Ordering::Relaxed);
        PsyncResponse::FullResync {
            repl_id: self.backlog.repl_id(),
            offset: self.backlog.current_offset(),
        }
    }

    /// Register a new replica.
    pub fn register_replica(&self, id: String, addr: SocketAddr, port: u16) -> Arc<ConnectedReplica> {
        let replica = Arc::new(ConnectedReplica::new(id.clone(), addr, port));
        self.replicas.insert(id, replica.clone());
        replica
    }

    /// Unregister a replica.
    pub fn unregister_replica(&self, id: &str) {
        self.replicas.remove(id);
    }

    /// Get replica count.
    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    /// Get connected replicas.
    pub fn get_replicas(&self) -> Vec<Arc<ConnectedReplica>> {
        self.replicas.iter().map(|e| e.value().clone()).collect()
    }

    /// Count replicas at or beyond the given offset.
    pub fn count_good_replicas(&self, offset: i64) -> usize {
        self.replicas
            .iter()
            .filter(|e| {
                let r = e.value();
                r.state.load() == ReplicaState::Online
                    && r.offset.load(Ordering::Relaxed) >= offset
                    && !r.is_stale(self.good_replicas_timeout)
            })
            .count()
    }

    /// Wait for replicas to acknowledge offset.
    pub async fn wait_for_replicas(&self, num_replicas: usize, offset: i64, timeout: Duration) -> usize {
        let start = Instant::now();

        loop {
            let count = self.count_good_replicas(offset);
            if count >= num_replicas {
                return count;
            }

            if start.elapsed() >= timeout {
                return count;
            }

            // Sleep briefly before checking again
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Update replica offset (from REPLCONF ACK).
    pub fn update_replica_offset(&self, replica_id: &str, offset: i64) {
        if let Some(replica) = self.replicas.get(replica_id) {
            replica.offset.store(offset, Ordering::Relaxed);
            replica.touch();
        }
    }

    /// Get master info (when replica).
    pub fn master_info(&self) -> Option<(String, u16)> {
        self.master.read().as_ref().map(|m| (m.host.clone(), m.port))
    }

    /// Get backlog reference.
    pub fn backlog(&self) -> Arc<ReplicationBacklog> {
        self.backlog.clone()
    }

    /// Get statistics.
    pub fn stats(&self) -> &ReplicationStats {
        &self.stats
    }

    /// Get replication info for INFO command.
    pub fn info(&self) -> ReplicationInfo {
        let role = *self.role.read();
        let master = self.master.read();

        ReplicationInfo {
            role,
            connected_replicas: self.replicas.len(),
            master_host: master.as_ref().map(|m| m.host.clone()),
            master_port: master.as_ref().map(|m| m.port),
            master_link_status: master.as_ref().map(|m| m.link_status.load(Ordering::Relaxed)),
            master_repl_offset: self.backlog.current_offset(),
            repl_backlog_active: self.backlog.current_size.load(Ordering::Relaxed) > 0,
            repl_backlog_size: self.backlog.max_size,
            repl_backlog_first_offset: self.backlog.first_offset.load(Ordering::Relaxed),
            repl_id: self.backlog.repl_id(),
        }
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Response to PSYNC command.
#[derive(Debug)]
pub enum PsyncResponse {
    /// Full resync required
    FullResync {
        /// Replication ID
        repl_id: String,
        /// Offset
        offset: i64,
    },
    /// Continue with partial resync
    Continue {
        /// Current offset
        offset: i64,
        /// Data to send
        data: Vec<Bytes>,
    },
}

/// Replication info for INFO command.
#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    /// Current role
    pub role: ReplicationRole,
    /// Number of connected replicas
    pub connected_replicas: usize,
    /// Master host (if replica)
    pub master_host: Option<String>,
    /// Master port (if replica)
    pub master_port: Option<u16>,
    /// Master link status (if replica)
    pub master_link_status: Option<bool>,
    /// Master replication offset
    pub master_repl_offset: i64,
    /// Backlog active
    pub repl_backlog_active: bool,
    /// Backlog size
    pub repl_backlog_size: usize,
    /// First backlog offset
    pub repl_backlog_first_offset: i64,
    /// Replication ID
    pub repl_id: String,
}

/// Shared replication manager type.
pub type SharedReplicationManager = Arc<ReplicationManager>;

/// Generate a random 40-character replication ID.
fn generate_repl_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&timestamp, &mut hasher);
    std::hash::Hash::hash(&std::process::id(), &mut hasher);

    let hash = std::hash::Hasher::finish(&hasher);

    format!("{:016x}{:016x}{:08x}", hash, timestamp as u64, std::process::id())
        .chars()
        .take(40)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backlog_add_and_get() {
        let backlog = ReplicationBacklog::new(1024);

        backlog.add(Bytes::from("SET foo bar\r\n"));
        backlog.add(Bytes::from("SET baz qux\r\n"));

        let data = backlog.get_from_offset(0).unwrap();
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn test_backlog_partial_resync() {
        let backlog = ReplicationBacklog::new(1024);
        let id = backlog.repl_id();

        backlog.add(Bytes::from("SET foo bar\r\n"));

        assert!(backlog.can_partial_resync(&id, 0));
        assert!(!backlog.can_partial_resync("wrong-id", 0));
    }

    #[test]
    fn test_replication_manager_role() {
        let manager = ReplicationManager::new();

        assert_eq!(manager.role(), ReplicationRole::Master);

        manager.set_replica_of("localhost".to_string(), 6379);
        assert_eq!(manager.role(), ReplicationRole::Replica);

        manager.set_master();
        assert_eq!(manager.role(), ReplicationRole::Master);
    }

    #[test]
    fn test_psync_full_resync() {
        let manager = ReplicationManager::new();

        // Unknown ID should trigger full resync
        let response = manager.handle_psync("unknown-id", 0);
        assert!(matches!(response, PsyncResponse::FullResync { .. }));
    }

    #[test]
    fn test_replica_registration() {
        let manager = ReplicationManager::new();

        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let replica = manager.register_replica("replica-1".to_string(), addr, 6380);

        assert_eq!(manager.replica_count(), 1);
        assert_eq!(replica.listening_port, 6380);

        manager.unregister_replica("replica-1");
        assert_eq!(manager.replica_count(), 0);
    }
}
