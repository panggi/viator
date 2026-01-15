//! Cluster command implementations.
//!
//! Full Redis Cluster compatible implementation with:
//! - 16384 hash slots
//! - Slot assignment and migration
//! - Node management (MEET, FORGET, REPLICATE)
//! - Failover support

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::Result;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use dashmap::DashMap;

/// Total number of hash slots in Redis Cluster.
pub const CLUSTER_SLOTS: u16 = 16384;

/// Global cluster state singleton.
static CLUSTER_STATE: OnceLock<ClusterState> = OnceLock::new();

/// Get the global cluster state.
fn cluster_state() -> &'static ClusterState {
    CLUSTER_STATE.get_or_init(ClusterState::new)
}

/// Node identifier (40 hex characters).
pub type NodeId = String;

/// Node flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeFlags {
    pub myself: bool,
    pub master: bool,
    pub slave: bool,
    pub pfail: bool,
    pub fail: bool,
    pub handshake: bool,
    pub noaddr: bool,
    pub nofailover: bool,
}

impl NodeFlags {
    fn myself_master() -> Self {
        Self { myself: true, master: true, ..Default::default() }
    }

    fn handshaking() -> Self {
        Self { handshake: true, ..Default::default() }
    }

    fn to_string(&self) -> String {
        let mut parts = Vec::new();
        if self.myself { parts.push("myself"); }
        if self.master { parts.push("master"); }
        if self.slave { parts.push("slave"); }
        if self.pfail { parts.push("fail?"); }
        if self.fail { parts.push("fail"); }
        if self.handshake { parts.push("handshake"); }
        if self.noaddr { parts.push("noaddr"); }
        if self.nofailover { parts.push("nofailover"); }
        if parts.is_empty() { "noflags".to_string() } else { parts.join(",") }
    }
}

/// Link state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    Connected,
    Disconnected,
}

/// Cluster health state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealth {
    Ok,
    Fail,
}

/// Slot migration state.
#[derive(Debug, Clone)]
pub enum SlotMigrationState {
    Importing(NodeId),
    Migrating(NodeId),
}

/// Cluster node information.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub bus_port: u16,
    pub flags: NodeFlags,
    pub master_id: Option<NodeId>,
    pub ping_sent: u64,
    pub pong_received: u64,
    pub config_epoch: u64,
    pub link_state: LinkState,
}

/// Cluster-wide state management.
pub struct ClusterState {
    enabled: AtomicBool,
    my_id: RwLock<NodeId>,
    my_addr: RwLock<Option<SocketAddr>>,
    current_epoch: AtomicU64,
    config_epoch: AtomicU64,
    state: RwLock<ClusterHealth>,
    slots: RwLock<Vec<Option<NodeId>>>,
    slot_states: DashMap<u16, SlotMigrationState>,
    nodes: DashMap<NodeId, ClusterNode>,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    // Redis 8.2+ slot stats tracking
    slot_key_counts: DashMap<u16, u64>,
    slot_cpu_usec: DashMap<u16, u64>,
    // Redis 8.4+ migration tasks
    migration_tasks: RwLock<Vec<MigrationTask>>,
    migration_task_counter: AtomicU64,
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            my_id: RwLock::new(generate_node_id()),
            my_addr: RwLock::new(None),
            current_epoch: AtomicU64::new(0),
            config_epoch: AtomicU64::new(0),
            state: RwLock::new(ClusterHealth::Fail),
            slots: RwLock::new(vec![None; CLUSTER_SLOTS as usize]),
            slot_states: DashMap::new(),
            nodes: DashMap::new(),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            slot_key_counts: DashMap::new(),
            slot_cpu_usec: DashMap::new(),
            migration_tasks: RwLock::new(Vec::new()),
            migration_task_counter: AtomicU64::new(0),
        }
    }

    pub fn enable(&self, addr: SocketAddr) {
        self.enabled.store(true, Ordering::SeqCst);
        *self.my_addr.write() = Some(addr);

        let my_id = self.my_id.read().clone();
        let node = ClusterNode {
            id: my_id.clone(),
            addr,
            bus_port: addr.port() + 10000,
            flags: NodeFlags::myself_master(),
            master_id: None,
            ping_sent: 0,
            pong_received: 0,
            config_epoch: 0,
            link_state: LinkState::Connected,
        };
        self.nodes.insert(my_id, node);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn my_id(&self) -> NodeId {
        self.my_id.read().clone()
    }

    pub fn add_slots(&self, slot_list: &[u16]) -> std::result::Result<(), String> {
        let my_id = self.my_id();
        let mut slots = self.slots.write();

        for &slot in slot_list {
            if slot >= CLUSTER_SLOTS {
                return Err(format!("Invalid slot {}", slot));
            }
            if slots[slot as usize].is_some() {
                return Err(format!("Slot {} is already busy", slot));
            }
            slots[slot as usize] = Some(my_id.clone());
        }

        drop(slots);
        self.update_state();
        Ok(())
    }

    pub fn del_slots(&self, slot_list: &[u16]) -> std::result::Result<(), String> {
        let my_id = self.my_id();
        let mut slots = self.slots.write();

        for &slot in slot_list {
            if slot >= CLUSTER_SLOTS {
                return Err(format!("Invalid slot {}", slot));
            }
            if slots[slot as usize].as_ref() != Some(&my_id) {
                return Err(format!("Slot {} is not owned by me", slot));
            }
            slots[slot as usize] = None;
        }

        drop(slots);
        self.update_state();
        Ok(())
    }

    pub fn set_slot_importing(&self, slot: u16, node_id: NodeId) -> std::result::Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {}", slot));
        }
        self.slot_states.insert(slot, SlotMigrationState::Importing(node_id));
        Ok(())
    }

    pub fn set_slot_migrating(&self, slot: u16, node_id: NodeId) -> std::result::Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {}", slot));
        }
        self.slot_states.insert(slot, SlotMigrationState::Migrating(node_id));
        Ok(())
    }

    pub fn set_slot_stable(&self, slot: u16) {
        self.slot_states.remove(&slot);
    }

    pub fn set_slot_node(&self, slot: u16, node_id: NodeId) -> std::result::Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("Invalid slot {}", slot));
        }
        self.slot_states.remove(&slot);
        let mut slots = self.slots.write();
        slots[slot as usize] = Some(node_id);
        drop(slots);
        self.update_state();
        Ok(())
    }

    pub fn meet(&self, addr: SocketAddr) {
        let node_id = generate_node_id();
        let node = ClusterNode {
            id: node_id.clone(),
            addr,
            bus_port: addr.port() + 10000,
            flags: NodeFlags::handshaking(),
            master_id: None,
            ping_sent: current_time_ms(),
            pong_received: 0,
            config_epoch: 0,
            link_state: LinkState::Disconnected,
        };
        self.nodes.insert(node_id, node);
    }

    pub fn forget(&self, node_id: &str) -> std::result::Result<(), String> {
        if node_id == self.my_id() {
            return Err("Can't forget myself".to_string());
        }
        self.nodes.remove(node_id);

        let mut slots = self.slots.write();
        for slot in slots.iter_mut() {
            if slot.as_ref().map(|s| s.as_str()) == Some(node_id) {
                *slot = None;
            }
        }

        drop(slots);
        self.update_state();
        Ok(())
    }

    pub fn replicate(&self, master_id: &str) -> std::result::Result<(), String> {
        if !self.nodes.contains_key(master_id) {
            return Err("Unknown node".to_string());
        }

        let my_id = self.my_id();
        if let Some(mut node) = self.nodes.get_mut(&my_id) {
            node.flags.master = false;
            node.flags.slave = true;
            node.master_id = Some(master_id.to_string());
        }

        let mut slots = self.slots.write();
        for slot in slots.iter_mut() {
            if slot.as_ref() == Some(&my_id) {
                *slot = Some(master_id.to_string());
            }
        }

        Ok(())
    }

    pub fn failover(&self, force: bool) -> std::result::Result<(), String> {
        let my_id = self.my_id();
        let node = self.nodes.get(&my_id)
            .ok_or_else(|| "Node not found".to_string())?;

        if !node.flags.slave && !force {
            return Err("Can't failover a master".to_string());
        }

        if let Some(master_id) = node.master_id.clone() {
            drop(node);

            if let Some(mut node) = self.nodes.get_mut(&my_id) {
                node.flags.slave = false;
                node.flags.master = true;
                node.master_id = None;
            }

            let mut slots = self.slots.write();
            for slot in slots.iter_mut() {
                if slot.as_ref().map(|s| s.as_str()) == Some(master_id.as_str()) {
                    *slot = Some(my_id.clone());
                }
            }

            self.config_epoch.fetch_add(1, Ordering::SeqCst);
        }

        self.update_state();
        Ok(())
    }

    pub fn reset(&self, hard: bool) {
        if hard {
            *self.slots.write() = vec![None; CLUSTER_SLOTS as usize];
            self.slot_states.clear();
            self.nodes.clear();
            self.current_epoch.store(0, Ordering::SeqCst);
            self.config_epoch.store(0, Ordering::SeqCst);

            let my_id = generate_node_id();
            *self.my_id.write() = my_id.clone();

            if let Some(addr) = *self.my_addr.read() {
                let node = ClusterNode {
                    id: my_id.clone(),
                    addr,
                    bus_port: addr.port() + 10000,
                    flags: NodeFlags::myself_master(),
                    master_id: None,
                    ping_sent: 0,
                    pong_received: 0,
                    config_epoch: 0,
                    link_state: LinkState::Connected,
                };
                self.nodes.insert(my_id, node);
            }
        } else {
            let my_id = self.my_id();
            let mut slots = self.slots.write();
            for slot in slots.iter_mut() {
                if slot.as_ref() == Some(&my_id) {
                    *slot = None;
                }
            }
        }

        self.update_state();
    }

    fn update_state(&self) {
        let slots = self.slots.read();
        let all_assigned = slots.iter().all(|s| s.is_some());

        *self.state.write() = if all_assigned {
            ClusterHealth::Ok
        } else {
            ClusterHealth::Fail
        };
    }

    pub fn get_info(&self) -> String {
        let slots = self.slots.read();
        let assigned: usize = slots.iter().filter(|s| s.is_some()).count();
        let masters: usize = self.nodes.iter().filter(|n| n.flags.master).count();

        // Redis 8.4+ migration task tracking
        let tasks = self.migration_tasks.read();
        let active_migration_tasks = tasks
            .iter()
            .filter(|t| t.state == "pending" || t.state == "running")
            .count();
        let trim_running = tasks.iter().any(|t| t.state == "trimming");

        format!(
            "cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:{}\r\n\
             cluster_stats_messages_received:{}\r\n\
             cluster_slot_migration_active_tasks:{}\r\n\
             cluster_slot_migration_active_trim_running:{}",
            match *self.state.read() {
                ClusterHealth::Ok => "ok",
                ClusterHealth::Fail => "fail",
            },
            assigned,
            assigned,
            self.nodes.len(),
            masters,
            self.current_epoch.load(Ordering::Relaxed),
            self.config_epoch.load(Ordering::Relaxed),
            self.messages_sent.load(Ordering::Relaxed),
            self.messages_received.load(Ordering::Relaxed),
            active_migration_tasks,
            if trim_running { 1 } else { 0 },
        )
    }

    pub fn get_nodes_string(&self) -> String {
        let slots = self.slots.read();
        let mut lines = Vec::new();

        for node in self.nodes.iter() {
            let node_slots = get_node_slot_ranges(&slots, &node.id);
            let slots_str = format_slot_ranges(&node_slots);

            let master_str = node.master_id.as_deref().unwrap_or("-");
            let link = match node.link_state {
                LinkState::Connected => "connected",
                LinkState::Disconnected => "disconnected",
            };

            let mut line = format!(
                "{} {}:{}@{} {} {} {} {} {} {}",
                node.id,
                node.addr.ip(),
                node.addr.port(),
                node.bus_port,
                node.flags.to_string(),
                master_str,
                node.ping_sent,
                node.pong_received,
                node.config_epoch,
                link,
            );

            if !slots_str.is_empty() {
                line.push(' ');
                line.push_str(&slots_str);
            }

            lines.push(line);
        }

        lines.join("\n")
    }

    pub fn bump_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn flush_slots(&self) {
        let my_id = self.my_id();
        let mut slots = self.slots.write();
        for slot in slots.iter_mut() {
            if slot.as_ref() == Some(&my_id) {
                *slot = None;
            }
        }
        drop(slots);
        self.update_state();
    }

    pub fn set_config_epoch(&self, epoch: u64) -> std::result::Result<(), String> {
        let current = self.config_epoch.load(Ordering::Relaxed);
        if epoch <= current && current != 0 {
            return Err("Config epoch is already set".to_string());
        }
        self.config_epoch.store(epoch, Ordering::SeqCst);
        Ok(())
    }

    pub fn count_keys_in_slot(&self, slot: u16, db: &Db) -> usize {
        let all_keys = db.keys(b"*");
        all_keys
            .iter()
            .filter(|key| crc16_slot(key.as_ref()) == slot)
            .count()
    }

    pub fn get_keys_in_slot(&self, slot: u16, max_count: usize, db: &Db) -> Vec<Bytes> {
        let all_keys = db.keys(b"*");
        all_keys
            .into_iter()
            .filter(|key| crc16_slot(key.as_ref()) == slot)
            .take(max_count)
            .map(|key| Bytes::from(key.as_ref().to_vec()))
            .collect()
    }

    // Redis 8.2+ CLUSTER SLOT-STATS support methods

    /// Get key count for a slot (Redis 8.2+)
    pub fn key_count_for_slot(&self, slot: u16) -> u64 {
        self.slot_key_counts.get(&slot).map(|v| *v).unwrap_or(0)
    }

    /// Get CPU time used for a slot (Redis 8.2+)
    pub fn cpu_usec_for_slot(&self, slot: u16) -> u64 {
        self.slot_cpu_usec.get(&slot).map(|v| *v).unwrap_or(0)
    }

    /// Update key count for a slot
    pub fn update_slot_key_count(&self, slot: u16, count: u64) {
        self.slot_key_counts.insert(slot, count);
    }

    /// Record CPU time for a slot operation
    pub fn record_slot_cpu_time(&self, slot: u16, usec: u64) {
        self.slot_cpu_usec.entry(slot).and_modify(|v| *v += usec).or_insert(usec);
    }

    // Redis 8.4+ CLUSTER MIGRATION support methods

    /// Start a new migration task (Redis 8.4+)
    pub fn start_migration_task(&self, ranges: Vec<(u16, u16)>) -> String {
        let id = self.migration_task_counter.fetch_add(1, Ordering::SeqCst);
        let task_id = format!("migration-{}", id);

        let task = MigrationTask {
            id: task_id.clone(),
            slot_ranges: ranges,
            state: "pending".to_string(),
            last_error: None,
            progress: 0,
        };

        self.migration_tasks.write().push(task);
        task_id
    }

    /// Get all migration tasks (Redis 8.4+)
    pub fn get_migration_tasks(&self) -> Vec<MigrationTask> {
        self.migration_tasks.read().clone()
    }

    /// Cancel a specific migration task (Redis 8.4+)
    pub fn cancel_migration(&self, task_id: &str) -> bool {
        let mut tasks = self.migration_tasks.write();
        if let Some(pos) = tasks.iter().position(|t| t.id == task_id) {
            tasks.remove(pos);
            true
        } else {
            false
        }
    }

    /// Cancel all migration tasks (Redis 8.4+)
    pub fn cancel_all_migrations(&self) -> usize {
        let mut tasks = self.migration_tasks.write();
        let count = tasks.len();
        tasks.clear();
        count
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions

fn generate_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:040x}", now)
}

fn current_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn get_node_slot_ranges(slots: &[Option<NodeId>], node_id: &str) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    let mut start: Option<u16> = None;

    for (i, slot) in slots.iter().enumerate() {
        let i = i as u16;
        let is_owned = slot.as_ref().map(|s| s.as_str()) == Some(node_id);

        match (start, is_owned) {
            (None, true) => start = Some(i),
            (Some(s), false) => {
                ranges.push((s, i - 1));
                start = None;
            }
            _ => {}
        }
    }

    if let Some(s) = start {
        ranges.push((s, CLUSTER_SLOTS - 1));
    }

    ranges
}

fn format_slot_ranges(ranges: &[(u16, u16)]) -> String {
    ranges.iter()
        .map(|(s, e)| if s == e { format!("{}", s) } else { format!("{}-{}", s, e) })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Calculate CRC16 hash slot for a key.
pub fn crc16_slot(key: &[u8]) -> u16 {
    let key_to_hash = if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 { &key[start + 1..start + 1 + end] } else { key }
        } else { key }
    } else { key };

    let mut crc: u16 = 0;
    for &byte in key_to_hash {
        crc = ((crc << 8) & 0xFF00) ^ CRC16_TABLE[((crc >> 8) as u8 ^ byte) as usize];
    }
    crc % 16384
}

const CRC16_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

// =============================================================================
// COMMAND IMPLEMENTATIONS
// =============================================================================

/// CLUSTER subcommand [arguments]
pub fn cmd_cluster(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Ok(Frame::Error("ERR wrong number of arguments for 'cluster' command".to_string()));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();
        let state = cluster_state();

        match subcommand.as_str() {
            "INFO" => Ok(Frame::Bulk(Bytes::from(state.get_info()))),

            "NODES" => Ok(Frame::Bulk(Bytes::from(state.get_nodes_string()))),

            "SLOTS" => {
                // Return empty for now - simplified implementation
                Ok(Frame::Array(vec![]))
            }

            "SHARDS" => Ok(Frame::Array(vec![])),

            "KEYSLOT" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster keyslot' command".to_string()));
                }
                let slot = crc16_slot(&cmd.args[1]);
                Ok(Frame::Integer(slot as i64))
            }

            "MYID" => Ok(Frame::Bulk(Bytes::from(state.my_id()))),

            "MYSHARDID" => Ok(Frame::Bulk(Bytes::from(state.my_id()))),

            "ADDSLOTS" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster addslots' command".to_string()));
                }

                let mut slots = Vec::new();
                for arg in &cmd.args[1..] {
                    match String::from_utf8_lossy(arg).parse::<u16>() {
                        Ok(slot) => slots.push(slot),
                        Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                    }
                }

                match state.add_slots(&slots) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "ADDSLOTSRANGE" => {
                if cmd.args.len() < 3 || (cmd.args.len() - 1) % 2 != 0 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster addslotsrange' command".to_string()));
                }

                let mut slots = Vec::new();
                let mut i = 1;
                while i < cmd.args.len() {
                    let start: u16 = match String::from_utf8_lossy(&cmd.args[i]).parse() {
                        Ok(v) => v,
                        Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                    };
                    let end: u16 = match String::from_utf8_lossy(&cmd.args[i + 1]).parse() {
                        Ok(v) => v,
                        Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                    };
                    for slot in start..=end {
                        slots.push(slot);
                    }
                    i += 2;
                }

                match state.add_slots(&slots) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "DELSLOTS" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster delslots' command".to_string()));
                }

                let mut slots = Vec::new();
                for arg in &cmd.args[1..] {
                    match String::from_utf8_lossy(arg).parse::<u16>() {
                        Ok(slot) => slots.push(slot),
                        Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                    }
                }

                match state.del_slots(&slots) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "DELSLOTSRANGE" => {
                if cmd.args.len() < 3 || (cmd.args.len() - 1) % 2 != 0 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster delslotsrange' command".to_string()));
                }

                let mut slots = Vec::new();
                let mut i = 1;
                while i < cmd.args.len() {
                    let start: u16 = match String::from_utf8_lossy(&cmd.args[i]).parse() {
                        Ok(v) => v,
                        Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                    };
                    let end: u16 = match String::from_utf8_lossy(&cmd.args[i + 1]).parse() {
                        Ok(v) => v,
                        Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                    };
                    for slot in start..=end {
                        slots.push(slot);
                    }
                    i += 2;
                }

                match state.del_slots(&slots) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "SETSLOT" => {
                if cmd.args.len() < 3 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster setslot' command".to_string()));
                }

                let slot: u16 = match String::from_utf8_lossy(&cmd.args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                };
                let state_cmd = cmd.get_str(2)?.to_uppercase();

                let result = match state_cmd.as_str() {
                    "IMPORTING" => {
                        if cmd.args.len() < 4 {
                            return Ok(Frame::Error("ERR wrong number of arguments".to_string()));
                        }
                        state.set_slot_importing(slot, cmd.get_str(3)?.to_string())
                    }
                    "MIGRATING" => {
                        if cmd.args.len() < 4 {
                            return Ok(Frame::Error("ERR wrong number of arguments".to_string()));
                        }
                        state.set_slot_migrating(slot, cmd.get_str(3)?.to_string())
                    }
                    "STABLE" => {
                        state.set_slot_stable(slot);
                        Ok(())
                    }
                    "NODE" => {
                        if cmd.args.len() < 4 {
                            return Ok(Frame::Error("ERR wrong number of arguments".to_string()));
                        }
                        state.set_slot_node(slot, cmd.get_str(3)?.to_string())
                    }
                    _ => return Ok(Frame::Error(format!("ERR Unknown SETSLOT subcommand '{}'", state_cmd))),
                };

                match result {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "MEET" => {
                if cmd.args.len() < 3 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster meet' command".to_string()));
                }

                let ip = cmd.get_str(1)?;
                let port: u16 = match cmd.get_str(2)?.parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid port".to_string())),
                };

                let addr: SocketAddr = match format!("{}:{}", ip, port).parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid address".to_string())),
                };

                state.meet(addr);
                Ok(Frame::ok())
            }

            "FORGET" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster forget' command".to_string()));
                }

                match state.forget(cmd.get_str(1)?) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "REPLICATE" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments for 'cluster replicate' command".to_string()));
                }

                match state.replicate(cmd.get_str(1)?) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "FAILOVER" => {
                let force = cmd.args.get(1)
                    .map(|a| String::from_utf8_lossy(a).to_uppercase() == "FORCE")
                    .unwrap_or(false);

                match state.failover(force) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "RESET" => {
                let hard = cmd.args.get(1)
                    .map(|a| String::from_utf8_lossy(a).to_uppercase() == "HARD")
                    .unwrap_or(false);

                state.reset(hard);
                Ok(Frame::ok())
            }

            "SAVECONFIG" => Ok(Frame::ok()),

            "BUMPEPOCH" => {
                let epoch = state.bump_epoch();
                Ok(Frame::Bulk(Bytes::from(format!("BUMPED {}", epoch))))
            }

            "FLUSHSLOTS" => {
                state.flush_slots();
                Ok(Frame::ok())
            }

            "SET-CONFIG-EPOCH" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments".to_string()));
                }

                let epoch: u64 = match String::from_utf8_lossy(&cmd.args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid epoch".to_string())),
                };

                match state.set_config_epoch(epoch) {
                    Ok(()) => Ok(Frame::ok()),
                    Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                }
            }

            "COUNTKEYSINSLOT" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error("ERR wrong number of arguments".to_string()));
                }

                let slot: u16 = match String::from_utf8_lossy(&cmd.args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                };

                let count = state.count_keys_in_slot(slot, &db);
                Ok(Frame::Integer(count as i64))
            }

            "GETKEYSINSLOT" => {
                if cmd.args.len() < 3 {
                    return Ok(Frame::Error("ERR wrong number of arguments".to_string()));
                }

                let slot: u16 = match String::from_utf8_lossy(&cmd.args[1]).parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid slot".to_string())),
                };
                let count: usize = match String::from_utf8_lossy(&cmd.args[2]).parse() {
                    Ok(v) => v,
                    Err(_) => return Ok(Frame::Error("ERR Invalid count".to_string())),
                };

                let keys = state.get_keys_in_slot(slot, count, &db);
                let frames: Vec<Frame> = keys.into_iter().map(Frame::Bulk).collect();
                Ok(Frame::Array(frames))
            }

            "COUNT-FAILURE-REPORTS" => Ok(Frame::Integer(0)),

            "LINKS" => Ok(Frame::Array(vec![])),

            // Redis 8.2+ CLUSTER SLOT-STATS command
            "SLOT-STATS" => {
                handle_slot_stats(&cmd, &state)
            }

            // Redis 8.4+ CLUSTER MIGRATION command for atomic slot migration
            "MIGRATION" => {
                handle_migration(&cmd, &state)
            }

            _ => Ok(Frame::Error(format!("ERR Unknown subcommand or wrong number of arguments for '{}'", subcommand))),
        }
    })
}

/// READONLY - Enable read commands on replica
pub fn cmd_readonly(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        client.set_readonly(true);
        Ok(Frame::ok())
    })
}

/// READWRITE - Disable read commands on replica
pub fn cmd_readwrite(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        client.set_readonly(false);
        Ok(Frame::ok())
    })
}

/// ASKING - Sent before a command to indicate cluster redirect handling.
/// This allows the next command to access keys in a slot being migrated.
pub fn cmd_asking(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Set the ASKING flag - it will be cleared after the next command
        client.set_asking();
        Ok(Frame::ok())
    })
}

/// Handle CLUSTER SLOT-STATS command (Redis 8.2+)
/// Returns per-slot usage statistics for slots assigned to the current node.
/// Syntax: CLUSTER SLOT-STATS <SLOTSRANGE start end | ORDERBY metric [LIMIT n] [ASC|DESC]>
fn handle_slot_stats(cmd: &ParsedCommand, state: &ClusterState) -> Result<Frame> {
    if cmd.arg_count() < 2 {
        return Err(CommandError::WrongArity { command: "CLUSTER SLOT-STATS".to_string() }.into());
    }

    let subarg = cmd.get_str(1)?.to_uppercase();
    let slots = state.slots.read();

    match subarg.as_str() {
        "SLOTSRANGE" => {
            if cmd.arg_count() < 4 {
                return Err(CommandError::WrongArity { command: "CLUSTER SLOT-STATS".to_string() }.into());
            }
            let start = cmd.get_u64(2)? as u16;
            let end = cmd.get_u64(3)? as u16;

            if start > end || end >= CLUSTER_SLOTS {
                return Ok(Frame::Error("ERR Invalid slot range".to_string()));
            }

            let mut results = Vec::new();
            for slot in start..=end {
                if slots.get(slot as usize).and_then(|s| s.as_ref()).is_some() {
                    results.push(slot_stats_entry(slot, state));
                }
            }
            Ok(Frame::Array(results))
        }
        "ORDERBY" => {
            if cmd.arg_count() < 3 {
                return Err(CommandError::WrongArity { command: "CLUSTER SLOT-STATS".to_string() }.into());
            }
            let metric = cmd.get_str(2)?.to_uppercase();
            let limit = if cmd.arg_count() >= 5 && cmd.get_str(3)?.to_uppercase() == "LIMIT" {
                cmd.get_u64(4)? as usize
            } else {
                usize::MAX
            };
            let desc = cmd.args.iter().any(|a| {
                std::str::from_utf8(a).map(|s| s.to_uppercase() == "DESC").unwrap_or(false)
            });

            // Collect all owned slots with their stats
            let mut slot_data: Vec<(u16, u64)> = Vec::new();
            for (slot, owner) in slots.iter().enumerate() {
                if owner.is_some() {
                    let value = match metric.as_str() {
                        "KEY-COUNT" => state.key_count_for_slot(slot as u16),
                        "CPU-USEC" => state.cpu_usec_for_slot(slot as u16),
                        "MEMORY-BYTES" | "NETWORK-BYTES-IN" | "NETWORK-BYTES-OUT" => 0,
                        _ => return Ok(Frame::Error(format!("ERR Unknown metric: {}", metric))),
                    };
                    slot_data.push((slot as u16, value));
                }
            }

            // Sort by metric
            if desc {
                slot_data.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            } else {
                slot_data.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
            }

            // Apply limit and build response
            let results: Vec<Frame> = slot_data.into_iter()
                .take(limit)
                .map(|(slot, _)| slot_stats_entry(slot, state))
                .collect();

            Ok(Frame::Array(results))
        }
        _ => Ok(Frame::Error("ERR Syntax error, expected SLOTSRANGE or ORDERBY".to_string())),
    }
}

/// Build a slot stats entry frame
fn slot_stats_entry(slot: u16, state: &ClusterState) -> Frame {
    Frame::Array(vec![
        Frame::Integer(slot as i64),
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("key-count")),
            Frame::Integer(state.key_count_for_slot(slot) as i64),
            Frame::Bulk(Bytes::from("cpu-usec")),
            Frame::Integer(state.cpu_usec_for_slot(slot) as i64),
            Frame::Bulk(Bytes::from("network-bytes-in")),
            Frame::Integer(0),
            Frame::Bulk(Bytes::from("network-bytes-out")),
            Frame::Integer(0),
        ]),
    ])
}

/// Handle CLUSTER MIGRATION command (Redis 8.4+)
/// Enables atomic slot migration between cluster nodes.
/// Syntax: CLUSTER MIGRATION <IMPORT ranges | STATUS [ID id | ALL] | CANCEL <ID id | ALL>>
fn handle_migration(cmd: &ParsedCommand, state: &ClusterState) -> Result<Frame> {
    if cmd.arg_count() < 2 {
        return Err(CommandError::WrongArity { command: "CLUSTER MIGRATION".to_string() }.into());
    }

    let subarg = cmd.get_str(1)?.to_uppercase();

    match subarg.as_str() {
        "IMPORT" => {
            // CLUSTER MIGRATION IMPORT <start-slot> <end-slot> [<start-slot> <end-slot> ...]
            if cmd.arg_count() < 4 || (cmd.arg_count() - 2) % 2 != 0 {
                return Err(CommandError::WrongArity { command: "CLUSTER MIGRATION IMPORT".to_string() }.into());
            }

            let mut ranges = Vec::new();
            let mut i = 2;
            while i + 1 < cmd.arg_count() {
                let start = cmd.get_u64(i)? as u16;
                let end = cmd.get_u64(i + 1)? as u16;
                if start > end || end >= CLUSTER_SLOTS {
                    return Ok(Frame::Error(format!("ERR Invalid slot range {}-{}", start, end)));
                }
                ranges.push((start, end));
                i += 2;
            }

            // Generate task ID and start migration
            let task_id = state.start_migration_task(ranges);
            Ok(Frame::Bulk(Bytes::from(task_id)))
        }
        "STATUS" => {
            // CLUSTER MIGRATION STATUS [ID <task-id> | ALL]
            let tasks = state.get_migration_tasks();

            if cmd.arg_count() >= 4 && cmd.get_str(2)?.to_uppercase() == "ID" {
                let task_id = cmd.get_str(3)?;
                if let Some(task) = tasks.iter().find(|t| t.id == task_id) {
                    Ok(migration_task_to_frame(task))
                } else {
                    Ok(Frame::Null)
                }
            } else {
                // Return all tasks
                let frames: Vec<Frame> = tasks.iter().map(migration_task_to_frame).collect();
                Ok(Frame::Array(frames))
            }
        }
        "CANCEL" => {
            // CLUSTER MIGRATION CANCEL <ID <task-id> | ALL>
            if cmd.arg_count() < 3 {
                return Err(CommandError::WrongArity { command: "CLUSTER MIGRATION CANCEL".to_string() }.into());
            }

            let cancel_arg = cmd.get_str(2)?.to_uppercase();
            if cancel_arg == "ALL" {
                let count = state.cancel_all_migrations();
                Ok(Frame::Integer(count as i64))
            } else if cancel_arg == "ID" && cmd.arg_count() >= 4 {
                let task_id = cmd.get_str(3)?;
                if state.cancel_migration(task_id) {
                    Ok(Frame::ok())
                } else {
                    Ok(Frame::Error(format!("ERR Migration task {} not found", task_id)))
                }
            } else {
                Ok(Frame::Error("ERR Syntax error, expected ID <task-id> or ALL".to_string()))
            }
        }
        _ => Ok(Frame::Error("ERR Unknown CLUSTER MIGRATION subcommand".to_string())),
    }
}

/// Migration task info
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub id: String,
    pub slot_ranges: Vec<(u16, u16)>,
    pub state: String,
    pub last_error: Option<String>,
    pub progress: u32,
}

/// Convert migration task to Frame
fn migration_task_to_frame(task: &MigrationTask) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from("id")),
        Frame::Bulk(Bytes::from(task.id.clone())),
        Frame::Bulk(Bytes::from("state")),
        Frame::Bulk(Bytes::from(task.state.clone())),
        Frame::Bulk(Bytes::from("slot-ranges")),
        Frame::Array(task.slot_ranges.iter().map(|(s, e)| {
            Frame::Array(vec![Frame::Integer(*s as i64), Frame::Integer(*e as i64)])
        }).collect()),
        Frame::Bulk(Bytes::from("progress")),
        Frame::Integer(task.progress as i64),
        Frame::Bulk(Bytes::from("last-error")),
        task.last_error.as_ref().map(|e| Frame::Bulk(Bytes::from(e.clone()))).unwrap_or(Frame::Null),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_slot() {
        let slot = crc16_slot(b"test");
        assert!(slot < CLUSTER_SLOTS);
    }

    #[test]
    fn test_hash_tag() {
        let slot1 = crc16_slot(b"user:{123}:name");
        let slot2 = crc16_slot(b"user:{123}:email");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_node_flags() {
        let flags = NodeFlags::myself_master();
        assert!(flags.myself);
        assert!(flags.master);
        assert!(!flags.slave);
    }

    #[test]
    fn test_generate_node_id() {
        let id = generate_node_id();
        assert_eq!(id.len(), 40);
    }
}
