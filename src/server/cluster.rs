//! Cluster slot management for Viator.
//!
//! This module implements Redis Cluster compatible slot management including:
//! - 16384 hash slots for key distribution
//! - Slot ownership and migration
//! - Node management (masters, replicas)
//! - Cluster topology and gossip state
//!
//! ## Architecture
//!
//! Redis Cluster uses consistent hashing with 16384 slots. Each key is mapped to
//! a slot using CRC16 (key) mod 16384. Slots are assigned to master nodes, and
//! replicas provide failover capability.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::info;

/// Encode bytes to hex string.
pub fn hex_encode(data: &[u8]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(data.len() * 2);
    for &byte in data {
        result.push(HEX_CHARS[(byte >> 4) as usize] as char);
        result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
    }
    result
}

/// Total number of slots in Redis Cluster.
pub const CLUSTER_SLOTS: u16 = 16384;

/// Slot type (0-16383).
pub type Slot = u16;

/// Calculate the slot for a key using CRC16.
#[must_use]
pub fn key_slot(key: &[u8]) -> Slot {
    // Handle hash tags: {tag}key -> use "tag" for slot calculation
    if let Some(slot_key) = extract_hash_tag(key) {
        crc16_ccitt(slot_key) % CLUSTER_SLOTS
    } else {
        crc16_ccitt(key) % CLUSTER_SLOTS
    }
}

/// Extract hash tag from key if present.
/// Hash tag is the substring between first { and first } after it.
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let start = key.iter().position(|&b| b == b'{')?;
    let end = key[start + 1..].iter().position(|&b| b == b'}')?;

    if end > 0 {
        Some(&key[start + 1..start + 1 + end])
    } else {
        None
    }
}

/// CRC16-CCITT implementation for slot calculation.
/// This matches the Redis implementation exactly.
fn crc16_ccitt(data: &[u8]) -> u16 {
    const CRC16_TABLE: [u16; 256] = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a,
        0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294,
        0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462,
        0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509,
        0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695,
        0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5,
        0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948,
        0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
        0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b,
        0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f,
        0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
        0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046,
        0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
        0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e,
        0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691,
        0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
        0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d,
        0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16,
        0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8,
        0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e,
        0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93,
        0x3eb2, 0x0ed1, 0x1ef0,
    ];

    let mut crc: u16 = 0;
    for &byte in data {
        let index = ((crc >> 8) ^ u16::from(byte)) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[index];
    }
    crc
}

/// Represents a range of slots [start, end] (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
    pub start: Slot,
    pub end: Slot,
}

impl SlotRange {
    /// Create a new slot range.
    #[must_use]
    pub fn new(start: Slot, end: Slot) -> Self {
        Self { start, end }
    }

    /// Check if a slot is within this range.
    #[must_use]
    pub fn contains(&self, slot: Slot) -> bool {
        slot >= self.start && slot <= self.end
    }

    /// Number of slots in this range.
    #[must_use]
    pub fn len(&self) -> usize {
        (self.end - self.start + 1) as usize
    }

    /// Check if the range is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false // A valid range always has at least one slot
    }

    /// Iterate over all slots in this range.
    pub fn iter(&self) -> impl Iterator<Item = Slot> {
        self.start..=self.end
    }
}

/// State of a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Normal operation
    Normal,
    /// Slot is being migrated to another node
    Migrating { target_node: NodeId },
    /// Slot is being imported from another node
    Importing { source_node: NodeId },
}

/// Unique identifier for a cluster node (40 hex characters in Redis).
pub type NodeId = [u8; 20];

/// Generate a random node ID.
#[must_use]
pub fn generate_node_id() -> NodeId {
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut id = [0u8; 20];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Use timestamp and random-ish data
    let bytes = now.to_le_bytes();
    id[..16].copy_from_slice(&bytes);

    // Add some variation using address for randomness
    let ptr = std::ptr::from_ref(&id) as usize;
    id[16..].copy_from_slice(&ptr.to_le_bytes()[..4]);

    id
}

/// Node flags indicating node state and role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeFlags {
    /// Node is a master
    pub master: bool,
    /// Node is a replica
    pub replica: bool,
    /// Node is possibly failing (PFAIL)
    pub pfail: bool,
    /// Node is confirmed failing (FAIL)
    pub fail: bool,
    /// This is the current node (myself)
    pub myself: bool,
    /// Node has no address yet
    pub noaddr: bool,
    /// Handshake in progress
    pub handshake: bool,
}

impl NodeFlags {
    /// Create flags for a master node.
    #[must_use]
    pub fn master() -> Self {
        Self {
            master: true,
            ..Default::default()
        }
    }

    /// Create flags for a replica node.
    #[must_use]
    pub fn replica() -> Self {
        Self {
            replica: true,
            ..Default::default()
        }
    }

    /// Format flags as Redis CLUSTER NODES string.
    #[must_use]
    pub fn to_string(&self) -> String {
        let mut parts = Vec::new();

        if self.myself {
            parts.push("myself");
        }
        if self.master {
            parts.push("master");
        }
        if self.replica {
            parts.push("slave");
        }
        if self.pfail {
            parts.push("fail?");
        }
        if self.fail {
            parts.push("fail");
        }
        if self.noaddr {
            parts.push("noaddr");
        }
        if self.handshake {
            parts.push("handshake");
        }

        if parts.is_empty() {
            "noflags".to_string()
        } else {
            parts.join(",")
        }
    }
}

/// Information about a cluster node.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique node identifier
    pub id: NodeId,
    /// Node address
    pub addr: SocketAddr,
    /// Cluster bus port (usually node port + 10000)
    pub bus_port: u16,
    /// Node flags
    pub flags: NodeFlags,
    /// Master ID if this is a replica
    pub master_id: Option<NodeId>,
    /// Ping sent timestamp
    pub ping_sent: Option<Instant>,
    /// Pong received timestamp
    pub pong_received: Option<Instant>,
    /// Configuration epoch
    pub config_epoch: u64,
    /// Link state
    pub link_state: LinkState,
    /// Slots owned by this node (for masters)
    pub slots: Vec<SlotRange>,
}

impl ClusterNode {
    /// Create a new cluster node.
    #[must_use]
    pub fn new(id: NodeId, addr: SocketAddr, flags: NodeFlags) -> Self {
        Self {
            id,
            addr,
            bus_port: addr.port() + 10000,
            flags,
            master_id: None,
            ping_sent: None,
            pong_received: Some(Instant::now()),
            config_epoch: 0,
            link_state: LinkState::Connected,
            slots: Vec::new(),
        }
    }

    /// Format node ID as hex string.
    #[must_use]
    pub fn id_string(&self) -> String {
        hex_encode(&self.id)
    }

    /// Check if this node is a master.
    #[must_use]
    pub fn is_master(&self) -> bool {
        self.flags.master
    }

    /// Check if this node is a replica.
    #[must_use]
    pub fn is_replica(&self) -> bool {
        self.flags.replica
    }

    /// Check if this node is healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self.flags.fail && !self.flags.pfail && self.link_state == LinkState::Connected
    }

    /// Count total slots owned.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.slots.iter().map(|r| r.len()).sum()
    }

    /// Check if this node owns a specific slot.
    #[must_use]
    pub fn owns_slot(&self, slot: Slot) -> bool {
        self.slots.iter().any(|r| r.contains(slot))
    }

    /// Format as CLUSTER NODES line.
    #[must_use]
    pub fn to_cluster_nodes_line(&self) -> String {
        let master_id = self
            .master_id
            .map(|id| hex_encode(&id))
            .unwrap_or_else(|| "-".to_string());

        let slots_str: Vec<String> = self
            .slots
            .iter()
            .map(|r| {
                if r.start == r.end {
                    format!("{}", r.start)
                } else {
                    format!("{}-{}", r.start, r.end)
                }
            })
            .collect();

        format!(
            "{} {}:{}@{} {} {} 0 {} {} {}",
            self.id_string(),
            self.addr.ip(),
            self.addr.port(),
            self.bus_port,
            self.flags.to_string(),
            master_id,
            self.config_epoch,
            match self.link_state {
                LinkState::Connected => "connected",
                LinkState::Disconnected => "disconnected",
            },
            slots_str.join(" ")
        )
    }
}

/// Link state between nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    Connected,
    Disconnected,
}

/// Overall cluster state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    /// All slots are covered
    Ok,
    /// Some slots are not covered
    Fail,
}

/// Cluster manager handling all cluster operations.
pub struct ClusterManager {
    /// This node's ID
    my_id: NodeId,
    /// This node's address
    my_addr: RwLock<Option<SocketAddr>>,
    /// Whether cluster mode is enabled
    enabled: RwLock<bool>,
    /// Current cluster state
    state: RwLock<ClusterState>,
    /// Current configuration epoch
    current_epoch: AtomicU64,
    /// All known nodes
    nodes: DashMap<NodeId, Arc<RwLock<ClusterNode>>>,
    /// Slot to node mapping (which node owns each slot)
    slot_owners: RwLock<[Option<NodeId>; CLUSTER_SLOTS as usize]>,
    /// Slot states (migrating/importing)
    slot_states: DashMap<Slot, SlotState>,
    /// Keys pending migration (slot -> keys)
    migrating_keys: DashMap<Slot, HashSet<Bytes>>,
    /// Statistics
    stats: ClusterStats,
}

impl std::fmt::Debug for ClusterManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterManager")
            .field("my_id", &hex_encode(&self.my_id))
            .field("enabled", &*self.enabled.read())
            .field("state", &*self.state.read())
            .field("node_count", &self.nodes.len())
            .finish()
    }
}

/// Statistics for cluster operations.
#[derive(Debug, Default)]
pub struct ClusterStats {
    /// Commands redirected with MOVED
    pub moved_redirects: AtomicU64,
    /// Commands redirected with ASK
    pub ask_redirects: AtomicU64,
    /// Keys migrated
    pub keys_migrated: AtomicU64,
    /// Keys imported
    pub keys_imported: AtomicU64,
    /// Cluster state changes
    pub state_changes: AtomicU64,
}

/// Result of checking slot ownership for a command.
#[derive(Debug)]
pub enum SlotCheck {
    /// This node owns the slot
    Owned,
    /// Redirect to another node with MOVED
    Moved { slot: Slot, addr: SocketAddr },
    /// Redirect to another node with ASK (migration in progress)
    Ask { slot: Slot, addr: SocketAddr },
    /// Slot is not covered by any node
    ClusterDown,
}

impl ClusterManager {
    /// Create a new cluster manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            my_id: generate_node_id(),
            my_addr: RwLock::new(None),
            enabled: RwLock::new(false),
            state: RwLock::new(ClusterState::Fail),
            current_epoch: AtomicU64::new(0),
            nodes: DashMap::new(),
            slot_owners: RwLock::new([None; CLUSTER_SLOTS as usize]),
            slot_states: DashMap::new(),
            migrating_keys: DashMap::new(),
            stats: ClusterStats::default(),
        }
    }

    /// Enable cluster mode.
    pub fn enable(&self, addr: SocketAddr) {
        *self.my_addr.write() = Some(addr);
        *self.enabled.write() = true;

        // Add ourselves as a node
        let flags = NodeFlags {
            master: true,
            myself: true,
            ..Default::default()
        };
        let node = ClusterNode::new(self.my_id, addr, flags);
        self.nodes.insert(self.my_id, Arc::new(RwLock::new(node)));
    }

    /// Disable cluster mode.
    pub fn disable(&self) {
        *self.enabled.write() = false;
        self.nodes.clear();
        *self.slot_owners.write() = [None; CLUSTER_SLOTS as usize];
        self.slot_states.clear();
    }

    /// Check if cluster mode is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        *self.enabled.read()
    }

    /// Get this node's ID.
    #[must_use]
    pub fn my_id(&self) -> NodeId {
        self.my_id
    }

    /// Get this node's ID as hex string.
    #[must_use]
    pub fn my_id_string(&self) -> String {
        hex_encode(&self.my_id)
    }

    /// Get current cluster state.
    #[must_use]
    pub fn state(&self) -> ClusterState {
        *self.state.read()
    }

    /// Check slot ownership for a key.
    pub fn check_slot(&self, key: &[u8]) -> SlotCheck {
        if !*self.enabled.read() {
            return SlotCheck::Owned;
        }

        let slot = key_slot(key);
        self.check_slot_number(slot)
    }

    /// Check slot ownership for a slot number.
    pub fn check_slot_number(&self, slot: Slot) -> SlotCheck {
        if !*self.enabled.read() {
            return SlotCheck::Owned;
        }

        // Check for migration states first
        if let Some(state) = self.slot_states.get(&slot) {
            match *state {
                SlotState::Migrating { target_node } => {
                    // Key is being migrated, need to check if it's already moved
                    if let Some(node) = self.nodes.get(&target_node) {
                        let addr = node.read().addr;
                        self.stats.ask_redirects.fetch_add(1, Ordering::Relaxed);
                        return SlotCheck::Ask { slot, addr };
                    }
                }
                SlotState::Importing { .. } => {
                    // We're importing this slot, so we handle it
                    return SlotCheck::Owned;
                }
                SlotState::Normal => {}
            }
        }

        // Check normal ownership
        let owners = self.slot_owners.read();
        match owners[slot as usize] {
            Some(owner_id) if owner_id == self.my_id => SlotCheck::Owned,
            Some(owner_id) => {
                if let Some(node) = self.nodes.get(&owner_id) {
                    let addr = node.read().addr;
                    self.stats.moved_redirects.fetch_add(1, Ordering::Relaxed);
                    SlotCheck::Moved { slot, addr }
                } else {
                    SlotCheck::ClusterDown
                }
            }
            None => SlotCheck::ClusterDown,
        }
    }

    /// Add slots to this node.
    pub fn add_slots(&self, slots: &[Slot]) {
        let mut owners = self.slot_owners.write();

        for &slot in slots {
            if slot < CLUSTER_SLOTS {
                owners[slot as usize] = Some(self.my_id);
            }
        }

        // Update our node's slot list
        if let Some(node) = self.nodes.get(&self.my_id) {
            let mut node = node.write();
            node.slots = Self::slots_to_ranges(&owners, self.my_id);
        }

        drop(owners);
        self.update_cluster_state();
    }

    /// Delete slots from this node.
    pub fn del_slots(&self, slots: &[Slot]) {
        let mut owners = self.slot_owners.write();

        for &slot in slots {
            if slot < CLUSTER_SLOTS && owners[slot as usize] == Some(self.my_id) {
                owners[slot as usize] = None;
            }
        }

        // Update our node's slot list
        if let Some(node) = self.nodes.get(&self.my_id) {
            let mut node = node.write();
            node.slots = Self::slots_to_ranges(&owners, self.my_id);
        }

        drop(owners);
        self.update_cluster_state();
    }

    /// Convert slot ownership array to ranges for a node.
    fn slots_to_ranges(
        owners: &[Option<NodeId>; CLUSTER_SLOTS as usize],
        node_id: NodeId,
    ) -> Vec<SlotRange> {
        let mut ranges = Vec::new();
        let mut start: Option<Slot> = None;

        for (slot, owner) in owners.iter().enumerate() {
            let slot = slot as Slot;
            let is_owned = *owner == Some(node_id);

            match (start, is_owned) {
                (None, true) => start = Some(slot),
                (Some(s), false) => {
                    ranges.push(SlotRange::new(s, slot - 1));
                    start = None;
                }
                _ => {}
            }
        }

        // Handle last range
        if let Some(s) = start {
            ranges.push(SlotRange::new(s, CLUSTER_SLOTS - 1));
        }

        ranges
    }

    /// Start migrating a slot to another node.
    pub fn set_slot_migrating(&self, slot: Slot, target_node: NodeId) -> bool {
        if slot >= CLUSTER_SLOTS {
            return false;
        }

        let owners = self.slot_owners.read();
        if owners[slot as usize] != Some(self.my_id) {
            return false;
        }
        drop(owners);

        self.slot_states
            .insert(slot, SlotState::Migrating { target_node });
        true
    }

    /// Start importing a slot from another node.
    pub fn set_slot_importing(&self, slot: Slot, source_node: NodeId) -> bool {
        if slot >= CLUSTER_SLOTS {
            return false;
        }

        self.slot_states
            .insert(slot, SlotState::Importing { source_node });
        true
    }

    /// Set slot to stable state (after migration complete).
    pub fn set_slot_stable(&self, slot: Slot) {
        self.slot_states.remove(&slot);
    }

    /// Assign slot to a specific node.
    pub fn set_slot_node(&self, slot: Slot, node_id: NodeId) -> bool {
        if slot >= CLUSTER_SLOTS {
            return false;
        }

        if !self.nodes.contains_key(&node_id) {
            return false;
        }

        let mut owners = self.slot_owners.write();
        owners[slot as usize] = Some(node_id);
        drop(owners);

        // Clear any migration state
        self.slot_states.remove(&slot);
        self.update_cluster_state();
        true
    }

    /// Add a node to the cluster.
    pub fn add_node(&self, id: NodeId, addr: SocketAddr, flags: NodeFlags) {
        let node = ClusterNode::new(id, addr, flags);
        self.nodes.insert(id, Arc::new(RwLock::new(node)));
    }

    /// Remove a node from the cluster.
    pub fn remove_node(&self, id: &NodeId) -> bool {
        if *id == self.my_id {
            return false;
        }

        // Clear any slots owned by this node
        let mut owners = self.slot_owners.write();
        for owner in owners.iter_mut() {
            if *owner == Some(*id) {
                *owner = None;
            }
        }
        drop(owners);

        self.nodes.remove(id);
        self.update_cluster_state();
        true
    }

    /// Get a node by ID.
    #[must_use]
    pub fn get_node(&self, id: &NodeId) -> Option<Arc<RwLock<ClusterNode>>> {
        self.nodes.get(id).map(|n| n.clone())
    }

    /// Get all nodes.
    #[must_use]
    pub fn get_all_nodes(&self) -> Vec<Arc<RwLock<ClusterNode>>> {
        self.nodes.iter().map(|n| n.clone()).collect()
    }

    /// Get node count.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get slot info for CLUSTER SLOTS command.
    #[must_use]
    pub fn get_slots_info(&self) -> Vec<SlotInfo> {
        let owners = self.slot_owners.read();
        let mut result = Vec::new();
        let mut current_start: Option<Slot> = None;
        let mut current_owner: Option<NodeId> = None;

        for (slot, owner) in owners.iter().enumerate() {
            let slot = slot as Slot;

            match (current_start, current_owner, owner) {
                (None, _, Some(id)) => {
                    current_start = Some(slot);
                    current_owner = Some(*id);
                }
                (Some(_start), Some(prev), Some(id)) if prev == *id => {
                    // Continue current range
                }
                (Some(start), Some(prev), next) => {
                    // End current range
                    if let Some(node) = self.nodes.get(&prev) {
                        let node = node.read();
                        let replicas = self.get_replicas_of(&prev);
                        result.push(SlotInfo {
                            start,
                            end: slot - 1,
                            master: NodeInfo {
                                addr: node.addr,
                                id: node.id,
                            },
                            replicas: replicas
                                .iter()
                                .map(|n| {
                                    let n = n.read();
                                    NodeInfo {
                                        addr: n.addr,
                                        id: n.id,
                                    }
                                })
                                .collect(),
                        });
                    }

                    current_start = next.map(|_| slot);
                    current_owner = *next;
                }
                (None, None, None) => {}
                _ => {}
            }
        }

        // Handle last range
        if let (Some(start), Some(owner)) = (current_start, current_owner) {
            if let Some(node) = self.nodes.get(&owner) {
                let node = node.read();
                let replicas = self.get_replicas_of(&owner);
                result.push(SlotInfo {
                    start,
                    end: CLUSTER_SLOTS - 1,
                    master: NodeInfo {
                        addr: node.addr,
                        id: node.id,
                    },
                    replicas: replicas
                        .iter()
                        .map(|n| {
                            let n = n.read();
                            NodeInfo {
                                addr: n.addr,
                                id: n.id,
                            }
                        })
                        .collect(),
                });
            }
        }

        result
    }

    /// Get replicas of a master node.
    fn get_replicas_of(&self, master_id: &NodeId) -> Vec<Arc<RwLock<ClusterNode>>> {
        self.nodes
            .iter()
            .filter(|n| n.read().master_id == Some(*master_id))
            .map(|n| n.clone())
            .collect()
    }

    /// Update cluster state based on slot coverage.
    fn update_cluster_state(&self) {
        let owners = self.slot_owners.read();
        let all_covered = owners.iter().all(|o| o.is_some());

        let new_state = if all_covered {
            ClusterState::Ok
        } else {
            ClusterState::Fail
        };

        let old_state = *self.state.read();
        if old_state != new_state {
            *self.state.write() = new_state;
            self.stats.state_changes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get CLUSTER INFO output.
    #[must_use]
    pub fn get_cluster_info(&self) -> HashMap<String, String> {
        let owners = self.slot_owners.read();
        let slots_assigned: usize = owners.iter().filter(|o| o.is_some()).count();
        let slots_ok: usize = slots_assigned; // Simplified - in real impl, check node health

        let mut info = HashMap::new();

        info.insert(
            "cluster_state".to_string(),
            match *self.state.read() {
                ClusterState::Ok => "ok".to_string(),
                ClusterState::Fail => "fail".to_string(),
            },
        );
        info.insert(
            "cluster_slots_assigned".to_string(),
            slots_assigned.to_string(),
        );
        info.insert("cluster_slots_ok".to_string(), slots_ok.to_string());
        info.insert("cluster_slots_pfail".to_string(), "0".to_string());
        info.insert("cluster_slots_fail".to_string(), "0".to_string());
        info.insert(
            "cluster_known_nodes".to_string(),
            self.nodes.len().to_string(),
        );
        info.insert(
            "cluster_size".to_string(),
            self.nodes
                .iter()
                .filter(|n| n.read().is_master())
                .count()
                .to_string(),
        );
        info.insert(
            "cluster_current_epoch".to_string(),
            self.current_epoch.load(Ordering::Relaxed).to_string(),
        );
        info.insert("cluster_my_epoch".to_string(), "0".to_string());

        info
    }

    /// Get CLUSTER NODES output.
    #[must_use]
    pub fn get_cluster_nodes(&self) -> String {
        self.nodes
            .iter()
            .map(|n| n.read().to_cluster_nodes_line())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Get statistics.
    #[must_use]
    pub fn stats(&self) -> &ClusterStats {
        &self.stats
    }

    /// Get the owner of a slot.
    #[must_use]
    pub fn get_slot_owner(&self, slot: Slot) -> Option<NodeId> {
        if slot >= CLUSTER_SLOTS {
            return None;
        }
        self.slot_owners.read()[slot as usize]
    }

    /// Count how many slots are assigned.
    #[must_use]
    pub fn slots_assigned(&self) -> usize {
        self.slot_owners
            .read()
            .iter()
            .filter(|o| o.is_some())
            .count()
    }

    /// Count how many slots this node owns.
    #[must_use]
    pub fn my_slots_count(&self) -> usize {
        self.slot_owners
            .read()
            .iter()
            .filter(|o| *o == &Some(self.my_id))
            .count()
    }

    /// Bump the current epoch.
    pub fn bump_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Get current epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }

    /// Set current epoch (used during failover).
    pub fn set_epoch(&self, epoch: u64) {
        self.current_epoch.store(epoch, Ordering::SeqCst);
    }

    /// Update epoch if the provided one is higher.
    pub fn update_epoch_if_higher(&self, epoch: u64) {
        loop {
            let current = self.current_epoch.load(Ordering::Relaxed);
            if epoch <= current {
                break;
            }
            if self
                .current_epoch
                .compare_exchange(current, epoch, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Perform failover: take over slots from a failed master.
    /// This is called when this replica wins the failover election.
    pub fn execute_failover(&self, failed_master_id: &NodeId) -> bool {
        // Get slots owned by the failed master
        let slots_to_take: Vec<Slot> = {
            let owners = self.slot_owners.read();
            owners
                .iter()
                .enumerate()
                .filter(|(_, owner)| *owner == &Some(*failed_master_id))
                .map(|(slot, _)| slot as Slot)
                .collect()
        };

        if slots_to_take.is_empty() {
            return false;
        }

        // Take over the slots
        {
            let mut owners = self.slot_owners.write();
            for slot in &slots_to_take {
                owners[*slot as usize] = Some(self.my_id);
            }
        }

        // Update our node to be a master
        if let Some(node) = self.nodes.get(&self.my_id) {
            let mut node = node.write();
            node.flags.master = true;
            node.flags.replica = false;
            node.master_id = None;
            node.slots = Self::slots_to_ranges(&self.slot_owners.read(), self.my_id);
        }

        // Bump config epoch
        self.bump_epoch();

        // Update cluster state
        self.update_cluster_state();

        info!(
            "Failover complete: took over {} slots from {}",
            slots_to_take.len(),
            hex_encode(failed_master_id)
        );

        true
    }

    /// Set this node as a replica of another node.
    pub fn set_as_replica_of(&self, master_id: NodeId) {
        if let Some(node) = self.nodes.get(&self.my_id) {
            let mut node = node.write();
            node.flags.master = false;
            node.flags.replica = true;
            node.master_id = Some(master_id);
            node.slots.clear();
        }

        // Clear any slots we own
        let mut owners = self.slot_owners.write();
        for owner in owners.iter_mut() {
            if *owner == Some(self.my_id) {
                *owner = None;
            }
        }
        drop(owners);
        self.update_cluster_state();
    }

    /// Get this node's master (if replica).
    #[must_use]
    pub fn get_my_master(&self) -> Option<NodeId> {
        self.nodes.get(&self.my_id).and_then(|n| n.read().master_id)
    }

    /// Check if this node is a replica.
    #[must_use]
    pub fn is_replica(&self) -> bool {
        self.nodes
            .get(&self.my_id)
            .map(|n| n.read().flags.replica)
            .unwrap_or(false)
    }

    /// Check if this node is a master.
    #[must_use]
    pub fn is_master(&self) -> bool {
        self.nodes
            .get(&self.my_id)
            .map(|n| n.read().flags.master)
            .unwrap_or(false)
    }

    /// Get all replicas of a master.
    #[must_use]
    pub fn get_replicas(&self, master_id: &NodeId) -> Vec<Arc<RwLock<ClusterNode>>> {
        self.get_replicas_of(master_id)
    }

    /// Get all healthy masters.
    #[must_use]
    pub fn get_healthy_masters(&self) -> Vec<Arc<RwLock<ClusterNode>>> {
        self.nodes
            .iter()
            .filter(|n| {
                let node = n.read();
                node.is_master() && node.is_healthy()
            })
            .map(|n| n.clone())
            .collect()
    }

    /// Mark a node as failed.
    pub fn mark_node_failed(&self, node_id: &NodeId) {
        if let Some(node) = self.nodes.get(node_id) {
            let mut node = node.write();
            node.flags.fail = true;
            node.link_state = LinkState::Disconnected;
        }
    }

    /// Mark a node as possibly failed (PFAIL).
    pub fn mark_node_pfail(&self, node_id: &NodeId) {
        if let Some(node) = self.nodes.get(node_id) {
            let mut node = node.write();
            node.flags.pfail = true;
        }
    }

    /// Clear failure flags for a node.
    pub fn clear_node_failure(&self, node_id: &NodeId) {
        if let Some(node) = self.nodes.get(node_id) {
            let mut node = node.write();
            node.flags.pfail = false;
            node.flags.fail = false;
            node.link_state = LinkState::Connected;
        }
    }

    /// Get this node's address.
    #[must_use]
    pub fn my_addr(&self) -> Option<SocketAddr> {
        *self.my_addr.read()
    }
}

impl Default for ClusterManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Slot information for CLUSTER SLOTS command.
#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub start: Slot,
    pub end: Slot,
    pub master: NodeInfo,
    pub replicas: Vec<NodeInfo>,
}

/// Node information for slot assignments.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub addr: SocketAddr,
    pub id: NodeId,
}

/// Shared cluster manager type.
pub type SharedClusterManager = Arc<ClusterManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_slot() {
        // Test basic slot calculation
        let slot = key_slot(b"test");
        assert!(slot < CLUSTER_SLOTS);

        // Same key should always give same slot
        assert_eq!(key_slot(b"test"), key_slot(b"test"));
    }

    #[test]
    fn test_hash_tag() {
        // Keys with same hash tag should have same slot
        let slot1 = key_slot(b"user:{123}:name");
        let slot2 = key_slot(b"user:{123}:email");
        assert_eq!(slot1, slot2);

        // Different hash tags should (likely) have different slots
        let slot3 = key_slot(b"user:{456}:name");
        // Note: could theoretically be same, but statistically unlikely
        assert_ne!(slot1, slot3);
    }

    #[test]
    fn test_empty_hash_tag() {
        // Empty hash tag should use full key
        let slot1 = key_slot(b"{}test");
        let slot2 = key_slot(b"test");
        // Empty tag means use full key including {}
        assert_ne!(slot1, slot2);
    }

    #[test]
    fn test_slot_range() {
        let range = SlotRange::new(100, 200);

        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(range.contains(200));
        assert!(!range.contains(99));
        assert!(!range.contains(201));
        assert_eq!(range.len(), 101);
    }

    #[test]
    fn test_cluster_manager_basic() {
        let manager = ClusterManager::new();
        assert!(!manager.is_enabled());
        assert_eq!(manager.state(), ClusterState::Fail);
    }

    #[test]
    fn test_add_slots() {
        let manager = ClusterManager::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.enable(addr);

        manager.add_slots(&[0, 1, 2, 3, 4]);

        assert_eq!(manager.my_slots_count(), 5);
        assert_eq!(manager.get_slot_owner(0), Some(manager.my_id()));
        assert_eq!(manager.get_slot_owner(5), None);
    }

    #[test]
    fn test_slot_check() {
        let manager = ClusterManager::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.enable(addr);

        // Add all slots to this node
        let all_slots: Vec<_> = (0..CLUSTER_SLOTS).collect();
        manager.add_slots(&all_slots);

        // Should own all slots now
        assert_eq!(manager.state(), ClusterState::Ok);

        match manager.check_slot(b"test") {
            SlotCheck::Owned => {}
            other => panic!("Expected Owned, got {:?}", other),
        }
    }

    #[test]
    fn test_cluster_state() {
        let manager = ClusterManager::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        manager.enable(addr);

        // Initially fail (no slots assigned)
        assert_eq!(manager.state(), ClusterState::Fail);

        // Add all slots
        let all_slots: Vec<_> = (0..CLUSTER_SLOTS).collect();
        manager.add_slots(&all_slots);

        // Now should be Ok
        assert_eq!(manager.state(), ClusterState::Ok);

        // Remove some slots
        manager.del_slots(&[0, 1, 2]);

        // Should be Fail again
        assert_eq!(manager.state(), ClusterState::Fail);
    }

    #[test]
    fn test_node_flags_to_string() {
        let flags = NodeFlags {
            master: true,
            myself: true,
            ..Default::default()
        };
        assert_eq!(flags.to_string(), "myself,master");

        let replica_flags = NodeFlags {
            replica: true,
            fail: true,
            ..Default::default()
        };
        assert_eq!(replica_flags.to_string(), "slave,fail");
    }
}
