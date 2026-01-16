//! Redis Sentinel command implementations.
//!
//! Provides high availability through automatic failover monitoring.
//! Sentinel monitors Redis master/replica instances and promotes
//! replicas when masters fail.

use super::ParsedCommand;
use crate::Result;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// Global sentinel state singleton.
static SENTINEL_STATE: OnceLock<SentinelState> = OnceLock::new();

/// Sentinel quorum configuration.
pub const DEFAULT_QUORUM: u32 = 2;

/// Default down-after-milliseconds.
pub const DEFAULT_DOWN_AFTER_MS: u64 = 30000;

/// Default failover timeout.
pub const DEFAULT_FAILOVER_TIMEOUT_MS: u64 = 180000;

/// Default parallel syncs during failover.
pub const DEFAULT_PARALLEL_SYNCS: u32 = 1;

/// Sentinel instance role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentinelRole {
    /// This is a master.
    Master,
    /// This is a replica.
    Replica,
    /// This is another sentinel.
    Sentinel,
}

/// Monitored master instance state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterState {
    /// Master is up and healthy.
    Up,
    /// Master is subjectively down (this sentinel thinks it's down).
    SDown,
    /// Master is objectively down (quorum agrees it's down).
    ODown,
    /// Failover is in progress.
    Failover,
}

impl std::fmt::Display for MasterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MasterState::Up => write!(f, "ok"),
            MasterState::SDown => write!(f, "s_down"),
            MasterState::ODown => write!(f, "o_down"),
            MasterState::Failover => write!(f, "failover_in_progress"),
        }
    }
}

/// Configuration for a monitored master.
#[derive(Debug, Clone)]
pub struct MasterConfig {
    /// Master name (identifier).
    pub name: String,
    /// Master address.
    pub addr: SocketAddr,
    /// Number of sentinels required to agree for failover.
    pub quorum: u32,
    /// Time before marking master as subjectively down.
    pub down_after_ms: u64,
    /// Timeout for failover operation.
    pub failover_timeout_ms: u64,
    /// Number of replicas to sync in parallel during failover.
    pub parallel_syncs: u32,
    /// Authentication password.
    pub auth_pass: Option<String>,
    /// Authentication user.
    pub auth_user: Option<String>,
}

impl MasterConfig {
    pub fn new(name: String, addr: SocketAddr) -> Self {
        Self {
            name,
            addr,
            quorum: DEFAULT_QUORUM,
            down_after_ms: DEFAULT_DOWN_AFTER_MS,
            failover_timeout_ms: DEFAULT_FAILOVER_TIMEOUT_MS,
            parallel_syncs: DEFAULT_PARALLEL_SYNCS,
            auth_pass: None,
            auth_user: None,
        }
    }
}

/// Monitored master instance.
#[derive(Debug)]
pub struct MonitoredMaster {
    /// Configuration for this master.
    pub config: RwLock<MasterConfig>,
    /// Current state.
    pub state: RwLock<MasterState>,
    /// Known replicas.
    pub replicas: DashMap<String, ReplicaInfo>,
    /// Known sentinels monitoring this master.
    pub sentinels: DashMap<String, SentinelInfo>,
    /// Last successful ping time.
    pub last_ping: RwLock<Option<Instant>>,
    /// Number of sentinels agreeing master is down.
    pub sdown_votes: AtomicU64,
    /// Config epoch for this master.
    pub config_epoch: AtomicU64,
    /// Failover epoch.
    pub failover_epoch: AtomicU64,
    /// Notification channel name.
    pub notification_channel: String,
}

impl MonitoredMaster {
    pub fn new(config: MasterConfig) -> Self {
        let notification_channel = format!("+switch-master:{}", config.name);
        Self {
            config: RwLock::new(config),
            state: RwLock::new(MasterState::Up),
            replicas: DashMap::new(),
            sentinels: DashMap::new(),
            last_ping: RwLock::new(Some(Instant::now())),
            sdown_votes: AtomicU64::new(0),
            config_epoch: AtomicU64::new(0),
            failover_epoch: AtomicU64::new(0),
            notification_channel,
        }
    }
}

/// Information about a replica.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Replica ID (runid or generated).
    pub id: String,
    /// Replica address.
    pub addr: SocketAddr,
    /// Is replica online?
    pub online: bool,
    /// Replication offset.
    pub offset: u64,
    /// Last ping time.
    pub last_ping_ms: u64,
    /// Flags (e.g., s_down, disconnected).
    pub flags: String,
    /// Master link status.
    pub master_link_status: String,
}

/// Information about another sentinel.
#[derive(Debug, Clone)]
pub struct SentinelInfo {
    /// Sentinel ID (runid).
    pub id: String,
    /// Sentinel address.
    pub addr: SocketAddr,
    /// Last hello time.
    pub last_hello_ms: u64,
    /// Voted leader for current epoch.
    pub voted_leader: Option<String>,
    /// Voted leader epoch.
    pub voted_leader_epoch: u64,
    /// Is sentinel reachable?
    pub is_reachable: bool,
}

/// Global sentinel state.
pub struct SentinelState {
    /// Is sentinel mode enabled?
    enabled: AtomicBool,
    /// This sentinel's ID.
    my_id: RwLock<String>,
    /// Current epoch (for leader election).
    current_epoch: AtomicU64,
    /// Monitored masters by name.
    masters: DashMap<String, Arc<MonitoredMaster>>,
    /// Announce IP (if configured).
    announce_ip: RwLock<Option<String>>,
    /// Announce port (if configured).
    announce_port: RwLock<Option<u16>>,
}

impl SentinelState {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            my_id: RwLock::new(generate_sentinel_id()),
            current_epoch: AtomicU64::new(0),
            masters: DashMap::new(),
            announce_ip: RwLock::new(None),
            announce_port: RwLock::new(None),
        }
    }

    pub fn global() -> &'static Self {
        SENTINEL_STATE.get_or_init(SentinelState::new)
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn my_id(&self) -> String {
        self.my_id.read().clone()
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }

    pub fn bump_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn monitor(&self, config: MasterConfig) {
        let name = config.name.clone();
        let master = Arc::new(MonitoredMaster::new(config));
        self.masters.insert(name, master);
    }

    pub fn remove(&self, name: &str) -> bool {
        self.masters.remove(name).is_some()
    }

    pub fn get_master(&self, name: &str) -> Option<Arc<MonitoredMaster>> {
        self.masters.get(name).map(|m| m.value().clone())
    }

    pub fn master_names(&self) -> Vec<String> {
        self.masters.iter().map(|e| e.key().clone()).collect()
    }

    pub fn master_count(&self) -> usize {
        self.masters.len()
    }

    pub fn set_announce(&self, ip: Option<String>, port: Option<u16>) {
        if let Some(ip) = ip {
            *self.announce_ip.write() = Some(ip);
        }
        if let Some(port) = port {
            *self.announce_port.write() = Some(port);
        }
    }
}

impl Default for SentinelState {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions

fn generate_sentinel_id() -> String {
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

/// Type alias for command handler future.
pub type SentinelCmdFuture = Pin<Box<dyn Future<Output = Result<Frame>> + Send + 'static>>;

/// Handle SENTINEL command.
pub fn handle_sentinel_command(
    cmd: &ParsedCommand,
    _client_state: &ClientState,
) -> SentinelCmdFuture {
    let cmd = cmd.clone();

    Box::pin(async move {
        if cmd.arg_count() < 1 {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for 'sentinel' command".into(),
            ));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();
        let state = SentinelState::global();

        match subcommand.as_str() {
            "MASTERS" | "MASTER" => handle_masters(&cmd, state),
            "SLAVES" | "REPLICAS" => handle_replicas(&cmd, state),
            "SENTINELS" => handle_sentinels(&cmd, state),
            "GET-MASTER-ADDR-BY-NAME" => handle_get_master_addr(&cmd, state),
            "CKQUORUM" => handle_ckquorum(&cmd, state),
            "FAILOVER" => handle_failover(&cmd, state),
            "FLUSHCONFIG" => handle_flushconfig(state),
            "MONITOR" => handle_monitor(&cmd, state),
            "REMOVE" => handle_remove(&cmd, state),
            "SET" => handle_set(&cmd, state),
            "RESET" => handle_reset(&cmd, state),
            "PENDING-SCRIPTS" => handle_pending_scripts(),
            "INFO-CACHE" => handle_info_cache(&cmd, state),
            "SIMULATE-FAILURE" => handle_simulate_failure(&cmd, state),
            "MYID" => Ok(Frame::Bulk(state.my_id().into())),
            "DEBUG" => handle_debug(&cmd, state),
            "CONFIG" => handle_config(&cmd, state),
            "IS-MASTER-DOWN-BY-ADDR" => handle_is_master_down(&cmd, state),
            _ => Ok(Frame::Error(format!(
                "ERR Unknown sentinel subcommand '{}'",
                subcommand
            ))),
        }
    })
}

fn handle_masters(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    if cmd.arg_count() == 1 {
        // SENTINEL MASTERS - list all masters
        let mut result = Vec::new();
        for entry in state.masters.iter() {
            let master = entry.value();
            result.push(master_to_frame(master));
        }
        Ok(Frame::Array(result))
    } else if cmd.arg_count() >= 2 {
        // SENTINEL MASTER <name> - get specific master
        let name = cmd.get_str(1)?;
        if let Some(master) = state.get_master(name) {
            Ok(master_to_frame(&master))
        } else {
            Ok(Frame::Error(format!(
                "ERR No such master with that name: {}",
                name
            )))
        }
    } else {
        Ok(Frame::Error(
            "ERR wrong number of arguments for 'sentinel master' command".into(),
        ))
    }
}

fn master_to_frame(master: &MonitoredMaster) -> Frame {
    let config = master.config.read();
    let state_val = *master.state.read();

    let mut fields = Vec::new();

    fields.push(Frame::Bulk("name".into()));
    fields.push(Frame::Bulk(config.name.clone().into()));

    fields.push(Frame::Bulk("ip".into()));
    fields.push(Frame::Bulk(config.addr.ip().to_string().into()));

    fields.push(Frame::Bulk("port".into()));
    fields.push(Frame::Integer(config.addr.port() as i64));

    fields.push(Frame::Bulk("runid".into()));
    fields.push(Frame::Bulk("".into()));

    fields.push(Frame::Bulk("flags".into()));
    fields.push(Frame::Bulk(format!("master,{}", state_val).into()));

    fields.push(Frame::Bulk("link-pending-commands".into()));
    fields.push(Frame::Integer(0));

    fields.push(Frame::Bulk("link-refcount".into()));
    fields.push(Frame::Integer(1));

    fields.push(Frame::Bulk("last-ping-sent".into()));
    fields.push(Frame::Integer(0));

    fields.push(Frame::Bulk("last-ok-ping-reply".into()));
    let ping_ms = master
        .last_ping
        .read()
        .map(|t| t.elapsed().as_millis() as i64)
        .unwrap_or(0);
    fields.push(Frame::Integer(ping_ms));

    fields.push(Frame::Bulk("last-ping-reply".into()));
    fields.push(Frame::Integer(ping_ms));

    fields.push(Frame::Bulk("down-after-milliseconds".into()));
    fields.push(Frame::Integer(config.down_after_ms as i64));

    fields.push(Frame::Bulk("info-refresh".into()));
    fields.push(Frame::Integer(current_time_ms() as i64));

    fields.push(Frame::Bulk("role-reported".into()));
    fields.push(Frame::Bulk("master".into()));

    fields.push(Frame::Bulk("role-reported-time".into()));
    fields.push(Frame::Integer(current_time_ms() as i64));

    fields.push(Frame::Bulk("config-epoch".into()));
    fields.push(Frame::Integer(
        master.config_epoch.load(Ordering::Relaxed) as i64
    ));

    fields.push(Frame::Bulk("num-slaves".into()));
    fields.push(Frame::Integer(master.replicas.len() as i64));

    fields.push(Frame::Bulk("num-other-sentinels".into()));
    fields.push(Frame::Integer(master.sentinels.len() as i64));

    fields.push(Frame::Bulk("quorum".into()));
    fields.push(Frame::Integer(config.quorum as i64));

    fields.push(Frame::Bulk("failover-timeout".into()));
    fields.push(Frame::Integer(config.failover_timeout_ms as i64));

    fields.push(Frame::Bulk("parallel-syncs".into()));
    fields.push(Frame::Integer(config.parallel_syncs as i64));

    Frame::Array(fields)
}

fn handle_replicas(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if let Some(master) = state.get_master(name) {
        let mut result = Vec::new();
        for entry in master.replicas.iter() {
            let replica = entry.value();
            result.push(replica_to_frame(replica, name));
        }
        Ok(Frame::Array(result))
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn replica_to_frame(replica: &ReplicaInfo, master_name: &str) -> Frame {
    let mut fields = Vec::new();

    fields.push(Frame::Bulk("name".into()));
    fields.push(Frame::Bulk(
        format!("{}:{}", replica.addr.ip(), replica.addr.port()).into(),
    ));

    fields.push(Frame::Bulk("ip".into()));
    fields.push(Frame::Bulk(replica.addr.ip().to_string().into()));

    fields.push(Frame::Bulk("port".into()));
    fields.push(Frame::Integer(replica.addr.port() as i64));

    fields.push(Frame::Bulk("runid".into()));
    fields.push(Frame::Bulk(replica.id.clone().into()));

    fields.push(Frame::Bulk("flags".into()));
    fields.push(Frame::Bulk(format!("slave,{}", replica.flags).into()));

    fields.push(Frame::Bulk("link-pending-commands".into()));
    fields.push(Frame::Integer(0));

    fields.push(Frame::Bulk("link-refcount".into()));
    fields.push(Frame::Integer(1));

    fields.push(Frame::Bulk("last-ping-sent".into()));
    fields.push(Frame::Integer(0));

    fields.push(Frame::Bulk("last-ok-ping-reply".into()));
    fields.push(Frame::Integer(replica.last_ping_ms as i64));

    fields.push(Frame::Bulk("last-ping-reply".into()));
    fields.push(Frame::Integer(replica.last_ping_ms as i64));

    fields.push(Frame::Bulk("master-link-down-time".into()));
    fields.push(Frame::Integer(0));

    fields.push(Frame::Bulk("master-link-status".into()));
    fields.push(Frame::Bulk(replica.master_link_status.clone().into()));

    fields.push(Frame::Bulk("master-host".into()));
    fields.push(Frame::Bulk(master_name.to_string().into()));

    fields.push(Frame::Bulk("master-port".into()));
    fields.push(Frame::Integer(0)); // Would need master addr

    fields.push(Frame::Bulk("slave-priority".into()));
    fields.push(Frame::Integer(100));

    fields.push(Frame::Bulk("slave-repl-offset".into()));
    fields.push(Frame::Integer(replica.offset as i64));

    Frame::Array(fields)
}

fn handle_sentinels(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if let Some(master) = state.get_master(name) {
        let mut result = Vec::new();
        for entry in master.sentinels.iter() {
            let sentinel = entry.value();
            result.push(sentinel_to_frame(sentinel));
        }
        Ok(Frame::Array(result))
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn sentinel_to_frame(sentinel: &SentinelInfo) -> Frame {
    let mut fields = Vec::new();

    fields.push(Frame::Bulk("name".into()));
    fields.push(Frame::Bulk(
        format!("{}:{}", sentinel.addr.ip(), sentinel.addr.port()).into(),
    ));

    fields.push(Frame::Bulk("ip".into()));
    fields.push(Frame::Bulk(sentinel.addr.ip().to_string().into()));

    fields.push(Frame::Bulk("port".into()));
    fields.push(Frame::Integer(sentinel.addr.port() as i64));

    fields.push(Frame::Bulk("runid".into()));
    fields.push(Frame::Bulk(sentinel.id.clone().into()));

    fields.push(Frame::Bulk("flags".into()));
    let flags = if sentinel.is_reachable {
        "sentinel"
    } else {
        "sentinel,s_down"
    };
    fields.push(Frame::Bulk(flags.into()));

    fields.push(Frame::Bulk("link-pending-commands".into()));
    fields.push(Frame::Integer(0));

    fields.push(Frame::Bulk("link-refcount".into()));
    fields.push(Frame::Integer(1));

    fields.push(Frame::Bulk("last-hello-message".into()));
    fields.push(Frame::Integer(sentinel.last_hello_ms as i64));

    fields.push(Frame::Bulk("voted-leader".into()));
    fields.push(Frame::Bulk(
        sentinel
            .voted_leader
            .clone()
            .unwrap_or_else(|| "?".into())
            .into(),
    ));

    fields.push(Frame::Bulk("voted-leader-epoch".into()));
    fields.push(Frame::Integer(sentinel.voted_leader_epoch as i64));

    Frame::Array(fields)
}

fn handle_get_master_addr(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if let Some(master) = state.get_master(name) {
        let config = master.config.read();
        Ok(Frame::Array(vec![
            Frame::Bulk(config.addr.ip().to_string().into()),
            Frame::Bulk(config.addr.port().to_string().into()),
        ]))
    } else {
        Ok(Frame::Null)
    }
}

fn handle_ckquorum(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if let Some(master) = state.get_master(name) {
        let config = master.config.read();
        let num_sentinels = master.sentinels.len() as u32 + 1; // +1 for self

        if num_sentinels >= config.quorum {
            Ok(Frame::Simple(format!(
                "OK {} usable Sentinels. Quorum and failover authorization is possible",
                num_sentinels
            )))
        } else {
            Ok(Frame::Error(format!(
                "NOQUORUM Not enough Sentinels ({}) to reach the quorum ({})",
                num_sentinels, config.quorum
            )))
        }
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn handle_failover(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if let Some(master) = state.get_master(name) {
        // Set state to ODOWN to trigger failover in the SentinelMonitor
        // The monitor will handle leader election and execute failover
        *master.state.write() = MasterState::ODown;
        let epoch = state.bump_epoch();
        master.failover_epoch.store(epoch, Ordering::SeqCst);

        Ok(Frame::Simple("OK".into()))
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn handle_flushconfig(_state: &SentinelState) -> Result<Frame> {
    // Configuration is stored in memory - flush to config file if configured
    // Currently, sentinel configuration is maintained in memory only
    // A full implementation would write to sentinel.conf
    Ok(Frame::Simple("OK".into()))
}

fn handle_monitor(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(5)?;
    let name = cmd.get_str(1)?;
    let ip = cmd.get_str(2)?;
    let port: u16 = cmd.get_u64(3)? as u16;
    let quorum: u32 = cmd.get_u64(4)? as u32;

    let addr: SocketAddr = format!("{}:{}", ip, port)
        .parse()
        .map_err(crate::error::Error::AddrParse)?;

    let mut config = MasterConfig::new(name.to_string(), addr);
    config.quorum = quorum;

    state.monitor(config);
    state.enable();

    Ok(Frame::Simple("OK".into()))
}

fn handle_remove(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if state.remove(name) {
        Ok(Frame::Simple("OK".into()))
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn handle_set(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(4)?;
    let name = cmd.get_str(1)?;
    let option = cmd.get_str(2)?.to_lowercase();
    let value = cmd.get_str(3)?;

    if let Some(master) = state.get_master(name) {
        let mut config = master.config.write();

        match option.as_str() {
            "down-after-milliseconds" => {
                config.down_after_ms = value.parse().unwrap_or(DEFAULT_DOWN_AFTER_MS);
            }
            "failover-timeout" => {
                config.failover_timeout_ms = value.parse().unwrap_or(DEFAULT_FAILOVER_TIMEOUT_MS);
            }
            "parallel-syncs" => {
                config.parallel_syncs = value.parse().unwrap_or(DEFAULT_PARALLEL_SYNCS);
            }
            "quorum" => {
                config.quorum = value.parse().unwrap_or(DEFAULT_QUORUM);
            }
            "auth-pass" => {
                config.auth_pass = Some(value.to_string());
            }
            "auth-user" => {
                config.auth_user = Some(value.to_string());
            }
            _ => {
                return Ok(Frame::Error(format!("ERR Unknown option '{}'", option)));
            }
        }

        Ok(Frame::Simple("OK".into()))
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn handle_reset(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let pattern = cmd.get_str(1)?;

    let mut count = 0;
    let names: Vec<String> = state
        .masters
        .iter()
        .filter(|e| matches_pattern(e.key(), pattern))
        .map(|e| e.key().clone())
        .collect();

    for name in names {
        if let Some(master) = state.get_master(&name) {
            // Reset master state
            *master.state.write() = MasterState::Up;
            master.replicas.clear();
            master.sentinels.clear();
            master.sdown_votes.store(0, Ordering::Relaxed);
            count += 1;
        }
    }

    Ok(Frame::Integer(count))
}

fn matches_pattern(name: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    // Simple glob matching
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return name == pattern;
    }

    let mut remaining = name;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !remaining.starts_with(part) {
                return false;
            }
            remaining = &remaining[part.len()..];
        } else if i == parts.len() - 1 {
            if !remaining.ends_with(part) {
                return false;
            }
        } else if let Some(pos) = remaining.find(part) {
            remaining = &remaining[pos + part.len()..];
        } else {
            return false;
        }
    }
    true
}

fn handle_pending_scripts() -> Result<Frame> {
    // Return empty array - no pending scripts
    Ok(Frame::Array(vec![]))
}

fn handle_info_cache(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let name = cmd.get_str(1)?;

    if let Some(_master) = state.get_master(name) {
        // Return cached INFO - simplified version
        Ok(Frame::Array(vec![
            Frame::Bulk(name.to_string().into()),
            Frame::Array(vec![
                Frame::Integer(current_time_ms() as i64),
                Frame::Bulk("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n".into()),
            ]),
        ]))
    } else {
        Ok(Frame::Error(format!(
            "ERR No such master with that name: {}",
            name
        )))
    }
}

fn handle_simulate_failure(cmd: &ParsedCommand, _state: &SentinelState) -> Result<Frame> {
    cmd.require_args(2)?;
    let failure_type = cmd.get_str(1)?.to_uppercase();

    match failure_type.as_str() {
        "CRASH-AFTER-ELECTION" | "CRASH-AFTER-PROMOTION" => Ok(Frame::Simple("OK".into())),
        _ => Ok(Frame::Error(format!(
            "ERR Unknown failure type: {}",
            failure_type
        ))),
    }
}

fn handle_debug(cmd: &ParsedCommand, _state: &SentinelState) -> Result<Frame> {
    if cmd.arg_count() < 2 {
        return Ok(Frame::Error(
            "ERR wrong number of arguments for 'sentinel debug' command".into(),
        ));
    }

    let subcommand = cmd.get_str(1)?.to_uppercase();

    match subcommand.as_str() {
        "PING" => Ok(Frame::Simple("PONG".into())),
        _ => Ok(Frame::Error(format!(
            "ERR Unknown debug subcommand: {}",
            subcommand
        ))),
    }
}

fn handle_config(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    if cmd.arg_count() < 2 {
        return Ok(Frame::Error(
            "ERR wrong number of arguments for 'sentinel config' command".into(),
        ));
    }

    let subcommand = cmd.get_str(1)?.to_uppercase();

    match subcommand.as_str() {
        "GET" => {
            cmd.require_args(3)?;
            let param = cmd.get_str(2)?.to_lowercase();

            match param.as_str() {
                "resolve-hostnames" => Ok(Frame::Array(vec![
                    Frame::Bulk("resolve-hostnames".into()),
                    Frame::Bulk("no".into()),
                ])),
                "announce-hostnames" => Ok(Frame::Array(vec![
                    Frame::Bulk("announce-hostnames".into()),
                    Frame::Bulk("no".into()),
                ])),
                _ => Ok(Frame::Array(vec![])),
            }
        }
        "SET" => {
            cmd.require_args(4)?;
            let param = cmd.get_str(2)?.to_lowercase();
            let value = cmd.get_str(3)?;

            match param.as_str() {
                "announce-ip" => {
                    state.set_announce(Some(value.to_string()), None);
                    Ok(Frame::Simple("OK".into()))
                }
                "announce-port" => {
                    let port: u16 = value.parse().unwrap_or(0);
                    state.set_announce(None, Some(port));
                    Ok(Frame::Simple("OK".into()))
                }
                _ => Ok(Frame::Simple("OK".into())),
            }
        }
        _ => Ok(Frame::Error(format!(
            "ERR Unknown config subcommand: {}",
            subcommand
        ))),
    }
}

fn handle_is_master_down(cmd: &ParsedCommand, state: &SentinelState) -> Result<Frame> {
    cmd.require_args(6)?;
    let ip = cmd.get_str(1)?;
    let port: u16 = cmd.get_u64(2)? as u16;
    let _current_epoch = cmd.get_u64(3)?;
    let _runid = cmd.get_str(4)?;

    // Find the master by address
    let target_addr: SocketAddr = format!("{}:{}", ip, port)
        .parse()
        .map_err(crate::error::Error::AddrParse)?;

    let mut found_master = None;
    for entry in state.masters.iter() {
        let config = entry.value().config.read();
        if config.addr == target_addr {
            found_master = Some(entry.value().clone());
            break;
        }
    }

    if let Some(master) = found_master {
        let master_state = *master.state.read();
        let is_down = matches!(master_state, MasterState::SDown | MasterState::ODown);

        // Response: down_state, leader, leader_epoch
        Ok(Frame::Array(vec![
            Frame::Integer(if is_down { 1 } else { 0 }),
            Frame::Bulk(if is_down { state.my_id() } else { "*".into() }.into()),
            Frame::Integer(state.current_epoch() as i64),
        ]))
    } else {
        Ok(Frame::Array(vec![
            Frame::Integer(0),
            Frame::Bulk("*".into()),
            Frame::Integer(0),
        ]))
    }
}

// =============================================================================
// COMMAND REGISTRY WRAPPER
// =============================================================================

/// SENTINEL subcommand \[arguments\] - Registry-compatible wrapper
pub fn cmd_sentinel(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    handle_sentinel_command(&cmd, &client)
}
