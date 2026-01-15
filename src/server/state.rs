//! Client connection state.

use crate::types::{DbIndex, Key};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// Maximum failed AUTH attempts before lockout.
const MAX_AUTH_FAILURES: u32 = 10;
/// Lockout duration in seconds after MAX_AUTH_FAILURES.
const AUTH_LOCKOUT_SECONDS: u64 = 60;

/// A queued command for transaction execution.
#[derive(Debug, Clone)]
pub struct QueuedCommand {
    /// Command name
    pub name: String,
    /// Command arguments
    pub args: Vec<Bytes>,
}

/// Pub/Sub subscription info.
#[derive(Debug)]
pub struct PubSubState {
    /// Subscribed channels with their receivers
    pub channel_receivers: std::collections::HashMap<Bytes, broadcast::Receiver<super::pubsub::PubSubMessage>>,
    /// Subscribed patterns with their receivers
    pub pattern_receivers: std::collections::HashMap<Bytes, broadcast::Receiver<super::pubsub::PubSubMessage>>,
}

impl Default for PubSubState {
    fn default() -> Self {
        Self {
            channel_receivers: std::collections::HashMap::new(),
            pattern_receivers: std::collections::HashMap::new(),
        }
    }
}

impl PubSubState {
    /// Get all channels
    pub fn channels(&self) -> HashSet<Bytes> {
        self.channel_receivers.keys().cloned().collect()
    }

    /// Get all patterns
    pub fn patterns(&self) -> HashSet<Bytes> {
        self.pattern_receivers.keys().cloned().collect()
    }
}

/// Client connection state.
///
/// This struct holds per-connection state such as the selected database,
/// authentication status, transaction state, etc.
#[derive(Debug)]
pub struct ClientState {
    /// Connection ID
    id: u64,
    /// Selected database index
    db_index: AtomicU16,
    /// Client name
    name: RwLock<Option<String>>,
    /// Is authenticated
    authenticated: AtomicBool,
    /// Is in transaction (MULTI)
    in_transaction: AtomicBool,
    /// Transaction was aborted (WATCH key was modified)
    transaction_aborted: AtomicBool,
    /// Queued commands for transaction
    transaction_queue: RwLock<Vec<QueuedCommand>>,
    /// Watched keys for WATCH command
    watched_keys: RwLock<HashSet<Key>>,
    /// Connection is closed
    closed: AtomicBool,
    /// Is read-only (replica mode)
    readonly: AtomicBool,
    /// Pub/Sub state
    pubsub: RwLock<PubSubState>,
    /// Is in pub/sub mode
    in_pubsub_mode: AtomicBool,
    /// ASKING flag for cluster ASK redirections
    asking: AtomicBool,
    /// Failed AUTH attempts count
    auth_failures: AtomicU32,
    /// Timestamp of last failed AUTH (seconds since UNIX epoch)
    auth_lockout_until: AtomicU64,
}

impl ClientState {
    /// Create a new client state.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            db_index: AtomicU16::new(0),
            name: RwLock::new(None),
            authenticated: AtomicBool::new(false),
            in_transaction: AtomicBool::new(false),
            transaction_aborted: AtomicBool::new(false),
            transaction_queue: RwLock::new(Vec::new()),
            watched_keys: RwLock::new(HashSet::new()),
            closed: AtomicBool::new(false),
            readonly: AtomicBool::new(false),
            pubsub: RwLock::new(PubSubState::default()),
            in_pubsub_mode: AtomicBool::new(false),
            asking: AtomicBool::new(false),
            auth_failures: AtomicU32::new(0),
            auth_lockout_until: AtomicU64::new(0),
        }
    }

    /// Get the connection ID.
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the current database index.
    #[inline]
    pub fn db_index(&self) -> DbIndex {
        self.db_index.load(Ordering::Relaxed)
    }

    /// Set the current database index.
    #[inline]
    pub fn set_db_index(&self, index: DbIndex) {
        self.db_index.store(index, Ordering::Relaxed);
    }

    /// Get the client name.
    pub fn name(&self) -> Option<String> {
        self.name.read().clone()
    }

    /// Set the client name.
    pub fn set_name(&self, name: String) {
        *self.name.write() = Some(name);
    }

    /// Check if the client is authenticated.
    #[inline]
    pub fn is_authenticated(&self) -> bool {
        self.authenticated.load(Ordering::Relaxed)
    }

    /// Set the authentication status.
    #[inline]
    pub fn set_authenticated(&self, authenticated: bool) {
        self.authenticated.store(authenticated, Ordering::Relaxed);
    }

    /// Check if the client is in a transaction.
    #[inline]
    pub fn is_in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Relaxed)
    }

    /// Set the transaction state.
    #[inline]
    pub fn set_in_transaction(&self, in_transaction: bool) {
        self.in_transaction.store(in_transaction, Ordering::Relaxed);
    }

    /// Check if the connection is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Close the connection.
    #[inline]
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }

    /// Check if the connection is read-only.
    #[inline]
    pub fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::Relaxed)
    }

    /// Set the read-only state.
    #[inline]
    pub fn set_readonly(&self, readonly: bool) {
        self.readonly.store(readonly, Ordering::Relaxed);
    }

    /// Check if ASKING flag is set (for cluster ASK redirections).
    #[inline]
    pub fn is_asking(&self) -> bool {
        self.asking.load(Ordering::Relaxed)
    }

    /// Set the ASKING flag.
    #[inline]
    pub fn set_asking(&self) {
        self.asking.store(true, Ordering::Relaxed);
    }

    /// Clear the ASKING flag (cleared after each command).
    #[inline]
    pub fn clear_asking(&self) {
        self.asking.store(false, Ordering::Relaxed);
    }

    // Transaction methods

    /// Start a transaction (MULTI).
    pub fn start_transaction(&self) {
        self.in_transaction.store(true, Ordering::Relaxed);
        self.transaction_aborted.store(false, Ordering::Relaxed);
        self.transaction_queue.write().clear();
    }

    /// Queue a command for the transaction.
    pub fn queue_command(&self, name: String, args: Vec<Bytes>) {
        self.transaction_queue.write().push(QueuedCommand { name, args });
    }

    /// Get the number of queued commands.
    pub fn queued_count(&self) -> usize {
        self.transaction_queue.read().len()
    }

    /// Take all queued commands (for EXEC).
    pub fn take_queued_commands(&self) -> Vec<QueuedCommand> {
        std::mem::take(&mut *self.transaction_queue.write())
    }

    /// Discard the transaction.
    pub fn discard_transaction(&self) {
        self.in_transaction.store(false, Ordering::Relaxed);
        self.transaction_aborted.store(false, Ordering::Relaxed);
        self.transaction_queue.write().clear();
        self.watched_keys.write().clear();
    }

    /// Check if the transaction was aborted.
    pub fn is_transaction_aborted(&self) -> bool {
        self.transaction_aborted.load(Ordering::Relaxed)
    }

    /// Abort the transaction (called when a watched key is modified).
    pub fn abort_transaction(&self) {
        self.transaction_aborted.store(true, Ordering::Relaxed);
    }

    /// Add a key to watch (WATCH).
    pub fn watch_key(&self, key: Key) {
        self.watched_keys.write().insert(key);
    }

    /// Get watched keys.
    pub fn get_watched_keys(&self) -> HashSet<Key> {
        self.watched_keys.read().clone()
    }

    /// Clear watched keys (UNWATCH or after EXEC/DISCARD).
    pub fn unwatch_all(&self) {
        self.watched_keys.write().clear();
    }

    /// Check if a key is being watched.
    pub fn is_watching(&self, key: &Key) -> bool {
        self.watched_keys.read().contains(key)
    }

    /// Check if any keys are being watched.
    pub fn has_watched_keys(&self) -> bool {
        !self.watched_keys.read().is_empty()
    }

    // Pub/Sub methods

    /// Check if the client is in pub/sub mode.
    #[inline]
    pub fn is_in_pubsub_mode(&self) -> bool {
        self.in_pubsub_mode.load(Ordering::Relaxed)
    }

    /// Subscribe to a channel.
    pub fn subscribe_channel(&self, channel: Bytes, receiver: broadcast::Receiver<super::pubsub::PubSubMessage>) {
        let mut state = self.pubsub.write();
        state.channel_receivers.insert(channel, receiver);
        self.in_pubsub_mode.store(true, Ordering::Relaxed);
    }

    /// Subscribe to a pattern.
    pub fn subscribe_pattern(&self, pattern: Bytes, receiver: broadcast::Receiver<super::pubsub::PubSubMessage>) {
        let mut state = self.pubsub.write();
        state.pattern_receivers.insert(pattern, receiver);
        self.in_pubsub_mode.store(true, Ordering::Relaxed);
    }

    /// Unsubscribe from a channel.
    pub fn unsubscribe_channel(&self, channel: &Bytes) -> bool {
        let mut state = self.pubsub.write();
        let removed = state.channel_receivers.remove(channel).is_some();
        if state.channel_receivers.is_empty() && state.pattern_receivers.is_empty() {
            self.in_pubsub_mode.store(false, Ordering::Relaxed);
        }
        removed
    }

    /// Unsubscribe from a pattern.
    pub fn unsubscribe_pattern(&self, pattern: &Bytes) -> bool {
        let mut state = self.pubsub.write();
        let removed = state.pattern_receivers.remove(pattern).is_some();
        if state.channel_receivers.is_empty() && state.pattern_receivers.is_empty() {
            self.in_pubsub_mode.store(false, Ordering::Relaxed);
        }
        removed
    }

    /// Unsubscribe from all channels.
    pub fn unsubscribe_all_channels(&self) -> Vec<Bytes> {
        let mut state = self.pubsub.write();
        let channels: Vec<Bytes> = state.channel_receivers.keys().cloned().collect();
        state.channel_receivers.clear();
        if state.pattern_receivers.is_empty() {
            self.in_pubsub_mode.store(false, Ordering::Relaxed);
        }
        channels
    }

    /// Unsubscribe from all patterns.
    pub fn unsubscribe_all_patterns(&self) -> Vec<Bytes> {
        let mut state = self.pubsub.write();
        let patterns: Vec<Bytes> = state.pattern_receivers.keys().cloned().collect();
        state.pattern_receivers.clear();
        if state.channel_receivers.is_empty() {
            self.in_pubsub_mode.store(false, Ordering::Relaxed);
        }
        patterns
    }

    /// Get the count of subscribed channels.
    pub fn subscribed_channel_count(&self) -> usize {
        self.pubsub.read().channel_receivers.len()
    }

    /// Get the count of subscribed patterns.
    pub fn subscribed_pattern_count(&self) -> usize {
        self.pubsub.read().pattern_receivers.len()
    }

    /// Get total subscription count.
    pub fn total_subscription_count(&self) -> usize {
        let state = self.pubsub.read();
        state.channel_receivers.len() + state.pattern_receivers.len()
    }

    /// Get subscribed channels.
    pub fn subscribed_channels(&self) -> Vec<Bytes> {
        self.pubsub.read().channel_receivers.keys().cloned().collect()
    }

    /// Get subscribed patterns.
    pub fn subscribed_patterns(&self) -> Vec<Bytes> {
        self.pubsub.read().pattern_receivers.keys().cloned().collect()
    }

    /// Take the Pub/Sub state for async message delivery.
    /// This transfers ownership of receivers to the caller.
    pub fn take_pubsub_state(&self) -> PubSubState {
        std::mem::take(&mut *self.pubsub.write())
    }

    /// Restore Pub/Sub state after async message delivery.
    pub fn restore_pubsub_state(&self, state: PubSubState) {
        *self.pubsub.write() = state;
        let has_subs = {
            let s = self.pubsub.read();
            !s.channel_receivers.is_empty() || !s.pattern_receivers.is_empty()
        };
        self.in_pubsub_mode.store(has_subs, Ordering::Relaxed);
    }

    // AUTH rate limiting methods

    /// Get current timestamp in seconds.
    fn current_timestamp_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Check if AUTH is currently blocked due to too many failures.
    /// Returns Some(seconds_remaining) if blocked, None if allowed.
    pub fn auth_is_blocked(&self) -> Option<u64> {
        let lockout_until = self.auth_lockout_until.load(Ordering::Relaxed);
        if lockout_until == 0 {
            return None;
        }
        let now = Self::current_timestamp_secs();
        if now < lockout_until {
            Some(lockout_until - now)
        } else {
            // Lockout expired, reset
            self.auth_lockout_until.store(0, Ordering::Relaxed);
            self.auth_failures.store(0, Ordering::Relaxed);
            None
        }
    }

    /// Record a failed AUTH attempt.
    /// Returns true if the client is now locked out.
    pub fn auth_record_failure(&self) -> bool {
        let failures = self.auth_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= MAX_AUTH_FAILURES {
            let lockout_until = Self::current_timestamp_secs() + AUTH_LOCKOUT_SECONDS;
            self.auth_lockout_until.store(lockout_until, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Reset AUTH failure counter (called on successful auth).
    pub fn auth_reset_failures(&self) {
        self.auth_failures.store(0, Ordering::Relaxed);
        self.auth_lockout_until.store(0, Ordering::Relaxed);
    }

    /// Get the current failed AUTH attempt count.
    pub fn auth_failure_count(&self) -> u32 {
        self.auth_failures.load(Ordering::Relaxed)
    }
}
