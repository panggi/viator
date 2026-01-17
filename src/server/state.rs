//! Client connection state.

use super::metrics::ServerMetrics;
use crate::storage::SharedServerStats;
use crate::types::{DbIndex, Key};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
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
#[derive(Debug, Default)]
pub struct PubSubState {
    /// Subscribed channels with their receivers
    pub channel_receivers:
        std::collections::HashMap<Bytes, broadcast::Receiver<super::pubsub::PubSubMessage>>,
    /// Subscribed patterns with their receivers
    pub pattern_receivers:
        std::collections::HashMap<Bytes, broadcast::Receiver<super::pubsub::PubSubMessage>>,
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

/// Default output buffer soft limit (32 MB).
pub const DEFAULT_OUTPUT_BUFFER_SOFT_LIMIT: usize = 32 * 1024 * 1024;
/// Default output buffer hard limit (64 MB).
pub const DEFAULT_OUTPUT_BUFFER_HARD_LIMIT: usize = 64 * 1024 * 1024;
/// Default soft limit timeout in seconds.
pub const DEFAULT_OUTPUT_BUFFER_SOFT_SECONDS: u64 = 60;

/// Client tracking mode flags.
#[derive(Debug, Clone, Default)]
pub struct TrackingState {
    /// Is tracking enabled
    pub enabled: bool,
    /// Redirect invalidations to this client ID (-1 for no redirect, uses RESP3 push)
    pub redirect: i64,
    /// Broadcast mode - track all key modifications, not just keys this client reads
    pub bcast: bool,
    /// Prefixes to track in BCAST mode (empty means all keys)
    pub prefixes: Vec<Bytes>,
    /// Opt-in mode - only track keys after CLIENT CACHING YES
    pub optin: bool,
    /// Opt-out mode - track all keys unless CLIENT CACHING NO
    pub optout: bool,
    /// Don't send invalidations for keys modified by this client
    pub noloop: bool,
    /// For OPTIN mode: next read should be tracked
    pub caching_yes: bool,
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
    /// Server-wide statistics (shared with all clients)
    server_stats: SharedServerStats,
    /// Server metrics (latency, throughput, etc.)
    server_metrics: Arc<ServerMetrics>,
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

    // Memory tracking fields
    /// Current query (input) buffer size in bytes
    query_buffer_size: AtomicU64,
    /// Current output buffer size in bytes
    output_buffer_size: AtomicU64,
    /// Peak output buffer size (for monitoring)
    output_buffer_peak: AtomicU64,
    /// Timestamp when output buffer exceeded soft limit (0 if under limit)
    output_buffer_soft_exceeded_at: AtomicU64,
    /// Output buffer soft limit (bytes)
    output_buffer_soft_limit: AtomicU64,
    /// Output buffer hard limit (bytes)
    output_buffer_hard_limit: AtomicU64,
    /// Soft limit timeout in seconds
    output_buffer_soft_seconds: AtomicU64,

    // Client-side caching tracking
    /// Tracking state for client-side caching
    tracking: RwLock<TrackingState>,
    /// Pending invalidation keys to send to this client
    pending_invalidations: RwLock<Vec<Key>>,
}

impl ClientState {
    /// Create a new client state.
    pub fn new(
        id: u64,
        server_stats: SharedServerStats,
        server_metrics: Arc<ServerMetrics>,
    ) -> Self {
        Self {
            id,
            db_index: AtomicU16::new(0),
            server_stats,
            server_metrics,
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
            // Memory tracking
            query_buffer_size: AtomicU64::new(0),
            output_buffer_size: AtomicU64::new(0),
            output_buffer_peak: AtomicU64::new(0),
            output_buffer_soft_exceeded_at: AtomicU64::new(0),
            output_buffer_soft_limit: AtomicU64::new(DEFAULT_OUTPUT_BUFFER_SOFT_LIMIT as u64),
            output_buffer_hard_limit: AtomicU64::new(DEFAULT_OUTPUT_BUFFER_HARD_LIMIT as u64),
            output_buffer_soft_seconds: AtomicU64::new(DEFAULT_OUTPUT_BUFFER_SOFT_SECONDS),
            // Client-side caching tracking
            tracking: RwLock::new(TrackingState::default()),
            pending_invalidations: RwLock::new(Vec::new()),
        }
    }

    /// Get the server statistics.
    #[inline]
    pub fn server_stats(&self) -> &SharedServerStats {
        &self.server_stats
    }

    /// Get the server metrics.
    #[inline]
    pub fn server_metrics(&self) -> &Arc<ServerMetrics> {
        &self.server_metrics
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
        self.transaction_queue
            .write()
            .push(QueuedCommand { name, args });
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
    pub fn subscribe_channel(
        &self,
        channel: Bytes,
        receiver: broadcast::Receiver<super::pubsub::PubSubMessage>,
    ) {
        let mut state = self.pubsub.write();
        state.channel_receivers.insert(channel, receiver);
        self.in_pubsub_mode.store(true, Ordering::Relaxed);
    }

    /// Subscribe to a pattern.
    pub fn subscribe_pattern(
        &self,
        pattern: Bytes,
        receiver: broadcast::Receiver<super::pubsub::PubSubMessage>,
    ) {
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
        self.pubsub
            .read()
            .channel_receivers
            .keys()
            .cloned()
            .collect()
    }

    /// Get subscribed patterns.
    pub fn subscribed_patterns(&self) -> Vec<Bytes> {
        self.pubsub
            .read()
            .pattern_receivers
            .keys()
            .cloned()
            .collect()
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
            self.auth_lockout_until
                .store(lockout_until, Ordering::Relaxed);
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

    // Memory tracking methods

    /// Update query buffer size.
    #[inline]
    pub fn set_query_buffer_size(&self, size: u64) {
        self.query_buffer_size.store(size, Ordering::Relaxed);
    }

    /// Get current query buffer size.
    #[inline]
    pub fn query_buffer_size(&self) -> u64 {
        self.query_buffer_size.load(Ordering::Relaxed)
    }

    /// Update output buffer size and check limits.
    /// Returns `Err(reason)` if the client should be disconnected due to buffer limits.
    pub fn update_output_buffer(&self, size: u64) -> Result<(), &'static str> {
        self.output_buffer_size.store(size, Ordering::Relaxed);

        // Update peak
        let peak = self.output_buffer_peak.load(Ordering::Relaxed);
        if size > peak {
            self.output_buffer_peak.store(size, Ordering::Relaxed);
        }

        let hard_limit = self.output_buffer_hard_limit.load(Ordering::Relaxed);
        let soft_limit = self.output_buffer_soft_limit.load(Ordering::Relaxed);
        let soft_seconds = self.output_buffer_soft_seconds.load(Ordering::Relaxed);

        // Check hard limit
        if hard_limit > 0 && size >= hard_limit {
            return Err("output buffer hard limit exceeded");
        }

        // Check soft limit
        if soft_limit > 0 && size >= soft_limit {
            let now = Self::current_timestamp_secs();
            let exceeded_at = self.output_buffer_soft_exceeded_at.load(Ordering::Relaxed);

            if exceeded_at == 0 {
                // First time exceeding soft limit
                self.output_buffer_soft_exceeded_at
                    .store(now, Ordering::Relaxed);
            } else if now - exceeded_at >= soft_seconds {
                // Soft limit exceeded for too long
                return Err("output buffer soft limit exceeded for too long");
            }
        } else {
            // Under soft limit, reset timer
            self.output_buffer_soft_exceeded_at
                .store(0, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Get current output buffer size.
    #[inline]
    pub fn output_buffer_size(&self) -> u64 {
        self.output_buffer_size.load(Ordering::Relaxed)
    }

    /// Get peak output buffer size.
    #[inline]
    pub fn output_buffer_peak(&self) -> u64 {
        self.output_buffer_peak.load(Ordering::Relaxed)
    }

    /// Set output buffer limits.
    pub fn set_output_buffer_limits(&self, soft_limit: u64, hard_limit: u64, soft_seconds: u64) {
        self.output_buffer_soft_limit
            .store(soft_limit, Ordering::Relaxed);
        self.output_buffer_hard_limit
            .store(hard_limit, Ordering::Relaxed);
        self.output_buffer_soft_seconds
            .store(soft_seconds, Ordering::Relaxed);
    }

    /// Get total memory usage for this client.
    pub fn total_memory_usage(&self) -> u64 {
        let query = self.query_buffer_size.load(Ordering::Relaxed);
        let output = self.output_buffer_size.load(Ordering::Relaxed);

        // Base client state overhead (estimated)
        let base_overhead = 512u64;

        // Transaction queue memory
        let transaction_mem = {
            let queue = self.transaction_queue.read();
            queue
                .iter()
                .map(|cmd| {
                    cmd.name.len() as u64 + cmd.args.iter().map(|a| a.len() as u64).sum::<u64>()
                })
                .sum::<u64>()
        };

        // Watched keys memory
        let watched_mem = {
            let watched = self.watched_keys.read();
            watched.iter().map(|k| k.len() as u64).sum::<u64>()
        };

        base_overhead + query + output + transaction_mem + watched_mem
    }

    /// Get memory info for CLIENT LIST command.
    pub fn memory_info(&self) -> ClientMemoryInfo {
        ClientMemoryInfo {
            query_buffer_size: self.query_buffer_size.load(Ordering::Relaxed),
            output_buffer_size: self.output_buffer_size.load(Ordering::Relaxed),
            output_buffer_peak: self.output_buffer_peak.load(Ordering::Relaxed),
            total: self.total_memory_usage(),
        }
    }

    // Client-side caching tracking methods

    /// Enable tracking with the given options.
    pub fn enable_tracking(
        &self,
        redirect: i64,
        bcast: bool,
        prefixes: Vec<Bytes>,
        optin: bool,
        optout: bool,
        noloop: bool,
    ) {
        let mut tracking = self.tracking.write();
        tracking.enabled = true;
        tracking.redirect = redirect;
        tracking.bcast = bcast;
        tracking.prefixes = prefixes;
        tracking.optin = optin;
        tracking.optout = optout;
        tracking.noloop = noloop;
        tracking.caching_yes = false;
    }

    /// Disable tracking.
    pub fn disable_tracking(&self) {
        let mut tracking = self.tracking.write();
        tracking.enabled = false;
        tracking.redirect = -1;
        tracking.bcast = false;
        tracking.prefixes.clear();
        tracking.optin = false;
        tracking.optout = false;
        tracking.noloop = false;
        tracking.caching_yes = false;
    }

    /// Check if tracking is enabled.
    pub fn is_tracking_enabled(&self) -> bool {
        self.tracking.read().enabled
    }

    /// Get tracking info for CLIENT TRACKINGINFO command.
    pub fn tracking_info(&self) -> TrackingState {
        self.tracking.read().clone()
    }

    /// Set CLIENT CACHING YES (for OPTIN mode).
    pub fn set_caching_yes(&self) {
        let mut tracking = self.tracking.write();
        if tracking.optin {
            tracking.caching_yes = true;
        }
    }

    /// Set CLIENT CACHING NO (for OPTOUT mode).
    pub fn set_caching_no(&self) {
        let mut tracking = self.tracking.write();
        if tracking.optout {
            tracking.caching_yes = false;
        }
    }

    /// Check if this key should be tracked based on current tracking state.
    /// Returns true if the key should be added to the tracking table.
    pub fn should_track_key(&self, key: &Key) -> bool {
        let tracking = self.tracking.read();
        if !tracking.enabled {
            return false;
        }

        // In OPTIN mode, only track if caching_yes was set
        if tracking.optin && !tracking.caching_yes {
            return false;
        }

        // In OPTOUT mode with caching_no, don't track
        // (caching_yes=false in optout mode means don't track)
        if tracking.optout && !tracking.caching_yes {
            // Actually in optout, caching_no means we should NOT track next key
            // The default is to track, caching_no opts out of next
            // This is inverted: we track unless explicitly opted out
        }

        // Check BCAST mode prefixes
        if tracking.bcast && !tracking.prefixes.is_empty() {
            let key_bytes = key.as_bytes();
            return tracking
                .prefixes
                .iter()
                .any(|prefix| key_bytes.starts_with(prefix.as_ref()));
        }

        true
    }

    /// Clear caching_yes flag after a read (for OPTIN mode).
    pub fn clear_caching_yes(&self) {
        let mut tracking = self.tracking.write();
        tracking.caching_yes = false;
    }

    /// Check if invalidations should not be sent back to this client (NOLOOP).
    pub fn is_noloop(&self) -> bool {
        self.tracking.read().noloop
    }

    /// Get the redirect client ID (-1 if no redirect).
    pub fn tracking_redirect(&self) -> i64 {
        self.tracking.read().redirect
    }

    /// Add a key to the pending invalidations list.
    pub fn add_pending_invalidation(&self, key: Key) {
        self.pending_invalidations.write().push(key);
    }

    /// Take all pending invalidations (clears the list).
    pub fn take_pending_invalidations(&self) -> Vec<Key> {
        std::mem::take(&mut *self.pending_invalidations.write())
    }

    /// Check if there are pending invalidations.
    pub fn has_pending_invalidations(&self) -> bool {
        !self.pending_invalidations.read().is_empty()
    }
}

/// Memory information for a client.
#[derive(Debug, Clone)]
pub struct ClientMemoryInfo {
    /// Query (input) buffer size
    pub query_buffer_size: u64,
    /// Output buffer size
    pub output_buffer_size: u64,
    /// Peak output buffer size
    pub output_buffer_peak: u64,
    /// Total memory usage
    pub total: u64,
}
