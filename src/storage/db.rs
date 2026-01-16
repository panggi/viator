//! Main database implementation.
//!
//! The database provides thread-safe access to Redis data structures
//! with O(1) key lookup and efficient concurrent access.

use crate::error::{CommandError, Error, Result, StorageError};
use crate::server::config::MaxMemoryPolicy;
use crate::server::metrics::get_memory_usage;
use crate::server::pubsub::{PubSubHub, SharedPubSubHub};
use crate::types::search::SearchManager;
use crate::types::{
    DbIndex, Expiry, Key, MAX_DB_INDEX, StoredValue, Timestamp, ValueType, ViatorValue,
    current_timestamp_ms,
};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Shared server authentication configuration.
/// This is shared across all databases and accessible by commands.
#[derive(Debug, Default)]
pub struct ServerAuth {
    /// The required password for AUTH command (None = no auth required)
    requirepass: RwLock<Option<String>>,
}

/// Shared server statistics for INFO command.
/// These are server-wide stats that all databases can read.
#[derive(Debug, Default)]
pub struct ServerStats {
    /// Currently connected clients
    pub connected_clients: AtomicU64,
    /// Total connections since server start
    pub total_connections: AtomicU64,
    /// Last VDB save timestamp (Unix timestamp)
    pub last_vdb_save_time: AtomicU64,
    /// VDB background save in progress
    pub vdb_bgsave_in_progress: std::sync::atomic::AtomicBool,
    /// Changes since last VDB save
    pub vdb_changes_since_save: AtomicU64,
    /// AOF enabled
    pub aof_enabled: std::sync::atomic::AtomicBool,
    /// AOF rewrite in progress
    pub aof_rewrite_in_progress: std::sync::atomic::AtomicBool,
}

impl ServerStats {
    /// Create new server stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment connection count.
    pub fn connection_opened(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement connection count.
    pub fn connection_closed(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a data modification (for VDB change tracking).
    pub fn record_modification(&self) {
        self.vdb_changes_since_save.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark VDB save started.
    pub fn vdb_save_started(&self) {
        self.vdb_bgsave_in_progress.store(true, Ordering::Relaxed);
    }

    /// Mark VDB save completed.
    pub fn vdb_save_completed(&self) {
        self.vdb_bgsave_in_progress.store(false, Ordering::Relaxed);
        self.vdb_changes_since_save.store(0, Ordering::Relaxed);
        self.last_vdb_save_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }
}

/// Shared reference to server stats.
pub type SharedServerStats = Arc<ServerStats>;

impl ServerAuth {
    /// Create a new ServerAuth with optional password.
    pub fn new(requirepass: Option<String>) -> Self {
        Self {
            requirepass: RwLock::new(requirepass),
        }
    }

    /// Check if authentication is required.
    pub fn is_auth_required(&self) -> bool {
        self.requirepass.read().is_some()
    }

    /// Validate a password. Returns true if:
    /// - No password is required (requirepass is None), OR
    /// - The provided password matches the required password
    pub fn validate_password(&self, password: &str) -> bool {
        match self.requirepass.read().as_ref() {
            None => true, // No password required
            Some(required) => {
                // Use constant-time comparison to prevent timing attacks
                use subtle::ConstantTimeEq;
                password.as_bytes().ct_eq(required.as_bytes()).into()
            }
        }
    }

    /// Get the required password (for CONFIG GET).
    pub fn get_requirepass(&self) -> Option<String> {
        self.requirepass.read().clone()
    }

    /// Set the required password (for CONFIG SET).
    pub fn set_requirepass(&self, password: Option<String>) {
        *self.requirepass.write() = password;
    }
}

/// Shared server auth reference.
pub type SharedServerAuth = Arc<ServerAuth>;

/// Memory management configuration for enforcing maxmemory limits.
#[derive(Debug)]
pub struct MemoryManager {
    /// Maximum memory in bytes (0 = no limit)
    maxmemory: AtomicUsize,
    /// Eviction policy
    policy: RwLock<MaxMemoryPolicy>,
    /// Count of keys evicted
    evicted_keys: AtomicU64,
}

impl MemoryManager {
    /// Create a new memory manager.
    pub fn new(maxmemory: usize, policy: MaxMemoryPolicy) -> Self {
        Self {
            maxmemory: AtomicUsize::new(maxmemory),
            policy: RwLock::new(policy),
            evicted_keys: AtomicU64::new(0),
        }
    }

    /// Check if maxmemory limit is configured.
    pub fn is_limited(&self) -> bool {
        self.maxmemory.load(Ordering::Relaxed) > 0
    }

    /// Get the maxmemory limit.
    pub fn maxmemory(&self) -> usize {
        self.maxmemory.load(Ordering::Relaxed)
    }

    /// Set the maxmemory limit.
    pub fn set_maxmemory(&self, bytes: usize) {
        self.maxmemory.store(bytes, Ordering::Relaxed);
    }

    /// Get the eviction policy.
    pub fn policy(&self) -> MaxMemoryPolicy {
        *self.policy.read()
    }

    /// Set the eviction policy.
    pub fn set_policy(&self, policy: MaxMemoryPolicy) {
        *self.policy.write() = policy;
    }

    /// Get count of evicted keys.
    pub fn evicted_keys(&self) -> u64 {
        self.evicted_keys.load(Ordering::Relaxed)
    }

    /// Increment evicted key count.
    pub fn record_eviction(&self) {
        self.evicted_keys.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if memory usage is over limit.
    /// Returns Ok(()) if under limit, Err with OOM if over limit and noeviction.
    pub fn check_memory(&self) -> Result<MemoryStatus> {
        let limit = self.maxmemory.load(Ordering::Relaxed);
        if limit == 0 {
            return Ok(MemoryStatus::Ok);
        }

        let (_, rss) = get_memory_usage();
        if rss > limit {
            let policy = *self.policy.read();
            if policy == MaxMemoryPolicy::NoEviction {
                return Err(Error::Command(CommandError::OutOfMemory));
            }
            Ok(MemoryStatus::NeedEviction { used: rss, limit })
        } else {
            Ok(MemoryStatus::Ok)
        }
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::new(0, MaxMemoryPolicy::NoEviction)
    }
}

/// Memory status after checking limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryStatus {
    /// Memory usage is within limits.
    Ok,
    /// Memory is over limit, eviction needed.
    NeedEviction { used: usize, limit: usize },
}

/// Shared memory manager reference.
pub type SharedMemoryManager = Arc<MemoryManager>;

/// A single Redis database (one of 16).
///
/// # Thread Safety
///
/// The database uses `DashMap` for the main key-value store, which provides:
/// - Lock-free reads in most cases
/// - Fine-grained write locking (per-shard)
/// - Excellent concurrent performance
///
/// # Memory Management
///
/// - Keys and values use reference counting (`Bytes`, `Arc`)
/// - Expired keys are lazily deleted on access
/// - Background expiry task handles proactive cleanup
#[derive(Debug)]
pub struct Db {
    /// Main key-value store
    data: DashMap<Key, StoredValue>,

    /// Keys with expiration times, for efficient expiry scanning
    expires: DashMap<Key, Timestamp>,

    /// Last access time for each key (for LRU eviction).
    /// Uses millisecond timestamps for ordering.
    access_times: DashMap<Key, u64>,

    /// Statistics
    stats: DbStats,

    /// Pub/Sub hub reference (shared across all databases)
    pubsub: SharedPubSubHub,

    /// Full-text search manager
    search_manager: Arc<SearchManager>,
}

/// Database statistics.
#[derive(Debug, Default)]
pub struct DbStats {
    /// Total number of key lookups
    pub hits: AtomicU64,
    /// Number of lookups that missed (key not found)
    pub misses: AtomicU64,
    /// Number of expired keys removed
    pub expired_keys: AtomicU64,
    /// Number of keys evicted due to maxmemory
    pub evicted_keys: AtomicU64,
}

impl Db {
    /// Create a new empty database.
    pub fn new() -> Self {
        Self::with_pubsub(Arc::new(PubSubHub::new()))
    }

    /// Create a new database with a shared Pub/Sub hub.
    pub fn with_pubsub(pubsub: SharedPubSubHub) -> Self {
        Self {
            data: DashMap::new(),
            expires: DashMap::new(),
            access_times: DashMap::new(),
            stats: DbStats::default(),
            pubsub,
            search_manager: Arc::new(SearchManager::new()),
        }
    }

    /// Get the Pub/Sub hub.
    #[inline]
    pub fn pubsub(&self) -> &SharedPubSubHub {
        &self.pubsub
    }

    /// Get the search manager.
    #[inline]
    pub fn search_manager(&self) -> &Arc<SearchManager> {
        &self.search_manager
    }

    /// Get the number of keys in the database.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the database is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a value by key.
    ///
    /// Returns `None` if the key doesn't exist or has expired.
    pub fn get(&self, key: &Key) -> Option<ViatorValue> {
        // Check if key exists
        let entry = self.data.get(key)?;

        // Check expiration
        if entry.is_expired() {
            drop(entry);
            self.delete(key);
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Update LRU access time
        self.access_times
            .insert(key.clone(), current_timestamp_ms() as u64);

        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry.value.clone())
    }

    /// Get a value with type checking.
    pub fn get_typed(&self, key: &Key, expected: ValueType) -> Result<Option<ViatorValue>> {
        match self.get(key) {
            Some(value) => {
                if value.value_type() == expected {
                    Ok(Some(value))
                } else {
                    Err(Error::Command(CommandError::WrongType))
                }
            }
            None => Ok(None),
        }
    }

    /// Get a string value.
    pub fn get_string(&self, key: &Key) -> Result<Option<Bytes>> {
        match self.get(key) {
            Some(ViatorValue::String(s)) => Ok(Some(s)),
            Some(_) => Err(Error::Command(CommandError::WrongType)),
            None => Ok(None),
        }
    }

    /// Set a value.
    pub fn set(&self, key: Key, value: ViatorValue) {
        self.set_with_expiry(key, value, Expiry::Never);
    }

    /// Set a value with expiration.
    pub fn set_with_expiry(&self, key: Key, value: ViatorValue, expiry: Expiry) {
        // Update expires index
        match expiry {
            Expiry::Never => {
                self.expires.remove(&key);
            }
            Expiry::At(ts) => {
                self.expires.insert(key.clone(), ts);
            }
        }

        // Update LRU access time
        self.access_times
            .insert(key.clone(), current_timestamp_ms() as u64);

        self.data
            .insert(key, StoredValue::with_expiry(value, expiry));
    }

    /// Set only if key doesn't exist (SETNX).
    pub fn set_nx(&self, key: Key, value: ViatorValue) -> bool {
        self.set_nx_with_expiry(key, value, Expiry::Never)
    }

    /// Set only if key doesn't exist, with expiration.
    pub fn set_nx_with_expiry(&self, key: Key, value: ViatorValue, expiry: Expiry) -> bool {
        // Check if key exists and is not expired
        if let Some(entry) = self.data.get(&key) {
            if !entry.is_expired() {
                return false;
            }
        }

        self.set_with_expiry(key, value, expiry);
        true
    }

    /// Set only if key exists (SETXX).
    pub fn set_xx(&self, key: Key, value: ViatorValue) -> bool {
        self.set_xx_with_expiry(key, value, Expiry::Never)
    }

    /// Set only if key exists, with expiration.
    pub fn set_xx_with_expiry(&self, key: Key, value: ViatorValue, expiry: Expiry) -> bool {
        // Check if key exists and is not expired
        match self.data.get(&key) {
            Some(entry) if !entry.is_expired() => {
                drop(entry);
                self.set_with_expiry(key, value, expiry);
                true
            }
            _ => false,
        }
    }

    /// Delete a key.
    pub fn delete(&self, key: &Key) -> bool {
        self.expires.remove(key);
        self.access_times.remove(key);
        self.data.remove(key).is_some()
    }

    /// Delete multiple keys.
    pub fn delete_multi<'a>(&self, keys: impl IntoIterator<Item = &'a Key>) -> usize {
        keys.into_iter().filter(|k| self.delete(k)).count()
    }

    /// Check if a key exists (and is not expired).
    pub fn exists(&self, key: &Key) -> bool {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    drop(entry);
                    self.delete(key);
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }

    /// Count existing keys.
    pub fn exists_multi<'a>(&self, keys: impl IntoIterator<Item = &'a Key>) -> usize {
        keys.into_iter().filter(|k| self.exists(k)).count()
    }

    /// Get the type of a key.
    pub fn key_type(&self, key: &Key) -> Option<ValueType> {
        self.get(key).map(|v| v.value_type())
    }

    /// Rename a key.
    pub fn rename(&self, old_key: &Key, new_key: Key) -> Result<()> {
        // Get the old value
        let (_, stored) = self.data.remove(old_key).ok_or(CommandError::NoSuchKey)?;

        if stored.is_expired() {
            self.stats.expired_keys.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Command(CommandError::NoSuchKey));
        }

        // Move expiry
        if let Some((_, ts)) = self.expires.remove(old_key) {
            self.expires.insert(new_key.clone(), ts);
        }

        // Insert with new key
        self.data.insert(new_key, stored);
        Ok(())
    }

    /// Rename only if new key doesn't exist.
    pub fn rename_nx(&self, old_key: &Key, new_key: Key) -> Result<bool> {
        // Check if new key exists
        if self.exists(&new_key) {
            return Ok(false);
        }

        self.rename(old_key, new_key)?;
        Ok(true)
    }

    /// Copy a key to a new key.
    pub fn copy(&self, source: &Key, dest: Key, replace: bool) -> Result<bool> {
        // Get the source value
        let stored = match self.data.get(source) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(false);
                }
                entry.clone()
            }
            None => return Ok(false),
        };

        // Check if destination exists
        if !replace && self.exists(&dest) {
            return Ok(false);
        }

        // Copy expiry if present
        if let Some(ts) = self.expires.get(source).map(|r| *r) {
            self.expires.insert(dest.clone(), ts);
        }

        // Insert the cloned value at destination
        self.data.insert(dest, stored);
        Ok(true)
    }

    /// Set expiration on a key.
    pub fn expire(&self, key: &Key, expiry: Expiry) -> bool {
        self.expire_with_options(key, expiry, false, false, false, false)
    }

    /// Set expiration on a key with options (NX, XX, GT, LT).
    /// - NX: Set expiry only when the key has no expiry
    /// - XX: Set expiry only when the key has an existing expiry
    /// - GT: Set expiry only when the new expiry is greater than current one
    /// - LT: Set expiry only when the new expiry is less than current one
    pub fn expire_with_options(
        &self,
        key: &Key,
        expiry: Expiry,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }

            let current_expiry = &entry.expiry;
            let has_expiry = matches!(current_expiry, Expiry::At(_));

            // NX: Only set if key has no expiry
            if nx && has_expiry {
                return false;
            }

            // XX: Only set if key has an expiry
            if xx && !has_expiry {
                return false;
            }

            // GT/LT: Compare with current expiry
            if gt || lt {
                let new_ts = match expiry {
                    Expiry::At(ts) => ts,
                    Expiry::Never => return false, // GT/LT don't make sense with PERSIST
                };

                if let Expiry::At(current_ts) = current_expiry {
                    if gt && new_ts <= *current_ts {
                        return false;
                    }
                    if lt && new_ts >= *current_ts {
                        return false;
                    }
                } else {
                    // No current expiry - GT always fails, LT always succeeds
                    if gt {
                        return false;
                    }
                }
            }

            entry.set_expiry(expiry);

            match expiry {
                Expiry::Never => {
                    self.expires.remove(key);
                }
                Expiry::At(ts) => {
                    self.expires.insert(key.clone(), ts);
                }
            }

            true
        } else {
            false
        }
    }

    /// Remove expiration from a key (PERSIST).
    pub fn persist(&self, key: &Key) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }

            if matches!(entry.expiry, Expiry::At(_)) {
                entry.persist();
                self.expires.remove(key);
                return true;
            }
        }
        false
    }

    /// Get TTL in seconds.
    pub fn ttl(&self, key: &Key) -> Option<i64> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return Some(-2); // Key doesn't exist
        }
        entry.ttl().map(|t| t.max(0)).or(Some(-1)) // -1 for no expiry
    }

    /// Get TTL in milliseconds.
    pub fn pttl(&self, key: &Key) -> Option<i64> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return Some(-2);
        }
        entry.pttl().map(|t| t.max(0)).or(Some(-1))
    }

    /// Get expiration timestamp.
    pub fn expiretime(&self, key: &Key) -> Option<i64> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return Some(-2);
        }
        match entry.expiry {
            Expiry::At(ts) => Some(ts / 1000),
            Expiry::Never => Some(-1),
        }
    }

    /// Get all keys matching a pattern.
    pub fn keys(&self, pattern: &[u8]) -> Vec<Key> {
        let now = current_timestamp_ms();
        self.data
            .iter()
            .filter(|entry| {
                // Skip expired keys
                match entry.value().expiry {
                    Expiry::At(ts) if ts <= now => return false,
                    _ => {}
                }
                entry.key().matches_pattern(pattern)
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Scan keys with cursor.
    pub fn scan(
        &self,
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
        type_filter: Option<ValueType>,
    ) -> (usize, Vec<Key>) {
        let now = current_timestamp_ms();
        let count = count.max(10);

        let mut keys = Vec::new();
        let mut idx = 0;
        let mut next_cursor = 0;

        for entry in self.data.iter() {
            // Skip expired keys
            match entry.value().expiry {
                Expiry::At(ts) if ts <= now => continue,
                _ => {}
            }

            if idx >= cursor {
                // Apply filters
                let matches_pattern = pattern
                    .map(|p| entry.key().matches_pattern(p))
                    .unwrap_or(true);
                let matches_type = type_filter
                    .map(|t| entry.value().value.value_type() == t)
                    .unwrap_or(true);

                if matches_pattern && matches_type {
                    keys.push(entry.key().clone());

                    if keys.len() >= count {
                        next_cursor = idx + 1;
                        break;
                    }
                }
            }

            idx += 1;
        }

        // If we didn't fill the count, we've reached the end
        if keys.len() < count {
            next_cursor = 0;
        }

        (next_cursor, keys)
    }

    /// Get a random key.
    pub fn random_key(&self) -> Option<Key> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();

        let now = current_timestamp_ms();

        // Try a few times to find a non-expired key
        for _ in 0..5 {
            if let Some(entry) = self.data.iter().choose(&mut rng) {
                match entry.value().expiry {
                    Expiry::At(ts) if ts <= now => continue,
                    _ => return Some(entry.key().clone()),
                }
            }
        }

        None
    }

    /// Flush the database (delete all keys).
    pub fn flush(&self) {
        self.data.clear();
        self.expires.clear();
        self.access_times.clear();
    }

    /// Evict a random key from the database.
    /// Returns true if a key was evicted, false if database is empty.
    pub fn evict_random(&self) -> bool {
        // Get a random key by iterating to a random position
        let len = self.data.len();
        if len == 0 {
            return false;
        }

        // Pick a random index
        let mut rng = rand::thread_rng();
        let target = rng.gen_range(0..len);

        // Iterate to find the key at that position
        if let Some(key) = self.data.iter().nth(target).map(|r| r.key().clone()) {
            self.data.remove(&key);
            self.expires.remove(&key);
            self.access_times.remove(&key);
            self.stats.evicted_keys.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Evict a random volatile key (one with TTL) from the database.
    /// Returns true if a key was evicted, false if no volatile keys exist.
    pub fn evict_random_volatile(&self) -> bool {
        let len = self.expires.len();
        if len == 0 {
            return false;
        }

        let mut rng = rand::thread_rng();
        let target = rng.gen_range(0..len);

        if let Some(key) = self.expires.iter().nth(target).map(|r| r.key().clone()) {
            self.data.remove(&key);
            self.expires.remove(&key);
            self.access_times.remove(&key);
            self.stats.evicted_keys.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Evict the volatile key with the smallest TTL.
    /// Returns true if a key was evicted, false if no volatile keys exist.
    pub fn evict_volatile_ttl(&self) -> bool {
        // Find the key with the smallest expiry time (soonest to expire)
        let mut min_expiry_key: Option<Key> = None;
        let mut min_expiry: i64 = i64::MAX;

        for entry in self.expires.iter() {
            let expiry = *entry.value();
            if expiry < min_expiry {
                min_expiry = expiry;
                min_expiry_key = Some(entry.key().clone());
            }
        }

        if let Some(key) = min_expiry_key {
            self.data.remove(&key);
            self.expires.remove(&key);
            self.access_times.remove(&key);
            self.stats.evicted_keys.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Evict the least recently used key from all keys.
    /// Uses Redis-style approximation: samples 5 random keys and evicts the oldest.
    /// Returns true if a key was evicted, false if database is empty.
    pub fn evict_lru(&self) -> bool {
        const SAMPLE_SIZE: usize = 5;
        let len = self.data.len();
        if len == 0 {
            return false;
        }

        let mut rng = rand::thread_rng();
        let mut oldest_key: Option<Key> = None;
        let mut oldest_time: u64 = u64::MAX;

        // Sample random keys and find the least recently used
        for _ in 0..SAMPLE_SIZE.min(len) {
            let target = rng.gen_range(0..len);
            if let Some(entry) = self.data.iter().nth(target) {
                let key = entry.key();
                let access_time = self.access_times.get(key).map(|r| *r).unwrap_or(0);
                if access_time < oldest_time {
                    oldest_time = access_time;
                    oldest_key = Some(key.clone());
                }
            }
        }

        if let Some(key) = oldest_key {
            self.data.remove(&key);
            self.expires.remove(&key);
            self.access_times.remove(&key);
            self.stats.evicted_keys.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Evict the least recently used volatile key (one with TTL).
    /// Uses Redis-style approximation: samples 5 random volatile keys and evicts the oldest.
    /// Returns true if a key was evicted, false if no volatile keys exist.
    pub fn evict_lru_volatile(&self) -> bool {
        const SAMPLE_SIZE: usize = 5;
        let len = self.expires.len();
        if len == 0 {
            return false;
        }

        let mut rng = rand::thread_rng();
        let mut oldest_key: Option<Key> = None;
        let mut oldest_time: u64 = u64::MAX;

        // Sample random volatile keys and find the least recently used
        for _ in 0..SAMPLE_SIZE.min(len) {
            let target = rng.gen_range(0..len);
            if let Some(entry) = self.expires.iter().nth(target) {
                let key = entry.key();
                let access_time = self.access_times.get(key).map(|r| *r).unwrap_or(0);
                if access_time < oldest_time {
                    oldest_time = access_time;
                    oldest_key = Some(key.clone());
                }
            }
        }

        if let Some(key) = oldest_key {
            self.data.remove(&key);
            self.expires.remove(&key);
            self.access_times.remove(&key);
            self.stats.evicted_keys.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Get database statistics.
    pub fn stats(&self) -> &DbStats {
        &self.stats
    }

    /// Get all keys with expiration for persistence.
    pub fn get_expires(&self) -> Vec<(Key, Timestamp)> {
        self.expires
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    /// Process expired keys (called periodically).
    ///
    /// Returns the number of keys removed.
    pub fn expire_keys(&self, max_keys: usize) -> usize {
        let now = current_timestamp_ms();

        // First, collect expired keys (to avoid modifying map during iteration)
        let mut expired_keys = Vec::new();
        for entry in self.expires.iter() {
            if expired_keys.len() >= max_keys {
                break;
            }

            if *entry.value() <= now {
                expired_keys.push(entry.key().clone());
            }
        }

        // Now delete them (after iteration completes)
        let mut removed = 0;
        for key in expired_keys {
            if self.delete(&key) {
                removed += 1;
                self.stats.expired_keys.fetch_add(1, Ordering::Relaxed);
            }
        }

        removed
    }

    /// Get or create a value with a specific type.
    pub fn get_or_create<F>(&self, key: Key, create: F) -> Result<ViatorValue>
    where
        F: FnOnce() -> ViatorValue,
    {
        // Try to get existing value
        if let Some(entry) = self.data.get(&key) {
            if !entry.is_expired() {
                return Ok(entry.value.clone());
            }
            drop(entry);
        }

        // Create new value
        let value = create();
        self.set(key, value.clone());
        Ok(value)
    }

    /// Update a value in place if it exists.
    pub fn update<F, R>(&self, key: &Key, f: F) -> Option<R>
    where
        F: FnOnce(&mut StoredValue) -> R,
    {
        let mut entry = self.data.get_mut(key)?;

        if entry.is_expired() {
            return None;
        }

        Some(f(entry.value_mut()))
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

/// The main database manager, handling multiple databases (0-15).
#[derive(Debug)]
pub struct Database {
    /// Array of 16 databases
    dbs: [Arc<Db>; 16],
    /// Shared Pub/Sub hub
    pubsub: SharedPubSubHub,
    /// Shared server authentication
    server_auth: SharedServerAuth,
    /// Memory manager for maxmemory enforcement
    memory_manager: SharedMemoryManager,
    /// Server-wide statistics (connections, persistence, etc.)
    server_stats: SharedServerStats,
    /// Shutdown requested flag (set by SHUTDOWN command)
    shutdown_requested: std::sync::atomic::AtomicBool,
    /// Save on shutdown flag (for SHUTDOWN SAVE vs NOSAVE)
    save_on_shutdown: std::sync::atomic::AtomicBool,
}

impl Database {
    /// Create a new database manager.
    pub fn new() -> Self {
        Self::with_config(None, 0, MaxMemoryPolicy::NoEviction)
    }

    /// Create a new database manager with optional authentication.
    pub fn with_password(requirepass: Option<String>) -> Self {
        Self::with_config(requirepass, 0, MaxMemoryPolicy::NoEviction)
    }

    /// Create a new database manager with full configuration.
    pub fn with_config(
        requirepass: Option<String>,
        maxmemory: usize,
        maxmemory_policy: MaxMemoryPolicy,
    ) -> Self {
        let pubsub = Arc::new(PubSubHub::new());
        let server_auth = Arc::new(ServerAuth::new(requirepass));
        let memory_manager = Arc::new(MemoryManager::new(maxmemory, maxmemory_policy));
        let server_stats = Arc::new(ServerStats::new());
        Self {
            dbs: std::array::from_fn(|_| Arc::new(Db::with_pubsub(pubsub.clone()))),
            pubsub,
            server_auth,
            memory_manager,
            server_stats,
            shutdown_requested: std::sync::atomic::AtomicBool::new(false),
            save_on_shutdown: std::sync::atomic::AtomicBool::new(true), // Default: save before shutdown
        }
    }

    /// Get the Pub/Sub hub.
    #[inline]
    pub fn pubsub(&self) -> &SharedPubSubHub {
        &self.pubsub
    }

    /// Get the server authentication configuration.
    #[inline]
    pub fn server_auth(&self) -> &SharedServerAuth {
        &self.server_auth
    }

    /// Get the memory manager.
    #[inline]
    pub fn memory_manager(&self) -> &SharedMemoryManager {
        &self.memory_manager
    }

    /// Get the server statistics.
    #[inline]
    pub fn server_stats(&self) -> &SharedServerStats {
        &self.server_stats
    }

    /// Record a new connection (call when client connects).
    pub fn connection_opened(&self) {
        self.server_stats.connection_opened();
    }

    /// Record a connection close (call when client disconnects).
    pub fn connection_closed(&self) {
        self.server_stats.connection_closed();
    }

    /// Get the current connected client count.
    #[inline]
    pub fn connected_clients(&self) -> u64 {
        self.server_stats.connected_clients.load(Ordering::Relaxed)
    }

    /// Get the total connections since server start.
    #[inline]
    pub fn total_connections(&self) -> u64 {
        self.server_stats.total_connections.load(Ordering::Relaxed)
    }

    /// Request server shutdown (called by SHUTDOWN command).
    pub fn request_shutdown(&self, save: bool) {
        self.save_on_shutdown.store(save, Ordering::SeqCst);
        self.shutdown_requested.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was requested.
    #[inline]
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Check if we should save before shutdown.
    #[inline]
    pub fn should_save_on_shutdown(&self) -> bool {
        self.save_on_shutdown.load(Ordering::SeqCst)
    }

    /// Cancel a pending shutdown (SHUTDOWN ABORT).
    pub fn cancel_shutdown(&self) {
        self.shutdown_requested.store(false, Ordering::SeqCst);
    }

    /// Check memory and perform eviction if needed.
    /// Call this before write operations when maxmemory is configured.
    /// Returns Ok(()) if write can proceed, Err(OOM) if over limit with NoEviction.
    pub fn check_and_evict(&self, db_index: DbIndex) -> Result<()> {
        loop {
            match self.memory_manager.check_memory()? {
                MemoryStatus::Ok => return Ok(()),
                MemoryStatus::NeedEviction { .. } => {
                    // Perform eviction based on policy
                    let policy = self.memory_manager.policy();
                    let evicted = self.evict_one(db_index, policy);
                    if !evicted {
                        // No keys to evict, but still over limit
                        return Err(Error::Command(CommandError::OutOfMemory));
                    }
                    self.memory_manager.record_eviction();
                    // Loop to check if more eviction needed
                }
            }
        }
    }

    /// Evict one key based on policy.
    fn evict_one(&self, db_index: DbIndex, policy: MaxMemoryPolicy) -> bool {
        let db = &self.dbs[db_index as usize];

        match policy {
            MaxMemoryPolicy::NoEviction => false,
            MaxMemoryPolicy::AllKeysRandom => db.evict_random(),
            MaxMemoryPolicy::VolatileRandom => db.evict_random_volatile(),
            MaxMemoryPolicy::VolatileTtl => db.evict_volatile_ttl(),
            MaxMemoryPolicy::AllKeysLru => db.evict_lru(),
            MaxMemoryPolicy::VolatileLru => db.evict_lru_volatile(),
            // LFU requires frequency tracking - fall back to LRU for now
            MaxMemoryPolicy::AllKeysLfu => db.evict_lru(),
            MaxMemoryPolicy::VolatileLfu => db.evict_lru_volatile(),
        }
    }

    /// Get a specific database by index.
    pub fn get_db(&self, index: DbIndex) -> Result<Arc<Db>> {
        if index > MAX_DB_INDEX {
            return Err(Error::Storage(StorageError::DbIndexOutOfRange));
        }
        Ok(self.dbs[index as usize].clone())
    }

    /// Get the default database (index 0).
    #[inline]
    pub fn default_db(&self) -> Arc<Db> {
        self.dbs[0].clone()
    }

    /// Flush all databases.
    pub fn flush_all(&self) {
        for db in &self.dbs {
            db.flush();
        }
    }

    /// Get total key count across all databases.
    pub fn total_keys(&self) -> usize {
        self.dbs.iter().map(|db| db.len()).sum()
    }

    /// Run expiration on all databases.
    pub fn expire_all(&self, max_keys_per_db: usize) -> usize {
        self.dbs
            .iter()
            .map(|db| db.expire_keys(max_keys_per_db))
            .sum()
    }

    /// Get statistics for all databases.
    pub fn all_stats(&self) -> Vec<(DbIndex, usize, &DbStats)> {
        self.dbs
            .iter()
            .enumerate()
            .filter(|(_, db)| !db.is_empty())
            .map(|(i, db)| (i as DbIndex, db.len(), db.stats()))
            .collect()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

/// An entry exported from the database for persistence.
#[derive(Debug, Clone)]
pub struct DbEntry {
    /// Key bytes
    pub key: Bytes,
    /// Value type
    pub value_type: ValueType,
    /// Expiry setting
    pub expiry: Expiry,
    /// String value (if type is String)
    pub string_value: Option<Bytes>,
    /// List value (if type is List)
    pub list_value: Option<Vec<Bytes>>,
    /// Set value (if type is Set)
    pub set_value: Option<Vec<Bytes>>,
    /// Sorted set value (if type is ZSet)
    pub zset_value: Option<Vec<(Bytes, f64)>>,
    /// Hash value (if type is Hash)
    pub hash_value: Option<Vec<(Bytes, Bytes)>>,
    /// Stream value (if type is Stream)
    pub stream_value: Option<StreamExport>,
    /// VectorSet value (if type is VectorSet)
    /// Format: (metric, dim, elements: Vec<(name, vector, attrs)>)
    pub vectorset_value: Option<VectorSetExport>,
}

/// Exported VectorSet data for persistence.
#[derive(Debug, Clone)]
pub struct VectorSetExport {
    /// Distance metric (0=Cosine, 1=Euclidean, 2=InnerProduct)
    pub metric: u8,
    /// Vector dimensionality
    pub dim: usize,
    /// Elements: (name, vector, attributes)
    pub elements: Vec<(Bytes, Vec<f32>, Vec<(Bytes, Bytes)>)>,
}

/// Exported Stream data for persistence.
#[derive(Debug, Clone)]
pub struct StreamExport {
    /// Last generated ID (ms, seq) - critical for ID generation
    pub last_id: (u64, u64),
    /// Total entries ever added (including deleted)
    pub entries_added: u64,
    /// Entries: (id_ms, id_seq, fields)
    pub entries: Vec<(u64, u64, Vec<(Bytes, Bytes)>)>,
}

impl Database {
    /// Get all data from a specific database for persistence.
    ///
    /// Returns entries with their values exported for serialization.
    pub fn get_db_data(&self, db_idx: DbIndex) -> Vec<DbEntry> {
        if db_idx > MAX_DB_INDEX {
            return Vec::new();
        }

        let db = &self.dbs[db_idx as usize];
        let now = current_timestamp_ms();
        let mut entries = Vec::new();

        for item in db.data.iter() {
            let stored = item.value();

            // Skip expired keys
            if let Expiry::At(ts) = stored.expiry {
                if ts <= now {
                    continue;
                }
            }

            let key = Bytes::copy_from_slice(item.key().as_bytes());
            let value_type = stored.value.value_type();
            let expiry = stored.expiry;

            let mut entry = DbEntry {
                key,
                value_type,
                expiry,
                string_value: None,
                list_value: None,
                set_value: None,
                zset_value: None,
                hash_value: None,
                stream_value: None,
                vectorset_value: None,
            };

            match &stored.value {
                ViatorValue::String(s) => {
                    entry.string_value = Some(s.clone());
                }
                ViatorValue::List(list) => {
                    let guard = list.read();
                    // Get all items from the list by using range
                    entry.list_value = Some(guard.range(0, -1).into_iter().cloned().collect());
                }
                ViatorValue::Set(set) => {
                    let guard = set.read();
                    entry.set_value = Some(guard.members().into_iter().cloned().collect());
                }
                ViatorValue::ZSet(zset) => {
                    let guard = zset.read();
                    entry.zset_value =
                        Some(guard.iter().map(|e| (e.member.clone(), e.score)).collect());
                }
                ViatorValue::Hash(hash) => {
                    let guard = hash.read();
                    entry.hash_value =
                        Some(guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
                }
                ViatorValue::Stream(stream) => {
                    let guard = stream.read();
                    let last_id = guard.last_id();
                    let entries: Vec<_> = guard
                        .range(crate::types::StreamId::min(), crate::types::StreamId::max())
                        .into_iter()
                        .map(|e| (e.id.ms, e.id.seq, e.fields))
                        .collect();
                    entry.stream_value = Some(StreamExport {
                        last_id: (last_id.ms, last_id.seq),
                        entries_added: guard.entries_added(),
                        entries,
                    });
                }
                ViatorValue::VectorSet(vs) => {
                    let guard = vs.read();
                    let metric = match guard.metric() {
                        crate::types::vectorset::DistanceMetric::Cosine => 0,
                        crate::types::vectorset::DistanceMetric::Euclidean => 1,
                        crate::types::vectorset::DistanceMetric::InnerProduct => 2,
                    };
                    let dim = guard.dim().unwrap_or(0);
                    let elements: Vec<_> = guard
                        .members()
                        .into_iter()
                        .filter_map(|name| {
                            let elem = guard.get(name)?;
                            let attrs: Vec<(Bytes, Bytes)> = elem
                                .attributes
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            Some((name.clone(), elem.vector.clone(), attrs))
                        })
                        .collect();
                    entry.vectorset_value = Some(VectorSetExport {
                        metric,
                        dim,
                        elements,
                    });
                }
            }

            entries.push(entry);
        }

        entries
    }

    /// Set a string value with expiry (for VDB loading).
    pub fn set_string_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        value: Bytes,
        expiry: Expiry,
    ) {
        if db_idx > MAX_DB_INDEX {
            return;
        }
        let db = &self.dbs[db_idx as usize];
        db.set_with_expiry(Key::from(key), ViatorValue::string(value), expiry);
    }

    /// Set a list value with expiry (for VDB loading).
    pub fn set_list_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        items: Vec<Bytes>,
        expiry: Expiry,
    ) {
        if db_idx > MAX_DB_INDEX {
            return;
        }
        let db = &self.dbs[db_idx as usize];
        let list = ViatorValue::new_list();
        if let ViatorValue::List(l) = &list {
            let mut guard = l.write();
            for item in items {
                guard.push_back(item);
            }
        }
        db.set_with_expiry(Key::from(key), list, expiry);
    }

    /// Set a set value with expiry (for VDB loading).
    pub fn set_set_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        members: Vec<Bytes>,
        expiry: Expiry,
    ) {
        if db_idx > MAX_DB_INDEX {
            return;
        }
        let db = &self.dbs[db_idx as usize];
        let set = ViatorValue::new_set();
        if let ViatorValue::Set(s) = &set {
            let mut guard = s.write();
            for member in members {
                guard.add(member);
            }
        }
        db.set_with_expiry(Key::from(key), set, expiry);
    }

    /// Set a sorted set value with expiry (for VDB loading).
    pub fn set_zset_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        entries: Vec<(Bytes, f64)>,
        expiry: Expiry,
    ) {
        if db_idx > MAX_DB_INDEX {
            return;
        }
        let db = &self.dbs[db_idx as usize];
        let zset = ViatorValue::new_zset();
        if let ViatorValue::ZSet(z) = &zset {
            let mut guard = z.write();
            for (member, score) in entries {
                guard.add(member, score);
            }
        }
        db.set_with_expiry(Key::from(key), zset, expiry);
    }

    /// Set a hash value with expiry (for VDB loading).
    pub fn set_hash_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        fields: Vec<(Bytes, Bytes)>,
        expiry: Expiry,
    ) {
        if db_idx > MAX_DB_INDEX {
            return;
        }
        let db = &self.dbs[db_idx as usize];
        let hash = ViatorValue::new_hash();
        if let ViatorValue::Hash(h) = &hash {
            let mut guard = h.write();
            for (field, value) in fields {
                guard.insert(field, value);
            }
        }
        db.set_with_expiry(Key::from(key), hash, expiry);
    }

    /// Set a vector set value with expiry (for VDB loading).
    pub fn set_vectorset_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        export: VectorSetExport,
        expiry: Expiry,
    ) {
        use crate::types::vectorset::{DistanceMetric, VectorSet};

        if db_idx > MAX_DB_INDEX {
            return;
        }

        let metric = match export.metric {
            0 => DistanceMetric::Cosine,
            1 => DistanceMetric::Euclidean,
            _ => DistanceMetric::InnerProduct,
        };

        let mut vs = VectorSet::with_params(export.dim, metric);
        for (name, vector, attrs) in export.elements {
            // Add the element
            let _ = vs.add(name.clone(), vector);
            // Add attributes
            for (attr_key, attr_val) in attrs {
                vs.set_attr(&name, attr_key, attr_val);
            }
        }

        let db = &self.dbs[db_idx as usize];
        db.set_with_expiry(Key::from(key), ViatorValue::from_vectorset(vs), expiry);
    }

    /// Set a stream value with expiry (for VDB/AOF loading).
    pub fn set_stream_with_expiry(
        &self,
        db_idx: DbIndex,
        key: Bytes,
        export: StreamExport,
        expiry: Expiry,
    ) {
        use crate::types::ViatorStream;

        if db_idx > MAX_DB_INDEX {
            return;
        }

        let stream = ViatorStream::from_export(export);
        let db = &self.dbs[db_idx as usize];
        db.set_with_expiry(Key::from(key), ViatorValue::from_stream(stream), expiry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let db = Db::new();
        let key = Key::from("test");

        db.set(key.clone(), ViatorValue::string("value"));
        let result = db.get(&key);

        assert!(result.is_some());
        assert_eq!(result.unwrap().as_string(), Some(&Bytes::from("value")));
    }

    #[test]
    fn test_delete() {
        let db = Db::new();
        let key = Key::from("test");

        db.set(key.clone(), ViatorValue::string("value"));
        assert!(db.exists(&key));

        assert!(db.delete(&key));
        assert!(!db.exists(&key));
    }

    #[test]
    fn test_expiration() {
        let db = Db::new();
        let key = Key::from("test");

        // Set with past expiration
        db.set_with_expiry(key.clone(), ViatorValue::string("value"), Expiry::At(0));

        // Should not exist (expired)
        assert!(!db.exists(&key));
        assert!(db.get(&key).is_none());
    }

    #[test]
    fn test_set_nx() {
        let db = Db::new();
        let key = Key::from("test");

        // First set should succeed
        assert!(db.set_nx(key.clone(), ViatorValue::string("value1")));

        // Second set should fail
        assert!(!db.set_nx(key.clone(), ViatorValue::string("value2")));

        // Value should be original
        assert_eq!(
            db.get(&key).unwrap().as_string(),
            Some(&Bytes::from("value1"))
        );
    }

    #[test]
    fn test_rename() {
        let db = Db::new();
        let key1 = Key::from("key1");
        let key2 = Key::from("key2");

        db.set(key1.clone(), ViatorValue::string("value"));
        db.rename(&key1, key2.clone()).unwrap();

        assert!(!db.exists(&key1));
        assert!(db.exists(&key2));
    }

    #[test]
    fn test_ttl() {
        let db = Db::new();
        let key = Key::from("test");

        db.set_with_expiry(
            key.clone(),
            ViatorValue::string("value"),
            Expiry::from_seconds(100),
        );

        let ttl = db.ttl(&key).unwrap();
        assert!(ttl > 0 && ttl <= 100);
    }

    #[test]
    fn test_keys_pattern() {
        let db = Db::new();

        db.set(Key::from("foo"), ViatorValue::string("1"));
        db.set(Key::from("foobar"), ViatorValue::string("2"));
        db.set(Key::from("bar"), ViatorValue::string("3"));

        let keys = db.keys(b"foo*");
        assert_eq!(keys.len(), 2);

        let keys = db.keys(b"*");
        assert_eq!(keys.len(), 3);

        let keys = db.keys(b"baz*");
        assert!(keys.is_empty());
    }

    #[test]
    fn test_database_manager() {
        let database = Database::new();

        let db0 = database.get_db(0).unwrap();
        let db1 = database.get_db(1).unwrap();

        db0.set(Key::from("key"), ViatorValue::string("in db0"));
        db1.set(Key::from("key"), ViatorValue::string("in db1"));

        assert_eq!(
            db0.get(&Key::from("key")).unwrap().as_string(),
            Some(&Bytes::from("in db0"))
        );
        assert_eq!(
            db1.get(&Key::from("key")).unwrap().as_string(),
            Some(&Bytes::from("in db1"))
        );

        assert!(database.get_db(16).is_err());
    }
}
