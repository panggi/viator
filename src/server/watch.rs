//! Global watch registry for cross-client WATCH invalidation.
//!
//! This module provides the infrastructure for implementing transactional
//! WATCH semantics where a client's transaction is aborted if any watched
//! key is modified by another client.
//!
//! ## How it works
//!
//! 1. When a client executes WATCH on a key, the key is registered in the global registry
//! 2. When any client modifies a key, the registry is checked
//! 3. All other clients watching that key have their transactions marked as aborted
//! 4. On EXEC, aborted transactions return null instead of executing

use crate::types::Key;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global watch registry for tracking watched keys across all clients.
pub struct WatchRegistry {
    /// Map of key -> set of client IDs watching that key
    watched_keys: DashMap<Key, HashSet<u64>>,
    /// Map of client ID -> set of keys being watched by that client
    client_watches: DashMap<u64, HashSet<Key>>,
    /// Callback registry for notifying clients when keys are modified
    /// Map of client ID -> callback function
    invalidation_callbacks: DashMap<u64, Arc<dyn Fn() + Send + Sync>>,
    /// Statistics
    stats: WatchStats,
}

impl std::fmt::Debug for WatchRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WatchRegistry")
            .field("watched_keys", &self.watched_keys.len())
            .field("client_watches", &self.client_watches.len())
            .field("invalidation_callbacks", &self.invalidation_callbacks.len())
            .field("stats", &self.stats)
            .finish()
    }
}

/// Statistics for watch operations.
#[derive(Debug, Default)]
pub struct WatchStats {
    /// Total WATCH commands executed
    pub watch_count: AtomicU64,
    /// Total keys currently being watched
    pub watched_keys_count: AtomicU64,
    /// Total invalidations triggered
    pub invalidation_count: AtomicU64,
    /// Total transactions aborted due to watch
    pub aborted_transactions: AtomicU64,
}

impl WatchRegistry {
    /// Create a new watch registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            watched_keys: DashMap::new(),
            client_watches: DashMap::new(),
            invalidation_callbacks: DashMap::new(),
            stats: WatchStats::default(),
        }
    }

    /// Register a client's callback for invalidation notifications.
    pub fn register_client(&self, client_id: u64, callback: Arc<dyn Fn() + Send + Sync>) {
        self.invalidation_callbacks.insert(client_id, callback);
    }

    /// Unregister a client (called when client disconnects).
    pub fn unregister_client(&self, client_id: u64) {
        // Remove from invalidation callbacks
        self.invalidation_callbacks.remove(&client_id);

        // Unwatch all keys for this client
        self.unwatch_all(client_id);
    }

    /// Add a WATCH on a key for a client.
    pub fn watch(&self, client_id: u64, key: Key) {
        // Add to key -> clients mapping
        self.watched_keys
            .entry(key.clone())
            .or_insert_with(HashSet::new)
            .insert(client_id);

        // Add to client -> keys mapping
        self.client_watches
            .entry(client_id)
            .or_insert_with(HashSet::new)
            .insert(key);

        self.stats.watch_count.fetch_add(1, Ordering::Relaxed);
        self.stats.watched_keys_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a WATCH on a key for a client.
    pub fn unwatch(&self, client_id: u64, key: &Key) {
        // Remove from key -> clients mapping
        if let Some(mut clients) = self.watched_keys.get_mut(key) {
            clients.remove(&client_id);
            if clients.is_empty() {
                drop(clients);
                self.watched_keys.remove(key);
            }
        }

        // Remove from client -> keys mapping
        if let Some(mut keys) = self.client_watches.get_mut(&client_id) {
            keys.remove(key);
            if keys.is_empty() {
                drop(keys);
                self.client_watches.remove(&client_id);
            }
        }

        self.stats.watched_keys_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Remove all WATCHes for a client.
    pub fn unwatch_all(&self, client_id: u64) {
        // Get all keys this client is watching
        if let Some((_, keys)) = self.client_watches.remove(&client_id) {
            let count = keys.len();

            // Remove client from each key's watcher set
            for key in keys {
                if let Some(mut clients) = self.watched_keys.get_mut(&key) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        drop(clients);
                        self.watched_keys.remove(&key);
                    }
                }
            }

            self.stats
                .watched_keys_count
                .fetch_sub(count as u64, Ordering::Relaxed);
        }
    }

    /// Notify watchers that a key has been modified.
    /// Returns the number of clients that were notified.
    pub fn notify_key_modified(&self, key: &Key, modifier_client_id: u64) -> usize {
        let mut notified = 0;

        if let Some(clients) = self.watched_keys.get(key) {
            for &client_id in clients.iter() {
                // Don't notify the client that made the modification
                if client_id == modifier_client_id {
                    continue;
                }

                // Call the invalidation callback
                if let Some(callback) = self.invalidation_callbacks.get(&client_id) {
                    callback();
                    notified += 1;
                    self.stats.invalidation_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if notified > 0 {
            self.stats
                .aborted_transactions
                .fetch_add(notified as u64, Ordering::Relaxed);
        }

        notified
    }

    /// Notify watchers for multiple keys (batch operation).
    pub fn notify_keys_modified(&self, keys: &[Key], modifier_client_id: u64) -> usize {
        let mut notified = 0;
        let mut notified_clients = HashSet::new();

        for key in keys {
            if let Some(clients) = self.watched_keys.get(key) {
                for &client_id in clients.iter() {
                    // Don't notify the client that made the modification
                    if client_id == modifier_client_id {
                        continue;
                    }

                    // Only notify each client once even if multiple keys are affected
                    if notified_clients.insert(client_id) {
                        if let Some(callback) = self.invalidation_callbacks.get(&client_id) {
                            callback();
                            notified += 1;
                            self.stats.invalidation_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }

        if notified > 0 {
            self.stats
                .aborted_transactions
                .fetch_add(notified as u64, Ordering::Relaxed);
        }

        notified
    }

    /// Check if a key is being watched by any client.
    #[must_use]
    pub fn is_key_watched(&self, key: &Key) -> bool {
        self.watched_keys.contains_key(key)
    }

    /// Get the number of clients watching a key.
    #[must_use]
    pub fn watcher_count(&self, key: &Key) -> usize {
        self.watched_keys
            .get(key)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Get all keys being watched by a client.
    #[must_use]
    pub fn get_watched_keys(&self, client_id: u64) -> Vec<Key> {
        self.client_watches
            .get(&client_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get statistics.
    #[must_use]
    pub fn stats(&self) -> &WatchStats {
        &self.stats
    }

    /// Get total number of watched keys.
    #[must_use]
    pub fn total_watched_keys(&self) -> usize {
        self.watched_keys.len()
    }

    /// Get total number of clients with watches.
    #[must_use]
    pub fn total_watching_clients(&self) -> usize {
        self.client_watches.len()
    }
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared watch registry type.
pub type SharedWatchRegistry = Arc<WatchRegistry>;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::AtomicBool;

    fn make_key(s: &str) -> Key {
        Key::from(Bytes::from(s.to_string()))
    }

    #[test]
    fn test_watch_basic() {
        let registry = WatchRegistry::new();
        let key = make_key("test:key");

        registry.watch(1, key.clone());

        assert!(registry.is_key_watched(&key));
        assert_eq!(registry.watcher_count(&key), 1);
        assert_eq!(registry.get_watched_keys(1), vec![key.clone()]);
    }

    #[test]
    fn test_unwatch() {
        let registry = WatchRegistry::new();
        let key = make_key("test:key");

        registry.watch(1, key.clone());
        assert!(registry.is_key_watched(&key));

        registry.unwatch(1, &key);
        assert!(!registry.is_key_watched(&key));
    }

    #[test]
    fn test_unwatch_all() {
        let registry = WatchRegistry::new();
        let key1 = make_key("test:key1");
        let key2 = make_key("test:key2");

        registry.watch(1, key1.clone());
        registry.watch(1, key2.clone());

        assert_eq!(registry.get_watched_keys(1).len(), 2);

        registry.unwatch_all(1);

        assert!(!registry.is_key_watched(&key1));
        assert!(!registry.is_key_watched(&key2));
        assert!(registry.get_watched_keys(1).is_empty());
    }

    #[test]
    fn test_notify_key_modified() {
        let registry = WatchRegistry::new();
        let key = make_key("test:key");

        let notified = Arc::new(AtomicBool::new(false));
        let notified_clone = notified.clone();

        registry.register_client(1, Arc::new(move || {
            notified_clone.store(true, Ordering::SeqCst);
        }));

        registry.watch(1, key.clone());

        // Client 2 modifies the key
        let count = registry.notify_key_modified(&key, 2);

        assert_eq!(count, 1);
        assert!(notified.load(Ordering::SeqCst));
    }

    #[test]
    fn test_modifier_not_notified() {
        let registry = WatchRegistry::new();
        let key = make_key("test:key");

        let notified = Arc::new(AtomicBool::new(false));
        let notified_clone = notified.clone();

        registry.register_client(1, Arc::new(move || {
            notified_clone.store(true, Ordering::SeqCst);
        }));

        registry.watch(1, key.clone());

        // Client 1 modifies its own watched key - should not be notified
        let count = registry.notify_key_modified(&key, 1);

        assert_eq!(count, 0);
        assert!(!notified.load(Ordering::SeqCst));
    }

    #[test]
    fn test_multiple_watchers() {
        let registry = WatchRegistry::new();
        let key = make_key("test:key");

        let notified1 = Arc::new(AtomicBool::new(false));
        let notified2 = Arc::new(AtomicBool::new(false));

        let n1 = notified1.clone();
        let n2 = notified2.clone();

        registry.register_client(1, Arc::new(move || {
            n1.store(true, Ordering::SeqCst);
        }));
        registry.register_client(2, Arc::new(move || {
            n2.store(true, Ordering::SeqCst);
        }));

        registry.watch(1, key.clone());
        registry.watch(2, key.clone());

        // Client 3 modifies the key
        let count = registry.notify_key_modified(&key, 3);

        assert_eq!(count, 2);
        assert!(notified1.load(Ordering::SeqCst));
        assert!(notified2.load(Ordering::SeqCst));
    }
}
