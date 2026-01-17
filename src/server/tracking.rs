//! Client-side caching tracking registry.
//!
//! This module implements the server-side tracking for Redis client-side caching.
//! When a client enables tracking, the server tracks which keys the client reads.
//! When those keys are modified, the server sends invalidation messages to the client.

use crate::types::Key;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Shared tracking registry type alias.
pub type SharedTrackingRegistry = Arc<TrackingRegistry>;

/// Registry for tracking which clients are caching which keys.
///
/// This implements the server-side component of Redis client-side caching.
/// It maintains a mapping from keys to sets of client IDs that are tracking them.
#[derive(Debug, Default)]
pub struct TrackingRegistry {
    /// Map from key to set of client IDs tracking that key
    key_to_clients: DashMap<Key, HashSet<u64>>,
    /// Map from client ID to set of keys being tracked (for cleanup)
    client_to_keys: DashMap<u64, HashSet<Key>>,
    /// Broadcast mode clients (track all keys matching prefixes)
    bcast_clients: DashMap<u64, Vec<Bytes>>,
}

impl TrackingRegistry {
    /// Create a new tracking registry.
    pub fn new() -> Self {
        Self {
            key_to_clients: DashMap::new(),
            client_to_keys: DashMap::new(),
            bcast_clients: DashMap::new(),
        }
    }

    /// Register a client's interest in a key.
    /// Called when a client reads a key while tracking is enabled.
    pub fn track_key(&self, client_id: u64, key: Key) {
        // Add to key->clients map
        self.key_to_clients
            .entry(key.clone())
            .or_insert_with(HashSet::new)
            .insert(client_id);

        // Add to client->keys map for cleanup
        self.client_to_keys
            .entry(client_id)
            .or_insert_with(HashSet::new)
            .insert(key);
    }

    /// Get all client IDs that should be invalidated for a key.
    /// This includes both direct tracking and broadcast mode clients.
    pub fn get_tracking_clients(&self, key: &Key) -> Vec<u64> {
        let mut clients = Vec::new();

        // Direct tracking clients
        if let Some(entry) = self.key_to_clients.get(key) {
            clients.extend(entry.iter());
        }

        // Broadcast mode clients (check prefix matching)
        let key_bytes = key.as_bytes();
        for entry in self.bcast_clients.iter() {
            let client_id = *entry.key();
            let prefixes = entry.value();

            // Empty prefixes means track all keys
            if prefixes.is_empty() || prefixes.iter().any(|p| key_bytes.starts_with(p.as_ref())) {
                if !clients.contains(&client_id) {
                    clients.push(client_id);
                }
            }
        }

        clients
    }

    /// Remove a key from tracking (called after invalidation is sent).
    /// Returns the client IDs that were tracking this key.
    pub fn invalidate_key(&self, key: &Key) -> Vec<u64> {
        let clients = if let Some((_, client_set)) = self.key_to_clients.remove(key) {
            // Remove key from each client's tracking set
            for client_id in &client_set {
                if let Some(mut keys) = self.client_to_keys.get_mut(client_id) {
                    keys.remove(key);
                }
            }
            client_set.into_iter().collect()
        } else {
            Vec::new()
        };

        // Also check broadcast clients
        let mut result = clients;
        let key_bytes = key.as_bytes();
        for entry in self.bcast_clients.iter() {
            let client_id = *entry.key();
            let prefixes = entry.value();

            if prefixes.is_empty() || prefixes.iter().any(|p| key_bytes.starts_with(p.as_ref())) {
                if !result.contains(&client_id) {
                    result.push(client_id);
                }
            }
        }

        result
    }

    /// Register a broadcast mode client.
    /// Broadcast clients receive invalidations for all keys matching their prefixes.
    pub fn register_bcast_client(&self, client_id: u64, prefixes: Vec<Bytes>) {
        self.bcast_clients.insert(client_id, prefixes);
    }

    /// Unregister a broadcast mode client.
    pub fn unregister_bcast_client(&self, client_id: u64) {
        self.bcast_clients.remove(&client_id);
    }

    /// Remove all tracking for a client (called when client disconnects or disables tracking).
    pub fn remove_client(&self, client_id: u64) {
        // Remove from broadcast clients
        self.bcast_clients.remove(&client_id);

        // Remove from key tracking
        if let Some((_, keys)) = self.client_to_keys.remove(&client_id) {
            for key in keys {
                if let Some(mut entry) = self.key_to_clients.get_mut(&key) {
                    entry.remove(&client_id);
                    // Clean up empty entries
                    if entry.is_empty() {
                        drop(entry);
                        self.key_to_clients.remove(&key);
                    }
                }
            }
        }
    }

    /// Get statistics about tracking.
    pub fn stats(&self) -> TrackingStats {
        let tracked_keys = self.key_to_clients.len();
        let tracking_clients = self.client_to_keys.len();
        let bcast_clients = self.bcast_clients.len();

        TrackingStats {
            tracked_keys,
            tracking_clients,
            bcast_clients,
        }
    }

    /// Flush all tracking (called on FLUSHDB/FLUSHALL).
    pub fn flush_all(&self) {
        self.key_to_clients.clear();
        self.client_to_keys.clear();
        // Don't clear bcast_clients - they still need invalidations
    }
}

/// Statistics about tracking.
#[derive(Debug, Clone)]
pub struct TrackingStats {
    /// Number of keys being tracked
    pub tracked_keys: usize,
    /// Number of clients with direct key tracking
    pub tracking_clients: usize,
    /// Number of broadcast mode clients
    pub bcast_clients: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_key() {
        let registry = TrackingRegistry::new();
        let key = Key::from("test");

        registry.track_key(1, key.clone());
        registry.track_key(2, key.clone());

        let clients = registry.get_tracking_clients(&key);
        assert!(clients.contains(&1));
        assert!(clients.contains(&2));
    }

    #[test]
    fn test_invalidate_key() {
        let registry = TrackingRegistry::new();
        let key = Key::from("test");

        registry.track_key(1, key.clone());
        registry.track_key(2, key.clone());

        let clients = registry.invalidate_key(&key);
        assert_eq!(clients.len(), 2);

        // Key should no longer be tracked
        let clients = registry.get_tracking_clients(&key);
        assert!(clients.is_empty());
    }

    #[test]
    fn test_bcast_client() {
        let registry = TrackingRegistry::new();
        let key = Key::from("user:123");

        registry.register_bcast_client(1, vec![Bytes::from("user:")]);

        let clients = registry.get_tracking_clients(&key);
        assert!(clients.contains(&1));

        // Key not matching prefix
        let other_key = Key::from("product:456");
        let clients = registry.get_tracking_clients(&other_key);
        assert!(clients.is_empty());
    }

    #[test]
    fn test_remove_client() {
        let registry = TrackingRegistry::new();
        let key = Key::from("test");

        registry.track_key(1, key.clone());
        registry.remove_client(1);

        let clients = registry.get_tracking_clients(&key);
        assert!(clients.is_empty());
    }
}
