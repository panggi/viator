//! Pub/Sub implementation for Redis.
//!
//! This module provides the publish/subscribe functionality that allows
//! clients to subscribe to channels and receive messages.
//!
//! ## Implementation Status
//!
//! - **PUBLISH**: Fully implemented - publishes messages to channels
//! - **PUBSUB CHANNELS/NUMSUB/NUMPAT**: Fully implemented - query subscription info
//! - **SUBSCRIBE/PSUBSCRIBE**: Infrastructure present but requires connection-level
//!   changes to support the async message delivery loop
//!
//! To fully implement SUBSCRIBE, the connection handler needs to be modified to:
//! 1. Track subscription state on the client
//! 2. Switch between command mode and pub/sub mode
//! 3. Listen for messages on subscribed channels and push them to the client

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Maximum number of pending messages per channel.
const CHANNEL_CAPACITY: usize = 1000;

/// A message published to a channel.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    /// The channel the message was published to
    pub channel: Bytes,
    /// The message content
    pub message: Bytes,
    /// Pattern that matched (for pattern subscriptions)
    pub pattern: Option<Bytes>,
}

/// Pub/Sub hub that manages all subscriptions and message routing.
#[derive(Debug)]
pub struct PubSubHub {
    /// Channel subscriptions: channel name -> broadcast sender
    channels: DashMap<Bytes, broadcast::Sender<PubSubMessage>>,
    /// Pattern subscriptions: pattern -> broadcast sender
    patterns: DashMap<Bytes, broadcast::Sender<PubSubMessage>>,
    /// Track subscribers per channel for NUMSUB
    channel_subscribers: DashMap<Bytes, usize>,
    /// Track pattern subscribers for NUMPAT
    pattern_count: RwLock<usize>,
}

impl PubSubHub {
    /// Create a new Pub/Sub hub.
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            patterns: DashMap::new(),
            channel_subscribers: DashMap::new(),
            pattern_count: RwLock::new(0),
        }
    }

    /// Subscribe to a channel. Returns a receiver for messages.
    pub fn subscribe(&self, channel: Bytes) -> broadcast::Receiver<PubSubMessage> {
        let entry = self.channels.entry(channel.clone()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            tx
        });

        // Increment subscriber count
        *self.channel_subscribers.entry(channel).or_insert(0) += 1;

        entry.subscribe()
    }

    /// Unsubscribe from a channel.
    pub fn unsubscribe(&self, channel: &Bytes) {
        if let Some(mut count) = self.channel_subscribers.get_mut(channel) {
            if *count > 0 {
                *count -= 1;
            }
            // Clean up if no subscribers
            if *count == 0 {
                drop(count);
                self.channel_subscribers.remove(channel);
                self.channels.remove(channel);
            }
        }
    }

    /// Subscribe to a pattern. Returns a receiver for messages.
    pub fn psubscribe(&self, pattern: Bytes) -> broadcast::Receiver<PubSubMessage> {
        let entry = self.patterns.entry(pattern).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            *self.pattern_count.write() += 1;
            tx
        });
        entry.subscribe()
    }

    /// Unsubscribe from a pattern.
    pub fn punsubscribe(&self, pattern: &Bytes) {
        if self.patterns.remove(pattern).is_some() {
            let mut count = self.pattern_count.write();
            if *count > 0 {
                *count -= 1;
            }
        }
    }

    /// Publish a message to a channel.
    /// Returns the number of subscribers that received the message.
    pub fn publish(&self, channel: Bytes, message: Bytes) -> usize {
        let mut count = 0;

        // Send to direct channel subscribers
        if let Some(sender) = self.channels.get(&channel) {
            let msg = PubSubMessage {
                channel: channel.clone(),
                message: message.clone(),
                pattern: None,
            };
            // send() returns the number of receivers (might be 0 if all dropped)
            if sender.send(msg).is_ok() {
                count += sender.receiver_count();
            }
        }

        // Send to pattern subscribers
        for entry in self.patterns.iter() {
            let pattern = entry.key();
            if Self::matches_pattern(pattern, &channel) {
                let msg = PubSubMessage {
                    channel: channel.clone(),
                    message: message.clone(),
                    pattern: Some(pattern.clone()),
                };
                if entry.value().send(msg).is_ok() {
                    count += entry.value().receiver_count();
                }
            }
        }

        count
    }

    /// Get the number of subscribers to a channel.
    pub fn numsub(&self, channel: &Bytes) -> usize {
        self.channel_subscribers.get(channel).map(|c| *c).unwrap_or(0)
    }

    /// Get the number of pattern subscriptions.
    pub fn numpat(&self) -> usize {
        *self.pattern_count.read()
    }

    /// Get all active channels (optionally filtered by pattern).
    pub fn channels(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        self.channel_subscribers
            .iter()
            .filter(|entry| {
                pattern.map(|p| Self::matches_pattern(p, entry.key())).unwrap_or(true)
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Check if a channel name matches a glob pattern.
    fn matches_pattern(pattern: &Bytes, channel: &Bytes) -> bool {
        // Simple glob matching: * matches any sequence, ? matches one char
        let pattern = pattern.as_ref();
        let channel = channel.as_ref();
        Self::glob_match(pattern, channel)
    }

    /// Glob pattern matching.
    fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
        let mut p = 0;
        let mut t = 0;
        let mut star_p = None;
        let mut star_t = 0;

        while t < text.len() {
            if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
                p += 1;
                t += 1;
            } else if p < pattern.len() && pattern[p] == b'*' {
                star_p = Some(p);
                star_t = t;
                p += 1;
            } else if let Some(sp) = star_p {
                p = sp + 1;
                star_t += 1;
                t = star_t;
            } else {
                return false;
            }
        }

        while p < pattern.len() && pattern[p] == b'*' {
            p += 1;
        }

        p == pattern.len()
    }
}

impl Default for PubSubHub {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared Pub/Sub hub type.
pub type SharedPubSubHub = Arc<PubSubHub>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(PubSubHub::glob_match(b"foo", b"foo"));
        assert!(!PubSubHub::glob_match(b"foo", b"bar"));
        assert!(PubSubHub::glob_match(b"foo*", b"foobar"));
        assert!(PubSubHub::glob_match(b"*bar", b"foobar"));
        assert!(PubSubHub::glob_match(b"f*r", b"foobar"));
        assert!(PubSubHub::glob_match(b"f?o", b"foo"));
        assert!(!PubSubHub::glob_match(b"f?o", b"fooo"));
        assert!(PubSubHub::glob_match(b"*", b"anything"));
        assert!(PubSubHub::glob_match(b"news.*", b"news.tech"));
    }

    #[tokio::test]
    async fn test_subscribe_publish() {
        let hub = PubSubHub::new();
        let channel = Bytes::from("test-channel");

        // Subscribe
        let mut rx = hub.subscribe(channel.clone());

        // Publish
        let count = hub.publish(channel.clone(), Bytes::from("hello"));
        assert_eq!(count, 1);

        // Receive
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.message, Bytes::from("hello"));
        assert_eq!(msg.channel, channel);
    }
}
