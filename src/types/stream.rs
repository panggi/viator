//! Redis Stream implementation.
//!
//! Streams are append-only data structures with unique IDs for each entry.

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

/// Stream entry ID in format `<milliseconds>-<sequence>`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    /// Milliseconds timestamp
    pub ms: u64,
    /// Sequence number
    pub seq: u64,
}

impl StreamId {
    /// Create a new stream ID
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Minimum possible ID
    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    /// Maximum possible ID
    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }

    /// Parse a stream ID from string
    /// Formats: "ms-seq", "ms" (seq defaults to 0), "*" (auto-generate)
    pub fn parse(s: &str) -> Option<StreamIdParsed> {
        if s == "*" {
            return Some(StreamIdParsed::Auto);
        }
        if s == "-" {
            return Some(StreamIdParsed::Min);
        }
        if s == "+" {
            return Some(StreamIdParsed::Max);
        }
        if s == "$" {
            return Some(StreamIdParsed::Last);
        }
        if s == ">" {
            return Some(StreamIdParsed::New);
        }

        let parts: Vec<&str> = s.split('-').collect();
        match parts.len() {
            1 => {
                let ms = parts[0].parse().ok()?;
                Some(StreamIdParsed::Partial { ms, seq: None })
            }
            2 => {
                let ms = parts[0].parse().ok()?;
                if parts[1] == "*" {
                    Some(StreamIdParsed::Partial { ms, seq: None })
                } else {
                    let seq = parts[1].parse().ok()?;
                    Some(StreamIdParsed::Exact(StreamId::new(ms, seq)))
                }
            }
            _ => None,
        }
    }

    /// Format as string
    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Parsed stream ID variants
#[derive(Debug, Clone, Copy)]
pub enum StreamIdParsed {
    /// Exact ID
    Exact(StreamId),
    /// Partial ID (seq may be auto-generated)
    Partial { ms: u64, seq: Option<u64> },
    /// Auto-generate ID
    Auto,
    /// Minimum ID (-)
    Min,
    /// Maximum ID (+)
    Max,
    /// Last entry ID ($)
    Last,
    /// New entries only (>)
    New,
}

/// A single stream entry
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Entry ID
    pub id: StreamId,
    /// Field-value pairs
    pub fields: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    /// Create a new stream entry
    pub fn new(id: StreamId, fields: Vec<(Bytes, Bytes)>) -> Self {
        Self { id, fields }
    }
}

/// A pending entry in the PEL (Pending Entries List)
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// The stream entry ID
    pub id: StreamId,
    /// Consumer that owns this entry
    pub consumer: Bytes,
    /// Time when the entry was delivered (milliseconds)
    pub delivery_time: u64,
    /// Number of times this entry has been delivered
    pub delivery_count: u32,
}

impl PendingEntry {
    /// Create a new pending entry
    pub fn new(id: StreamId, consumer: Bytes, delivery_time: u64) -> Self {
        Self {
            id,
            consumer,
            delivery_time,
            delivery_count: 1,
        }
    }
}

/// A consumer in a consumer group
#[derive(Debug, Clone, Default)]
pub struct StreamConsumer {
    /// Consumer name
    pub name: Bytes,
    /// Last time the consumer was seen (milliseconds)
    pub last_seen: u64,
    /// Number of pending entries for this consumer
    pub pending_count: usize,
    /// Last successful delivery time
    pub last_delivery: u64,
}

impl StreamConsumer {
    /// Create a new consumer
    pub fn new(name: Bytes) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            name,
            last_seen: now,
            pending_count: 0,
            last_delivery: 0,
        }
    }

    /// Update last seen time
    pub fn touch(&mut self) {
        self.last_seen = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
    }
}

/// A consumer group for stream processing
#[derive(Debug, Clone, Default)]
pub struct ConsumerGroup {
    /// Group name
    pub name: Bytes,
    /// Last delivered ID (entries after this are new)
    pub last_delivered_id: StreamId,
    /// Consumers in this group
    pub consumers: HashMap<Bytes, StreamConsumer>,
    /// Pending Entries List - entries delivered but not acknowledged
    pub pel: BTreeMap<StreamId, PendingEntry>,
    /// Total entries ever read by this group
    pub entries_read: u64,
}

impl ConsumerGroup {
    /// Create a new consumer group
    pub fn new(name: Bytes, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            consumers: HashMap::new(),
            pel: BTreeMap::new(),
            entries_read: 0,
        }
    }

    /// Get or create a consumer
    pub fn get_or_create_consumer(&mut self, name: &Bytes) -> &mut StreamConsumer {
        if !self.consumers.contains_key(name) {
            self.consumers.insert(name.clone(), StreamConsumer::new(name.clone()));
        }
        self.consumers.get_mut(name).unwrap()
    }

    /// Add entry to PEL (mark as delivered to consumer)
    pub fn add_to_pel(&mut self, id: StreamId, consumer: &Bytes) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        self.pel.insert(id, PendingEntry::new(id, consumer.clone(), now));
        
        if let Some(c) = self.consumers.get_mut(consumer) {
            c.pending_count += 1;
            c.last_delivery = now;
            c.touch();
        }
        
        self.entries_read += 1;
    }

    /// Acknowledge entries (remove from PEL)
    /// Returns number of entries acknowledged
    pub fn ack(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if let Some(entry) = self.pel.remove(id) {
                count += 1;
                if let Some(c) = self.consumers.get_mut(&entry.consumer) {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
        }
        count
    }

    /// Claim entries from other consumers
    /// Returns claimed entries with updated delivery info
    pub fn claim(
        &mut self,
        ids: &[StreamId],
        new_consumer: &Bytes,
        min_idle_time: u64,
        set_time: Option<u64>,
        set_retrycount: Option<u32>,
        force: bool,
    ) -> Vec<StreamId> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut claimed = Vec::new();

        for id in ids {
            let should_claim = if force {
                // Force claim even if not in PEL
                true
            } else if let Some(entry) = self.pel.get(id) {
                // Check if idle time is sufficient
                now.saturating_sub(entry.delivery_time) >= min_idle_time
            } else {
                false
            };

            if should_claim {
                // Remove from old consumer's count
                if let Some(entry) = self.pel.get(id) {
                    if let Some(c) = self.consumers.get_mut(&entry.consumer) {
                        c.pending_count = c.pending_count.saturating_sub(1);
                    }
                }

                // Update or insert the PEL entry
                let delivery_time = set_time.unwrap_or(now);
                let entry = self.pel.entry(*id).or_insert_with(|| {
                    PendingEntry::new(*id, new_consumer.clone(), delivery_time)
                });
                
                entry.consumer = new_consumer.clone();
                entry.delivery_time = delivery_time;
                if let Some(rc) = set_retrycount {
                    entry.delivery_count = rc;
                } else {
                    entry.delivery_count += 1;
                }

                // Add to new consumer's count
                let consumer = self.get_or_create_consumer(new_consumer);
                consumer.pending_count += 1;
                consumer.touch();

                claimed.push(*id);
            }
        }

        claimed
    }

    /// Auto-claim idle entries
    /// Returns (claimed_ids, next_start_id)
    pub fn autoclaim(
        &mut self,
        min_idle_time: u64,
        start_id: StreamId,
        count: usize,
        new_consumer: &Bytes,
    ) -> (Vec<StreamId>, StreamId) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut claimed = Vec::new();
        let mut next_id = StreamId::min();

        for (id, entry) in self.pel.range(start_id..) {
            if claimed.len() >= count {
                next_id = *id;
                break;
            }

            let idle_time = now.saturating_sub(entry.delivery_time);
            if idle_time >= min_idle_time {
                claimed.push(*id);
            }
        }

        // Actually claim the entries
        for id in &claimed {
            if let Some(entry) = self.pel.get(id) {
                if let Some(c) = self.consumers.get_mut(&entry.consumer) {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
            
            if let Some(entry) = self.pel.get_mut(id) {
                entry.consumer = new_consumer.clone();
                entry.delivery_time = now;
                entry.delivery_count += 1;
            }

            let consumer = self.get_or_create_consumer(new_consumer);
            consumer.pending_count += 1;
            consumer.touch();
        }

        (claimed, next_id)
    }

    /// Get pending entries info
    pub fn pending_summary(&self) -> (usize, Option<StreamId>, Option<StreamId>, Vec<(Bytes, usize)>) {
        let count = self.pel.len();
        let min_id = self.pel.keys().next().copied();
        let max_id = self.pel.keys().next_back().copied();
        
        // Count per consumer
        let mut consumer_counts: HashMap<Bytes, usize> = HashMap::new();
        for entry in self.pel.values() {
            *consumer_counts.entry(entry.consumer.clone()).or_insert(0) += 1;
        }
        let consumers: Vec<_> = consumer_counts.into_iter().collect();

        (count, min_id, max_id, consumers)
    }

    /// Get detailed pending entries for a consumer
    pub fn pending_entries(
        &self,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer_filter: Option<&Bytes>,
    ) -> Vec<&PendingEntry> {
        self.pel
            .range(start..=end)
            .filter(|(_, entry)| {
                consumer_filter.map_or(true, |c| &entry.consumer == c)
            })
            .take(count)
            .map(|(_, entry)| entry)
            .collect()
    }
}

/// Redis Stream data structure
#[derive(Debug, Clone, Default)]
pub struct ViatorStream {
    /// Entries stored by ID
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Last generated ID
    last_id: StreamId,
    /// Total number of entries ever added (including deleted)
    entries_added: u64,
    /// First entry ID (may change with trimming)
    first_entry_id: Option<StreamId>,
    /// Consumer groups for this stream
    consumer_groups: HashMap<Bytes, ConsumerGroup>,
}

impl ViatorStream {
    /// Create a new empty stream
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::new(0, 0),
            entries_added: 0,
            first_entry_id: None,
            consumer_groups: HashMap::new(),
        }
    }

    /// Restore a stream from persistence export data.
    ///
    /// This reconstructs the stream with all metadata intact, ensuring
    /// that ID generation continues correctly after restore.
    pub fn from_export(export: crate::storage::StreamExport) -> Self {
        let mut entries = BTreeMap::new();
        let mut first_entry_id = None;

        for (ms, seq, fields) in export.entries {
            let id = StreamId::new(ms, seq);
            if first_entry_id.is_none() {
                first_entry_id = Some(id);
            }
            entries.insert(id, fields);
        }

        Self {
            entries,
            last_id: StreamId::new(export.last_id.0, export.last_id.1),
            entries_added: export.entries_added,
            first_entry_id,
            consumer_groups: HashMap::new(), // Consumer groups are not persisted yet
        }
    }

    /// Add an entry to the stream
    /// Returns the generated ID or None if the ID is invalid
    pub fn add(&mut self, id: StreamIdParsed, fields: Vec<(Bytes, Bytes)>) -> Option<StreamId> {
        // SAFETY: System time before UNIX_EPOCH (1970) indicates misconfigured clock
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| {
                unreachable!("system clock is before UNIX_EPOCH - check system time")
            })
            .as_millis() as u64;

        let new_id = match id {
            StreamIdParsed::Auto => {
                // Auto-generate ID based on current time
                if now_ms > self.last_id.ms {
                    StreamId::new(now_ms, 0)
                } else {
                    StreamId::new(self.last_id.ms, self.last_id.seq + 1)
                }
            }
            StreamIdParsed::Partial { ms, seq } => {
                let seq = seq.unwrap_or_else(|| {
                    if ms == self.last_id.ms {
                        self.last_id.seq + 1
                    } else {
                        0
                    }
                });
                StreamId::new(ms, seq)
            }
            StreamIdParsed::Exact(id) => id,
            _ => return None, // Min, Max, Last, New not valid for XADD
        };

        // Check that the new ID is greater than the last ID
        if new_id <= self.last_id && !self.entries.is_empty() {
            return None;
        }

        // Special case: ID 0-0 is not allowed unless stream is empty
        if new_id == StreamId::new(0, 0) && !self.entries.is_empty() {
            return None;
        }

        self.entries.insert(new_id, fields);
        self.last_id = new_id;
        self.entries_added += 1;

        if self.first_entry_id.is_none() {
            self.first_entry_id = Some(new_id);
        }

        Some(new_id)
    }

    /// Get the length of the stream
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the stream is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the last entry ID
    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    /// Set the last entry ID (used by XSETID)
    pub fn set_last_id(&mut self, id: StreamId) {
        self.last_id = id;
    }

    /// Get the first entry ID
    pub fn first_id(&self) -> Option<StreamId> {
        self.first_entry_id
    }

    /// Get entries in a range (inclusive)
    pub fn range(&self, start: StreamId, end: StreamId) -> Vec<StreamEntry> {
        self.entries
            .range(start..=end)
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
            .collect()
    }

    /// Get entries in reverse range (inclusive)
    pub fn rev_range(&self, start: StreamId, end: StreamId) -> Vec<StreamEntry> {
        self.entries
            .range(start..=end)
            .rev()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
            .collect()
    }

    /// Get entries with count limit
    pub fn range_count(&self, start: StreamId, end: StreamId, count: usize) -> Vec<StreamEntry> {
        self.entries
            .range(start..=end)
            .take(count)
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
            .collect()
    }

    /// Trim the stream to a maximum length
    /// Returns the number of entries deleted
    pub fn trim_maxlen(&mut self, maxlen: usize, approximate: bool) -> usize {
        if self.entries.len() <= maxlen {
            return 0;
        }

        let to_remove = self.entries.len() - maxlen;
        let mut removed = 0;

        // If approximate, we can be a bit lenient
        let target = if approximate {
            to_remove.saturating_sub(to_remove / 10)
        } else {
            to_remove
        };

        let ids_to_remove: Vec<StreamId> = self.entries.keys().take(target).copied().collect();

        for id in ids_to_remove {
            self.entries.remove(&id);
            removed += 1;
        }

        // Update first entry ID
        self.first_entry_id = self.entries.keys().next().copied();

        removed
    }

    /// Trim entries with ID less than minid
    pub fn trim_minid(&mut self, minid: StreamId, approximate: bool) -> usize {
        let ids_to_remove: Vec<StreamId> = self.entries.range(..minid).map(|(id, _)| *id).collect();

        let mut removed = 0;
        let target = if approximate {
            ids_to_remove.len().saturating_sub(ids_to_remove.len() / 10)
        } else {
            ids_to_remove.len()
        };

        for id in ids_to_remove.into_iter().take(target) {
            self.entries.remove(&id);
            removed += 1;
        }

        // Update first entry ID
        self.first_entry_id = self.entries.keys().next().copied();

        removed
    }

    /// Delete specific entries by ID
    pub fn delete(&mut self, ids: &[StreamId]) -> usize {
        let mut deleted = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                deleted += 1;
            }
        }

        // Update first entry ID
        self.first_entry_id = self.entries.keys().next().copied();

        deleted
    }

    /// Read entries after a given ID (exclusive)
    pub fn read_after(&self, after_id: StreamId, count: Option<usize>) -> Vec<StreamEntry> {
        let iter = self
            .entries
            .range((
                std::ops::Bound::Excluded(after_id),
                std::ops::Bound::Unbounded,
            ))
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()));

        match count {
            Some(n) => iter.take(n).collect(),
            None => iter.collect(),
        }
    }

    /// Get total entries added
    pub fn entries_added(&self) -> u64 {
        self.entries_added
    }

    /// Estimate memory usage
    pub fn memory_usage(&self) -> usize {
        let base = size_of::<Self>();
        let entries: usize = self
            .entries
            .values()
            .map(|fields| 16 + fields.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
            .sum();
        base + entries
    }

    // ==================== Consumer Group Methods ====================

    /// Create a new consumer group
    /// Returns true if created, false if group already exists
    pub fn create_group(&mut self, name: Bytes, last_delivered_id: StreamId, mkstream: bool) -> Result<bool, &'static str> {
        if self.consumer_groups.contains_key(&name) {
            return Ok(false);
        }

        // Validate that the ID exists (unless it's 0-0 or $ for last)
        if last_delivered_id != StreamId::min() && last_delivered_id != self.last_id {
            if !self.entries.contains_key(&last_delivered_id) && !mkstream {
                // ID doesn't exist but we allow it (Redis does too)
            }
        }

        self.consumer_groups.insert(name.clone(), ConsumerGroup::new(name, last_delivered_id));
        Ok(true)
    }

    /// Destroy a consumer group
    /// Returns the number of pending entries that were in the group
    pub fn destroy_group(&mut self, name: &Bytes) -> Option<usize> {
        self.consumer_groups.remove(name).map(|g| g.pel.len())
    }

    /// Get a consumer group by name
    pub fn get_group(&self, name: &Bytes) -> Option<&ConsumerGroup> {
        self.consumer_groups.get(name)
    }

    /// Get a mutable consumer group by name
    pub fn get_group_mut(&mut self, name: &Bytes) -> Option<&mut ConsumerGroup> {
        self.consumer_groups.get_mut(name)
    }

    /// Get all consumer groups
    pub fn groups(&self) -> impl Iterator<Item = &ConsumerGroup> {
        self.consumer_groups.values()
    }

    /// Set the last delivered ID for a consumer group
    pub fn set_group_id(&mut self, name: &Bytes, id: StreamId) -> bool {
        if let Some(group) = self.consumer_groups.get_mut(name) {
            group.last_delivered_id = id;
            true
        } else {
            false
        }
    }

    /// Delete a consumer from a group
    /// Returns the number of pending entries that were removed
    pub fn delete_consumer(&mut self, group_name: &Bytes, consumer_name: &Bytes) -> Option<usize> {
        if let Some(group) = self.consumer_groups.get_mut(group_name) {
            if let Some(consumer) = group.consumers.remove(consumer_name) {
                // Remove all pending entries for this consumer
                let pending_count = consumer.pending_count;
                group.pel.retain(|_, entry| entry.consumer != *consumer_name);
                return Some(pending_count);
            }
        }
        None
    }

    /// Read entries for a consumer group
    /// This method handles the ">" ID (new entries) and specific IDs (pending entries)
    /// Returns entries and updates PEL if not noack
    pub fn read_group(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        after_id: StreamId,
        count: Option<usize>,
        noack: bool,
    ) -> Option<Vec<StreamEntry>> {
        let group = self.consumer_groups.get_mut(group_name)?;
        
        // Ensure consumer exists
        group.get_or_create_consumer(consumer_name);

        // Determine what to read
        let is_new_only = after_id == StreamId::max();
        
        let entries = if is_new_only {
            // Read new entries (after last_delivered_id)
            let start_id = StreamId::new(
                group.last_delivered_id.ms,
                group.last_delivered_id.seq.saturating_add(1),
            );
            
            let entries: Vec<StreamEntry> = self.entries
                .range(start_id..)
                .take(count.unwrap_or(usize::MAX))
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect();

            // Update last_delivered_id
            if let Some(last) = entries.last() {
                group.last_delivered_id = last.id;
            }

            // Add to PEL if not noack
            if !noack {
                for entry in &entries {
                    group.add_to_pel(entry.id, consumer_name);
                }
            }

            entries
        } else {
            // Read pending entries (after specified ID)
            // This returns entries from PEL that belong to this consumer
            let entries: Vec<StreamEntry> = group.pel
                .range(after_id..)
                .filter(|(_, pending)| pending.consumer == *consumer_name)
                .take(count.unwrap_or(usize::MAX))
                .filter_map(|(id, _)| {
                    self.entries.get(id).map(|fields| {
                        StreamEntry::new(*id, fields.clone())
                    })
                })
                .collect();

            entries
        };

        Some(entries)
    }

    /// Get an entry by ID (for use with claimed entries)
    pub fn get_entry(&self, id: &StreamId) -> Option<StreamEntry> {
        self.entries.get(id).map(|fields| StreamEntry::new(*id, fields.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_parse() {
        assert!(matches!(StreamId::parse("*"), Some(StreamIdParsed::Auto)));
        assert!(matches!(StreamId::parse("-"), Some(StreamIdParsed::Min)));
        assert!(matches!(StreamId::parse("+"), Some(StreamIdParsed::Max)));

        if let Some(StreamIdParsed::Exact(id)) = StreamId::parse("1000-0") {
            assert_eq!(id.ms, 1000);
            assert_eq!(id.seq, 0);
        } else {
            panic!("Failed to parse 1000-0");
        }

        if let Some(StreamIdParsed::Partial { ms, seq }) = StreamId::parse("1000") {
            assert_eq!(ms, 1000);
            assert!(seq.is_none());
        } else {
            panic!("Failed to parse 1000");
        }
    }

    #[test]
    fn test_stream_add() {
        let mut stream = ViatorStream::new();

        let id = stream.add(
            StreamIdParsed::Auto,
            vec![(Bytes::from("field"), Bytes::from("value"))],
        );
        assert!(id.is_some());
        assert_eq!(stream.len(), 1);

        let id2 = stream.add(
            StreamIdParsed::Auto,
            vec![(Bytes::from("f2"), Bytes::from("v2"))],
        );
        assert!(id2.is_some());
        assert!(id2.unwrap() > id.unwrap());
    }

    #[test]
    fn test_stream_range() {
        let mut stream = ViatorStream::new();

        for i in 1..=5 {
            stream.add(
                StreamIdParsed::Exact(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
            );
        }

        let entries = stream.range(StreamId::new(2000, 0), StreamId::new(4000, 0));
        assert_eq!(entries.len(), 3);
    }
}
