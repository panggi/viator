//! Redis Stream implementation.
//!
//! Streams are append-only data structures with unique IDs for each entry.

use bytes::Bytes;
use std::collections::BTreeMap;

/// Stream entry ID in format <milliseconds>-<sequence>
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
}

impl ViatorStream {
    /// Create a new empty stream
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::new(0, 0),
            entries_added: 0,
            first_entry_id: None,
        }
    }

    /// Add an entry to the stream
    /// Returns the generated ID or None if the ID is invalid
    pub fn add(&mut self, id: StreamIdParsed, fields: Vec<(Bytes, Bytes)>) -> Option<StreamId> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
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
            .iter()
            .map(|(_, fields)| 16 + fields.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
            .sum();
        base + entries
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
