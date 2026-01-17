//! Stream command implementations.
//!
//! Redis Streams are append-only data structures with unique IDs for each entry.
//! They provide pub/sub with persistence and consumer groups.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, StreamId, StreamIdParsed, ValueType, ViatorStream, ViatorValue};
use bytes::Bytes;
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Get or create a stream from the database
fn get_or_create_stream(db: &Db, key: &Key) -> Result<ViatorValue> {
    match db.get(key) {
        Some(v) if v.is_stream() => Ok(v),
        Some(_) => Err(CommandError::WrongType.into()),
        None => Ok(ViatorValue::new_stream()),
    }
}

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] [KEEPREF|DELREF] [ACKED] *|id field value [field value ...]
/// Redis 8.2+ adds KEEPREF/DELREF/ACKED options for trimming behavior.
pub fn cmd_xadd(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?; // key id field value (minimum)

        let key = Key::from(cmd.args[0].clone());
        let mut idx = 1;
        let mut nomkstream = false;
        let mut maxlen: Option<(usize, bool)> = None; // (threshold, approximate)
        let mut minid: Option<(StreamId, bool)> = None;
        let mut _keep_ref = false; // Redis 8.2+ KEEPREF option
        let mut _del_ref = true; // Redis 8.2+ DELREF is default
        let mut _acked_only = false; // Redis 8.2+ ACKED option

        // Parse options
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "NOMKSTREAM" => {
                    nomkstream = true;
                    idx += 1;
                }
                "MAXLEN" => {
                    idx += 1;
                    let approximate = if idx < cmd.args.len() {
                        let next = cmd.get_str(idx)?;
                        if next == "~" {
                            idx += 1;
                            true
                        } else if next == "=" {
                            idx += 1;
                            false
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    let threshold = cmd.get_u64(idx)? as usize;
                    maxlen = Some((threshold, approximate));
                    idx += 1;
                }
                "MINID" => {
                    idx += 1;
                    let approximate = if idx < cmd.args.len() {
                        let next = cmd.get_str(idx)?;
                        if next == "~" {
                            idx += 1;
                            true
                        } else if next == "=" {
                            idx += 1;
                            false
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    let id_str = cmd.get_str(idx)?;
                    let id = match StreamId::parse(id_str) {
                        Some(StreamIdParsed::Exact(id)) => id,
                        Some(StreamIdParsed::Partial { ms, seq }) => {
                            StreamId::new(ms, seq.unwrap_or(0))
                        }
                        _ => return Err(CommandError::SyntaxError.into()),
                    };
                    minid = Some((id, approximate));
                    idx += 1;
                }
                "LIMIT" => {
                    // LIMIT is used with MAXLEN/MINID but we ignore it for simplicity
                    idx += 2;
                }
                "KEEPREF" => {
                    _keep_ref = true;
                    _del_ref = false;
                    idx += 1;
                }
                "DELREF" => {
                    _del_ref = true;
                    _keep_ref = false;
                    idx += 1;
                }
                "ACKED" => {
                    _acked_only = true;
                    idx += 1;
                }
                _ => break, // Not an option, must be ID
            }
        }

        // Parse ID
        if idx >= cmd.args.len() {
            return Err(CommandError::WrongArity {
                command: "XADD".to_string(),
            }
            .into());
        }

        let id_str = cmd.get_str(idx)?;
        let id = StreamId::parse(id_str).ok_or(CommandError::StreamInvalidId)?;
        idx += 1;

        // Parse field-value pairs
        let remaining = cmd.args.len() - idx;
        if remaining == 0 || remaining % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "XADD".to_string(),
            }
            .into());
        }

        let mut fields = Vec::with_capacity(remaining / 2);
        while idx < cmd.args.len() {
            let field = cmd.args[idx].clone();
            let value = cmd.args[idx + 1].clone();
            fields.push((field, value));
            idx += 2;
        }

        // Check if stream exists when NOMKSTREAM is set
        if nomkstream && db.get(&key).is_none() {
            return Ok(Frame::Null);
        }

        // Get or create stream
        let value = get_or_create_stream(&db, &key)?;
        let stream = value
            .as_stream()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
        let mut guard = stream.write();

        // Add the entry
        let new_id = guard
            .add(id, fields)
            .ok_or(CommandError::StreamIdTooSmall)?;

        // Apply trimming if specified
        if let Some((threshold, approximate)) = maxlen {
            guard.trim_maxlen(threshold, approximate);
        }
        if let Some((min_id, approximate)) = minid {
            guard.trim_minid(min_id, approximate);
        }

        drop(guard);

        // Store the value
        db.set(key, value);

        Ok(Frame::bulk(new_id.to_string()))
    })
}

/// XLEN key
pub fn cmd_xlen(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        let len = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => v
                .as_stream()
                .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"))
                .read()
                .len(),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// XRANGE key start end [COUNT count]
pub fn cmd_xrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let start_str = cmd.get_str(1)?;
        let end_str = cmd.get_str(2)?;

        let start = parse_range_id(start_str, true)?;
        let end = parse_range_id(end_str, false)?;

        let count = if cmd.args.len() >= 5 {
            let opt = cmd.get_str(3)?.to_uppercase();
            if opt == "COUNT" {
                Some(cmd.get_u64(4)? as usize)
            } else {
                return Err(CommandError::SyntaxError.into());
            }
        } else {
            None
        };

        let entries = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                let guard = stream.read();
                match count {
                    Some(n) => guard.range_count(start, end, n),
                    None => guard.range(start, end),
                }
            }
            None => Vec::new(),
        };

        Ok(stream_entries_to_frame(entries))
    })
}

/// XREVRANGE key end start [COUNT count]
pub fn cmd_xrevrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let end_str = cmd.get_str(1)?;
        let start_str = cmd.get_str(2)?;

        let end = parse_range_id(end_str, false)?;
        let start = parse_range_id(start_str, true)?;

        let count = if cmd.args.len() >= 5 {
            let opt = cmd.get_str(3)?.to_uppercase();
            if opt == "COUNT" {
                Some(cmd.get_u64(4)? as usize)
            } else {
                return Err(CommandError::SyntaxError.into());
            }
        } else {
            None
        };

        let entries = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                let guard = stream.read();
                let mut entries = guard.rev_range(start, end);
                if let Some(n) = count {
                    entries.truncate(n);
                }
                entries
            }
            None => Vec::new(),
        };

        Ok(stream_entries_to_frame(entries))
    })
}

/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
pub fn cmd_xread(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let mut idx = 0;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;

        // Parse options
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                    idx += 1;
                }
                "BLOCK" => {
                    idx += 1;
                    block_ms = Some(cmd.get_u64(idx)?);
                    idx += 1;
                }
                "STREAMS" => {
                    idx += 1;
                    break;
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
        }

        // Parse keys and IDs
        let remaining = cmd.args.len() - idx;
        if remaining == 0 || remaining % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "XREAD".to_string(),
            }
            .into());
        }

        let num_keys = remaining / 2;
        let keys: Vec<Key> = cmd.args[idx..idx + num_keys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let ids: Vec<StreamId> = cmd.args[idx + num_keys..]
            .iter()
            .map(|b| {
                let s = std::str::from_utf8(b).unwrap_or("");
                match StreamId::parse(s) {
                    Some(StreamIdParsed::Exact(id)) => id,
                    Some(StreamIdParsed::Last) => StreamId::max(),
                    Some(StreamIdParsed::New) => StreamId::max(),
                    Some(StreamIdParsed::Partial { ms, seq }) => {
                        StreamId::new(ms, seq.unwrap_or(0))
                    }
                    _ => StreamId::min(),
                }
            })
            .collect();

        // Calculate deadline for blocking
        let deadline =
            block_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));

        // Blocking loop - keep trying until we get results or timeout
        loop {
            // Read from each stream
            let mut results = Vec::new();
            for (key, after_id) in keys.iter().zip(ids.iter()) {
                if let Some(v) = db.get_typed(key, ValueType::Stream)? {
                    let stream = v
                        .as_stream()
                        .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                    let guard = stream.read();
                    let entries = guard.read_after(*after_id, count);

                    if !entries.is_empty() {
                        results.push(Frame::Array(vec![
                            Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                            stream_entries_to_frame(entries),
                        ]));
                    }
                }
            }

            // If we have results, return them
            if !results.is_empty() {
                return Ok(Frame::Array(results));
            }

            // If not blocking, return null immediately
            if block_ms.is_none() {
                return Ok(Frame::Null);
            }

            // Check if we've exceeded the deadline
            if let Some(deadline) = deadline {
                if std::time::Instant::now() >= deadline {
                    return Ok(Frame::Null);
                }
            }

            // Sleep briefly before retrying (100ms or remaining time, whichever is less)
            let sleep_duration = if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                std::cmp::min(remaining, std::time::Duration::from_millis(100))
            } else {
                std::time::Duration::from_millis(100)
            };

            if sleep_duration.is_zero() {
                return Ok(Frame::Null);
            }

            tokio::time::sleep(sleep_duration).await;
        }
    })
}

/// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count] [KEEPREF|DELREF] [ACKED]
/// Redis 8.2+ adds KEEPREF/DELREF/ACKED options for trimming behavior.
pub fn cmd_xtrim(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let strategy = cmd.get_str(1)?.to_uppercase();

        let mut idx = 2;
        let mut _keep_ref = false; // Redis 8.2+ KEEPREF option
        let mut _del_ref = true; // Redis 8.2+ DELREF is default
        let mut _acked_only = false; // Redis 8.2+ ACKED option

        let approximate = if idx < cmd.args.len() {
            let next = cmd.get_str(idx)?;
            if next == "~" {
                idx += 1;
                true
            } else if next == "=" {
                idx += 1;
                false
            } else {
                false
            }
        } else {
            false
        };

        // Get threshold value
        let threshold_idx = idx;
        idx += 1;

        // Parse remaining options
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "LIMIT" => {
                    idx += 2; // Skip LIMIT and count
                }
                "KEEPREF" => {
                    _keep_ref = true;
                    _del_ref = false;
                    idx += 1;
                }
                "DELREF" => {
                    _del_ref = true;
                    _keep_ref = false;
                    idx += 1;
                }
                "ACKED" => {
                    _acked_only = true;
                    idx += 1;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        let deleted = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                let mut guard = stream.write();

                match strategy.as_str() {
                    "MAXLEN" => {
                        let threshold = cmd.get_u64(threshold_idx)? as usize;
                        guard.trim_maxlen(threshold, approximate)
                    }
                    "MINID" => {
                        let id_str = cmd.get_str(threshold_idx)?;
                        let id = match StreamId::parse(id_str) {
                            Some(StreamIdParsed::Exact(id)) => id,
                            Some(StreamIdParsed::Partial { ms, seq }) => {
                                StreamId::new(ms, seq.unwrap_or(0))
                            }
                            _ => return Err(CommandError::StreamInvalidId.into()),
                        };
                        guard.trim_minid(id, approximate)
                    }
                    _ => return Err(CommandError::SyntaxError.into()),
                }
            }
            None => 0,
        };

        Ok(Frame::Integer(deleted as i64))
    })
}

/// XDEL key id [id ...]
pub fn cmd_xdel(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());

        let ids: Vec<StreamId> = cmd.args[1..]
            .iter()
            .filter_map(|b| {
                let s = std::str::from_utf8(b).ok()?;
                match StreamId::parse(s) {
                    Some(StreamIdParsed::Exact(id)) => Some(id),
                    Some(StreamIdParsed::Partial { ms, seq }) => {
                        Some(StreamId::new(ms, seq.unwrap_or(0)))
                    }
                    _ => None,
                }
            })
            .collect();

        let deleted = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                let mut guard = stream.write();
                guard.delete(&ids)
            }
            None => 0,
        };

        Ok(Frame::Integer(deleted as i64))
    })
}

/// XINFO STREAM key [FULL [COUNT count]]
pub fn cmd_xinfo(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();
        let key = Key::from(cmd.args[1].clone());

        match subcommand.as_str() {
            "STREAM" => {
                let value = db
                    .get_typed(&key, ValueType::Stream)?
                    .ok_or(CommandError::NoSuchKey)?;

                let stream = value
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                let guard = stream.read();

                let mut info = vec![
                    Frame::bulk("length"),
                    Frame::Integer(guard.len() as i64),
                    Frame::bulk("entries-added"),
                    Frame::Integer(guard.entries_added() as i64),
                    Frame::bulk("last-generated-id"),
                    Frame::bulk(guard.last_id().to_string()),
                ];

                if let Some(first_id) = guard.first_id() {
                    info.push(Frame::bulk("first-entry"));
                    let entries = guard.range(first_id, first_id);
                    if let Some(entry) = entries.first() {
                        info.push(stream_entry_to_frame(entry));
                    } else {
                        info.push(Frame::Null);
                    }
                } else {
                    info.push(Frame::bulk("first-entry"));
                    info.push(Frame::Null);
                }

                let last_id = guard.last_id();
                let last_entries = guard.range(last_id, last_id);
                info.push(Frame::bulk("last-entry"));
                if let Some(entry) = last_entries.first() {
                    info.push(stream_entry_to_frame(entry));
                } else {
                    info.push(Frame::Null);
                }

                Ok(Frame::Array(info))
            }
            "GROUPS" => {
                let value = db
                    .get_typed(&key, ValueType::Stream)?
                    .ok_or(CommandError::NoSuchKey)?;

                let stream = value
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let guard = stream.read();

                let groups: Vec<Frame> = guard
                    .groups()
                    .map(|group| {
                        let (pending_count, _min_id, _max_id, _) = group.pending_summary();
                        Frame::Array(vec![
                            Frame::bulk("name"),
                            Frame::Bulk(group.name.clone()),
                            Frame::bulk("consumers"),
                            Frame::Integer(group.consumers.len() as i64),
                            Frame::bulk("pending"),
                            Frame::Integer(pending_count as i64),
                            Frame::bulk("last-delivered-id"),
                            Frame::bulk(group.last_delivered_id.to_string()),
                            Frame::bulk("entries-read"),
                            Frame::Integer(group.entries_read as i64),
                            Frame::bulk("lag"),
                            Frame::Integer(0), // Lag calculation would need stream length comparison
                        ])
                    })
                    .collect();

                Ok(Frame::Array(groups))
            }
            "CONSUMERS" => {
                if cmd.args.len() < 3 {
                    return Err(CommandError::WrongArity {
                        command: "XINFO CONSUMERS".to_string(),
                    }
                    .into());
                }

                let group_name = cmd.args[2].clone();

                let value = db
                    .get_typed(&key, ValueType::Stream)?
                    .ok_or(CommandError::NoSuchKey)?;

                let stream = value
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let guard = stream.read();

                if let Some(group) = guard.get_group(&group_name) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);

                    let consumers: Vec<Frame> = group
                        .consumers
                        .values()
                        .map(|consumer| {
                            let idle = now.saturating_sub(consumer.last_seen);
                            Frame::Array(vec![
                                Frame::bulk("name"),
                                Frame::Bulk(consumer.name.clone()),
                                Frame::bulk("pending"),
                                Frame::Integer(consumer.pending_count as i64),
                                Frame::bulk("idle"),
                                Frame::Integer(idle as i64),
                                Frame::bulk("inactive"),
                                Frame::Integer(idle as i64),
                            ])
                        })
                        .collect();

                    Ok(Frame::Array(consumers))
                } else {
                    Ok(Frame::Error("NOGROUP No such consumer group".to_string()))
                }
            }
            _ => Err(CommandError::SyntaxError.into()),
        }
    })
}

/// Helper to parse range IDs (-, +, or exact ID)
fn parse_range_id(s: &str, is_start: bool) -> Result<StreamId> {
    match StreamId::parse(s) {
        Some(StreamIdParsed::Min) => Ok(StreamId::min()),
        Some(StreamIdParsed::Max) => Ok(StreamId::max()),
        Some(StreamIdParsed::Exact(id)) => Ok(id),
        Some(StreamIdParsed::Partial { ms, seq }) => {
            if is_start {
                Ok(StreamId::new(ms, seq.unwrap_or(0)))
            } else {
                Ok(StreamId::new(ms, seq.unwrap_or(u64::MAX)))
            }
        }
        _ => Err(CommandError::StreamInvalidId.into()),
    }
}

/// Convert stream entries to RESP frame
fn stream_entries_to_frame(entries: Vec<crate::types::StreamEntry>) -> Frame {
    Frame::Array(entries.iter().map(stream_entry_to_frame).collect())
}

/// Convert a single stream entry to RESP frame
fn stream_entry_to_frame(entry: &crate::types::StreamEntry) -> Frame {
    let fields: Vec<Frame> = entry
        .fields
        .iter()
        .flat_map(|(k, v)| vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())])
        .collect();

    Frame::Array(vec![
        Frame::bulk(entry.id.to_string()),
        Frame::Array(fields),
    ])
}

/// XGROUP CREATE|CREATECONSUMER|DELCONSUMER|DESTROY|SETID key groupname [consumer] [id|$] [MKSTREAM]
pub fn cmd_xgroup(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for 'xgroup' command".to_string(),
            ));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "CREATE" => {
                cmd.require_args(3)?;
                let key = Key::from(cmd.args[1].clone());
                let group_name = cmd.args[2].clone();
                let id_str = cmd.get_str(3)?;

                // Check for MKSTREAM option
                let mkstream = cmd.args.iter().skip(4).any(|a| {
                    std::str::from_utf8(a)
                        .map(|s| s.to_uppercase() == "MKSTREAM")
                        .unwrap_or(false)
                });

                // Parse the ID
                let last_delivered_id = if id_str == "$" {
                    // Use the stream's last ID
                    if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                        let stream = v.as_stream().unwrap();
                        stream.read().last_id()
                    } else if mkstream {
                        StreamId::min()
                    } else {
                        return Ok(Frame::Error(
                            "ERR The XGROUP subcommand requires the key to exist".to_string(),
                        ));
                    }
                } else if id_str == "0" || id_str == "0-0" {
                    StreamId::min()
                } else {
                    match StreamId::parse(id_str) {
                        Some(StreamIdParsed::Exact(id)) => id,
                        Some(StreamIdParsed::Partial { ms, seq }) => {
                            StreamId::new(ms, seq.unwrap_or(0))
                        }
                        _ => return Ok(Frame::Error("ERR Invalid stream ID".to_string())),
                    }
                };

                // Create stream if MKSTREAM and doesn't exist
                if mkstream && db.get(&key).is_none() {
                    db.set(
                        key.clone(),
                        ViatorValue::Stream(Arc::new(RwLock::new(ViatorStream::new()))),
                    );
                }

                // Get or create the stream and add the consumer group
                if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                    let stream = v.as_stream().unwrap();
                    let mut guard = stream.write();
                    match guard.create_group(group_name, last_delivered_id, mkstream) {
                        Ok(true) => Ok(Frame::ok()),
                        Ok(false) => Ok(Frame::Error(
                            "BUSYGROUP Consumer Group name already exists".to_string(),
                        )),
                        Err(e) => Ok(Frame::Error(format!("ERR {}", e))),
                    }
                } else {
                    Ok(Frame::Error(
                        "ERR The XGROUP subcommand requires the key to exist".to_string(),
                    ))
                }
            }
            "CREATECONSUMER" => {
                cmd.require_args(3)?;
                let key = Key::from(cmd.args[1].clone());
                let group_name = cmd.args[2].clone();
                let consumer_name = cmd.args[3].clone();

                if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                    let stream = v.as_stream().unwrap();
                    let mut guard = stream.write();
                    if let Some(group) = guard.get_group_mut(&group_name) {
                        let is_new = !group.consumers.contains_key(&consumer_name);
                        group.get_or_create_consumer(&consumer_name);
                        Ok(Frame::Integer(if is_new { 1 } else { 0 }))
                    } else {
                        Ok(Frame::Error("NOGROUP No such consumer group".to_string()))
                    }
                } else {
                    Ok(Frame::Error(
                        "ERR The XGROUP subcommand requires the key to exist".to_string(),
                    ))
                }
            }
            "DELCONSUMER" => {
                cmd.require_args(3)?;
                let key = Key::from(cmd.args[1].clone());
                let group_name = cmd.args[2].clone();
                let consumer_name = cmd.args[3].clone();

                if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                    let stream = v.as_stream().unwrap();
                    let mut guard = stream.write();
                    match guard.delete_consumer(&group_name, &consumer_name) {
                        Some(pending) => Ok(Frame::Integer(pending as i64)),
                        None => Ok(Frame::Integer(0)),
                    }
                } else {
                    Ok(Frame::Error(
                        "ERR The XGROUP subcommand requires the key to exist".to_string(),
                    ))
                }
            }
            "DESTROY" => {
                cmd.require_args(2)?;
                let key = Key::from(cmd.args[1].clone());
                let group_name = cmd.args[2].clone();

                if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                    let stream = v.as_stream().unwrap();
                    let mut guard = stream.write();
                    match guard.destroy_group(&group_name) {
                        Some(_) => Ok(Frame::Integer(1)),
                        None => Ok(Frame::Integer(0)),
                    }
                } else {
                    Ok(Frame::Integer(0))
                }
            }
            "SETID" => {
                cmd.require_args(3)?;
                let key = Key::from(cmd.args[1].clone());
                let group_name = cmd.args[2].clone();
                let id_str = cmd.get_str(3)?;

                let new_id = if id_str == "$" {
                    if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                        v.as_stream().unwrap().read().last_id()
                    } else {
                        return Ok(Frame::Error(
                            "ERR The XGROUP subcommand requires the key to exist".to_string(),
                        ));
                    }
                } else {
                    match StreamId::parse(id_str) {
                        Some(StreamIdParsed::Exact(id)) => id,
                        Some(StreamIdParsed::Partial { ms, seq }) => {
                            StreamId::new(ms, seq.unwrap_or(0))
                        }
                        _ => return Ok(Frame::Error("ERR Invalid stream ID".to_string())),
                    }
                };

                if let Some(v) = db.get_typed(&key, ValueType::Stream)? {
                    let stream = v.as_stream().unwrap();
                    let mut guard = stream.write();
                    if guard.set_group_id(&group_name, new_id) {
                        Ok(Frame::ok())
                    } else {
                        Ok(Frame::Error("NOGROUP No such consumer group".to_string()))
                    }
                } else {
                    Ok(Frame::Error(
                        "ERR The XGROUP subcommand requires the key to exist".to_string(),
                    ))
                }
            }
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("XGROUP CREATE key groupname id|$ [MKSTREAM]"),
                Frame::bulk("XGROUP CREATECONSUMER key groupname consumername"),
                Frame::bulk("XGROUP DELCONSUMER key groupname consumername"),
                Frame::bulk("XGROUP DESTROY key groupname"),
                Frame::bulk("XGROUP SETID key groupname id|$"),
            ])),
            _ => Ok(Frame::Error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            ))),
        }
    })
}

/// XACK key group id [id ...]
pub fn cmd_xack(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let group_name = cmd.args[1].clone();

        // Parse IDs to acknowledge
        let ids: Vec<StreamId> = cmd.args[2..]
            .iter()
            .filter_map(|b| {
                let s = std::str::from_utf8(b).ok()?;
                match StreamId::parse(s) {
                    Some(StreamIdParsed::Exact(id)) => Some(id),
                    Some(StreamIdParsed::Partial { ms, seq }) => {
                        Some(StreamId::new(ms, seq.unwrap_or(0)))
                    }
                    _ => None,
                }
            })
            .collect();

        let acked = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let mut guard = stream.write();
                if let Some(group) = guard.get_group_mut(&group_name) {
                    group.ack(&ids)
                } else {
                    0
                }
            }
            None => 0,
        };

        Ok(Frame::Integer(acked as i64))
    })
}

/// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
pub fn cmd_xclaim(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?;

        let key = Key::from(cmd.args[0].clone());
        let group_name = cmd.args[1].clone();
        let consumer_name = cmd.args[2].clone();
        let min_idle_time = cmd.get_u64(3)?;

        // Parse options and IDs
        let mut idx = 4;
        let mut ids = Vec::new();
        let mut set_idle: Option<u64> = None;
        let mut set_time: Option<u64> = None;
        let mut set_retrycount: Option<u32> = None;
        let mut force = false;
        let mut justid = false;

        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?;
            let upper = arg.to_uppercase();
            match upper.as_str() {
                "IDLE" => {
                    idx += 1;
                    set_idle = Some(cmd.get_u64(idx)?);
                    idx += 1;
                }
                "TIME" => {
                    idx += 1;
                    set_time = Some(cmd.get_u64(idx)?);
                    idx += 1;
                }
                "RETRYCOUNT" => {
                    idx += 1;
                    set_retrycount = Some(cmd.get_u64(idx)? as u32);
                    idx += 1;
                }
                "FORCE" => {
                    force = true;
                    idx += 1;
                }
                "JUSTID" => {
                    justid = true;
                    idx += 1;
                }
                "LASTID" => {
                    // Redis 7+ LASTID option (ignored for now)
                    idx += 2;
                }
                _ => {
                    // Must be an ID
                    if let Some(parsed) = StreamId::parse(arg) {
                        match parsed {
                            StreamIdParsed::Exact(id) => ids.push(id),
                            StreamIdParsed::Partial { ms, seq } => {
                                ids.push(StreamId::new(ms, seq.unwrap_or(0)))
                            }
                            _ => {}
                        }
                    }
                    idx += 1;
                }
            }
        }

        // Calculate effective time from IDLE or TIME
        let effective_time = if let Some(idle) = set_idle {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            Some(now.saturating_sub(idle))
        } else {
            set_time
        };

        match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let mut guard = stream.write();

                if let Some(group) = guard.get_group_mut(&group_name) {
                    let claimed = group.claim(
                        &ids,
                        &consumer_name,
                        min_idle_time,
                        effective_time,
                        set_retrycount,
                        force,
                    );

                    if justid {
                        // Return just the IDs
                        Ok(Frame::Array(
                            claimed
                                .into_iter()
                                .map(|id| Frame::bulk(id.to_string()))
                                .collect(),
                        ))
                    } else {
                        // Return full entries
                        let entries: Vec<Frame> = claimed
                            .into_iter()
                            .filter_map(|id| {
                                guard.get_entry(&id).map(|entry| {
                                    let fields: Vec<Frame> = entry
                                        .fields
                                        .iter()
                                        .flat_map(|(k, v)| {
                                            vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())]
                                        })
                                        .collect();
                                    Frame::Array(vec![
                                        Frame::bulk(entry.id.to_string()),
                                        Frame::Array(fields),
                                    ])
                                })
                            })
                            .collect();
                        Ok(Frame::Array(entries))
                    }
                } else {
                    Ok(Frame::Error("NOGROUP No such consumer group".to_string()))
                }
            }
            None => Ok(Frame::Error("ERR no such key".to_string())),
        }
    })
}

/// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
pub fn cmd_xpending(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let group_name = cmd.args[1].clone();

        match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let guard = stream.read();

                if let Some(group) = guard.get_group(&group_name) {
                    // Check if detailed format (with start/end/count)
                    if cmd.args.len() >= 5 {
                        // Parse optional IDLE filter
                        let mut idx = 2;
                        let mut min_idle: Option<u64> = None;

                        if cmd.get_str(idx)?.to_uppercase() == "IDLE" {
                            idx += 1;
                            min_idle = Some(cmd.get_u64(idx)?);
                            idx += 1;
                        }

                        let start_str = cmd.get_str(idx)?;
                        let start = parse_range_id(start_str, true)?;
                        idx += 1;

                        let end_str = cmd.get_str(idx)?;
                        let end = parse_range_id(end_str, false)?;
                        idx += 1;

                        let count = cmd.get_u64(idx)? as usize;
                        idx += 1;

                        let consumer_filter = if idx < cmd.args.len() {
                            Some(cmd.args[idx].clone())
                        } else {
                            None
                        };

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);

                        // Get detailed entries
                        let pending_entries =
                            group.pending_entries(start, end, count, consumer_filter.as_ref());

                        // Filter by idle time if specified
                        let entries: Vec<Frame> = pending_entries
                            .into_iter()
                            .filter(|entry| {
                                if let Some(min) = min_idle {
                                    now.saturating_sub(entry.delivery_time) >= min
                                } else {
                                    true
                                }
                            })
                            .map(|entry| {
                                Frame::Array(vec![
                                    Frame::bulk(entry.id.to_string()),
                                    Frame::Bulk(entry.consumer.clone()),
                                    Frame::Integer(now.saturating_sub(entry.delivery_time) as i64),
                                    Frame::Integer(entry.delivery_count as i64),
                                ])
                            })
                            .collect();

                        Ok(Frame::Array(entries))
                    } else {
                        // Summary format: [pending_count, min_id, max_id, [[consumer, count]...]]
                        let (count, min_id, max_id, consumers) = group.pending_summary();

                        let min_frame = min_id
                            .map(|id| Frame::bulk(id.to_string()))
                            .unwrap_or(Frame::Null);
                        let max_frame = max_id
                            .map(|id| Frame::bulk(id.to_string()))
                            .unwrap_or(Frame::Null);

                        let consumers_frame = if consumers.is_empty() {
                            Frame::Null
                        } else {
                            Frame::Array(
                                consumers
                                    .into_iter()
                                    .map(|(name, cnt)| {
                                        Frame::Array(vec![
                                            Frame::Bulk(name),
                                            Frame::bulk(cnt.to_string()),
                                        ])
                                    })
                                    .collect(),
                            )
                        };

                        Ok(Frame::Array(vec![
                            Frame::Integer(count as i64),
                            min_frame,
                            max_frame,
                            consumers_frame,
                        ]))
                    }
                } else {
                    Ok(Frame::Error("NOGROUP No such consumer group".to_string()))
                }
            }
            None => Ok(Frame::Error("ERR no such key".to_string())),
        }
    })
}

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
pub fn cmd_xautoclaim(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?;

        let key = Key::from(cmd.args[0].clone());
        let group_name = cmd.args[1].clone();
        let consumer_name = cmd.args[2].clone();
        let min_idle_time = cmd.get_u64(3)?;
        let start_str = cmd.get_str(4)?;

        let start_id = match StreamId::parse(start_str) {
            Some(StreamIdParsed::Exact(id)) => id,
            Some(StreamIdParsed::Partial { ms, seq }) => StreamId::new(ms, seq.unwrap_or(0)),
            Some(StreamIdParsed::Min) => StreamId::min(),
            _ => StreamId::min(),
        };

        // Parse optional arguments
        let mut count = 100usize; // Redis default
        let mut justid = false;
        let mut idx = 5;

        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    idx += 1;
                    count = cmd.get_u64(idx)? as usize;
                    idx += 1;
                }
                "JUSTID" => {
                    justid = true;
                    idx += 1;
                }
                _ => {
                    idx += 1;
                }
            }
        }

        match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let mut guard = stream.write();

                if let Some(group) = guard.get_group_mut(&group_name) {
                    let (claimed_ids, next_id) =
                        group.autoclaim(min_idle_time, start_id, count, &consumer_name);

                    let next_id_str = if next_id == StreamId::min() {
                        "0-0".to_string()
                    } else {
                        next_id.to_string()
                    };

                    if justid {
                        // Return: [next_id, [claimed ids], [deleted ids]]
                        Ok(Frame::Array(vec![
                            Frame::bulk(next_id_str),
                            Frame::Array(
                                claimed_ids
                                    .into_iter()
                                    .map(|id| Frame::bulk(id.to_string()))
                                    .collect(),
                            ),
                            Frame::Array(vec![]), // No deleted IDs tracking for now
                        ]))
                    } else {
                        // Return: [next_id, [claimed entries], [deleted ids]]
                        let entries: Vec<Frame> = claimed_ids
                            .into_iter()
                            .filter_map(|id| {
                                guard.get_entry(&id).map(|entry| {
                                    let fields: Vec<Frame> = entry
                                        .fields
                                        .iter()
                                        .flat_map(|(k, v)| {
                                            vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())]
                                        })
                                        .collect();
                                    Frame::Array(vec![
                                        Frame::bulk(entry.id.to_string()),
                                        Frame::Array(fields),
                                    ])
                                })
                            })
                            .collect();

                        Ok(Frame::Array(vec![
                            Frame::bulk(next_id_str),
                            Frame::Array(entries),
                            Frame::Array(vec![]), // No deleted IDs tracking for now
                        ]))
                    }
                } else {
                    Ok(Frame::Error("NOGROUP No such consumer group".to_string()))
                }
            }
            None => Ok(Frame::Error("ERR no such key".to_string())),
        }
    })
}

/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] [CLAIM min-idle-time] STREAMS key [key ...] id [id ...]
/// The CLAIM option (Redis 8.4+) allows claiming pending messages while reading.
/// When CLAIM is specified with id ">", both idle pending messages and new messages are returned.
pub fn cmd_xreadgroup(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(7)?;

        let mut idx = 0;

        // Expect GROUP keyword
        if cmd.get_str(idx)?.to_uppercase() != "GROUP" {
            return Err(CommandError::SyntaxError.into());
        }
        idx += 1;

        let group_name = cmd.args[idx].clone();
        idx += 1;

        let consumer_name = cmd.args[idx].clone();
        idx += 1;

        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;
        let mut noack = false;
        let mut _claim_idle: Option<u64> = None; // Redis 8.4+ CLAIM option

        // Parse options
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                    idx += 1;
                }
                "BLOCK" => {
                    idx += 1;
                    block_ms = Some(cmd.get_u64(idx)?);
                    idx += 1;
                }
                "NOACK" => {
                    noack = true;
                    idx += 1;
                }
                "CLAIM" => {
                    // Redis 8.4+ inline claiming
                    idx += 1;
                    _claim_idle = Some(cmd.get_u64(idx)?);
                    idx += 1;
                }
                "STREAMS" => {
                    idx += 1;
                    break;
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
        }

        // Parse keys and IDs
        let remaining = cmd.args.len() - idx;
        if remaining == 0 || remaining % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "XREADGROUP".to_string(),
            }
            .into());
        }

        let num_keys = remaining / 2;
        let keys: Vec<Key> = cmd.args[idx..idx + num_keys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let requested_ids: Vec<&str> = cmd.args[idx + num_keys..]
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or("0"))
            .collect();

        let ids: Vec<StreamId> = requested_ids
            .iter()
            .map(|s| {
                match *s {
                    ">" => StreamId::max(),         // Read new messages only
                    "0" | "0-0" => StreamId::min(), // Read all pending
                    _ => match StreamId::parse(s) {
                        Some(StreamIdParsed::Exact(id)) => id,
                        Some(StreamIdParsed::Partial { ms, seq }) => {
                            StreamId::new(ms, seq.unwrap_or(0))
                        }
                        _ => StreamId::min(),
                    },
                }
            })
            .collect();

        // Calculate deadline for blocking
        let deadline =
            block_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));

        // Blocking loop - keep trying until we get results or timeout
        loop {
            // Read from each stream using read_group for proper PEL tracking
            let mut results = Vec::new();

            for (key, after_id) in keys.iter().zip(ids.iter()) {
                if let Some(v) = db.get_typed(key, ValueType::Stream)? {
                    let stream = v
                        .as_stream()
                        .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                    let mut guard = stream.write();

                    // Use read_group for proper consumer group handling with PEL
                    if let Some(entries) =
                        guard.read_group(&group_name, &consumer_name, *after_id, count, noack)
                    {
                        if !entries.is_empty() {
                            let stream_entries: Vec<Frame> = entries
                                .into_iter()
                                .map(|entry| {
                                    let fields: Vec<Frame> = entry
                                        .fields
                                        .iter()
                                        .flat_map(|(k, v)| {
                                            vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())]
                                        })
                                        .collect();
                                    Frame::Array(vec![
                                        Frame::bulk(entry.id.to_string()),
                                        Frame::Array(fields),
                                    ])
                                })
                                .collect();

                            results.push(Frame::Array(vec![
                                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                                Frame::Array(stream_entries),
                            ]));
                        }
                    } else {
                        // Consumer group doesn't exist
                        return Ok(Frame::Error(format!(
                            "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP",
                            std::str::from_utf8(key.as_bytes()).unwrap_or("<invalid>"),
                            std::str::from_utf8(&group_name).unwrap_or("<invalid>")
                        )));
                    }
                }
            }

            // If we have results, return them
            if !results.is_empty() {
                return Ok(Frame::Array(results));
            }

            // If not blocking, return null immediately
            if block_ms.is_none() {
                return Ok(Frame::Null);
            }

            // Check if we've exceeded the deadline
            if let Some(deadline) = deadline {
                if std::time::Instant::now() >= deadline {
                    return Ok(Frame::Null);
                }
            }

            // Sleep briefly before retrying (100ms or remaining time, whichever is less)
            let sleep_duration = if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                std::cmp::min(remaining, std::time::Duration::from_millis(100))
            } else {
                std::time::Duration::from_millis(100)
            };

            if sleep_duration.is_zero() {
                return Ok(Frame::Null);
            }

            tokio::time::sleep(sleep_duration).await;
        }
    })
}

/// XDELEX key [COUNT count] [IDLE ms] [TIME unix-ms] [KEEPREF | DELREF] [ACKED]
/// Delete entries based on time criteria (Redis 8.2+).
/// Deletes entries older than a specified time or idle time.
/// Options:
///   KEEPREF - Keep consumer group references after deletion
///   DELREF - Delete consumer group references (default)
///   ACKED - Only delete acknowledged entries
pub fn cmd_xdelex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let key = Key::from(cmd.args[0].clone());
        let mut count: Option<usize> = None;
        let mut idle_ms: Option<u64> = None;
        let mut time_ms: Option<u64> = None;
        let mut _keep_ref = false; // KEEPREF option
        let mut _del_ref = true; // DELREF is default
        let mut _acked_only = false; // ACKED option

        // Parse options
        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    count = Some(cmd.get_u64(i)? as usize);
                    i += 1;
                }
                "IDLE" => {
                    i += 1;
                    idle_ms = Some(cmd.get_u64(i)?);
                    i += 1;
                }
                "TIME" => {
                    i += 1;
                    time_ms = Some(cmd.get_u64(i)?);
                    i += 1;
                }
                "KEEPREF" => {
                    _keep_ref = true;
                    _del_ref = false;
                    i += 1;
                }
                "DELREF" => {
                    _del_ref = true;
                    _keep_ref = false;
                    i += 1;
                }
                "ACKED" => {
                    _acked_only = true;
                    i += 1;
                }
                _ => return Err(CommandError::SyntaxError.into()),
            }
        }

        // Get current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Calculate threshold time
        let threshold_ms = if let Some(t) = time_ms {
            t
        } else if let Some(idle) = idle_ms {
            now.saturating_sub(idle)
        } else {
            // If no time criteria specified, delete nothing
            return Ok(Frame::Integer(0));
        };

        let deleted = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
                let mut guard = stream.write();

                // Find entries older than threshold and delete them
                let threshold_id = StreamId::new(threshold_ms, 0);
                let old_entries = guard.range(StreamId::min(), threshold_id);

                let ids_to_delete: Vec<StreamId> = if let Some(max_count) = count {
                    old_entries
                        .into_iter()
                        .take(max_count)
                        .map(|e| e.id)
                        .collect()
                } else {
                    old_entries.into_iter().map(|e| e.id).collect()
                };

                guard.delete(&ids_to_delete)
            }
            None => 0,
        };

        Ok(Frame::Integer(deleted as i64))
    })
}

/// XACKDEL key group [KEEPREF | DELREF] [ACKED] id [id ...]
/// Atomically acknowledge and delete messages (Redis 8.2+).
/// Combines XACK and XDEL into a single atomic operation.
/// Options:
///   KEEPREF - Keep consumer group references after deletion
///   DELREF - Delete consumer group references (default)
///   ACKED - Only delete already acknowledged entries
pub fn cmd_xackdel(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let group_name = cmd.args[1].clone();

        let mut _keep_ref = false;
        let mut _del_ref = true;
        let mut _acked_only = false;
        let mut id_start_idx = 2;

        // Parse optional flags before IDs
        while id_start_idx < cmd.args.len() {
            let arg = cmd.get_str(id_start_idx)?.to_uppercase();
            match arg.as_str() {
                "KEEPREF" => {
                    _keep_ref = true;
                    _del_ref = false;
                    id_start_idx += 1;
                }
                "DELREF" => {
                    _del_ref = true;
                    _keep_ref = false;
                    id_start_idx += 1;
                }
                "ACKED" => {
                    _acked_only = true;
                    id_start_idx += 1;
                }
                _ => break, // Must be an ID
            }
        }

        // Parse IDs to acknowledge and delete
        let ids: Vec<StreamId> = cmd.args[id_start_idx..]
            .iter()
            .filter_map(|b| {
                let s = std::str::from_utf8(b).ok()?;
                match StreamId::parse(s) {
                    Some(StreamIdParsed::Exact(id)) => Some(id),
                    Some(StreamIdParsed::Partial { ms, seq }) => {
                        Some(StreamId::new(ms, seq.unwrap_or(0)))
                    }
                    _ => None,
                }
            })
            .collect();

        // Atomically acknowledge and delete the entries
        let deleted = match db.get_typed(&key, ValueType::Stream)? {
            Some(v) => {
                let stream = v
                    .as_stream()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_typed"));
                let mut guard = stream.write();

                // First, acknowledge entries in the consumer group (remove from PEL)
                if let Some(group) = guard.get_group_mut(&group_name) {
                    group.ack(&ids);
                }

                // Then delete the entries from the stream
                guard.delete(&ids)
            }
            None => 0,
        };

        Ok(Frame::Integer(deleted as i64))
    })
}

/// XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
/// Sets the last-generated-id of a stream.
pub fn cmd_xsetid(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let id_str = cmd.get_str(1)?;

        let new_last_id = match StreamId::parse(id_str) {
            Some(StreamIdParsed::Exact(id)) => id,
            Some(StreamIdParsed::Partial { ms, seq }) => StreamId::new(ms, seq.unwrap_or(0)),
            _ => return Err(CommandError::StreamInvalidId.into()),
        };

        // Parse optional arguments
        let mut _entries_added: Option<u64> = None;
        let mut _max_deleted_id: Option<StreamId> = None;

        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "ENTRIESADDED" => {
                    i += 1;
                    _entries_added = Some(cmd.get_u64(i)?);
                    i += 1;
                }
                "MAXDELETEDID" => {
                    i += 1;
                    let max_del_str = cmd.get_str(i)?;
                    _max_deleted_id = match StreamId::parse(max_del_str) {
                        Some(StreamIdParsed::Exact(id)) => Some(id),
                        Some(StreamIdParsed::Partial { ms, seq }) => {
                            Some(StreamId::new(ms, seq.unwrap_or(0)))
                        }
                        _ => return Err(CommandError::StreamInvalidId.into()),
                    };
                    i += 1;
                }
                _ => return Err(CommandError::SyntaxError.into()),
            }
        }

        // Get or create stream
        let value = get_or_create_stream(&db, &key)?;
        let stream = value
            .as_stream()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_stream"));
        let mut guard = stream.write();

        // The new ID must be >= current last ID
        let current_last = guard.last_id();
        if new_last_id < current_last {
            return Err(CommandError::StreamIdTooSmall.into());
        }

        // Set the new last ID
        guard.set_last_id(new_last_id);

        drop(guard);
        db.set(key, value);

        Ok(Frame::ok())
    })
}
