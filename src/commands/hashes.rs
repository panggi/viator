//! Hash command implementations.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ValueType, ViatorValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Helper to get or create a hash
/// Uses fast path that skips LRU tracking for better performance.
fn get_or_create_hash(db: &Db, key: &Key) -> Result<ViatorValue> {
    match db.get_fast(key) {
        Some(v) if v.is_hash() => Ok(v),
        Some(_) => Err(CommandError::WrongType.into()),
        None => Ok(ViatorValue::new_hash()),
    }
}

/// HSET key field value [field value ...]
/// Fast path: skips LRU tracking for better performance.
pub fn cmd_hset(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if (cmd.args.len() - 1) % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "HSET".to_string(),
            }
            .into());
        }

        let key = Key::from(cmd.args[0].clone());
        let value = get_or_create_hash(&db, &key)?;

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash = hash.write();
        let mut added = 0;

        for chunk in cmd.args[1..].chunks(2) {
            let field = chunk[0].clone();
            let val = chunk[1].clone();
            if hash.insert(field, val).is_none() {
                added += 1;
            }
        }

        drop(hash);
        // Use fast path for writes
        db.set_fast(key, value);

        Ok(Frame::Integer(added))
    })
}

/// HGET key field
pub fn cmd_hget(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let field = &cmd.args[1];

        let value = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
            .read();
        match hash.get(field.as_ref()) {
            Some(v) => Ok(Frame::Bulk(v.clone())),
            None => Ok(Frame::Null),
        }
    })
}

/// HMSET key field value [field value ...] (deprecated, use HSET)
pub fn cmd_hmset(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd_hset(cmd, db, client).await?;
        Ok(Frame::ok())
    })
}

/// HMGET key field [field ...]
pub fn cmd_hmget(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = db.get_typed(&key, ValueType::Hash)?;

        let frames: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|field| match &value {
                Some(v) => {
                    let hash = v
                        .as_hash()
                        .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
                        .read();
                    match hash.get(field.as_ref()) {
                        Some(v) => Frame::Bulk(v.clone()),
                        None => Frame::Null,
                    }
                }
                None => Frame::Null,
            })
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// HDEL key field [field ...]
pub fn cmd_hdel(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash_guard = hash.write();
        let deleted = cmd.args[1..]
            .iter()
            .filter(|f| hash_guard.remove(f.as_ref()).is_some())
            .count();
        let is_empty = hash_guard.is_empty();
        drop(hash_guard);

        // Auto-delete empty hash (Redis behavior)
        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Integer(deleted as i64))
    })
}

/// HEXISTS key field
pub fn cmd_hexists(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let field = &cmd.args[1];

        let exists = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v
                .as_hash()
                .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
                .read()
                .contains_key(field.as_ref()),
            None => false,
        };

        Ok(Frame::Integer(if exists { 1 } else { 0 }))
    })
}

/// HLEN key
pub fn cmd_hlen(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let len = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v
                .as_hash()
                .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
                .read()
                .len(),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// HKEYS key
pub fn cmd_hkeys(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let keys = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v
                .as_hash()
                .unwrap()
                .read()
                .keys()
                .cloned()
                .map(Frame::Bulk)
                .collect(),
            None => vec![],
        };

        Ok(Frame::Array(keys))
    })
}

/// HVALS key
pub fn cmd_hvals(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let vals = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v
                .as_hash()
                .unwrap()
                .read()
                .values()
                .cloned()
                .map(Frame::Bulk)
                .collect(),
            None => vec![],
        };

        Ok(Frame::Array(vals))
    })
}

/// HGETALL key
pub fn cmd_hgetall(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let pairs = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => {
                let hash = v
                    .as_hash()
                    .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
                    .read();
                hash.iter()
                    .flat_map(|(k, v)| vec![Frame::Bulk(k.clone()), Frame::Bulk(v.clone())])
                    .collect()
            }
            None => vec![],
        };

        Ok(Frame::Array(pairs))
    })
}

/// HINCRBY key field increment
pub fn cmd_hincrby(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let field = cmd.args[1].clone();
        let increment = cmd.get_i64(2)?;

        let value = get_or_create_hash(&db, &key)?;
        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash = hash.write();

        let current = match hash.get(&field) {
            Some(v) => {
                let s = std::str::from_utf8(v).map_err(|_| CommandError::NotInteger)?;
                s.parse::<i64>().map_err(|_| CommandError::NotInteger)?
            }
            None => 0,
        };

        let new_value = current
            .checked_add(increment)
            .ok_or(CommandError::OutOfRange)?;

        hash.insert(field, Bytes::from(new_value.to_string()));
        drop(hash);

        db.set(key, value);
        Ok(Frame::Integer(new_value))
    })
}

/// HINCRBYFLOAT key field increment
pub fn cmd_hincrbyfloat(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let field = cmd.args[1].clone();
        let increment = cmd.get_f64(2)?;

        let value = get_or_create_hash(&db, &key)?;
        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash = hash.write();

        let current = match hash.get(&field) {
            Some(v) => {
                let s = std::str::from_utf8(v).map_err(|_| CommandError::NotFloat)?;
                s.parse::<f64>().map_err(|_| CommandError::NotFloat)?
            }
            None => 0.0,
        };

        let new_value = current + increment;
        if new_value.is_infinite() || new_value.is_nan() {
            return Err(CommandError::OutOfRange.into());
        }

        let value_str = format!("{new_value}");
        hash.insert(field, Bytes::from(value_str.clone()));
        drop(hash);

        db.set(key, value);
        Ok(Frame::Bulk(Bytes::from(value_str)))
    })
}

/// HSETNX key field value
pub fn cmd_hsetnx(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let field = cmd.args[1].clone();
        let val = cmd.args[2].clone();

        let value = get_or_create_hash(&db, &key)?;
        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash = hash.write();

        let inserted = if let std::collections::hash_map::Entry::Vacant(e) = hash.entry(field) {
            e.insert(val);
            true
        } else {
            false
        };

        drop(hash);
        db.set(key, value);

        Ok(Frame::Integer(if inserted { 1 } else { 0 }))
    })
}

/// HSTRLEN key field
pub fn cmd_hstrlen(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let field = &cmd.args[1];

        let len = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v
                .as_hash()
                .unwrap()
                .read()
                .get(field.as_ref())
                .map(|v| v.len())
                .unwrap_or(0),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
pub fn cmd_hscan(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let cursor = cmd.get_u64(1)? as usize;

        let mut pattern: Option<Bytes> = None;
        let mut count: usize = 10;
        let mut no_values = false;

        // Parse options
        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "MATCH" => {
                    i += 1;
                    pattern = Some(cmd.args.get(i).cloned().ok_or(CommandError::SyntaxError)?);
                }
                "COUNT" => {
                    i += 1;
                    count = cmd.get_u64(i)? as usize;
                }
                "NOVALUES" => {
                    no_values = true;
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
            i += 1;
        }

        let value = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v,
            None => {
                return Ok(Frame::Array(vec![
                    Frame::Bulk(Bytes::from("0")),
                    Frame::Array(vec![]),
                ]));
            }
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
            .read();
        let entries: Vec<(Bytes, Bytes)> =
            hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        // Simple scan implementation - just paginate through entries
        let total = entries.len();
        let start = cursor;
        let end = std::cmp::min(start + count, total);

        let mut result_entries = Vec::new();
        for i in start..end {
            let (field, value) = &entries[i];
            // Apply pattern filter if specified (only on field names)
            let matches = pattern
                .as_ref()
                .map(|p| match_pattern(p, field))
                .unwrap_or(true);
            if matches {
                result_entries.push(Frame::Bulk(field.clone()));
                if !no_values {
                    result_entries.push(Frame::Bulk(value.clone()));
                }
            }
        }

        let next_cursor = if end >= total { 0 } else { end };

        Ok(Frame::Array(vec![
            Frame::Bulk(Bytes::from(next_cursor.to_string())),
            Frame::Array(result_entries),
        ]))
    })
}

/// HRANDFIELD key [count [WITHVALUES]]
pub fn cmd_hrandfield(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        use rand::seq::SliceRandom;

        let key = Key::from(cmd.args[0].clone());

        let count = if cmd.args.len() > 1 {
            Some(cmd.get_i64(1)?)
        } else {
            None
        };

        let with_values = if cmd.args.len() > 2 {
            let opt = cmd.get_str(2)?.to_uppercase();
            opt == "WITHVALUES"
        } else {
            false
        };

        let value = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v,
            None => {
                return Ok(if count.is_some() {
                    Frame::Array(vec![])
                } else {
                    Frame::Null
                });
            }
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
            .read();
        let fields: Vec<Bytes> = hash.keys().cloned().collect();

        if fields.is_empty() {
            return Ok(if count.is_some() {
                Frame::Array(vec![])
            } else {
                Frame::Null
            });
        }

        let mut rng = rand::thread_rng();

        match count {
            Some(count) => {
                let allow_duplicates = count < 0;
                let count = count.unsigned_abs() as usize;

                let selected_fields: Vec<Bytes> = if allow_duplicates {
                    // With duplicates allowed (negative count)
                    (0..count)
                        .map(|_| fields.choose(&mut rng).unwrap().clone())
                        .collect()
                } else {
                    // Without duplicates
                    let take = std::cmp::min(count, fields.len());
                    let mut shuffled = fields.clone();
                    shuffled.shuffle(&mut rng);
                    shuffled.into_iter().take(take).collect()
                };

                if with_values {
                    let mut result = Vec::new();
                    for field in selected_fields {
                        if let Some(val) = hash.get(&field) {
                            result.push(Frame::Bulk(field));
                            result.push(Frame::Bulk(val.clone()));
                        }
                    }
                    Ok(Frame::Array(result))
                } else {
                    Ok(Frame::Array(
                        selected_fields.into_iter().map(Frame::Bulk).collect(),
                    ))
                }
            }
            None => match fields.choose(&mut rng) {
                Some(field) => Ok(Frame::Bulk(field.clone())),
                None => Ok(Frame::Null),
            },
        }
    })
}

/// HGETDEL key field [field ...]
pub fn cmd_hgetdel(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v,
            None => {
                // Return array of nulls
                let nulls: Vec<Frame> = cmd.args[1..].iter().map(|_| Frame::Null).collect();
                return Ok(Frame::Array(nulls));
            }
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash_guard = hash.write();
        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|field| match hash_guard.remove(field.as_ref()) {
                Some(v) => Frame::Bulk(v),
                None => Frame::Null,
            })
            .collect();

        let is_empty = hash_guard.is_empty();
        drop(hash_guard);

        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Array(results))
    })
}

/// HGETEX key [EX seconds | PX milliseconds | EXAT unix-time | PXAT unix-time-ms | PERSIST] field [field ...]
/// Note: Hash field-level TTL is simulated (returns values but doesn't actually expire fields)
pub fn cmd_hgetex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());

        // Parse TTL options (not actually implemented, just parsed for compatibility)
        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    i += 2; // Skip option and value
                }
                "PERSIST" => {
                    i += 1;
                }
                _ => break, // Start of field names
            }
        }

        let value = match db.get_typed(&key, ValueType::Hash)? {
            Some(v) => v,
            None => {
                let nulls: Vec<Frame> = cmd.args[i..].iter().map(|_| Frame::Null).collect();
                return Ok(Frame::Array(nulls));
            }
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"))
            .read();
        let results: Vec<Frame> = cmd.args[i..]
            .iter()
            .map(|field| match hash.get(field.as_ref()) {
                Some(v) => Frame::Bulk(v.clone()),
                None => Frame::Null,
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// HSETEX key [FNX | FXX] [EX seconds | PX milliseconds | EXAT unix-time | PXAT unix-time-ms | KEEPTTL] field value [field value ...]
/// Note: Hash field-level TTL is simulated
pub fn cmd_hsetex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());

        // Parse options
        let mut fnx = false;
        let mut fxx = false;
        let mut i = 1;

        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "FNX" => {
                    fnx = true;
                    i += 1;
                }
                "FXX" => {
                    fxx = true;
                    i += 1;
                }
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    i += 2;
                }
                "KEEPTTL" => {
                    i += 1;
                }
                _ => break,
            }
        }

        // Remaining args are field-value pairs
        let pairs = &cmd.args[i..];
        if pairs.len() % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "HSETEX".to_string(),
            }
            .into());
        }

        let value = match db.get(&key) {
            Some(v) if v.is_hash() => v,
            Some(_) => return Err(CommandError::WrongType.into()),
            None => ViatorValue::new_hash(),
        };

        let hash = value
            .as_hash()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_hash"));
        let mut hash_guard = hash.write();
        let mut added = 0i64;

        for chunk in pairs.chunks(2) {
            let field = chunk[0].clone();
            let val = chunk[1].clone();
            let field_exists = hash_guard.contains_key(&field);

            if fnx && field_exists {
                continue;
            }
            if fxx && !field_exists {
                continue;
            }

            if hash_guard.insert(field, val).is_none() {
                added += 1;
            }
        }

        drop(hash_guard);
        db.set(key, value);

        Ok(Frame::Integer(added))
    })
}

/// HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
/// Stub implementation - returns 1 for each field (success)
pub fn cmd_hexpire(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;

        let key = Key::from(cmd.args[0].clone());

        // Check if hash exists
        if db.get_typed(&key, ValueType::Hash)?.is_none() {
            return Ok(Frame::Array(vec![]));
        }

        // Find FIELDS keyword and count fields
        let mut fields_idx = None;
        for (i, _arg) in cmd.args.iter().enumerate() {
            if cmd
                .get_str(i)
                .map(|s| s.to_uppercase() == "FIELDS")
                .unwrap_or(false)
            {
                fields_idx = Some(i);
                break;
            }
        }

        let fields_idx = fields_idx.ok_or(CommandError::SyntaxError)?;
        let _numfields = cmd.get_u64(fields_idx + 1)? as usize;

        // Return 1 for each field (stub: success)
        let results: Vec<Frame> = cmd.args[fields_idx + 2..]
            .iter()
            .map(|_| Frame::Integer(1))
            .collect();

        Ok(Frame::Array(results))
    })
}

/// HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
pub fn cmd_hexpireat(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_hexpire(cmd, db, client)
}

/// HEXPIRETIME key FIELDS numfields field [field ...]
/// Returns -1 for each field (no expiration set - stub)
pub fn cmd_hexpiretime(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());

        if db.get_typed(&key, ValueType::Hash)?.is_none() {
            return Ok(Frame::Array(vec![]));
        }

        // Find FIELDS keyword
        let mut fields_idx = None;
        for (i, _) in cmd.args.iter().enumerate() {
            if cmd
                .get_str(i)
                .map(|s| s.to_uppercase() == "FIELDS")
                .unwrap_or(false)
            {
                fields_idx = Some(i);
                break;
            }
        }

        let fields_idx = fields_idx.ok_or(CommandError::SyntaxError)?;

        // Return -1 for each field (no expiration)
        let results: Vec<Frame> = cmd.args[fields_idx + 2..]
            .iter()
            .map(|_| Frame::Integer(-1))
            .collect();

        Ok(Frame::Array(results))
    })
}

/// HPEXPIRE key milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
pub fn cmd_hpexpire(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_hexpire(cmd, db, client)
}

/// HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
pub fn cmd_hpexpireat(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_hexpire(cmd, db, client)
}

/// HPEXPIRETIME key FIELDS numfields field [field ...]
pub fn cmd_hpexpiretime(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_hexpiretime(cmd, db, client)
}

/// HTTL key FIELDS numfields field [field ...]
/// Returns -1 for each field (no expiration - stub)
pub fn cmd_httl(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_hexpiretime(cmd, db, client)
}

/// HPTTL key FIELDS numfields field [field ...]
pub fn cmd_hpttl(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_hexpiretime(cmd, db, client)
}

/// HPERSIST key FIELDS numfields field [field ...]
/// Returns 1 for each field (success - stub, no actual expiration to remove)
pub fn cmd_hpersist(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());

        if db.get_typed(&key, ValueType::Hash)?.is_none() {
            return Ok(Frame::Array(vec![]));
        }

        // Find FIELDS keyword
        let mut fields_idx = None;
        for (i, _) in cmd.args.iter().enumerate() {
            if cmd
                .get_str(i)
                .map(|s| s.to_uppercase() == "FIELDS")
                .unwrap_or(false)
            {
                fields_idx = Some(i);
                break;
            }
        }

        let fields_idx = fields_idx.ok_or(CommandError::SyntaxError)?;

        // Return -1 for each field (no expiration to remove)
        let results: Vec<Frame> = cmd.args[fields_idx + 2..]
            .iter()
            .map(|_| Frame::Integer(-1))
            .collect();

        Ok(Frame::Array(results))
    })
}

/// Simple glob pattern matching for SCAN commands
fn match_pattern(pattern: &Bytes, text: &Bytes) -> bool {
    let pattern = pattern.as_ref();
    let text = text.as_ref();

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
