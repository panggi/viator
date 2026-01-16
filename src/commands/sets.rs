//! Set command implementations.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ValueType, ViatorSet, ViatorValue};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

fn get_or_create_set(db: &Db, key: &Key) -> Result<ViatorValue> {
    match db.get(key) {
        Some(v) if v.is_set() => Ok(v),
        Some(_) => Err(CommandError::WrongType.into()),
        None => Ok(ViatorValue::new_set()),
    }
}

/// SADD key member [member ...]
pub fn cmd_sadd(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let value = get_or_create_set(&db, &key)?;

        let set = value
            .as_set()
            .expect("type guaranteed by get_or_create_set");
        let mut set = set.write();
        let added = set.add_multi(cmd.args[1..].iter().cloned());

        drop(set);
        db.set(key, value);

        Ok(Frame::Integer(added as i64))
    })
}

/// SREM key member [member ...]
pub fn cmd_srem(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let set = value
            .as_set()
            .expect("type guaranteed by get_or_create_set");
        let mut set_guard = set.write();
        let removed = set_guard.remove_multi(cmd.args[1..].iter().map(|b| b.as_ref()));
        let is_empty = set_guard.is_empty();
        drop(set_guard);

        // Auto-delete empty set (Redis behavior)
        if is_empty {
            db.delete(&key);
        }

        Ok(Frame::Integer(removed as i64))
    })
}

/// SMEMBERS key
pub fn cmd_smembers(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let members = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v
                .as_set()
                .unwrap()
                .read()
                .members()
                .into_iter()
                .map(|m| Frame::Bulk(m.clone()))
                .collect(),
            None => vec![],
        };

        Ok(Frame::Array(members))
    })
}

/// SISMEMBER key member
pub fn cmd_sismember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let member = &cmd.args[1];

        let is_member = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v
                .as_set()
                .expect("type guaranteed by get_or_create_set")
                .read()
                .contains(member),
            None => false,
        };

        Ok(Frame::Integer(if is_member { 1 } else { 0 }))
    })
}

/// SCARD key
pub fn cmd_scard(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let len = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v
                .as_set()
                .expect("type guaranteed by get_or_create_set")
                .read()
                .len(),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// SPOP key [count]
pub fn cmd_spop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let count = if cmd.args.len() > 1 {
            Some(cmd.get_u64(1)? as usize)
        } else {
            None
        };

        let value = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let set = value
            .as_set()
            .expect("type guaranteed by get_or_create_set");

        let result = match count {
            Some(count) => {
                let mut set_guard = set.write();
                let popped = set_guard.pop_multi(count);
                let is_empty = set_guard.is_empty();
                drop(set_guard);

                // Auto-delete empty set (Redis behavior)
                if is_empty {
                    db.delete(&key);
                }

                if popped.is_empty() {
                    Frame::Array(vec![])
                } else {
                    Frame::Array(popped.into_iter().map(Frame::Bulk).collect())
                }
            }
            None => {
                let mut set_guard = set.write();
                let popped = set_guard.pop();
                let is_empty = set_guard.is_empty();
                drop(set_guard);

                // Auto-delete empty set (Redis behavior)
                if is_empty {
                    db.delete(&key);
                }

                match popped {
                    Some(v) => Frame::Bulk(v),
                    None => Frame::Null,
                }
            }
        };

        Ok(result)
    })
}

/// SRANDMEMBER key [count]
pub fn cmd_srandmember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let count = if cmd.args.len() > 1 {
            Some(cmd.get_i64(1)?)
        } else {
            None
        };

        let value = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v,
            None => {
                return Ok(if count.is_some() {
                    Frame::Array(vec![])
                } else {
                    Frame::Null
                });
            }
        };

        let set = value
            .as_set()
            .expect("type guaranteed by get_or_create_set")
            .read();

        match count {
            Some(count) => {
                let members = set.random_members(count);
                Ok(Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(m.clone()))
                        .collect(),
                ))
            }
            None => match set.random_member() {
                Some(v) => Ok(Frame::Bulk(v.clone())),
                None => Ok(Frame::Null),
            },
        }
    })
}

/// SMOVE source destination member
pub fn cmd_smove(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let src_key = Key::from(cmd.args[0].clone());
        let dst_key = Key::from(cmd.args[1].clone());
        let member = &cmd.args[2];

        let src = match db.get_typed(&src_key, ValueType::Set)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let dst = get_or_create_set(&db, &dst_key)?;

        let src_set = src.as_set().expect("type guaranteed by get_or_create_set");
        let dst_set = dst.as_set().expect("type guaranteed by get_or_create_set");

        let moved = src_set.write().move_to(&mut dst_set.write(), member);

        if moved {
            db.set(dst_key, dst);
        }

        Ok(Frame::Integer(if moved { 1 } else { 0 }))
    })
}

/// SINTER key [key ...]
pub fn cmd_sinter(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let sets = collect_sets(&db, &cmd.args)?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::intersection_multi(&set_refs);
        let frames: Vec<Frame> = result.into_iter().map(Frame::Bulk).collect();

        Ok(Frame::Array(frames))
    })
}

/// SUNION key [key ...]
pub fn cmd_sunion(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let sets = collect_sets(&db, &cmd.args)?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::union_multi(&set_refs);
        let frames: Vec<Frame> = result.into_iter().map(Frame::Bulk).collect();

        Ok(Frame::Array(frames))
    })
}

/// SDIFF key [key ...]
pub fn cmd_sdiff(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let sets = collect_sets(&db, &cmd.args)?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::difference_multi(&set_refs);
        let frames: Vec<Frame> = result.into_iter().map(Frame::Bulk).collect();

        Ok(Frame::Array(frames))
    })
}

/// SINTERSTORE destination key [key ...]
pub fn cmd_sinterstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let sets = collect_sets(&db, &cmd.args[1..])?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::intersection_multi(&set_refs);
        let count = result.len() as i64;

        if result.is_empty() {
            db.delete(&dest_key);
        } else {
            let dest_value = ViatorValue::new_set();
            let dest_set = dest_value
                .as_set()
                .expect("type guaranteed by get_or_create_set");
            let mut guard = dest_set.write();
            for member in result {
                guard.add(member);
            }
            drop(guard);
            db.set(dest_key, dest_value);
        }

        Ok(Frame::Integer(count))
    })
}

/// SUNIONSTORE destination key [key ...]
pub fn cmd_sunionstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let sets = collect_sets(&db, &cmd.args[1..])?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::union_multi(&set_refs);
        let count = result.len() as i64;

        if result.is_empty() {
            db.delete(&dest_key);
        } else {
            let dest_value = ViatorValue::new_set();
            let dest_set = dest_value
                .as_set()
                .expect("type guaranteed by get_or_create_set");
            let mut guard = dest_set.write();
            for member in result {
                guard.add(member);
            }
            drop(guard);
            db.set(dest_key, dest_value);
        }

        Ok(Frame::Integer(count))
    })
}

/// SDIFFSTORE destination key [key ...]
pub fn cmd_sdiffstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let sets = collect_sets(&db, &cmd.args[1..])?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::difference_multi(&set_refs);
        let count = result.len() as i64;

        if result.is_empty() {
            db.delete(&dest_key);
        } else {
            let dest_value = ViatorValue::new_set();
            let dest_set = dest_value
                .as_set()
                .expect("type guaranteed by get_or_create_set");
            let mut guard = dest_set.write();
            for member in result {
                guard.add(member);
            }
            drop(guard);
            db.set(dest_key, dest_value);
        }

        Ok(Frame::Integer(count))
    })
}

/// Helper to collect sets from multiple keys
fn collect_sets(db: &Db, keys: &[bytes::Bytes]) -> Result<Vec<ViatorSet>> {
    keys.iter()
        .map(|k| {
            let key = Key::from(k.clone());
            match db.get_typed(&key, ValueType::Set)? {
                Some(v) => Ok(v
                    .as_set()
                    .expect("type guaranteed by get_or_create_set")
                    .read()
                    .clone()),
                None => Ok(ViatorSet::new()),
            }
        })
        .collect()
}

/// SMISMEMBER key member [member ...]
pub fn cmd_smismember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = db.get_typed(&key, ValueType::Set)?;

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|member| {
                let is_member = value
                    .as_ref()
                    .map(|v| {
                        v.as_set()
                            .expect("type guaranteed by get_or_create_set")
                            .read()
                            .contains(member)
                    })
                    .unwrap_or(false);
                Frame::Integer(if is_member { 1 } else { 0 })
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
pub fn cmd_sscan(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let cursor = cmd.get_u64(1)? as usize;

        let mut pattern: Option<bytes::Bytes> = None;
        let mut count: usize = 10;

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
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
            i += 1;
        }

        let value = match db.get_typed(&key, ValueType::Set)? {
            Some(v) => v,
            None => {
                return Ok(Frame::Array(vec![
                    Frame::Bulk(bytes::Bytes::from("0")),
                    Frame::Array(vec![]),
                ]));
            }
        };

        let set = value
            .as_set()
            .expect("type guaranteed by get_or_create_set")
            .read();
        let members: Vec<bytes::Bytes> = set.members().into_iter().cloned().collect();

        // Simple scan implementation - just paginate through members
        let total = members.len();
        let start = cursor;
        let end = std::cmp::min(start + count, total);

        let mut result_members = Vec::new();
        for i in start..end {
            let member = &members[i];
            // Apply pattern filter if specified
            let matches = pattern
                .as_ref()
                .map(|p| match_pattern(p, member))
                .unwrap_or(true);
            if matches {
                result_members.push(Frame::Bulk(member.clone()));
            }
        }

        let next_cursor = if end >= total { 0 } else { end };

        Ok(Frame::Array(vec![
            Frame::Bulk(bytes::Bytes::from(next_cursor.to_string())),
            Frame::Array(result_members),
        ]))
    })
}

/// SINTERCARD numkeys key [key ...] [LIMIT limit]
pub fn cmd_sintercard(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let numkeys = cmd.get_u64(0)? as usize;

        if cmd.args.len() < numkeys + 1 {
            return Err(CommandError::WrongArity {
                command: "SINTERCARD".to_string(),
            }
            .into());
        }

        let keys: Vec<bytes::Bytes> = cmd.args[1..=numkeys].to_vec();

        // Parse LIMIT option
        let mut limit: Option<usize> = None;
        let mut i = numkeys + 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            if opt == "LIMIT" {
                i += 1;
                limit = Some(cmd.get_u64(i)? as usize);
            }
            i += 1;
        }

        let sets = collect_sets(&db, &keys)?;
        let set_refs: Vec<&ViatorSet> = sets.iter().collect();

        let result = ViatorSet::intersection_multi(&set_refs);
        let count = match limit {
            Some(l) if l > 0 => std::cmp::min(result.len(), l),
            _ => result.len(),
        };

        Ok(Frame::Integer(count as i64))
    })
}

/// Simple glob pattern matching for SCAN commands
fn match_pattern(pattern: &bytes::Bytes, text: &bytes::Bytes) -> bool {
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
