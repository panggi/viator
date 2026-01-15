//! List command implementations.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ViatorValue, ValueType};
use crate::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Helper to get or create a list
fn get_or_create_list(db: &Db, key: &Key) -> Result<ViatorValue> {
    match db.get(key) {
        Some(v) if v.is_list() => Ok(v),
        Some(_) => Err(CommandError::WrongType.into()),
        None => Ok(ViatorValue::new_list()),
    }
}

/// LPUSH key element [element ...]
pub fn cmd_lpush(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let value = get_or_create_list(&db, &key)?;

        let list = value.as_list().unwrap();
        let mut list = list.write();

        for arg in cmd.args.iter().skip(1) {
            list.push_front(arg.clone());
        }

        let len = list.len();
        drop(list);

        db.set(key, value);
        Ok(Frame::Integer(len as i64))
    })
}

/// RPUSH key element [element ...]
pub fn cmd_rpush(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let value = get_or_create_list(&db, &key)?;

        let list = value.as_list().unwrap();
        let mut list = list.write();

        for arg in cmd.args.iter().skip(1) {
            list.push_back(arg.clone());
        }

        let len = list.len();
        drop(list);

        db.set(key, value);
        Ok(Frame::Integer(len as i64))
    })
}

/// LPOP key [count]
pub fn cmd_lpop(
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

        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let list = value.as_list().unwrap();
        let mut list = list.write();

        let result = match count {
            Some(count) => {
                let popped = list.pop_front_multi(count);
                if popped.is_empty() {
                    Frame::Null
                } else {
                    let frames: Vec<Frame> = popped.into_iter().map(Frame::Bulk).collect();
                    Frame::Array(frames)
                }
            }
            None => match list.pop_front() {
                Some(v) => Frame::Bulk(v),
                None => Frame::Null,
            },
        };

        // Delete the key if the list is now empty
        let is_empty = list.is_empty();
        drop(list);
        if is_empty {
            db.delete(&key);
        }

        Ok(result)
    })
}

/// RPOP key [count]
pub fn cmd_rpop(
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

        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let list = value.as_list().unwrap();
        let mut list = list.write();

        let result = match count {
            Some(count) => {
                let popped = list.pop_back_multi(count);
                if popped.is_empty() {
                    Frame::Null
                } else {
                    let frames: Vec<Frame> = popped.into_iter().map(Frame::Bulk).collect();
                    Frame::Array(frames)
                }
            }
            None => match list.pop_back() {
                Some(v) => Frame::Bulk(v),
                None => Frame::Null,
            },
        };

        // Delete the key if the list is now empty
        let is_empty = list.is_empty();
        drop(list);
        if is_empty {
            db.delete(&key);
        }

        Ok(result)
    })
}

/// LLEN key
pub fn cmd_llen(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let len = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v.as_list().unwrap().read().len(),
            None => 0,
        };

        Ok(Frame::Integer(len as i64))
    })
}

/// LRANGE key start stop
pub fn cmd_lrange(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let start = cmd.get_i64(1)?;
        let stop = cmd.get_i64(2)?;

        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let list = value.as_list().unwrap().read();
        let range = list.range(start, stop);
        let frames: Vec<Frame> = range.into_iter().map(|b| Frame::Bulk(b.clone())).collect();

        Ok(Frame::Array(frames))
    })
}

/// LINDEX key index
pub fn cmd_lindex(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let index = cmd.get_i64(1)?;

        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let list = value.as_list().unwrap().read();
        match list.get(index) {
            Some(v) => Ok(Frame::Bulk(v.clone())),
            None => Ok(Frame::Null),
        }
    })
}

/// LSET key index element
pub fn cmd_lset(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let index = cmd.get_i64(1)?;
        let element = cmd.args[2].clone();

        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Err(CommandError::NoSuchKey.into()),
        };

        let list = value.as_list().unwrap();
        if list.write().set(index, element) {
            Ok(Frame::ok())
        } else {
            Err(CommandError::IndexOutOfRange.into())
        }
    })
}

/// LTRIM key start stop
pub fn cmd_ltrim(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let start = cmd.get_i64(1)?;
        let stop = cmd.get_i64(2)?;

        if let Some(value) = db.get_typed(&key, ValueType::List)? {
            let list = value.as_list().unwrap();
            list.write().trim(start, stop);
        }

        Ok(Frame::ok())
    })
}

/// LREM key count element
pub fn cmd_lrem(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let count = cmd.get_i64(1)?;
        let element = &cmd.args[2];

        let removed = match db.get_typed(&key, ValueType::List)? {
            Some(value) => {
                let list = value.as_list().unwrap();
                let mut list_guard = list.write();
                let removed = list_guard.remove(count, element);
                let is_empty = list_guard.is_empty();
                drop(list_guard);

                // Auto-delete empty list (Redis behavior)
                if is_empty {
                    db.delete(&key);
                }

                removed
            }
            None => 0,
        };

        Ok(Frame::Integer(removed as i64))
    })
}

/// LINSERT key BEFORE|AFTER pivot element
pub fn cmd_linsert(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let position = cmd.get_str(1)?.to_uppercase();
        let pivot = &cmd.args[2];
        let element = cmd.args[3].clone();

        let before = match position.as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        let result = match db.get_typed(&key, ValueType::List)? {
            Some(value) => {
                let list = value.as_list().unwrap();
                list.write().insert(pivot, element, before)
            }
            None => 0,
        };

        Ok(Frame::Integer(result))
    })
}

/// LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
pub fn cmd_lpos(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let element = &cmd.args[1];

        // Parse options
        let mut rank: i64 = 1;
        let mut count: Option<usize> = None;
        let mut maxlen: Option<usize> = None;

        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "RANK" => {
                    i += 1;
                    rank = cmd.get_i64(i)?;
                    if rank == 0 {
                        return Err(CommandError::SyntaxError.into());
                    }
                }
                "COUNT" => {
                    i += 1;
                    count = Some(cmd.get_u64(i)? as usize);
                }
                "MAXLEN" => {
                    i += 1;
                    maxlen = Some(cmd.get_u64(i)? as usize);
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
            i += 1;
        }

        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(if count.is_some() { Frame::Array(vec![]) } else { Frame::Null }),
        };

        let list = value.as_list().unwrap().read();
        let list_len = list.len();
        let max_iterations = maxlen.unwrap_or(list_len);

        let mut positions: Vec<i64> = Vec::new();
        let mut matches_to_skip = rank.unsigned_abs() as usize - 1;
        let want_count = count.unwrap_or(1);

        if rank > 0 {
            // Forward search (from head to tail)
            for idx in 0..std::cmp::min(list_len, max_iterations) {
                if let Some(elem) = list.get(idx as i64) {
                    if elem == element {
                        if matches_to_skip > 0 {
                            matches_to_skip -= 1;
                        } else {
                            positions.push(idx as i64);
                            if positions.len() >= want_count {
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // Reverse search (from tail to head)
            let start = if maxlen.is_some() {
                list_len.saturating_sub(max_iterations)
            } else {
                0
            };

            for idx in (start..list_len).rev() {
                if let Some(elem) = list.get(idx as i64) {
                    if elem == element {
                        if matches_to_skip > 0 {
                            matches_to_skip -= 1;
                        } else {
                            positions.push(idx as i64);
                            if positions.len() >= want_count {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if count.is_some() {
            // Return array of positions
            Ok(Frame::Array(positions.into_iter().map(Frame::Integer).collect()))
        } else {
            // Return single position or null
            match positions.first() {
                Some(&pos) => Ok(Frame::Integer(pos)),
                None => Ok(Frame::Null),
            }
        }
    })
}

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
pub fn cmd_lmpop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let numkeys = cmd.get_u64(0)? as usize;

        if cmd.args.len() < numkeys + 2 {
            return Err(CommandError::WrongArity {
                command: "LMPOP".to_string(),
            }
            .into());
        }

        let keys: Vec<Key> = cmd.args[1..=numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let direction = cmd.get_str(numkeys + 1)?.to_uppercase();
        let from_left = match direction.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        // Parse optional COUNT
        let mut count: usize = 1;
        let mut i = numkeys + 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            if opt == "COUNT" {
                i += 1;
                count = cmd.get_u64(i)? as usize;
            }
            i += 1;
        }

        // Find first non-empty list
        for key in keys {
            if let Some(value) = db.get_typed(&key, ValueType::List)? {
                let list = value.as_list().unwrap();
                let mut list_guard = list.write();

                if list_guard.is_empty() {
                    continue;
                }

                let popped = if from_left {
                    list_guard.pop_front_multi(count)
                } else {
                    list_guard.pop_back_multi(count)
                };

                let is_empty = list_guard.is_empty();
                drop(list_guard);

                // Auto-delete empty list
                if is_empty {
                    db.delete(&key);
                }

                if !popped.is_empty() {
                    let frames: Vec<Frame> = popped.into_iter().map(Frame::Bulk).collect();
                    return Ok(Frame::Array(vec![
                        Frame::Bulk(key.to_bytes()),
                        Frame::Array(frames),
                    ]));
                }
            }
        }

        Ok(Frame::Null)
    })
}

/// LPUSHX key element [element ...]
pub fn cmd_lpushx(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        // Only push if key exists and is a list
        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let list = value.as_list().unwrap();
        let mut list = list.write();

        for arg in cmd.args.iter().skip(1) {
            list.push_front(arg.clone());
        }

        Ok(Frame::Integer(list.len() as i64))
    })
}

/// RPUSHX key element [element ...]
pub fn cmd_rpushx(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        // Only push if key exists and is a list
        let value = match db.get_typed(&key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let list = value.as_list().unwrap();
        let mut list = list.write();

        for arg in cmd.args.iter().skip(1) {
            list.push_back(arg.clone());
        }

        Ok(Frame::Integer(list.len() as i64))
    })
}

/// RPOPLPUSH source destination (deprecated, use LMOVE)
pub fn cmd_rpoplpush(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let src_key = Key::from(cmd.args[0].clone());
        let dst_key = Key::from(cmd.args[1].clone());

        // Get source list
        let src_value = match db.get_typed(&src_key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let src_list = src_value.as_list().unwrap();
        let mut src_guard = src_list.write();

        // Pop from right of source
        let element = match src_guard.pop_back() {
            Some(e) => e,
            None => return Ok(Frame::Null),
        };

        let src_is_empty = src_guard.is_empty();
        drop(src_guard);

        // Auto-delete empty source list
        if src_is_empty {
            db.delete(&src_key);
        }

        // Get or create destination list
        let dst_value = get_or_create_list(&db, &dst_key)?;
        let dst_list = dst_value.as_list().unwrap();
        let mut dst_guard = dst_list.write();

        // Push to left of destination
        dst_guard.push_front(element.clone());
        drop(dst_guard);
        db.set(dst_key, dst_value);

        Ok(Frame::Bulk(element))
    })
}

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
pub fn cmd_lmove(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let src_key = Key::from(cmd.args[0].clone());
        let dst_key = Key::from(cmd.args[1].clone());
        let wherefrom = cmd.get_str(2)?.to_uppercase();
        let whereto = cmd.get_str(3)?.to_uppercase();

        let from_left = match wherefrom.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        let to_left = match whereto.as_str() {
            "LEFT" => true,
            "RIGHT" => false,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        // Get source list
        let src_value = match db.get_typed(&src_key, ValueType::List)? {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let src_list = src_value.as_list().unwrap();
        let mut src_guard = src_list.write();

        // Pop from source
        let element = if from_left {
            src_guard.pop_front()
        } else {
            src_guard.pop_back()
        };

        let element = match element {
            Some(e) => e,
            None => return Ok(Frame::Null),
        };

        let src_is_empty = src_guard.is_empty();
        drop(src_guard);

        // Auto-delete empty source list
        if src_is_empty {
            db.delete(&src_key);
        }

        // Get or create destination list
        let dst_value = get_or_create_list(&db, &dst_key)?;
        let dst_list = dst_value.as_list().unwrap();
        let mut dst_guard = dst_list.write();

        // Push to destination
        if to_left {
            dst_guard.push_front(element.clone());
        } else {
            dst_guard.push_back(element.clone());
        }

        drop(dst_guard);
        db.set(dst_key, dst_value);

        Ok(Frame::Bulk(element))
    })
}
