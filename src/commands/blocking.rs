//! Blocking command implementations.
//!
//! Commands that can block waiting for data to become available.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ViatorValue};
use crate::Result;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// BLPOP key [key ...] timeout
pub fn cmd_blpop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let timeout_idx = cmd.args.len() - 1;
        let timeout: f64 = cmd.get_f64(timeout_idx)?;
        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let keys: Vec<Key> = cmd.args[..timeout_idx]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let start = std::time::Instant::now();

        loop {
            // Check each key for available data
            for key in &keys {
                if let Some(value) = db.get(key) {
                    if let Some(list) = value.as_list() {
                        let mut guard = list.write();
                        if let Some(elem) = guard.pop_front() {
                            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                            drop(guard);
                            // Delete key if list is empty
                            if value.as_list().map(|l| l.read().is_empty()).unwrap_or(false) {
                                db.delete(key);
                            }
                            return Ok(Frame::Array(vec![
                                Frame::Bulk(key_bytes),
                                Frame::Bulk(elem),
                            ]));
                        }
                    }
                }
            }

            // Check timeout
            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            // Sleep briefly before retry
            tokio::time::sleep(Duration::from_millis(10)).await;

            // For zero timeout, return immediately
            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BRPOP key [key ...] timeout
pub fn cmd_brpop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let timeout_idx = cmd.args.len() - 1;
        let timeout: f64 = cmd.get_f64(timeout_idx)?;
        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let keys: Vec<Key> = cmd.args[..timeout_idx]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let start = std::time::Instant::now();

        loop {
            for key in &keys {
                if let Some(value) = db.get(key) {
                    if let Some(list) = value.as_list() {
                        let mut guard = list.write();
                        if let Some(elem) = guard.pop_back() {
                            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                            drop(guard);
                            if value.as_list().map(|l| l.read().is_empty()).unwrap_or(false) {
                                db.delete(key);
                            }
                            return Ok(Frame::Array(vec![
                                Frame::Bulk(key_bytes),
                                Frame::Bulk(elem),
                            ]));
                        }
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BRPOPLPUSH source destination timeout (deprecated, use BLMOVE)
pub fn cmd_brpoplpush(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let src_key = Key::from(cmd.args[0].clone());
        let dst_key = Key::from(cmd.args[1].clone());
        let timeout: f64 = cmd.get_f64(2)?;

        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let start = std::time::Instant::now();

        loop {
            if let Some(src_value) = db.get(&src_key) {
                if let Some(src_list) = src_value.as_list() {
                    let mut src_guard = src_list.write();

                    if let Some(elem) = src_guard.pop_back() {
                        drop(src_guard);

                        // Get or create destination list
                        let dst_value = db.get(&dst_key).unwrap_or_else(ViatorValue::new_list);
                        if let Some(dst_list) = dst_value.as_list() {
                            let mut dst_guard = dst_list.write();
                            dst_guard.push_front(elem.clone());
                            drop(dst_guard);
                            db.set(dst_key, dst_value);
                        }

                        // Delete source if empty
                        if src_value.as_list().map(|l| l.read().is_empty()).unwrap_or(false) {
                            db.delete(&src_key);
                        }

                        return Ok(Frame::Bulk(elem));
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
pub fn cmd_blmove(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(5)?;

        let src_key = Key::from(cmd.args[0].clone());
        let dst_key = Key::from(cmd.args[1].clone());
        let wherefrom = cmd.get_str(2)?.to_uppercase();
        let whereto = cmd.get_str(3)?.to_uppercase();
        let timeout: f64 = cmd.get_f64(4)?;

        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let start = std::time::Instant::now();

        loop {
            if let Some(src_value) = db.get(&src_key) {
                if let Some(src_list) = src_value.as_list() {
                    let mut src_guard = src_list.write();

                    let elem = match wherefrom.as_str() {
                        "LEFT" => src_guard.pop_front(),
                        "RIGHT" => src_guard.pop_back(),
                        _ => return Err(CommandError::SyntaxError.into()),
                    };

                    if let Some(elem) = elem {
                        drop(src_guard);

                        // Get or create destination list
                        let dst_value = db.get(&dst_key).unwrap_or_else(ViatorValue::new_list);
                        if let Some(dst_list) = dst_value.as_list() {
                            let mut dst_guard = dst_list.write();
                            match whereto.as_str() {
                                "LEFT" => dst_guard.push_front(elem.clone()),
                                "RIGHT" => dst_guard.push_back(elem.clone()),
                                _ => return Err(CommandError::SyntaxError.into()),
                            }
                            drop(dst_guard);
                            db.set(dst_key, dst_value);
                        }

                        // Delete source if empty
                        if src_value.as_list().map(|l| l.read().is_empty()).unwrap_or(false) {
                            db.delete(&src_key);
                        }

                        return Ok(Frame::Bulk(elem));
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
pub fn cmd_blmpop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;

        let timeout: f64 = cmd.get_f64(0)?;
        let numkeys: usize = cmd.get_u64(1)? as usize;

        if cmd.args.len() < 3 + numkeys {
            return Err(CommandError::WrongArity { command: "BLMPOP".to_string() }.into());
        }

        let keys: Vec<Key> = cmd.args[2..2 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let direction = cmd.get_str(2 + numkeys)?.to_uppercase();
        let mut count = 1usize;

        if cmd.args.len() > 3 + numkeys {
            let count_arg = cmd.get_str(3 + numkeys)?.to_uppercase();
            if count_arg == "COUNT" && cmd.args.len() > 4 + numkeys {
                count = cmd.get_u64(4 + numkeys)? as usize;
            }
        }

        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let start = std::time::Instant::now();

        loop {
            for key in &keys {
                if let Some(value) = db.get(key) {
                    if let Some(list) = value.as_list() {
                        let mut guard = list.write();
                        if !guard.is_empty() {
                            let mut elements = Vec::new();
                            for _ in 0..count {
                                let elem = match direction.as_str() {
                                    "LEFT" => guard.pop_front(),
                                    "RIGHT" => guard.pop_back(),
                                    _ => return Err(CommandError::SyntaxError.into()),
                                };
                                if let Some(e) = elem {
                                    elements.push(Frame::Bulk(e));
                                } else {
                                    break;
                                }
                            }

                            if !elements.is_empty() {
                                let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                                drop(guard);

                                if value.as_list().map(|l| l.read().is_empty()).unwrap_or(false) {
                                    db.delete(key);
                                }

                                return Ok(Frame::Array(vec![
                                    Frame::Bulk(key_bytes),
                                    Frame::Array(elements),
                                ]));
                            }
                        }
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BZPOPMIN key [key ...] timeout
pub fn cmd_bzpopmin(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let timeout_idx = cmd.args.len() - 1;
        let timeout: f64 = cmd.get_f64(timeout_idx)?;
        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let keys: Vec<Key> = cmd.args[..timeout_idx]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let start = std::time::Instant::now();

        loop {
            for key in &keys {
                if let Some(value) = db.get(key) {
                    if let Some(zset) = value.as_zset() {
                        let mut guard = zset.write();
                        let popped = guard.pop_min(1);
                        if let Some(entry) = popped.into_iter().next() {
                            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                            drop(guard);

                            if value.as_zset().map(|z| z.read().len() == 0).unwrap_or(false) {
                                db.delete(key);
                            }

                            return Ok(Frame::Array(vec![
                                Frame::Bulk(key_bytes),
                                Frame::Bulk(entry.member),
                                Frame::Bulk(Bytes::from(entry.score.to_string())),
                            ]));
                        }
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BZPOPMAX key [key ...] timeout
pub fn cmd_bzpopmax(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let timeout_idx = cmd.args.len() - 1;
        let timeout: f64 = cmd.get_f64(timeout_idx)?;
        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let keys: Vec<Key> = cmd.args[..timeout_idx]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let start = std::time::Instant::now();

        loop {
            for key in &keys {
                if let Some(value) = db.get(key) {
                    if let Some(zset) = value.as_zset() {
                        let mut guard = zset.write();
                        let popped = guard.pop_max(1);
                        if let Some(entry) = popped.into_iter().next() {
                            let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                            drop(guard);

                            if value.as_zset().map(|z| z.read().len() == 0).unwrap_or(false) {
                                db.delete(key);
                            }

                            return Ok(Frame::Array(vec![
                                Frame::Bulk(key_bytes),
                                Frame::Bulk(entry.member),
                                Frame::Bulk(Bytes::from(entry.score.to_string())),
                            ]));
                        }
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
pub fn cmd_bzmpop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;

        let timeout: f64 = cmd.get_f64(0)?;
        let numkeys: usize = cmd.get_u64(1)? as usize;

        if cmd.args.len() < 3 + numkeys {
            return Err(CommandError::WrongArity { command: "BZMPOP".to_string() }.into());
        }

        let keys: Vec<Key> = cmd.args[2..2 + numkeys]
            .iter()
            .map(|b| Key::from(b.clone()))
            .collect();

        let direction = cmd.get_str(2 + numkeys)?.to_uppercase();
        let mut count = 1usize;

        if cmd.args.len() > 3 + numkeys {
            let count_arg = cmd.get_str(3 + numkeys)?.to_uppercase();
            if count_arg == "COUNT" && cmd.args.len() > 4 + numkeys {
                count = cmd.get_u64(4 + numkeys)? as usize;
            }
        }

        let timeout_duration = if timeout <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(timeout))
        };

        let start = std::time::Instant::now();

        loop {
            for key in &keys {
                if let Some(value) = db.get(key) {
                    if let Some(zset) = value.as_zset() {
                        let mut guard = zset.write();
                        if guard.len() > 0 {
                            let entries = match direction.as_str() {
                                "MIN" => guard.pop_min(count),
                                "MAX" => guard.pop_max(count),
                                _ => return Err(CommandError::SyntaxError.into()),
                            };

                            if !entries.is_empty() {
                                let elements: Vec<Frame> = entries
                                    .into_iter()
                                    .map(|entry| Frame::Array(vec![
                                        Frame::Bulk(entry.member),
                                        Frame::Bulk(Bytes::from(entry.score.to_string())),
                                    ]))
                                    .collect();

                                let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                                drop(guard);

                                if value.as_zset().map(|z| z.read().len() == 0).unwrap_or(false) {
                                    db.delete(key);
                                }

                                return Ok(Frame::Array(vec![
                                    Frame::Bulk(key_bytes),
                                    Frame::Array(elements),
                                ]));
                            }
                        }
                    }
                }
            }

            if let Some(duration) = timeout_duration {
                if start.elapsed() >= duration {
                    return Ok(Frame::Null);
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            if timeout_duration.is_none() && timeout == 0.0 {
                return Ok(Frame::Null);
            }
        }
    })
}
