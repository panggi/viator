//! JSON command implementations.
//!
//! Redis Stack compatible JSON.* commands.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::Result;
use bytes::Bytes;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for JSON values.
static JSON_STORAGE: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, Value>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Parse a JSONPath query - simplified implementation.
fn query_path<'a>(value: &'a Value, path: &str) -> Vec<&'a Value> {
    let path = path.trim();

    // Root reference
    if path == "$" || path == "." || path.is_empty() {
        return vec![value];
    }

    // Remove leading $ or .
    let path = path.trim_start_matches('$').trim_start_matches('.');

    // Split by . and navigate
    let mut current = vec![value];
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }

        let mut next = Vec::new();
        for v in current {
            // Handle array index notation: [n] or [*]
            if let Some(array_idx) = segment.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
                if let Value::Array(arr) = v {
                    if array_idx == "*" {
                        next.extend(arr.iter());
                    } else if let Ok(idx) = array_idx.parse::<usize>() {
                        if let Some(elem) = arr.get(idx) {
                            next.push(elem);
                        }
                    }
                }
            } else if segment == "*" {
                // Wildcard
                match v {
                    Value::Object(obj) => next.extend(obj.values()),
                    Value::Array(arr) => next.extend(arr.iter()),
                    _ => {}
                }
            } else {
                // Object key
                if let Value::Object(obj) = v {
                    if let Some(child) = obj.get(segment) {
                        next.push(child);
                    }
                }
            }
        }
        current = next;
    }

    current
}

/// Set value at path - simplified implementation.
fn set_at_path(root: &mut Value, path: &str, new_value: Value) -> bool {
    let path = path.trim().trim_start_matches('$').trim_start_matches('.');

    if path.is_empty() {
        *root = new_value;
        return true;
    }

    let segments: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    set_at_path_recursive(root, &segments, 0, new_value)
}

fn set_at_path_recursive(current: &mut Value, segments: &[&str], idx: usize, new_value: Value) -> bool {
    if idx >= segments.len() {
        return false;
    }

    let segment = segments[idx];
    let is_last = idx == segments.len() - 1;

    if let Some(array_idx_str) = segment.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        if let Value::Array(arr) = current {
            if let Ok(array_idx) = array_idx_str.parse::<usize>() {
                if is_last {
                    if array_idx < arr.len() {
                        arr[array_idx] = new_value;
                        return true;
                    } else if array_idx == arr.len() {
                        arr.push(new_value);
                        return true;
                    }
                    return false;
                }
                if array_idx < arr.len() {
                    return set_at_path_recursive(&mut arr[array_idx], segments, idx + 1, new_value);
                }
            }
        }
        return false;
    }

    if let Value::Object(obj) = current {
        if is_last {
            obj.insert(segment.to_string(), new_value);
            return true;
        }
        if !obj.contains_key(segment) {
            obj.insert(segment.to_string(), json!({}));
        }
        if let Some(child) = obj.get_mut(segment) {
            return set_at_path_recursive(child, segments, idx + 1, new_value);
        }
    }

    false
}

/// Delete value at path.
fn delete_at_path(root: &mut Value, path: &str) -> bool {
    let path = path.trim().trim_start_matches('$').trim_start_matches('.');

    if path.is_empty() {
        return false;
    }

    let segments: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    delete_at_path_recursive(root, &segments, 0)
}

fn delete_at_path_recursive(current: &mut Value, segments: &[&str], idx: usize) -> bool {
    if idx >= segments.len() {
        return false;
    }

    let segment = segments[idx];
    let is_last = idx == segments.len() - 1;

    if let Some(array_idx_str) = segment.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        if let Value::Array(arr) = current {
            if let Ok(array_idx) = array_idx_str.parse::<usize>() {
                if is_last {
                    if array_idx < arr.len() {
                        arr.remove(array_idx);
                        return true;
                    }
                    return false;
                }
                if array_idx < arr.len() {
                    return delete_at_path_recursive(&mut arr[array_idx], segments, idx + 1);
                }
            }
        }
        return false;
    }

    if let Value::Object(obj) = current {
        if is_last {
            return obj.remove(segment).is_some();
        }
        if let Some(child) = obj.get_mut(segment) {
            return delete_at_path_recursive(child, segments, idx + 1);
        }
    }

    false
}

/// JSON.SET key path value [NX | XX]
/// Set a JSON value at a path.
pub fn cmd_json_set(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let json_str = cmd.get_str(2)?;

        let value: Value = serde_json::from_str(json_str)
            .map_err(|_| CommandError::SyntaxError)?;

        let nx = cmd.args.len() > 3 && cmd.get_str(3)?.to_uppercase() == "NX";
        let xx = cmd.args.len() > 3 && cmd.get_str(3)?.to_uppercase() == "XX";

        let mut storage = JSON_STORAGE.write();

        if nx && storage.contains_key(&key) {
            return Ok(Frame::Null);
        }
        if xx && !storage.contains_key(&key) {
            return Ok(Frame::Null);
        }

        let root = storage.entry(key).or_insert_with(|| json!({}));

        if set_at_path(root, path, value) {
            Ok(Frame::ok())
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.GET key [path [path ...]]
/// Get JSON values at paths.
pub fn cmd_json_get(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        if cmd.args.len() == 1 {
            // No path specified, return whole document
            let json_str = serde_json::to_string(value).unwrap_or_default();
            return Ok(Frame::Bulk(json_str.into()));
        }

        // Multiple paths
        if cmd.args.len() == 2 {
            let path = cmd.get_str(1)?;
            let results = query_path(value, path);
            if results.is_empty() {
                return Ok(Frame::Null);
            }
            if results.len() == 1 {
                let json_str = serde_json::to_string(results[0]).unwrap_or_default();
                return Ok(Frame::Bulk(json_str.into()));
            }
            let arr: Vec<&Value> = results;
            let json_str = serde_json::to_string(&arr).unwrap_or_default();
            return Ok(Frame::Bulk(json_str.into()));
        }

        // Multiple paths - return object with path as keys
        let mut result_obj = serde_json::Map::new();
        for i in 1..cmd.args.len() {
            let path = cmd.get_str(i)?;
            let results = query_path(value, path);
            if !results.is_empty() {
                if results.len() == 1 {
                    result_obj.insert(path.to_string(), results[0].clone());
                } else {
                    result_obj.insert(path.to_string(), Value::Array(results.iter().cloned().cloned().collect()));
                }
            }
        }

        let json_str = serde_json::to_string(&Value::Object(result_obj)).unwrap_or_default();
        Ok(Frame::Bulk(json_str.into()))
    })
}

/// JSON.MGET key [key ...] path
/// Get values from multiple keys at a path.
pub fn cmd_json_mget(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        // Last argument is the path
        let path = cmd.get_str(cmd.args.len() - 1)?;
        let storage = JSON_STORAGE.read();

        let results: Vec<Frame> = cmd.args[0..cmd.args.len() - 1]
            .iter()
            .map(|key| {
                let key_vec = key.to_vec();
                match storage.get(&key_vec) {
                    Some(value) => {
                        let queried = query_path(value, path);
                        if queried.is_empty() {
                            Frame::Null
                        } else if queried.len() == 1 {
                            let json_str = serde_json::to_string(queried[0]).unwrap_or_default();
                            Frame::Bulk(json_str.into())
                        } else {
                            let json_str = serde_json::to_string(&queried).unwrap_or_default();
                            Frame::Bulk(json_str.into())
                        }
                    }
                    None => Frame::Null,
                }
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// JSON.DEL key [path]
/// Delete a value at a path.
pub fn cmd_json_del(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let mut storage = JSON_STORAGE.write();

        if cmd.args.len() == 1 || cmd.get_str(1)? == "$" {
            // Delete entire key
            return Ok(Frame::Integer(if storage.remove(&key).is_some() { 1 } else { 0 }));
        }

        let path = cmd.get_str(1)?;
        let value = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let deleted = delete_at_path(value, path);
        Ok(Frame::Integer(if deleted { 1 } else { 0 }))
    })
}

/// JSON.FORGET key [path]
/// Alias for JSON.DEL.
pub fn cmd_json_forget(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_json_del(cmd, db, client)
}

/// JSON.TYPE key [path]
/// Get the type of value at a path.
pub fn cmd_json_type(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };
        let results = query_path(value, path);

        if results.is_empty() {
            return Ok(Frame::Null);
        }

        let types: Vec<Frame> = results
            .iter()
            .map(|v| {
                let type_name = match v {
                    Value::Null => "null",
                    Value::Bool(_) => "boolean",
                    Value::Number(n) => {
                        if n.is_i64() { "integer" } else { "number" }
                    }
                    Value::String(_) => "string",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                };
                Frame::bulk(type_name)
            })
            .collect();

        if types.len() == 1 {
            Ok(types.into_iter().next().unwrap())
        } else {
            Ok(Frame::Array(types))
        }
    })
}

/// JSON.NUMINCRBY key path value
/// Increment a number at a path.
pub fn cmd_json_numincrby(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let increment: f64 = cmd.get_str(2)?.parse().map_err(|_| CommandError::NotFloat)?;

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        // Find and increment
        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_and_incr(current: &mut Value, segments: &[&str], idx: usize, increment: f64) -> Option<f64> {
            if idx >= segments.len() {
                if let Value::Number(n) = current {
                    let new_val = n.as_f64().unwrap_or(0.0) + increment;
                    *current = json!(new_val);
                    return Some(new_val);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_and_incr(child, segments, idx + 1, increment);
                }
            }
            None
        }

        if let Some(new_val) = navigate_and_incr(root, &segments, 0, increment) {
            Ok(Frame::Bulk(format!("{}", new_val).into()))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.NUMMULTBY key path value
/// Multiply a number at a path.
pub fn cmd_json_nummultby(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let multiplier: f64 = cmd.get_str(2)?.parse().map_err(|_| CommandError::NotFloat)?;

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_and_mult(current: &mut Value, segments: &[&str], idx: usize, multiplier: f64) -> Option<f64> {
            if idx >= segments.len() {
                if let Value::Number(n) = current {
                    let new_val = n.as_f64().unwrap_or(0.0) * multiplier;
                    *current = json!(new_val);
                    return Some(new_val);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_and_mult(child, segments, idx + 1, multiplier);
                }
            }
            None
        }

        if let Some(new_val) = navigate_and_mult(root, &segments, 0, multiplier) {
            Ok(Frame::Bulk(format!("{}", new_val).into()))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.STRAPPEND key [path] value
/// Append a string at a path.
pub fn cmd_json_strappend(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let (path, append_value) = if cmd.args.len() == 2 {
            ("$", cmd.get_str(1)?)
        } else {
            (cmd.get_str(1)?, cmd.get_str(2)?)
        };

        // Parse the JSON string to append
        let append_str: String = serde_json::from_str(append_value)
            .map_err(|_| CommandError::SyntaxError)?;

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_and_append(current: &mut Value, segments: &[&str], idx: usize, append_str: &str) -> Option<usize> {
            if idx >= segments.len() {
                if let Value::String(s) = current {
                    s.push_str(append_str);
                    return Some(s.len());
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_and_append(child, segments, idx + 1, append_str);
                }
            }
            None
        }

        if let Some(len) = navigate_and_append(root, &segments, 0, &append_str) {
            Ok(Frame::Integer(len as i64))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.STRLEN key [path]
/// Get the length of a string at a path.
pub fn cmd_json_strlen(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results = query_path(value, path);
        let lengths: Vec<Frame> = results
            .iter()
            .map(|v| {
                if let Value::String(s) = v {
                    Frame::Integer(s.len() as i64)
                } else {
                    Frame::Null
                }
            })
            .collect();

        if lengths.len() == 1 {
            Ok(lengths.into_iter().next().unwrap())
        } else {
            Ok(Frame::Array(lengths))
        }
    })
}

/// JSON.ARRAPPEND key path value [value ...]
/// Append values to an array.
pub fn cmd_json_arrappend(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;

        // Parse all values to append
        let mut values_to_append = Vec::new();
        for i in 2..cmd.args.len() {
            let json_str = cmd.get_str(i)?;
            let value: Value = serde_json::from_str(json_str)
                .map_err(|_| CommandError::SyntaxError)?;
            values_to_append.push(value);
        }

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_to_array<'a>(current: &'a mut Value, segments: &[&str], idx: usize) -> Option<&'a mut Vec<Value>> {
            if idx >= segments.len() {
                if let Value::Array(arr) = current {
                    return Some(arr);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_to_array(child, segments, idx + 1);
                }
            }
            None
        }

        if let Some(arr) = navigate_to_array(root, &segments, 0) {
            for value in values_to_append {
                arr.push(value);
            }
            Ok(Frame::Integer(arr.len() as i64))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.ARRINDEX key path value [start [stop]]
/// Find index of value in array.
pub fn cmd_json_arrindex(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let search_str = cmd.get_str(2)?;
        let search_value: Value = serde_json::from_str(search_str)
            .map_err(|_| CommandError::SyntaxError)?;

        let start = if cmd.args.len() > 3 {
            cmd.get_i64(3)?
        } else {
            0
        };

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results = query_path(value, path);
        let indices: Vec<Frame> = results
            .iter()
            .map(|v| {
                if let Value::Array(arr) = v {
                    let start_idx = if start < 0 {
                        (arr.len() as i64 + start).max(0) as usize
                    } else {
                        start as usize
                    };

                    for (i, elem) in arr.iter().enumerate().skip(start_idx) {
                        if elem == &search_value {
                            return Frame::Integer(i as i64);
                        }
                    }
                    Frame::Integer(-1)
                } else {
                    Frame::Null
                }
            })
            .collect();

        if indices.len() == 1 {
            Ok(indices.into_iter().next().unwrap())
        } else {
            Ok(Frame::Array(indices))
        }
    })
}

/// JSON.ARRLEN key [path]
/// Get the length of an array.
pub fn cmd_json_arrlen(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results = query_path(value, path);
        let lengths: Vec<Frame> = results
            .iter()
            .map(|v| {
                if let Value::Array(arr) = v {
                    Frame::Integer(arr.len() as i64)
                } else {
                    Frame::Null
                }
            })
            .collect();

        if lengths.len() == 1 {
            Ok(lengths.into_iter().next().unwrap())
        } else {
            Ok(Frame::Array(lengths))
        }
    })
}

/// JSON.ARRPOP key [path [index]]
/// Pop element from array.
pub fn cmd_json_arrpop(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };
        let index: i64 = if cmd.args.len() > 2 {
            cmd.get_i64(2)?
        } else {
            -1
        };

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_to_array<'a>(current: &'a mut Value, segments: &[&str], idx: usize) -> Option<&'a mut Vec<Value>> {
            if idx >= segments.len() {
                if let Value::Array(arr) = current {
                    return Some(arr);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_to_array(child, segments, idx + 1);
                }
            }
            None
        }

        if let Some(arr) = navigate_to_array(root, &segments, 0) {
            if arr.is_empty() {
                return Ok(Frame::Null);
            }

            let actual_index = if index < 0 {
                (arr.len() as i64 + index).max(0) as usize
            } else {
                (index as usize).min(arr.len() - 1)
            };

            let removed = arr.remove(actual_index);
            let json_str = serde_json::to_string(&removed).unwrap_or_default();
            Ok(Frame::Bulk(json_str.into()))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.ARRTRIM key path start stop
/// Trim array to range.
pub fn cmd_json_arrtrim(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(4)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let start = cmd.get_i64(2)?;
        let stop = cmd.get_i64(3)?;

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_to_array<'a>(current: &'a mut Value, segments: &[&str], idx: usize) -> Option<&'a mut Vec<Value>> {
            if idx >= segments.len() {
                if let Value::Array(arr) = current {
                    return Some(arr);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_to_array(child, segments, idx + 1);
                }
            }
            None
        }

        if let Some(arr) = navigate_to_array(root, &segments, 0) {
            let len = arr.len() as i64;
            let start_idx = if start < 0 { (len + start).max(0) as usize } else { start as usize };
            let stop_idx = if stop < 0 { (len + stop).max(0) as usize } else { stop as usize };

            if start_idx >= arr.len() {
                arr.clear();
            } else {
                let end = (stop_idx + 1).min(arr.len());
                *arr = arr[start_idx..end].to_vec();
            }

            Ok(Frame::Integer(arr.len() as i64))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.ARRINSERT key path index value [value ...]
/// Insert values into array.
pub fn cmd_json_arrinsert(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let index = cmd.get_i64(2)?;

        // Parse all values to insert
        let mut values_to_insert = Vec::new();
        for i in 3..cmd.args.len() {
            let json_str = cmd.get_str(i)?;
            let value: Value = serde_json::from_str(json_str)
                .map_err(|_| CommandError::SyntaxError)?;
            values_to_insert.push(value);
        }

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_to_array<'a>(current: &'a mut Value, segments: &[&str], idx: usize) -> Option<&'a mut Vec<Value>> {
            if idx >= segments.len() {
                if let Value::Array(arr) = current {
                    return Some(arr);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_to_array(child, segments, idx + 1);
                }
            }
            None
        }

        if let Some(arr) = navigate_to_array(root, &segments, 0) {
            let actual_index = if index < 0 {
                (arr.len() as i64 + index).max(0) as usize
            } else {
                (index as usize).min(arr.len())
            };

            // Insert in reverse order to maintain order
            for value in values_to_insert.into_iter().rev() {
                arr.insert(actual_index, value);
            }

            Ok(Frame::Integer(arr.len() as i64))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.OBJKEYS key [path]
/// Get keys of object at path.
pub fn cmd_json_objkeys(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results = query_path(value, path);
        let keys: Vec<Frame> = results
            .iter()
            .map(|v| {
                if let Value::Object(obj) = v {
                    Frame::Array(
                        obj.keys()
                            .map(|k| Frame::Bulk(Bytes::from(k.clone())))
                            .collect(),
                    )
                } else {
                    Frame::Null
                }
            })
            .collect();

        if keys.len() == 1 {
            Ok(keys.into_iter().next().unwrap())
        } else {
            Ok(Frame::Array(keys))
        }
    })
}

/// JSON.OBJLEN key [path]
/// Get number of keys in object at path.
pub fn cmd_json_objlen(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results = query_path(value, path);
        let lengths: Vec<Frame> = results
            .iter()
            .map(|v| {
                if let Value::Object(obj) = v {
                    Frame::Integer(obj.len() as i64)
                } else {
                    Frame::Null
                }
            })
            .collect();

        if lengths.len() == 1 {
            Ok(lengths.into_iter().next().unwrap())
        } else {
            Ok(Frame::Array(lengths))
        }
    })
}

/// JSON.TOGGLE key path
/// Toggle boolean value at path.
pub fn cmd_json_toggle(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_and_toggle(current: &mut Value, segments: &[&str], idx: usize) -> Option<bool> {
            if idx >= segments.len() {
                if let Value::Bool(b) = current {
                    *b = !*b;
                    return Some(*b);
                }
                return None;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_and_toggle(child, segments, idx + 1);
                }
            }
            None
        }

        if let Some(new_val) = navigate_and_toggle(root, &segments, 0) {
            Ok(Frame::Integer(if new_val { 1 } else { 0 }))
        } else {
            Err(CommandError::SyntaxError.into())
        }
    })
}

/// JSON.CLEAR key [path]
/// Clear container (object/array) at path.
pub fn cmd_json_clear(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        fn navigate_and_clear(current: &mut Value, segments: &[&str], idx: usize) -> i64 {
            if idx >= segments.len() {
                return match current {
                    Value::Object(obj) => {
                        obj.clear();
                        1
                    }
                    Value::Array(arr) => {
                        arr.clear();
                        1
                    }
                    Value::Number(_) => {
                        *current = json!(0);
                        1
                    }
                    _ => 0,
                };
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_and_clear(child, segments, idx + 1);
                }
            }
            0
        }

        let cleared = navigate_and_clear(root, &segments, 0);
        Ok(Frame::Integer(cleared))
    })
}

/// JSON.RESP key [path]
/// Return JSON in Redis Serialization Protocol (RESP).
pub fn cmd_json_resp(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();
        let path = if cmd.args.len() > 1 { cmd.get_str(1)? } else { "$" };

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        let results = query_path(value, path);

        fn value_to_resp(v: &Value) -> Frame {
            match v {
                Value::Null => Frame::Null,
                Value::Bool(b) => Frame::bulk(if *b { "true" } else { "false" }),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Frame::Integer(i)
                    } else {
                        Frame::Bulk(n.to_string().into())
                    }
                }
                Value::String(s) => Frame::Bulk(s.clone().into()),
                Value::Array(arr) => {
                    let mut frames = vec![Frame::bulk("[")];
                    frames.extend(arr.iter().map(value_to_resp));
                    Frame::Array(frames)
                }
                Value::Object(obj) => {
                    let mut frames = vec![Frame::bulk("{")];
                    for (k, v) in obj {
                        frames.push(Frame::Bulk(k.clone().into()));
                        frames.push(value_to_resp(v));
                    }
                    Frame::Array(frames)
                }
            }
        }

        if results.len() == 1 {
            Ok(value_to_resp(results[0]))
        } else {
            Ok(Frame::Array(results.iter().map(|v| value_to_resp(v)).collect()))
        }
    })
}

/// JSON.DEBUG MEMORY key [path]
/// Get memory usage of JSON value.
pub fn cmd_json_debug(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let subcmd = cmd.get_str(0)?.to_uppercase();

        if subcmd != "MEMORY" {
            return Err(CommandError::SyntaxError.into());
        }

        let key = cmd.args[1].to_vec();

        let storage = JSON_STORAGE.read();
        let value = storage.get(&key).ok_or(CommandError::NoSuchKey)?;

        // Estimate memory usage
        let json_str = serde_json::to_string(value).unwrap_or_default();
        Ok(Frame::Integer(json_str.len() as i64))
    })
}

/// JSON.MERGE key path value
/// Merge a JSON value with an existing JSON value (RFC 7396 JSON Merge Patch).
pub fn cmd_json_merge(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let path = cmd.get_str(1)?;
        let patch_str = cmd.get_str(2)?;

        let patch: Value = serde_json::from_str(patch_str)
            .map_err(|_| CommandError::SyntaxError)?;

        let mut storage = JSON_STORAGE.write();
        let root = storage.get_mut(&key).ok_or(CommandError::NoSuchKey)?;

        let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
        let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

        /// RFC 7396 JSON Merge Patch implementation
        fn merge_patch(target: &mut Value, patch: &Value) {
            match patch {
                Value::Object(patch_obj) => {
                    if !target.is_object() {
                        *target = json!({});
                    }
                    if let Value::Object(target_obj) = target {
                        for (key, value) in patch_obj {
                            if value.is_null() {
                                target_obj.remove(key);
                            } else {
                                let entry = target_obj
                                    .entry(key.clone())
                                    .or_insert(json!(null));
                                merge_patch(entry, value);
                            }
                        }
                    }
                }
                _ => {
                    *target = patch.clone();
                }
            }
        }

        fn navigate_and_merge(current: &mut Value, segments: &[&str], idx: usize, patch: &Value) -> bool {
            if idx >= segments.len() {
                merge_patch(current, patch);
                return true;
            }

            let segment = segments[idx];
            if let Value::Object(obj) = current {
                if let Some(child) = obj.get_mut(segment) {
                    return navigate_and_merge(child, segments, idx + 1, patch);
                }
            }
            false
        }

        navigate_and_merge(root, &segments, 0, &patch);
        Ok(Frame::ok())
    })
}

/// JSON.MSET key path value [key path value ...]
/// Set JSON values at paths in multiple keys.
pub fn cmd_json_mset(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        // Must have triplets
        if cmd.args.len() % 3 != 0 {
            return Err(CommandError::WrongArity {
                command: "JSON.MSET".to_string(),
            }
            .into());
        }

        let mut storage = JSON_STORAGE.write();

        let mut i = 0;
        while i < cmd.args.len() {
            let key = cmd.args[i].to_vec();
            let path = cmd.get_str(i + 1)?;
            let json_str = cmd.get_str(i + 2)?;

            let new_value: Value = serde_json::from_str(json_str)
                .map_err(|_| CommandError::SyntaxError)?;

            if path == "$" || path == "." || path.is_empty() {
                // Set root
                storage.insert(key, new_value);
            } else {
                // Set at path - create if not exists
                let root = storage
                    .entry(key)
                    .or_insert_with(|| json!({}));

                let path_str = path.trim().trim_start_matches('$').trim_start_matches('.');
                let segments: Vec<&str> = path_str.split('.').filter(|s| !s.is_empty()).collect();

                set_at_path_recursive(root, &segments, 0, new_value);
            }

            i += 3;
        }

        Ok(Frame::ok())
    })
}
