//! Lua scripting command implementations.
//!
//! Redis supports server-side Lua scripting via EVAL and EVALSHA commands.
//! Scripts are executed atomically and can call Redis commands.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::Key;
use crate::Result;
use bytes::Bytes;
use mlua::{Lua, MultiValue, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

/// Script cache storing SHA1 -> script content
static SCRIPT_CACHE: std::sync::LazyLock<RwLock<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Calculate SHA1 hash of a script (Redis-compatible).
///
/// Redis uses SHA1 to identify cached scripts. Clients can pre-compute
/// the SHA1 hash and use EVALSHA to call cached scripts efficiently.
fn sha1_hash(script: &str) -> String {
    use sha1::{Sha1, Digest};

    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    let result = hasher.finalize();

    // Convert to lowercase hex string (40 characters)
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Convert a Lua value to a Redis Frame
fn lua_to_frame(value: Value) -> Frame {
    match value {
        Value::Nil => Frame::Null,
        Value::Boolean(b) => {
            if b {
                Frame::Integer(1)
            } else {
                Frame::Null
            }
        }
        Value::Integer(i) => Frame::Integer(i),
        Value::Number(n) => Frame::Integer(n as i64),
        Value::String(s) => {
            let bytes: Vec<u8> = s.as_bytes().to_vec();
            Frame::Bulk(Bytes::from(bytes))
        }
        Value::Table(t) => {
            // Check if it's an array-like table
            let mut frames = Vec::new();
            let mut idx = 1i64;
            while let Ok(v) = t.get::<Value>(idx) {
                if matches!(v, Value::Nil) {
                    break;
                }
                frames.push(lua_to_frame(v));
                idx += 1;
            }
            if frames.is_empty() {
                // Check for error format { err = "message" }
                if let Ok(err) = t.get::<String>("err") {
                    return Frame::Error(format!("ERR {err}"));
                }
                // Check for ok format { ok = "message" }
                if let Ok(ok) = t.get::<String>("ok") {
                    return Frame::Simple(ok);
                }
            }
            Frame::Array(frames)
        }
        _ => Frame::Null,
    }
}

/// Execute a Redis command from Lua context
fn execute_viator_command(db: &Arc<Db>, cmd_name: &str, args: &[Bytes]) -> Frame {
    match cmd_name.to_uppercase().as_str() {
        "GET" => {
            if args.is_empty() {
                return Frame::Error("ERR wrong number of arguments for 'get' command".to_string());
            }
            let key = Key::from(args[0].clone());
            match db.get(&key) {
                Some(v) => {
                    if let Some(s) = v.as_string() {
                        Frame::Bulk(s.clone())
                    } else {
                        Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
                None => Frame::Null,
            }
        }
        "SET" => {
            if args.len() < 2 {
                return Frame::Error("ERR wrong number of arguments for 'set' command".to_string());
            }
            let key = Key::from(args[0].clone());
            db.set(key, crate::types::ViatorValue::String(args[1].clone()));
            Frame::ok()
        }
        "DEL" => {
            let mut deleted = 0i64;
            for arg in args {
                let key = Key::from(arg.clone());
                if db.delete(&key) {
                    deleted += 1;
                }
            }
            Frame::Integer(deleted)
        }
        "EXISTS" => {
            let mut count = 0i64;
            for arg in args {
                let key = Key::from(arg.clone());
                if db.exists(&key) {
                    count += 1;
                }
            }
            Frame::Integer(count)
        }
        "INCR" => {
            if args.is_empty() {
                return Frame::Error(
                    "ERR wrong number of arguments for 'incr' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            // Get current value, increment, set
            let current = db.get(&key).and_then(|v| v.as_i64()).unwrap_or(0);
            let new_val = current + 1;
            db.set(key, crate::types::ViatorValue::from(new_val));
            Frame::Integer(new_val)
        }
        "DECR" => {
            if args.is_empty() {
                return Frame::Error(
                    "ERR wrong number of arguments for 'decr' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            let current = db.get(&key).and_then(|v| v.as_i64()).unwrap_or(0);
            let new_val = current - 1;
            db.set(key, crate::types::ViatorValue::from(new_val));
            Frame::Integer(new_val)
        }
        "HGET" => {
            if args.len() < 2 {
                return Frame::Error(
                    "ERR wrong number of arguments for 'hget' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            match db.get(&key) {
                Some(v) => {
                    if let Some(h) = v.as_hash() {
                        let guard = h.read();
                        match guard.get(&args[1]) {
                            Some(val) => Frame::Bulk(val.clone()),
                            None => Frame::Null,
                        }
                    } else {
                        Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
                None => Frame::Null,
            }
        }
        "HSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Frame::Error(
                    "ERR wrong number of arguments for 'hset' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            let value = db
                .get(&key)
                .unwrap_or_else(crate::types::ViatorValue::new_hash);
            if let Some(h) = value.as_hash() {
                let mut guard = h.write();
                let mut added = 0i64;
                for chunk in args[1..].chunks(2) {
                    if chunk.len() == 2 && guard.insert(chunk[0].clone(), chunk[1].clone()).is_none()
                    {
                        added += 1;
                    }
                }
                drop(guard);
                db.set(key, value);
                Frame::Integer(added)
            } else {
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )
            }
        }
        "LPUSH" => {
            if args.len() < 2 {
                return Frame::Error(
                    "ERR wrong number of arguments for 'lpush' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            let value = db
                .get(&key)
                .unwrap_or_else(crate::types::ViatorValue::new_list);
            if let Some(l) = value.as_list() {
                let mut guard = l.write();
                for arg in args[1..].iter().rev() {
                    guard.push_front(arg.clone());
                }
                let len = guard.len();
                drop(guard);
                db.set(key, value);
                Frame::Integer(len as i64)
            } else {
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )
            }
        }
        "RPUSH" => {
            if args.len() < 2 {
                return Frame::Error(
                    "ERR wrong number of arguments for 'rpush' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            let value = db
                .get(&key)
                .unwrap_or_else(crate::types::ViatorValue::new_list);
            if let Some(l) = value.as_list() {
                let mut guard = l.write();
                for arg in &args[1..] {
                    guard.push_back(arg.clone());
                }
                let len = guard.len();
                drop(guard);
                db.set(key, value);
                Frame::Integer(len as i64)
            } else {
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )
            }
        }
        "LRANGE" => {
            if args.len() < 3 {
                return Frame::Error(
                    "ERR wrong number of arguments for 'lrange' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            let start: i64 = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let stop: i64 = std::str::from_utf8(&args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);

            match db.get(&key) {
                Some(v) => {
                    if let Some(l) = v.as_list() {
                        let guard = l.read();
                        let items = guard.range(start, stop);
                        Frame::Array(items.into_iter().map(|b| Frame::Bulk(b.clone())).collect())
                    } else {
                        Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
                None => Frame::Array(vec![]),
            }
        }
        "SADD" => {
            if args.len() < 2 {
                return Frame::Error(
                    "ERR wrong number of arguments for 'sadd' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            let value = db
                .get(&key)
                .unwrap_or_else(crate::types::ViatorValue::new_set);
            if let Some(s) = value.as_set() {
                let mut guard = s.write();
                let mut added = 0i64;
                for arg in &args[1..] {
                    if guard.add(arg.clone()) {
                        added += 1;
                    }
                }
                drop(guard);
                db.set(key, value);
                Frame::Integer(added)
            } else {
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )
            }
        }
        "SMEMBERS" => {
            if args.is_empty() {
                return Frame::Error(
                    "ERR wrong number of arguments for 'smembers' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            match db.get(&key) {
                Some(v) => {
                    if let Some(s) = v.as_set() {
                        let guard = s.read();
                        Frame::Array(
                            guard
                                .members()
                                .into_iter()
                                .map(|b| Frame::Bulk(b.clone()))
                                .collect(),
                        )
                    } else {
                        Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
                None => Frame::Array(vec![]),
            }
        }
        "TYPE" => {
            if args.is_empty() {
                return Frame::Error(
                    "ERR wrong number of arguments for 'type' command".to_string(),
                );
            }
            let key = Key::from(args[0].clone());
            match db.get(&key) {
                Some(v) => Frame::Simple(v.value_type().as_str().to_string()),
                None => Frame::Simple("none".to_string()),
            }
        }
        "KEYS" => {
            if args.is_empty() {
                return Frame::Error(
                    "ERR wrong number of arguments for 'keys' command".to_string(),
                );
            }
            let keys = db.keys(&args[0]);
            Frame::Array(
                keys.into_iter()
                    .map(|k| Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())))
                    .collect(),
            )
        }
        _ => Frame::Error(format!(
            "ERR unknown command '{cmd_name}', with args beginning with: ",
        )),
    }
}

/// EVAL script numkeys [key ...] [arg ...]
pub fn cmd_eval(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let script = cmd.get_str(0)?;
        let numkeys = cmd.get_i64(1)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "EVAL".to_string(),
            }
            .into());
        }

        let keys: Vec<Bytes> = cmd.args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = cmd.args[2 + numkeys..].to_vec();

        // Cache the script
        let sha = sha1_hash(script);
        {
            let mut cache = SCRIPT_CACHE.write().unwrap();
            cache.insert(sha.clone(), script.to_string());
        }

        execute_script(&db, script, keys, argv)
    })
}

/// EVALSHA sha1 numkeys [key ...] [arg ...]
pub fn cmd_evalsha(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let sha = cmd.get_str(0)?;
        let numkeys = cmd.get_i64(1)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "EVALSHA".to_string(),
            }
            .into());
        }

        let keys: Vec<Bytes> = cmd.args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = cmd.args[2 + numkeys..].to_vec();

        // Look up script in cache
        let script = {
            let cache = SCRIPT_CACHE.read().unwrap();
            cache.get(sha).cloned()
        };

        match script {
            Some(script) => execute_script(&db, &script, keys, argv),
            None => Err(CommandError::NoScript(sha.to_string()).into()),
        }
    })
}

/// SCRIPT subcommand [args...]
pub fn cmd_script(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "LOAD" => {
                cmd.require_args(2)?;
                let script = cmd.get_str(1)?;
                let sha = sha1_hash(script);
                {
                    let mut cache = SCRIPT_CACHE.write().unwrap();
                    cache.insert(sha.clone(), script.to_string());
                }
                Ok(Frame::bulk(sha))
            }
            "EXISTS" => {
                let cache = SCRIPT_CACHE.read().unwrap();
                let results: Vec<Frame> = cmd.args[1..]
                    .iter()
                    .map(|arg| {
                        let sha = std::str::from_utf8(arg).unwrap_or("");
                        Frame::Integer(i64::from(cache.contains_key(sha)))
                    })
                    .collect();
                Ok(Frame::Array(results))
            }
            "FLUSH" => {
                let mut cache = SCRIPT_CACHE.write().unwrap();
                cache.clear();
                Ok(Frame::ok())
            }
            "KILL" => {
                // We don't support long-running script killing yet
                Ok(Frame::ok())
            }
            "DEBUG" => {
                // Debug mode not implemented
                Ok(Frame::ok())
            }
            _ => Err(CommandError::SyntaxError.into()),
        }
    })
}

/// Execute a Lua script
fn execute_script(db: &Arc<Db>, script: &str, keys: Vec<Bytes>, argv: Vec<Bytes>) -> Result<Frame> {
    let lua = Lua::new();

    // Create KEYS table
    let keys_table = lua
        .create_table()
        .map_err(|e| crate::error::Error::Internal(format!("Failed to create KEYS table: {e}")))?;
    for (i, key) in keys.iter().enumerate() {
        keys_table
            .set(i + 1, std::str::from_utf8(key).unwrap_or(""))
            .map_err(|e| {
                crate::error::Error::Internal(format!("Failed to set KEYS[{}]: {e}", i + 1))
            })?;
    }
    lua.globals()
        .set("KEYS", keys_table)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set KEYS global: {e}")))?;

    // Create ARGV table
    let argv_table = lua
        .create_table()
        .map_err(|e| crate::error::Error::Internal(format!("Failed to create ARGV table: {e}")))?;
    for (i, arg) in argv.iter().enumerate() {
        argv_table
            .set(i + 1, std::str::from_utf8(arg).unwrap_or(""))
            .map_err(|e| {
                crate::error::Error::Internal(format!("Failed to set ARGV[{}]: {e}", i + 1))
            })?;
    }
    lua.globals()
        .set("ARGV", argv_table)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set ARGV global: {e}")))?;

    // Create redis table with call function
    // Note: Due to borrow checker constraints, we create a simple redis.call
    // that stores commands for later execution
    let viator_table = lua
        .create_table()
        .map_err(|e| crate::error::Error::Internal(format!("Failed to create redis table: {e}")))?;

    // For now, we'll execute commands through a different mechanism
    // Store the db reference in Lua's registry
    let db_clone = db.clone();

    let call_fn = lua
        .create_function(move |lua_ctx, args: MultiValue| {
            let args_vec: Vec<Value> = args.into_iter().collect();
            if args_vec.is_empty() {
                return Ok(Value::Nil);
            }

            // First arg is command name
            let cmd_name = match &args_vec[0] {
                Value::String(s) => s.to_str().ok().map(|b| b.to_string()).unwrap_or_default(),
                _ => return Ok(Value::Nil),
            };

            // Rest are arguments
            let cmd_args: Vec<Bytes> = args_vec[1..]
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(Bytes::copy_from_slice(&s.as_bytes())),
                    Value::Integer(i) => Some(Bytes::from(i.to_string())),
                    Value::Number(n) => Some(Bytes::from(n.to_string())),
                    _ => None,
                })
                .collect();

            let result = execute_viator_command(&db_clone, &cmd_name, &cmd_args);

            // Convert Frame back to Lua value
            match result {
                Frame::Null => Ok(Value::Boolean(false)),
                Frame::Integer(i) => Ok(Value::Integer(i)),
                Frame::Bulk(b) => {
                    let s = lua_ctx
                        .create_string(b.as_ref())
                        .map_err(|e| mlua::Error::external(e))?;
                    Ok(Value::String(s))
                }
                Frame::Simple(s) => {
                    let t = lua_ctx
                        .create_table()
                        .map_err(|e| mlua::Error::external(e))?;
                    t.set("ok", s).map_err(|e| mlua::Error::external(e))?;
                    Ok(Value::Table(t))
                }
                Frame::Error(e) => {
                    let t = lua_ctx
                        .create_table()
                        .map_err(|e| mlua::Error::external(e))?;
                    t.set("err", e).map_err(|e| mlua::Error::external(e))?;
                    Ok(Value::Table(t))
                }
                Frame::Array(arr) => {
                    let t = lua_ctx
                        .create_table()
                        .map_err(|e| mlua::Error::external(e))?;
                    for (i, frame) in arr.into_iter().enumerate() {
                        let val = frame_to_lua_value(lua_ctx, frame)?;
                        t.set(i + 1, val).map_err(|e| mlua::Error::external(e))?;
                    }
                    Ok(Value::Table(t))
                }
            }
        })
        .map_err(|e| crate::error::Error::Internal(format!("Failed to create redis.call: {e}")))?;

    viator_table
        .set("call", call_fn)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set redis.call: {e}")))?;

    // Add redis.log function (no-op for now)
    let log_fn = lua
        .create_function(|_, _: MultiValue| Ok(()))
        .map_err(|e| crate::error::Error::Internal(format!("Failed to create redis.log: {e}")))?;
    viator_table
        .set("log", log_fn)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set redis.log: {e}")))?;

    // Add log level constants
    viator_table
        .set("LOG_DEBUG", 0)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set LOG_DEBUG: {e}")))?;
    viator_table
        .set("LOG_VERBOSE", 1)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set LOG_VERBOSE: {e}")))?;
    viator_table
        .set("LOG_NOTICE", 2)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set LOG_NOTICE: {e}")))?;
    viator_table
        .set("LOG_WARNING", 3)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set LOG_WARNING: {e}")))?;

    lua.globals()
        .set("redis", viator_table)
        .map_err(|e| crate::error::Error::Internal(format!("Failed to set redis global: {e}")))?;

    // Execute the script
    let result: Value = lua
        .load(script)
        .eval()
        .map_err(|e| crate::error::Error::Command(CommandError::ScriptError(e.to_string())))?;

    Ok(lua_to_frame(result))
}

/// Helper to convert Frame to Lua Value
fn frame_to_lua_value(lua: &Lua, frame: Frame) -> mlua::Result<Value> {
    match frame {
        Frame::Null => Ok(Value::Boolean(false)),
        Frame::Integer(i) => Ok(Value::Integer(i)),
        Frame::Bulk(b) => {
            let s = lua.create_string(b.as_ref())?;
            Ok(Value::String(s))
        }
        Frame::Simple(s) => {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            Ok(Value::Table(t))
        }
        Frame::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", e)?;
            Ok(Value::Table(t))
        }
        Frame::Array(arr) => {
            let t = lua.create_table()?;
            for (i, f) in arr.into_iter().enumerate() {
                let val = frame_to_lua_value(lua, f)?;
                t.set(i + 1, val)?;
            }
            Ok(Value::Table(t))
        }
    }
}

/// Function library cache storing library_name -> (library_code, functions)
static FUNCTION_CACHE: std::sync::LazyLock<RwLock<HashMap<String, (String, HashMap<String, String>)>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// EVAL_RO script numkeys [key ...] [arg ...]
/// Read-only variant of EVAL - safe to run on replicas
pub fn cmd_eval_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    // For now, EVAL_RO works identically to EVAL
    // In a full implementation, write operations would be blocked
    cmd_eval(cmd, db, client)
}

/// EVALSHA_RO sha1 numkeys [key ...] [arg ...]
/// Read-only variant of EVALSHA - safe to run on replicas
pub fn cmd_evalsha_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    // For now, EVALSHA_RO works identically to EVALSHA
    cmd_evalsha(cmd, db, client)
}

/// FCALL function numkeys [key ...] [arg ...]
/// Call a Redis function
pub fn cmd_fcall(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let function_name = cmd.get_str(0)?;
        let numkeys = cmd.get_i64(1)? as usize;

        if cmd.args.len() < 2 + numkeys {
            return Err(CommandError::WrongArity {
                command: "FCALL".to_string(),
            }
            .into());
        }

        let keys: Vec<Bytes> = cmd.args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = cmd.args[2 + numkeys..].to_vec();

        // Look up function in cache
        let script = {
            let cache = FUNCTION_CACHE.read().unwrap();
            let mut found_script = None;
            for (_lib_name, (_lib_code, functions)) in cache.iter() {
                if let Some(func_code) = functions.get(function_name) {
                    found_script = Some(func_code.clone());
                    break;
                }
            }
            found_script
        };

        match script {
            Some(script) => execute_script(&db, &script, keys, argv),
            None => Err(CommandError::NoFunction(function_name.to_string()).into()),
        }
    })
}

/// FCALL_RO function numkeys [key ...] [arg ...]
/// Read-only variant of FCALL - safe to run on replicas
pub fn cmd_fcall_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    // For now, FCALL_RO works identically to FCALL
    cmd_fcall(cmd, db, client)
}

/// FUNCTION subcommand [args...]
/// Manage Redis functions
pub fn cmd_function(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "LOAD" => {
                // FUNCTION LOAD [REPLACE] library_code
                cmd.require_args(2)?;

                let mut idx = 1;
                let _replace = if cmd.get_str(idx).map(|s| s.to_uppercase() == "REPLACE").unwrap_or(false) {
                    idx += 1;
                    true
                } else {
                    false
                };

                let library_code = cmd.get_str(idx)?;

                // Parse the library code to extract function definitions
                // For now, we use a simple parser that looks for function names
                // Real Redis uses a special format with #!lua directives
                let library_name = format!("lib_{}", sha1_hash(library_code).chars().take(8).collect::<String>());

                // Store the library
                let mut functions = HashMap::new();
                // For simplicity, register the entire code as a single function named after library
                functions.insert(library_name.clone(), library_code.to_string());

                {
                    let mut cache = FUNCTION_CACHE.write().unwrap();
                    cache.insert(library_name.clone(), (library_code.to_string(), functions));
                }

                Ok(Frame::bulk(library_name))
            }
            "DELETE" => {
                cmd.require_args(2)?;
                let library_name = cmd.get_str(1)?;

                let mut cache = FUNCTION_CACHE.write().unwrap();
                if cache.remove(library_name).is_some() {
                    Ok(Frame::ok())
                } else {
                    Err(CommandError::NoFunction(library_name.to_string()).into())
                }
            }
            "LIST" => {
                let cache = FUNCTION_CACHE.read().unwrap();
                let mut result = Vec::new();

                for (lib_name, (lib_code, functions)) in cache.iter() {
                    let mut lib_info = Vec::new();
                    lib_info.push(Frame::bulk("library_name"));
                    lib_info.push(Frame::bulk(lib_name.clone()));
                    lib_info.push(Frame::bulk("engine"));
                    lib_info.push(Frame::bulk("LUA"));
                    lib_info.push(Frame::bulk("functions"));

                    let func_list: Vec<Frame> = functions.keys().map(|f| {
                        Frame::Array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(f.clone()),
                            Frame::bulk("description"),
                            Frame::Null,
                            Frame::bulk("flags"),
                            Frame::Array(vec![]),
                        ])
                    }).collect();

                    lib_info.push(Frame::Array(func_list));
                    lib_info.push(Frame::bulk("library_code"));
                    lib_info.push(Frame::bulk(lib_code.clone()));

                    result.push(Frame::Array(lib_info));
                }

                Ok(Frame::Array(result))
            }
            "FLUSH" => {
                let mut cache = FUNCTION_CACHE.write().unwrap();
                cache.clear();
                Ok(Frame::ok())
            }
            "DUMP" => {
                // Return serialized functions (stub - returns empty for now)
                Ok(Frame::Bulk(Bytes::new()))
            }
            "RESTORE" => {
                // Restore functions from serialized data (stub)
                Ok(Frame::ok())
            }
            "STATS" => {
                // Return function statistics
                let cache = FUNCTION_CACHE.read().unwrap();
                let running_scripts = 0i64;
                let engines = vec![
                    Frame::Array(vec![
                        Frame::bulk("engine_name"),
                        Frame::bulk("LUA"),
                        Frame::bulk("libraries_count"),
                        Frame::Integer(cache.len() as i64),
                        Frame::bulk("functions_count"),
                        Frame::Integer(cache.values().map(|(_, f)| f.len()).sum::<usize>() as i64),
                    ])
                ];

                Ok(Frame::Array(vec![
                    Frame::bulk("running_scripts"),
                    Frame::Integer(running_scripts),
                    Frame::bulk("engines"),
                    Frame::Array(engines),
                ]))
            }
            "KILL" => {
                // Kill running function (stub)
                Ok(Frame::ok())
            }
            "HELP" => {
                Ok(Frame::Array(vec![
                    Frame::bulk("FUNCTION LOAD [REPLACE] library_code"),
                    Frame::bulk("FUNCTION DELETE library_name"),
                    Frame::bulk("FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]"),
                    Frame::bulk("FUNCTION FLUSH [ASYNC|SYNC]"),
                    Frame::bulk("FUNCTION DUMP"),
                    Frame::bulk("FUNCTION RESTORE serialized_value [FLUSH|APPEND|REPLACE]"),
                    Frame::bulk("FUNCTION STATS"),
                    Frame::bulk("FUNCTION KILL"),
                ]))
            }
            _ => Err(CommandError::SyntaxError.into()),
        }
    })
}
