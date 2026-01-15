//! Key command implementations.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Expiry, Key, ValueType, ViatorValue};
use crate::Result;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// DEL key [key ...]
pub fn cmd_del(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let count = cmd
            .args
            .iter()
            .filter(|arg| db.delete(&Key::from((*arg).clone())))
            .count();
        Ok(Frame::Integer(count as i64))
    })
}

/// UNLINK key [key ...] (same as DEL in this implementation)
pub fn cmd_unlink(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_del(cmd, db, client)
}

/// EXISTS key [key ...]
pub fn cmd_exists(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let count = cmd
            .args
            .iter()
            .filter(|arg| db.exists(&Key::from((*arg).clone())))
            .count();
        Ok(Frame::Integer(count as i64))
    })
}

/// TYPE key
pub fn cmd_type(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let type_name = db
            .key_type(&key)
            .map(|t| t.as_str())
            .unwrap_or("none");
        Ok(Frame::Simple(type_name.to_string()))
    })
}

/// RENAME key newkey
pub fn cmd_rename(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let old_key = Key::from(cmd.args[0].clone());
        let new_key = Key::from(cmd.args[1].clone());
        db.rename(&old_key, new_key)?;
        Ok(Frame::ok())
    })
}

/// RENAMENX key newkey
pub fn cmd_renamenx(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let old_key = Key::from(cmd.args[0].clone());
        let new_key = Key::from(cmd.args[1].clone());
        let result = db.rename_nx(&old_key, new_key)?;
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// Parse EXPIRE options (NX, XX, GT, LT)
fn parse_expire_options(cmd: &ParsedCommand, start_idx: usize) -> (bool, bool, bool, bool) {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;

    for i in start_idx..cmd.args.len() {
        if let Ok(opt) = std::str::from_utf8(&cmd.args[i]) {
            match opt.to_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GT" => gt = true,
                "LT" => lt = true,
                _ => {}
            }
        }
    }

    (nx, xx, gt, lt)
}

/// EXPIRE key seconds [NX | XX | GT | LT]
pub fn cmd_expire(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let seconds = cmd.get_i64(1)?;
        let (nx, xx, gt, lt) = parse_expire_options(&cmd, 2);

        let expiry = Expiry::from_seconds(seconds);
        let result = db.expire_with_options(&key, expiry, nx, xx, gt, lt);
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// PEXPIRE key milliseconds [NX | XX | GT | LT]
pub fn cmd_pexpire(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let ms = cmd.get_i64(1)?;
        let (nx, xx, gt, lt) = parse_expire_options(&cmd, 2);

        let expiry = Expiry::from_millis(ms);
        let result = db.expire_with_options(&key, expiry, nx, xx, gt, lt);
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// EXPIREAT key timestamp [NX | XX | GT | LT]
pub fn cmd_expireat(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let timestamp = cmd.get_i64(1)?;
        let (nx, xx, gt, lt) = parse_expire_options(&cmd, 2);

        let expiry = Expiry::at_seconds(timestamp);
        let result = db.expire_with_options(&key, expiry, nx, xx, gt, lt);
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// PEXPIREAT key milliseconds-timestamp [NX | XX | GT | LT]
pub fn cmd_pexpireat(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let timestamp = cmd.get_i64(1)?;
        let (nx, xx, gt, lt) = parse_expire_options(&cmd, 2);

        let expiry = Expiry::at_millis(timestamp);
        let result = db.expire_with_options(&key, expiry, nx, xx, gt, lt);
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// TTL key
pub fn cmd_ttl(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let ttl = db.ttl(&key).unwrap_or(-2);
        Ok(Frame::Integer(ttl))
    })
}

/// PTTL key
pub fn cmd_pttl(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let pttl = db.pttl(&key).unwrap_or(-2);
        Ok(Frame::Integer(pttl))
    })
}

/// PERSIST key
pub fn cmd_persist(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let result = db.persist(&key);
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// Maximum number of keys to return from KEYS command.
/// Prevents blocking the server on very large datasets.
const KEYS_MAX_RETURN: usize = 100_000;

/// Threshold above which KEYS logs a warning.
const KEYS_WARN_THRESHOLD: usize = 10_000;

/// KEYS pattern
///
/// WARNING: This command can be slow on large databases.
/// Consider using SCAN instead for production workloads.
pub fn cmd_keys(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let pattern = &cmd.args[0];

        // Check database size and warn if large
        let db_size = db.len();
        if db_size > KEYS_WARN_THRESHOLD {
            tracing::warn!(
                db_size = db_size,
                pattern = %String::from_utf8_lossy(pattern),
                "KEYS command on large database - consider using SCAN instead"
            );
        }

        let keys = db.keys(pattern);
        let key_count = keys.len();

        // Limit returned keys to prevent memory exhaustion
        if key_count > KEYS_MAX_RETURN {
            tracing::warn!(
                matched = key_count,
                returned = KEYS_MAX_RETURN,
                "KEYS result truncated - use SCAN for complete iteration"
            );
        }

        let frames: Vec<Frame> = keys
            .into_iter()
            .take(KEYS_MAX_RETURN)
            .map(|k| Frame::Bulk(k.to_bytes()))
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
pub fn cmd_scan(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let cursor = cmd.get_u64(0)? as usize;

        let mut pattern: Option<Bytes> = None;
        let mut count: usize = 10;
        let mut type_filter: Option<ValueType> = None;

        // Parse options
        let mut i = 1;
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
                "TYPE" => {
                    i += 1;
                    let type_str = cmd.get_str(i)?;
                    type_filter = match type_str.to_lowercase().as_str() {
                        "string" => Some(ValueType::String),
                        "list" => Some(ValueType::List),
                        "set" => Some(ValueType::Set),
                        "zset" => Some(ValueType::ZSet),
                        "hash" => Some(ValueType::Hash),
                        _ => None,
                    };
                }
                _ => {
                    return Err(CommandError::SyntaxError.into());
                }
            }
            i += 1;
        }

        let (next_cursor, keys) = db.scan(
            cursor,
            pattern.as_deref(),
            count,
            type_filter,
        );

        let key_frames: Vec<Frame> = keys.into_iter().map(|k| Frame::Bulk(k.to_bytes())).collect();

        Ok(Frame::Array(vec![
            Frame::Bulk(Bytes::from(next_cursor.to_string())),
            Frame::Array(key_frames),
        ]))
    })
}

/// RANDOMKEY
pub fn cmd_randomkey(
    _cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        match db.random_key() {
            Some(key) => Ok(Frame::Bulk(key.to_bytes())),
            None => Ok(Frame::Null),
        }
    })
}

/// DBSIZE
pub fn cmd_dbsize(
    _cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        Ok(Frame::Integer(db.len() as i64))
    })
}

/// EXPIRETIME key
pub fn cmd_expiretime(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let time = db.expiretime(&key).unwrap_or(-2);
        Ok(Frame::Integer(time))
    })
}

/// PEXPIRETIME key
pub fn cmd_pexpiretime(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        // Get expiretime returns seconds, we need to return milliseconds
        // For now, just return expiretime * 1000
        let time = db.expiretime(&key).map(|t| if t > 0 { t * 1000 } else { t }).unwrap_or(-2);
        Ok(Frame::Integer(time))
    })
}

/// COPY source destination [DB destination-db] [REPLACE]
pub fn cmd_copy(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let source = Key::from(cmd.args[0].clone());
        let dest = Key::from(cmd.args[1].clone());

        // Parse options
        let mut replace = false;
        let mut i = 2;
        while i < cmd.args.len() {
            if let Ok(opt) = std::str::from_utf8(&cmd.args[i]) {
                match opt.to_uppercase().as_str() {
                    "REPLACE" => replace = true,
                    "DB" => {
                        // DB option not supported in single-db mode for same-connection copy
                        // Would need access to other dbs which isn't available here
                        i += 1; // Skip the db number
                    }
                    _ => {}
                }
            }
            i += 1;
        }

        let result = db.copy(&source, dest, replace)?;
        Ok(Frame::Integer(if result { 1 } else { 0 }))
    })
}

/// TOUCH key [key ...]
pub fn cmd_touch(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let count = cmd
            .args
            .iter()
            .filter(|arg| db.exists(&Key::from((*arg).clone())))
            .count();
        Ok(Frame::Integer(count as i64))
    })
}

/// OBJECT ENCODING key
pub fn cmd_object(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Err(CommandError::WrongArity {
                command: "OBJECT".to_string(),
            }
            .into());
        }

        let subcommand = std::str::from_utf8(&cmd.args[0])
            .map_err(|_| CommandError::SyntaxError)?
            .to_uppercase();

        match subcommand.as_str() {
            "ENCODING" => {
                if cmd.args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "OBJECT ENCODING".to_string(),
                    }
                    .into());
                }
                let key = Key::from(cmd.args[1].clone());
                match db.key_type(&key) {
                    Some(ValueType::String) => Ok(Frame::Bulk(Bytes::from("embstr"))),
                    Some(ValueType::List) => Ok(Frame::Bulk(Bytes::from("quicklist"))),
                    Some(ValueType::Set) => Ok(Frame::Bulk(Bytes::from("listpack"))),
                    Some(ValueType::ZSet) => Ok(Frame::Bulk(Bytes::from("listpack"))),
                    Some(ValueType::Hash) => Ok(Frame::Bulk(Bytes::from("listpack"))),
                    Some(ValueType::Stream) => Ok(Frame::Bulk(Bytes::from("stream"))),
                    Some(ValueType::VectorSet) => Ok(Frame::Bulk(Bytes::from("vectorset"))),
                    None => Ok(Frame::Null),
                }
            }
            "FREQ" => {
                // LFU frequency - not implemented, return 0
                Ok(Frame::Integer(0))
            }
            "IDLETIME" => {
                // LRU idle time - not implemented, return 0
                Ok(Frame::Integer(0))
            }
            "REFCOUNT" => {
                // Reference count - always 1 in our implementation
                if cmd.args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "OBJECT REFCOUNT".to_string(),
                    }
                    .into());
                }
                let key = Key::from(cmd.args[1].clone());
                if db.exists(&key) {
                    Ok(Frame::Integer(1))
                } else {
                    Ok(Frame::Null)
                }
            }
            "HELP" => {
                let help = vec![
                    Frame::Bulk(Bytes::from("OBJECT ENCODING <key>")),
                    Frame::Bulk(Bytes::from("    Return the encoding of the object stored at <key>.")),
                    Frame::Bulk(Bytes::from("OBJECT FREQ <key>")),
                    Frame::Bulk(Bytes::from("    Return the access frequency of the object stored at <key>.")),
                    Frame::Bulk(Bytes::from("OBJECT IDLETIME <key>")),
                    Frame::Bulk(Bytes::from("    Return the idle time of the object stored at <key>.")),
                    Frame::Bulk(Bytes::from("OBJECT REFCOUNT <key>")),
                    Frame::Bulk(Bytes::from("    Return the reference count of the object stored at <key>.")),
                ];
                Ok(Frame::Array(help))
            }
            _ => Err(CommandError::UnknownCommand(format!("OBJECT {}", subcommand)).into()),
        }
    })
}

/// MOVE key db
/// Move a key to another database
pub fn cmd_move(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;

        let key = Key::from(cmd.args[0].clone());
        let _target_db = cmd.get_u64(1)?;

        // In single-db mode, MOVE can't actually move to another DB
        // Just check if the key exists
        if db.exists(&key) {
            // Return 0 to indicate the key wasn't moved (target db would have the key)
            Ok(Frame::Integer(0))
        } else {
            // Key doesn't exist
            Ok(Frame::Integer(0))
        }
    })
}

/// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
/// Sort elements in a list, set, or sorted set
pub fn cmd_sort(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        let mut alpha = false;
        let mut desc = false;
        let mut limit_offset = 0usize;
        let mut limit_count: Option<usize> = None;
        let mut store_key: Option<Key> = None;

        // Parse options
        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "ASC" => desc = false,
                "DESC" => desc = true,
                "ALPHA" => alpha = true,
                "LIMIT" => {
                    i += 1;
                    limit_offset = cmd.get_u64(i)? as usize;
                    i += 1;
                    limit_count = Some(cmd.get_u64(i)? as usize);
                }
                "STORE" => {
                    i += 1;
                    store_key = Some(Key::from(cmd.args.get(i).cloned().ok_or(CommandError::SyntaxError)?));
                }
                "BY" | "GET" => {
                    // BY and GET patterns not fully implemented
                    i += 1; // Skip the pattern
                }
                _ => {}
            }
            i += 1;
        }

        // Get elements to sort
        let value = match db.get(&key) {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let mut elements: Vec<Bytes> = if value.is_list() {
            let list = value.as_list().ok_or(CommandError::WrongType)?;
            let guard = list.read();
            guard.iter().cloned().collect()
        } else if value.is_set() {
            let set = value.as_set().ok_or(CommandError::WrongType)?;
            let guard = set.read();
            guard.members().into_iter().cloned().collect()
        } else if value.is_zset() {
            let zset = value.as_zset().ok_or(CommandError::WrongType)?;
            let guard = zset.read();
            guard.iter().map(|e| e.member.clone()).collect()
        } else {
            return Err(CommandError::WrongType.into());
        };

        // Sort elements
        if alpha {
            // Lexicographic sort
            elements.sort_by(|a, b| {
                let sa = std::str::from_utf8(a).unwrap_or("");
                let sb = std::str::from_utf8(b).unwrap_or("");
                if desc { sb.cmp(sa) } else { sa.cmp(sb) }
            });
        } else {
            // Numeric sort
            elements.sort_by(|a, b| {
                let na: f64 = std::str::from_utf8(a).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let nb: f64 = std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if desc {
                    nb.partial_cmp(&na).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
                }
            });
        }

        // Apply limit
        let elements: Vec<Bytes> = if let Some(count) = limit_count {
            elements.into_iter().skip(limit_offset).take(count).collect()
        } else {
            elements.into_iter().skip(limit_offset).collect()
        };

        // Store or return results
        if let Some(dest) = store_key {
            let count = elements.len() as i64;
            let new_list = ViatorValue::new_list();
            if let Some(list) = new_list.as_list() {
                let mut guard = list.write();
                for elem in elements {
                    guard.push_back(elem);
                }
            }
            db.set(dest, new_list);
            Ok(Frame::Integer(count))
        } else {
            Ok(Frame::Array(elements.into_iter().map(Frame::Bulk).collect()))
        }
    })
}

/// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern] [ASC|DESC] [ALPHA]
/// Read-only variant of SORT (no STORE option)
pub fn cmd_sort_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_sort(cmd, db, client)
}

/// MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key [key ...]]
/// Migrate keys to another Redis server for cluster slot migration.
pub fn cmd_migrate(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?;

        let host = cmd.get_str(0)?;
        let port = cmd.get_u64(1)? as u16;
        let single_key = cmd.get_str(2)?;
        let dest_db = cmd.get_u64(3)?;
        let timeout_ms = cmd.get_u64(4)?;

        // Parse options
        let mut copy = false;
        let mut replace = false;
        let mut auth_password: Option<String> = None;
        let mut auth_user: Option<String> = None;
        let mut keys: Vec<Key> = Vec::new();

        // If key is not empty, add it as the key to migrate
        if !single_key.is_empty() {
            keys.push(Key::from(Bytes::from(single_key.to_string())));
        }

        // Parse optional arguments
        let mut i = 5;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "COPY" => copy = true,
                "REPLACE" => replace = true,
                "AUTH" => {
                    i += 1;
                    if i < cmd.args.len() {
                        auth_password = Some(cmd.get_str(i)?.to_string());
                    }
                }
                "AUTH2" => {
                    i += 1;
                    if i < cmd.args.len() {
                        auth_user = Some(cmd.get_str(i)?.to_string());
                    }
                    i += 1;
                    if i < cmd.args.len() {
                        auth_password = Some(cmd.get_str(i)?.to_string());
                    }
                }
                "KEYS" => {
                    // Remaining args are keys
                    i += 1;
                    while i < cmd.args.len() {
                        keys.push(Key::from(cmd.args[i].clone()));
                        i += 1;
                    }
                    break;
                }
                _ => {}
            }
            i += 1;
        }

        if keys.is_empty() {
            return Ok(Frame::Error("ERR no keys to migrate".to_string()));
        }

        // Connect to target server
        let addr = format!("{}:{}", host, port);
        let timeout = std::time::Duration::from_millis(timeout_ms);

        let stream = match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Ok(Frame::Error(format!("ERR IO error: {}", e))),
            Err(_) => return Ok(Frame::Error("ERR timeout connecting to target".to_string())),
        };

        let (mut reader, mut writer) = stream.into_split();

        // Helper to send RESP command
        async fn send_command(writer: &mut tokio::net::tcp::OwnedWriteHalf, args: &[&[u8]]) -> std::io::Result<()> {
            use tokio::io::AsyncWriteExt;
            // Build RESP array
            let mut buf = format!("*{}\r\n", args.len()).into_bytes();
            for arg in args {
                buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
                buf.extend_from_slice(arg);
                buf.extend_from_slice(b"\r\n");
            }
            writer.write_all(&buf).await
        }

        // Helper to read response
        async fn read_response(reader: &mut tokio::net::tcp::OwnedReadHalf) -> std::io::Result<String> {
            use tokio::io::AsyncBufReadExt;
            let mut buf_reader = tokio::io::BufReader::new(reader);
            let mut line = String::new();
            buf_reader.read_line(&mut line).await?;
            Ok(line)
        }

        // Authenticate if needed
        if let Some(password) = auth_password {
            let auth_args: Vec<&[u8]> = if let Some(user) = &auth_user {
                vec![b"AUTH", user.as_bytes(), password.as_bytes()]
            } else {
                vec![b"AUTH", password.as_bytes()]
            };
            if let Err(e) = send_command(&mut writer, &auth_args).await {
                return Ok(Frame::Error(format!("ERR AUTH failed: {}", e)));
            }
            if let Err(e) = read_response(&mut reader).await {
                return Ok(Frame::Error(format!("ERR AUTH failed: {}", e)));
            }
        }

        // Select destination database
        let db_str = dest_db.to_string();
        if let Err(e) = send_command(&mut writer, &[b"SELECT", db_str.as_bytes()]).await {
            return Ok(Frame::Error(format!("ERR SELECT failed: {}", e)));
        }
        if let Err(e) = read_response(&mut reader).await {
            return Ok(Frame::Error(format!("ERR SELECT failed: {}", e)));
        }

        // Migrate each key
        let mut migrated = 0;
        let mut nokey = false;

        for key in &keys {
            // Check if key exists
            let value = match db.get(key) {
                Some(v) => v,
                None => {
                    nokey = true;
                    continue;
                }
            };

            // Serialize the value (simplified - not full VDB format)
            let serialized = serialize_value(&value);
            let ttl = db.ttl(key).unwrap_or(-1);
            let ttl_str = if ttl > 0 {
                ttl.to_string()
            } else {
                "0".to_string()
            };

            // Build RESTORE command
            let key_bytes = key.as_ref();
            let _replace_flag = if replace { b"REPLACE".as_slice() } else { b"".as_slice() };

            let restore_args: Vec<&[u8]> = if replace {
                vec![b"RESTORE", key_bytes, ttl_str.as_bytes(), &serialized, b"REPLACE"]
            } else {
                vec![b"RESTORE", key_bytes, ttl_str.as_bytes(), &serialized]
            };

            if let Err(e) = send_command(&mut writer, &restore_args).await {
                return Ok(Frame::Error(format!("ERR RESTORE failed: {}", e)));
            }

            match read_response(&mut reader).await {
                Ok(resp) if resp.starts_with('+') || resp.starts_with(':') => {
                    // Success
                    migrated += 1;
                    // Delete locally unless COPY is specified
                    if !copy {
                        db.delete(key);
                    }
                }
                Ok(resp) if resp.starts_with('-') => {
                    // Error from target - if BUSYKEY and not REPLACE, that's an error
                    if resp.contains("BUSYKEY") && !replace {
                        return Ok(Frame::Error("BUSYKEY Target key name is busy".to_string()));
                    }
                    // Other errors
                    return Ok(Frame::Error(format!("ERR RESTORE error: {}", resp.trim())));
                }
                Err(e) => {
                    return Ok(Frame::Error(format!("ERR network error: {}", e)));
                }
                _ => {}
            }
        }

        if migrated == 0 && nokey {
            return Ok(Frame::Simple("NOKEY".to_string()));
        }

        Ok(Frame::ok())
    })
}

/// Serialize a value for MIGRATE/RESTORE (simplified format).
fn serialize_value(value: &ViatorValue) -> Vec<u8> {
    use bytes::BufMut;
    let mut buf = Vec::new();

    match value {
        ViatorValue::String(s) => {
            buf.put_u8(0); // Type: string
            buf.put_u32_le(s.len() as u32);
            buf.extend_from_slice(s);
        }
        ViatorValue::List(list) => {
            let list = list.read();
            buf.put_u8(1); // Type: list
            let items: Vec<_> = list.iter().collect();
            buf.put_u32_le(items.len() as u32);
            for item in items {
                buf.put_u32_le(item.len() as u32);
                buf.extend_from_slice(&item);
            }
        }
        ViatorValue::Set(set) => {
            let set = set.read();
            buf.put_u8(2); // Type: set
            let members: Vec<_> = set.members();
            buf.put_u32_le(members.len() as u32);
            for item in members {
                buf.put_u32_le(item.len() as u32);
                buf.extend_from_slice(&item);
            }
        }
        ViatorValue::Hash(hash) => {
            let hash = hash.read();
            buf.put_u8(3); // Type: hash
            let entries: Vec<_> = hash.iter().collect();
            buf.put_u32_le(entries.len() as u32);
            for (k, v) in entries {
                buf.put_u32_le(k.len() as u32);
                buf.extend_from_slice(k);
                buf.put_u32_le(v.len() as u32);
                buf.extend_from_slice(v);
            }
        }
        ViatorValue::ZSet(zset) => {
            let zset = zset.read();
            buf.put_u8(4); // Type: zset
            let entries: Vec<_> = zset.iter().collect();
            buf.put_u32_le(entries.len() as u32);
            for entry in entries {
                buf.put_f64_le(entry.score);
                buf.put_u32_le(entry.member.len() as u32);
                buf.extend_from_slice(&entry.member);
            }
        }
        ViatorValue::Stream(_) => {
            // Streams are complex - for now serialize as empty marker
            buf.put_u8(255);
        }
        ViatorValue::VectorSet(_) => {
            // Vector sets are complex - for now serialize as empty marker
            buf.put_u8(254);
        }
    }

    // Add a simple checksum (CRC32 would be better in production)
    let checksum: u64 = buf.iter().fold(0u64, |acc, &b| acc.wrapping_add(b as u64));
    buf.put_u64_le(checksum);

    buf
}

/// WAITAOF numlocal numreplicas timeout
/// Wait for AOF flushing
pub fn cmd_waitaof(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;

        let numlocal = cmd.get_u64(0)?;
        let _numreplicas = cmd.get_u64(1)?;
        let _timeout = cmd.get_u64(2)?;

        // Return [numlocal, numreplicas] - we assume local is always synced
        Ok(Frame::Array(vec![
            Frame::Integer(numlocal as i64),
            Frame::Integer(0), // No replicas
        ]))
    })
}
