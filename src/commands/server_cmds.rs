//! Server command implementations.

use super::ParsedCommand;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::server::metrics::{format_bytes, get_cpu_usage, get_memory_usage};
use crate::storage::Db;
use crate::types::{Expiry, Key, ViatorValue};
use crate::{REDIS_VERSION, Result, VERSION};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// PING [message]
pub fn cmd_ping(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            Ok(Frame::pong())
        } else {
            Ok(Frame::Bulk(cmd.args[0].clone()))
        }
    })
}

/// ECHO message
pub fn cmd_echo(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move { Ok(Frame::Bulk(cmd.args[0].clone())) })
}

/// INFO [section]
pub fn cmd_info(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let section = cmd.args.first().and_then(|s| std::str::from_utf8(s).ok());

        let mut info = String::new();

        let include_all = section.is_none() || section == Some("all");

        if include_all || section == Some("server") {
            info.push_str("# Server\r\n");
            info.push_str(&format!("redis_version:{REDIS_VERSION}\r\n"));
            info.push_str(&format!("redis_rs_version:{VERSION}\r\n"));
            info.push_str("viator_mode:standalone\r\n");
            info.push_str(&format!("os:{}\r\n", std::env::consts::OS));
            info.push_str(&format!("arch_bits:{}\r\n", (usize::BITS as usize)));
            info.push_str(&format!("tcp_port:{}\r\n", 6379));
            info.push_str("\r\n");
        }

        if include_all || section == Some("clients") {
            let server_stats = client.server_stats();
            info.push_str("# Clients\r\n");
            info.push_str(&format!(
                "connected_clients:{}\r\n",
                server_stats
                    .connected_clients
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("memory") {
            let (used_memory, used_memory_rss) = get_memory_usage();
            info.push_str("# Memory\r\n");
            info.push_str(&format!("used_memory:{}\r\n", used_memory));
            info.push_str(&format!(
                "used_memory_human:{}\r\n",
                format_bytes(used_memory)
            ));
            info.push_str(&format!("used_memory_rss:{}\r\n", used_memory_rss));
            info.push_str(&format!(
                "used_memory_rss_human:{}\r\n",
                format_bytes(used_memory_rss)
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("stats") {
            let stats = db.stats();
            let server_stats = client.server_stats();
            info.push_str("# Stats\r\n");
            info.push_str(&format!(
                "total_connections_received:{}\r\n",
                server_stats
                    .total_connections
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str(&format!(
                "keyspace_hits:{}\r\n",
                stats.hits.load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str(&format!(
                "keyspace_misses:{}\r\n",
                stats.misses.load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str(&format!(
                "expired_keys:{}\r\n",
                stats
                    .expired_keys
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("persistence") {
            let server_stats = client.server_stats();
            info.push_str("# Persistence\r\n");
            info.push_str("loading:0\r\n");
            info.push_str(&format!(
                "vdb_changes_since_last_save:{}\r\n",
                server_stats
                    .vdb_changes_since_save
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str(&format!(
                "vdb_bgsave_in_progress:{}\r\n",
                if server_stats
                    .vdb_bgsave_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    1
                } else {
                    0
                }
            ));
            let last_save_time = server_stats
                .last_vdb_save_time
                .load(std::sync::atomic::Ordering::Relaxed);
            info.push_str(&format!(
                "vdb_last_save_time:{}\r\n",
                if last_save_time == 0 {
                    // If never saved, report current time (like Redis does on startup)
                    chrono::Utc::now().timestamp() as u64
                } else {
                    last_save_time
                }
            ));
            info.push_str("vdb_last_bgsave_status:ok\r\n");
            info.push_str("vdb_last_bgsave_time_sec:0\r\n");
            info.push_str("vdb_current_bgsave_time_sec:-1\r\n");
            info.push_str("vdb_last_cow_size:0\r\n");
            info.push_str(&format!(
                "aof_enabled:{}\r\n",
                if server_stats
                    .aof_enabled
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    1
                } else {
                    0
                }
            ));
            info.push_str(&format!(
                "aof_rewrite_in_progress:{}\r\n",
                if server_stats
                    .aof_rewrite_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    1
                } else {
                    0
                }
            ));
            info.push_str("aof_rewrite_scheduled:0\r\n");
            info.push_str("aof_last_rewrite_time_sec:-1\r\n");
            info.push_str("aof_current_rewrite_time_sec:-1\r\n");
            info.push_str("aof_last_bgrewrite_status:ok\r\n");
            info.push_str("aof_last_write_status:ok\r\n");
            info.push_str("aof_last_cow_size:0\r\n");
            info.push_str("\r\n");
        }

        if include_all || section == Some("replication") {
            info.push_str("# Replication\r\n");
            info.push_str("role:master\r\n");
            info.push_str("connected_slaves:0\r\n");
            info.push_str("master_failover_state:no-failover\r\n");
            info.push_str("master_replid:0000000000000000000000000000000000000000\r\n");
            info.push_str("master_replid2:0000000000000000000000000000000000000000\r\n");
            info.push_str("master_repl_offset:0\r\n");
            info.push_str("second_repl_offset:-1\r\n");
            info.push_str("repl_backlog_active:0\r\n");
            info.push_str("repl_backlog_size:1048576\r\n");
            info.push_str("repl_backlog_first_byte_offset:0\r\n");
            info.push_str("repl_backlog_histlen:0\r\n");
            info.push_str("\r\n");
        }

        if include_all || section == Some("cpu") {
            let cpu = get_cpu_usage();
            info.push_str("# CPU\r\n");
            info.push_str(&format!("used_cpu_sys:{:.6}\r\n", cpu.sys));
            info.push_str(&format!("used_cpu_user:{:.6}\r\n", cpu.user));
            info.push_str(&format!(
                "used_cpu_sys_children:{:.6}\r\n",
                cpu.sys_children
            ));
            info.push_str(&format!(
                "used_cpu_user_children:{:.6}\r\n",
                cpu.user_children
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("modules") {
            info.push_str("# Modules\r\n");
            info.push_str("\r\n");
        }

        if include_all || section == Some("errorstats") {
            info.push_str("# Errorstats\r\n");
            info.push_str("\r\n");
        }

        if include_all || section == Some("cluster") {
            info.push_str("# Cluster\r\n");
            info.push_str("cluster_enabled:0\r\n");
            info.push_str("\r\n");
        }

        if include_all || section == Some("latencystats") {
            let metrics = client.server_metrics();
            info.push_str("# Latencystats\r\n");
            info.push_str(&format!(
                "latency_p50_us:{}\r\n",
                metrics.command_latency.percentile(50.0).as_micros()
            ));
            info.push_str(&format!(
                "latency_p99_us:{}\r\n",
                metrics.command_latency.percentile(99.0).as_micros()
            ));
            info.push_str(&format!(
                "latency_p999_us:{}\r\n",
                metrics.command_latency.percentile(99.9).as_micros()
            ));
            info.push_str(&format!(
                "latency_avg_us:{}\r\n",
                metrics.command_latency.average().as_micros()
            ));
            info.push_str(&format!(
                "total_commands_processed:{}\r\n",
                metrics
                    .commands_processed
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
            info.push_str(&format!("ops_per_sec:{:.2}\r\n", metrics.ops_per_second()));
            info.push_str("\r\n");
        }

        if include_all || section == Some("keyspace") {
            info.push_str("# Keyspace\r\n");
            let keys = db.len();
            if keys > 0 {
                info.push_str(&format!(
                    "db{}:keys={},expires=0\r\n",
                    client.db_index(),
                    keys
                ));
            }
            info.push_str("\r\n");
        }

        Ok(Frame::Bulk(Bytes::from(info)))
    })
}

/// DBSIZE
pub fn cmd_dbsize(
    _cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move { Ok(Frame::Integer(db.len() as i64)) })
}

/// FLUSHDB [ASYNC|SYNC]
pub fn cmd_flushdb(
    _cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        db.flush();
        Ok(Frame::ok())
    })
}

/// FLUSHALL [ASYNC|SYNC]
pub fn cmd_flushall(
    _cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        db.flush();
        Ok(Frame::ok())
    })
}

/// TIME
pub fn cmd_time(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let now = chrono::Utc::now();
        let secs = now.timestamp();
        let micros = now.timestamp_subsec_micros();

        Ok(Frame::Array(vec![
            Frame::Bulk(Bytes::from(secs.to_string())),
            Frame::Bulk(Bytes::from(micros.to_string())),
        ]))
    })
}

/// COMMAND [COUNT | DOCS | GETKEYS | INFO | LIST]
pub fn cmd_command(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            // Return command count for now
            Ok(Frame::Array(vec![]))
        } else {
            let subcommand = cmd.get_str(0)?.to_uppercase();
            match subcommand.as_str() {
                "COUNT" => Ok(Frame::Integer(100)), // Approximate
                "DOCS" => Ok(Frame::Array(vec![])),
                "INFO" => Ok(Frame::Array(vec![])),
                "LIST" => Ok(Frame::Array(vec![])),
                _ => Ok(Frame::Array(vec![])),
            }
        }
    })
}

/// DEBUG subcommand
pub fn cmd_debug(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for DEBUG".to_string(),
            ));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "SLEEP" => {
                let seconds = cmd.get_f64(1)?;
                tokio::time::sleep(std::time::Duration::from_secs_f64(seconds)).await;
                Ok(Frame::ok())
            }
            "OBJECT" => {
                Ok(Frame::Simple("Value at:0x000000000000 refcount:1 encoding:raw serializedlength:1 lru:0 lru_seconds_idle:0".to_string()))
            }
            "DIGEST" => {
                // Compute XXH3 digest of database contents
                let digest = db.compute_digest();
                Ok(Frame::Bulk(Bytes::from(digest)))
            }
            "DIGEST-VALUE" => {
                // Compute digest of a specific key's value
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error(
                        "ERR DEBUG DIGEST-VALUE requires at least one key".to_string(),
                    ));
                }
                let mut results = Vec::new();
                for arg in &cmd.args[1..] {
                    let key = Key::from(arg.clone());
                    let digest = db.compute_key_digest(&key);
                    results.push(Frame::Bulk(Bytes::from(digest)));
                }
                Ok(Frame::Array(results))
            }
            "HTSTATS" => {
                // Hash table statistics
                let db_num = cmd.args.get(1)
                    .and_then(|s| std::str::from_utf8(s).ok())
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                let stats = db.get_hashtable_stats(db_num);
                Ok(Frame::Bulk(Bytes::from(stats)))
            }
            "STRUCTSIZE" => {
                // Report size of internal data structures
                use crate::types::ViatorValue;
                let sizes = format!(
                    "bits:64 robj:{} sdshdr:{} dictEntry:{} dictht:{}\n\
                     skiplistNode:{} skiplist:{} quicklist:{} quicklistNode:{}\n\
                     client:{} cluster:{} clusterNode:{}\n",
                    std::mem::size_of::<ViatorValue>(),
                    32, // Approximate SDS header
                    std::mem::size_of::<(Key, ViatorValue)>(),
                    std::mem::size_of::<std::collections::HashMap<Key, ViatorValue>>(),
                    64, // Approximate skiplist node
                    32, // Approximate skiplist
                    48, // Approximate quicklist
                    24, // Approximate quicklist node
                    256, // Approximate client state
                    128, // Approximate cluster state
                    64, // Approximate cluster node
                );
                Ok(Frame::Bulk(Bytes::from(sizes)))
            }
            "SEGFAULT" => {
                // Intentional crash - only in debug builds
                #[cfg(debug_assertions)]
                {
                    panic!("DEBUG SEGFAULT called");
                }
                #[cfg(not(debug_assertions))]
                {
                    Ok(Frame::Error(
                        "ERR DEBUG SEGFAULT disabled in release builds".to_string(),
                    ))
                }
            }
            "SET-ACTIVE-EXPIRE" => {
                // Toggle active expiry (0 = disabled, 1 = enabled)
                let enabled = cmd.args.get(1)
                    .and_then(|s| std::str::from_utf8(s).ok())
                    .and_then(|s| s.parse::<u8>().ok())
                    .unwrap_or(1);
                db.set_active_expire_enabled(enabled != 0);
                Ok(Frame::ok())
            }
            "QUICKLIST-PACKED-THRESHOLD" => {
                // Get/set quicklist threshold
                if let Some(threshold) = cmd.args.get(1)
                    .and_then(|s| std::str::from_utf8(s).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    db.set_quicklist_packed_threshold(threshold);
                }
                let current = db.get_quicklist_packed_threshold();
                Ok(Frame::Integer(current as i64))
            }
            "RELOAD" => {
                // Reload VDB (for testing)
                Ok(Frame::Error(
                    "ERR DEBUG RELOAD not implemented - use SHUTDOWN SAVE && restart".to_string(),
                ))
            }
            "CRASH-AND-RECOVER" | "CRASH-AND-ABORT" => {
                Ok(Frame::Error(
                    "ERR DEBUG crash commands disabled for safety".to_string(),
                ))
            }
            "PAUSE-CRON" => {
                // Pause cron jobs (useful for debugging)
                let pause_ms = cmd.args.get(1)
                    .and_then(|s| std::str::from_utf8(s).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                if pause_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(pause_ms)).await;
                }
                Ok(Frame::ok())
            }
            "CHANGE-REPL-STATE" => {
                Ok(Frame::Error(
                    "ERR DEBUG CHANGE-REPL-STATE not implemented".to_string(),
                ))
            }
            "STRINGMATCH-TEST" | "LISTPACK-ENTRIES" => {
                Ok(Frame::ok())
            }
            "HELP" => {
                let help = vec![
                    Frame::Bulk(Bytes::from("DEBUG SLEEP <seconds> -- Pause for seconds.")),
                    Frame::Bulk(Bytes::from("DEBUG OBJECT <key> -- Show object info.")),
                    Frame::Bulk(Bytes::from("DEBUG DIGEST -- Compute database digest.")),
                    Frame::Bulk(Bytes::from("DEBUG DIGEST-VALUE <key> [key...] -- Compute key digest.")),
                    Frame::Bulk(Bytes::from("DEBUG HTSTATS <dbid> -- Show hashtable stats.")),
                    Frame::Bulk(Bytes::from("DEBUG STRUCTSIZE -- Show struct sizes.")),
                    Frame::Bulk(Bytes::from("DEBUG SET-ACTIVE-EXPIRE <0|1> -- Toggle active expiry.")),
                    Frame::Bulk(Bytes::from("DEBUG PAUSE-CRON <ms> -- Pause for ms.")),
                    Frame::Bulk(Bytes::from("DEBUG SEGFAULT -- Crash server (debug only).")),
                    Frame::Bulk(Bytes::from("DEBUG HELP -- Show this help.")),
                ];
                Ok(Frame::Array(help))
            }
            _ => Ok(Frame::ok()),
        }
    })
}

/// DUMP key - Serialize value at key in Redis-compatible format
/// Format: [type_byte][serialized_data...][rdb_version][crc64]
pub fn cmd_dump(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        // Return null if key doesn't exist
        let value = match db.get(&key) {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        // Serialize the value
        let mut serialized = Vec::new();

        match &value {
            ViatorValue::String(s) => {
                // Type: 0 = string
                serialized.push(0x00);
                // Length-prefixed string
                let len = s.len();
                serialize_length(&mut serialized, len);
                serialized.extend_from_slice(s.as_ref());
            }
            ViatorValue::List(list) => {
                // Type: 1 = list (quicklist encoding)
                serialized.push(0x01);
                let guard = list.read();
                let items: Vec<_> = guard.iter().collect();
                serialize_length(&mut serialized, items.len());
                for item in items {
                    serialize_length(&mut serialized, item.len());
                    serialized.extend_from_slice(item.as_ref());
                }
            }
            ViatorValue::Set(set) => {
                // Type: 2 = set
                serialized.push(0x02);
                let guard = set.read();
                let items = guard.members();
                serialize_length(&mut serialized, items.len());
                for item in items {
                    serialize_length(&mut serialized, item.len());
                    serialized.extend_from_slice(item.as_ref());
                }
            }
            ViatorValue::ZSet(zset) => {
                // Type: 3 = sorted set
                serialized.push(0x03);
                let guard = zset.read();
                let count = guard.len();
                serialize_length(&mut serialized, count);
                for entry in guard.iter() {
                    serialize_length(&mut serialized, entry.member.len());
                    serialized.extend_from_slice(entry.member.as_ref());
                    serialized.extend_from_slice(&entry.score.to_be_bytes());
                }
            }
            ViatorValue::Hash(hash) => {
                // Type: 4 = hash
                serialized.push(0x04);
                let guard = hash.read();
                let count = guard.len();
                serialize_length(&mut serialized, count);
                for (field, value) in guard.iter() {
                    serialize_length(&mut serialized, field.len());
                    serialized.extend_from_slice(field.as_ref());
                    serialize_length(&mut serialized, value.len());
                    serialized.extend_from_slice(value.as_ref());
                }
            }
            ViatorValue::Stream(stream) => {
                // Type: 5 = stream (simplified)
                serialized.push(0x05);
                let guard = stream.read();
                // Serialize as empty - streams have complex state
                serialize_length(&mut serialized, 0);
                // Store last ID
                let last_id = guard.last_id();
                serialized.extend_from_slice(&last_id.ms.to_be_bytes());
                serialized.extend_from_slice(&last_id.seq.to_be_bytes());
            }
            ViatorValue::VectorSet(vset) => {
                // Type: 6 = vector set
                serialized.push(0x06);
                let guard = vset.read();
                let count = guard.len();
                serialize_length(&mut serialized, count);
                // Vector set serialization is simplified - serialize count only
                // Full vector data would require iterating with get_vector
            }
        }

        // RDB version byte (9)
        serialized.push(0x09);

        // CRC64 checksum (8 bytes) - use simple checksum for now
        let crc = compute_crc64(&serialized);
        serialized.extend_from_slice(&crc.to_le_bytes());

        Ok(Frame::Bulk(Bytes::from(serialized)))
    })
}

/// Helper to serialize length in Redis RDB format
fn serialize_length(buf: &mut Vec<u8>, len: usize) {
    if len < 64 {
        // 00xxxxxx - 6-bit length
        buf.push(len as u8);
    } else if len < 16384 {
        // 01xxxxxx xxxxxxxx - 14-bit length
        buf.push(0x40 | ((len >> 8) as u8));
        buf.push(len as u8);
    } else {
        // 10000000 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx - 32-bit length
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Helper to deserialize length from Redis RDB format
fn deserialize_length(data: &[u8], pos: &mut usize) -> Option<usize> {
    if *pos >= data.len() {
        return None;
    }

    let first = data[*pos];
    *pos += 1;

    match first >> 6 {
        0 => Some(first as usize),
        1 => {
            if *pos >= data.len() {
                return None;
            }
            let second = data[*pos];
            *pos += 1;
            Some((((first & 0x3F) as usize) << 8) | (second as usize))
        }
        2 => {
            if *pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            Some(len)
        }
        _ => None,
    }
}

/// Simple CRC64 computation (ECMA polynomial)
fn compute_crc64(data: &[u8]) -> u64 {
    let mut crc: u64 = 0;

    for &byte in data {
        let index = ((crc ^ (byte as u64)) & 0xFF) as usize;
        crc = CRC64_TABLE[index] ^ (crc >> 8);
    }

    crc
}

// CRC64 lookup table
const CRC64_TABLE: [u64; 256] = {
    const POLY: u64 = 0xC96C5795D7870F42;
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
pub fn cmd_restore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let ttl = cmd.get_i64(1)?;
        let serialized = &cmd.args[2];

        // Parse options
        let mut replace = false;
        let mut absttl = false;
        let mut _idletime: Option<u64> = None;
        let mut _freq: Option<u8> = None;

        let mut idx = 3;
        while idx < cmd.args.len() {
            let opt = cmd.get_str(idx)?.to_uppercase();
            match opt.as_str() {
                "REPLACE" => {
                    replace = true;
                    idx += 1;
                }
                "ABSTTL" => {
                    absttl = true;
                    idx += 1;
                }
                "IDLETIME" => {
                    idx += 1;
                    if idx < cmd.args.len() {
                        _idletime = Some(cmd.get_u64(idx)?);
                        idx += 1;
                    }
                }
                "FREQ" => {
                    idx += 1;
                    if idx < cmd.args.len() {
                        _freq = Some(cmd.get_u64(idx)? as u8);
                        idx += 1;
                    }
                }
                _ => {
                    return Ok(Frame::Error(format!("ERR Unknown option '{}'", opt)));
                }
            }
        }

        // Check if key exists and REPLACE not specified
        if db.get(&key).is_some() && !replace {
            return Ok(Frame::Error("BUSYKEY Target key name already exists.".to_string()));
        }

        // Verify minimum length (type + version + crc)
        if serialized.len() < 10 {
            return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
        }

        // Verify CRC
        let payload_len = serialized.len() - 8;
        let stored_crc = u64::from_le_bytes([
            serialized[payload_len],
            serialized[payload_len + 1],
            serialized[payload_len + 2],
            serialized[payload_len + 3],
            serialized[payload_len + 4],
            serialized[payload_len + 5],
            serialized[payload_len + 6],
            serialized[payload_len + 7],
        ]);
        let computed_crc = compute_crc64(&serialized[..payload_len]);
        if stored_crc != computed_crc {
            return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
        }

        // Parse type byte
        let type_byte = serialized[0];
        let mut pos = 1;
        let data = &serialized[..payload_len - 1]; // Exclude version byte

        let value = match type_byte {
            0x00 => {
                // String
                let len = match deserialize_length(data, &mut pos) {
                    Some(l) => l,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                if pos + len > data.len() {
                    return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                }
                let s = Bytes::copy_from_slice(&data[pos..pos + len]);
                ViatorValue::String(s)
            }
            0x01 => {
                // List
                let count = match deserialize_length(data, &mut pos) {
                    Some(c) => c,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                let list = ViatorValue::new_list();
                let list_ref = list.as_list().unwrap();
                let mut guard = list_ref.write();
                for _ in 0..count {
                    let len = match deserialize_length(data, &mut pos) {
                        Some(l) => l,
                        None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                    };
                    if pos + len > data.len() {
                        return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                    }
                    guard.push_back(Bytes::copy_from_slice(&data[pos..pos + len]));
                    pos += len;
                }
                drop(guard);
                list
            }
            0x02 => {
                // Set
                let count = match deserialize_length(data, &mut pos) {
                    Some(c) => c,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                let set = ViatorValue::new_set();
                let set_ref = set.as_set().unwrap();
                let mut guard = set_ref.write();
                for _ in 0..count {
                    let len = match deserialize_length(data, &mut pos) {
                        Some(l) => l,
                        None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                    };
                    if pos + len > data.len() {
                        return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                    }
                    guard.add(Bytes::copy_from_slice(&data[pos..pos + len]));
                    pos += len;
                }
                drop(guard);
                set
            }
            0x03 => {
                // Sorted Set
                let count = match deserialize_length(data, &mut pos) {
                    Some(c) => c,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                let zset = ViatorValue::new_zset();
                let zset_ref = zset.as_zset().unwrap();
                let mut guard = zset_ref.write();
                for _ in 0..count {
                    let len = match deserialize_length(data, &mut pos) {
                        Some(l) => l,
                        None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                    };
                    if pos + len + 8 > data.len() {
                        return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                    }
                    let member = Bytes::copy_from_slice(&data[pos..pos + len]);
                    pos += len;
                    let score = f64::from_be_bytes([
                        data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                        data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
                    ]);
                    pos += 8;
                    guard.add(member, score);
                }
                drop(guard);
                zset
            }
            0x04 => {
                // Hash
                let count = match deserialize_length(data, &mut pos) {
                    Some(c) => c,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                let hash = ViatorValue::new_hash();
                let hash_ref = hash.as_hash().unwrap();
                let mut guard = hash_ref.write();
                for _ in 0..count {
                    let field_len = match deserialize_length(data, &mut pos) {
                        Some(l) => l,
                        None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                    };
                    if pos + field_len > data.len() {
                        return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                    }
                    let field = Bytes::copy_from_slice(&data[pos..pos + field_len]);
                    pos += field_len;

                    let value_len = match deserialize_length(data, &mut pos) {
                        Some(l) => l,
                        None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                    };
                    if pos + value_len > data.len() {
                        return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                    }
                    let value = Bytes::copy_from_slice(&data[pos..pos + value_len]);
                    pos += value_len;

                    guard.insert(field, value);
                }
                drop(guard);
                hash
            }
            0x05 => {
                // Stream (simplified - create empty stream)
                let _count = match deserialize_length(data, &mut pos) {
                    Some(c) => c,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                // Skip to last ID
                if pos + 16 > data.len() {
                    return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string()));
                }
                ViatorValue::new_stream()
            }
            0x06 => {
                // Vector Set (simplified - create empty)
                let _count = match deserialize_length(data, &mut pos) {
                    Some(c) => c,
                    None => return Ok(Frame::Error("ERR DUMP payload version or checksum are wrong".to_string())),
                };
                ViatorValue::new_vectorset()
            }
            _ => {
                return Ok(Frame::Error("ERR Unknown value type in DUMP payload".to_string()));
            }
        };

        // Set the value
        db.set(key.clone(), value);

        // Handle TTL
        if ttl > 0 {
            let expire_at = if absttl {
                // TTL is absolute Unix timestamp in milliseconds
                ttl
            } else {
                // TTL is relative in milliseconds
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                now + ttl
            };
            db.expire(&key, Expiry::At(expire_at));
        }

        Ok(Frame::ok())
    })
}

/// MEMORY DOCTOR|HELP|MALLOC-SIZE|PURGE|STATS|USAGE key [SAMPLES count]
pub fn cmd_memory(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for 'memory' command".to_string(),
            ));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "DOCTOR" => {
                // Analyze memory and provide diagnostics
                let (used_memory, used_memory_rss) = get_memory_usage();
                let mut report = String::new();

                // Header
                report.push_str("Sam, I have a few memory concerns:\n\n");

                let mut issues_found = 0;

                // Check RSS vs used memory ratio (fragmentation)
                if used_memory > 0 && used_memory_rss > 0 {
                    let frag_ratio = used_memory_rss as f64 / used_memory as f64;
                    if frag_ratio > 1.4 {
                        issues_found += 1;
                        report.push_str(&format!(
                            "* High memory fragmentation detected (ratio: {:.2})\n\
                             Fragmentation occurs when the allocator is unable to release memory back to the OS.\n\
                             Consider restarting the server if this persists.\n\n",
                            frag_ratio
                        ));
                    }
                }

                // Check total memory size (warn if > 75% of typical system RAM)
                let large_memory_threshold = 4_000_000_000u64; // 4GB
                if used_memory_rss as u64 > large_memory_threshold {
                    issues_found += 1;
                    report.push_str(&format!(
                        "* Large memory usage detected ({} bytes RSS)\n\
                         Consider setting maxmemory to control memory usage.\n\
                         Current memory usage may affect system stability.\n\n",
                        used_memory_rss
                    ));
                }

                // Check database statistics
                let key_count = db.len();
                if key_count > 0 {
                    let bytes_per_key = if key_count > 0 {
                        used_memory / key_count
                    } else {
                        0
                    };

                    if bytes_per_key > 10_000 {
                        issues_found += 1;
                        report.push_str(&format!(
                            "* High average key size ({} bytes/key with {} keys)\n\
                             Large values can cause memory issues and slow operations.\n\
                             Consider optimizing value sizes or using streaming for large data.\n\n",
                            bytes_per_key, key_count
                        ));
                    }
                }

                // Check for expired keys backlog
                let stats = db.stats();
                if stats.expired_keys.load(std::sync::atomic::Ordering::Relaxed) > 10_000 {
                    issues_found += 1;
                    report.push_str(
                        "* Large number of expired keys processed.\n\
                         Consider reviewing TTL settings and expiry patterns.\n\n"
                    );
                }

                // Report summary
                if issues_found == 0 {
                    report.clear();
                    report.push_str("Sam, I have no memory problems\n");
                } else {
                    report.push_str(&format!(
                        "Summary: {} potential issue(s) detected.\n\
                         Memory: {} bytes used, {} bytes RSS\n\
                         Keys: {}\n",
                        issues_found, used_memory, used_memory_rss, key_count
                    ));
                }

                Ok(Frame::Bulk(Bytes::from(report)))
            }
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("MEMORY DOCTOR"),
                Frame::bulk("MEMORY HELP"),
                Frame::bulk("MEMORY MALLOC-SIZE"),
                Frame::bulk("MEMORY PURGE"),
                Frame::bulk("MEMORY STATS"),
                Frame::bulk("MEMORY USAGE key [SAMPLES count]"),
            ])),
            "MALLOC-SIZE" => {
                // Return a stub value
                Ok(Frame::Integer(0))
            }
            "PURGE" => Ok(Frame::ok()),
            "STATS" => Ok(Frame::Bulk(Bytes::from(
                "peak.allocated:0\ntotal.allocated:0\nstartup.allocated:0\nreplication.backlog:0\nclients.slaves:0\nclients.normal:0\naof.buffer:0",
            ))),
            "USAGE" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'memory usage' command".to_string(),
                    ));
                }

                let key = Key::from(cmd.args[1].clone());
                match db.get(&key) {
                    Some(value) => {
                        // Return estimated memory usage
                        let size = value.memory_usage();
                        Ok(Frame::Integer(size as i64))
                    }
                    None => Ok(Frame::Null),
                }
            }
            _ => Ok(Frame::Error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            ))),
        }
    })
}

/// SLOWLOG GET|LEN|RESET [count]
pub fn cmd_slowlog(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for 'slowlog' command".to_string(),
            ));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "GET" => {
                // Return empty slow log
                Ok(Frame::Array(vec![]))
            }
            "LEN" => Ok(Frame::Integer(0)),
            "RESET" => Ok(Frame::ok()),
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("SLOWLOG GET [count]"),
                Frame::bulk("SLOWLOG LEN"),
                Frame::bulk("SLOWLOG RESET"),
            ])),
            _ => Ok(Frame::Error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            ))),
        }
    })
}

/// ACL CAT|DELUSER|DRYRUN|GENPASS|GETUSER|LIST|LOAD|LOG|SAVE|SETUSER|USERS|WHOAMI
pub fn cmd_acl(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Ok(Frame::Error(
                "ERR wrong number of arguments for 'acl' command".to_string(),
            ));
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "CAT" => {
                // Return command categories
                if cmd.args.len() > 1 {
                    // Return commands in category
                    Ok(Frame::Array(vec![]))
                } else {
                    Ok(Frame::Array(vec![
                        Frame::bulk("read"),
                        Frame::bulk("write"),
                        Frame::bulk("set"),
                        Frame::bulk("sortedset"),
                        Frame::bulk("list"),
                        Frame::bulk("hash"),
                        Frame::bulk("string"),
                        Frame::bulk("bitmap"),
                        Frame::bulk("hyperloglog"),
                        Frame::bulk("geo"),
                        Frame::bulk("stream"),
                        Frame::bulk("pubsub"),
                        Frame::bulk("admin"),
                        Frame::bulk("fast"),
                        Frame::bulk("slow"),
                        Frame::bulk("blocking"),
                        Frame::bulk("dangerous"),
                        Frame::bulk("connection"),
                        Frame::bulk("transaction"),
                        Frame::bulk("scripting"),
                    ]))
                }
            }
            "DELUSER" => {
                // Return number of users deleted (stub)
                Ok(Frame::Integer(0))
            }
            "DRYRUN" => {
                // Always allow in stub mode
                Ok(Frame::ok())
            }
            "GENPASS" => {
                // Generate random password
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let password: String = (0..64)
                    .map(|_| {
                        let idx = rng.gen_range(0..16);
                        "0123456789abcdef".chars().nth(idx).unwrap()
                    })
                    .collect();
                Ok(Frame::Bulk(Bytes::from(password)))
            }
            "GETUSER" => {
                if cmd.args.len() < 2 {
                    return Ok(Frame::Error(
                        "ERR wrong number of arguments for 'acl getuser' command".to_string(),
                    ));
                }
                // Return user info (stub for default user)
                Ok(Frame::Array(vec![
                    Frame::bulk("flags"),
                    Frame::Array(vec![
                        Frame::bulk("on"),
                        Frame::bulk("allkeys"),
                        Frame::bulk("allcommands"),
                    ]),
                    Frame::bulk("passwords"),
                    Frame::Array(vec![]),
                    Frame::bulk("commands"),
                    Frame::bulk("+@all"),
                    Frame::bulk("keys"),
                    Frame::Array(vec![Frame::bulk("*")]),
                    Frame::bulk("channels"),
                    Frame::Array(vec![Frame::bulk("*")]),
                ]))
            }
            "LIST" => {
                // Return list of users
                Ok(Frame::Array(vec![Frame::bulk(
                    "user default on nopass ~* &* +@all",
                )]))
            }
            "LOAD" => Ok(Frame::ok()),
            "LOG" => {
                // Return empty log
                Ok(Frame::Array(vec![]))
            }
            "SAVE" => Ok(Frame::ok()),
            "SETUSER" => Ok(Frame::ok()),
            "USERS" => Ok(Frame::Array(vec![Frame::bulk("default")])),
            "WHOAMI" => Ok(Frame::Bulk(Bytes::from("default"))),
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("ACL CAT [category]"),
                Frame::bulk("ACL DELUSER username [username ...]"),
                Frame::bulk("ACL DRYRUN username command [arg ...]"),
                Frame::bulk("ACL GENPASS [bits]"),
                Frame::bulk("ACL GETUSER username"),
                Frame::bulk("ACL LIST"),
                Frame::bulk("ACL LOAD"),
                Frame::bulk("ACL LOG [count|RESET]"),
                Frame::bulk("ACL SAVE"),
                Frame::bulk("ACL SETUSER username [rule [rule ...]]"),
                Frame::bulk("ACL USERS"),
                Frame::bulk("ACL WHOAMI"),
            ])),
            _ => Ok(Frame::Error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            ))),
        }
    })
}

/// WAIT numreplicas timeout - Wait for replicas to acknowledge writes
pub fn cmd_wait(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;

        // In standalone mode, immediately return 0 (no replicas)
        Ok(Frame::Integer(0))
    })
}

/// BGSAVE [SCHEDULE]
/// Start background save (VDB snapshot)
pub fn cmd_bgsave(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // In this implementation, we return success but don't actually persist
        // A full implementation would spawn a background task for VDB saving
        Ok(Frame::Simple("Background saving started".to_string()))
    })
}

/// BGREWRITEAOF
/// Start background rewrite of AOF file
pub fn cmd_bgrewriteaof(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        Ok(Frame::Simple(
            "Background append only file rewriting started".to_string(),
        ))
    })
}

/// SAVE
/// Synchronous save (blocking)
pub fn cmd_save(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Stub: return OK
        Ok(Frame::ok())
    })
}

/// LASTSAVE
/// Return Unix timestamp of last successful save
pub fn cmd_lastsave(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let now = chrono::Utc::now().timestamp();
        Ok(Frame::Integer(now))
    })
}

/// CONFIG GET|SET|REWRITE|RESETSTAT parameter [value]
pub fn cmd_config(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "GET" => {
                if cmd.args.len() < 2 {
                    return Err(crate::error::CommandError::WrongArity {
                        command: "CONFIG GET".to_string(),
                    }
                    .into());
                }
                let pattern = cmd.get_str(1)?;

                // Return common configuration values
                let mut result = Vec::new();

                // Match common patterns
                let configs = [
                    ("maxclients", "10000"),
                    ("maxmemory", "0"),
                    ("maxmemory-policy", "noeviction"),
                    ("timeout", "0"),
                    ("tcp-keepalive", "300"),
                    ("databases", "16"),
                    ("save", "3600 1 300 100 60 10000"),
                    ("appendonly", "no"),
                    ("appendfsync", "everysec"),
                    ("loglevel", "notice"),
                    ("logfile", ""),
                    ("dir", "."),
                    ("dbfilename", "dump.vdb"),
                    ("appendfilename", "appendonly.aof"),
                    ("bind", "127.0.0.1"),
                    ("port", "6379"),
                    ("requirepass", ""),
                    ("masterauth", ""),
                    ("replica-read-only", "yes"),
                    ("slave-read-only", "yes"),
                    ("lazyfree-lazy-eviction", "no"),
                    ("lazyfree-lazy-expire", "no"),
                    ("lazyfree-lazy-server-del", "no"),
                    ("replica-lazy-flush", "no"),
                    ("lua-time-limit", "5000"),
                    ("slowlog-log-slower-than", "10000"),
                    ("slowlog-max-len", "128"),
                    ("hash-max-listpack-entries", "512"),
                    ("hash-max-listpack-value", "64"),
                    ("list-max-listpack-size", "-2"),
                    ("list-compress-depth", "0"),
                    ("set-max-intset-entries", "512"),
                    ("zset-max-listpack-entries", "128"),
                    ("zset-max-listpack-value", "64"),
                    ("hll-sparse-max-bytes", "3000"),
                    ("stream-node-max-bytes", "4096"),
                    ("stream-node-max-entries", "100"),
                    ("activerehashing", "yes"),
                    ("activedefrag", "no"),
                    ("protected-mode", "yes"),
                    (
                        "client-output-buffer-limit",
                        "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
                    ),
                    ("hz", "10"),
                    ("dynamic-hz", "yes"),
                    ("aof-rewrite-incremental-fsync", "yes"),
                    ("vdb-save-incremental-fsync", "yes"),
                ];

                for (key, value) in configs {
                    if pattern == "*" || pattern == key || glob_match(pattern, key) {
                        result.push(Frame::bulk(key));
                        result.push(Frame::bulk(value));
                    }
                }

                Ok(Frame::Array(result))
            }
            "SET" => {
                if cmd.args.len() < 3 {
                    return Err(crate::error::CommandError::WrongArity {
                        command: "CONFIG SET".to_string(),
                    }
                    .into());
                }
                // Stub: acknowledge the set but don't actually persist
                Ok(Frame::ok())
            }
            "REWRITE" => Ok(Frame::ok()),
            "RESETSTAT" => Ok(Frame::ok()),
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("CONFIG GET <pattern>"),
                Frame::bulk("CONFIG SET <parameter> <value>"),
                Frame::bulk("CONFIG REWRITE"),
                Frame::bulk("CONFIG RESETSTAT"),
            ])),
            _ => Err(crate::error::CommandError::SyntaxError.into()),
        }
    })
}

/// Simple glob matching for CONFIG GET
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let (prefix, suffix) = (parts[0], parts[1]);
            return text.starts_with(prefix) && text.ends_with(suffix);
        }
    }
    pattern == text
}

/// SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT]
pub fn cmd_shutdown(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Parse options
        let mut abort = false;

        for arg in &cmd.args {
            let opt = std::str::from_utf8(arg).unwrap_or("").to_uppercase();
            if opt == "ABORT" {
                abort = true;
            }
        }

        if abort {
            // Cancel pending shutdown
            return Ok(Frame::ok());
        }

        // In a real implementation, this would initiate shutdown
        // For this stub, we just return OK
        // Note: A real server would exit here
        Ok(Frame::ok())
    })
}

/// REPLICAOF host port | NO ONE
/// Configure server as replica of another instance
pub fn cmd_replicaof(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let arg1 = cmd.get_str(0)?.to_uppercase();

        if arg1 == "NO" {
            // REPLICAOF NO ONE - become master
            return Ok(Frame::ok());
        }

        // REPLICAOF host port
        cmd.require_exact_args(2)?;

        // Stub: acknowledge but don't actually replicate
        Ok(Frame::ok())
    })
}

/// SLAVEOF host port | NO ONE (deprecated alias for REPLICAOF)
pub fn cmd_slaveof(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    cmd_replicaof(cmd, db, client)
}

/// LATENCY DOCTOR|GRAPH|HELP|HISTOGRAM|HISTORY|LATEST|RESET
pub fn cmd_latency(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "DOCTOR" => Ok(Frame::Bulk(Bytes::from(
                "I have no latency reports to show you at this time.",
            ))),
            "GRAPH" => {
                if cmd.args.len() < 2 {
                    return Err(crate::error::CommandError::WrongArity {
                        command: "LATENCY GRAPH".to_string(),
                    }
                    .into());
                }
                // Return empty graph
                Ok(Frame::Bulk(Bytes::new()))
            }
            "HISTOGRAM" => {
                // Return empty histogram
                Ok(Frame::Array(vec![]))
            }
            "HISTORY" => {
                if cmd.args.len() < 2 {
                    return Err(crate::error::CommandError::WrongArity {
                        command: "LATENCY HISTORY".to_string(),
                    }
                    .into());
                }
                // Return empty history
                Ok(Frame::Array(vec![]))
            }
            "LATEST" => {
                // Return empty latest
                Ok(Frame::Array(vec![]))
            }
            "RESET" => Ok(Frame::ok()),
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("LATENCY DOCTOR"),
                Frame::bulk("LATENCY GRAPH event"),
                Frame::bulk("LATENCY HISTOGRAM [command ...]"),
                Frame::bulk("LATENCY HISTORY event"),
                Frame::bulk("LATENCY LATEST"),
                Frame::bulk("LATENCY RESET [event ...]"),
            ])),
            _ => Err(crate::error::CommandError::SyntaxError.into()),
        }
    })
}

/// MODULE LIST|LOAD|LOADEX|UNLOAD
pub fn cmd_module(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "LIST" => {
                // No modules loaded
                Ok(Frame::Array(vec![]))
            }
            "LOAD" | "LOADEX" => {
                // Modules not supported
                Err(crate::error::CommandError::InvalidArgument {
                    command: "MODULE".to_string(),
                    arg: "modules not supported".to_string(),
                }
                .into())
            }
            "UNLOAD" => Err(crate::error::CommandError::InvalidArgument {
                command: "MODULE UNLOAD".to_string(),
                arg: "no such module".to_string(),
            }
            .into()),
            "HELP" => Ok(Frame::Array(vec![
                Frame::bulk("MODULE LIST"),
                Frame::bulk("MODULE LOAD <path> [arg ...]"),
                Frame::bulk("MODULE LOADEX <path> [CONFIG name value ...] [ARGS arg ...]"),
                Frame::bulk("MODULE UNLOAD <name>"),
            ])),
            _ => Err(crate::error::CommandError::SyntaxError.into()),
        }
    })
}

/// SWAPDB index1 index2
/// Swap two databases
pub fn cmd_swapdb(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;

        let _idx1 = cmd.get_u64(0)?;
        let _idx2 = cmd.get_u64(1)?;

        // Stub: return OK but don't actually swap
        // Full implementation would swap database contents
        Ok(Frame::ok())
    })
}

/// LOLWUT [VERSION version]
/// Easter egg command that displays art
pub fn cmd_lolwut(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let version = if cmd.args.len() >= 2 {
            let opt = cmd.get_str(0)?.to_uppercase();
            if opt == "VERSION" {
                cmd.get_u64(1)? as u32
            } else {
                6
            }
        } else {
            6
        };

        let art = match version {
            5 => {
                r#"
      ^__^
     (oo)\_______
     (__)\       )\/\
         ||----w |
         ||     ||
Viator - A Rust implementation
"#
            }
            _ => {
                r#"
Viator LOLWUT
              ___
             /   \
            |     |
            |     |
             \___/
Version 6 - Dragon
"#
            }
        };

        Ok(Frame::Bulk(Bytes::from(art.trim())))
    })
}

/// FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]
pub fn cmd_failover(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Parse options
        let mut abort = false;

        for arg in &cmd.args {
            let opt = std::str::from_utf8(arg).unwrap_or("").to_uppercase();
            if opt == "ABORT" {
                abort = true;
            }
        }

        if abort {
            return Ok(Frame::ok());
        }

        // In standalone mode, failover is not supported
        Ok(Frame::Error(
            "ERR FAILOVER requires connected replicas".to_string(),
        ))
    })
}

/// ROLE
/// Return the role of the server (master/slave)
pub fn cmd_role(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Return master role with no replicas
        Ok(Frame::Array(vec![
            Frame::bulk("master"),
            Frame::Integer(0),    // Replication offset
            Frame::Array(vec![]), // Empty list of replicas
        ]))
    })
}

/// PSYNC replicationid offset
/// Partial resynchronization command (used by replicas)
pub fn cmd_psync(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Return FULLRESYNC with a new replication ID
        Ok(Frame::Simple(
            "FULLRESYNC 0000000000000000000000000000000000000000 0".to_string(),
        ))
    })
}

/// REPLCONF option value [option value ...]
/// Replication configuration command
pub fn cmd_replconf(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move { Ok(Frame::ok()) })
}
