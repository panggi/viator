//! Server command implementations.

use super::ParsedCommand;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::server::metrics::{format_bytes, get_memory_usage};
use crate::storage::Db;
use crate::types::Key;
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
            info.push_str("# CPU\r\n");
            info.push_str("used_cpu_sys:0.000000\r\n");
            info.push_str("used_cpu_user:0.000000\r\n");
            info.push_str("used_cpu_sys_children:0.000000\r\n");
            info.push_str("used_cpu_user_children:0.000000\r\n");
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
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
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
            _ => Ok(Frame::ok()),
        }
    })
}

/// DUMP key - Serialize value at key
pub fn cmd_dump(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        // Return null if key doesn't exist
        if db.get(&key).is_none() {
            return Ok(Frame::Null);
        }

        // Return a placeholder serialization (real DUMP returns VDB-format serialization)
        // This stub returns the key name as bytes with a header marker
        let mut serialized = vec![0x00, 0x09]; // Version marker
        serialized.extend_from_slice(cmd.args[0].as_ref());
        serialized.extend_from_slice(&[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // CRC placeholder

        Ok(Frame::Bulk(Bytes::from(serialized)))
    })
}

/// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
pub fn cmd_restore(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        // Stub: return OK but don't actually restore
        // Real implementation would deserialize VDB format
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
            "DOCTOR" => Ok(Frame::Bulk(Bytes::from("Sam, I have no memory problems"))),
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
