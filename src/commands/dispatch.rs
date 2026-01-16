//! Fast command dispatch using perfect hashing and zero-allocation lookup.
//!
//! This module provides O(1) command lookup without heap allocations
//! by using compile-time perfect hashing and case-insensitive ASCII comparison.

/// Fast case-insensitive ASCII comparison.
/// Returns true if `a` equals `b` ignoring ASCII case.
#[inline]
pub fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Command identifier for fast dispatch.
/// Using an enum instead of strings allows for match-based dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum CommandId {
    // String commands
    Get = 0,
    Set,
    Append,
    GetRange,
    SetRange,
    Strlen,
    Mget,
    Mset,
    Incr,
    Incrby,
    Incrbyfloat,
    Decr,
    Decrby,
    Setnx,
    Setex,

    // Key commands
    Del,
    Exists,
    Expire,
    Expireat,
    Ttl,
    Pttl,
    Persist,
    Type,
    Keys,
    Scan,
    Rename,
    Touch,
    Unlink,

    // List commands
    Lpush,
    Rpush,
    Lpop,
    Rpop,
    Llen,
    Lrange,
    Lindex,
    Lset,

    // Set commands
    Sadd,
    Srem,
    Smembers,
    Sismember,
    Scard,
    Spop,

    // Sorted set commands
    Zadd,
    Zrem,
    Zscore,
    Zrank,
    Zrange,
    Zcount,
    Zcard,
    Zincrby,

    // Hash commands
    Hset,
    Hget,
    Hmset,
    Hmget,
    Hdel,
    Hexists,
    Hlen,
    Hkeys,
    Hvals,
    Hgetall,
    Hincrby,
    Hsetnx,

    // Stream commands
    Xadd,
    Xread,
    Xrange,
    Xlen,

    // Pub/Sub commands
    Subscribe,
    Unsubscribe,
    Publish,
    Pubsub,

    // Transaction commands
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,

    // Scripting commands
    Eval,
    Evalsha,
    Script,

    // Server commands
    Ping,
    Echo,
    Quit,
    Select,
    Dbsize,
    Flushdb,
    Flushall,
    Info,
    Config,
    Command,
    Client,
    Debug,
    Memory,
    Time,
    Bgsave,
    Save,
    Shutdown,
    Acl,
    Auth,
    Hello,

    // Cluster commands
    Cluster,
    Readonly,
    Readwrite,
    Asking,

    // Unknown command (fallback)
    Unknown = 0xFFFF,
}

// Include phf for compile-time perfect hashing
use phf::phf_map;

/// Static perfect hash map for command lookup.
/// All command names are uppercase for consistent lookup.
static COMMANDS: phf::Map<&'static [u8], CommandId> = phf_map! {
    // String commands
    b"GET" => CommandId::Get,
    b"SET" => CommandId::Set,
    b"APPEND" => CommandId::Append,
    b"GETRANGE" => CommandId::GetRange,
    b"SETRANGE" => CommandId::SetRange,
    b"STRLEN" => CommandId::Strlen,
    b"MGET" => CommandId::Mget,
    b"MSET" => CommandId::Mset,
    b"INCR" => CommandId::Incr,
    b"INCRBY" => CommandId::Incrby,
    b"INCRBYFLOAT" => CommandId::Incrbyfloat,
    b"DECR" => CommandId::Decr,
    b"DECRBY" => CommandId::Decrby,
    b"SETNX" => CommandId::Setnx,
    b"SETEX" => CommandId::Setex,

    // Key commands
    b"DEL" => CommandId::Del,
    b"EXISTS" => CommandId::Exists,
    b"EXPIRE" => CommandId::Expire,
    b"EXPIREAT" => CommandId::Expireat,
    b"TTL" => CommandId::Ttl,
    b"PTTL" => CommandId::Pttl,
    b"PERSIST" => CommandId::Persist,
    b"TYPE" => CommandId::Type,
    b"KEYS" => CommandId::Keys,
    b"SCAN" => CommandId::Scan,
    b"RENAME" => CommandId::Rename,
    b"TOUCH" => CommandId::Touch,
    b"UNLINK" => CommandId::Unlink,

    // List commands
    b"LPUSH" => CommandId::Lpush,
    b"RPUSH" => CommandId::Rpush,
    b"LPOP" => CommandId::Lpop,
    b"RPOP" => CommandId::Rpop,
    b"LLEN" => CommandId::Llen,
    b"LRANGE" => CommandId::Lrange,
    b"LINDEX" => CommandId::Lindex,
    b"LSET" => CommandId::Lset,

    // Set commands
    b"SADD" => CommandId::Sadd,
    b"SREM" => CommandId::Srem,
    b"SMEMBERS" => CommandId::Smembers,
    b"SISMEMBER" => CommandId::Sismember,
    b"SCARD" => CommandId::Scard,
    b"SPOP" => CommandId::Spop,

    // Sorted set commands
    b"ZADD" => CommandId::Zadd,
    b"ZREM" => CommandId::Zrem,
    b"ZSCORE" => CommandId::Zscore,
    b"ZRANK" => CommandId::Zrank,
    b"ZRANGE" => CommandId::Zrange,
    b"ZCOUNT" => CommandId::Zcount,
    b"ZCARD" => CommandId::Zcard,
    b"ZINCRBY" => CommandId::Zincrby,

    // Hash commands
    b"HSET" => CommandId::Hset,
    b"HGET" => CommandId::Hget,
    b"HMSET" => CommandId::Hmset,
    b"HMGET" => CommandId::Hmget,
    b"HDEL" => CommandId::Hdel,
    b"HEXISTS" => CommandId::Hexists,
    b"HLEN" => CommandId::Hlen,
    b"HKEYS" => CommandId::Hkeys,
    b"HVALS" => CommandId::Hvals,
    b"HGETALL" => CommandId::Hgetall,
    b"HINCRBY" => CommandId::Hincrby,
    b"HSETNX" => CommandId::Hsetnx,

    // Stream commands
    b"XADD" => CommandId::Xadd,
    b"XREAD" => CommandId::Xread,
    b"XRANGE" => CommandId::Xrange,
    b"XLEN" => CommandId::Xlen,

    // Pub/Sub commands
    b"SUBSCRIBE" => CommandId::Subscribe,
    b"UNSUBSCRIBE" => CommandId::Unsubscribe,
    b"PUBLISH" => CommandId::Publish,
    b"PUBSUB" => CommandId::Pubsub,

    // Transaction commands
    b"MULTI" => CommandId::Multi,
    b"EXEC" => CommandId::Exec,
    b"DISCARD" => CommandId::Discard,
    b"WATCH" => CommandId::Watch,
    b"UNWATCH" => CommandId::Unwatch,

    // Scripting commands
    b"EVAL" => CommandId::Eval,
    b"EVALSHA" => CommandId::Evalsha,
    b"SCRIPT" => CommandId::Script,

    // Server commands
    b"PING" => CommandId::Ping,
    b"ECHO" => CommandId::Echo,
    b"QUIT" => CommandId::Quit,
    b"SELECT" => CommandId::Select,
    b"DBSIZE" => CommandId::Dbsize,
    b"FLUSHDB" => CommandId::Flushdb,
    b"FLUSHALL" => CommandId::Flushall,
    b"INFO" => CommandId::Info,
    b"CONFIG" => CommandId::Config,
    b"COMMAND" => CommandId::Command,
    b"CLIENT" => CommandId::Client,
    b"DEBUG" => CommandId::Debug,
    b"MEMORY" => CommandId::Memory,
    b"TIME" => CommandId::Time,
    b"BGSAVE" => CommandId::Bgsave,
    b"SAVE" => CommandId::Save,
    b"SHUTDOWN" => CommandId::Shutdown,
    b"ACL" => CommandId::Acl,
    b"AUTH" => CommandId::Auth,
    b"HELLO" => CommandId::Hello,

    // Cluster commands
    b"CLUSTER" => CommandId::Cluster,
    b"READONLY" => CommandId::Readonly,
    b"READWRITE" => CommandId::Readwrite,
    b"ASKING" => CommandId::Asking,
};

/// Thread-local buffer for uppercase conversion to avoid allocation.
/// 64 bytes is enough for any Redis command name.
#[inline]
fn to_uppercase_stack(src: &[u8]) -> Option<[u8; 64]> {
    if src.len() > 64 {
        return None;
    }
    let mut buf = [0u8; 64];
    for (i, &b) in src.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }
    Some(buf)
}

/// Lookup command ID from command name bytes.
/// This uses perfect hashing for O(1) lookup without allocation.
#[inline]
pub fn lookup_command(name: &[u8]) -> CommandId {
    // Convert to uppercase on stack (no allocation)
    let upper = match to_uppercase_stack(name) {
        Some(u) => u,
        None => return CommandId::Unknown,
    };

    // Lookup in perfect hash map
    COMMANDS
        .get(&upper[..name.len()])
        .copied()
        .unwrap_or(CommandId::Unknown)
}

/// Get the command name as a static string for a CommandId.
/// Useful for error messages and logging.
#[inline]
pub fn command_name(id: CommandId) -> &'static str {
    match id {
        CommandId::Get => "GET",
        CommandId::Set => "SET",
        CommandId::Del => "DEL",
        CommandId::Ping => "PING",
        CommandId::Info => "INFO",
        CommandId::Unknown => "UNKNOWN",
        _ => "COMMAND",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_common_commands() {
        assert_eq!(lookup_command(b"GET"), CommandId::Get);
        assert_eq!(lookup_command(b"get"), CommandId::Get);
        assert_eq!(lookup_command(b"Get"), CommandId::Get);
        assert_eq!(lookup_command(b"SET"), CommandId::Set);
        assert_eq!(lookup_command(b"set"), CommandId::Set);
        assert_eq!(lookup_command(b"PING"), CommandId::Ping);
        assert_eq!(lookup_command(b"ping"), CommandId::Ping);
        assert_eq!(lookup_command(b"DEL"), CommandId::Del);
        assert_eq!(lookup_command(b"LPUSH"), CommandId::Lpush);
        assert_eq!(lookup_command(b"HSET"), CommandId::Hset);
        assert_eq!(lookup_command(b"ZADD"), CommandId::Zadd);
    }

    #[test]
    fn test_lookup_unknown() {
        assert_eq!(lookup_command(b"NOTACOMMAND"), CommandId::Unknown);
        assert_eq!(lookup_command(b""), CommandId::Unknown);
    }

    #[test]
    fn test_ascii_eq_ignore_case() {
        assert!(ascii_eq_ignore_case(b"GET", b"get"));
        assert!(ascii_eq_ignore_case(b"GET", b"GET"));
        assert!(ascii_eq_ignore_case(b"get", b"GET"));
        assert!(!ascii_eq_ignore_case(b"GET", b"SET"));
        assert!(!ascii_eq_ignore_case(b"GET", b"GETS"));
    }
}
