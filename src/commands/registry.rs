//! Command registry for looking up and managing Redis commands.

use super::{CommandFlags, ParsedCommand};
use crate::Result;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Type alias for async command handler.
pub type CommandHandler = fn(
    ParsedCommand,
    Arc<Db>,
    Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>>;

/// Command definition.
#[derive(Clone)]
pub struct Command {
    /// Command name
    pub name: &'static str,
    /// Minimum argument count
    pub min_args: i32,
    /// Maximum argument count (-1 for unlimited)
    pub max_args: i32,
    /// Command flags
    pub flags: CommandFlags,
    /// Handler function
    pub handler: CommandHandler,
    /// Help text
    pub summary: &'static str,
}

impl Command {
    /// Create a new command definition.
    pub const fn new(
        name: &'static str,
        min_args: i32,
        max_args: i32,
        flags: CommandFlags,
        handler: CommandHandler,
        summary: &'static str,
    ) -> Self {
        Self {
            name,
            min_args,
            max_args,
            flags,
            handler,
            summary,
        }
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Command")
            .field("name", &self.name)
            .field("min_args", &self.min_args)
            .field("max_args", &self.max_args)
            .field("flags", &self.flags)
            .field("summary", &self.summary)
            .finish()
    }
}

/// Registry of all available commands.
#[derive(Debug, Default)]
pub struct CommandRegistry {
    commands: HashMap<String, Command>,
}

impl CommandRegistry {
    /// Create a new command registry with all built-in commands.
    pub fn new() -> Self {
        let mut registry = Self {
            commands: HashMap::new(),
        };
        registry.register_all();
        registry
    }

    /// Register a command.
    pub fn register(&mut self, cmd: Command) {
        self.commands.insert(cmd.name.to_uppercase(), cmd);
    }

    /// Look up a command by name.
    pub fn get(&self, name: &str) -> Option<&Command> {
        self.commands.get(&name.to_uppercase())
    }

    /// Get all registered commands.
    pub fn commands(&self) -> impl Iterator<Item = &Command> {
        self.commands.values()
    }

    /// Get command count.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Register all built-in commands.
    fn register_all(&mut self) {
        // String commands
        self.register_string_commands();
        // Key commands
        self.register_key_commands();
        // List commands
        self.register_list_commands();
        // Hash commands
        self.register_hash_commands();
        // Set commands
        self.register_set_commands();
        // Sorted set commands
        self.register_sorted_set_commands();
        // Server commands
        self.register_server_commands();
        // Connection commands
        self.register_connection_commands();
        // Transaction commands
        self.register_transaction_commands();
        // Bitmap commands
        self.register_bitmap_commands();
        // HyperLogLog commands
        self.register_hyperloglog_commands();
        // Stream commands
        self.register_stream_commands();
        // Scripting commands
        self.register_scripting_commands();
        // Pub/Sub commands
        self.register_pubsub_commands();
        // Geo commands
        self.register_geo_commands();
        // Blocking commands
        self.register_blocking_commands();
        // Cluster commands
        self.register_cluster_commands();
        // Sentinel commands
        self.register_sentinel_commands();
        // Redis Stack module commands
        self.register_bloom_commands();
        self.register_cuckoo_commands();
        self.register_cms_commands();
        self.register_topk_commands();
        self.register_tdigest_commands();
        self.register_timeseries_commands();
        self.register_json_commands();
        self.register_vectorset_commands();
        self.register_search_commands();
    }

    fn register_string_commands(&mut self) {
        use super::strings::*;

        self.register(Command::new(
            "GET",
            1,
            1,
            CommandFlags::readonly(),
            cmd_get,
            "Get the value of a key",
        ));
        self.register(Command::new(
            "SET",
            2,
            -1,
            CommandFlags::write(),
            cmd_set,
            "Set the string value of a key",
        ));
        self.register(Command::new(
            "SETNX",
            2,
            2,
            CommandFlags::write(),
            cmd_setnx,
            "Set the value of a key, only if the key does not exist",
        ));
        self.register(Command::new(
            "SETEX",
            3,
            3,
            CommandFlags::write(),
            cmd_setex,
            "Set the value and expiration of a key",
        ));
        self.register(Command::new(
            "PSETEX",
            3,
            3,
            CommandFlags::write(),
            cmd_psetex,
            "Set the value and expiration in milliseconds of a key",
        ));
        self.register(Command::new(
            "MGET",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_mget,
            "Get the values of all the given keys",
        ));
        self.register(Command::new(
            "MSET",
            2,
            -1,
            CommandFlags::write(),
            cmd_mset,
            "Set multiple keys to multiple values",
        ));
        self.register(Command::new(
            "MSETNX",
            2,
            -1,
            CommandFlags::write(),
            cmd_msetnx,
            "Set multiple keys to multiple values, only if none of the keys exist",
        ));
        self.register(Command::new(
            "INCR",
            1,
            1,
            CommandFlags::write(),
            cmd_incr,
            "Increment the integer value of a key by one",
        ));
        self.register(Command::new(
            "INCRBY",
            2,
            2,
            CommandFlags::write(),
            cmd_incrby,
            "Increment the integer value of a key by the given amount",
        ));
        self.register(Command::new(
            "INCRBYFLOAT",
            2,
            2,
            CommandFlags::write(),
            cmd_incrbyfloat,
            "Increment the float value of a key by the given amount",
        ));
        self.register(Command::new(
            "DECR",
            1,
            1,
            CommandFlags::write(),
            cmd_decr,
            "Decrement the integer value of a key by one",
        ));
        self.register(Command::new(
            "DECRBY",
            2,
            2,
            CommandFlags::write(),
            cmd_decrby,
            "Decrement the integer value of a key by the given amount",
        ));
        self.register(Command::new(
            "APPEND",
            2,
            2,
            CommandFlags::write(),
            cmd_append,
            "Append a value to a key",
        ));
        self.register(Command::new(
            "STRLEN",
            1,
            1,
            CommandFlags::readonly(),
            cmd_strlen,
            "Get the length of the value stored in a key",
        ));
        self.register(Command::new(
            "GETRANGE",
            3,
            3,
            CommandFlags::readonly(),
            cmd_getrange,
            "Get a substring of the string stored at a key",
        ));
        self.register(Command::new(
            "SETRANGE",
            3,
            3,
            CommandFlags::write(),
            cmd_setrange,
            "Overwrite part of a string at key starting at the specified offset",
        ));
        self.register(Command::new(
            "GETSET",
            2,
            2,
            CommandFlags::write(),
            cmd_getset,
            "Set the string value of a key and return its old value",
        ));
        self.register(Command::new(
            "GETDEL",
            1,
            1,
            CommandFlags::write(),
            cmd_getdel,
            "Get the value of a key and delete the key",
        ));
        self.register(Command::new(
            "GETEX",
            1,
            -1,
            CommandFlags::write(),
            cmd_getex,
            "Get the value of a key and optionally set its expiration",
        ));
        self.register(Command::new(
            "SUBSTR",
            3,
            3,
            CommandFlags::readonly(),
            cmd_substr,
            "Get a substring of the string stored at a key (deprecated, use GETRANGE)",
        ));
        self.register(Command::new(
            "LCS",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_lcs,
            "Find longest common substring",
        ));
        self.register(Command::new(
            "MSETEX",
            3,
            -1,
            CommandFlags::write(),
            cmd_msetex,
            "Set multiple keys with expiration",
        ));
        self.register(Command::new(
            "DELEX",
            1,
            -1,
            CommandFlags::write(),
            cmd_delex,
            "Delete keys matching a pattern (custom command)",
        ));
        self.register(Command::new(
            "DIGEST",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_digest,
            "Get SHA256 digest of values",
        ));
    }

    fn register_key_commands(&mut self) {
        use super::keys::*;

        self.register(Command::new(
            "DEL",
            1,
            -1,
            CommandFlags::write(),
            cmd_del,
            "Delete a key",
        ));
        self.register(Command::new(
            "UNLINK",
            1,
            -1,
            CommandFlags::write(),
            cmd_unlink,
            "Delete a key asynchronously",
        ));
        self.register(Command::new(
            "EXISTS",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_exists,
            "Determine if a key exists",
        ));
        self.register(Command::new(
            "TYPE",
            1,
            1,
            CommandFlags::readonly(),
            cmd_type,
            "Determine the type stored at key",
        ));
        self.register(Command::new(
            "RENAME",
            2,
            2,
            CommandFlags::write(),
            cmd_rename,
            "Rename a key",
        ));
        self.register(Command::new(
            "RENAMENX",
            2,
            2,
            CommandFlags::write(),
            cmd_renamenx,
            "Rename a key, only if the new key does not exist",
        ));
        self.register(Command::new(
            "EXPIRE",
            2,
            -1,
            CommandFlags::write(),
            cmd_expire,
            "Set a key's time to live in seconds",
        ));
        self.register(Command::new(
            "PEXPIRE",
            2,
            -1,
            CommandFlags::write(),
            cmd_pexpire,
            "Set a key's time to live in milliseconds",
        ));
        self.register(Command::new(
            "EXPIREAT",
            2,
            -1,
            CommandFlags::write(),
            cmd_expireat,
            "Set the expiration for a key as a UNIX timestamp",
        ));
        self.register(Command::new(
            "PEXPIREAT",
            2,
            -1,
            CommandFlags::write(),
            cmd_pexpireat,
            "Set the expiration for a key as a UNIX timestamp in milliseconds",
        ));
        self.register(Command::new(
            "TTL",
            1,
            1,
            CommandFlags::readonly(),
            cmd_ttl,
            "Get the time to live for a key in seconds",
        ));
        self.register(Command::new(
            "PTTL",
            1,
            1,
            CommandFlags::readonly(),
            cmd_pttl,
            "Get the time to live for a key in milliseconds",
        ));
        self.register(Command::new(
            "PERSIST",
            1,
            1,
            CommandFlags::write(),
            cmd_persist,
            "Remove the expiration from a key",
        ));
        self.register(Command::new(
            "KEYS",
            1,
            1,
            CommandFlags::readonly(),
            cmd_keys,
            "Find all keys matching the given pattern",
        ));
        self.register(Command::new(
            "SCAN",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_scan,
            "Incrementally iterate the keys space",
        ));
        self.register(Command::new(
            "RANDOMKEY",
            0,
            0,
            CommandFlags::readonly(),
            cmd_randomkey,
            "Return a random key from the keyspace",
        ));
        self.register(Command::new(
            "DBSIZE",
            0,
            0,
            CommandFlags::readonly(),
            cmd_dbsize,
            "Return the number of keys in the selected database",
        ));
        self.register(Command::new(
            "EXPIRETIME",
            1,
            1,
            CommandFlags::readonly(),
            cmd_expiretime,
            "Get the expiration Unix timestamp for a key",
        ));
        self.register(Command::new(
            "PEXPIRETIME",
            1,
            1,
            CommandFlags::readonly(),
            cmd_pexpiretime,
            "Get the expiration Unix timestamp for a key in milliseconds",
        ));
        self.register(Command::new(
            "COPY",
            2,
            -1,
            CommandFlags::write(),
            cmd_copy,
            "Copy a key",
        ));
        self.register(Command::new(
            "TOUCH",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_touch,
            "Alters the last access time of a key(s)",
        ));
        self.register(Command::new(
            "OBJECT",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_object,
            "Inspect the internals of Redis objects",
        ));
        self.register(Command::new(
            "MOVE",
            2,
            2,
            CommandFlags::write(),
            cmd_move,
            "Move a key to another database",
        ));
        self.register(Command::new(
            "SORT",
            1,
            -1,
            CommandFlags::write(),
            cmd_sort,
            "Sort the elements in a list, set or sorted set",
        ));
        self.register(Command::new(
            "SORT_RO",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_sort_ro,
            "Read-only variant of SORT",
        ));
        self.register(Command::new(
            "MIGRATE",
            5,
            -1,
            CommandFlags::write(),
            cmd_migrate,
            "Atomically transfer a key from a Redis instance to another",
        ));
        self.register(Command::new(
            "WAITAOF",
            3,
            3,
            CommandFlags::readonly(),
            cmd_waitaof,
            "Wait for AOF synchronization",
        ));
    }

    fn register_list_commands(&mut self) {
        use super::lists::*;

        self.register(Command::new(
            "LPUSH",
            2,
            -1,
            CommandFlags::write(),
            cmd_lpush,
            "Prepend one or multiple elements to a list",
        ));
        self.register(Command::new(
            "RPUSH",
            2,
            -1,
            CommandFlags::write(),
            cmd_rpush,
            "Append one or multiple elements to a list",
        ));
        self.register(Command::new(
            "LPOP",
            1,
            2,
            CommandFlags::write(),
            cmd_lpop,
            "Remove and get the first element in a list",
        ));
        self.register(Command::new(
            "RPOP",
            1,
            2,
            CommandFlags::write(),
            cmd_rpop,
            "Remove and get the last element in a list",
        ));
        self.register(Command::new(
            "LLEN",
            1,
            1,
            CommandFlags::readonly(),
            cmd_llen,
            "Get the length of a list",
        ));
        self.register(Command::new(
            "LRANGE",
            3,
            3,
            CommandFlags::readonly(),
            cmd_lrange,
            "Get a range of elements from a list",
        ));
        self.register(Command::new(
            "LINDEX",
            2,
            2,
            CommandFlags::readonly(),
            cmd_lindex,
            "Get an element from a list by its index",
        ));
        self.register(Command::new(
            "LSET",
            3,
            3,
            CommandFlags::write(),
            cmd_lset,
            "Set the value of an element in a list by its index",
        ));
        self.register(Command::new(
            "LTRIM",
            3,
            3,
            CommandFlags::write(),
            cmd_ltrim,
            "Trim a list to the specified range",
        ));
        self.register(Command::new(
            "LREM",
            3,
            3,
            CommandFlags::write(),
            cmd_lrem,
            "Remove elements from a list",
        ));
        self.register(Command::new(
            "LINSERT",
            4,
            4,
            CommandFlags::write(),
            cmd_linsert,
            "Insert an element before or after another element in a list",
        ));
        self.register(Command::new(
            "LPOS",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_lpos,
            "Return the index of matching elements on a list",
        ));
        self.register(Command::new(
            "LMPOP",
            3,
            -1,
            CommandFlags::write(),
            cmd_lmpop,
            "Pop elements from a list",
        ));
        self.register(Command::new(
            "LMOVE",
            4,
            4,
            CommandFlags::write(),
            cmd_lmove,
            "Pop an element from a list, push it to another list and return it",
        ));
        self.register(Command::new(
            "LPUSHX",
            2,
            -1,
            CommandFlags::write(),
            cmd_lpushx,
            "Prepend elements to a list only if it exists",
        ));
        self.register(Command::new(
            "RPUSHX",
            2,
            -1,
            CommandFlags::write(),
            cmd_rpushx,
            "Append elements to a list only if it exists",
        ));
        self.register(Command::new(
            "RPOPLPUSH",
            2,
            2,
            CommandFlags::write(),
            cmd_rpoplpush,
            "Remove last element from one list and prepend it to another (deprecated)",
        ));
    }

    fn register_hash_commands(&mut self) {
        use super::hashes::*;

        self.register(Command::new(
            "HSET",
            3,
            -1,
            CommandFlags::write(),
            cmd_hset,
            "Set the string value of a hash field",
        ));
        self.register(Command::new(
            "HGET",
            2,
            2,
            CommandFlags::readonly(),
            cmd_hget,
            "Get the value of a hash field",
        ));
        self.register(Command::new(
            "HMSET",
            3,
            -1,
            CommandFlags::write(),
            cmd_hmset,
            "Set multiple hash fields to multiple values",
        ));
        self.register(Command::new(
            "HMGET",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_hmget,
            "Get the values of all the given hash fields",
        ));
        self.register(Command::new(
            "HDEL",
            2,
            -1,
            CommandFlags::write(),
            cmd_hdel,
            "Delete one or more hash fields",
        ));
        self.register(Command::new(
            "HEXISTS",
            2,
            2,
            CommandFlags::readonly(),
            cmd_hexists,
            "Determine if a hash field exists",
        ));
        self.register(Command::new(
            "HLEN",
            1,
            1,
            CommandFlags::readonly(),
            cmd_hlen,
            "Get the number of fields in a hash",
        ));
        self.register(Command::new(
            "HKEYS",
            1,
            1,
            CommandFlags::readonly(),
            cmd_hkeys,
            "Get all the fields in a hash",
        ));
        self.register(Command::new(
            "HVALS",
            1,
            1,
            CommandFlags::readonly(),
            cmd_hvals,
            "Get all the values in a hash",
        ));
        self.register(Command::new(
            "HGETALL",
            1,
            1,
            CommandFlags::readonly(),
            cmd_hgetall,
            "Get all the fields and values in a hash",
        ));
        self.register(Command::new(
            "HINCRBY",
            3,
            3,
            CommandFlags::write(),
            cmd_hincrby,
            "Increment the integer value of a hash field by the given number",
        ));
        self.register(Command::new(
            "HINCRBYFLOAT",
            3,
            3,
            CommandFlags::write(),
            cmd_hincrbyfloat,
            "Increment the float value of a hash field by the given amount",
        ));
        self.register(Command::new(
            "HSETNX",
            3,
            3,
            CommandFlags::write(),
            cmd_hsetnx,
            "Set the value of a hash field, only if the field does not exist",
        ));
        self.register(Command::new(
            "HSTRLEN",
            2,
            2,
            CommandFlags::readonly(),
            cmd_hstrlen,
            "Get the length of the value of a hash field",
        ));
        self.register(Command::new(
            "HSCAN",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_hscan,
            "Incrementally iterate hash fields and associated values",
        ));
        self.register(Command::new(
            "HRANDFIELD",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_hrandfield,
            "Get one or multiple random fields from a hash",
        ));
        self.register(Command::new(
            "HGETDEL",
            2,
            -1,
            CommandFlags::write(),
            cmd_hgetdel,
            "Get and delete fields from a hash",
        ));
        self.register(Command::new(
            "HGETEX",
            2,
            -1,
            CommandFlags::write(),
            cmd_hgetex,
            "Get fields and optionally set expiration",
        ));
        self.register(Command::new(
            "HSETEX",
            3,
            -1,
            CommandFlags::write(),
            cmd_hsetex,
            "Set fields with expiration",
        ));
        self.register(Command::new(
            "HEXPIRE",
            4,
            -1,
            CommandFlags::write(),
            cmd_hexpire,
            "Set expiration on hash fields",
        ));
        self.register(Command::new(
            "HEXPIREAT",
            4,
            -1,
            CommandFlags::write(),
            cmd_hexpireat,
            "Set expiration timestamp on hash fields",
        ));
        self.register(Command::new(
            "HEXPIRETIME",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_hexpiretime,
            "Get expiration time of hash fields",
        ));
        self.register(Command::new(
            "HPEXPIRE",
            4,
            -1,
            CommandFlags::write(),
            cmd_hpexpire,
            "Set expiration in milliseconds on hash fields",
        ));
        self.register(Command::new(
            "HPEXPIREAT",
            4,
            -1,
            CommandFlags::write(),
            cmd_hpexpireat,
            "Set expiration timestamp in milliseconds on hash fields",
        ));
        self.register(Command::new(
            "HPEXPIRETIME",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_hpexpiretime,
            "Get expiration time of hash fields in milliseconds",
        ));
        self.register(Command::new(
            "HTTL",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_httl,
            "Get TTL of hash fields in seconds",
        ));
        self.register(Command::new(
            "HPTTL",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_hpttl,
            "Get TTL of hash fields in milliseconds",
        ));
        self.register(Command::new(
            "HPERSIST",
            3,
            -1,
            CommandFlags::write(),
            cmd_hpersist,
            "Remove expiration from hash fields",
        ));
    }

    fn register_set_commands(&mut self) {
        use super::sets::*;

        self.register(Command::new(
            "SADD",
            2,
            -1,
            CommandFlags::write(),
            cmd_sadd,
            "Add one or more members to a set",
        ));
        self.register(Command::new(
            "SREM",
            2,
            -1,
            CommandFlags::write(),
            cmd_srem,
            "Remove one or more members from a set",
        ));
        self.register(Command::new(
            "SMEMBERS",
            1,
            1,
            CommandFlags::readonly(),
            cmd_smembers,
            "Get all the members in a set",
        ));
        self.register(Command::new(
            "SISMEMBER",
            2,
            2,
            CommandFlags::readonly(),
            cmd_sismember,
            "Determine if a given value is a member of a set",
        ));
        self.register(Command::new(
            "SCARD",
            1,
            1,
            CommandFlags::readonly(),
            cmd_scard,
            "Get the number of members in a set",
        ));
        self.register(Command::new(
            "SPOP",
            1,
            2,
            CommandFlags::write(),
            cmd_spop,
            "Remove and return one or multiple random members from a set",
        ));
        self.register(Command::new(
            "SRANDMEMBER",
            1,
            2,
            CommandFlags::readonly(),
            cmd_srandmember,
            "Get one or multiple random members from a set",
        ));
        self.register(Command::new(
            "SMOVE",
            3,
            3,
            CommandFlags::write(),
            cmd_smove,
            "Move a member from one set to another",
        ));
        self.register(Command::new(
            "SINTER",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_sinter,
            "Intersect multiple sets",
        ));
        self.register(Command::new(
            "SUNION",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_sunion,
            "Add multiple sets",
        ));
        self.register(Command::new(
            "SDIFF",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_sdiff,
            "Subtract multiple sets",
        ));
        self.register(Command::new(
            "SMISMEMBER",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_smismember,
            "Returns the membership associated with the given elements for a set",
        ));
        self.register(Command::new(
            "SSCAN",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_sscan,
            "Incrementally iterate Set elements",
        ));
        self.register(Command::new(
            "SINTERCARD",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_sintercard,
            "Intersect multiple sets and return the cardinality of the result",
        ));
        self.register(Command::new(
            "SINTERSTORE",
            2,
            -1,
            CommandFlags::write(),
            cmd_sinterstore,
            "Intersect multiple sets and store the resulting set in a key",
        ));
        self.register(Command::new(
            "SUNIONSTORE",
            2,
            -1,
            CommandFlags::write(),
            cmd_sunionstore,
            "Add multiple sets and store the resulting set in a key",
        ));
        self.register(Command::new(
            "SDIFFSTORE",
            2,
            -1,
            CommandFlags::write(),
            cmd_sdiffstore,
            "Subtract multiple sets and store the resulting set in a key",
        ));
    }

    fn register_sorted_set_commands(&mut self) {
        use super::sorted_sets::*;

        self.register(Command::new(
            "ZADD",
            3,
            -1,
            CommandFlags::write(),
            cmd_zadd,
            "Add one or more members to a sorted set",
        ));
        self.register(Command::new(
            "ZREM",
            2,
            -1,
            CommandFlags::write(),
            cmd_zrem,
            "Remove one or more members from a sorted set",
        ));
        self.register(Command::new(
            "ZSCORE",
            2,
            2,
            CommandFlags::readonly(),
            cmd_zscore,
            "Get the score associated with the given member in a sorted set",
        ));
        self.register(Command::new(
            "ZRANK",
            2,
            2,
            CommandFlags::readonly(),
            cmd_zrank,
            "Determine the index of a member in a sorted set",
        ));
        self.register(Command::new(
            "ZREVRANK",
            2,
            2,
            CommandFlags::readonly(),
            cmd_zrevrank,
            "Determine the index of a member in a sorted set, with scores ordered from high to low",
        ));
        self.register(Command::new(
            "ZCARD",
            1,
            1,
            CommandFlags::readonly(),
            cmd_zcard,
            "Get the number of members in a sorted set",
        ));
        self.register(Command::new(
            "ZCOUNT",
            3,
            3,
            CommandFlags::readonly(),
            cmd_zcount,
            "Count the members in a sorted set with scores within the given values",
        ));
        self.register(Command::new(
            "ZRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_zrange,
            "Return a range of members in a sorted set",
        ));
        self.register(Command::new(
            "ZREVRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_zrevrange,
            "Return a range of members in a sorted set, by index, with scores ordered from high to low",
        ));
        self.register(Command::new(
            "ZINCRBY",
            3,
            3,
            CommandFlags::write(),
            cmd_zincrby,
            "Increment the score of a member in a sorted set",
        ));
        self.register(Command::new(
            "ZPOPMIN",
            1,
            2,
            CommandFlags::write(),
            cmd_zpopmin,
            "Remove and return members with the lowest scores in a sorted set",
        ));
        self.register(Command::new(
            "ZPOPMAX",
            1,
            2,
            CommandFlags::write(),
            cmd_zpopmax,
            "Remove and return members with the highest scores in a sorted set",
        ));
        self.register(Command::new(
            "ZSCAN",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_zscan,
            "Incrementally iterate sorted sets elements and associated scores",
        ));
        self.register(Command::new(
            "ZRANDMEMBER",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_zrandmember,
            "Get one or multiple random elements from a sorted set",
        ));
        self.register(Command::new(
            "ZRANGESTORE",
            4,
            -1,
            CommandFlags::write(),
            cmd_zrangestore,
            "Store a range of members from sorted set into another key",
        ));
        self.register(Command::new(
            "ZUNION",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_zunion,
            "Add multiple sorted sets",
        ));
        self.register(Command::new(
            "ZINTER",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_zinter,
            "Intersect multiple sorted sets",
        ));
        self.register(Command::new(
            "ZDIFF",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_zdiff,
            "Subtract multiple sorted sets",
        ));
        self.register(Command::new(
            "ZMPOP",
            3,
            -1,
            CommandFlags::write(),
            cmd_zmpop,
            "Pop elements from sorted set",
        ));
        self.register(Command::new(
            "ZDIFFSTORE",
            3,
            -1,
            CommandFlags::write(),
            cmd_zdiffstore,
            "Subtract sorted sets and store result",
        ));
        self.register(Command::new(
            "ZINTERSTORE",
            3,
            -1,
            CommandFlags::write(),
            cmd_zinterstore,
            "Intersect sorted sets and store result",
        ));
        self.register(Command::new(
            "ZUNIONSTORE",
            3,
            -1,
            CommandFlags::write(),
            cmd_zunionstore,
            "Union sorted sets and store result",
        ));
        self.register(Command::new(
            "ZINTERCARD",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_zintercard,
            "Return intersection cardinality of sorted sets",
        ));
        self.register(Command::new(
            "ZMSCORE",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_zmscore,
            "Get scores of multiple members in a sorted set",
        ));
        self.register(Command::new(
            "ZLEXCOUNT",
            3,
            3,
            CommandFlags::readonly(),
            cmd_zlexcount,
            "Count members in a sorted set within lexicographic range",
        ));
        self.register(Command::new(
            "ZRANGEBYLEX",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_zrangebylex,
            "Return range by lexicographic order",
        ));
        self.register(Command::new(
            "ZREVRANGEBYLEX",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_zrevrangebylex,
            "Return range by reverse lexicographic order",
        ));
        self.register(Command::new(
            "ZRANGEBYSCORE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_zrangebyscore,
            "Return range by score",
        ));
        self.register(Command::new(
            "ZREVRANGEBYSCORE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_zrevrangebyscore,
            "Return range by score in reverse order",
        ));
        self.register(Command::new(
            "ZREMRANGEBYLEX",
            3,
            3,
            CommandFlags::write(),
            cmd_zremrangebylex,
            "Remove members in lexicographic range",
        ));
        self.register(Command::new(
            "ZREMRANGEBYRANK",
            3,
            3,
            CommandFlags::write(),
            cmd_zremrangebyrank,
            "Remove members by rank range",
        ));
        self.register(Command::new(
            "ZREMRANGEBYSCORE",
            3,
            3,
            CommandFlags::write(),
            cmd_zremrangebyscore,
            "Remove members by score range",
        ));
    }

    fn register_server_commands(&mut self) {
        use super::server_cmds::*;

        self.register(Command::new(
            "PING",
            0,
            1,
            CommandFlags::readonly(),
            cmd_ping,
            "Ping the server",
        ));
        self.register(Command::new(
            "ECHO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_echo,
            "Echo the given string",
        ));
        self.register(Command::new(
            "INFO",
            0,
            1,
            CommandFlags::readonly(),
            cmd_info,
            "Get information and statistics about the server",
        ));
        self.register(Command::new(
            "DBSIZE",
            0,
            0,
            CommandFlags::readonly(),
            cmd_dbsize,
            "Return the number of keys in the selected database",
        ));
        self.register(Command::new(
            "FLUSHDB",
            0,
            1,
            CommandFlags::write(),
            cmd_flushdb,
            "Remove all keys from the current database",
        ));
        self.register(Command::new(
            "FLUSHALL",
            0,
            1,
            CommandFlags::write(),
            cmd_flushall,
            "Remove all keys from all databases",
        ));
        self.register(Command::new(
            "TIME",
            0,
            0,
            CommandFlags::readonly(),
            cmd_time,
            "Return the current server time",
        ));
        self.register(Command::new(
            "COMMAND",
            0,
            -1,
            CommandFlags::readonly(),
            cmd_command,
            "Get information about Redis commands",
        ));
        self.register(Command::new(
            "DEBUG",
            1,
            -1,
            CommandFlags::admin(),
            cmd_debug,
            "Debug commands",
        ));
        self.register(Command::new(
            "DUMP",
            1,
            1,
            CommandFlags::readonly(),
            cmd_dump,
            "Return a serialized version of the value stored at key",
        ));
        self.register(Command::new(
            "RESTORE",
            3,
            -1,
            CommandFlags::write(),
            cmd_restore,
            "Create a key using the serialized value",
        ));
        self.register(Command::new(
            "MEMORY",
            1,
            -1,
            CommandFlags::admin(),
            cmd_memory,
            "Memory management commands",
        ));
        self.register(Command::new(
            "SLOWLOG",
            1,
            -1,
            CommandFlags::admin(),
            cmd_slowlog,
            "Manages the Redis slow queries log",
        ));
        self.register(Command::new(
            "ACL",
            1,
            -1,
            CommandFlags::admin(),
            cmd_acl,
            "Access Control List commands",
        ));
        self.register(Command::new(
            "WAIT",
            2,
            2,
            CommandFlags::readonly(),
            cmd_wait,
            "Wait for replication",
        ));
        self.register(Command::new(
            "BGSAVE",
            0,
            1,
            CommandFlags::admin(),
            cmd_bgsave,
            "Asynchronously save the dataset to disk",
        ));
        self.register(Command::new(
            "BGREWRITEAOF",
            0,
            0,
            CommandFlags::admin(),
            cmd_bgrewriteaof,
            "Asynchronously rewrite the append-only file",
        ));
        self.register(Command::new(
            "SAVE",
            0,
            0,
            CommandFlags::admin(),
            cmd_save,
            "Synchronously save the dataset to disk",
        ));
        self.register(Command::new(
            "LASTSAVE",
            0,
            0,
            CommandFlags::readonly(),
            cmd_lastsave,
            "Get the UNIX time stamp of the last successful save to disk",
        ));
        self.register(Command::new(
            "CONFIG",
            1,
            -1,
            CommandFlags::admin(),
            cmd_config,
            "Get/Set configuration parameters",
        ));
        self.register(Command::new(
            "SHUTDOWN",
            0,
            -1,
            CommandFlags::admin(),
            cmd_shutdown,
            "Synchronously save the dataset to disk and then shut down the server",
        ));
        self.register(Command::new(
            "REPLICAOF",
            1,
            2,
            CommandFlags::admin(),
            cmd_replicaof,
            "Make the server a replica of another instance",
        ));
        self.register(Command::new(
            "SLAVEOF",
            1,
            2,
            CommandFlags::admin(),
            cmd_slaveof,
            "Make the server a replica of another instance (deprecated)",
        ));
        self.register(Command::new(
            "LATENCY",
            1,
            -1,
            CommandFlags::admin(),
            cmd_latency,
            "Latency monitoring commands",
        ));
        self.register(Command::new(
            "MODULE",
            1,
            -1,
            CommandFlags::admin(),
            cmd_module,
            "Module management commands",
        ));
        self.register(Command::new(
            "SWAPDB",
            2,
            2,
            CommandFlags::write(),
            cmd_swapdb,
            "Swap two Redis databases",
        ));
        self.register(Command::new(
            "LOLWUT",
            0,
            -1,
            CommandFlags::readonly(),
            cmd_lolwut,
            "Display some computer art and the Redis version",
        ));
        self.register(Command::new(
            "FAILOVER",
            0,
            -1,
            CommandFlags::admin(),
            cmd_failover,
            "Start a coordinated failover",
        ));
        self.register(Command::new(
            "ROLE",
            0,
            0,
            CommandFlags::readonly(),
            cmd_role,
            "Return the role of the server",
        ));
        self.register(Command::new(
            "PSYNC",
            2,
            2,
            CommandFlags::admin(),
            cmd_psync,
            "Internal command for replication",
        ));
        self.register(Command::new(
            "REPLCONF",
            1,
            -1,
            CommandFlags::admin(),
            cmd_replconf,
            "Replication configuration command",
        ));
    }

    fn register_connection_commands(&mut self) {
        use super::connection::*;

        self.register(Command::new(
            "SELECT",
            1,
            1,
            CommandFlags::readonly(),
            cmd_select,
            "Change the selected database for the current connection",
        ));
        self.register(Command::new(
            "CLIENT",
            1,
            -1,
            CommandFlags::admin(),
            cmd_client,
            "Client connection commands",
        ));
        self.register(Command::new(
            "QUIT",
            0,
            0,
            CommandFlags::readonly(),
            cmd_quit,
            "Close the connection",
        ));
        // NOTE: AUTH and HELLO are handled specially by CommandExecutor
        // (see executor.rs handle_auth/handle_hello) and are NOT registered here.
        // They need access to server auth config which the registry doesn't have.
        self.register(Command::new(
            "RESET",
            0,
            0,
            CommandFlags::readonly(),
            cmd_reset,
            "Reset the connection state",
        ));
    }

    fn register_transaction_commands(&mut self) {
        use super::transactions::*;

        self.register(Command::new(
            "MULTI",
            0,
            0,
            CommandFlags::readonly(),
            cmd_multi,
            "Mark the start of a transaction block",
        ));
        self.register(Command::new(
            "EXEC",
            0,
            0,
            CommandFlags::write(),
            cmd_exec,
            "Execute all commands issued after MULTI",
        ));
        self.register(Command::new(
            "DISCARD",
            0,
            0,
            CommandFlags::readonly(),
            cmd_discard,
            "Discard all commands issued after MULTI",
        ));
        self.register(Command::new(
            "WATCH",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_watch,
            "Watch the given keys to determine execution of the MULTI/EXEC block",
        ));
        self.register(Command::new(
            "UNWATCH",
            0,
            0,
            CommandFlags::readonly(),
            cmd_unwatch,
            "Forget about all watched keys",
        ));
    }

    fn register_bitmap_commands(&mut self) {
        use super::bitmap::*;

        self.register(Command::new(
            "SETBIT",
            3,
            3,
            CommandFlags::write(),
            cmd_setbit,
            "Sets or clears the bit at offset in the string value stored at key",
        ));
        self.register(Command::new(
            "GETBIT",
            2,
            2,
            CommandFlags::readonly(),
            cmd_getbit,
            "Returns the bit value at offset in the string value stored at key",
        ));
        self.register(Command::new(
            "BITCOUNT",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_bitcount,
            "Count the number of set bits (population counting) in a string",
        ));
        self.register(Command::new(
            "BITOP",
            3,
            -1,
            CommandFlags::write(),
            cmd_bitop,
            "Perform bitwise operations between strings",
        ));
        self.register(Command::new(
            "BITPOS",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_bitpos,
            "Find first bit set or clear in a string",
        ));
        self.register(Command::new(
            "BITFIELD",
            2,
            -1,
            CommandFlags::write(),
            cmd_bitfield,
            "Perform arbitrary bitfield integer operations on strings",
        ));
        self.register(Command::new(
            "BITFIELD_RO",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_bitfield_ro,
            "Read-only variant of BITFIELD",
        ));
    }

    fn register_hyperloglog_commands(&mut self) {
        use super::hyperloglog::*;

        self.register(Command::new(
            "PFADD",
            2,
            -1,
            CommandFlags::write(),
            cmd_pfadd,
            "Adds elements to a HyperLogLog data structure",
        ));
        self.register(Command::new(
            "PFCOUNT",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_pfcount,
            "Return the approximated cardinality of the sets",
        ));
        self.register(Command::new(
            "PFMERGE",
            2,
            -1,
            CommandFlags::write(),
            cmd_pfmerge,
            "Merge multiple HyperLogLog values into a unique value",
        ));
        self.register(Command::new(
            "PFDEBUG",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_pfdebug,
            "Internal commands for debugging HyperLogLog values",
        ));
    }

    fn register_stream_commands(&mut self) {
        use super::streams::*;

        self.register(Command::new(
            "XADD",
            4,
            -1,
            CommandFlags::write(),
            cmd_xadd,
            "Appends a new entry to a stream",
        ));
        self.register(Command::new(
            "XLEN",
            1,
            1,
            CommandFlags::readonly(),
            cmd_xlen,
            "Returns the number of entries in a stream",
        ));
        self.register(Command::new(
            "XRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_xrange,
            "Returns entries from a stream in a range",
        ));
        self.register(Command::new(
            "XREVRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_xrevrange,
            "Returns entries from a stream in reverse order",
        ));
        self.register(Command::new(
            "XREAD",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_xread,
            "Read data from one or multiple streams",
        ));
        self.register(Command::new(
            "XTRIM",
            3,
            -1,
            CommandFlags::write(),
            cmd_xtrim,
            "Trims the stream to a certain size",
        ));
        self.register(Command::new(
            "XDEL",
            2,
            -1,
            CommandFlags::write(),
            cmd_xdel,
            "Removes specific entries from a stream",
        ));
        self.register(Command::new(
            "XINFO",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_xinfo,
            "Returns information about a stream",
        ));
        self.register(Command::new(
            "XGROUP",
            2,
            -1,
            CommandFlags::write(),
            cmd_xgroup,
            "Manage consumer groups",
        ));
        self.register(Command::new(
            "XACK",
            3,
            -1,
            CommandFlags::write(),
            cmd_xack,
            "Acknowledge messages in a stream consumer group",
        ));
        self.register(Command::new(
            "XCLAIM",
            5,
            -1,
            CommandFlags::write(),
            cmd_xclaim,
            "Claim pending messages from a stream consumer group",
        ));
        self.register(Command::new(
            "XPENDING",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_xpending,
            "Return information about pending messages of a stream consumer group",
        ));
        self.register(Command::new(
            "XAUTOCLAIM",
            5,
            -1,
            CommandFlags::write(),
            cmd_xautoclaim,
            "Auto-claim pending messages from a stream consumer group",
        ));
        self.register(Command::new(
            "XREADGROUP",
            7,
            -1,
            CommandFlags::readonly(),
            cmd_xreadgroup,
            "Read data from a stream via consumer group",
        ));
        self.register(Command::new(
            "XSETID",
            2,
            -1,
            CommandFlags::write(),
            cmd_xsetid,
            "Set the last ID of a stream",
        ));
        // Redis 8.2+ stream commands
        self.register(Command::new(
            "XDELEX",
            1,
            -1,
            CommandFlags::write(),
            cmd_xdelex,
            "Delete entries based on time criteria (Redis 8.2+)",
        ));
        self.register(Command::new(
            "XACKDEL",
            3,
            -1,
            CommandFlags::write(),
            cmd_xackdel,
            "Atomically acknowledge and delete messages (Redis 8.2+)",
        ));
    }

    fn register_scripting_commands(&mut self) {
        use super::scripting::*;

        self.register(Command::new(
            "EVAL",
            2,
            -1,
            CommandFlags::write(),
            cmd_eval,
            "Execute a Lua script server side",
        ));
        self.register(Command::new(
            "EVALSHA",
            2,
            -1,
            CommandFlags::write(),
            cmd_evalsha,
            "Execute a cached Lua script server side by its SHA1 digest",
        ));
        self.register(Command::new(
            "SCRIPT",
            1,
            -1,
            CommandFlags::admin(),
            cmd_script,
            "Scripting commands",
        ));
        self.register(Command::new(
            "EVAL_RO",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_eval_ro,
            "Execute a read-only Lua script",
        ));
        self.register(Command::new(
            "EVALSHA_RO",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_evalsha_ro,
            "Execute a cached read-only Lua script",
        ));
        self.register(Command::new(
            "FCALL",
            3,
            -1,
            CommandFlags::write(),
            cmd_fcall,
            "Call a function",
        ));
        self.register(Command::new(
            "FCALL_RO",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_fcall_ro,
            "Call a read-only function",
        ));
        self.register(Command::new(
            "FUNCTION",
            1,
            -1,
            CommandFlags::admin(),
            cmd_function,
            "Function management commands",
        ));
    }

    fn register_pubsub_commands(&mut self) {
        use super::pubsub::*;

        self.register(Command::new(
            "SUBSCRIBE",
            1,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_subscribe,
            "Listen for messages published to channels",
        ));
        self.register(Command::new(
            "PSUBSCRIBE",
            1,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_psubscribe,
            "Listen for messages published to channels matching patterns",
        ));
        self.register(Command::new(
            "UNSUBSCRIBE",
            0,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_unsubscribe,
            "Stop listening for messages",
        ));
        self.register(Command::new(
            "PUNSUBSCRIBE",
            0,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_punsubscribe,
            "Stop listening for messages matching patterns",
        ));
        self.register(Command::new(
            "PUBLISH",
            2,
            2,
            CommandFlags::pubsub_cmd(),
            cmd_publish,
            "Post a message to a channel",
        ));
        self.register(Command::new(
            "PUBSUB",
            1,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_pubsub,
            "Inspect the Pub/Sub subsystem",
        ));
        self.register(Command::new(
            "SPUBLISH",
            2,
            2,
            CommandFlags::pubsub_cmd(),
            cmd_spublish,
            "Post a message to a shard channel",
        ));
        self.register(Command::new(
            "SSUBSCRIBE",
            1,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_ssubscribe,
            "Subscribe to shard channels",
        ));
        self.register(Command::new(
            "SUNSUBSCRIBE",
            0,
            -1,
            CommandFlags::pubsub_cmd(),
            cmd_sunsubscribe,
            "Unsubscribe from shard channels",
        ));
    }

    fn register_geo_commands(&mut self) {
        use super::geo::*;

        self.register(Command::new(
            "GEOADD",
            4,
            -1,
            CommandFlags::write(),
            cmd_geoadd,
            "Add geospatial items to a sorted set",
        ));
        self.register(Command::new(
            "GEODIST",
            3,
            4,
            CommandFlags::readonly(),
            cmd_geodist,
            "Returns the distance between two members",
        ));
        self.register(Command::new(
            "GEOHASH",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_geohash,
            "Returns Geohash strings representing positions",
        ));
        self.register(Command::new(
            "GEOPOS",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_geopos,
            "Returns longitude and latitude of members",
        ));
        self.register(Command::new(
            "GEORADIUS",
            5,
            -1,
            CommandFlags::readonly(),
            cmd_georadius,
            "Query items in a radius from center coordinates",
        ));
        self.register(Command::new(
            "GEORADIUSBYMEMBER",
            4,
            -1,
            CommandFlags::readonly(),
            cmd_georadiusbymember,
            "Query items in a radius from a member",
        ));
        self.register(Command::new(
            "GEOSEARCH",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_geosearch,
            "Query items in a geospatial index by radius or box",
        ));
        self.register(Command::new(
            "GEOSEARCHSTORE",
            4,
            -1,
            CommandFlags::write(),
            cmd_geosearchstore,
            "Store geosearch results in a key",
        ));
        self.register(Command::new(
            "GEORADIUS_RO",
            5,
            -1,
            CommandFlags::readonly(),
            cmd_georadius_ro,
            "Read-only variant of GEORADIUS",
        ));
        self.register(Command::new(
            "GEORADIUSBYMEMBER_RO",
            4,
            -1,
            CommandFlags::readonly(),
            cmd_georadiusbymember_ro,
            "Read-only variant of GEORADIUSBYMEMBER",
        ));
    }

    fn register_blocking_commands(&mut self) {
        use super::blocking::*;

        self.register(Command::new(
            "BLPOP",
            2,
            -1,
            CommandFlags::blocking(),
            cmd_blpop,
            "Remove and get the first element in a list, or block",
        ));
        self.register(Command::new(
            "BRPOP",
            2,
            -1,
            CommandFlags::blocking(),
            cmd_brpop,
            "Remove and get the last element in a list, or block",
        ));
        self.register(Command::new(
            "BLMOVE",
            5,
            5,
            CommandFlags::blocking(),
            cmd_blmove,
            "Pop and push element from one list to another, or block",
        ));
        self.register(Command::new(
            "BLMPOP",
            4,
            -1,
            CommandFlags::blocking(),
            cmd_blmpop,
            "Pop elements from multiple lists, or block",
        ));
        self.register(Command::new(
            "BZPOPMIN",
            2,
            -1,
            CommandFlags::blocking(),
            cmd_bzpopmin,
            "Remove and return the member with lowest score from sorted sets, or block",
        ));
        self.register(Command::new(
            "BZPOPMAX",
            2,
            -1,
            CommandFlags::blocking(),
            cmd_bzpopmax,
            "Remove and return the member with highest score from sorted sets, or block",
        ));
        self.register(Command::new(
            "BZMPOP",
            4,
            -1,
            CommandFlags::blocking(),
            cmd_bzmpop,
            "Pop elements from sorted sets, or block",
        ));
        self.register(Command::new(
            "BRPOPLPUSH",
            3,
            3,
            CommandFlags::blocking(),
            cmd_brpoplpush,
            "Pop from one list and push to another, or block (deprecated)",
        ));
    }

    fn register_cluster_commands(&mut self) {
        use super::cluster::*;

        self.register(Command::new(
            "CLUSTER",
            1,
            -1,
            CommandFlags::admin(),
            cmd_cluster,
            "Cluster management commands",
        ));
        self.register(Command::new(
            "READONLY",
            0,
            0,
            CommandFlags::readonly(),
            cmd_readonly,
            "Enable read queries on a replica",
        ));
        self.register(Command::new(
            "READWRITE",
            0,
            0,
            CommandFlags::readonly(),
            cmd_readwrite,
            "Disable read queries on a replica",
        ));
        self.register(Command::new(
            "ASKING",
            0,
            0,
            CommandFlags::readonly(),
            cmd_asking,
            "Set cluster slot redirection flag",
        ));
    }

    fn register_sentinel_commands(&mut self) {
        use super::sentinel::cmd_sentinel;

        self.register(Command::new(
            "SENTINEL",
            1,
            -1,
            CommandFlags::admin(),
            cmd_sentinel,
            "Sentinel high availability commands",
        ));
    }

    // Redis Stack Module Commands

    fn register_bloom_commands(&mut self) {
        use super::bloom_cmds::*;

        self.register(Command::new(
            "BF.ADD",
            2,
            2,
            CommandFlags::write(),
            cmd_bf_add,
            "Add an item to a Bloom filter",
        ));
        self.register(Command::new(
            "BF.MADD",
            2,
            -1,
            CommandFlags::write(),
            cmd_bf_madd,
            "Add multiple items to a Bloom filter",
        ));
        self.register(Command::new(
            "BF.EXISTS",
            2,
            2,
            CommandFlags::readonly(),
            cmd_bf_exists,
            "Check if an item exists in a Bloom filter",
        ));
        self.register(Command::new(
            "BF.MEXISTS",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_bf_mexists,
            "Check if multiple items exist in a Bloom filter",
        ));
        self.register(Command::new(
            "BF.RESERVE",
            3,
            -1,
            CommandFlags::write(),
            cmd_bf_reserve,
            "Create a Bloom filter with capacity and error rate",
        ));
        self.register(Command::new(
            "BF.INFO",
            1,
            2,
            CommandFlags::readonly(),
            cmd_bf_info,
            "Get information about a Bloom filter",
        ));
        self.register(Command::new(
            "BF.CARD",
            1,
            1,
            CommandFlags::readonly(),
            cmd_bf_card,
            "Get the cardinality of a Bloom filter",
        ));
        self.register(Command::new(
            "BF.INSERT",
            2,
            -1,
            CommandFlags::write(),
            cmd_bf_insert,
            "Insert items into a Bloom filter with options",
        ));
    }

    fn register_cuckoo_commands(&mut self) {
        use super::cuckoo_cmds::*;

        self.register(Command::new(
            "CF.ADD",
            2,
            2,
            CommandFlags::write(),
            cmd_cf_add,
            "Add an item to a Cuckoo filter",
        ));
        self.register(Command::new(
            "CF.ADDNX",
            2,
            2,
            CommandFlags::write(),
            cmd_cf_addnx,
            "Add an item to a Cuckoo filter if not exists",
        ));
        self.register(Command::new(
            "CF.EXISTS",
            2,
            2,
            CommandFlags::readonly(),
            cmd_cf_exists,
            "Check if an item exists in a Cuckoo filter",
        ));
        self.register(Command::new(
            "CF.MEXISTS",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_cf_mexists,
            "Check if multiple items exist in a Cuckoo filter",
        ));
        self.register(Command::new(
            "CF.DEL",
            2,
            2,
            CommandFlags::write(),
            cmd_cf_del,
            "Delete an item from a Cuckoo filter",
        ));
        self.register(Command::new(
            "CF.COUNT",
            2,
            2,
            CommandFlags::readonly(),
            cmd_cf_count,
            "Count occurrences of an item in a Cuckoo filter",
        ));
        self.register(Command::new(
            "CF.RESERVE",
            2,
            -1,
            CommandFlags::write(),
            cmd_cf_reserve,
            "Create a Cuckoo filter with capacity",
        ));
        self.register(Command::new(
            "CF.INFO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_cf_info,
            "Get information about a Cuckoo filter",
        ));
        self.register(Command::new(
            "CF.INSERT",
            2,
            -1,
            CommandFlags::write(),
            cmd_cf_insert,
            "Insert items into a Cuckoo filter with options",
        ));
        self.register(Command::new(
            "CF.INSERTNX",
            2,
            -1,
            CommandFlags::write(),
            cmd_cf_insertnx,
            "Insert items into a Cuckoo filter only if not exists",
        ));
    }

    fn register_cms_commands(&mut self) {
        use super::cms_cmds::*;

        self.register(Command::new(
            "CMS.INITBYDIM",
            3,
            3,
            CommandFlags::write(),
            cmd_cms_initbydim,
            "Initialize a Count-Min Sketch by dimensions",
        ));
        self.register(Command::new(
            "CMS.INITBYPROB",
            3,
            3,
            CommandFlags::write(),
            cmd_cms_initbyprob,
            "Initialize a Count-Min Sketch by probability",
        ));
        self.register(Command::new(
            "CMS.INCRBY",
            3,
            -1,
            CommandFlags::write(),
            cmd_cms_incrby,
            "Increment item counts in a Count-Min Sketch",
        ));
        self.register(Command::new(
            "CMS.QUERY",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_cms_query,
            "Query item counts in a Count-Min Sketch",
        ));
        self.register(Command::new(
            "CMS.MERGE",
            3,
            -1,
            CommandFlags::write(),
            cmd_cms_merge,
            "Merge multiple Count-Min Sketches",
        ));
        self.register(Command::new(
            "CMS.INFO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_cms_info,
            "Get information about a Count-Min Sketch",
        ));
    }

    fn register_topk_commands(&mut self) {
        use super::topk_cmds::*;

        self.register(Command::new(
            "TOPK.RESERVE",
            2,
            5,
            CommandFlags::write(),
            cmd_topk_reserve,
            "Reserve a Top-K structure",
        ));
        self.register(Command::new(
            "TOPK.ADD",
            2,
            -1,
            CommandFlags::write(),
            cmd_topk_add,
            "Add items to a Top-K",
        ));
        self.register(Command::new(
            "TOPK.INCRBY",
            3,
            -1,
            CommandFlags::write(),
            cmd_topk_incrby,
            "Increment item counts in a Top-K",
        ));
        self.register(Command::new(
            "TOPK.QUERY",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_topk_query,
            "Check if items are in the Top-K",
        ));
        self.register(Command::new(
            "TOPK.COUNT",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_topk_count,
            "Get estimated counts for items",
        ));
        self.register(Command::new(
            "TOPK.LIST",
            1,
            2,
            CommandFlags::readonly(),
            cmd_topk_list,
            "List the top-k items",
        ));
        self.register(Command::new(
            "TOPK.INFO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_topk_info,
            "Get information about a Top-K",
        ));
    }

    fn register_tdigest_commands(&mut self) {
        use super::tdigest_cmds::*;

        self.register(Command::new(
            "TDIGEST.CREATE",
            1,
            -1,
            CommandFlags::write(),
            cmd_tdigest_create,
            "Create a T-Digest structure",
        ));
        self.register(Command::new(
            "TDIGEST.RESET",
            1,
            1,
            CommandFlags::write(),
            cmd_tdigest_reset,
            "Reset a T-Digest structure",
        ));
        self.register(Command::new(
            "TDIGEST.ADD",
            2,
            -1,
            CommandFlags::write(),
            cmd_tdigest_add,
            "Add values to a T-Digest",
        ));
        self.register(Command::new(
            "TDIGEST.MERGE",
            3,
            -1,
            CommandFlags::write(),
            cmd_tdigest_merge,
            "Merge multiple T-Digests",
        ));
        self.register(Command::new(
            "TDIGEST.QUANTILE",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_tdigest_quantile,
            "Get quantile values from a T-Digest",
        ));
        self.register(Command::new(
            "TDIGEST.CDF",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_tdigest_cdf,
            "Get CDF values from a T-Digest",
        ));
        self.register(Command::new(
            "TDIGEST.MIN",
            1,
            1,
            CommandFlags::readonly(),
            cmd_tdigest_min,
            "Get minimum value from a T-Digest",
        ));
        self.register(Command::new(
            "TDIGEST.MAX",
            1,
            1,
            CommandFlags::readonly(),
            cmd_tdigest_max,
            "Get maximum value from a T-Digest",
        ));
        self.register(Command::new(
            "TDIGEST.TRIMMED_MEAN",
            3,
            3,
            CommandFlags::readonly(),
            cmd_tdigest_trimmed_mean,
            "Get trimmed mean from a T-Digest",
        ));
        self.register(Command::new(
            "TDIGEST.RANK",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_tdigest_rank,
            "Get rank of values",
        ));
        self.register(Command::new(
            "TDIGEST.REVRANK",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_tdigest_revrank,
            "Get reverse rank of values",
        ));
        self.register(Command::new(
            "TDIGEST.BYRANK",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_tdigest_byrank,
            "Get values by rank",
        ));
        self.register(Command::new(
            "TDIGEST.BYREVRANK",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_tdigest_byrevrank,
            "Get values by reverse rank",
        ));
        self.register(Command::new(
            "TDIGEST.INFO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_tdigest_info,
            "Get information about a T-Digest",
        ));
    }

    fn register_timeseries_commands(&mut self) {
        use super::ts_cmds::*;

        self.register(Command::new(
            "TS.CREATE",
            1,
            -1,
            CommandFlags::write(),
            cmd_ts_create,
            "Create a time series",
        ));
        self.register(Command::new(
            "TS.ADD",
            3,
            -1,
            CommandFlags::write(),
            cmd_ts_add,
            "Add a sample to a time series",
        ));
        self.register(Command::new(
            "TS.MADD",
            3,
            -1,
            CommandFlags::write(),
            cmd_ts_madd,
            "Add samples to multiple time series",
        ));
        self.register(Command::new(
            "TS.GET",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_ts_get,
            "Get the last sample from a time series",
        ));
        self.register(Command::new(
            "TS.MGET",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_ts_mget,
            "Get the last sample from multiple time series",
        ));
        self.register(Command::new(
            "TS.RANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_ts_range,
            "Query a range from a time series",
        ));
        self.register(Command::new(
            "TS.REVRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_ts_revrange,
            "Query a range in reverse from a time series",
        ));
        self.register(Command::new(
            "TS.INCRBY",
            2,
            -1,
            CommandFlags::write(),
            cmd_ts_incrby,
            "Increment a time series value",
        ));
        self.register(Command::new(
            "TS.DECRBY",
            2,
            -1,
            CommandFlags::write(),
            cmd_ts_decrby,
            "Decrement a time series value",
        ));
        self.register(Command::new(
            "TS.DEL",
            3,
            3,
            CommandFlags::write(),
            cmd_ts_del,
            "Delete samples from a time series",
        ));
        self.register(Command::new(
            "TS.INFO",
            1,
            2,
            CommandFlags::readonly(),
            cmd_ts_info,
            "Get information about a time series",
        ));
        self.register(Command::new(
            "TS.ALTER",
            1,
            -1,
            CommandFlags::write(),
            cmd_ts_alter,
            "Alter a time series configuration",
        ));
        self.register(Command::new(
            "TS.CREATERULE",
            4,
            -1,
            CommandFlags::write(),
            cmd_ts_createrule,
            "Create a compaction rule",
        ));
        self.register(Command::new(
            "TS.DELETERULE",
            2,
            2,
            CommandFlags::write(),
            cmd_ts_deleterule,
            "Delete a compaction rule",
        ));
        self.register(Command::new(
            "TS.MRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_ts_mrange,
            "Query a range from multiple time series",
        ));
        self.register(Command::new(
            "TS.MREVRANGE",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_ts_mrevrange,
            "Query a range in reverse from multiple time series",
        ));
        self.register(Command::new(
            "TS.QUERYINDEX",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_ts_queryindex,
            "Query time series by label filter",
        ));
    }

    fn register_json_commands(&mut self) {
        use super::json_cmds::*;

        self.register(Command::new(
            "JSON.SET",
            3,
            -1,
            CommandFlags::write(),
            cmd_json_set,
            "Set a JSON value",
        ));
        self.register(Command::new(
            "JSON.GET",
            1,
            -1,
            CommandFlags::readonly(),
            cmd_json_get,
            "Get a JSON value",
        ));
        self.register(Command::new(
            "JSON.MGET",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_json_mget,
            "Get JSON values from multiple keys",
        ));
        self.register(Command::new(
            "JSON.DEL",
            1,
            2,
            CommandFlags::write(),
            cmd_json_del,
            "Delete a JSON value",
        ));
        self.register(Command::new(
            "JSON.FORGET",
            1,
            2,
            CommandFlags::write(),
            cmd_json_forget,
            "Delete a JSON value (alias for JSON.DEL)",
        ));
        self.register(Command::new(
            "JSON.TYPE",
            1,
            2,
            CommandFlags::readonly(),
            cmd_json_type,
            "Get the type of a JSON value",
        ));
        self.register(Command::new(
            "JSON.NUMINCRBY",
            3,
            3,
            CommandFlags::write(),
            cmd_json_numincrby,
            "Increment a JSON number",
        ));
        self.register(Command::new(
            "JSON.NUMMULTBY",
            3,
            3,
            CommandFlags::write(),
            cmd_json_nummultby,
            "Multiply a JSON number",
        ));
        self.register(Command::new(
            "JSON.STRAPPEND",
            2,
            3,
            CommandFlags::write(),
            cmd_json_strappend,
            "Append to a JSON string",
        ));
        self.register(Command::new(
            "JSON.STRLEN",
            1,
            2,
            CommandFlags::readonly(),
            cmd_json_strlen,
            "Get the length of a JSON string",
        ));
        self.register(Command::new(
            "JSON.ARRAPPEND",
            3,
            -1,
            CommandFlags::write(),
            cmd_json_arrappend,
            "Append to a JSON array",
        ));
        self.register(Command::new(
            "JSON.ARRINDEX",
            3,
            5,
            CommandFlags::readonly(),
            cmd_json_arrindex,
            "Find index of an element in a JSON array",
        ));
        self.register(Command::new(
            "JSON.ARRLEN",
            1,
            2,
            CommandFlags::readonly(),
            cmd_json_arrlen,
            "Get the length of a JSON array",
        ));
        self.register(Command::new(
            "JSON.ARRPOP",
            1,
            3,
            CommandFlags::write(),
            cmd_json_arrpop,
            "Pop an element from a JSON array",
        ));
        self.register(Command::new(
            "JSON.ARRTRIM",
            4,
            4,
            CommandFlags::write(),
            cmd_json_arrtrim,
            "Trim a JSON array",
        ));
        self.register(Command::new(
            "JSON.ARRINSERT",
            4,
            -1,
            CommandFlags::write(),
            cmd_json_arrinsert,
            "Insert into a JSON array",
        ));
        self.register(Command::new(
            "JSON.OBJKEYS",
            1,
            2,
            CommandFlags::readonly(),
            cmd_json_objkeys,
            "Get keys of a JSON object",
        ));
        self.register(Command::new(
            "JSON.OBJLEN",
            1,
            2,
            CommandFlags::readonly(),
            cmd_json_objlen,
            "Get the number of keys in a JSON object",
        ));
        self.register(Command::new(
            "JSON.TOGGLE",
            2,
            2,
            CommandFlags::write(),
            cmd_json_toggle,
            "Toggle a JSON boolean",
        ));
        self.register(Command::new(
            "JSON.CLEAR",
            1,
            2,
            CommandFlags::write(),
            cmd_json_clear,
            "Clear a JSON value",
        ));
        self.register(Command::new(
            "JSON.RESP",
            1,
            2,
            CommandFlags::readonly(),
            cmd_json_resp,
            "Get JSON as RESP",
        ));
        self.register(Command::new(
            "JSON.DEBUG",
            2,
            3,
            CommandFlags::readonly(),
            cmd_json_debug,
            "Debug JSON commands",
        ));
        self.register(Command::new(
            "JSON.MERGE",
            3,
            3,
            CommandFlags::write(),
            cmd_json_merge,
            "Merge a JSON value using RFC 7396 JSON Merge Patch",
        ));
        self.register(Command::new(
            "JSON.MSET",
            3,
            -1,
            CommandFlags::write(),
            cmd_json_mset,
            "Set JSON values at paths in multiple keys",
        ));
    }

    fn register_vectorset_commands(&mut self) {
        use super::vectorset_cmds::*;

        self.register(Command::new(
            "VADD",
            3,
            -1,
            CommandFlags::write(),
            cmd_vadd,
            "Add elements to a vector set",
        ));
        self.register(Command::new(
            "VCARD",
            1,
            1,
            CommandFlags::readonly(),
            cmd_vcard,
            "Get the cardinality of a vector set",
        ));
        self.register(Command::new(
            "VDIM",
            1,
            1,
            CommandFlags::readonly(),
            cmd_vdim,
            "Get the dimensionality of a vector set",
        ));
        self.register(Command::new(
            "VEMB",
            2,
            3,
            CommandFlags::readonly(),
            cmd_vemb,
            "Get the embedding vector for an element",
        ));
        self.register(Command::new(
            "VSIM",
            3,
            -1,
            CommandFlags::readonly(),
            cmd_vsim,
            "Find similar elements in a vector set",
        ));
        self.register(Command::new(
            "VREM",
            2,
            -1,
            CommandFlags::write(),
            cmd_vrem,
            "Remove elements from a vector set",
        ));
        self.register(Command::new(
            "VGETATTR",
            3,
            3,
            CommandFlags::readonly(),
            cmd_vgetattr,
            "Get an attribute from a vector set element",
        ));
        self.register(Command::new(
            "VSETATTR",
            4,
            4,
            CommandFlags::write(),
            cmd_vsetattr,
            "Set an attribute on a vector set element",
        ));
        self.register(Command::new(
            "VINFO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_vinfo,
            "Get information about a vector set",
        ));
        self.register(Command::new(
            "VLINKS",
            2,
            4,
            CommandFlags::readonly(),
            cmd_vlinks,
            "Get links to similar elements",
        ));
        self.register(Command::new(
            "VRANDMEMBER",
            1,
            2,
            CommandFlags::readonly(),
            cmd_vrandmember,
            "Get random members from a vector set",
        ));
        self.register(Command::new(
            "VISMEMBER",
            2,
            2,
            CommandFlags::readonly(),
            cmd_vismember,
            "Check if an element exists in a vector set",
        ));
        self.register(Command::new(
            "VRANGE",
            3,
            4,
            CommandFlags::readonly(),
            cmd_vrange,
            "Return elements in a lexicographical range",
        ));
    }

    fn register_search_commands(&mut self) {
        use super::search_cmds::*;

        self.register(Command::new(
            "FT.CREATE",
            4,
            -1,
            CommandFlags::write(),
            cmd_ft_create,
            "Create a full-text search index",
        ));
        self.register(Command::new(
            "FT.DROPINDEX",
            1,
            2,
            CommandFlags::write(),
            cmd_ft_dropindex,
            "Drop a full-text search index",
        ));
        self.register(Command::new(
            "FT.ADD",
            5,
            -1,
            CommandFlags::write(),
            cmd_ft_add,
            "Add a document to a search index",
        ));
        self.register(Command::new(
            "FT.SEARCH",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_ft_search,
            "Search a full-text index",
        ));
        self.register(Command::new(
            "FT.INFO",
            1,
            1,
            CommandFlags::readonly(),
            cmd_ft_info,
            "Get information about an index",
        ));
        self.register(Command::new(
            "FT._LIST",
            0,
            0,
            CommandFlags::readonly(),
            cmd_ft_list,
            "List all search indexes",
        ));
        // Redis 8.4+ hybrid search
        self.register(Command::new(
            "FT.HYBRID",
            2,
            -1,
            CommandFlags::readonly(),
            cmd_ft_hybrid,
            "Hybrid search combining vector and full-text (Redis 8.4+)",
        ));
    }
}
