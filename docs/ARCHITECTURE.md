# Viator System Architecture

> **Note:** Viator is Redis® 8.4.0 protocol-compatible. Redis is a registered trademark of Redis Ltd. Viator is not affiliated with or endorsed by Redis Ltd.

This document provides a comprehensive overview of Viator's internal architecture, design decisions, and implementation details.

## Table of Contents

1. [High-Level Architecture](#high-level-architecture)
2. [Module Overview](#module-overview)
3. [Protocol Layer](#protocol-layer)
4. [Command Execution](#command-execution)
5. [Storage Engine](#storage-engine)
6. [Data Types](#data-types)
7. [Persistence](#persistence)
8. [Security](#security)
9. [Distributed Systems](#distributed-systems)
10. [Performance Optimizations](#performance-optimizations)

---

## High-Level Architecture

Viator follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Network Layer                                   │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────────┐│
│  │  TCP Listener  │  │  TLS Termination│  │    Connection Manager         ││
│  │  (Tokio)       │  │  (rustls)       │  │    (Rate Limiting, ACL)       ││
│  └────────────────┘  └────────────────┘  └────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Protocol Layer                                  │
│  ┌────────────────────────────┐  ┌────────────────────────────────────────┐│
│  │       RESP3 Parser         │  │         Frame Builder                  ││
│  │  (Zero-copy, Streaming)    │  │    (Bulk, Array, Error, etc.)          ││
│  └────────────────────────────┘  └────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Command Layer                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │                    Command Registry (200+ commands)                    ││
│  │  Strings │ Lists │ Hashes │ Sets │ Sorted Sets │ Streams │ Geo │ JSON ││
│  └────────────────────────────────────────────────────────────────────────┘│
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────────┐│
│  │  Transactions  │  │  Lua Scripting │  │       Pub/Sub                  ││
│  │  (MULTI/EXEC)  │  │  (Sandboxed)   │  │    (Channel-based)             ││
│  └────────────────┘  └────────────────┘  └────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Storage Layer                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │              Database (DashMap - Concurrent Hash Table)                ││
│  └────────────────────────────────────────────────────────────────────────┘│
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────────┐│
│  │ Expiry Manager │  │  Watch Manager │  │    Buffer Pool                 ││
│  │ (Lazy + Active)│  │  (Optimistic)  │  │    (Memory Reuse)              ││
│  └────────────────┘  └────────────────┘  └────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Persistence Layer                                 │
│  ┌────────────────────────────┐  ┌────────────────────────────────────────┐│
│  │        AOF Writer          │  │          VDB Snapshots                  ││
│  │  (Append-Only, fsync)      │  │    (Background, Fork-free)              ││
│  └────────────────────────────┘  └────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Distributed Layer                                  │
│  ┌────────────────────┐  ┌──────────────────┐  ┌─────────────────────────┐ │
│  │    Cluster Bus     │  │    Sentinel      │  │    Replication          │ │
│  │  (Gossip Protocol) │  │   (Monitoring)   │  │  (Master/Replica)       │ │
│  │  (Failure Detection)│ │   (Failover)     │  │  (PSYNC)                │ │
│  └────────────────────┘  └──────────────────┘  └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Module Overview

### Source Structure

```
src/
├── commands/           # Command implementations
│   ├── mod.rs          # ParsedCommand, CommandFlags
│   ├── registry.rs     # CommandRegistry with 200+ commands
│   ├── executor.rs     # CommandExecutor coordination
│   ├── strings.rs      # String commands (GET, SET, etc.)
│   ├── lists.rs        # List commands (LPUSH, RPOP, etc.)
│   ├── hashes.rs       # Hash commands (HSET, HGET, etc.)
│   ├── sets.rs         # Set commands (SADD, SMEMBERS, etc.)
│   ├── sorted_sets.rs  # Sorted set commands (ZADD, ZRANGE, etc.)
│   ├── streams.rs      # Stream commands (XADD, XREAD, etc.)
│   ├── geo.rs          # Geo commands (GEOADD, GEODIST, etc.)
│   ├── bitmap.rs       # Bitmap commands (SETBIT, BITCOUNT, etc.)
│   ├── hyperloglog.rs  # HyperLogLog (PFADD, PFCOUNT, etc.)
│   ├── keys.rs         # Key commands (DEL, EXISTS, EXPIRE, etc.)
│   ├── server_cmds.rs  # Server commands (INFO, CONFIG, etc.)
│   ├── connection.rs   # Connection commands (AUTH, SELECT, etc.)
│   ├── transactions.rs # Transaction commands (MULTI, EXEC, etc.)
│   ├── pubsub.rs       # Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.)
│   ├── scripting.rs    # Lua scripting (EVAL, EVALSHA, etc.)
│   ├── blocking.rs     # Blocking commands (BLPOP, BRPOP, etc.)
│   ├── cluster.rs      # Cluster commands (CLUSTER INFO, etc.)
│   ├── sentinel.rs     # Sentinel commands (SENTINEL MASTER, etc.)
│   ├── json_cmds.rs    # JSON module (JSON.SET, JSON.GET, etc.)
│   ├── ts_cmds.rs      # TimeSeries (TS.CREATE, TS.ADD, etc.)
│   ├── bloom_cmds.rs   # Bloom filter (BF.ADD, BF.EXISTS, etc.)
│   ├── cuckoo_cmds.rs  # Cuckoo filter (CF.ADD, CF.DEL, etc.)
│   ├── topk_cmds.rs    # Top-K (TOPK.RESERVE, TOPK.ADD, etc.)
│   ├── tdigest_cmds.rs # T-Digest (TDIGEST.CREATE, etc.)
│   └── cms_cmds.rs     # Count-Min Sketch (CMS.INITBYPROB, etc.)
│
├── protocol/           # RESP3 protocol handling
│   ├── mod.rs          # Module exports
│   ├── frame.rs        # Frame enum (Bulk, Array, Error, etc.)
│   └── parser.rs       # RespParser (streaming, zero-copy)
│
├── server/             # Server infrastructure
│   ├── mod.rs          # Server struct, main loop
│   ├── config.rs       # Server configuration
│   ├── connection.rs   # Connection handling
│   ├── state.rs        # ClientState (per-connection)
│   ├── metrics.rs      # ServerMetrics (counters, latency, CPU)
│   ├── pubsub.rs       # PubSubHub (channel management)
│   ├── watch.rs        # WatchRegistry (WATCH/UNWATCH)
│   ├── rate_limit.rs   # Rate limiting per IP
│   ├── replication.rs  # Master/Replica replication
│   ├── cluster.rs      # ClusterManager (slot management)
│   ├── cluster_bus.rs  # ClusterBus (gossip protocol)
│   └── sentinel.rs     # SentinelMonitor (HA monitoring)
│
├── storage/            # Data storage
│   ├── mod.rs          # Module exports
│   ├── db.rs           # Database (DashMap-based)
│   └── expiry.rs       # ExpiryManager (TTL handling)
│
├── persistence/        # Data persistence
│   ├── mod.rs          # Module exports, PersistenceConfig
│   ├── aof.rs          # Append-Only File
│   └── vdb.rs          # VDB (Viator Database) snapshots
│
├── security/           # Security features
│   ├── mod.rs          # Module exports
│   ├── acl.rs          # Access Control Lists
│   ├── password.rs     # Argon2id password hashing
│   ├── tls_config.rs   # TLS configuration
│   ├── audit.rs        # Audit logging
│   ├── lua_sandbox.rs  # Lua script sandboxing
│   ├── command_protection.rs  # Command-level protection
│   └── connection_limits.rs   # Connection/buffer limits
│
├── types/              # Data type implementations
│   ├── mod.rs          # Module exports, ViatorValue enum
│   ├── value.rs        # Value wrapper with metadata
│   ├── key.rs          # Key type with hash optimization
│   ├── list.rs         # List implementation (VecDeque)
│   ├── set.rs          # Set implementation (HashSet)
│   ├── sorted_set.rs   # Sorted set (skip list + hash)
│   ├── stream.rs       # Stream (radix tree-like)
│   ├── bloom.rs        # Bloom filter
│   ├── cuckoo.rs       # Cuckoo filter
│   ├── topk.rs         # Top-K (Heavy Keeper)
│   ├── tdigest.rs      # T-Digest (quantile estimation)
│   ├── cms.rs          # Count-Min Sketch
│   ├── timeseries.rs   # Time series data
│   ├── search.rs       # Full-text search index
│   └── graph.rs        # Graph data structure
│
├── pool.rs             # Buffer pooling
├── error.rs            # Error types
├── telemetry.rs        # OpenTelemetry integration
├── lib.rs              # Library root
└── main.rs             # Binary entry point
```

---

## Protocol Layer

### RESP3 Parser

The protocol layer implements the Redis Serialization Protocol version 3 (RESP3).

```rust
// Frame types supported
pub enum Frame {
    Simple(String),           // +OK\r\n
    Error(String),            // -ERR message\r\n
    Integer(i64),             // :123\r\n
    Bulk(Bytes),              // $5\r\nhello\r\n
    Array(Vec<Frame>),        // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Null,                     // _\r\n
    Boolean(bool),            // #t\r\n or #f\r\n
    Double(f64),              // ,3.14\r\n
    BigNumber(String),        // (12345678901234567890\r\n
    Map(Vec<(Frame, Frame)>), // %2\r\n...
    Set(Vec<Frame>),          // ~3\r\n...
    Push(Vec<Frame>),         // >2\r\n...
}
```

### Parser Design

The parser uses a streaming, zero-copy design:

1. **Streaming**: Handles partial reads without buffering entire messages
2. **Zero-copy**: Uses `Bytes` for efficient buffer sharing
3. **SIMD-optimized**: Uses `memchr` for fast CRLF detection

```rust
pub struct RespParser {
    buffer: BytesMut,
    state: ParseState,
}

impl RespParser {
    pub fn parse(&mut self) -> Result<Option<Frame>> {
        // Attempt to parse a complete frame
        // Returns None if more data needed
    }
}
```

---

## Command Execution

### Command Registry

All commands are registered in a central registry with metadata:

```rust
pub struct Command {
    pub name: &'static str,
    pub min_args: i32,
    pub max_args: i32,  // -1 for unlimited
    pub flags: CommandFlags,
    pub handler: CommandHandler,
    pub summary: &'static str,
}

pub struct CommandFlags {
    pub readonly: bool,   // Doesn't modify data
    pub write: bool,      // Modifies data
    pub admin: bool,      // Admin-only
    pub blocking: bool,   // May block
    pub no_keys: bool,    // Doesn't access keys
    pub fast: bool,       // O(1) complexity
    pub pubsub: bool,     // Pub/Sub command
}
```

### Command Flow

```
┌─────────────┐    ┌──────────────┐    ┌───────────────┐    ┌─────────────┐
│   Client    │───▶│    Parser    │───▶│   Registry    │───▶│   Handler   │
│   Request   │    │  (RESP3)     │    │   (Lookup)    │    │  (Execute)  │
└─────────────┘    └──────────────┘    └───────────────┘    └─────────────┘
                                                                   │
                                                                   ▼
┌─────────────┐    ┌──────────────┐    ┌───────────────┐    ┌─────────────┐
│   Client    │◀───│    Frame     │◀───│   Storage     │◀───│   Result    │
│   Response  │    │   Builder    │    │   (DashMap)   │    │             │
└─────────────┘    └──────────────┘    └───────────────┘    └─────────────┘
```

### Async Execution

Commands are async and use Tokio for concurrency:

```rust
pub type CommandHandler = fn(
    ParsedCommand,
    Arc<Db>,
    Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>>;
```

---

## Storage Engine

### Database Structure

The core database uses DashMap for concurrent access:

```rust
pub struct Database {
    // Main key-value storage
    data: DashMap<Key, StoredValue>,

    // Expiry tracking
    expiry_index: DashMap<Key, Instant>,

    // LRU tracking (access times for eviction)
    access_times: DashMap<Key, u64>,

    // Database selection (0-15)
    db_index: usize,

    // Connection tracking (for accurate INFO metrics)
    connected_clients: AtomicU64,
    total_connections: AtomicU64,

    // Shutdown coordination
    shutdown_requested: AtomicBool,
    save_on_shutdown: AtomicBool,
}

pub struct StoredValue {
    value: ViatorValue,
    expiry: Option<Instant>,
    created_at: Instant,
    accessed_at: AtomicU64,
}
```

### Concurrency Model

- **Lock-free reads**: DashMap provides concurrent read access
- **Sharded writes**: Keys are sharded across multiple internal maps
- **Atomic operations**: INCR, HINCRBY, etc. use atomic primitives

### Memory Management

```rust
// jemalloc for reduced fragmentation
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Buffer pooling for connection buffers
pub struct BufferPool {
    pool: ArrayQueue<BytesMut>,
    buffer_size: usize,
}
```

### LRU Eviction

Viator implements Redis-compatible LRU eviction using approximation:

```rust
const SAMPLE_SIZE: usize = 5;

pub fn evict_lru(&self) -> bool {
    // Redis-style LRU: sample 5 random keys, evict oldest
    // This gives ~99% accuracy with O(1) overhead (vs O(n) for true LRU)
    let mut oldest_key = None;
    let mut oldest_time = u64::MAX;

    for entry in self.data.iter().take(SAMPLE_SIZE) {
        let key = entry.key();
        let access_time = self.access_times.get(key).map(|r| *r).unwrap_or(0);
        if access_time < oldest_time {
            oldest_time = access_time;
            oldest_key = Some(key.clone());
        }
    }

    if let Some(key) = oldest_key {
        self.data.remove(&key);
        self.access_times.remove(&key);
        true
    } else {
        false
    }
}
```

**Eviction policies supported:**
- `allkeys-lru`: Evict any key using LRU approximation
- `volatile-lru`: Evict only keys with TTL using LRU
- `allkeys-random`: Evict random keys
- `volatile-random`: Evict random keys with TTL
- `volatile-ttl`: Evict keys closest to expiration
- `noeviction`: Return error when memory limit reached

---

## Data Types

### Type Hierarchy

```rust
pub enum ViatorValue {
    String(Bytes),
    List(ViatorList),
    Hash(ViatorHash),
    Set(ViatorSet),
    SortedSet(ViatorSortedSet),
    Stream(ViatorStream),
    // Redis Stack types
    Bloom(BloomFilter),
    Cuckoo(CuckooFilter),
    TopK(TopK),
    TDigest(TDigest),
    CMS(CountMinSketch),
    TimeSeries(TimeSeries),
    Json(serde_json::Value),
}
```

### Sorted Set Implementation

Uses a dual data structure for O(1) access and O(log n) range queries:

```rust
pub struct ViatorSortedSet {
    // Score -> Members (for range queries)
    by_score: SkipMap<OrderedFloat<f64>, HashSet<Bytes>>,

    // Member -> Score (for O(1) lookups)
    by_member: HashMap<Bytes, f64>,
}
```

### Probabilistic Data Structures

#### Bloom Filter
```rust
pub struct BloomFilter {
    bits: BitVec,
    num_hashes: u32,
    num_items: u64,
}
```

#### Cuckoo Filter
```rust
pub struct CuckooFilter {
    buckets: Vec<Bucket>,
    bucket_size: usize,
    max_kicks: u32,
    count: u64,
}
```

#### Count-Min Sketch
```rust
pub struct CountMinSketch {
    table: Vec<Vec<u64>>,
    width: usize,
    depth: usize,
}
```

---

## Persistence

### Append-Only File (AOF)

```rust
pub struct AofWriter {
    file: File,
    buffer: BytesMut,
    sync_policy: SyncPolicy,
}

pub enum SyncPolicy {
    Always,     // fsync after every write
    EverySec,   // fsync every second
    No,         // Let OS decide
}
```

AOF format uses RESP protocol for commands:

```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n
```

### VDB Snapshots

VDB (Viator Database) provides point-in-time snapshots with full data integrity:

```rust
pub struct VdbSaver {
    file: BufWriter<File>,
    crc64: u64,  // CRC64-ECMA checksum
}

pub struct VdbLoader {
    data: Vec<u8>,
    pos: usize,
    crc64: u64,  // Running checksum for validation
}

// VDB file format
// +-------+---------+----------+--------+---------+
// | VIATR | VERSION | DB_SEL   | KEY_VAL|  EOF    |
// | MAGIC | (0009)  | ECTOR    |  PAIRS | + CRC64 |
// +-------+---------+----------+--------+---------+
```

#### Key Features

- **CRC64-ECMA Checksum**: Full file integrity validation using polynomial `0xC96C5795D7870F42`
- **LZF Compression**: Transparent decompression of LZF-compressed strings
- **Stream Persistence**: Full stream metadata (last_id, entries_added) preserved for ID continuity
- **VectorSet Persistence**: HNSW vector indexes saved with VDB type 20
- **Atomic Writes**: Temp file + rename pattern for crash safety

#### Supported Data Types

| VDB Type Constant | Value |
|-------------------|-------|
| VDB_TYPE_STRING | 0 |
| VDB_TYPE_LIST | 1 |
| VDB_TYPE_SET | 2 |
| VDB_TYPE_ZSET | 3 |
| VDB_TYPE_HASH | 4 |
| VDB_TYPE_VECTORSET | 20 |
| VDB_TYPE_STREAM | 21 |

---

## Security

### ACL System

```rust
pub struct AclUser {
    name: String,
    password_hash: Option<String>,
    enabled: bool,
    commands: CommandPermissions,
    keys: KeyPatterns,
    channels: ChannelPatterns,
}

pub struct CommandPermissions {
    allowed: HashSet<String>,
    denied: HashSet<String>,
    categories: HashSet<String>,
}
```

### Password Hashing

Uses Argon2id for secure password storage:

```rust
pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool> {
    let parsed = PasswordHash::new(hash)?;
    Ok(Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
}
```

### AUTH Rate Limiting

Protects against brute-force attacks:

```rust
// Per-connection tracking
const MAX_AUTH_FAILURES: u32 = 10;
const AUTH_LOCKOUT_SECONDS: u64 = 60;

pub struct ClientState {
    auth_failures: AtomicU32,
    auth_lockout_until: AtomicU64,
}

impl ClientState {
    pub fn auth_is_blocked(&self) -> Option<u64>;  // Returns seconds remaining
    pub fn auth_record_failure(&self) -> bool;     // Returns true if now locked
    pub fn auth_reset_failures(&self);             // Called on success
}
```

### Lua Sandboxing

```rust
pub struct LuaSandbox {
    lua: Lua,
    memory_limit: usize,
    instruction_limit: u64,
}

// Real SHA1 hashing for script identification (Redis-compatible)
fn sha1_hash(script: &str) -> String {
    use sha1::{Sha1, Digest};
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

// Restricted function set
const ALLOWED_GLOBALS: &[&str] = &[
    "redis", "KEYS", "ARGV",
    "string", "table", "math",
    "tonumber", "tostring", "type",
    "pairs", "ipairs", "next",
    "unpack", "select", "error",
];
```

---

## Distributed Systems

### Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Cluster State                             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              Slot Assignment (16384 slots)              │    │
│  │  Node A: 0-5460 │ Node B: 5461-10922 │ Node C: 10923-16383│  │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Node A       │     │    Node B       │     │    Node C       │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │  Master   │  │◀───▶│  │  Master   │  │◀───▶│  │  Master   │  │
│  │ 0-5460    │  │     │  │ 5461-10922│  │     │  │10923-16383│  │
│  └───────────┘  │     │  └───────────┘  │     │  └───────────┘  │
│       │         │     │       │         │     │       │         │
│       ▼         │     │       ▼         │     │       ▼         │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │  Replica  │  │     │  │  Replica  │  │     │  │  Replica  │  │
│  └───────────┘  │     │  └───────────┘  │     │  └───────────┘  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Gossip Protocol

The cluster bus uses a gossip protocol for state propagation:

```rust
pub struct ClusterMsgHeader {
    signature: [u8; 4],     // "RCmb"
    totlen: u32,
    version: u16,
    port: u16,
    msg_type: ClusterMsgType,
    sender: NodeId,
    slots: [u8; 2048],      // Slot bitmap
    master_id: NodeId,
    flags: u16,
    current_epoch: u64,
    config_epoch: u64,
}

pub enum ClusterMsgType {
    Ping,
    Pong,
    Meet,
    Fail,
    Publish,
    FailAuth,
    Update,
}
```

### Failure Detection

```rust
// Node state machine
pub enum NodeState {
    Online,
    PFail,  // Possibly failed (local detection)
    Fail,   // Confirmed failed (cluster consensus)
}

// Failure detection parameters
const CLUSTER_NODE_TIMEOUT: Duration = Duration::from_secs(15);
const CLUSTER_FAIL_REPORT_VALIDITY: Duration = Duration::from_secs(60);
```

### Slot Migration (Redis 8.4)

```rust
pub struct MigrationTask {
    id: String,
    slot_ranges: Vec<(u16, u16)>,
    state: String,  // pending, running, trimming, completed
    progress: u32,
}

// CLUSTER MIGRATION IMPORT 0-1000 NODES source-id
// CLUSTER MIGRATION STATUS
// CLUSTER MIGRATION CANCEL task-id
```

### Sentinel Monitoring

```rust
pub struct SentinelMonitor {
    masters: DashMap<String, Arc<MonitoredMaster>>,
    sentinels: DashMap<String, InstanceConnection>,
    my_id: String,
    current_epoch: AtomicU64,
}

pub struct MonitoredMaster {
    config: RwLock<MasterConfig>,
    state: RwLock<MasterState>,
    replicas: DashMap<String, ReplicaInfo>,
    sentinels: DashMap<String, SentinelInfo>,
    last_ping: AtomicU64,
    failover_state: RwLock<FailoverState>,
}
```

---

## Performance Optimizations

### Memory Efficiency

1. **Small String Optimization**: SmallVec for short strings
2. **Buffer Pooling**: Reuse connection buffers
3. **Zero-copy Parsing**: Bytes crate for shared ownership

### Concurrency

1. **Lock-free Data Structures**: DashMap, crossbeam-queue
2. **Atomic Operations**: AtomicU64 for counters
3. **Sharded State**: Reduce contention

### I/O Optimization

1. **Tokio Runtime**: Multi-threaded async I/O
2. **io_uring Support**: Linux io_uring via tokio-uring
3. **Batched Writes**: Coalesce small writes

### CPU Optimization

1. **SIMD**: memchr for fast byte searching
2. **Cache-friendly**: Contiguous memory layouts
3. **Branch Prediction**: Likely/unlikely hints

---

## Configuration

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `port` | 6379 | TCP port |
| `bind` | 127.0.0.1 | Bind address |
| `max_clients` | 10000 | Max connections |
| `timeout` | 0 | Client timeout (0=never) |
| `tcp_keepalive` | 300 | TCP keepalive (seconds) |

### Memory Options

| Option | Default | Description |
|--------|---------|-------------|
| `maxmemory` | 0 | Max memory (0=unlimited) |
| `maxmemory_policy` | noeviction | Eviction policy |

### Persistence Options

| Option | Default | Description |
|--------|---------|-------------|
| `appendonly` | no | Enable AOF |
| `appendfsync` | everysec | AOF fsync policy |
| `save` | "" | VDB snapshot triggers |

---

## Metrics and Monitoring

### Server Metrics

```rust
pub struct ServerMetrics {
    // Connections
    pub active_connections: AtomicU64,
    pub total_connections: AtomicU64,
    pub rejected_connections: AtomicU64,

    // Commands
    pub commands_processed: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,

    // Errors
    pub errors: AtomicU64,
    pub parse_errors: AtomicU64,

    // Latency histogram
    pub command_latency: LatencyHistogram,
}

// CPU metrics via getrusage()
pub struct CpuUsage {
    pub sys: f64,          // System CPU time (main process)
    pub user: f64,         // User CPU time (main process)
    pub sys_children: f64, // System CPU time (background tasks)
    pub user_children: f64,// User CPU time (background tasks)
}
```

### OpenTelemetry Integration

Optional telemetry export via OTLP:

```rust
#[cfg(feature = "telemetry")]
pub fn init_telemetry(endpoint: &str) -> Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    Ok(())
}
```

---

## Testing Strategy

### Unit Tests

Each module has comprehensive unit tests:

```bash
cargo test                    # Run all tests
cargo test --lib              # Library tests only
cargo test commands::         # Command tests
cargo test storage::          # Storage tests
```

### Integration Tests

```bash
# Using viator-cli for real protocol tests
viator-cli -p 6380 PING
viator-cli -p 6380 SET key value
viator-cli -p 6380 GET key
```

### Benchmarks

```bash
cargo bench protocol          # Protocol parsing
cargo bench storage           # Storage operations
```

### Property Testing

Using proptest for fuzzing:

```rust
proptest! {
    #[test]
    fn bulk_string_roundtrip(s: String) {
        let frame = Frame::Bulk(Bytes::from(s.clone()));
        let serialized = frame.serialize();
        let parsed = RespParser::parse(&serialized)?;
        assert_eq!(parsed, frame);
    }
}
```

---

## Related Documentation

For detailed information on specific topics:

- **[COMPATIBILITY.md](COMPATIBILITY.md)** - Redis 8.4.0 command compatibility spec
- **[PERSISTENCE.md](PERSISTENCE.md)** - AOF and VDB persistence details
- **[SECURITY.md](SECURITY.md)** - Authentication, ACL, TLS, and sandboxing
- **[VECTORSET.md](VECTORSET.md)** - HNSW vector similarity search
