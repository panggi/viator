# Viator

A complete Redis-compatible server implementation

[![Crates.io](https://img.shields.io/crates/v/viator.svg)](https://crates.io/crates/viator)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Redis Compatible](https://img.shields.io/badge/redis-8.4.0-red.svg)](https://redis.io/)

> **Note:** Viator is Redis® 8.4.0 protocol-compatible. Redis is a registered trademark of Redis Ltd. Viator is not affiliated with or endorsed by Redis Ltd.

## Status

**Published on crates.io** - v0.1.3 complete Redis 8.4.0 feature parity. Verified with redis-cli 8.4.0. Suitable for testing and non-production workloads.

| Category | Status |
|----------|--------|
| Core Data Types | ✅ String, List, Hash, Set, Sorted Set, Stream |
| Redis Stack | ✅ JSON, Bloom, Cuckoo, CMS, TopK, TimeSeries |
| Persistence (VDB/AOF) | ✅ CRC64 checksums, LZF compression, atomic writes, non-blocking |
| Security | ✅ ACL, TLS, AUTH rate limiting, constant-time password comparison |
| Memory Management | ✅ LRU eviction with Redis-style 5-key sampling |
| Cluster/Sentinel | ✅ Full implementation |
| Lua Scripting | ✅ Lua 5.4, real SHA1, EVALSHA |
| VectorSet | ✅ HNSW-based similarity search |
| Operability | ✅ Accurate INFO metrics, SHUTDOWN SAVE/NOSAVE/ABORT |

## Overview

Viator is a complete Redis-compatible server implementation featuring:

- **Full RESP3 Protocol** - Wire-compatible with Redis clients
- **All Core Data Types** - String, List, Hash, Set, Sorted Set, Stream, VectorSet
- **Vector Similarity Search** - HNSW algorithm for semantic search and RAG applications
- **Redis Stack Modules** - JSON, TimeSeries, Bloom, Cuckoo, TopK, T-Digest, CMS
- **High Availability** - Cluster mode with automatic failover, Sentinel monitoring
- **Persistence** - AOF and VDB with CRC64 checksums and LZF compression
- **Lua Scripting** - Full Lua 5.4 scripting with real SHA1 hashing
- **Enterprise Security** - ACL, TLS, audit logging, AUTH rate limiting

## Installation

### From crates.io

```bash
cargo install viator
```

This installs all binaries to `~/.cargo/bin/`:
- `viator` - Main server
- `viator-cli` - Command-line client
- `viator-benchmark` - Performance benchmarking
- `viator-check-vdb` - VDB file integrity checker
- `viator-check-aof` - AOF file checker
- `viator-sentinel` - High availability monitor

### Build from Source

```bash
# Clone the repository
git clone https://github.com/panggi/viator.git
cd viator

# Build in release mode
cargo build --release

# Run the server
./target/release/viator
```

## Quick Start

```bash
# Start the server
viator --port 6379 --bind 127.0.0.1
```

### Using viator-cli

```bash
# Connect with viator-cli
viator-cli -p 6379

# Test basic commands
127.0.0.1:6379> SET hello "world"
OK
127.0.0.1:6379> GET hello
"world"
```

### Configuration Options

```bash
viator --port 6380           # Custom port
viator --bind 0.0.0.0        # Listen on all interfaces
viator --help                # Show all options
```

## Features

### Data Types

| Type | Commands |
|------|----------|
| **Strings** | GET, SET, MGET, MSET, INCR, APPEND, GETRANGE, SETEX, SETNX |
| **Lists** | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LINSERT, LLEN |
| **Hashes** | HSET, HGET, HMGET, HGETALL, HDEL, HINCRBY, HGETDEL*, HSETEX* |
| **Sets** | SADD, SREM, SMEMBERS, SINTER, SUNION, SDIFF, SCARD |
| **Sorted Sets** | ZADD, ZREM, ZRANGE, ZRANK, ZSCORE, ZINCRBY, ZRANGEBYSCORE |
| **Streams** | XADD, XREAD, XRANGE, XLEN, XGROUP, XACK, XCLAIM |
| **HyperLogLog** | PFADD, PFCOUNT, PFMERGE |
| **Bitmaps** | SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS |
| **Geo** | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEOSEARCH |
| **VectorSet** | VADD, VSIM, VGET, VDEL, VCARD, VINFO |

*Redis 8.0+ commands

### Redis Stack Modules

```bash
# JSON
JSON.SET user $ '{"name":"John","age":30}'
JSON.GET user $.name

# Bloom Filter
BF.ADD myfilter "item1"
BF.EXISTS myfilter "item1"

# Time Series
TS.CREATE temperature RETENTION 86400
TS.ADD temperature * 23.5

# Top-K
TOPK.RESERVE trending 10
TOPK.ADD trending "item1" "item2"
```

### Vector Similarity Search

HNSW-based vector similarity search for semantic search, recommendations, and RAG:

```bash
# Add vectors (128-dimensional embeddings)
VADD embeddings FP32 VALUES 128 0.1 0.2 ... 0.9 "doc1"
VADD embeddings FP32 VALUES 128 0.2 0.3 ... 0.8 "doc2" SETATTR category "tech"

# Find similar vectors
VSIM embeddings FP32 VALUES 128 0.1 0.2 ... 0.9 TOPK 10

# With filtering
VSIM embeddings FP32 VALUES 128 0.1 0.2 ... 0.9 TOPK 10 FILTER "category == 'tech'"

# Get vector info
VINFO embeddings
VCARD embeddings
```

See [VECTORSET.md](docs/VECTORSET.md) for detailed documentation.

### Cluster Mode (Redis 8.4 Compatible)

```bash
# Cluster information
CLUSTER INFO
CLUSTER NODES
CLUSTER SLOTS

# Redis 8.2+ Slot Statistics
CLUSTER SLOT-STATS SLOTSRANGE 0 100

# Redis 8.4 Atomic Migration
CLUSTER MIGRATION IMPORT 0-1000 NODES source-node-id
CLUSTER MIGRATION STATUS
```

### Sentinel High Availability

```bash
# Monitor a master
SENTINEL MONITOR mymaster 127.0.0.1 6379 2

# Get master address
SENTINEL GET-MASTER-ADDR-BY-NAME mymaster

# Check quorum
SENTINEL CKQUORUM mymaster
```

### Transactions & Scripting

```bash
# Transactions
MULTI
SET key1 "value1"
INCR counter
EXEC

# Lua Scripting
EVAL "return redis.call('GET', KEYS[1])" 1 mykey
```

### Pub/Sub

```bash
# Subscribe to channels
SUBSCRIBE news sports

# Publish messages
PUBLISH news "Breaking news!"
```

## Architecture

Viator is built on a modular architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────────┐ │
│  │  RESP3  │  │   TLS   │  │  Rate   │  │  Connection     │ │
│  │ Parser  │  │ Support │  │ Limiter │  │  Manager        │ │
│  └─────────┘  └─────────┘  └─────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                       Command Layer                          │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Command Registry (200+ commands)           ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────┐ │
│  │  Strings │ │  Lists   │ │  Hashes  │ │  Sorted Sets   │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────────────┘ │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────┐ │
│  │  Streams │ │   Geo    │ │   JSON   │ │  TimeSeries    │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────────────┘ │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                       Storage Layer                          │
│  ┌─────────────────────────────────────────────────────────┐│
│  │          DashMap (Concurrent Hash Tables)               ││
│  └─────────────────────────────────────────────────────────┘│
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐ │
│  │    Expiry    │  │   Pub/Sub    │  │    Persistence    │ │
│  │   Manager    │  │     Hub      │  │   (AOF + VDB)     │ │
│  └──────────────┘  └──────────────┘  └───────────────────┘ │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                    Distributed Layer                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  Cluster Bus    │  │    Sentinel     │  │ Replication │ │
│  │  (Gossip)       │  │    Monitor      │  │   Manager   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

For detailed documentation:

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - System design and module overview
- **[COMPATIBILITY.md](docs/COMPATIBILITY.md)** - Redis 8.4.0 command compatibility spec
- **[PERSISTENCE.md](docs/PERSISTENCE.md)** - AOF and VDB persistence
- **[SECURITY.md](docs/SECURITY.md)** - Authentication, ACL, TLS
- **[VECTORSET.md](docs/VECTORSET.md)** - Vector similarity search

## Performance

Viator leverages Rust's zero-cost abstractions for high performance:

- **Memory Allocator**: mimalloc for fast short-lived allocations
- **Concurrency**: Lock-free data structures (DashMap, crossbeam)
- **I/O**: Tokio async runtime with io_uring support (Linux)
- **Zero-Copy**: Bytes crate for efficient buffer management
- **SIMD**: memchr for fast byte searching
- **Perfect Hashing**: O(1) command dispatch with PHF
- **Sharded Storage**: 64-shard architecture for reduced lock contention
- **Inline Responses**: Pre-allocated RESP responses for common replies
- **Response Coalescing**: Batched writes to reduce syscall overhead

### Benchmark Results

Tested with `redis-benchmark` on macOS (M-series Apple Silicon):

**Single Commands** (50 clients, 100k requests):

| Command | Viator | Redis 8.4.0 | Ratio |
|---------|-------:|------------:|------:|
| SET | 91K/s | 116K/s | 78% |
| GET | 92K/s | 111K/s | 83% |

**With Pipelining** (P=16, batching 16 commands per round-trip):

| Command | Viator | Redis 8.4.0 | Ratio |
|---------|-------:|------------:|------:|
| SET | **1.19M/s** | 709K/s | **168%** |
| GET | **1.45M/s** | 1.33M/s | **109%** |

**Summary**: With pipelining (the typical production pattern), Viator **outperforms Redis by 68% on writes**. The concurrent DashMap architecture shines when TCP round-trip overhead is minimized.

### Persistence Size

VDB format matches Redis RDB size:
- 6,000 keys × 100-byte values → VDB: 761KB, RDB: 762KB

## Security

### Memory Safety
- Written in safe Rust (`#![forbid(unsafe_code)]`)
- Eliminates buffer overflows, use-after-free, data races
- Bounds checking on all array/slice access
- NaN-safe float comparisons (no panic on edge cases)

### Authentication & Authorization
- ACL system with user permissions and key patterns
- Argon2id password hashing (memory-hard)
- **Constant-time password comparison** (prevents timing attacks)
- **AUTH rate limiting**: 10 failures → 60-second lockout
- Command-level access control with categories

### Memory Management
- **Maxmemory enforcement** with configurable limits
- **LRU eviction** with Redis-style 5-key sampling algorithm
- Volatile-TTL, volatile-random, allkeys-random policies supported
- Real-time memory metrics via INFO command

### Network Security
- TLS 1.3 via rustls (memory-safe)
- Rate limiting per IP
- Connection limits and buffer size controls

### Lua Sandboxing
- Restricted function set (no os, io, debug)
- Memory and instruction limits
- Execution timeouts
- Real SHA1 script identification (Redis-compatible)

See [SECURITY.md](docs/SECURITY.md) for detailed security documentation.

## Building

### Requirements

- Rust 1.85 or later
- Cargo

### Feature Flags

```bash
# Default features
cargo build --release

# With TLS support
cargo build --release --features tls

# With OpenTelemetry
cargo build --release --features telemetry

# Minimal build
cargo build --release --no-default-features
```

### Running Tests

```bash
# Run all tests
cargo test

# Run benchmarks
cargo bench

# Test with viator-cli (start server first)
./target/release/viator &
viator-cli PING
viator-cli SET foo bar
viator-cli GET foo
```

## CLI Tools

Viator includes a complete suite of command-line utilities:

### viator

The main server binary.

```bash
viator --port 6379 --bind 127.0.0.1
viator --help
```

### viator-cli

Interactive command-line client (viator-cli compatible).

```bash
# Connect to server
viator-cli -h 127.0.0.1 -p 6379

# Execute single command
viator-cli -h 127.0.0.1 -p 6379 SET foo bar

# With authentication
viator-cli -h 127.0.0.1 -p 6379 -a mypassword
```

### viator-benchmark

Performance benchmarking tool (viator-benchmark compatible).

```bash
# Run default benchmarks
viator-benchmark -h 127.0.0.1 -p 6379

# Custom benchmark
viator-benchmark -h 127.0.0.1 -p 6379 -c 50 -n 100000 -t set,get

# With pipelining
viator-benchmark -h 127.0.0.1 -p 6379 -P 16
```

### viator-check-vdb

VDB (snapshot) file integrity checker.

```bash
# Check VDB file integrity
viator-check-vdb dump.vdb

# Verbose output
viator-check-vdb -v dump.vdb
```

### viator-check-aof

AOF (append-only file) integrity checker and repair tool.

```bash
# Check AOF file
viator-check-aof appendonly.aof

# Attempt to fix corrupted AOF
viator-check-aof --fix appendonly.aof
```

### viator-sentinel

High availability monitoring daemon.

```bash
# Monitor a master
viator-sentinel --master mymaster --host 127.0.0.1 --port 6379 --quorum 2

# With down-after threshold
viator-sentinel --master mymaster --host 127.0.0.1 --port 6379 --quorum 2 --down-after 5000
```

## Compatibility

Viator targets Redis 8.4.0 protocol compatibility:

| Feature | Status | Details |
|---------|--------|---------|
| RESP3 Protocol | ✅ Full | All frame types, streaming |
| Core Commands | ✅ 200+ | String, List, Hash, Set, ZSet, Stream |
| VectorSet | ✅ Full | HNSW similarity search, persistence |
| Cluster Mode | ✅ Full | Gossip, failover, Redis 8.4 migration |
| Sentinel | ✅ Full | Monitoring, automatic failover |
| Lua Scripting | ✅ Full | Lua 5.4, real SHA1, sandboxed |
| Pub/Sub | ✅ Full | Channels, patterns, sharded |
| Transactions | ✅ Full | MULTI/EXEC, WATCH/UNWATCH |
| VDB Persistence | ✅ Full | CRC64 checksums, LZF decompression |
| AOF Persistence | ✅ Full | Atomic rewrite, fsync policies |
| Redis Stack | ✅ Full | JSON, TimeSeries, Bloom, Cuckoo, TopK, T-Digest, CMS |
| Security | ✅ Full | ACL, TLS 1.3, AUTH rate limiting |

## Project Structure

```
├── src/
│   ├── bin/            # CLI tool binaries
│   │   ├── viator-cli.rs        # Command-line client
│   │   ├── viator-benchmark.rs  # Performance benchmarking
│   │   ├── viator-check-vdb.rs  # VDB file checker
│   │   ├── viator-check-aof.rs  # AOF file checker
│   │   └── viator-sentinel.rs   # HA monitoring daemon
│   ├── commands/       # Command implementations (200+ commands)
│   ├── protocol/       # RESP3 parser and frame handling
│   ├── server/         # TCP server, connections, cluster, sentinel
│   ├── storage/        # Database, expiry management
│   ├── persistence/    # AOF and VDB persistence (CRC64, LZF)
│   ├── security/       # ACL, TLS, audit, rate limiting
│   ├── types/          # Data type implementations (VectorSet, etc.)
│   ├── pool.rs         # Buffer pooling
│   ├── error.rs        # Error types
│   ├── main.rs         # Server entry point (viator binary)
│   └── lib.rs          # Library root
├── docs/
│   ├── ARCHITECTURE.md # System design overview
│   ├── PERSISTENCE.md  # VDB/AOF documentation
│   ├── SECURITY.md     # Security features
│   └── VECTORSET.md    # Vector similarity search
├── fuzz/               # Fuzz testing targets
├── benches/            # Performance benchmarks
└── tests/              # Integration tests
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- [Redis Ltd.](https://redis.io/) for creating Redis and the RESP protocol specification. Redis is a registered trademark of Redis Ltd.
- The Rust community for excellent async and systems libraries
- tokio, dashmap, bytes, and other crate authors
