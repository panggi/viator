# Viator Persistence

> **Note:** Viator is Redis® 8.4.0 protocol-compatible. Redis is a registered trademark of Redis Ltd.

This document details Viator's persistence mechanisms for data durability.

## Overview

Viator supports two complementary persistence strategies:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **AOF** | Append-Only File logging | Maximum durability, command replay |
| **VDB** | Point-in-time snapshots | Fast restarts, backup/restore |

---

## Append-Only File (AOF)

AOF logs every write command in RESP format, enabling complete data reconstruction.

### Configuration

```bash
viator --appendonly yes --appendfsync everysec
```

| Option | Values | Description |
|--------|--------|-------------|
| `appendonly` | yes/no | Enable AOF persistence |
| `appendfsync` | always/everysec/no | Fsync policy |
| `aof-rewrite-incremental-fsync` | yes/no | Incremental fsync during rewrite |

### Fsync Policies

```rust
pub enum AofFsync {
    /// Fsync after every write command (safest, slowest)
    Always,

    /// Fsync once per second (good balance) - DEFAULT
    EverySec,

    /// Never explicitly fsync (fastest, least safe)
    No,
}
```

**Trade-offs:**

| Policy | Durability | Performance | Data Loss Risk |
|--------|------------|-------------|----------------|
| Always | Maximum | Lowest | None |
| EverySec | High | Good | Up to 1 second |
| No | Low | Highest | OS-dependent |

### AOF Format

Commands are stored in RESP format:

```
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nAlice\r\n
*4\r\n$4\r\nHSET\r\n$4\r\nuser\r\n$3\r\nage\r\n$2\r\n30\r\n
*2\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$1\r\n5\r\n
```

### AOF Rewrite

AOF files grow over time. Rewriting compacts them:

```bash
# Trigger rewrite
BGREWRITEAOF
```

**Atomic Rewrite Process:**

```
1. Create temp file: appendonly.aof.temp.<pid>
2. Write current database state
3. Fsync temp file
4. Atomic rename over original
5. Clean up on error
```

This ensures crash-safety: if rewrite fails, the original AOF remains intact.

### Non-Blocking Persistence

Persistence operations use `spawn_blocking` to avoid blocking the async runtime:

```rust
pub async fn save_vdb(db: &Arc<Database>, path: &Path) -> Result<()> {
    let db = db.clone();
    let path = path.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let saver = VdbSaver::new(&path)?;
        saver.save(&db)
    }).await?
}

pub async fn rewrite_aof(db: &Arc<Database>, path: &Path) -> Result<()> {
    let db = db.clone();
    let path = path.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let writer = AofWriter::new(&path)?;
        writer.rewrite_from_db(&db)
    }).await?
}
```

**Why spawn_blocking:**
- Prevents fsync/write calls from blocking the Tokio runtime
- Keeps client latency low even during persistence operations
- Allows concurrent request handling during saves

### VectorSet in AOF

VectorSets are persisted using VADD commands:

```
*8\r\n$4\r\nVADD\r\n$6\r\nmyindex\r\n$4\r\nFP32\r\n$6\r\nVALUES\r\n$1\r\n3\r\n...
```

---

## VDB Snapshots

VDB creates compact binary snapshots of the entire dataset.

### Configuration

```bash
viator --save "900 1" --save "300 10" --save "60 10000"
```

Format: `--save "<seconds> <changes>"` - Save if N changes in M seconds.

### VDB File Format

```
+----------+----------+--------+----------+--------+----------+
| "VIATR"  | VERSION  | AUX    | DATABASE | ...    | EOF+CRC  |
| (5 bytes)| (4 bytes)| FIELDS | SELECTOR |        | (9 bytes)|
+----------+----------+--------+----------+--------+----------+
```

#### Header

| Field | Size | Description |
|-------|------|-------------|
| Magic | 5 | "VIATR" |
| Version | 4 | "0009" (VDB version 9) |

#### Auxiliary Fields

| Code | Description |
|------|-------------|
| 0xFA | Auxiliary field (key-value metadata) |
| 0xFB | Resize DB (hash table size hints) |
| 0xFC | Expire time (milliseconds) |
| 0xFD | Expire time (seconds) |
| 0xFE | Database selector |
| 0xFF | EOF marker |

### Data Type Encoding

| Type | Code | Description |
|------|------|-------------|
| STRING | 0 | String value |
| LIST | 1 | List (quicklist) |
| SET | 2 | Set (hashtable or intset) |
| ZSET | 3 | Sorted set (skiplist) |
| HASH | 4 | Hash (hashtable or ziplist) |
| STREAM | 15 | Stream with consumer groups |
| VECTORSET | 20 | HNSW vector index |

### CRC64 Checksum

Viator uses CRC64-ECMA for file integrity:

```rust
const CRC64_POLY: u64 = 0xC96C5795D7870F42;

fn crc64_update(crc: u64, data: &[u8]) -> u64 {
    data.iter().fold(crc, |acc, &byte| {
        let idx = ((acc ^ byte as u64) & 0xFF) as usize;
        CRC64_TABLE[idx] ^ (acc >> 8)
    })
}
```

On load, the checksum is validated:

```rust
if computed_crc != stored_crc {
    return Err(StorageError::ChecksumMismatch {
        expected: stored_crc,
        actual: computed_crc,
    });
}
```

### LZF Compression

Large strings may be LZF-compressed:

```rust
fn lzf_decompress(compressed: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(expected_len);
    let mut pos = 0;

    while pos < compressed.len() {
        let ctrl = compressed[pos];
        pos += 1;

        if ctrl < 32 {
            // Literal run: copy ctrl+1 bytes
            let len = (ctrl as usize) + 1;
            output.extend_from_slice(&compressed[pos..pos + len]);
            pos += len;
        } else {
            // Back-reference
            let len = (ctrl >> 5) as usize + 2;
            let offset = (((ctrl & 0x1F) as usize) << 8) | compressed[pos] as usize;
            pos += 1;

            let start = output.len() - offset - 1;
            for i in 0..len {
                output.push(output[start + i]);
            }
        }
    }

    Ok(output)
}
```

### VectorSet Persistence

VectorSets (HNSW indexes) are fully persisted:

```
+--------+--------+------+---------+----------+------------+
| METRIC | DIM    | COUNT| ELEMENT | VECTOR   | ATTRIBUTES |
| (1 byte)| (u32) | (u32)| NAME    | (f32*dim)| (key-value)|
+--------+--------+------+---------+----------+------------+
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| Metric | u8 | 0=Cosine, 1=Euclidean, 2=Inner Product |
| Dimension | u32 | Vector dimensionality |
| Count | u32 | Number of elements |
| Elements | Array | Name, vector values, attributes |

---

## Commands

### AOF Commands

| Command | Description |
|---------|-------------|
| `BGREWRITEAOF` | Trigger background AOF rewrite |
| `CONFIG SET appendonly yes` | Enable AOF |
| `CONFIG SET appendfsync always` | Set fsync policy |

### VDB Commands

| Command | Description |
|---------|-------------|
| `BGSAVE` | Trigger background VDB save |
| `SAVE` | Synchronous VDB save (blocks) |
| `LASTSAVE` | Unix timestamp of last save |
| `DEBUG RELOAD` | Save and reload VDB |

### INFO Persistence

```
# Persistence
loading:0
vdb_changes_since_last_save:42
vdb_bgsave_in_progress:0
vdb_last_save_time:1705312800
vdb_last_bgsave_status:ok
aof_enabled:1
aof_rewrite_in_progress:0
aof_last_rewrite_time_sec:-1
aof_current_size:1048576
aof_base_size:524288
```

---

## Best Practices

### High Durability

```bash
viator --appendonly yes --appendfsync always
```

- Every write is fsynced
- Zero data loss on crash
- Higher latency

### Balanced (Recommended)

```bash
viator --appendonly yes --appendfsync everysec --save "900 1" --save "300 10"
```

- AOF with 1-second fsync window
- VDB snapshots as backup
- Good performance, minimal data loss

### High Performance

```bash
viator --appendonly no --save "3600 1"
```

- No AOF overhead
- Hourly VDB snapshots
- Risk: up to 1 hour data loss

---

## Recovery

### Startup Order

1. Check for AOF file if `appendonly=yes`
2. Load AOF if present, otherwise load VDB
3. Apply any incremental changes

### Corrupted Files

```bash
# Check AOF integrity
viator-check-aof --fix appendonly.aof

# Check VDB integrity
viator-check-vdb dump.vdb
```

### Manual Recovery

```bash
# Force VDB load
viator --appendonly no

# After startup, enable AOF
viator-cli CONFIG SET appendonly yes
viator-cli BGREWRITEAOF
```
