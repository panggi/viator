# Viator Redis Compatibility Specification

> **Note:** Viator is Redis® 8.4.0 protocol-compatible. Redis is a registered trademark of Redis Ltd. Viator is not affiliated with or endorsed by Redis Ltd.

> **Purpose**: Authoritative specification for Redis protocol compatibility. This document serves as the single source of truth for tracking feature parity with Redis releases.
>
> **Maintainer**: Update this document whenever Redis releases a new version or when implementing new features.
>
> **Verification**: Each feature should be verified against the official Redis documentation before marking as complete.

---

## Table of Contents

1. [How to Update This Document](#how-to-update-this-document)
2. [Redis 8.4.0 Specification](#redis-840-specification)
3. [Redis 8.2.0 Specification](#redis-820-specification)
4. [Redis 8.0.0 Specification](#redis-800-specification)
5. [Core Commands Reference](#core-commands-reference)
6. [Data Structure Commands](#data-structure-commands)
7. [Implementation Status Summary](#implementation-status-summary)
8. [Verification Checklist](#verification-checklist)
9. [Known Limitations](#known-limitations)

## How to Update This Document

When a new Redis version is released:

1. **Fetch Official Sources**:
   - Release notes: `https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/`
   - What's new: `https://redis.io/docs/latest/develop/whats-new/`
   - Command reference: `https://redis.io/docs/latest/commands/`
   - GitHub releases: `https://github.com/redis/redis/releases`

2. **Create New Version Section**:
   - Copy the template from an existing version section
   - Document ALL new commands with full syntax
   - Document ALL command enhancements (new options)
   - Document ALL configuration parameter changes
   - Document ALL behavioral changes
   - Document ALL breaking changes

3. **For Each Feature**:
   - Record the official syntax exactly as documented
   - List all options with types and default values
   - Note the return value format
   - Add example commands for testing
   - Link to official documentation

4. **Update Implementation**:
   - Implement the feature
   - Add tests
   - Update status in this document
   - Add to verification checklist

---

## Redis 8.4.0 Specification

> **Release Date**: November 2025
> **Official Docs**: https://redis.io/docs/latest/develop/whats-new/8-4/
> **Release Notes**: https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.4-release-notes/

### New Commands

#### CLUSTER MIGRATION

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/cluster-migration/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |
| **Complexity** | O(N) where N is the number of keys in the slot |

**Syntax**:
```
CLUSTER MIGRATION <slot> <destination-node-id> [TIMEOUT <ms>] [COPY | REPLACE]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| slot | integer | Yes | Slot number (0-16383) |
| destination-node-id | string | Yes | Target node ID |
| TIMEOUT ms | integer | No | Migration timeout in milliseconds |
| COPY | flag | No | Do not remove keys from source |
| REPLACE | flag | No | Replace existing keys at destination |

**Return Value**: Simple string "OK" or error

**Test Commands**:
```redis
CLUSTER MIGRATION 1234 abc123def456 TIMEOUT 5000
CLUSTER MIGRATION 1234 abc123def456 COPY
```

**Implementation File**: `src/commands/cluster.rs`

---

#### DELEX

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/delex/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |
| **Complexity** | O(1) |

**Syntax**:
```
DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| key | key | Yes | Key to delete |
| IFEQ value | bulk string | No | Delete only if current value equals |
| IFNE value | bulk string | No | Delete only if current value not equals |
| IFDEQ digest | string | No | Delete only if XXH3 digest equals |
| IFDNE digest | string | No | Delete only if XXH3 digest not equals |

**Return Value**:
- Integer 1 if deleted
- Integer 0 if not deleted (condition not met or key doesn't exist)

**Test Commands**:
```redis
SET mykey "hello"
DELEX mykey IFEQ "hello"    # Returns 1
SET mykey "world"
DELEX mykey IFNE "world"    # Returns 0
DELEX mykey IFNE "hello"    # Returns 1
```

**Implementation File**: `src/commands/strings.rs`

**Notes**:
- Digest is XXH3 64-bit hash, lowercase hex (16 chars)
- Conditions are mutually exclusive

---

#### DIGEST

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/digest/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |
| **Complexity** | O(N) where N is the size of the value |

**Syntax**:
```
DIGEST key
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| key | key | Yes | Key to compute digest for |

**Return Value**:
- Bulk string: 16-character lowercase hex XXH3 digest
- Null if key doesn't exist

**Test Commands**:
```redis
SET mykey "hello"
DIGEST mykey    # Returns XXH3 hash like "d4a1185d1fa49281"
DIGEST nonexistent    # Returns (nil)
```

**Implementation File**: `src/commands/strings.rs`

**Notes**:
- Uses XXH3 64-bit algorithm (NOT SHA256)
- Returns lowercase hexadecimal
- Used with IFEQ/IFDEQ options on SET/DELEX

---

#### MSETEX

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/msetex/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |
| **Complexity** | O(N) where N is the number of keys |

**Syntax**:
```
MSETEX numkeys key value [key value ...] [NX | XX] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| numkeys | integer | Yes | Number of key-value pairs |
| key value | pairs | Yes | Key-value pairs to set |
| NX | flag | No | Only set if none of the keys exist |
| XX | flag | No | Only set if all keys exist |
| EX seconds | integer | No | Expiration in seconds |
| PX milliseconds | integer | No | Expiration in milliseconds |
| EXAT timestamp | integer | No | Absolute expiration (Unix seconds) |
| PXAT timestamp | integer | No | Absolute expiration (Unix milliseconds) |
| KEEPTTL | flag | No | Retain existing TTL |

**Return Value**:
- Simple string "OK" on success
- Null if NX/XX condition not met

**Test Commands**:
```redis
MSETEX 2 key1 val1 key2 val2 EX 3600
MSETEX 2 key1 val1 key2 val2 NX
MSETEX 2 key1 val1 key2 val2 XX PX 60000
```

**Implementation File**: `src/commands/strings.rs`

---

#### FT.HYBRID

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/ft.hybrid/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |
| **Complexity** | O(N) |

**Syntax**:
```
FT.HYBRID index
    SEARCH query
    VSIM @field $param [KNN count K k [EF_RUNTIME ef]] [RANGE count RADIUS r]
    [COMBINE RRF count [CONSTANT c] [WINDOW w] | COMBINE LINEAR count [ALPHA a] [BETA b] [WINDOW w]]
    [FILTER expr]
    [LIMIT offset num]
    [SORTBY field [ASC|DESC]]
    [LOAD count field ...]
    [PARAMS nargs name value ...]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| index | string | Yes | Index name |
| SEARCH query | string | Yes | Text search query |
| VSIM @field $param | - | Yes | Vector field and parameter reference |
| KNN count K k | integers | No | K-nearest neighbors search |
| EF_RUNTIME ef | integer | No | HNSW exploration factor |
| RANGE count RADIUS r | - | No | Range-based search |
| COMBINE RRF count | integer | No | Reciprocal Rank Fusion |
| CONSTANT c | float | No | RRF constant (default: 60) |
| WINDOW w | integer | No | Result window size |
| COMBINE LINEAR count | integer | No | Linear combination |
| ALPHA a | float | No | Text score weight (default: 0.5) |
| BETA b | float | No | Vector score weight (default: 0.5) |
| FILTER expr | string | No | Post-combination filter |
| LIMIT offset num | integers | No | Pagination |
| SORTBY field [ASC\|DESC] | - | No | Sort results |
| LOAD count field ... | - | No | Load additional fields |
| PARAMS nargs name value ... | - | No | Query parameters |

**Return Value**: Array of results with scores

**Test Commands**:
```redis
FT.HYBRID myidx SEARCH "hello world" VSIM @vec $query KNN 10 K 100 COMBINE RRF 10 PARAMS 2 query "\x00\x00..."
FT.HYBRID myidx SEARCH "test" VSIM @embedding $v COMBINE LINEAR 10 ALPHA 0.7 BETA 0.3 LIMIT 0 10
```

**Implementation File**: `src/commands/search_cmds.rs`

**Notes**:
- RRF formula: `score = sum(1 / (constant + rank))`
- LINEAR formula: `score = alpha * text_score + beta * vector_score`
- FILTER is applied after COMBINE

---

### Command Enhancements

#### SET - New Conditional Options

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/set/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |

**New Options**:
| Option | Syntax | Description |
|--------|--------|-------------|
| IFEQ | `IFEQ value` | Set only if current value equals |
| IFNE | `IFNE value` | Set only if current value not equals |
| IFDEQ | `IFDEQ digest` | Set only if XXH3 digest equals |
| IFDNE | `IFDNE digest` | Set only if XXH3 digest not equals |

**Full Syntax**:
```
SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL] [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
```

**Test Commands**:
```redis
SET mykey "value1"
SET mykey "value2" IFEQ "value1"    # Succeeds
SET mykey "value3" IFNE "value2"    # Fails
SET mykey "value3" IFDEQ "abc123..."   # Check digest
```

**Implementation File**: `src/commands/strings.rs`

**Notes**:
- IF* options are mutually exclusive with NX/XX
- Digest is XXH3 64-bit, lowercase hex

---

#### XREADGROUP - CLAIM Option

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/xreadgroup/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |

**New Option**:
| Option | Syntax | Description |
|--------|--------|-------------|
| CLAIM | `CLAIM min-idle-time` | Claim pending entries older than min-idle-time |

**Full Syntax**:
```
XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] [CLAIM min-idle-time] STREAMS key [key ...] id [id ...]
```

**Test Commands**:
```redis
XREADGROUP GROUP mygroup myconsumer CLAIM 60000 STREAMS mystream >
XREADGROUP GROUP mygroup myconsumer COUNT 10 CLAIM 30000 STREAMS mystream 0
```

**Implementation File**: `src/commands/streams.rs`

**Notes**:
- CLAIM automatically reclaims idle pending entries
- min-idle-time is in milliseconds
- Combines XPENDING + XCLAIM + XREAD in one operation

---

#### CLUSTER SLOT-STATS

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/cluster-slot-stats/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.4.0 |

**Syntax**:
```
CLUSTER SLOT-STATS SLOTSRANGE start end [COUNT count] [ORDERBY metric [ASC|DESC]]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| SLOTSRANGE start end | integers | Yes | Slot range |
| COUNT count | integer | No | Limit results |
| ORDERBY metric | string | No | Sort by: key-count, cpu-usec, network-bytes-in, network-bytes-out |
| ASC\|DESC | flag | No | Sort order |

**Implementation File**: `src/commands/cluster.rs`

---

### Configuration Parameters

| Parameter | Type | Default | Description | Status |
|-----------|------|---------|-------------|--------|
| `lookahead` | integer | 16 | Command prefetching depth | ✅ |
| `aof-load-corrupt-tail-max-size` | integer | 0 | Max corrupted AOF tail (bytes) for auto-repair | ✅ |
| `decode_array_with_array_mt` | boolean | false | Lua JSON empty array handling | ✅ |
| `search-default-scorer` | enum | BM25STD | Default text/tag scorer (BM25STD, TFIDF, TFIDF_NORM) | ✅ |
| `search-on-oom` | enum | FAIL | OOM behavior (FAIL, CONTINUE) | ✅ |
| `search-io-threads` | integer | 1 | Cluster coordination threads | ✅ |

**Implementation File**: `src/server/config.rs`

---

## Redis 8.2.0 Specification

> **Release Date**: September 2025
> **Official Docs**: https://redis.io/docs/latest/develop/whats-new/8-2/
> **Release Notes**: https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/

### New Commands

#### XDELEX

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/xdelex/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.2.0 |

**Syntax**:
```
XDELEX key [COUNT count] [IDLE ms] [TIME unix-ms] [KEEPREF | DELREF] [ACKED] ID [ID ...]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| key | key | Yes | Stream key |
| COUNT count | integer | No | Max entries to delete |
| IDLE ms | integer | No | Only entries idle for at least ms |
| TIME unix-ms | integer | No | Delete entries before this time |
| KEEPREF | flag | No | Keep consumer group references |
| DELREF | flag | No | Delete consumer group references (default) |
| ACKED | flag | No | Only delete acknowledged entries |
| ID | string | Yes | Entry IDs to delete |

**Implementation File**: `src/commands/streams.rs`

---

#### XACKDEL

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/xackdel/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.2.0 |

**Syntax**:
```
XACKDEL key group [KEEPREF | DELREF] [ACKED] ID [ID ...]
```

**Arguments**:
| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| key | key | Yes | Stream key |
| group | string | Yes | Consumer group name |
| KEEPREF | flag | No | Keep consumer group references |
| DELREF | flag | No | Delete consumer group references (default) |
| ACKED | flag | No | Only delete already acknowledged entries |
| ID | string | Yes | Entry IDs to acknowledge and delete |

**Implementation File**: `src/commands/streams.rs`

---

### Command Enhancements

#### XADD/XTRIM - Consumer Group Reference Options

**New Options**:
| Option | Description |
|--------|-------------|
| KEEPREF | Keep consumer group references during trimming |
| DELREF | Delete consumer group references (default) |
| ACKED | Only trim acknowledged entries |

**Implementation File**: `src/commands/streams.rs`

---

#### BITOP - New Operations

| Operation | Syntax | Description | Status |
|-----------|--------|-------------|--------|
| DIFF | `BITOP DIFF destkey key1 key2` | Bitwise difference (key1 AND NOT key2) | ✅ |
| DIFF1 | `BITOP DIFF1 destkey key1 key2` | Single-bit difference | ✅ |
| ANDOR | `BITOP ANDOR destkey key1 key2 key3` | (key1 AND key2) OR key3 | ✅ |
| ONE | `BITOP ONE destkey key` | Single-bit operations | ✅ |

**Implementation File**: `src/commands/bitmap.rs`

---

#### VSIM - IN Operator for Filters

**New Filter Syntax**:
```
VSIM key ELE element FILTER "@attr IN {val1, val2, val3}"
VSIM key ELE element FILTER "@attr NOT IN {val1, val2}"
```

**Implementation File**: `src/commands/vectorset_cmds.rs`

---

### Keyspace Notifications

**New Event Flags**:
| Flag | Event | Description |
|------|-------|-------------|
| O | OVERWRITTEN | Key value completely replaced |
| T | TYPE_CHANGED | Key data type changed |

**Implementation File**: `src/server/config.rs`

---

## Redis 8.0.0 Specification

> **Release Date**: May 2025
> **Official Docs**: https://redis.io/docs/latest/develop/whats-new/8-0/

### New Commands

#### Hash Field Expiration Commands

| Command | Syntax | Status |
|---------|--------|--------|
| HGETEX | `HGETEX key field [EX s \| PX ms \| EXAT ts \| PXAT ts \| PERSIST]` | ✅ |
| HSETEX | `HSETEX key field value [EX s \| PX ms \| EXAT ts \| PXAT ts]` | ✅ |
| HGETDEL | `HGETDEL key field [field ...]` | ✅ |

**Implementation File**: `src/commands/hashes.rs`

---

### Vector Set Commands (Beta)

> **Note**: Vector Sets were introduced in Redis 8.0 as a beta feature.

| Command | Syntax | Status |
|---------|--------|--------|
| VADD | See [VADD Specification](#vadd-full-specification) | ✅ |
| VCARD | `VCARD key` | ✅ |
| VDIM | `VDIM key` | ✅ |
| VEMB | `VEMB key element [RAW]` | ✅ |
| VGETATTR | `VGETATTR key element [attr]` | ✅ |
| VINFO | `VINFO key` | ✅ |
| VISMEMBER | `VISMEMBER key element` | ✅ |
| VLINKS | `VLINKS key element [WITHSCORES]` | ✅ |
| VRANDMEMBER | `VRANDMEMBER key [count]` | ✅ |
| VRANGE | `VRANGE key start end [count]` | ✅ |
| VREM | `VREM key element [element ...]` | ✅ |
| VSETATTR | `VSETATTR key element json-object` | ✅ |
| VSIM | See [VSIM Specification](#vsim-full-specification) | ✅ |

---

#### VADD Full Specification

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/vadd/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.0.0 |
| **Complexity** | O(log N) per element |

**Syntax**:
```
VADD key [REDUCE dim] (FP32 | VALUES num) vector element
    [CAS] [NOQUANT | Q8 | BIN] [EF build-exploration-factor]
    [SETATTR json-object] [M numlinks] [NOCREATE]
```

**Arguments**:
| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| key | key | Yes | - | Vector set key |
| REDUCE dim | integer | No | - | Reduce dimensionality via random projection |
| FP32 | flag | No* | - | Vector is binary FP32 blob (little-endian) |
| VALUES num | integer | No* | - | Next num args are float values |
| vector | blob/floats | Yes | - | The vector data |
| element | string | Yes | - | Element name |
| CAS | flag | No | false | Compare-and-swap (don't overwrite) |
| NOQUANT | flag | No | - | No quantization (full FP32) |
| Q8 | flag | No | default | 8-bit signed quantization |
| BIN | flag | No | - | Binary quantization |
| EF | integer | No | 200 | Build exploration factor |
| M | integer | No | 16 | Max HNSW connections (layer 0 = M*2) |
| SETATTR | json | No | - | JSON attributes to set |
| NOCREATE | flag | No | false | Fail if key doesn't exist |

*Either FP32 or VALUES must be specified

**Return Value**: Integer (1 if added, 0 if updated)

**Test Commands**:
```redis
VADD myvec VALUES 3 0.1 0.2 0.3 elem1
VADD myvec FP32 "\x00\x00\x80?\x00\x00\x00@\x00\x00@@" elem2
VADD myvec VALUES 3 0.1 0.2 0.3 elem3 CAS
VADD myvec VALUES 3 0.1 0.2 0.3 elem4 Q8 EF 400 M 32
VADD myvec VALUES 3 0.1 0.2 0.3 elem5 SETATTR '{"color":"red"}'
```

**Implementation File**: `src/commands/vectorset_cmds.rs`

---

#### VSIM Full Specification

| Attribute | Value |
|-----------|-------|
| **Official Doc** | https://redis.io/docs/latest/commands/vsim/ |
| **Status** | ✅ Implemented |
| **Since** | Redis 8.0.0 |
| **Complexity** | O(log N) |

**Syntax**:
```
VSIM key (ELE | FP32 | VALUES num) (element | vector)
    [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta]
    [EF search-exploration-factor] [FILTER expression] [FILTER-EF max-effort]
    [TRUTH] [NOTHREAD]
```

**Arguments**:
| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| key | key | Yes | - | Vector set key |
| ELE | flag | No* | - | Query by existing element name |
| FP32 | flag | No* | - | Query by binary FP32 vector |
| VALUES num | integer | No* | - | Query by num float values |
| element/vector | - | Yes | - | Query data |
| WITHSCORES | flag | No | false | Include similarity scores |
| WITHATTRIBS | flag | No | false | Include JSON attributes |
| COUNT num | integer | No | 10 | Max results |
| EPSILON delta | float | No | - | Similarity threshold (0-1) |
| EF | integer | No | 200 | Search exploration factor |
| FILTER expr | string | No | - | Filter expression |
| FILTER-EF | integer | No | - | Max filtering attempts |
| TRUTH | flag | No | false | Exact linear scan |
| NOTHREAD | flag | No | false | Run in main thread |

*Exactly one of ELE, FP32, or VALUES must be specified

**Filter Expression Syntax**:
```
@attr == "value"
@attr != "value"
@attr IN {val1, val2, val3}
@attr NOT IN {val1, val2}
```

**Return Value**: Array of elements (with optional scores and attributes)

**Test Commands**:
```redis
VSIM myvec ELE elem1 COUNT 5
VSIM myvec ELE elem1 WITHSCORES WITHATTRIBS COUNT 10
VSIM myvec VALUES 3 0.1 0.2 0.3 WITHSCORES
VSIM myvec ELE elem1 FILTER "@color == \"red\""
VSIM myvec ELE elem1 FILTER "@category IN {sports, music}"
VSIM myvec ELE elem1 EPSILON 0.2 COUNT 100
```

**Implementation File**: `src/commands/vectorset_cmds.rs`

---

## Core Commands Reference

### String Commands

| Command | Status | Options/Notes |
|---------|--------|---------------|
| APPEND | ✅ | - |
| DECR | ✅ | - |
| DECRBY | ✅ | - |
| DELEX | ✅ | IFEQ/IFNE/IFDEQ/IFDNE (8.4) |
| DIGEST | ✅ | XXH3 algorithm (8.4) |
| GET | ✅ | - |
| GETDEL | ✅ | - |
| GETEX | ✅ | EX/PX/EXAT/PXAT/PERSIST |
| GETRANGE | ✅ | - |
| GETSET | ✅ | Deprecated, use SET GET |
| INCR | ✅ | - |
| INCRBY | ✅ | - |
| INCRBYFLOAT | ✅ | - |
| LCS | ✅ | LEN/IDX/MINMATCHLEN/WITHMATCHLEN |
| MGET | ✅ | - |
| MSET | ✅ | - |
| MSETEX | ✅ | NX/XX + expiry options (8.4) |
| MSETNX | ✅ | - |
| PSETEX | ✅ | - |
| SET | ✅ | NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL/IFEQ/IFNE/IFDEQ/IFDNE |
| SETEX | ✅ | - |
| SETNX | ✅ | - |
| SETRANGE | ✅ | - |
| STRLEN | ✅ | - |
| SUBSTR | ✅ | Deprecated, use GETRANGE |

**Total: 25/25 (100%)**

---

### Hash Commands

| Command | Status | Options/Notes |
|---------|--------|---------------|
| HDEL | ✅ | - |
| HEXISTS | ✅ | - |
| HEXPIRE | ✅ | NX/XX/GT/LT + FIELDS (7.4+) |
| HEXPIREAT | ✅ | NX/XX/GT/LT + FIELDS |
| HEXPIRETIME | ✅ | FIELDS |
| HGET | ✅ | - |
| HGETALL | ✅ | - |
| HGETDEL | ✅ | (8.0) |
| HGETEX | ✅ | EX/PX/EXAT/PXAT/PERSIST (8.0) |
| HINCRBY | ✅ | - |
| HINCRBYFLOAT | ✅ | - |
| HKEYS | ✅ | - |
| HLEN | ✅ | - |
| HMGET | ✅ | - |
| HMSET | ✅ | Deprecated, use HSET |
| HPERSIST | ✅ | FIELDS |
| HPEXPIRE | ✅ | NX/XX/GT/LT + FIELDS |
| HPEXPIREAT | ✅ | NX/XX/GT/LT + FIELDS |
| HPEXPIRETIME | ✅ | FIELDS |
| HPTTL | ✅ | FIELDS |
| HRANDFIELD | ✅ | COUNT/WITHVALUES |
| HSCAN | ✅ | MATCH/COUNT/NOVALUES |
| HSET | ✅ | - |
| HSETEX | ✅ | EX/PX/EXAT/PXAT (8.0) |
| HSETNX | ✅ | - |
| HSTRLEN | ✅ | - |
| HTTL | ✅ | FIELDS |
| HVALS | ✅ | - |

**Total: 28/28 (100%)**

---

### Stream Commands

| Command | Status | Options/Notes |
|---------|--------|---------------|
| XACK | ✅ | - |
| XADD | ✅ | NOMKSTREAM/MAXLEN/MINID/LIMIT/KEEPREF/DELREF/ACKED (8.2) |
| XACKDEL | ✅ | KEEPREF/DELREF/ACKED (8.2) |
| XAUTOCLAIM | ✅ | JUSTID/COUNT |
| XCLAIM | ✅ | IDLE/TIME/RETRYCOUNT/FORCE/JUSTID/LASTID |
| XDEL | ✅ | - |
| XDELEX | ✅ | COUNT/IDLE/TIME/KEEPREF/DELREF/ACKED (8.2) |
| XGROUP | ✅ | CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER |
| XINFO | ✅ | CONSUMERS/GROUPS/STREAM/HELP |
| XLEN | ✅ | - |
| XPENDING | ✅ | IDLE/consumer filters |
| XRANGE | ✅ | COUNT |
| XREAD | ✅ | COUNT/BLOCK |
| XREADGROUP | ✅ | GROUP/COUNT/BLOCK/NOACK/CLAIM (8.4) |
| XREVRANGE | ✅ | COUNT |
| XSETID | ✅ | ENTRIESADDED/MAXDELETEDID |
| XTRIM | ✅ | MAXLEN/MINID/LIMIT/KEEPREF/DELREF/ACKED (8.2) |

**Total: 17/17 (100%)**

---

## Data Structure Commands

### Probabilistic Data Structures

#### Bloom Filter (8 commands)
| Command | Status |
|---------|--------|
| BF.ADD | ✅ |
| BF.CARD | ✅ |
| BF.EXISTS | ✅ |
| BF.INFO | ✅ |
| BF.INSERT | ✅ |
| BF.MADD | ✅ |
| BF.MEXISTS | ✅ |
| BF.RESERVE | ✅ |

#### Cuckoo Filter (10 commands)
| Command | Status |
|---------|--------|
| CF.ADD | ✅ |
| CF.ADDNX | ✅ |
| CF.COUNT | ✅ |
| CF.DEL | ✅ |
| CF.EXISTS | ✅ |
| CF.INFO | ✅ |
| CF.INSERT | ✅ |
| CF.INSERTNX | ✅ |
| CF.MEXISTS | ✅ |
| CF.RESERVE | ✅ |

#### Count-Min Sketch (6 commands)
| Command | Status |
|---------|--------|
| CMS.INCRBY | ✅ |
| CMS.INFO | ✅ |
| CMS.INITBYDIM | ✅ |
| CMS.INITBYPROB | ✅ |
| CMS.MERGE | ✅ |
| CMS.QUERY | ✅ |

#### Top-K (7 commands)
| Command | Status |
|---------|--------|
| TOPK.ADD | ✅ |
| TOPK.COUNT | ✅ |
| TOPK.INCRBY | ✅ |
| TOPK.INFO | ✅ |
| TOPK.LIST | ✅ |
| TOPK.QUERY | ✅ |
| TOPK.RESERVE | ✅ |

#### T-Digest (12 commands)
| Command | Status |
|---------|--------|
| TDIGEST.ADD | ✅ |
| TDIGEST.CDF | ✅ |
| TDIGEST.CREATE | ✅ |
| TDIGEST.INFO | ✅ |
| TDIGEST.MAX | ✅ |
| TDIGEST.MERGE | ✅ |
| TDIGEST.MIN | ✅ |
| TDIGEST.QUANTILE | ✅ |
| TDIGEST.RANK | ✅ |
| TDIGEST.RESET | ✅ |
| TDIGEST.REVRANK | ✅ |
| TDIGEST.TRIMMED_MEAN | ✅ |

---

### Search Commands (10 commands)

| Command | Status | Notes |
|---------|--------|-------|
| FT.CREATE | ✅ | Full schema support |
| FT.SEARCH | ✅ | Full query syntax |
| FT.AGGREGATE | ✅ | Grouping, reducers |
| FT.INFO | ✅ | - |
| FT.DROPINDEX | ✅ | DD option |
| FT.ALTER | ✅ | SCHEMA ADD |
| FT.ALIASADD | ✅ | - |
| FT.ALIASDEL | ✅ | - |
| FT.ALIASUPDATE | ✅ | - |
| FT.HYBRID | ✅ | RRF/LINEAR (8.4) |

---

### TimeSeries Commands (12 commands)

| Command | Status |
|---------|--------|
| TS.ADD | ✅ |
| TS.ALTER | ✅ |
| TS.CREATE | ✅ |
| TS.CREATERULE | ✅ |
| TS.DECRBY | ✅ |
| TS.DEL | ✅ |
| TS.DELETERULE | ✅ |
| TS.GET | ✅ |
| TS.INCRBY | ✅ |
| TS.INFO | ✅ |
| TS.MADD | ✅ |
| TS.RANGE | ✅ |

---

### JSON Commands (20 commands)

| Command | Status |
|---------|--------|
| JSON.ARRAPPEND | ✅ |
| JSON.ARRINDEX | ✅ |
| JSON.ARRINSERT | ✅ |
| JSON.ARRLEN | ✅ |
| JSON.ARRPOP | ✅ |
| JSON.ARRTRIM | ✅ |
| JSON.CLEAR | ✅ |
| JSON.DEBUG | ✅ |
| JSON.DEL | ✅ |
| JSON.GET | ✅ |
| JSON.MGET | ✅ |
| JSON.NUMINCRBY | ✅ |
| JSON.OBJKEYS | ✅ |
| JSON.OBJLEN | ✅ |
| JSON.RESP | ✅ |
| JSON.SET | ✅ |
| JSON.STRAPPEND | ✅ |
| JSON.STRLEN | ✅ |
| JSON.TOGGLE | ✅ |
| JSON.TYPE | ✅ |

---

### Graph Commands (8 commands)

| Command | Status |
|---------|--------|
| GRAPH.CONFIG | ✅ |
| GRAPH.DELETE | ✅ |
| GRAPH.EXPLAIN | ✅ |
| GRAPH.LIST | ✅ |
| GRAPH.PROFILE | ✅ |
| GRAPH.QUERY | ✅ |
| GRAPH.RO_QUERY | ✅ |
| GRAPH.SLOWLOG | ✅ |

---

## Implementation Status Summary

| Category | Implemented | Total | Coverage |
|----------|-------------|-------|----------|
| Strings | 25 | 25 | 100% |
| Hashes | 28 | 28 | 100% |
| Lists | 22 | 22 | 100% |
| Sets | 17 | 17 | 100% |
| Sorted Sets | 35 | 35 | 100% |
| Streams | 17 | 17 | 100% |
| Bitmap | 7 | 7 | 100% |
| HyperLogLog | 4 | 4 | 100% |
| Geo | 10 | 10 | 100% |
| Pub/Sub | 9 | 9 | 100% |
| Transactions | 5 | 5 | 100% |
| Scripting | 8 | 8 | 100% |
| Connection | 8 | 8 | 100% |
| Generic | 29 | 29 | 100% |
| Server | 26 | 26 | 100% |
| Cluster | 6 | 6 | 100% |
| Vector Set | 13 | 13 | 100% |
| Search | 10 | 10 | 100% |
| Bloom Filter | 8 | 8 | 100% |
| Cuckoo Filter | 10 | 10 | 100% |
| Count-Min Sketch | 6 | 6 | 100% |
| Top-K | 7 | 7 | 100% |
| T-Digest | 12 | 12 | 100% |
| TimeSeries | 12 | 12 | 100% |
| Graph | 8 | 8 | 100% |
| JSON | 20 | 20 | 100% |
| **TOTAL** | **353** | **353** | **100%** |

---

## Verification Checklist

Use this checklist when verifying compatibility with a new Redis version:

### Pre-Implementation

- [ ] Downloaded and reviewed official release notes
- [ ] Fetched command documentation for each new/changed command
- [ ] Created tracking entries in this document
- [ ] Identified breaking changes

### Per-Command Verification

- [ ] Syntax matches official documentation exactly
- [ ] All options are implemented
- [ ] Default values match
- [ ] Return value format matches
- [ ] Error conditions match
- [ ] Test commands execute correctly
- [ ] Edge cases handled

### Post-Implementation

- [ ] All unit tests pass
- [ ] Integration tests with real Redis pass (if available)
- [ ] This document is updated
- [ ] CHANGELOG is updated

---

## Known Limitations

### Stubs (Partial Implementation)

These commands return appropriate responses but don't fully implement the feature:

| Command | Limitation |
|---------|------------|
| ACL | Basic user info only; rules not enforced |
| CLUSTER | Returns standalone mode responses |
| FAILOVER | Acknowledged but no-op |
| LATENCY | Returns empty data |
| MODULE | Returns empty list |
| PSYNC/REPLCONF | Basic handshake only |
| REPLICAOF/SLAVEOF | Acknowledged but no replication |

### Performance Differences

- HNSW index is simulated via brute-force for small sets
- Some SIMD optimizations not implemented

### Known Incompatibilities

- None currently documented

---

## Version History

| Version | Date | Redis Target | Changes |
|---------|------|--------------|---------|
| 0.1.18 | 2026-01 | 8.4.0 | Consumer group PEL, CLIENT TRACKING, DUMP/RESTORE, MEMORY USAGE |
| 0.1.17 | 2026-01 | 8.4.0 | Fix VDB loading on startup |
| 0.1.16 | 2026-01 | 8.4.0 | Docker fixes, code hardening |
| 0.1.6 | 2026-01 | 8.4.0 | LZF compression for VDB persistence |
| 0.1.5 | 2026-01 | 8.4.0 | Fix binary smoke tests for llvm-cov |
| 0.1.4 | 2026-01 | 8.4.0 | Fix clippy warnings |
| 0.1.3 | 2026-01 | 8.4.0 | Code formatting fix |
| 0.1.2 | 2026-01 | 8.4.0 | Stream persistence (VDB/AOF), real CPU metrics |
| 0.1.1 | 2026-01 | 8.4.0 | Production hardening, panic-safe locks, backpressure |
| 0.1.0 | 2026-01 | 8.4.0 | Full 8.4 compatibility, VectorSet, CRC64/LZF persistence, AUTH rate limiting |

---

## Future Redis Versions

When Redis 8.6 or later is released, add a new section following this template:

```markdown
## Redis X.Y.Z Specification

> **Release Date**: [DATE]
> **Official Docs**: [URL]
> **Release Notes**: [URL]

### New Commands

#### COMMAND_NAME

| Attribute | Value |
|-----------|-------|
| **Official Doc** | [URL] |
| **Status** | ❌ Not Implemented |
| **Since** | Redis X.Y.Z |
| **Complexity** | O(?) |

**Syntax**:
```
COMMAND_NAME arg1 arg2 [OPTION value]
```

**Arguments**:
| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| arg1 | type | Yes | - | Description |

**Return Value**: [Description]

**Test Commands**:
```redis
COMMAND_NAME example
```

**Implementation File**: `src/commands/xxx.rs`

---

### Command Enhancements

[Document any changed commands]

### Configuration Parameters

[Document any new config options]

### Breaking Changes

[Document any breaking changes]
```
