# Viator API Reference

Guide for connecting to Viator and using the RESP3 protocol.

## Table of Contents

- [Protocol Overview](#protocol-overview)
- [Connection](#connection)
- [Authentication](#authentication)
- [RESP3 Frame Types](#resp3-frame-types)
- [Command Execution](#command-execution)
- [Pipelining](#pipelining)
- [Pub/Sub](#pubsub)
- [Transactions](#transactions)
- [Client Libraries](#client-libraries)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

---

## Protocol Overview

Viator implements the Redis RESP3 (REdis Serialization Protocol version 3) protocol, providing full wire compatibility with Redis clients.

### Protocol Versions

| Version | Features | Default |
|---------|----------|---------|
| RESP2 | Basic types, backward compatible | Yes |
| RESP3 | Maps, sets, streaming, attributes | Via HELLO 3 |

### Switching to RESP3

```bash
# Negotiate RESP3 with authentication
HELLO 3 AUTH username password

# Negotiate RESP3 without auth
HELLO 3
```

---

## Connection

### Default Settings

| Setting | Value |
|---------|-------|
| Default Port | 6379 |
| Default Host | 127.0.0.1 |
| Protocol | TCP |
| TLS Port | 6380 (when enabled) |

### Connection Lifecycle

```
Client                          Server
  │                               │
  │──── TCP Connect ─────────────►│
  │                               │
  │◄─── Connection Accepted ──────│
  │                               │
  │──── HELLO 3 AUTH user pass ──►│
  │                               │
  │◄─── Server Info Response ─────│
  │                               │
  │──── Commands ────────────────►│
  │◄─── Responses ────────────────│
  │                               │
  │──── QUIT ────────────────────►│
  │◄─── +OK ──────────────────────│
  │                               │
  └─── TCP Close ─────────────────┘
```

### Connection Commands

| Command | Description |
|---------|-------------|
| `PING [message]` | Test connection, returns PONG or message |
| `ECHO message` | Returns the message |
| `QUIT` | Close connection gracefully |
| `RESET` | Reset connection state |
| `SELECT index` | Switch database (0-15) |

```bash
# Test connection
PING
# +PONG

# Echo test
ECHO "hello"
# $5
# hello

# Switch to database 1
SELECT 1
# +OK
```

---

## Authentication

### Simple Authentication

```bash
# Single password
AUTH password
```

### ACL Authentication (Username + Password)

```bash
# With username
AUTH username password

# Or via HELLO
HELLO 3 AUTH username password
```

### HELLO Command Response

```bash
HELLO 3
# Returns a map with server info:
# %7
# $6 server
# $6 viator
# $7 version
# $6 0.1.18
# $5 proto
# :3
# ...
```

### Client Identification

```bash
# Set client name
CLIENT SETNAME myapp-worker-1

# Get client name
CLIENT GETNAME

# Get client ID
CLIENT ID
```

---

## RESP3 Frame Types

### Simple Types

| Type | Prefix | Example | Description |
|------|--------|---------|-------------|
| Simple String | `+` | `+OK\r\n` | Status messages |
| Error | `-` | `-ERR message\r\n` | Error responses |
| Integer | `:` | `:42\r\n` | 64-bit signed integers |
| Bulk String | `$` | `$5\r\nhello\r\n` | Binary-safe strings |
| Null | `_` | `_\r\n` | Null value |
| Boolean | `#` | `#t\r\n` or `#f\r\n` | True/false |
| Double | `,` | `,3.14159\r\n` | Floating point |
| Big Number | `(` | `(12345678901234567890\r\n` | Arbitrary precision |

### Aggregate Types

| Type | Prefix | Example | Description |
|------|--------|---------|-------------|
| Array | `*` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` | Ordered list |
| Map | `%` | `%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n` | Key-value pairs |
| Set | `~` | `~2\r\n$3\r\none\r\n$3\r\ntwo\r\n` | Unordered unique |

### Special Types

| Type | Prefix | Description |
|------|--------|-------------|
| Verbatim String | `=` | String with encoding hint |
| Push | `>` | Server-initiated message |
| Attribute | `\|` | Metadata for next value |

### Example Responses

```bash
# Simple string
+OK

# Error
-ERR unknown command 'FOO'

# Integer
:42

# Bulk string
$5
hello

# Null
_

# Array
*3
$3
one
$3
two
$5
three

# Map (RESP3)
%2
$4
key1
$6
value1
$4
key2
$6
value2
```

---

## Command Execution

### Command Format

Commands are sent as RESP arrays:

```
*3\r\n
$3\r\n
SET\r\n
$5\r\n
mykey\r\n
$7\r\n
myvalue\r\n
```

### Inline Commands

Simple commands can be sent inline (for manual testing):

```
PING\r\n
GET mykey\r\n
```

### Command Categories

| Category | Examples |
|----------|----------|
| String | GET, SET, MGET, MSET, INCR, APPEND |
| List | LPUSH, RPUSH, LPOP, RPOP, LRANGE |
| Hash | HSET, HGET, HMGET, HGETALL |
| Set | SADD, SREM, SMEMBERS, SINTER |
| Sorted Set | ZADD, ZRANGE, ZSCORE, ZRANK |
| Stream | XADD, XREAD, XRANGE, XGROUP |
| Pub/Sub | SUBSCRIBE, PUBLISH, PSUBSCRIBE |
| Transaction | MULTI, EXEC, DISCARD, WATCH |
| Scripting | EVAL, EVALSHA, SCRIPT |
| Server | INFO, CONFIG, CLIENT, DEBUG |

See [COMPATIBILITY.md](COMPATIBILITY.md) for complete command reference.

---

## Pipelining

### Overview

Pipelining sends multiple commands without waiting for responses:

```
Client                          Server
  │                               │
  │──── SET key1 value1 ─────────►│
  │──── SET key2 value2 ─────────►│
  │──── GET key1 ────────────────►│
  │──── GET key2 ────────────────►│
  │                               │
  │◄─── +OK ──────────────────────│
  │◄─── +OK ──────────────────────│
  │◄─── $6 value1 ────────────────│
  │◄─── $6 value2 ────────────────│
```

### Benefits

- Reduced network round trips
- Higher throughput
- Lower latency for batch operations

### Example (Python)

```python
import redis

r = redis.Redis()
pipe = r.pipeline()

# Queue commands
pipe.set('key1', 'value1')
pipe.set('key2', 'value2')
pipe.get('key1')
pipe.get('key2')

# Execute all at once
results = pipe.execute()
# [True, True, b'value1', b'value2']
```

---

## Pub/Sub

### Subscribe Mode

When a client subscribes, it enters Pub/Sub mode:

```bash
# Subscribe to channels
SUBSCRIBE news sports

# Subscribe to patterns
PSUBSCRIBE user:*

# In Pub/Sub mode, only these commands work:
# SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT
```

### Publishing

```bash
# Publish to channel
PUBLISH news "Breaking news!"
# Returns number of subscribers that received the message
```

### Message Format

```
# Subscription confirmation
*3
$9
subscribe
$4
news
:1

# Received message
*3
$7
message
$4
news
$14
Breaking news!
```

### Sharded Pub/Sub (Redis 7.0+)

```bash
# Subscribe to sharded channel
SSUBSCRIBE {user:1000}.notifications

# Publish to sharded channel
SPUBLISH {user:1000}.notifications "New message"
```

---

## Transactions

### Basic Transaction

```bash
MULTI
SET key1 "value1"
SET key2 "value2"
INCR counter
EXEC
# Returns array of results
```

### With WATCH (Optimistic Locking)

```bash
WATCH mykey
val = GET mykey
MULTI
SET mykey (val + 1)
EXEC
# Returns nil if mykey was modified by another client
```

### Transaction Commands

| Command | Description |
|---------|-------------|
| `MULTI` | Start transaction |
| `EXEC` | Execute queued commands |
| `DISCARD` | Abort transaction |
| `WATCH key [key ...]` | Watch keys for changes |
| `UNWATCH` | Unwatch all keys |

---

## Client Libraries

### Recommended Libraries

| Language | Library | URL |
|----------|---------|-----|
| Python | redis-py | https://github.com/redis/redis-py |
| Node.js | ioredis | https://github.com/luin/ioredis |
| Java | Jedis | https://github.com/redis/jedis |
| Go | go-redis | https://github.com/go-redis/redis |
| Rust | redis-rs | https://github.com/redis-rs/redis-rs |
| C# | StackExchange.Redis | https://github.com/StackExchange/StackExchange.Redis |

### Connection Examples

**Python (redis-py)**
```python
import redis

# Simple connection
r = redis.Redis(host='localhost', port=6379, db=0)

# With connection pool
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)

# With authentication
r = redis.Redis(host='localhost', port=6379, password='secret')

# With RESP3
r = redis.Redis(host='localhost', port=6379, protocol=3)
```

**Node.js (ioredis)**
```javascript
const Redis = require('ioredis');

// Simple connection
const redis = new Redis();

// With options
const redis = new Redis({
  host: 'localhost',
  port: 6379,
  password: 'secret',
  db: 0
});

// Cluster mode
const cluster = new Redis.Cluster([
  { host: '127.0.0.1', port: 7001 },
  { host: '127.0.0.1', port: 7002 }
]);
```

**Go (go-redis)**
```go
import "github.com/go-redis/redis/v8"

// Simple connection
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
})

// Cluster mode
rdb := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{"127.0.0.1:7001", "127.0.0.1:7002"},
})
```

---

## Error Handling

### Error Response Format

```
-ERR <error message>\r\n
-WRONGTYPE Operation against a key holding the wrong kind of value\r\n
```

### Common Error Types

| Error | Description |
|-------|-------------|
| `ERR` | Generic error |
| `WRONGTYPE` | Operation on wrong data type |
| `NOAUTH` | Authentication required |
| `NOPERM` | Permission denied |
| `MOVED` | Cluster redirect (slot moved) |
| `ASK` | Cluster redirect (slot migrating) |
| `CLUSTERDOWN` | Cluster is unavailable |
| `BUSY` | Server is busy (script running) |
| `OOM` | Out of memory |

### Handling Errors

```python
import redis

r = redis.Redis()

try:
    r.get('mykey')
except redis.ConnectionError:
    # Connection failed
    pass
except redis.AuthenticationError:
    # Wrong password
    pass
except redis.ResponseError as e:
    # Command error
    print(f"Error: {e}")
```

### Cluster Redirects

```python
# MOVED error
# -MOVED 3999 127.0.0.1:7002

# ASK error (slot being migrated)
# -ASK 3999 127.0.0.1:7002

# Client should:
# 1. For MOVED: Update slot mapping, retry to new node
# 2. For ASK: Send ASKING, then retry to new node
```

---

## Best Practices

### Connection Management

1. **Use connection pools** for multi-threaded applications
2. **Set appropriate timeouts** for connect and read operations
3. **Handle reconnection** gracefully
4. **Close connections** when done

```python
# Connection pool example
pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    max_connections=50,
    socket_timeout=5.0,
    socket_connect_timeout=2.0
)
```

### Performance

1. **Use pipelining** for batch operations
2. **Use MGET/MSET** instead of multiple GET/SET
3. **Prefer hash tags** for related keys in cluster mode
4. **Use appropriate data structures** (Hash vs String for objects)

```python
# Bad: Multiple round trips
for key in keys:
    values.append(r.get(key))

# Good: Single round trip
values = r.mget(keys)
```

### Memory Efficiency

1. **Use short key names** in high-volume scenarios
2. **Set TTL** on temporary data
3. **Use appropriate data types** (Hashes save memory for small objects)
4. **Monitor memory** with INFO memory

### Security

1. **Always use authentication** in production
2. **Use TLS** for sensitive data
3. **Restrict bind address** (not 0.0.0.0)
4. **Use ACLs** for fine-grained access control

```bash
# Secure configuration
requirepass your-strong-password
bind 127.0.0.1
tls-port 6380
tls-cert-file /path/to/cert.pem
tls-key-file /path/to/key.pem
```

---

## See Also

- [COMPATIBILITY.md](COMPATIBILITY.md) - Complete command reference
- [CONFIG.md](CONFIG.md) - Configuration options
- [SECURITY.md](SECURITY.md) - Security features
- [CLUSTER.md](CLUSTER.md) - Cluster deployment
