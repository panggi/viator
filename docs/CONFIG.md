# Viator Configuration Reference

Complete reference for all Viator server configuration options.

## Table of Contents

- [Command Line Options](#command-line-options)
- [Configuration File](#configuration-file)
- [Server Options](#server-options)
- [Security Options](#security-options)
- [Memory Management](#memory-management)
- [Persistence Options](#persistence-options)
- [Keyspace Notifications](#keyspace-notifications)
- [Redis 8.4+ Options](#redis-84-options)
- [Example Configuration](#example-configuration)

---

## Command Line Options

```bash
viator-server [OPTIONS]
```

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--config <FILE>` | `-c` | Load configuration from file | None |
| `--port <PORT>` | `-p` | Server port | 6379 |
| `--bind <ADDR>` | `-b` | Bind address | 127.0.0.1 |
| `--requirepass <PWD>` | `-a` | Server password | None |
| `--daemonize` | `-d` | Run as daemon | No |
| `--pidfile <FILE>` | | Write PID to file | None |
| `--logfile <FILE>` | | Write logs to file | stdout |
| `--loglevel <LEVEL>` | | Log level | notice |
| `--maxmemory <BYTES>` | | Max memory limit | 0 (unlimited) |
| `--databases <NUM>` | | Number of databases | 16 |
| `--appendonly` | | Enable AOF persistence | No |
| `--dir <DIR>` | | Working directory | . |
| `--help` | `-h` | Print help | |
| `--version` | `-v` | Print version | |

CLI options override config file settings.

---

## Configuration File

Viator uses Redis-compatible configuration file format:

```text
# Comment
directive value
directive "value with spaces"
```

Load with: `viator-server -c /path/to/viator.conf`

### Include Directive

Include other configuration files:

```text
include /path/to/common.conf
include /path/to/local.conf
```

---

## Server Options

### bind

Bind address for incoming connections.

| Attribute | Value |
|-----------|-------|
| **Type** | String |
| **Default** | `127.0.0.1` |
| **CLI** | `--bind`, `-b` |

```text
bind 0.0.0.0
bind 127.0.0.1
```

### port

TCP port for client connections.

| Attribute | Value |
|-----------|-------|
| **Type** | Integer (1-65535) |
| **Default** | `6379` |
| **CLI** | `--port`, `-p` |

```text
port 6379
port 6380
```

### daemonize

Run server as background daemon (Unix only).

| Attribute | Value |
|-----------|-------|
| **Type** | Boolean (yes/no) |
| **Default** | `no` |
| **CLI** | `--daemonize`, `-d` |

```text
daemonize yes
```

### pidfile

Path to write the server PID file.

| Attribute | Value |
|-----------|-------|
| **Type** | Path |
| **Default** | None |
| **CLI** | `--pidfile` |

```text
pidfile /var/run/viator.pid
```

### logfile

Path for log output. Empty for stdout.

| Attribute | Value |
|-----------|-------|
| **Type** | Path |
| **Default** | None (stdout) |
| **CLI** | `--logfile` |

```text
logfile /var/log/viator.log
logfile ""
```

### loglevel

Logging verbosity level.

| Attribute | Value |
|-----------|-------|
| **Type** | Enum |
| **Default** | `notice` |
| **CLI** | `--loglevel` |
| **Values** | `debug`, `verbose`, `notice`, `warning` |

```text
loglevel notice
loglevel debug
```

### databases

Number of databases (0-15 by default).

| Attribute | Value |
|-----------|-------|
| **Type** | Integer |
| **Default** | `16` |
| **CLI** | `--databases` |

```text
databases 16
```

### maxclients

Maximum simultaneous client connections.

| Attribute | Value |
|-----------|-------|
| **Type** | Integer |
| **Default** | `10000` |
| **CLI** | None |

```text
maxclients 10000
```

### timeout

Close idle client connections after N seconds. 0 to disable.

| Attribute | Value |
|-----------|-------|
| **Type** | Integer (seconds) |
| **Default** | `0` |
| **CLI** | None |

```text
timeout 300
timeout 0
```

### tcp-keepalive

TCP keepalive interval in seconds. 0 to disable.

| Attribute | Value |
|-----------|-------|
| **Type** | Integer (seconds) |
| **Default** | `300` |
| **CLI** | None |

```text
tcp-keepalive 300
```

### dir

Working directory for persistence files.

| Attribute | Value |
|-----------|-------|
| **Type** | Path |
| **Default** | `.` |
| **CLI** | `--dir` |

```text
dir /var/lib/viator
```

---

## Security Options

### requirepass

Password for client authentication.

| Attribute | Value |
|-----------|-------|
| **Type** | String |
| **Default** | None |
| **CLI** | `--requirepass`, `-a` |

```text
requirepass "your-strong-password"
```

Clients must use `AUTH password` before other commands.

---

## Memory Management

### maxmemory

Maximum memory usage in bytes. 0 for unlimited.

| Attribute | Value |
|-----------|-------|
| **Type** | Memory size |
| **Default** | `0` |
| **CLI** | `--maxmemory` |
| **Suffixes** | `k`, `kb`, `m`, `mb`, `g`, `gb` |

```text
maxmemory 1gb
maxmemory 512mb
maxmemory 104857600
```

### maxmemory-policy

Eviction policy when maxmemory is reached.

| Attribute | Value |
|-----------|-------|
| **Type** | Enum |
| **Default** | `noeviction` |
| **CLI** | None |

| Policy | Description |
|--------|-------------|
| `noeviction` | Return errors on write operations |
| `allkeys-lru` | Evict least recently used keys |
| `allkeys-lfu` | Evict least frequently used keys |
| `allkeys-random` | Evict random keys |
| `volatile-lru` | Evict LRU keys with TTL |
| `volatile-lfu` | Evict LFU keys with TTL |
| `volatile-random` | Evict random keys with TTL |
| `volatile-ttl` | Evict keys with shortest TTL |

```text
maxmemory-policy allkeys-lru
```

---

## Persistence Options

### VDB (Snapshot) Persistence

#### dbfilename

VDB snapshot filename.

| Attribute | Value |
|-----------|-------|
| **Type** | String |
| **Default** | `dump.vdb` |
| **CLI** | None |

```text
dbfilename dump.vdb
```

#### save

Trigger VDB save after N seconds if M keys changed. Multiple rules allowed.

| Attribute | Value |
|-----------|-------|
| **Type** | `save <seconds> <changes>` |
| **Default** | See below |
| **CLI** | None |

Default save rules:
```text
save 3600 1      # After 1 hour if at least 1 key changed
save 300 100     # After 5 minutes if at least 100 keys changed
save 60 10000    # After 1 minute if at least 10000 keys changed
```

Disable VDB persistence:
```text
save ""
```

### AOF (Append-Only File) Persistence

#### appendonly

Enable AOF persistence.

| Attribute | Value |
|-----------|-------|
| **Type** | Boolean (yes/no) |
| **Default** | `no` |
| **CLI** | `--appendonly` |

```text
appendonly yes
```

#### appendfilename

AOF filename.

| Attribute | Value |
|-----------|-------|
| **Type** | String |
| **Default** | `appendonly.aof` |
| **CLI** | None |

```text
appendfilename "appendonly.aof"
```

#### appendfsync

AOF fsync policy.

| Attribute | Value |
|-----------|-------|
| **Type** | Enum |
| **Default** | `everysec` |
| **CLI** | None |

| Policy | Description |
|--------|-------------|
| `always` | Fsync after every write (safest, slowest) |
| `everysec` | Fsync once per second (recommended) |
| `no` | Let OS handle fsync (fastest, least safe) |

```text
appendfsync everysec
```

---

## Keyspace Notifications

### notify-keyspace-events

Enable keyspace event notifications via Pub/Sub.

| Attribute | Value |
|-----------|-------|
| **Type** | String (flags) |
| **Default** | `""` (disabled) |
| **CLI** | None |

| Flag | Description |
|------|-------------|
| `K` | Keyspace events (`__keyspace@<db>__` prefix) |
| `E` | Keyevent events (`__keyevent@<db>__` prefix) |
| `g` | Generic commands (DEL, EXPIRE, RENAME, etc.) |
| `$` | String commands |
| `l` | List commands |
| `s` | Set commands |
| `h` | Hash commands |
| `z` | Sorted set commands |
| `x` | Expired events |
| `e` | Evicted events |
| `t` | Stream commands |
| `m` | Key-miss events |
| `n` | New key events |
| `O` | Overwritten events (Redis 8.2+) |
| `T` | Type changed events (Redis 8.2+) |
| `A` | Alias for all events |

```text
# Enable all keyspace and keyevent notifications
notify-keyspace-events KEA

# Enable only expired key notifications
notify-keyspace-events Ex
```

---

## Redis 8.4+ Options

### lookahead

Command prefetching depth for pipeline optimization.

| Attribute | Value |
|-----------|-------|
| **Type** | Integer |
| **Default** | `16` |
| **CLI** | None |

```text
lookahead 16
```

### aof-load-corrupt-tail-max-size

Maximum corrupted tail size for automatic AOF repair on load.

| Attribute | Value |
|-----------|-------|
| **Type** | Memory size |
| **Default** | `0` |
| **CLI** | None |

```text
aof-load-corrupt-tail-max-size 1mb
```

---

## Example Configuration

Complete example configuration file:

```text
# Viator Configuration File
# /etc/viator/viator.conf

################################ GENERAL ################################

# Network
bind 0.0.0.0
port 6379
timeout 0
tcp-keepalive 300

# Daemon
daemonize yes
pidfile /var/run/viator/viator.pid
logfile /var/log/viator/viator.log
loglevel notice

# Databases
databases 16

################################ SECURITY ###############################

requirepass "your-strong-password-here"

################################ LIMITS #################################

maxclients 10000
maxmemory 2gb
maxmemory-policy allkeys-lru

################################ PERSISTENCE ############################

# Working directory
dir /var/lib/viator

# VDB snapshots
dbfilename dump.vdb
save 900 1
save 300 10
save 60 10000

# AOF
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec

############################ NOTIFICATIONS ##############################

# Disable by default for performance
notify-keyspace-events ""

############################# REDIS 8.4+ ################################

lookahead 16
```

---

## Environment Variables

Viator respects the `RUST_LOG` environment variable for logging:

```bash
# Enable debug logging
RUST_LOG=debug viator-server

# Enable specific module logging
RUST_LOG=viator::commands=debug viator-server
```

---

## Runtime Configuration

Some settings can be modified at runtime via the `CONFIG SET` command:

```bash
# Change max memory
CONFIG SET maxmemory 4gb

# Change eviction policy
CONFIG SET maxmemory-policy volatile-lru

# View current configuration
CONFIG GET maxmemory
CONFIG GET *
```

Note: Not all settings support runtime modification. Server restart may be required for some changes.

---

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - System design overview
- [PERSISTENCE.md](PERSISTENCE.md) - Detailed persistence documentation
- [SECURITY.md](SECURITY.md) - Security features and best practices
