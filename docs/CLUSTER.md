# Viator Cluster and Sentinel Guide

Complete guide for deploying Viator in distributed configurations with automatic failover.

## Table of Contents

- [Overview](#overview)
- [Cluster Mode](#cluster-mode)
  - [Cluster Architecture](#cluster-architecture)
  - [Setting Up a Cluster](#setting-up-a-cluster)
  - [Cluster Commands](#cluster-commands)
  - [Slot Management](#slot-management)
  - [Failover](#failover)
  - [Adding/Removing Nodes](#addingremoving-nodes)
- [Sentinel Mode](#sentinel-mode)
  - [Sentinel Architecture](#sentinel-architecture)
  - [Setting Up Sentinel](#setting-up-sentinel)
  - [Sentinel Commands](#sentinel-commands)
  - [Automatic Failover](#automatic-failover)
- [Client Connectivity](#client-connectivity)
- [Monitoring](#monitoring)
- [Best Practices](#best-practices)

---

## Overview

Viator supports two high-availability deployment modes:

| Mode | Use Case | Automatic Failover | Data Sharding |
|------|----------|-------------------|---------------|
| **Cluster** | Large datasets, horizontal scaling | Yes | Yes (16384 slots) |
| **Sentinel** | Smaller datasets, simple HA | Yes | No |

---

## Cluster Mode

### Cluster Architecture

Viator Cluster distributes data across multiple nodes using hash slots:

```
┌─────────────────────────────────────────────────────────────────┐
│                    16384 Hash Slots                              │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  Slots 0-5460   │ Slots 5461-10922│    Slots 10923-16383        │
│                 │                 │                             │
│   ┌─────────┐   │   ┌─────────┐   │      ┌─────────┐            │
│   │ Master1 │   │   │ Master2 │   │      │ Master3 │            │
│   └────┬────┘   │   └────┬────┘   │      └────┬────┘            │
│        │        │        │        │           │                 │
│   ┌────▼────┐   │   ┌────▼────┐   │      ┌────▼────┐            │
│   │Replica1a│   │   │Replica2a│   │      │Replica3a│            │
│   └─────────┘   │   └─────────┘   │      └─────────┘            │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

**Key Concepts:**
- **Hash Slots**: Keys are mapped to 16384 slots using CRC16
- **Hash Tags**: Use `{tag}` in key names to force co-location
- **MOVED/ASK**: Redirect responses guide clients to correct nodes

### Setting Up a Cluster

#### Step 1: Start Cluster Nodes

Start multiple Viator instances on different ports:

```bash
# Node 1 (Master)
viator-server --port 7001

# Node 2 (Master)
viator-server --port 7002

# Node 3 (Master)
viator-server --port 7003

# Node 4 (Replica for Node 1)
viator-server --port 7004

# Node 5 (Replica for Node 2)
viator-server --port 7005

# Node 6 (Replica for Node 3)
viator-server --port 7006
```

#### Step 2: Create the Cluster

Connect nodes using `CLUSTER MEET`:

```bash
# From Node 1, meet all other nodes
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7002
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7003
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7004
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7005
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7006
```

#### Step 3: Assign Slots

Distribute slots across master nodes:

```bash
# Assign slots 0-5460 to Node 1
viator-cli -p 7001 CLUSTER ADDSLOTSRANGE 0 5460

# Assign slots 5461-10922 to Node 2
viator-cli -p 7002 CLUSTER ADDSLOTSRANGE 5461 10922

# Assign slots 10923-16383 to Node 3
viator-cli -p 7003 CLUSTER ADDSLOTSRANGE 10923 16383
```

#### Step 4: Configure Replicas

Set up replicas to follow their masters:

```bash
# Get master node IDs
MASTER1_ID=$(viator-cli -p 7001 CLUSTER MYID)
MASTER2_ID=$(viator-cli -p 7002 CLUSTER MYID)
MASTER3_ID=$(viator-cli -p 7003 CLUSTER MYID)

# Configure replicas
viator-cli -p 7004 CLUSTER REPLICATE $MASTER1_ID
viator-cli -p 7005 CLUSTER REPLICATE $MASTER2_ID
viator-cli -p 7006 CLUSTER REPLICATE $MASTER3_ID
```

#### Step 5: Verify Cluster

```bash
# Check cluster state
viator-cli -p 7001 CLUSTER INFO

# View all nodes
viator-cli -p 7001 CLUSTER NODES
```

### Cluster Commands

#### Information Commands

| Command | Description |
|---------|-------------|
| `CLUSTER INFO` | Cluster state and statistics |
| `CLUSTER NODES` | List all nodes with their roles and slots |
| `CLUSTER SLOTS` | Slot-to-node mapping |
| `CLUSTER SHARDS` | Shard information (Redis 7.0+) |
| `CLUSTER MYID` | Current node's ID |
| `CLUSTER MYSHARDID` | Current shard ID |

```bash
# Get cluster information
CLUSTER INFO
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_slots_ok:16384
# cluster_known_nodes:6
# cluster_size:3

# Get nodes list
CLUSTER NODES
# <node-id> 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5460
# <node-id> 127.0.0.1:7002@17002 master - 0 0 2 connected 5461-10922
# ...
```

#### Slot Management Commands

| Command | Description |
|---------|-------------|
| `CLUSTER KEYSLOT key` | Get slot number for a key |
| `CLUSTER ADDSLOTS slot [slot ...]` | Assign slots to current node |
| `CLUSTER ADDSLOTSRANGE start end [start end ...]` | Assign slot ranges |
| `CLUSTER DELSLOTS slot [slot ...]` | Remove slot assignments |
| `CLUSTER DELSLOTSRANGE start end [start end ...]` | Remove slot ranges |
| `CLUSTER SETSLOT slot IMPORTING node-id` | Begin importing slot |
| `CLUSTER SETSLOT slot MIGRATING node-id` | Begin migrating slot |
| `CLUSTER SETSLOT slot STABLE` | Cancel migration |
| `CLUSTER SETSLOT slot NODE node-id` | Assign slot to node |
| `CLUSTER COUNTKEYSINSLOT slot` | Count keys in slot |
| `CLUSTER GETKEYSINSLOT slot count` | Get keys in slot |
| `CLUSTER FLUSHSLOTS` | Remove all slot assignments |

```bash
# Check which slot a key belongs to
CLUSTER KEYSLOT mykey
# (integer) 5397

# Check which slot a hash-tagged key belongs to
CLUSTER KEYSLOT {user:1000}.profile
# (integer) 7893
CLUSTER KEYSLOT {user:1000}.settings
# (integer) 7893  # Same slot!

# Count keys in a slot
CLUSTER COUNTKEYSINSLOT 5397
# (integer) 42
```

#### Node Management Commands

| Command | Description |
|---------|-------------|
| `CLUSTER MEET ip port` | Add node to cluster |
| `CLUSTER FORGET node-id` | Remove node from cluster |
| `CLUSTER REPLICATE node-id` | Make current node a replica |
| `CLUSTER FAILOVER [FORCE\|TAKEOVER]` | Trigger manual failover |
| `CLUSTER RESET [HARD\|SOFT]` | Reset cluster configuration |
| `CLUSTER SAVECONFIG` | Save cluster config to disk |

```bash
# Add a new node
CLUSTER MEET 192.168.1.100 7007

# Make this node a replica
CLUSTER REPLICATE abc123def456...

# Trigger failover (from replica)
CLUSTER FAILOVER
```

#### Redis 8.2+ Slot Statistics

```bash
# Get statistics for slot range
CLUSTER SLOT-STATS SLOTSRANGE 0 100

# Order by key count
CLUSTER SLOT-STATS ORDERBY key-count LIMIT 10 DESC
```

#### Redis 8.4+ Atomic Migration

```bash
# Start atomic migration
CLUSTER MIGRATION IMPORT 0-1000 NODES source-node-id

# Check migration status
CLUSTER MIGRATION STATUS

# Abort migration
CLUSTER MIGRATION ABORT
```

### Slot Management

#### Hash Slot Calculation

Keys are assigned to slots using CRC16:

```
slot = CRC16(key) mod 16384
```

#### Hash Tags for Co-location

Use `{tag}` syntax to ensure related keys are in the same slot:

```bash
# These keys will be in the same slot
SET {user:1000}.profile "John"
SET {user:1000}.settings "{...}"
SET {user:1000}.sessions "[...]"

# Multi-key operations work within the same slot
MGET {user:1000}.profile {user:1000}.settings
```

#### Migrating Slots

Manual slot migration between nodes:

```bash
# On destination node: prepare to import
CLUSTER SETSLOT 5000 IMPORTING <source-node-id>

# On source node: prepare to migrate
CLUSTER SETSLOT 5000 MIGRATING <dest-node-id>

# Get keys in slot
CLUSTER GETKEYSINSLOT 5000 100

# Migrate each key (using MIGRATE command)
MIGRATE dest-host dest-port "" 0 5000 KEYS key1 key2 key3

# Finalize on both nodes
CLUSTER SETSLOT 5000 NODE <dest-node-id>
```

### Failover

#### Automatic Failover

When a master fails:
1. Replicas detect master failure via heartbeat timeout
2. Replicas start election after random delay
3. Winning replica promotes itself to master
4. Cluster updates slot assignments

#### Manual Failover

Trigger controlled failover from a replica:

```bash
# Graceful failover (waits for sync)
CLUSTER FAILOVER

# Force failover (doesn't wait for master)
CLUSTER FAILOVER FORCE

# Takeover (no election, immediate)
CLUSTER FAILOVER TAKEOVER
```

### Adding/Removing Nodes

#### Adding a New Master

```bash
# Start new node
viator-server --port 7007

# Join cluster
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7007

# Reshard slots to new node
# (move some slots from existing masters)
```

#### Adding a New Replica

```bash
# Start new node
viator-server --port 7008

# Join cluster
viator-cli -p 7001 CLUSTER MEET 127.0.0.1 7008

# Make it a replica
viator-cli -p 7008 CLUSTER REPLICATE <master-node-id>
```

#### Removing a Node

```bash
# If master: migrate all slots first
# If replica: just forget

# From any other node
CLUSTER FORGET <node-id>
```

---

## Sentinel Mode

### Sentinel Architecture

Sentinel provides high availability without data sharding:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Sentinel Cluster                            │
│                                                                 │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                 │
│   │Sentinel 1│◄──►│Sentinel 2│◄──►│Sentinel 3│                 │
│   └────┬─────┘    └────┬─────┘    └────┬─────┘                 │
│        │               │               │                        │
│        └───────────────┼───────────────┘                        │
│                        │                                        │
│                   Monitoring                                    │
│                        │                                        │
│                        ▼                                        │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                     Master                               │  │
│   │                  (read/write)                            │  │
│   └──────────────────────┬──────────────────────────────────┘  │
│                          │                                      │
│            ┌─────────────┴─────────────┐                       │
│            ▼                           ▼                       │
│   ┌─────────────────┐         ┌─────────────────┐              │
│   │    Replica 1    │         │    Replica 2    │              │
│   │   (read-only)   │         │   (read-only)   │              │
│   └─────────────────┘         └─────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- Monitors master and replicas
- Automatic failover when master fails
- Quorum-based decision making
- Client notification of topology changes

### Setting Up Sentinel

#### Step 1: Start Master and Replicas

```bash
# Master
viator-server --port 6379

# Replica 1
viator-server --port 6380
viator-cli -p 6380 REPLICAOF 127.0.0.1 6379

# Replica 2
viator-server --port 6381
viator-cli -p 6381 REPLICAOF 127.0.0.1 6379
```

#### Step 2: Start Sentinel Instances

```bash
# Sentinel 1
viator-sentinel --master mymaster --host 127.0.0.1 --port 6379 --quorum 2 --sentinel-port 26379

# Sentinel 2
viator-sentinel --master mymaster --host 127.0.0.1 --port 6379 --quorum 2 --sentinel-port 26380

# Sentinel 3
viator-sentinel --master mymaster --host 127.0.0.1 --port 6379 --quorum 2 --sentinel-port 26381
```

#### Step 3: Verify Setup

```bash
# Check sentinel status
viator-cli -p 26379 SENTINEL MASTERS

# Get master address
viator-cli -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
```

### Sentinel Commands

#### Monitoring Commands

| Command | Description |
|---------|-------------|
| `SENTINEL MASTERS` | List all monitored masters |
| `SENTINEL MASTER name` | Get master details |
| `SENTINEL REPLICAS name` | List replicas of a master |
| `SENTINEL SENTINELS name` | List other sentinels |
| `SENTINEL GET-MASTER-ADDR-BY-NAME name` | Get current master address |
| `SENTINEL CKQUORUM name` | Check if quorum is reachable |
| `SENTINEL MYID` | Get sentinel's ID |

```bash
# List all monitored masters
SENTINEL MASTERS

# Get specific master info
SENTINEL MASTER mymaster

# Get replicas
SENTINEL REPLICAS mymaster

# Get master address (for clients)
SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
# 1) "127.0.0.1"
# 2) "6379"

# Check quorum
SENTINEL CKQUORUM mymaster
# OK 3 usable Sentinels. Quorum and failover authorization is possible.
```

#### Management Commands

| Command | Description |
|---------|-------------|
| `SENTINEL MONITOR name ip port quorum` | Start monitoring a master |
| `SENTINEL REMOVE name` | Stop monitoring a master |
| `SENTINEL SET name option value` | Change configuration |
| `SENTINEL RESET pattern` | Reset matching masters |
| `SENTINEL FAILOVER name` | Force failover |
| `SENTINEL FLUSHCONFIG` | Rewrite config file |

```bash
# Add a new master to monitor
SENTINEL MONITOR mymaster2 192.168.1.100 6379 2

# Change down-after setting
SENTINEL SET mymaster down-after-milliseconds 10000

# Force failover
SENTINEL FAILOVER mymaster

# Stop monitoring
SENTINEL REMOVE mymaster2
```

### Automatic Failover

#### Failover Process

1. **Subjective Down (SDOWN)**: A sentinel considers master down after `down-after-milliseconds`
2. **Objective Down (ODOWN)**: Quorum sentinels agree master is down
3. **Leader Election**: Sentinels elect a leader to perform failover
4. **Replica Selection**: Best replica is chosen based on:
   - Replica priority
   - Replication offset (most data)
   - Run ID (lexicographic)
5. **Promotion**: Selected replica becomes master
6. **Reconfiguration**: Other replicas follow new master

#### Failover Configuration

```bash
# Time to wait before considering master down
SENTINEL SET mymaster down-after-milliseconds 5000

# Parallel syncs during failover
SENTINEL SET mymaster parallel-syncs 1

# Failover timeout
SENTINEL SET mymaster failover-timeout 60000
```

---

## Client Connectivity

### Cluster-Aware Clients

Clients should handle:
- `MOVED` redirects (slot permanently moved)
- `ASK` redirects (slot being migrated)
- Connection pooling per node

```python
# Example with redis-py
from redis.cluster import RedisCluster

rc = RedisCluster(
    host="127.0.0.1",
    port=7001,
    decode_responses=True
)

rc.set("key", "value")  # Automatically routed
```

### Sentinel-Aware Clients

Clients should:
- Query sentinel for master address
- Subscribe to failover notifications
- Reconnect on failover

```python
# Example with redis-py
from redis.sentinel import Sentinel

sentinel = Sentinel([
    ('127.0.0.1', 26379),
    ('127.0.0.1', 26380),
    ('127.0.0.1', 26381)
])

master = sentinel.master_for('mymaster')
master.set('key', 'value')
```

---

## Monitoring

### Cluster Health

```bash
# Overall cluster state
CLUSTER INFO

# Key metrics to watch:
# - cluster_state: ok/fail
# - cluster_slots_assigned: should be 16384
# - cluster_slots_ok: should equal assigned
# - cluster_known_nodes: expected node count
```

### Sentinel Health

```bash
# Check all masters
SENTINEL MASTERS

# Verify quorum
SENTINEL CKQUORUM mymaster

# Key metrics:
# - flags: should not contain s_down or o_down
# - num-slaves: expected replica count
# - num-other-sentinels: expected sentinel count - 1
```

---

## Best Practices

### Cluster Deployment

1. **Minimum 3 masters** for fault tolerance
2. **At least 1 replica per master** for HA
3. **Spread nodes across availability zones**
4. **Use hash tags** for multi-key operations
5. **Monitor slot distribution** for balance

### Sentinel Deployment

1. **Minimum 3 sentinels** for quorum
2. **Run sentinels on separate hosts** from data nodes
3. **Set appropriate `down-after-milliseconds`** (5000-30000ms)
4. **Configure `parallel-syncs`** based on bandwidth
5. **Use DNS or service discovery** for client access

### General

1. **Enable persistence** on at least one replica
2. **Monitor replication lag** for data safety
3. **Test failover regularly** in non-production
4. **Keep Viator versions consistent** across nodes
5. **Use `READONLY`** for read-heavy workloads on replicas

---

## See Also

- [CONFIG.md](CONFIG.md) - Configuration reference
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design overview
- [COMPATIBILITY.md](COMPATIBILITY.md) - Command compatibility
