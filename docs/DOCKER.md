# Viator Docker Deployment Guide

Complete guide for running Viator in Docker containers.

## Table of Contents

- [Quick Start](#quick-start)
- [Docker Images](#docker-images)
- [Docker Compose](#docker-compose)
- [Configuration](#configuration)
- [Persistence](#persistence)
- [Networking](#networking)
- [Cluster Deployment](#cluster-deployment)
- [Production Considerations](#production-considerations)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### Using Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/panggi/viator.git
cd viator

# Start Viator
docker-compose up -d

# Connect with CLI
docker exec -it viator viator-cli
```

### Using Docker Directly

```bash
# Build the image
docker build -t viator:latest .

# Run a container
docker run -d \
  --name viator \
  -p 6379:6379 \
  -v viator-data:/data \
  viator:latest

# Connect
docker exec -it viator viator-cli
```

---

## Docker Images

### Building the Image

The included Dockerfile uses a multi-stage build for minimal image size:

```bash
# Build with default settings
docker build -t viator:latest .

# Build with specific Rust version
docker build --build-arg RUST_VERSION=1.85 -t viator:latest .
```

### Image Structure

| Layer | Size | Contents |
|-------|------|----------|
| Base | ~80MB | Debian Bookworm slim |
| Runtime | ~5MB | CA certs, netcat |
| Binaries | ~15MB | viator-server, viator-cli, tools |
| **Total** | **~100MB** | |

### Included Binaries

- `viator-server` - Main server
- `viator-cli` - Command-line client
- `viator-benchmark` - Performance testing
- `viator-check-vdb` - VDB integrity checker
- `viator-check-aof` - AOF integrity checker

---

## Docker Compose

### Basic Setup

```yaml
# docker-compose.yml
services:
  viator:
    build: .
    image: viator:latest
    container_name: viator
    ports:
      - "6379:6379"
    volumes:
      - viator-data:/data
      - ./viator.conf:/etc/viator/viator.conf:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "echo 'PING' | nc -q1 localhost 6379 | grep -q PONG"]
      interval: 30s
      timeout: 3s
      retries: 3

volumes:
  viator-data:
```

### With Persistence

```bash
# Start with AOF persistence enabled
docker-compose --profile persistent up -d
```

This uses the `viator-persistent` service with AOF enabled.

### Cluster Mode

```bash
# Start a 3-node cluster
docker-compose --profile cluster up -d
```

This starts:
- `viator-node-1` on port 7001
- `viator-node-2` on port 7002
- `viator-node-3` on port 7003

---

## Configuration

### Using a Config File

Mount your configuration file:

```yaml
services:
  viator:
    volumes:
      - ./my-viator.conf:/etc/viator/viator.conf:ro
```

### Using Command Line Arguments

Override the default command:

```yaml
services:
  viator:
    command: ["--port", "6380", "--appendonly", "--maxmemory", "512mb"]
```

### Environment Variables

Set `RUST_LOG` for logging:

```yaml
services:
  viator:
    environment:
      - RUST_LOG=info
```

### Common Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | 6379 | Server port |
| `--bind` | 127.0.0.1 | Bind address (use 0.0.0.0 in Docker) |
| `--appendonly` | false | Enable AOF persistence |
| `--maxmemory` | 0 | Memory limit (0 = unlimited) |
| `--requirepass` | none | Set password |
| `--dir` | /data | Data directory |

---

## Persistence

### Data Volume

Always use a named volume or bind mount for `/data`:

```yaml
volumes:
  viator-data:  # Named volume (recommended)

# Or bind mount
volumes:
  - /path/on/host:/data
```

### VDB Snapshots

VDB files are saved to `/data/dump.vdb` by default.

```yaml
services:
  viator:
    volumes:
      - viator-data:/data
    command: ["--config", "/etc/viator/viator.conf"]
```

Config file settings for VDB:
```text
dir /data
dbfilename dump.vdb
save 900 1
save 300 10
save 60 10000
```

### AOF Persistence

Enable AOF for durability:

```yaml
services:
  viator:
    command: ["--appendonly", "--dir", "/data"]
```

Or in config file:
```text
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
```

### Backup Strategies

```bash
# Backup VDB snapshot
docker cp viator:/data/dump.vdb ./backup/

# Backup while running (uses copy-on-write)
docker exec viator viator-cli BGSAVE
docker cp viator:/data/dump.vdb ./backup/

# Verify backup integrity
docker run --rm -v $(pwd)/backup:/backup viator:latest \
  viator-check-vdb /backup/dump.vdb
```

---

## Networking

### Port Mapping

```yaml
services:
  viator:
    ports:
      - "6379:6379"      # Standard port
      - "127.0.0.1:6379:6379"  # Localhost only
```

### Custom Networks

```yaml
services:
  viator:
    networks:
      - backend

  app:
    networks:
      - backend
    environment:
      - REDIS_URL=redis://viator:6379

networks:
  backend:
```

### Container-to-Container

Other containers can connect using the service name:

```bash
# From another container in the same network
viator-cli -h viator -p 6379 PING
```

---

## Cluster Deployment

### Docker Compose Cluster

```yaml
services:
  viator-1:
    image: viator:latest
    ports:
      - "7001:6379"
    volumes:
      - viator-1-data:/data
    networks:
      - cluster-net

  viator-2:
    image: viator:latest
    ports:
      - "7002:6379"
    volumes:
      - viator-2-data:/data
    networks:
      - cluster-net

  viator-3:
    image: viator:latest
    ports:
      - "7003:6379"
    volumes:
      - viator-3-data:/data
    networks:
      - cluster-net

networks:
  cluster-net:

volumes:
  viator-1-data:
  viator-2-data:
  viator-3-data:
```

### Initialize Cluster

```bash
# Start all nodes
docker-compose up -d

# Meet nodes
docker exec viator-1 viator-cli CLUSTER MEET viator-2 6379
docker exec viator-1 viator-cli CLUSTER MEET viator-3 6379

# Assign slots
docker exec viator-1 viator-cli CLUSTER ADDSLOTSRANGE 0 5460
docker exec viator-2 viator-cli CLUSTER ADDSLOTSRANGE 5461 10922
docker exec viator-3 viator-cli CLUSTER ADDSLOTSRANGE 10923 16383

# Verify
docker exec viator-1 viator-cli CLUSTER INFO
```

---

## Production Considerations

### Resource Limits

Set memory and CPU limits:

```yaml
services:
  viator:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2'
        reservations:
          memory: 512M
          cpus: '0.5'
```

**Important**: Set `maxmemory` slightly below the container limit to prevent OOM kills:

```bash
# Container limit: 2G
# Viator maxmemory: 1.8G (leave headroom)
command: ["--maxmemory", "1800mb", "--maxmemory-policy", "allkeys-lru"]
```

### Health Checks

The default healthcheck uses PING:

```yaml
healthcheck:
  test: ["CMD-SHELL", "echo 'PING' | nc -q1 localhost 6379 | grep -q PONG"]
  interval: 30s
  timeout: 3s
  retries: 3
  start_period: 5s
```

### Logging

Configure Docker logging:

```yaml
services:
  viator:
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "3"
```

### Security

1. **Don't expose to internet** without authentication:
```yaml
ports:
  - "127.0.0.1:6379:6379"  # Localhost only
```

2. **Use passwords**:
```yaml
command: ["--requirepass", "your-strong-password"]
```

3. **Run as non-root** (default in Dockerfile):
```dockerfile
USER viator
```

4. **Read-only filesystem** (with data volume):
```yaml
services:
  viator:
    read_only: true
    tmpfs:
      - /tmp
    volumes:
      - viator-data:/data
```

### High Availability

For production HA, consider:

1. **Docker Swarm**:
```yaml
deploy:
  replicas: 1
  placement:
    constraints:
      - node.role == worker
  restart_policy:
    condition: on-failure
```

2. **Kubernetes**: Use StatefulSets for persistent identity

3. **External load balancer**: For client connections

---

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs viator

# Common issues:
# - Port already in use
# - Invalid config file
# - Permission denied on data volume
```

### Connection Refused

```bash
# Check if server is running
docker exec viator viator-cli PING

# Check port mapping
docker port viator

# Check network
docker network inspect bridge
```

### Data Not Persisting

```bash
# Verify volume is mounted
docker inspect viator | grep -A10 Mounts

# Check data directory inside container
docker exec viator ls -la /data

# Ensure proper shutdown for VDB save
docker stop viator  # Graceful shutdown
```

### Out of Memory

```bash
# Check memory usage
docker stats viator

# Solutions:
# 1. Increase container memory limit
# 2. Set maxmemory with eviction policy
# 3. Enable LRU eviction
```

### Performance Issues

```bash
# Run benchmark
docker exec viator viator-benchmark -n 100000

# Check for:
# - Network latency (use host network for benchmarks)
# - Disk I/O (use faster storage)
# - Memory pressure
```

### Using Host Network (for benchmarks)

```yaml
services:
  viator-bench:
    image: viator:latest
    network_mode: host
    command: ["--bind", "127.0.0.1"]
```

---

## Quick Reference

### Common Commands

```bash
# Start
docker-compose up -d

# Stop (graceful, saves VDB)
docker-compose stop

# Remove (keeps volumes)
docker-compose down

# Remove with volumes (DATA LOSS!)
docker-compose down -v

# View logs
docker-compose logs -f viator

# Execute CLI
docker exec -it viator viator-cli

# Backup
docker exec viator viator-cli BGSAVE
docker cp viator:/data/dump.vdb ./backup/

# Restore
docker cp ./backup/dump.vdb viator:/data/
docker restart viator
```

---

## See Also

- [CONFIG.md](CONFIG.md) - Configuration reference
- [CLUSTER.md](CLUSTER.md) - Cluster and Sentinel guide
- [PERSISTENCE.md](PERSISTENCE.md) - Persistence documentation
