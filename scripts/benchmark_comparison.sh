#!/bin/bash
# Performance comparison: Your redis-rs vs Official Redis
# Usage: ./benchmark_comparison.sh <redis-rs-port> <redis-port>

REDIS_RS_PORT=${1:-6380}
REDIS_PORT=${2:-6379}
REQUESTS=100000
CLIENTS=50
PIPELINE=16

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║     Redis Performance Comparison: redis-rs vs Official       ║${NC}"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Configuration:"
echo "  Requests: $REQUESTS"
echo "  Clients: $CLIENTS"
echo "  Pipeline: $PIPELINE"
echo ""

run_benchmark() {
    local port=$1
    local name=$2

    echo -e "${CYAN}=== $name (port $port) ===${NC}"

    # Warm up
    redis-benchmark -p $port -n 1000 -c 10 -q > /dev/null 2>&1

    # Run benchmarks
    redis-benchmark -p $port -n $REQUESTS -c $CLIENTS -P $PIPELINE -q \
        -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange,mset 2>&1

    echo ""

    # Latency test
    echo "Latency distribution (SET command):"
    redis-benchmark -p $port -n 10000 -c 1 -t set --csv 2>&1 | tail -1

    echo ""
}

# Check if servers are running
if ! redis-cli -p $REDIS_PORT PING > /dev/null 2>&1; then
    echo -e "${RED}Official Redis not running on port $REDIS_PORT${NC}"
    echo "Start it with: redis-server --port $REDIS_PORT"
    exit 1
fi

if ! redis-cli -p $REDIS_RS_PORT PING > /dev/null 2>&1; then
    echo -e "${RED}redis-rs not running on port $REDIS_RS_PORT${NC}"
    echo "Start it with: cargo run --release -- --port $REDIS_RS_PORT"
    exit 1
fi

# Run benchmarks
run_benchmark $REDIS_PORT "Official Redis"
run_benchmark $REDIS_RS_PORT "redis-rs (Your Implementation)"

echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Additional Analysis Commands:"
echo ""
echo "  # Detailed latency histogram"
echo "  redis-benchmark -p $REDIS_RS_PORT -n 100000 -t get --csv"
echo ""
echo "  # Memory efficiency comparison"
echo "  redis-cli -p $REDIS_PORT INFO memory | grep used_memory_human"
echo "  redis-cli -p $REDIS_RS_PORT INFO memory | grep used_memory_human"
echo ""
echo "  # Pipeline performance"
echo "  redis-benchmark -p $REDIS_RS_PORT -n 100000 -P 100 -t set,get -q"
echo ""
