#!/usr/bin/env bash
#
# Comprehensive load testing script for redis-rs
# Compares performance against official Redis
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REDIS_RS_PORT=${REDIS_RS_PORT:-6380}
REDIS_OFFICIAL_PORT=${REDIS_OFFICIAL_PORT:-6379}
NUM_REQUESTS=${NUM_REQUESTS:-100000}
NUM_CLIENTS=${NUM_CLIENTS:-50}
PIPELINE_SIZE=${PIPELINE_SIZE:-16}
DATA_SIZE=${DATA_SIZE:-256}

print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}\n"
}

print_section() {
    echo -e "\n${YELLOW}▶ $1${NC}\n"
}

check_redis_cli() {
    if ! command -v redis-cli &> /dev/null; then
        echo -e "${RED}Error: redis-cli not found${NC}"
        echo "Please install redis-cli to run load tests"
        exit 1
    fi
}

check_server() {
    local port=$1
    local name=$2
    
    if redis-cli -p "$port" PING &> /dev/null; then
        echo -e "${GREEN}✓${NC} $name is running on port $port"
        return 0
    else
        echo -e "${RED}✗${NC} $name is not running on port $port"
        return 1
    fi
}

run_benchmark() {
    local port=$1
    local name=$2
    local test_type=$3
    
    echo -e "${YELLOW}Testing $name ($test_type)...${NC}"
    
    case $test_type in
        "basic")
            redis-benchmark -p "$port" -q -n "$NUM_REQUESTS" -c "$NUM_CLIENTS" \
                -t ping,set,get,incr,lpush,rpush,lpop,rpop,sadd,spop,mset
            ;;
        "pipeline")
            redis-benchmark -p "$port" -q -n "$NUM_REQUESTS" -c "$NUM_CLIENTS" \
                -P "$PIPELINE_SIZE" -t set,get
            ;;
        "large-values")
            redis-benchmark -p "$port" -q -n "$((NUM_REQUESTS / 10))" -c "$NUM_CLIENTS" \
                -d "$DATA_SIZE" -t set,get
            ;;
        "high-concurrency")
            redis-benchmark -p "$port" -q -n "$NUM_REQUESTS" -c 500 -t set,get
            ;;
    esac
}

run_latency_test() {
    local port=$1
    local name=$2
    
    echo -e "${YELLOW}Latency histogram for $name...${NC}"
    redis-cli -p "$port" --latency-history -i 1 &
    local pid=$!
    sleep 5
    kill $pid 2>/dev/null || true
}

run_memory_test() {
    local port=$1
    local name=$2
    
    echo -e "${YELLOW}Memory efficiency test for $name...${NC}"
    
    # Clear existing data
    redis-cli -p "$port" FLUSHALL > /dev/null
    
    # Insert test data
    local num_keys=10000
    echo "Inserting $num_keys keys..."
    
    for i in $(seq 1 $num_keys); do
        redis-cli -p "$port" SET "testkey:$i" "$(head -c 100 /dev/urandom | base64)" > /dev/null
    done
    
    # Get memory stats
    echo -e "\nMemory stats:"
    redis-cli -p "$port" INFO memory | grep -E "used_memory_human|used_memory_peak_human|mem_fragmentation"
    
    # Clean up
    redis-cli -p "$port" FLUSHALL > /dev/null
}

run_stress_test() {
    local port=$1
    local name=$2
    local duration=${3:-30}
    
    echo -e "${YELLOW}Stress test for $name (${duration}s)...${NC}"
    
    # Run continuous load for specified duration
    timeout "$duration" redis-benchmark -p "$port" -q -l -c 100 -t set,get 2>/dev/null || true
}

compare_results() {
    local rs_result=$1
    local official_result=$2
    local metric=$3
    
    if (( $(echo "$rs_result >= $official_result" | bc -l) )); then
        echo -e "${GREEN}✓${NC} redis-rs ${metric}: ${rs_result} >= Redis ${official_result}"
    else
        local pct=$(echo "scale=2; ($official_result - $rs_result) / $official_result * 100" | bc)
        echo -e "${YELLOW}△${NC} redis-rs ${metric}: ${rs_result} (${pct}% slower than Redis ${official_result})"
    fi
}

main() {
    print_header "Redis-RS Load Testing Suite"
    
    check_redis_cli
    
    echo "Configuration:"
    echo "  - Requests: $NUM_REQUESTS"
    echo "  - Clients: $NUM_CLIENTS"
    echo "  - Pipeline: $PIPELINE_SIZE"
    echo "  - Data size: $DATA_SIZE bytes"
    
    # Check servers
    print_section "Checking Servers"
    
    local rs_running=false
    local official_running=false
    
    if check_server "$REDIS_RS_PORT" "redis-rs"; then
        rs_running=true
    fi
    
    if check_server "$REDIS_OFFICIAL_PORT" "Official Redis"; then
        official_running=true
    fi
    
    if ! $rs_running && ! $official_running; then
        echo -e "\n${RED}No Redis servers running. Please start at least one server.${NC}"
        echo "  redis-rs:        cargo run -- --port $REDIS_RS_PORT"
        echo "  Official Redis:  redis-server --port $REDIS_OFFICIAL_PORT"
        exit 1
    fi
    
    # Basic throughput tests
    print_section "Basic Throughput Tests"
    
    if $rs_running; then
        run_benchmark "$REDIS_RS_PORT" "redis-rs" "basic"
    fi
    
    if $official_running; then
        run_benchmark "$REDIS_OFFICIAL_PORT" "Official Redis" "basic"
    fi
    
    # Pipeline tests
    print_section "Pipeline Tests (P=$PIPELINE_SIZE)"
    
    if $rs_running; then
        run_benchmark "$REDIS_RS_PORT" "redis-rs" "pipeline"
    fi
    
    if $official_running; then
        run_benchmark "$REDIS_OFFICIAL_PORT" "Official Redis" "pipeline"
    fi
    
    # High concurrency tests
    print_section "High Concurrency Tests (500 clients)"
    
    if $rs_running; then
        run_benchmark "$REDIS_RS_PORT" "redis-rs" "high-concurrency"
    fi
    
    if $official_running; then
        run_benchmark "$REDIS_OFFICIAL_PORT" "Official Redis" "high-concurrency"
    fi
    
    # Large value tests
    print_section "Large Value Tests (${DATA_SIZE} bytes)"
    
    if $rs_running; then
        run_benchmark "$REDIS_RS_PORT" "redis-rs" "large-values"
    fi
    
    if $official_running; then
        run_benchmark "$REDIS_OFFICIAL_PORT" "Official Redis" "large-values"
    fi
    
    print_header "Load Testing Complete"
    
    echo "For more detailed analysis, consider using:"
    echo "  - memtier_benchmark (more realistic workloads)"
    echo "  - redis-cli --latency-history (latency distribution)"
    echo "  - Rust integration tests: cargo test --test load_test -- --ignored"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --rs-port)
            REDIS_RS_PORT="$2"
            shift 2
            ;;
        --redis-port)
            REDIS_OFFICIAL_PORT="$2"
            shift 2
            ;;
        -n|--requests)
            NUM_REQUESTS="$2"
            shift 2
            ;;
        -c|--clients)
            NUM_CLIENTS="$2"
            shift 2
            ;;
        -P|--pipeline)
            PIPELINE_SIZE="$2"
            shift 2
            ;;
        -d|--data-size)
            DATA_SIZE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --rs-port PORT     redis-rs port (default: 6380)"
            echo "  --redis-port PORT  Official Redis port (default: 6379)"
            echo "  -n, --requests N   Number of requests (default: 100000)"
            echo "  -c, --clients N    Number of clients (default: 50)"
            echo "  -P, --pipeline N   Pipeline size (default: 16)"
            echo "  -d, --data-size N  Data size in bytes (default: 256)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

main
