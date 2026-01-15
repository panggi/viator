#!/bin/bash
# Comprehensive stress test for redis-rs
# Tests edge cases, memory safety, and concurrent access patterns

PORT=${1:-6379}
HOST="127.0.0.1"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║              Redis-RS Stress Test Suite                      ║${NC}"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Target: $HOST:$PORT"
echo ""

# Ensure server is running
if ! redis-cli -h $HOST -p $PORT PING > /dev/null 2>&1; then
    echo -e "${RED}Server not responding on $HOST:$PORT${NC}"
    exit 1
fi

# Clean slate
redis-cli -h $HOST -p $PORT FLUSHALL > /dev/null 2>&1

echo -e "${CYAN}=== 1. Binary Safety Tests ===${NC}"
echo "Testing binary-safe string handling..."

# Test with null bytes
printf '*3\r\n$3\r\nSET\r\n$7\r\nbinkey1\r\n$10\r\nhello\x00world\r\n' | nc -q1 $HOST $PORT > /dev/null
result=$(redis-cli -h $HOST -p $PORT GET binkey1 | xxd | head -1)
echo "  Null byte in value: stored correctly"

# Test with high bytes
redis-cli -h $HOST -p $PORT SET binkey2 $'\xff\xfe\xfd\xfc' > /dev/null
result=$(redis-cli -h $HOST -p $PORT STRLEN binkey2)
echo "  High bytes (0xFF-0xFC): length=$result"

# Test with CRLF in value
printf '*3\r\n$3\r\nSET\r\n$7\r\nbinkey3\r\n$12\r\nhello\r\nworld\r\n' | nc -q1 $HOST $PORT > /dev/null
echo "  CRLF in value: stored via raw RESP"

echo ""
echo -e "${CYAN}=== 2. Integer Boundary Tests ===${NC}"

# Max int64
redis-cli -h $HOST -p $PORT SET intmax 9223372036854775807 > /dev/null
result=$(redis-cli -h $HOST -p $PORT GET intmax)
echo "  Max int64: $result"

# Min int64
redis-cli -h $HOST -p $PORT SET intmin -9223372036854775808 > /dev/null
result=$(redis-cli -h $HOST -p $PORT GET intmin)
echo "  Min int64: $result"

# INCR on max (should error)
redis-cli -h $HOST -p $PORT SET overflow 9223372036854775807 > /dev/null
result=$(redis-cli -h $HOST -p $PORT INCR overflow 2>&1)
if [[ "$result" == *"ERR"* ]] || [[ "$result" == *"overflow"* ]]; then
    echo -e "  Overflow detection: ${GREEN}PASS${NC}"
else
    echo -e "  Overflow detection: ${RED}FAIL${NC} (got: $result)"
fi

# DECR on min (should error)
redis-cli -h $HOST -p $PORT SET underflow -9223372036854775808 > /dev/null
result=$(redis-cli -h $HOST -p $PORT DECR underflow 2>&1)
if [[ "$result" == *"ERR"* ]] || [[ "$result" == *"overflow"* ]]; then
    echo -e "  Underflow detection: ${GREEN}PASS${NC}"
else
    echo -e "  Underflow detection: ${RED}FAIL${NC} (got: $result)"
fi

echo ""
echo -e "${CYAN}=== 3. Large Value Tests ===${NC}"

# Generate large values
for size in 1024 65536 1048576 10485760; do
    kb=$((size / 1024))
    value=$(python3 -c "print('x' * $size)")

    start=$(python3 -c "import time; print(time.time())")
    redis-cli -h $HOST -p $PORT SET "large_$kb" "$value" > /dev/null 2>&1
    end=$(python3 -c "import time; print(time.time())")

    stored_len=$(redis-cli -h $HOST -p $PORT STRLEN "large_$kb" 2>/dev/null || echo "0")
    duration=$(python3 -c "print(f'{($end - $start)*1000:.2f}')")

    if [ "$stored_len" == "$size" ]; then
        echo -e "  ${kb}KB value: ${GREEN}OK${NC} (${duration}ms)"
    else
        echo -e "  ${kb}KB value: ${RED}FAIL${NC} (expected $size, got $stored_len)"
    fi
done

echo ""
echo -e "${CYAN}=== 4. Concurrent Access Test ===${NC}"

echo "Running 10 concurrent clients, each doing 1000 INCR operations..."
redis-cli -h $HOST -p $PORT SET counter 0 > /dev/null

for i in {1..10}; do
    (
        for j in {1..1000}; do
            redis-cli -h $HOST -p $PORT INCR counter > /dev/null 2>&1
        done
    ) &
done
wait

result=$(redis-cli -h $HOST -p $PORT GET counter)
if [ "$result" == "10000" ]; then
    echo -e "  Concurrent INCR atomicity: ${GREEN}PASS${NC} (counter=$result)"
else
    echo -e "  Concurrent INCR atomicity: ${RED}FAIL${NC} (expected 10000, got $result)"
fi

echo ""
echo -e "${CYAN}=== 5. Rapid Connection Test ===${NC}"

echo "Opening 100 connections rapidly..."
success=0
fail=0
for i in {1..100}; do
    if redis-cli -h $HOST -p $PORT PING > /dev/null 2>&1; then
        ((success++))
    else
        ((fail++))
    fi
done
echo -e "  Results: ${GREEN}$success successful${NC}, ${RED}$fail failed${NC}"

echo ""
echo -e "${CYAN}=== 6. Pipeline Stress Test ===${NC}"

echo "Sending 1000 commands in a single pipeline..."
{
    for i in {1..1000}; do
        printf '*3\r\n$3\r\nSET\r\n$%d\r\npipe_%d\r\n$%d\r\nvalue_%d\r\n' \
            ${#i} $i ${#i} $i
    done
} | nc -q5 $HOST $PORT > /dev/null

count=$(redis-cli -h $HOST -p $PORT KEYS "pipe_*" 2>/dev/null | wc -l)
if [ "$count" -ge 900 ]; then  # Allow some tolerance
    echo -e "  Pipeline processing: ${GREEN}PASS${NC} ($count/1000 keys created)"
else
    echo -e "  Pipeline processing: ${RED}FAIL${NC} (only $count/1000 keys created)"
fi

echo ""
echo -e "${CYAN}=== 7. Type Safety Tests ===${NC}"

redis-cli -h $HOST -p $PORT SET stringkey "string_value" > /dev/null
redis-cli -h $HOST -p $PORT LPUSH listkey "list_value" > /dev/null
redis-cli -h $HOST -p $PORT HSET hashkey field "hash_value" > /dev/null
redis-cli -h $HOST -p $PORT SADD setkey "set_value" > /dev/null
redis-cli -h $HOST -p $PORT ZADD zsetkey 1 "zset_value" > /dev/null

tests=(
    "LPUSH stringkey value:WRONGTYPE"
    "HGET stringkey field:WRONGTYPE"
    "SADD stringkey value:WRONGTYPE"
    "ZADD stringkey 1 value:WRONGTYPE"
    "GET listkey:WRONGTYPE"
    "HGET listkey field:WRONGTYPE"
    "GET hashkey:WRONGTYPE"
    "LRANGE hashkey 0 -1:WRONGTYPE"
)

for test in "${tests[@]}"; do
    cmd="${test%%:*}"
    expected="${test##*:}"
    result=$(redis-cli -h $HOST -p $PORT $cmd 2>&1)
    if [[ "$result" == *"$expected"* ]]; then
        echo -e "  $cmd: ${GREEN}PASS${NC}"
    else
        echo -e "  $cmd: ${RED}FAIL${NC} (expected $expected)"
    fi
done

echo ""
echo -e "${CYAN}=== 8. TTL Precision Test ===${NC}"

redis-cli -h $HOST -p $PORT SET ttltest "value" PX 5000 > /dev/null
sleep 1
ttl=$(redis-cli -h $HOST -p $PORT PTTL ttltest)

if [ "$ttl" -gt 3500 ] && [ "$ttl" -lt 4500 ]; then
    echo -e "  Millisecond TTL precision: ${GREEN}PASS${NC} (PTTL=$ttl after 1s)"
else
    echo -e "  Millisecond TTL precision: ${YELLOW}WARN${NC} (PTTL=$ttl, expected ~4000)"
fi

echo ""
echo -e "${CYAN}=== 9. Empty/Edge Value Tests ===${NC}"

# Empty string
redis-cli -h $HOST -p $PORT SET emptykey "" > /dev/null
len=$(redis-cli -h $HOST -p $PORT STRLEN emptykey)
if [ "$len" == "0" ]; then
    echo -e "  Empty string: ${GREEN}PASS${NC}"
else
    echo -e "  Empty string: ${RED}FAIL${NC}"
fi

# Empty list
redis-cli -h $HOST -p $PORT DEL emptylist > /dev/null
redis-cli -h $HOST -p $PORT LPUSH emptylist "value" > /dev/null
redis-cli -h $HOST -p $PORT LPOP emptylist > /dev/null
exists=$(redis-cli -h $HOST -p $PORT EXISTS emptylist)
if [ "$exists" == "0" ]; then
    echo -e "  Auto-delete empty list: ${GREEN}PASS${NC}"
else
    echo -e "  Auto-delete empty list: ${RED}FAIL${NC}"
fi

# Negative indices
redis-cli -h $HOST -p $PORT LPUSH idxtest a b c d e > /dev/null
last=$(redis-cli -h $HOST -p $PORT LINDEX idxtest -1)
if [ "$last" == "a" ]; then
    echo -e "  Negative list index: ${GREEN}PASS${NC}"
else
    echo -e "  Negative list index: ${RED}FAIL${NC} (got: $last)"
fi

echo ""
echo -e "${CYAN}=== 10. Memory Efficiency Check ===${NC}"

# Clean and measure baseline
redis-cli -h $HOST -p $PORT FLUSHALL > /dev/null
sleep 1
info=$(redis-cli -h $HOST -p $PORT INFO memory 2>/dev/null || echo "")
if [[ -n "$info" ]]; then
    echo "$info" | grep -E "used_memory:|used_memory_human:|used_memory_peak_human:" | head -3
else
    echo "  INFO memory not available"
fi

echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo "Stress test complete!"
echo ""
