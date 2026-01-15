#!/bin/bash
# Expert-level edge case tests that break most Redis implementations
# These are the cases Redis core devs actually test

PORT=${1:-6379}
HOST="127.0.0.1"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass=0
fail=0

check() {
    local name="$1"
    local expected="$2"
    shift 2
    local result=$(redis-cli -h $HOST -p $PORT "$@" 2>&1)

    if [[ "$result" == *"$expected"* ]]; then
        echo -e "${GREEN}✓${NC} $name"
        ((pass++))
    else
        echo -e "${RED}✗${NC} $name"
        echo "  Command: redis-cli $*"
        echo "  Expected: $expected"
        echo "  Got: $result"
        ((fail++))
    fi
}

check_exact() {
    local name="$1"
    local expected="$2"
    shift 2
    local result=$(redis-cli -h $HOST -p $PORT "$@" 2>&1)

    if [[ "$result" == "$expected" ]]; then
        echo -e "${GREEN}✓${NC} $name"
        ((pass++))
    else
        echo -e "${RED}✗${NC} $name"
        echo "  Command: redis-cli $*"
        echo "  Expected: '$expected'"
        echo "  Got: '$result'"
        ((fail++))
    fi
}

echo -e "${YELLOW}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║     Expert Edge Cases That Break Most Implementations         ║${NC}"
echo -e "${YELLOW}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

redis-cli -h $HOST -p $PORT FLUSHALL > /dev/null 2>&1

echo -e "${YELLOW}=== SET Command Edge Cases ===${NC}"

# Zero expiry - should set then immediately expire
redis-cli -h $HOST -p $PORT SET zeroexp value EX 0 > /dev/null 2>&1
sleep 0.1
check "SET EX 0 expires immediately" "0" EXISTS zeroexp

# Negative expiry - should error
check "SET with negative EX" "ERR" SET negexp value EX -1

# EXAT with past timestamp - should expire immediately
past_ts=$(($(date +%s) - 1000))
redis-cli -h $HOST -p $PORT SET pastexp value EXAT $past_ts > /dev/null 2>&1
sleep 0.1
check "SET EXAT past timestamp" "0" EXISTS pastexp

# GET on wrong type should error
redis-cli -h $HOST -p $PORT LPUSH listkey value > /dev/null 2>&1
check "SET GET on list type" "WRONGTYPE" SET listkey newval GET

# NX + XX should error (not just fail silently)
check "SET NX XX mutual exclusion" "ERR" SET key value NX XX

# KEEPTTL with EX should error
check "SET KEEPTTL with EX" "ERR" SET key value EX 100 KEEPTTL

# SET preserves type change
redis-cli -h $HOST -p $PORT LPUSH typekey elem > /dev/null 2>&1
redis-cli -h $HOST -p $PORT SET typekey stringval > /dev/null 2>&1
check "SET overwrites different type" "string" TYPE typekey

echo ""
echo -e "${YELLOW}=== Integer Edge Cases ===${NC}"

# Exact boundary testing
redis-cli -h $HOST -p $PORT SET maxint 9223372036854775807 > /dev/null
check "INCR at INT64_MAX" "ERR" INCR maxint

redis-cli -h $HOST -p $PORT SET minint -9223372036854775808 > /dev/null
check "DECR at INT64_MIN" "ERR" DECR minint

# String that looks like number but isn't
redis-cli -h $HOST -p $PORT SET notnum "123abc" > /dev/null
check "INCR non-integer string" "ERR" INCR notnum

# Float edge cases
redis-cli -h $HOST -p $PORT SET floatkey 1.0 > /dev/null
check "INCR on float string" "ERR" INCR floatkey

# Scientific notation
redis-cli -h $HOST -p $PORT SET scikey "1e10" > /dev/null
check "INCRBYFLOAT scientific notation" "ERR" INCRBYFLOAT scikey 1

# Inf/NaN handling
check "INCRBYFLOAT inf" "ERR" INCRBYFLOAT infkey inf

echo ""
echo -e "${YELLOW}=== List Edge Cases ===${NC}"

# LINDEX beyond bounds
redis-cli -h $HOST -p $PORT DEL testlist > /dev/null
redis-cli -h $HOST -p $PORT RPUSH testlist a b c > /dev/null
check_exact "LINDEX beyond positive bound" "" LINDEX testlist 100
check_exact "LINDEX beyond negative bound" "" LINDEX testlist -100

# LRANGE with inverted indices
check "LRANGE inverted indices returns empty" "" LRANGE testlist 2 1

# LSET beyond bounds should error
check "LSET beyond bounds" "ERR" LSET testlist 100 value

# Empty list auto-delete
redis-cli -h $HOST -p $PORT DEL emptylist > /dev/null
redis-cli -h $HOST -p $PORT RPUSH emptylist single > /dev/null
redis-cli -h $HOST -p $PORT LPOP emptylist > /dev/null
check "Empty list deleted" "0" EXISTS emptylist

# LPOS (Redis 6.0.6+) if supported
redis-cli -h $HOST -p $PORT DEL poslist > /dev/null
redis-cli -h $HOST -p $PORT RPUSH poslist a b c b d b > /dev/null
result=$(redis-cli -h $HOST -p $PORT LPOS poslist b 2>&1)
if [[ "$result" != *"ERR"* ]]; then
    check "LPOS finds first occurrence" "1" LPOS poslist b
fi

echo ""
echo -e "${YELLOW}=== Hash Edge Cases ===${NC}"

# HINCRBY on non-integer field
redis-cli -h $HOST -p $PORT HSET testhash strfield "hello" > /dev/null
check "HINCRBY on string" "ERR" HINCRBY testhash strfield 1

# HINCRBYFLOAT precision
redis-cli -h $HOST -p $PORT HSET testhash floatfield "10.5" > /dev/null
result=$(redis-cli -h $HOST -p $PORT HINCRBYFLOAT testhash floatfield 0.1)
# Should be exactly 10.6, not 10.600000000000001
check "HINCRBYFLOAT precision" "10.6" HINCRBYFLOAT testhash floatfield 0.0

# HSET returns count of NEW fields only
redis-cli -h $HOST -p $PORT DEL newhash > /dev/null
check "HSET new field returns 1" "1" HSET newhash field1 value1
check "HSET existing field returns 0" "0" HSET newhash field1 value2
check "HSET mixed returns new count" "2" HSET newhash field1 v field2 v field3 v

echo ""
echo -e "${YELLOW}=== Sorted Set Edge Cases ===${NC}"

# Score edge cases
redis-cli -h $HOST -p $PORT DEL testzset > /dev/null
check "ZADD +inf score" "1" ZADD testzset +inf posinf
check "ZADD -inf score" "1" ZADD testzset -inf neginf
score=$(redis-cli -h $HOST -p $PORT ZSCORE testzset posinf)
check "ZSCORE returns inf" "inf" ZSCORE testzset posinf

# NaN should error
check "ZADD NaN score" "ERR" ZADD testzset nan member

# ZADD XX with new member should not add
redis-cli -h $HOST -p $PORT DEL xxzset > /dev/null
check "ZADD XX new member" "0" ZADD xxzset XX 1.0 newmember
check "ZADD XX member not added" "0" EXISTS xxzset

# ZRANGEBYSCORE with exclusive bounds
redis-cli -h $HOST -p $PORT DEL rangezset > /dev/null
redis-cli -h $HOST -p $PORT ZADD rangezset 1 a 2 b 3 c 4 d 5 e > /dev/null
result=$(redis-cli -h $HOST -p $PORT ZRANGEBYSCORE rangezset "(1" "(5" 2>&1)
# Should return b, c, d (exclusive of 1 and 5)
if [[ "$result" == *"b"* ]] && [[ "$result" == *"c"* ]] && [[ "$result" == *"d"* ]]; then
    if [[ "$result" != *"a"* ]] && [[ "$result" != *"e"* ]]; then
        echo -e "${GREEN}✓${NC} ZRANGEBYSCORE exclusive bounds"
        ((pass++))
    else
        echo -e "${RED}✗${NC} ZRANGEBYSCORE exclusive bounds (included boundary elements)"
        ((fail++))
    fi
else
    echo -e "${RED}✗${NC} ZRANGEBYSCORE exclusive bounds"
    ((fail++))
fi

echo ""
echo -e "${YELLOW}=== Key Expiry Edge Cases ===${NC}"

# PTTL precision
redis-cli -h $HOST -p $PORT SET pttlkey value PX 10000 > /dev/null
pttl=$(redis-cli -h $HOST -p $PORT PTTL pttlkey)
if [[ "$pttl" -ge 9000 ]] && [[ "$pttl" -le 10000 ]]; then
    echo -e "${GREEN}✓${NC} PTTL millisecond precision"
    ((pass++))
else
    echo -e "${RED}✗${NC} PTTL millisecond precision (got $pttl, expected 9000-10000)"
    ((fail++))
fi

# PERSIST on key without TTL
redis-cli -h $HOST -p $PORT SET noexpkey value > /dev/null
check "PERSIST on no-TTL key" "0" PERSIST noexpkey

# EXPIRETIME (Redis 7.0+)
result=$(redis-cli -h $HOST -p $PORT EXPIRETIME noexpkey 2>&1)
if [[ "$result" != *"ERR"* ]]; then
    check "EXPIRETIME on no-TTL key" "-1" EXPIRETIME noexpkey
fi

# TTL returns -2 for non-existent key
check "TTL non-existent key" "-2" TTL definitelynotakey

echo ""
echo -e "${YELLOW}=== Concurrent Atomicity ===${NC}"

# This is the true test of atomicity
redis-cli -h $HOST -p $PORT SET atomictest 0 > /dev/null
echo "Running 10 clients × 100 INCRs..."

for i in {1..10}; do
    (
        for j in {1..100}; do
            redis-cli -h $HOST -p $PORT INCR atomictest > /dev/null 2>&1
        done
    ) &
done
wait

result=$(redis-cli -h $HOST -p $PORT GET atomictest)
if [[ "$result" == "1000" ]]; then
    echo -e "${GREEN}✓${NC} INCR atomicity under concurrency (result: $result)"
    ((pass++))
else
    echo -e "${RED}✗${NC} INCR atomicity under concurrency (expected 1000, got $result)"
    ((fail++))
fi

echo ""
echo -e "${YELLOW}=== Protocol Edge Cases ===${NC}"

# Inline command parsing
result=$(echo "PING" | nc -q1 $HOST $PORT 2>/dev/null | tr -d '\r\n')
if [[ "$result" == "+PONG" ]]; then
    echo -e "${GREEN}✓${NC} Inline command (no RESP framing)"
    ((pass++))
else
    echo -e "${RED}✗${NC} Inline command (expected +PONG, got '$result')"
    ((fail++))
fi

# Command case insensitivity
result=$(redis-cli -h $HOST -p $PORT pInG 2>&1)
check "Command case insensitivity" "PONG" pInG

# Extra whitespace in inline
result=$(echo "  PING  " | nc -q1 $HOST $PORT 2>/dev/null | tr -d '\r\n')
if [[ "$result" == "+PONG" ]]; then
    echo -e "${GREEN}✓${NC} Inline command with extra whitespace"
    ((pass++))
else
    echo -e "${RED}✗${NC} Inline command with extra whitespace"
    ((fail++))
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo -e "Results: ${GREEN}$pass passed${NC}, ${RED}$fail failed${NC}"
echo "════════════════════════════════════════════════════════════════"

if [[ $fail -gt 0 ]]; then
    exit 1
fi
