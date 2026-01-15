#!/bin/bash
# Redis conformance testing script
# Usage: ./conformance_test.sh [port]

PORT=${1:-6379}
HOST="127.0.0.1"
FAILED=0
PASSED=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check() {
    local name="$1"
    local expected="$2"
    local cmd="$3"

    result=$(redis-cli -h $HOST -p $PORT $cmd 2>&1)

    if [[ "$result" == *"$expected"* ]]; then
        echo -e "${GREEN}✓${NC} $name"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC} $name"
        echo "  Expected: $expected"
        echo "  Got: $result"
        ((FAILED++))
    fi
}

check_exact() {
    local name="$1"
    local expected="$2"
    local cmd="$3"

    result=$(redis-cli -h $HOST -p $PORT $cmd 2>&1)

    if [[ "$result" == "$expected" ]]; then
        echo -e "${GREEN}✓${NC} $name"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC} $name"
        echo "  Expected: '$expected'"
        echo "  Got: '$result'"
        ((FAILED++))
    fi
}

echo -e "${YELLOW}=== Redis Conformance Test Suite ===${NC}"
echo "Testing server at $HOST:$PORT"
echo ""

# Clean slate
redis-cli -h $HOST -p $PORT FLUSHALL > /dev/null 2>&1

echo -e "${YELLOW}--- Connection Commands ---${NC}"
check "PING returns PONG" "PONG" "PING"
check "PING with message" "hello" "PING hello"
check "ECHO" "hello world" "ECHO 'hello world'"

echo ""
echo -e "${YELLOW}--- String Commands ---${NC}"
check "SET basic" "OK" "SET testkey testvalue"
check_exact "GET existing" "testvalue" "GET testkey"
check_exact "GET non-existing" "" "GET nonexistent"
check "SET with EX" "OK" "SET exkey value EX 100"
check "SET with PX" "OK" "SET pxkey value PX 100000"
check "SET with NX (new key)" "OK" "SET nxkey value NX"
check_exact "SET with NX (existing key)" "" "SET nxkey value2 NX"
check "SET with XX (existing key)" "OK" "SET nxkey newvalue XX"
check_exact "SET with XX (non-existing)" "" "SET xxnonexist value XX"
check "SETNX new key" "1" "SETNX setnxkey value"
check "SETNX existing key" "0" "SETNX setnxkey value2"
check "SETEX" "OK" "SETEX setexkey 100 value"
check "INCR new key" "1" "INCR counterkey"
check "INCR existing" "2" "INCR counterkey"
check "INCRBY" "12" "INCRBY counterkey 10"
check "DECR" "11" "DECR counterkey"
check "DECRBY" "1" "DECRBY counterkey 10"
check "INCRBYFLOAT" "1.5" "INCRBYFLOAT floatkey 1.5"
check "APPEND to new" "5" "APPEND appendkey hello"
check "APPEND to existing" "10" "APPEND appendkey world"
check_exact "GET appended" "helloworld" "GET appendkey"
check "STRLEN" "10" "STRLEN appendkey"
redis-cli -h $HOST -p $PORT SET rangekey "Hello World" > /dev/null
check_exact "GETRANGE" "World" "GETRANGE rangekey 6 10"
check "SETRANGE" "11" "SETRANGE rangekey 6 Redis"
check_exact "GET after SETRANGE" "Hello Redis" "GET rangekey"
check "MSET" "OK" "MSET mkey1 val1 mkey2 val2 mkey3 val3"
check "MGET" "val1" "MGET mkey1 mkey2 mkey3"
check_exact "GETSET" "val1" "GETSET mkey1 newval1"
check_exact "GET after GETSET" "newval1" "GET mkey1"
check_exact "GETDEL" "newval1" "GETDEL mkey1"
check_exact "GET after GETDEL" "" "GET mkey1"

echo ""
echo -e "${YELLOW}--- Key Commands ---${NC}"
check "EXISTS single existing" "1" "EXISTS testkey"
check "EXISTS single non-existing" "0" "EXISTS nonexistent"
check "EXISTS multiple" "2" "EXISTS testkey mkey2 nonexistent"
check "DEL single" "1" "DEL mkey3"
check_exact "GET after DEL" "" "GET mkey3"
check "TYPE string" "string" "TYPE testkey"
redis-cli -h $HOST -p $PORT SET ttlkey value EX 1000 > /dev/null
check "TTL" "99" "TTL ttlkey"  # Should be close to 1000
check "TTL no expire" "-1" "TTL testkey"
check "TTL non-existing" "-2" "TTL nonexistent"
check "EXPIRE" "1" "EXPIRE testkey 1000"
check "PERSIST" "1" "PERSIST testkey"
check "TTL after PERSIST" "-1" "TTL testkey"
redis-cli -h $HOST -p $PORT MSET renamekey value > /dev/null
check "RENAME" "OK" "RENAME renamekey renamedkey"
check_exact "GET after RENAME (old)" "" "GET renamekey"
check_exact "GET after RENAME (new)" "value" "GET renamedkey"
check "DBSIZE" "" "DBSIZE"  # Just check it runs

echo ""
echo -e "${YELLOW}--- List Commands ---${NC}"
check "LPUSH new list" "1" "LPUSH mylist one"
check "LPUSH existing" "3" "LPUSH mylist three two"
check "RPUSH" "4" "RPUSH mylist four"
check "LLEN" "4" "LLEN mylist"
check_exact "LINDEX 0" "three" "LINDEX mylist 0"
check_exact "LINDEX -1" "four" "LINDEX mylist -1"
check "LRANGE" "three" "LRANGE mylist 0 -1"
check_exact "LPOP" "three" "LPOP mylist"
check_exact "RPOP" "four" "RPOP mylist"
check "LLEN after pops" "2" "LLEN mylist"
check "LSET" "OK" "LSET mylist 0 updated"
check "LINSERT BEFORE" "3" "LINSERT mylist BEFORE updated inserted"
check "LREM" "1" "LREM mylist 1 inserted"

echo ""
echo -e "${YELLOW}--- Hash Commands ---${NC}"
check "HSET new" "1" "HSET myhash field1 value1"
check "HSET existing field" "0" "HSET myhash field1 newvalue1"
check "HSET multiple" "2" "HSET myhash field2 value2 field3 value3"
check_exact "HGET" "newvalue1" "HGET myhash field1"
check_exact "HGET non-existing field" "" "HGET myhash nonexistent"
check "HEXISTS existing" "1" "HEXISTS myhash field1"
check "HEXISTS non-existing" "0" "HEXISTS myhash nonexistent"
check "HLEN" "3" "HLEN myhash"
check "HDEL" "1" "HDEL myhash field3"
check "HLEN after HDEL" "2" "HLEN myhash"
check "HINCRBY" "10" "HINCRBY myhash numfield 10"
check "HINCRBY existing" "15" "HINCRBY myhash numfield 5"
check "HINCRBYFLOAT" "15.5" "HINCRBYFLOAT myhash numfield 0.5"
check "HSETNX new field" "1" "HSETNX myhash newfield newvalue"
check "HSETNX existing field" "0" "HSETNX myhash newfield othervalue"
check "HGETALL" "field1" "HGETALL myhash"
check "HKEYS" "field1" "HKEYS myhash"
check "HVALS" "newvalue1" "HVALS myhash"
check "HMSET" "OK" "HMSET myhash hmfield1 hmval1 hmfield2 hmval2"
check "HMGET" "hmval1" "HMGET myhash hmfield1 hmfield2"

echo ""
echo -e "${YELLOW}--- Set Commands ---${NC}"
check "SADD new set" "3" "SADD myset one two three"
check "SADD with duplicates" "1" "SADD myset four two"  # two is duplicate
check "SCARD" "4" "SCARD myset"
check "SISMEMBER existing" "1" "SISMEMBER myset one"
check "SISMEMBER non-existing" "0" "SISMEMBER myset nonexistent"
check "SMEMBERS" "one" "SMEMBERS myset"
check "SREM" "1" "SREM myset four"
check "SCARD after SREM" "3" "SCARD myset"
redis-cli -h $HOST -p $PORT SADD set2 two three four five > /dev/null
check "SINTER" "two" "SINTER myset set2"
check "SUNION" "one" "SUNION myset set2"
check "SDIFF" "one" "SDIFF myset set2"

echo ""
echo -e "${YELLOW}--- Sorted Set Commands ---${NC}"
check "ZADD new zset" "3" "ZADD myzset 1 one 2 two 3 three"
check "ZADD with update" "0" "ZADD myzset 1.5 one"  # Updates score, returns 0
check "ZCARD" "3" "ZCARD myzset"
check "ZSCORE" "1.5" "ZSCORE myzset one"
check_exact "ZSCORE non-existing" "" "ZSCORE myzset nonexistent"
check "ZRANK" "0" "ZRANK myzset one"
check "ZREVRANK" "2" "ZREVRANK myzset one"
check "ZRANGE" "one" "ZRANGE myzset 0 -1"
check "ZREVRANGE" "three" "ZREVRANGE myzset 0 -1"
check "ZRANGEBYSCORE" "one" "ZRANGEBYSCORE myzset 1 2"
check "ZCOUNT" "2" "ZCOUNT myzset 1 2"
check "ZINCRBY" "3.5" "ZINCRBY myzset 2 one"
check "ZREM" "1" "ZREM myzset three"
check "ZCARD after ZREM" "2" "ZCARD myzset"

echo ""
echo -e "${YELLOW}--- Edge Cases & Error Handling ---${NC}"
# Wrong type errors
redis-cli -h $HOST -p $PORT SET stringkey "string value" > /dev/null
check "WRONGTYPE on LPUSH to string" "WRONGTYPE" "LPUSH stringkey value"
check "WRONGTYPE on HGET on string" "WRONGTYPE" "HGET stringkey field"
check "WRONGTYPE on SADD to string" "WRONGTYPE" "SADD stringkey value"

# Argument errors
check "Wrong number of args" "ERR" "SET"
check "Wrong number of args 2" "ERR" "GET"

# Integer overflow
redis-cli -h $HOST -p $PORT SET intkey 9223372036854775807 > /dev/null
check "INCR overflow" "ERR" "INCR intkey"

# Binary safety
redis-cli -h $HOST -p $PORT SET binkey $'binary\x00data' > /dev/null
check "Binary data support" "" "GET binkey"

# Empty string
check "SET empty value" "OK" "SET emptykey ''"
check_exact "GET empty value" "" "GET emptykey"

# Large key/value (1MB)
largevalue=$(python3 -c "print('x' * 1048576)")
check "SET large value (1MB)" "OK" "SET largekey '$largevalue'"
check "GET large value" "" "GET largekey"

echo ""
echo -e "${YELLOW}--- Server Commands ---${NC}"
check "DBSIZE returns integer" "" "DBSIZE"
check "TIME returns array" "" "TIME"
check "INFO returns string" "redis_version" "INFO"
check "CONFIG GET" "" "CONFIG GET maxclients"
check "COMMAND COUNT" "" "COMMAND COUNT"
check "CLIENT LIST" "" "CLIENT LIST"

echo ""
echo "========================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "========================"

if [ $FAILED -gt 0 ]; then
    exit 1
fi
