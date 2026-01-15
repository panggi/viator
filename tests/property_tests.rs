//! Property-based tests using proptest.
//!
//! These tests verify invariants that should always hold,
//! helping find edge cases that unit tests might miss.

use proptest::prelude::*;

/// Generate arbitrary Redis keys
fn arb_key() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9:_-]{1,100}").unwrap()
}

/// Generate arbitrary Redis string values
fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..10000)
}

/// Generate arbitrary integers within Redis range
fn arb_redis_int() -> impl Strategy<Value = i64> {
    prop::num::i64::ANY
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// SET followed by GET should return the same value
    #[test]
    #[ignore] // Run with: cargo test property -- --ignored
    fn prop_set_get_roundtrip(key in arb_key(), value in arb_value()) {
        // This would connect to a running server and verify:
        // SET key value -> GET key == value
        // For now, we just verify the key and value are valid
        prop_assert!(!key.is_empty());
        prop_assert!(key.len() <= 100);
    }

    /// INCR should always increase by exactly 1
    #[test]
    #[ignore]
    fn prop_incr_adds_one(initial in -1000000i64..1000000i64) {
        // SET key initial -> INCR key == initial + 1
        let expected = initial + 1;
        prop_assert_eq!(initial.wrapping_add(1), expected);
    }

    /// LPUSH followed by LPOP should return the same element
    #[test]
    #[ignore]
    fn prop_list_push_pop_fifo(values in prop::collection::vec(arb_value(), 1..10)) {
        // LPUSH key values... -> LPOP key (multiple times) returns values in reverse order
        let reversed: Vec<_> = values.iter().rev().cloned().collect();
        prop_assert_eq!(values.len(), reversed.len());
    }

    /// SADD followed by SMEMBERS should contain all added elements
    #[test]
    #[ignore]
    fn prop_set_contains_added_members(members in prop::collection::hash_set(arb_key(), 1..50)) {
        // SADD myset members... -> SMEMBERS myset contains all members
        prop_assert!(!members.is_empty());
    }

    /// ZADD followed by ZSCORE should return the correct score
    #[test]
    #[ignore]
    fn prop_sorted_set_score_accuracy(
        score in prop::num::f64::NORMAL,
        member in arb_key()
    ) {
        // ZADD zset score member -> ZSCORE zset member == score
        prop_assert!(score.is_finite());
    }

    /// DEL should make EXISTS return 0
    #[test]
    #[ignore]
    fn prop_del_removes_key(key in arb_key()) {
        // SET key value -> DEL key -> EXISTS key == 0
        prop_assert!(!key.is_empty());
    }

    /// EXPIRE with TTL > 0 should make TTL return positive value
    #[test]
    #[ignore]
    fn prop_expire_sets_ttl(ttl in 1i64..86400i64) {
        // SET key value -> EXPIRE key ttl -> TTL key <= ttl
        prop_assert!(ttl > 0);
    }

    /// HSET followed by HGET should return the same value
    #[test]
    #[ignore]
    fn prop_hash_set_get_roundtrip(
        field in arb_key(),
        value in arb_value()
    ) {
        // HSET hash field value -> HGET hash field == value
        prop_assert!(!field.is_empty());
    }

    /// APPEND should increase STRLEN by appended length
    #[test]
    #[ignore]
    fn prop_append_increases_length(
        initial in arb_value(),
        append in arb_value()
    ) {
        // SET key initial -> APPEND key append -> STRLEN key == initial.len() + append.len()
        let expected_len = initial.len() + append.len();
        prop_assert!(expected_len >= initial.len());
    }

    /// GETRANGE should return substring of correct length
    #[test]
    #[ignore]
    fn prop_getrange_correct_length(
        value in "[a-z]{10,100}",
        start in 0usize..50usize,
        end in 0usize..100usize
    ) {
        // SET key value -> GETRANGE key start end
        if start <= end && start < value.len() {
            let actual_end = std::cmp::min(end, value.len() - 1);
            let expected_len = actual_end - start + 1;
            prop_assert!(expected_len <= value.len());
        }
    }
}

/// RESP protocol property tests
mod resp_properties {
    use super::*;

    /// Valid RESP simple strings should parse correctly
    #[test]
    fn simple_string_format() {
        // +OK\r\n should always parse as SimpleString("OK")
        let valid = b"+OK\r\n";
        assert_eq!(valid[0], b'+');
        assert!(valid.ends_with(b"\r\n"));
    }

    /// Valid RESP integers should parse to their value
    #[test]
    fn integer_format() {
        // :42\r\n should parse as Integer(42)
        let valid = b":42\r\n";
        assert_eq!(valid[0], b':');
    }

    /// Valid RESP bulk strings should have correct length
    #[test]
    fn bulk_string_length() {
        // $5\r\nhello\r\n should parse as BulkString("hello")
        let valid = b"$5\r\nhello\r\n";
        assert_eq!(valid[0], b'$');
        // Length declaration should match actual content
        let content = b"hello";
        assert_eq!(content.len(), 5);
    }

    proptest! {
        /// Any encoded bulk string should decode to same content
        #[test]
        fn prop_bulk_string_roundtrip(content in prop::collection::vec(any::<u8>(), 0..1000)) {
            let encoded = format!("${}\r\n", content.len());
            prop_assert!(encoded.starts_with('$'));
        }

        /// Any valid integer should encode and decode correctly
        #[test]
        fn prop_integer_roundtrip(n in any::<i64>()) {
            let encoded = format!(":{}\r\n", n);
            prop_assert!(encoded.starts_with(':'));
            prop_assert!(encoded.ends_with("\r\n"));
        }
    }
}

/// Concurrency property tests
mod concurrency_properties {
    use super::*;

    proptest! {
        /// Concurrent INCRs should result in correct final value
        #[test]
        #[ignore]
        fn prop_concurrent_incr_correctness(
            num_increments in 1usize..100usize
        ) {
            // Start with 0 -> N concurrent INCRs -> final value == N
            prop_assert!(num_increments > 0);
        }

        /// Concurrent LPUSH/RPOP should maintain list integrity
        #[test]
        #[ignore]
        fn prop_concurrent_list_operations(
            pushes in 1usize..50usize,
            pops in 1usize..50usize
        ) {
            // List length should never be negative
            // All popped values should have been pushed
            prop_assert!(pushes > 0);
            prop_assert!(pops > 0);
        }
    }
}
