//! Pre-allocated RESP responses for zero-allocation common replies.
//!
//! This module provides static byte slices for frequently-used responses,
//! avoiding allocation overhead for common cases like OK, NULL, integers 0-100, etc.

use bytes::Bytes;

/// Static OK response: +OK\r\n
pub static OK: &[u8] = b"+OK\r\n";

/// Static PONG response: +PONG\r\n
pub static PONG: &[u8] = b"+PONG\r\n";

/// Static QUEUED response (for transactions): +QUEUED\r\n
pub static QUEUED: &[u8] = b"+QUEUED\r\n";

/// Static NULL bulk string: $-1\r\n
pub static NULL_BULK: &[u8] = b"$-1\r\n";

/// Static NULL array: *-1\r\n
pub static NULL_ARRAY: &[u8] = b"*-1\r\n";

/// Static empty bulk string: $0\r\n\r\n
pub static EMPTY_BULK: &[u8] = b"$0\r\n\r\n";

/// Static empty array: *0\r\n
pub static EMPTY_ARRAY: &[u8] = b"*0\r\n";

/// Static integer 0: :0\r\n
pub static ZERO: &[u8] = b":0\r\n";

/// Static integer 1: :1\r\n
pub static ONE: &[u8] = b":1\r\n";

/// Static integer -1: :-1\r\n
pub static NEG_ONE: &[u8] = b":-1\r\n";

/// Static integer -2: :-2\r\n
pub static NEG_TWO: &[u8] = b":-2\r\n";

/// Pre-computed small integer responses (0-100)
/// Avoids formatting for common cases like INCR results, lengths, etc.
static SMALL_INTEGERS: [&[u8]; 101] = [
    b":0\r\n",
    b":1\r\n",
    b":2\r\n",
    b":3\r\n",
    b":4\r\n",
    b":5\r\n",
    b":6\r\n",
    b":7\r\n",
    b":8\r\n",
    b":9\r\n",
    b":10\r\n",
    b":11\r\n",
    b":12\r\n",
    b":13\r\n",
    b":14\r\n",
    b":15\r\n",
    b":16\r\n",
    b":17\r\n",
    b":18\r\n",
    b":19\r\n",
    b":20\r\n",
    b":21\r\n",
    b":22\r\n",
    b":23\r\n",
    b":24\r\n",
    b":25\r\n",
    b":26\r\n",
    b":27\r\n",
    b":28\r\n",
    b":29\r\n",
    b":30\r\n",
    b":31\r\n",
    b":32\r\n",
    b":33\r\n",
    b":34\r\n",
    b":35\r\n",
    b":36\r\n",
    b":37\r\n",
    b":38\r\n",
    b":39\r\n",
    b":40\r\n",
    b":41\r\n",
    b":42\r\n",
    b":43\r\n",
    b":44\r\n",
    b":45\r\n",
    b":46\r\n",
    b":47\r\n",
    b":48\r\n",
    b":49\r\n",
    b":50\r\n",
    b":51\r\n",
    b":52\r\n",
    b":53\r\n",
    b":54\r\n",
    b":55\r\n",
    b":56\r\n",
    b":57\r\n",
    b":58\r\n",
    b":59\r\n",
    b":60\r\n",
    b":61\r\n",
    b":62\r\n",
    b":63\r\n",
    b":64\r\n",
    b":65\r\n",
    b":66\r\n",
    b":67\r\n",
    b":68\r\n",
    b":69\r\n",
    b":70\r\n",
    b":71\r\n",
    b":72\r\n",
    b":73\r\n",
    b":74\r\n",
    b":75\r\n",
    b":76\r\n",
    b":77\r\n",
    b":78\r\n",
    b":79\r\n",
    b":80\r\n",
    b":81\r\n",
    b":82\r\n",
    b":83\r\n",
    b":84\r\n",
    b":85\r\n",
    b":86\r\n",
    b":87\r\n",
    b":88\r\n",
    b":89\r\n",
    b":90\r\n",
    b":91\r\n",
    b":92\r\n",
    b":93\r\n",
    b":94\r\n",
    b":95\r\n",
    b":96\r\n",
    b":97\r\n",
    b":98\r\n",
    b":99\r\n",
    b":100\r\n",
];

/// Get a pre-allocated integer response if available.
/// Returns None for integers outside the pre-allocated range.
#[inline]
pub fn integer(n: i64) -> Option<&'static [u8]> {
    if (0..=100).contains(&n) {
        Some(SMALL_INTEGERS[n as usize])
    } else if n == -1 {
        Some(NEG_ONE)
    } else if n == -2 {
        Some(NEG_TWO)
    } else {
        None
    }
}

/// Get a pre-allocated integer response, or format a new one.
#[inline]
pub fn integer_or_format(n: i64) -> Bytes {
    if let Some(static_resp) = integer(n) {
        Bytes::from_static(static_resp)
    } else {
        Bytes::from(format!(":{}\r\n", n))
    }
}

/// Common error responses
pub mod errors {
    /// WRONGTYPE error
    pub static WRONGTYPE: &[u8] =
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    /// ERR syntax error
    pub static SYNTAX_ERROR: &[u8] = b"-ERR syntax error\r\n";

    /// ERR invalid integer
    pub static NOT_INTEGER: &[u8] = b"-ERR value is not an integer or out of range\r\n";

    /// ERR invalid float
    pub static NOT_FLOAT: &[u8] = b"-ERR value is not a valid float\r\n";

    /// ERR no such key
    pub static NO_SUCH_KEY: &[u8] = b"-ERR no such key\r\n";

    /// ERR unknown command
    pub static UNKNOWN_COMMAND: &[u8] = b"-ERR unknown command\r\n";

    /// NOAUTH authentication required
    pub static NOAUTH: &[u8] = b"-NOAUTH Authentication required.\r\n";

    /// ERR invalid password
    pub static INVALID_PASSWORD: &[u8] =
        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n";

    /// ERR out of memory
    pub static OOM: &[u8] = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n";

    /// ERR busy
    pub static BUSY: &[u8] = b"-BUSY Redis is busy running a script.\r\n";

    /// EXECABORT transaction aborted
    pub static EXECABORT: &[u8] =
        b"-EXECABORT Transaction discarded because of previous errors.\r\n";

    /// ERR MULTI nesting not allowed
    pub static MULTI_NESTED: &[u8] = b"-ERR MULTI calls can not be nested\r\n";

    /// ERR EXEC without MULTI
    pub static EXEC_WITHOUT_MULTI: &[u8] = b"-ERR EXEC without MULTI\r\n";

    /// ERR DISCARD without MULTI
    pub static DISCARD_WITHOUT_MULTI: &[u8] = b"-ERR DISCARD without MULTI\r\n";
}

/// Common type name responses for TYPE command
pub mod types {
    pub static STRING: &[u8] = b"+string\r\n";
    pub static LIST: &[u8] = b"+list\r\n";
    pub static SET: &[u8] = b"+set\r\n";
    pub static ZSET: &[u8] = b"+zset\r\n";
    pub static HASH: &[u8] = b"+hash\r\n";
    pub static STREAM: &[u8] = b"+stream\r\n";
    pub static NONE: &[u8] = b"+none\r\n";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_integers() {
        assert_eq!(integer(0), Some(b":0\r\n".as_slice()));
        assert_eq!(integer(1), Some(b":1\r\n".as_slice()));
        assert_eq!(integer(100), Some(b":100\r\n".as_slice()));
        assert_eq!(integer(-1), Some(b":-1\r\n".as_slice()));
        assert_eq!(integer(-2), Some(b":-2\r\n".as_slice()));
        assert_eq!(integer(101), None);
        assert_eq!(integer(-3), None);
    }

    #[test]
    fn test_integer_or_format() {
        assert_eq!(&integer_or_format(50)[..], b":50\r\n");
        assert_eq!(&integer_or_format(200)[..], b":200\r\n");
        assert_eq!(&integer_or_format(-100)[..], b":-100\r\n");
    }

    #[test]
    fn test_static_responses() {
        assert_eq!(OK, b"+OK\r\n");
        assert_eq!(PONG, b"+PONG\r\n");
        assert_eq!(NULL_BULK, b"$-1\r\n");
        assert_eq!(EMPTY_ARRAY, b"*0\r\n");
    }
}
