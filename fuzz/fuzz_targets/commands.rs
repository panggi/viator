#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::{Arbitrary, Unstructured};
use bytes::Bytes;

/// Arbitrary command generator for fuzzing command parsing
#[derive(Debug)]
struct FuzzCommand {
    name: String,
    args: Vec<Bytes>,
}

impl<'a> Arbitrary<'a> for FuzzCommand {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // Generate random command names
        let commands = [
            "GET", "SET", "DEL", "MGET", "MSET", "INCR", "DECR",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE",
            "SADD", "SREM", "SMEMBERS", "SISMEMBER",
            "HSET", "HGET", "HDEL", "HGETALL",
            "ZADD", "ZRANGE", "ZSCORE", "ZRANK",
            "PING", "ECHO", "INFO", "DBSIZE",
            "EXPIRE", "TTL", "PTTL", "PERSIST",
            "EXISTS", "TYPE", "KEYS", "SCAN",
        ];

        let name = u.choose(&commands)?.to_string();

        // Generate 0-10 random arguments
        let num_args = u.int_in_range(0..=10)?;
        let mut args = Vec::with_capacity(num_args);

        for _ in 0..num_args {
            let arg_len = u.int_in_range(0..=1000)?;
            let arg_data: Vec<u8> = (0..arg_len)
                .map(|_| u.arbitrary())
                .collect::<arbitrary::Result<_>>()?;
            args.push(Bytes::from(arg_data));
        }

        Ok(FuzzCommand { name, args })
    }
}

fuzz_target!(|cmd: FuzzCommand| {
    // Build RESP array for the command
    let mut resp = format!("*{}\r\n", cmd.args.len() + 1);
    resp.push_str(&format!("${}\r\n{}\r\n", cmd.name.len(), cmd.name));

    for arg in &cmd.args {
        resp.push_str(&format!("${}\r\n", arg.len()));
        // Note: This might not be valid UTF-8, but that's intentional for fuzzing
    }

    // The actual command execution would happen here in a full integration test
    // For now, we just verify the RESP building doesn't panic
    let _ = resp.as_bytes();
});
