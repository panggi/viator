#![no_main]

use libfuzzer_sys::fuzz_target;
use bytes::BytesMut;
use viator::RespParser;

fuzz_target!(|data: &[u8]| {
    // The parser should never panic, regardless of input
    let mut buffer = BytesMut::from(data);
    let mut parser = RespParser::new();

    // Try to parse - should never panic
    let _ = parser.parse(&mut buffer);

    // Try multiple parses in case there's state corruption
    let mut buffer2 = BytesMut::from(data);
    let _ = parser.parse(&mut buffer2);
});
