#![no_main]

use libfuzzer_sys::fuzz_target;
use viator::RespParser;

fuzz_target!(|data: &[u8]| {
    // The parser should never panic, regardless of input
    let mut parser = RespParser::new();

    // Add fuzz data to the parser's internal buffer
    parser.buffer_mut().extend_from_slice(data);

    // Try to parse - should never panic
    let _ = parser.parse();

    // Try multiple parses in case there's state corruption
    parser.buffer_mut().extend_from_slice(data);
    let _ = parser.parse();
});
