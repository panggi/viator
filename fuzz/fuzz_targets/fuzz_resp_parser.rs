//! Fuzz target for RESP protocol parser.
//!
//! This target tests the RESP parser with arbitrary input to find
//! parsing bugs, panics, and potential security issues.

#![no_main]

use libfuzzer_sys::fuzz_target;
use viator::RespParser;

fuzz_target!(|data: &[u8]| {
    let mut parser = RespParser::new();
    parser.buffer_mut().extend_from_slice(data);

    // Try to parse - should never panic
    let _ = parser.parse();
});
