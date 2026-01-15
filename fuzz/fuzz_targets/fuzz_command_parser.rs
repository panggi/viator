//! Fuzz target for command parsing.
//!
//! Tests command argument parsing and validation.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use viator::Frame;

fuzz_target!(|data: &[u8]| {
    // Create a frame from fuzzer input
    let bytes = Bytes::copy_from_slice(data);
    
    // Try various frame constructions
    let _ = Frame::Bulk(bytes.clone());
    let _ = Frame::Simple(String::from_utf8_lossy(data).into_owned());
    
    // Try parsing as integer
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = s.parse::<i64>();
    }
});
