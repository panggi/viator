//! Fuzz target for Frame serialization/deserialization.
//!
//! Tests that frame serialization is robust against malformed input.

#![no_main]

use arbitrary::Arbitrary;
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use viator::{Frame, RespParser};

#[derive(Arbitrary, Debug)]
enum FuzzFrame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<FuzzFrame>),
}

impl From<FuzzFrame> for Frame {
    fn from(f: FuzzFrame) -> Self {
        match f {
            FuzzFrame::Simple(s) => Frame::Simple(s),
            FuzzFrame::Error(s) => Frame::Error(s),
            FuzzFrame::Integer(i) => Frame::Integer(i),
            FuzzFrame::Bulk(b) => Frame::Bulk(b.into()),
            FuzzFrame::Null => Frame::Null,
            FuzzFrame::Array(arr) => Frame::Array(arr.into_iter().map(Into::into).collect()),
        }
    }
}

fuzz_target!(|frame: FuzzFrame| {
    let frame: Frame = frame.into();

    // Serialize the frame
    let mut buffer = BytesMut::new();
    frame.serialize(&mut buffer);

    // Try to parse it back - roundtrip should work
    let mut parser = RespParser::new();
    parser.buffer_mut().unsplit(buffer);
    let _ = parser.parse();
});
