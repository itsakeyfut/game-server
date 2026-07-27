#![no_main]
//! Fuzz target for codec framing.
//!
//! Feeds arbitrary bytes to `gsf-codec`'s length-prefix framer and asserts it
//! **never panics** on malformed / truncated / oversized input: `decode` must always
//! return `Ok(Some)`, `Ok(None)`, or `Err`, and `decode_eof` must total-ly resolve
//! the leftover. A small frame cap makes the over-cap error path common in the corpus.

use bytes::BytesMut;
use gsf_codec::LengthDelimited;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let codec = LengthDelimited::new(4096);
    let mut buf = BytesMut::from(data);
    // Pull off every complete frame; stop on "need more" or an error.
    while let Ok(Some(_frame)) = codec.decode(&mut buf) {}
    // Exercise the end-of-stream path over whatever remains.
    let _ = codec.decode_eof(&mut buf);
});
