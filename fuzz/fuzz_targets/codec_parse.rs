#![no_main]
//! Fuzz target for the codec framing → deserialization pipeline.
//!
//! Feeds arbitrary bytes to `gsf-codec`'s length-prefix framer and asserts it
//! **never panics** on malformed / truncated / oversized input: `decode` must always
//! return `Ok(Some)`, `Ok(None)`, or `Err`, and `decode_eof` must total-ly resolve
//! the leftover. A small frame cap makes the over-cap error path common in the corpus.
//!
//! Each deframed frame is then fed to the real message codecs (the production
//! frame→decode path), which must likewise never panic. `codec_message` fuzzes the
//! deserializers directly; here they run on genuinely-framed payloads.

use bytes::BytesMut;
use gsf_codec::{Codec, LengthDelimited, PostcardCodec, TaggedCodec};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let codec = LengthDelimited::new(4096);
    let mut buf = BytesMut::from(data);
    // Pull off every complete frame; decode each with both codecs (never panics).
    while let Ok(Some(frame)) = codec.decode(&mut buf) {
        let _ = PostcardCodec.decode::<String>(&frame);
        let _ = PostcardCodec.decode::<Vec<u64>>(&frame);
        let _ = TaggedCodec.decode::<String>(&frame);
        let _ = TaggedCodec.decode::<Vec<u64>>(&frame);
    }
    // Exercise the end-of-stream path over whatever remains.
    let _ = codec.decode_eof(&mut buf);
});
