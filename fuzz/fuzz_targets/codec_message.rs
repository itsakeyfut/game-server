#![no_main]
//! Fuzz target for the codec's message deserializers.
//!
//! Feeds arbitrary bytes straight to the real deserializers — `PostcardCodec` (internal)
//! and `TaggedCodec` (public / client-facing CBOR) — and asserts they **never panic** on
//! malformed / truncated / adversarial input: `decode` must always return `Ok` or `Err`.
//! Decoding directly (without framing first) exercises the deserializer itself, which a
//! frame-gated fuzzer would rarely reach. Several target shapes cover distinct decode
//! paths: a struct (CBOR map / skip-unknown), an enum (variant tag), and length-prefixed
//! collections (the over-allocation guard).

use gsf_codec::{Codec, PostcardCodec, TaggedCodec};
use libfuzzer_sys::fuzz_target;
use serde::Deserialize;

// Fields exist to define the wire shape for `Deserialize`; the decoded value is
// discarded (we assert no panic), so the analyzer sees the fields as never read.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Message {
    player_id: u64,
    text: String,
    score: i32,
    alive: bool,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
enum Command {
    Ping,
    Move { x: f32, y: f32 },
    Chat(String),
}

fuzz_target!(|data: &[u8]| {
    // Every `decode` must total-ly resolve to `Ok`/`Err`, never panic. The result is
    // deliberately discarded — we are asserting the absence of a panic / UB.
    let postcard = PostcardCodec;
    let _ = postcard.decode::<Message>(data);
    let _ = postcard.decode::<Command>(data);
    let _ = postcard.decode::<Vec<u64>>(data);
    let _ = postcard.decode::<String>(data);

    let tagged = TaggedCodec;
    let _ = tagged.decode::<Message>(data);
    let _ = tagged.decode::<Command>(data);
    let _ = tagged.decode::<Vec<u64>>(data);
    let _ = tagged.decode::<String>(data);
});
