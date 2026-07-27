//! End-to-end round-trip property tests over the codec's public API.
//!
//! These exercise the full send/receive pipeline — serialize with a [`Codec`], frame
//! the bytes with [`LengthDelimited`], then deframe and deserialize — as production
//! does, for both the internal ([`PostcardCodec`]) and public ([`TaggedCodec`]) codecs.
//! Per-layer round-trip / never-panic proptests live in the crate's unit tests; this
//! file covers the *combined* pipeline and the public codec's field-evolution
//! (version-compat) property (operability §3.1).

use bytes::BytesMut;
use gsf_codec::{Codec, LengthDelimited, PostcardCodec, TaggedCodec};
use proptest::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A representative public message.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Message {
    player_id: u64,
    text: String,
    score: i32,
    alive: bool,
}

/// The older shape of `MessageV2`, without `timestamp`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct MessageV1 {
    player_id: u64,
    text: String,
}

/// `MessageV1` evolved with an added field.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct MessageV2 {
    player_id: u64,
    text: String,
    #[serde(default)]
    timestamp: u64,
}

/// Serialize `value` with `codec`, frame it, then deframe and deserialize — the exact
/// production path — and assert the message survives intact.
fn pipeline_round_trip<C, T>(codec: &C, value: &T)
where
    C: Codec,
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let framer = LengthDelimited::default();

    let payload = codec.encode(value).expect("encode");
    let mut wire = BytesMut::new();
    framer.encode(&payload, &mut wire).expect("frame");

    let frame = framer
        .decode(&mut wire)
        .expect("deframe")
        .expect("one full frame");
    assert!(wire.is_empty(), "the frame's bytes were fully consumed");

    let back: T = codec.decode(&frame).expect("decode");
    assert_eq!(&back, value);
}

#[test]
fn pipeline_should_round_trip_a_large_message_through_both_codecs() {
    // A payload larger than a tiny buffer, so the large-frame path runs (not just tiny
    // values the proptests favor).
    let big = Message {
        player_id: 7,
        text: "x".repeat(100_000),
        score: -42,
        alive: true,
    };
    pipeline_round_trip(&PostcardCodec, &big);
    pipeline_round_trip(&TaggedCodec, &big);
}

proptest! {
    #[test]
    fn postcard_pipeline_round_trip_should_recover_the_message(
        player_id: u64,
        text: String,
        score: i32,
        alive: bool,
    ) {
        let msg = Message { player_id, text, score, alive };
        pipeline_round_trip(&PostcardCodec, &msg);
    }

    #[test]
    fn tagged_pipeline_round_trip_should_recover_the_message(
        player_id: u64,
        text: String,
        score: i32,
        alive: bool,
    ) {
        let msg = Message { player_id, text, score, alive };
        pipeline_round_trip(&TaggedCodec, &msg);
    }

    /// The public codec's version-compat property: whatever the values, an old client
    /// (`MessageV1`) still decodes a message that gained a field (`MessageV2`), the
    /// shared fields surviving. Generalizes the fixed-value backward-compat unit test.
    #[test]
    fn tagged_field_evolution_should_survive_arbitrary_values(
        player_id: u64,
        text: String,
        timestamp: u64,
    ) {
        let codec = TaggedCodec;
        let v2 = MessageV2 { player_id, text: text.clone(), timestamp };
        let bytes = codec.encode(&v2).expect("encode v2");
        let v1: MessageV1 = codec.decode(&bytes).expect("old client decodes new message");
        prop_assert_eq!(v1, MessageV1 { player_id, text });
    }
}
