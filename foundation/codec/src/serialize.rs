//! The pluggable serialization codec: bytes ↔ typed messages.
//!
//! [`Codec`] abstracts the *serialization format* (distinct from the [`LengthDelimited`]
//! *framing* in [`frame`](crate)). [`PostcardCodec`] is the internal default —
//! [postcard](https://docs.rs/postcard), a compact serde format for same-version
//! internal / node-to-node / short-lived messages. Other formats (bitcode, rkyv, a
//! tagged public format, …) can implement [`Codec`] and are selected at compile time:
//! the methods are generic over the message type, which precludes `dyn Codec` but
//! keeps encoding zero-overhead and works with any `serde` type.
//!
//! A message is put on the wire by serializing it with a [`Codec`] and then framing
//! the bytes with [`LengthDelimited`](crate::LengthDelimited); the reverse on receive.
//!
//! ```
//! use gsf_codec::{Codec, PostcardCodec};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct Ping { seq: u32 }
//!
//! let codec = PostcardCodec;
//! let bytes = codec.encode(&Ping { seq: 7 }).unwrap();
//! let back: Ping = codec.decode(&bytes).unwrap();
//! assert_eq!(back, Ping { seq: 7 });
//! ```

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::CodecError;

/// A swappable serialization format for messages.
///
/// Implementors turn a serde-serializable value into bytes and back. The internal
/// default is [`PostcardCodec`]; the format is swapped by choosing a different
/// implementor (chosen at compile time — the generic methods are not `dyn`-safe).
pub trait Codec {
    /// Serialize `value` into bytes.
    ///
    /// # Errors
    /// [`CodecError::Encode`] if serialization fails.
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>, CodecError>;

    /// Deserialize a `T` from `bytes`, which must be *exactly* one encoded message
    /// (leftover bytes are rejected).
    ///
    /// Returns an error for malformed, truncated, or trailing-byte input — never a
    /// panic, even on adversarial bytes.
    ///
    /// # Errors
    /// [`CodecError::Decode`] if `bytes` is not a valid encoding of `T`;
    /// [`CodecError::TrailingBytes`] if bytes remain after a complete message.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;
}

/// The internal default [`Codec`]: [postcard](https://docs.rs/postcard) — compact,
/// serde-based, for same-version internal messaging.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostcardCodec;

impl Codec for PostcardCodec {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        postcard::to_allocvec(value).map_err(|e| CodecError::Encode(Box::new(e)))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        // `take_from_bytes` (unlike `from_bytes`) returns the unconsumed tail, so we can
        // reject a message with trailing bytes — a frame must decode to exactly one value.
        let (value, rest) =
            postcard::take_from_bytes(bytes).map_err(|e| CodecError::Decode(Box::new(e)))?;
        if !rest.is_empty() {
            return Err(CodecError::TrailingBytes {
                remaining: rest.len(),
            });
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Player {
        id: u64,
        name: String,
        score: i32,
        alive: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum Command {
        Ping,
        Move { x: f32, y: f32 },
        Chat(String),
    }

    /// A nested shape: `Option`, a map, and a vector — the serde forms real packets use.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Inventory {
        owner: Option<u64>,
        items: BTreeMap<String, u32>,
        tags: Vec<String>,
    }

    /// Round-trip `value` through an arbitrary [`Codec`] — exercises the trait
    /// generically (any codec, any message type), not just the concrete impl.
    fn round_trip<C, T>(codec: &C, value: &T)
    where
        C: Codec,
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let bytes = codec.encode(value).unwrap();
        let back: T = codec.decode(&bytes).unwrap();
        assert_eq!(&back, value);
    }

    #[test]
    fn postcard_codec_should_round_trip_a_struct() {
        let codec = PostcardCodec;
        let player = Player {
            id: 7,
            name: "neo".to_string(),
            score: -3,
            alive: true,
        };
        let bytes = codec.encode(&player).unwrap();
        let back: Player = codec.decode(&bytes).unwrap();
        assert_eq!(back, player);
    }

    #[test]
    fn postcard_codec_should_round_trip_an_enum() {
        let codec = PostcardCodec;
        for cmd in [
            Command::Ping,
            Command::Move { x: 1.5, y: -2.0 },
            Command::Chat("hello".to_string()),
        ] {
            let bytes = codec.encode(&cmd).unwrap();
            let back: Command = codec.decode(&bytes).unwrap();
            assert_eq!(back, cmd);
        }
    }

    #[test]
    fn postcard_codec_should_round_trip_collections() {
        let codec = PostcardCodec;
        // A large collection exercises the sequence-encoding path, not just tiny values.
        let players: Vec<Player> = (0..1000)
            .map(|i| Player {
                id: i,
                name: format!("player-{i}"),
                score: i as i32 - 500,
                alive: i % 2 == 0,
            })
            .collect();
        let bytes = codec.encode(&players).unwrap();
        let back: Vec<Player> = codec.decode(&bytes).unwrap();
        assert_eq!(back, players);
    }

    #[test]
    fn decode_should_error_not_panic_on_truncated_bytes() {
        let codec = PostcardCodec;
        let player = Player {
            id: 7,
            name: "neo".to_string(),
            score: -3,
            alive: true,
        };
        let bytes = codec.encode(&player).unwrap();
        // Half a message: decoding must error, not panic.
        let err = codec
            .decode::<Player>(&bytes[..bytes.len() / 2])
            .unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }

    #[test]
    fn decode_should_error_on_empty_bytes() {
        let codec = PostcardCodec;
        let err = codec.decode::<Player>(&[]).unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }

    #[test]
    fn decode_should_reject_trailing_bytes() {
        let codec = PostcardCodec;
        let player = Player {
            id: 7,
            name: "neo".to_string(),
            score: -3,
            alive: true,
        };
        let mut bytes = codec.encode(&player).unwrap();
        bytes.extend_from_slice(&[0xAA, 0xBB]); // garbage after a complete message
        let err = codec.decode::<Player>(&bytes).unwrap_err();
        assert!(matches!(err, CodecError::TrailingBytes { remaining: 2 }));
    }

    #[test]
    fn decode_should_not_over_allocate_on_huge_length_claims() {
        let codec = PostcardCodec;
        // A postcard varint (LEB128) claiming a ~4-billion length, with nothing after.
        // Every length-prefixed shape must error *fast* (no OOM): postcard reads from
        // the finite buffer and serde's `size_hint::cautious` caps any pre-allocation,
        // so an attacker length cannot force an unbounded allocation (§1.4).
        let evil = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
        assert!(matches!(
            codec.decode::<Vec<u64>>(&evil).unwrap_err(),
            CodecError::Decode(_)
        ));
        assert!(matches!(
            codec.decode::<Vec<u8>>(&evil).unwrap_err(),
            CodecError::Decode(_)
        ));
        assert!(matches!(
            codec.decode::<String>(&evil).unwrap_err(),
            CodecError::Decode(_)
        ));
        assert!(matches!(
            codec.decode::<HashMap<u64, u64>>(&evil).unwrap_err(),
            CodecError::Decode(_)
        ));
    }

    #[test]
    fn codec_should_round_trip_message_shapes_generically() {
        let codec = PostcardCodec;
        // Exercise the `Codec` trait generically over several distinct message types.
        round_trip(
            &codec,
            &Player {
                id: 1,
                name: "a".to_string(),
                score: 0,
                alive: true,
            },
        );
        round_trip(&codec, &Command::Move { x: 0.0, y: 9.9 });
        round_trip(&codec, &Vec::<Player>::new()); // empty-collection boundary
        round_trip(
            &codec,
            &Inventory {
                owner: Some(42),
                items: BTreeMap::from([("gold".to_string(), 10), ("wood".to_string(), 3)]),
                tags: vec!["rare".to_string(), "quest".to_string()],
            },
        );
        round_trip(
            &codec,
            &Inventory {
                owner: None,
                items: BTreeMap::new(),
                tags: vec![],
            },
        );
    }

    #[test]
    fn postcard_codec_should_round_trip_float_edge_cases() {
        let codec = PostcardCodec;
        for f in [
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            -0.0_f32,
            f32::MIN,
            f32::MAX,
        ] {
            let bytes = codec.encode(&f).unwrap();
            let back: f32 = codec.decode(&bytes).unwrap();
            // Compare bit patterns so NaN (which is `!= itself`) round-trips exactly.
            assert_eq!(back.to_bits(), f.to_bits());
        }
    }

    proptest::proptest! {
        #[test]
        fn postcard_codec_should_round_trip_arbitrary_values(
            id: u64,
            name: String,
            score: i32,
            alive: bool,
        ) {
            let codec = PostcardCodec;
            let player = Player { id, name, score, alive };
            let bytes = codec.encode(&player).unwrap();
            let back: Player = codec.decode(&bytes).unwrap();
            proptest::prop_assert_eq!(back, player);
        }

        /// Decoding arbitrary bytes into a message type must never panic (returns
        /// `Ok` or `Err`) — the never-panic guarantee for untrusted input.
        #[test]
        fn decode_arbitrary_bytes_should_never_panic(data: Vec<u8>) {
            let codec = PostcardCodec;
            let _ = codec.decode::<Player>(&data);
            let _ = codec.decode::<Command>(&data);
            let _ = codec.decode::<Vec<u64>>(&data);
        }
    }
}
