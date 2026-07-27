//! The public-protocol [`Codec`]: a tagged, self-describing wire format.
//!
//! [`PostcardCodec`](crate::PostcardCodec) is compact but **non-tagged** and
//! field-order-dependent — it cannot skip an unknown field, so it can't evolve a
//! client-facing protocol. [`TaggedCodec`] encodes with [CBOR](https://www.rfc-editor.org/rfc/rfc8949)
//! (via [ciborium](https://docs.rs/ciborium)), which is **self-describing**: structs
//! become maps keyed by field name, so serde skips a field the decoder doesn't know.
//! That is what lets a [`Visibility::Public`](crate::Visibility::Public) message gain a
//! field while old clients keep decoding it.
//!
//! **Evolution rule:** adding a field is backward-compatible for old decoders (they
//! ignore the unknown field); for a new decoder to read an *old* message, the added
//! field must be `#[serde(default)]` or `Option<_>` (else its value is missing).
//!
//! CBOR is the interim backing format; the self-generated neutral schema (numeric
//! field tags, cross-language bindings) is a later concern (spec §2.2.4, open).
//!
//! ```
//! use gsf_codec::{Codec, TaggedCodec};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize)]
//! struct ChatV2 { text: String, reply_to: u64 }
//!
//! #[derive(Deserialize, PartialEq, Debug)]
//! struct ChatV1 { text: String } // an older client, without `reply_to`
//!
//! let codec = TaggedCodec;
//! let bytes = codec.encode(&ChatV2 { text: "hi".into(), reply_to: 7 }).unwrap();
//! let old: ChatV1 = codec.decode(&bytes).unwrap(); // the added field is skipped
//! assert_eq!(old, ChatV1 { text: "hi".into() });
//! ```

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::CodecError;
use crate::serialize::Codec;

/// The public-protocol [`Codec`]: a tagged, self-describing CBOR format so
/// client-facing messages evolve their fields backward-compatibly.
#[derive(Debug, Clone, Copy, Default)]
pub struct TaggedCodec;

impl Codec for TaggedCodec {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        let mut buf = Vec::new();
        ciborium::into_writer(value, &mut buf).map_err(|e| CodecError::Encode(Box::new(e)))?;
        Ok(buf)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        // Read from a slice cursor: `&[u8]: Read` advances the cursor to the unconsumed
        // tail, so we can enforce the "exactly one message per frame" contract by
        // rejecting leftover bytes — matching `PostcardCodec`.
        let mut cursor = bytes;
        let value =
            ciborium::from_reader(&mut cursor).map_err(|e| CodecError::Decode(Box::new(e)))?;
        if !cursor.is_empty() {
            return Err(CodecError::TrailingBytes {
                remaining: cursor.len(),
            });
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use super::*;

    // A public message and its evolved form: v2 adds `timestamp`.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct MessageV1 {
        player_id: u64,
        text: String,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct MessageV2 {
        player_id: u64,
        text: String,
        // `#[serde(default)]` so a *new* decoder can also read an *old* message that
        // predates this field — the other half of the evolution rule.
        #[serde(default)]
        timestamp: u64,
    }

    #[test]
    fn tagged_codec_should_decode_a_new_message_with_an_added_field_on_an_old_client() {
        let codec = TaggedCodec;
        let v2 = MessageV2 {
            player_id: 42,
            text: "gg".to_string(),
            timestamp: 1234,
        };
        let bytes = codec.encode(&v2).unwrap();

        // The old client (MessageV1, no `timestamp`) decodes the newer message: the
        // unknown field is skipped rather than erroring. This is the core requirement.
        let old: MessageV1 = codec.decode(&bytes).unwrap();
        assert_eq!(
            old,
            MessageV1 {
                player_id: 42,
                text: "gg".to_string(),
            }
        );
    }

    #[test]
    fn tagged_codec_should_decode_an_old_message_on_a_new_client() {
        let codec = TaggedCodec;
        let v1 = MessageV1 {
            player_id: 7,
            text: "hi".to_string(),
        };
        let bytes = codec.encode(&v1).unwrap();

        // The new client (MessageV2) reads the older message; the missing `timestamp`
        // falls back to its `#[serde(default)]` value.
        let new: MessageV2 = codec.decode(&bytes).unwrap();
        assert_eq!(
            new,
            MessageV2 {
                player_id: 7,
                text: "hi".to_string(),
                timestamp: 0,
            }
        );
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum Command {
        Ping,
        Move { x: f32, y: f32 },
        Chat(String),
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Inventory {
        owner: Option<u64>,
        items: BTreeMap<String, u32>,
        tags: Vec<String>,
    }

    #[test]
    fn tagged_codec_should_round_trip_struct_enum_and_nested() {
        let codec = TaggedCodec;

        let msg = MessageV2 {
            player_id: 1,
            text: "hello".to_string(),
            timestamp: 99,
        };
        let bytes = codec.encode(&msg).unwrap();
        assert_eq!(codec.decode::<MessageV2>(&bytes).unwrap(), msg);

        for cmd in [
            Command::Ping,
            Command::Move { x: 1.5, y: -2.0 },
            Command::Chat("hey".to_string()),
        ] {
            let bytes = codec.encode(&cmd).unwrap();
            assert_eq!(codec.decode::<Command>(&bytes).unwrap(), cmd);
        }

        for inv in [
            Inventory {
                owner: Some(42),
                items: BTreeMap::from([("gold".to_string(), 10), ("wood".to_string(), 3)]),
                tags: vec!["rare".to_string()],
            },
            Inventory {
                owner: None,
                items: BTreeMap::new(),
                tags: vec![],
            },
        ] {
            let bytes = codec.encode(&inv).unwrap();
            assert_eq!(codec.decode::<Inventory>(&bytes).unwrap(), inv);
        }
    }

    #[test]
    fn tagged_codec_should_round_trip_a_large_collection() {
        let codec = TaggedCodec;
        // A large payload exercises the sequence path, not just tiny values.
        let msgs: Vec<MessageV1> = (0..1000)
            .map(|i| MessageV1 {
                player_id: i,
                text: format!("m{i}"),
            })
            .collect();
        let bytes = codec.encode(&msgs).unwrap();
        assert_eq!(codec.decode::<Vec<MessageV1>>(&bytes).unwrap(), msgs);
    }

    #[test]
    fn tagged_decode_should_reject_trailing_bytes() {
        let codec = TaggedCodec;
        let mut bytes = codec
            .encode(&MessageV1 {
                player_id: 1,
                text: "x".to_string(),
            })
            .unwrap();
        bytes.extend_from_slice(&[0xAA, 0xBB]); // garbage after a complete message
        assert!(matches!(
            codec.decode::<MessageV1>(&bytes).unwrap_err(),
            CodecError::TrailingBytes { remaining: 2 }
        ));
    }

    #[test]
    fn tagged_decode_should_error_not_panic_on_truncated_bytes() {
        let codec = TaggedCodec;
        let bytes = codec
            .encode(&MessageV1 {
                player_id: 1,
                text: "truncate me".to_string(),
            })
            .unwrap();
        // Half a message must error, not panic.
        let err = codec
            .decode::<MessageV1>(&bytes[..bytes.len() / 2])
            .unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }

    #[test]
    fn tagged_decode_should_not_over_allocate_on_huge_length_claims() {
        let codec = TaggedCodec;
        // CBOR headers advertising a ~u64::MAX count with no payload after. Decoding
        // must error *fast* (hit EOF), never pre-allocating the advertised size: serde's
        // `size_hint::cautious` caps any reserve, so an attacker length cannot OOM us.
        // 0x9B = array, 8-byte length follows.
        let huge_array = [0x9B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(matches!(
            codec.decode::<Vec<u64>>(&huge_array).unwrap_err(),
            CodecError::Decode(_)
        ));
        // 0x7B = text string, 8-byte length follows.
        let huge_string = [0x7B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(matches!(
            codec.decode::<String>(&huge_string).unwrap_err(),
            CodecError::Decode(_)
        ));
        // 0xBB = map, 8-byte length follows.
        let huge_map = [0xBB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(matches!(
            codec.decode::<BTreeMap<u64, u64>>(&huge_map).unwrap_err(),
            CodecError::Decode(_)
        ));
    }

    proptest::proptest! {
        #[test]
        fn tagged_codec_should_round_trip_arbitrary_values(
            player_id: u64,
            text: String,
        ) {
            let codec = TaggedCodec;
            let msg = MessageV1 { player_id, text };
            let bytes = codec.encode(&msg).unwrap();
            let back: MessageV1 = codec.decode(&bytes).unwrap();
            proptest::prop_assert_eq!(back, msg);
        }

        /// Decoding arbitrary bytes into a public message type must never panic — the
        /// never-panic guarantee for untrusted client input.
        #[test]
        fn tagged_decode_arbitrary_bytes_should_never_panic(data: Vec<u8>) {
            let codec = TaggedCodec;
            let _ = codec.decode::<MessageV1>(&data);
            let _ = codec.decode::<Command>(&data);
            let _ = codec.decode::<Vec<u64>>(&data);
        }
    }
}
