//! Length-prefixed framing with a size cap.
//!
//! The wire format is a big-endian [`u32`] length prefix followed by exactly that
//! many payload bytes:
//!
//! ```text
//! +--------------------+------------------------+
//! | length (u32, BE)   | payload (length bytes) |
//! +--------------------+------------------------+
//! ```
//!
//! [`LengthDelimited`] turns a byte *stream* back into discrete frames. The length
//! field is **capped** ([`max_frame_len`](LengthDelimited::max_frame_len)): a frame
//! whose prefix advertises more than the cap is rejected *before* any buffer is
//! allocated for it, so an attacker-controlled length can never trigger an unbounded
//! allocation. Decoding is total — arbitrary, malformed, or truncated input yields an
//! error or "need more bytes", never a panic.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::CodecError;

/// The width of the length prefix, in bytes (a big-endian `u32`).
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// The default maximum frame payload length (4 MiB) — a safe-by-default cap.
pub const DEFAULT_MAX_FRAME_LEN: usize = 4 * 1024 * 1024;

/// A length-prefix framer with a payload-size cap.
///
/// Cheap to copy; construct with [`LengthDelimited::new`] (or [`Default`], which uses
/// [`DEFAULT_MAX_FRAME_LEN`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LengthDelimited {
    max_frame_len: usize,
}

impl Default for LengthDelimited {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_LEN)
    }
}

impl LengthDelimited {
    /// Create a framer that caps frame payloads at `max_frame_len` bytes.
    #[must_use]
    pub const fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }

    /// The maximum payload length this framer will encode or decode.
    #[must_use]
    pub const fn max_frame_len(&self) -> usize {
        self.max_frame_len
    }

    /// Frame `payload` into `dst` as `[len: u32 BE][payload]`.
    ///
    /// # Errors
    /// [`CodecError::FrameTooLarge`] if `payload` is longer than
    /// [`max_frame_len`](Self::max_frame_len) (or does not fit in a `u32`).
    pub fn encode(&self, payload: &[u8], dst: &mut BytesMut) -> Result<(), CodecError> {
        let len = payload.len();
        if len > self.max_frame_len || u32::try_from(len).is_err() {
            return Err(CodecError::FrameTooLarge {
                len,
                max: self.max_frame_len,
            });
        }
        dst.reserve(LENGTH_PREFIX_BYTES + len);
        dst.put_u32(len as u32); // big-endian
        dst.put_slice(payload);
        Ok(())
    }

    /// Try to decode one complete frame from the front of `src`, consuming its bytes.
    ///
    /// Returns `Ok(Some(frame))` with the payload, or `Ok(None)` if more bytes are
    /// needed to complete the frame (a streaming caller should read more and retry).
    ///
    /// # Errors
    /// [`CodecError::FrameTooLarge`] if the advertised length exceeds the cap — checked
    /// *before* any payload buffer is reserved.
    pub fn decode(&self, src: &mut BytesMut) -> Result<Option<Bytes>, CodecError> {
        if src.len() < LENGTH_PREFIX_BYTES {
            return Ok(None); // not enough bytes for the length prefix yet
        }

        // Peek the length without consuming — bounds already checked above.
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len > self.max_frame_len {
            return Err(CodecError::FrameTooLarge {
                len,
                max: self.max_frame_len,
            });
        }

        // `src.len() >= LENGTH_PREFIX_BYTES` (checked above), so the subtraction can't
        // underflow — and comparing this way avoids ever adding to the attacker's
        // `len` (which could overflow `usize` on a 32-bit target with a huge cap).
        if src.len() - LENGTH_PREFIX_BYTES < len {
            // The full payload has not arrived. Do not reserve based on `len`: the
            // buffer simply grows as the transport appends more bytes.
            return Ok(None);
        }

        src.advance(LENGTH_PREFIX_BYTES);
        Ok(Some(src.split_to(len).freeze()))
    }

    /// Like [`decode`](Self::decode), but for end-of-stream: any leftover bytes that
    /// cannot form a complete frame are a truncation error rather than "need more".
    ///
    /// # Errors
    /// [`CodecError::FrameTooLarge`] as in [`decode`](Self::decode); or
    /// [`CodecError::Truncated`] if the stream ended with a partial frame.
    pub fn decode_eof(&self, src: &mut BytesMut) -> Result<Option<Bytes>, CodecError> {
        match self.decode(src)? {
            Some(frame) => Ok(Some(frame)),
            None if src.is_empty() => Ok(None), // clean end of stream
            None => Err(CodecError::Truncated { have: src.len() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frame `payload` with `codec` and return the raw wire bytes.
    fn framed(codec: &LengthDelimited, payload: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        codec.encode(payload, &mut buf).unwrap();
        buf
    }

    #[test]
    fn encode_then_decode_should_round_trip() {
        let codec = LengthDelimited::default();
        let mut buf = framed(&codec, b"hello world");
        // A 4-byte prefix + 11-byte payload.
        assert_eq!(buf.len(), LENGTH_PREFIX_BYTES + 11);

        let frame = codec.decode(&mut buf).unwrap();
        assert_eq!(frame.as_deref(), Some(&b"hello world"[..]));
        assert!(buf.is_empty(), "the frame's bytes were fully consumed");
    }

    #[test]
    fn empty_payload_should_round_trip() {
        let codec = LengthDelimited::default();
        let mut buf = framed(&codec, b"");
        assert_eq!(buf.len(), LENGTH_PREFIX_BYTES);
        let frame = codec.decode(&mut buf).unwrap();
        assert_eq!(frame.as_deref(), Some(&b""[..]));
    }

    #[test]
    fn decode_should_return_none_when_prefix_incomplete() {
        let codec = LengthDelimited::default();
        let mut buf = BytesMut::from(&[0x00, 0x00][..]); // only 2 of 4 prefix bytes
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), 2, "nothing consumed while waiting");
    }

    #[test]
    fn decode_should_return_none_when_payload_incomplete() {
        let codec = LengthDelimited::default();
        let mut buf = BytesMut::new();
        buf.put_u32(10); // advertises 10 payload bytes...
        buf.put_slice(b"abc"); // ...but only 3 have arrived
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), LENGTH_PREFIX_BYTES + 3, "nothing consumed");
    }

    #[test]
    fn decode_should_error_when_length_exceeds_cap() {
        let codec = LengthDelimited::new(16);
        let mut buf = BytesMut::new();
        buf.put_u32(1_000_000); // way over the 16-byte cap
        buf.put_slice(b"only a few bytes follow");

        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            CodecError::FrameTooLarge {
                len: 1_000_000,
                max: 16
            }
        ));
    }

    #[test]
    fn encode_should_error_when_payload_exceeds_cap() {
        let codec = LengthDelimited::new(4);
        let mut dst = BytesMut::new();
        let err = codec.encode(b"too long", &mut dst).unwrap_err();
        assert!(matches!(err, CodecError::FrameTooLarge { len: 8, max: 4 }));
        assert!(dst.is_empty(), "nothing written on error");
    }

    #[test]
    fn cap_boundary_should_accept_max_and_reject_max_plus_one() {
        let codec = LengthDelimited::new(8);

        // Exactly the cap round-trips, on both encode and decode.
        let mut buf = BytesMut::new();
        codec.encode(&[0xAB; 8], &mut buf).unwrap();
        assert_eq!(
            codec.decode(&mut buf).unwrap().as_deref(),
            Some(&[0xAB; 8][..])
        );

        // One byte over the cap: encode errors.
        let err = codec.encode(&[0u8; 9], &mut BytesMut::new()).unwrap_err();
        assert!(matches!(err, CodecError::FrameTooLarge { len: 9, max: 8 }));

        // A prefix advertising exactly the cap is accepted...
        let mut at_cap = BytesMut::new();
        at_cap.put_u32(8);
        at_cap.put_slice(&[0u8; 8]);
        assert!(codec.decode(&mut at_cap).unwrap().is_some());

        // ...but one over the cap errors before allocating a payload buffer.
        let mut over_cap = BytesMut::new();
        over_cap.put_u32(9);
        let err = codec.decode(&mut over_cap).unwrap_err();
        assert!(matches!(err, CodecError::FrameTooLarge { len: 9, max: 8 }));
    }

    #[test]
    fn decode_eof_should_error_on_truncated_frame() {
        let codec = LengthDelimited::default();
        let mut buf = BytesMut::new();
        buf.put_u32(10);
        buf.put_slice(b"abc"); // partial frame at EOF

        let err = codec.decode_eof(&mut buf).unwrap_err();
        assert!(matches!(err, CodecError::Truncated { have: 7 }));
    }

    #[test]
    fn decode_eof_should_return_none_on_clean_end() {
        let codec = LengthDelimited::default();
        let mut buf = BytesMut::new();
        assert_eq!(codec.decode_eof(&mut buf).unwrap(), None);
    }

    #[test]
    fn decode_should_extract_multiple_frames() {
        let codec = LengthDelimited::default();
        let mut buf = BytesMut::new();
        codec.encode(b"one", &mut buf).unwrap();
        codec.encode(b"two", &mut buf).unwrap();
        codec.encode(b"three", &mut buf).unwrap();

        assert_eq!(
            codec.decode(&mut buf).unwrap().as_deref(),
            Some(&b"one"[..])
        );
        assert_eq!(
            codec.decode(&mut buf).unwrap().as_deref(),
            Some(&b"two"[..])
        );
        assert_eq!(
            codec.decode(&mut buf).unwrap().as_deref(),
            Some(&b"three"[..])
        );
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn decode_should_reassemble_frame_split_across_reads() {
        let codec = LengthDelimited::default();
        let whole = framed(&codec, b"reassemble me");

        // Feed the wire bytes one at a time; only the last byte completes the frame.
        let mut buf = BytesMut::new();
        for (i, byte) in whole.iter().enumerate() {
            buf.put_u8(*byte);
            let decoded = codec.decode(&mut buf).unwrap();
            if i + 1 < whole.len() {
                assert_eq!(
                    decoded,
                    None,
                    "incomplete after {} of {} bytes",
                    i + 1,
                    whole.len()
                );
            } else {
                assert_eq!(decoded.as_deref(), Some(&b"reassemble me"[..]));
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn roundtrip_any_payload_within_cap(payload: Vec<u8>) {
            let codec = LengthDelimited::default();
            let mut buf = BytesMut::new();
            codec.encode(&payload, &mut buf).unwrap();
            let frame = codec.decode(&mut buf).unwrap();
            proptest::prop_assert_eq!(frame.as_deref(), Some(&payload[..]));
            proptest::prop_assert!(buf.is_empty());
        }

        /// Decoding arbitrary bytes must never panic (the on-box proxy for the fuzz
        /// target). A small cap makes over-cap lengths — and thus the error path —
        /// common in the generated inputs.
        #[test]
        fn decode_arbitrary_bytes_should_never_panic(data: Vec<u8>) {
            let codec = LengthDelimited::new(64);
            let mut buf = BytesMut::from(&data[..]);
            while let Ok(Some(_)) = codec.decode(&mut buf) {}
            let _ = codec.decode_eof(&mut buf);
        }

        /// Valid frames extract intact and in order even with trailing garbage, and
        /// decoding the garbage never panics — this exercises the extract path
        /// (`advance` / `split_to`) that random bytes almost never reach (a random
        /// 4-byte prefix is over-cap and errors immediately).
        #[test]
        fn decode_should_extract_valid_frames_before_garbage(
            payloads in proptest::collection::vec(
                proptest::collection::vec(proptest::prelude::any::<u8>(), 0..40),
                0..6,
            ),
            garbage in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..40),
        ) {
            let codec = LengthDelimited::new(64);
            let mut buf = BytesMut::new();
            for payload in &payloads {
                codec.encode(payload, &mut buf).unwrap();
            }
            buf.extend_from_slice(&garbage);

            // The leading valid frames come out intact, in order.
            for payload in &payloads {
                let frame = codec.decode(&mut buf).unwrap();
                proptest::prop_assert_eq!(frame.as_deref(), Some(&payload[..]));
            }
            // Whatever garbage remains resolves without panicking.
            while let Ok(Some(_)) = codec.decode(&mut buf) {}
            let _ = codec.decode_eof(&mut buf);
        }
    }
}
