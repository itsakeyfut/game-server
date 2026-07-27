//! Errors produced by the codec.

/// An error from framing or parsing wire bytes.
///
/// `#[non_exhaustive]` because later codec layers (postcard, the tagged public
/// format, versioning) will contribute their own failure modes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodecError {
    /// A frame's length prefix exceeds the configured maximum. The frame is rejected
    /// *before* any buffer is allocated for it, so an attacker-controlled length
    /// cannot drive an unbounded allocation.
    #[error("frame length {len} exceeds the maximum {max}")]
    FrameTooLarge {
        /// The length the prefix advertised.
        len: usize,
        /// The configured maximum frame length.
        max: usize,
    },

    /// The stream ended with a partial frame (fewer bytes than the length prefix
    /// advertised, or a partial prefix).
    #[error("stream ended mid-frame: {have} bytes remaining")]
    Truncated {
        /// The number of leftover bytes that could not form a complete frame.
        have: usize,
    },

    /// Serializing a message failed. The source is kept codec-agnostic (boxed) so
    /// `CodecError` does not couple to any one serialization format.
    #[error("failed to encode message: {0}")]
    Encode(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Deserializing a message failed — malformed or truncated bytes. Deserialization
    /// returns this error rather than panicking, even on adversarial input.
    #[error("failed to decode message: {0}")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// A message decoded successfully but bytes remained afterward. A frame must hold
    /// exactly one message, so leftover bytes are rejected (they would otherwise let
    /// two distinct byte strings decode to the same message).
    #[error("{remaining} trailing bytes after the decoded message")]
    TrailingBytes {
        /// The number of unconsumed bytes after the message.
        remaining: usize,
    },
}
