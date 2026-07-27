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
}
