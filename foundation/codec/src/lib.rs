//! Serialization / framing / message registry / versioning.
//!
//! The first layer is [`LengthDelimited`] framing: a capped `u32`-length-prefixed
//! wire format that turns a byte stream into discrete frames and rejects
//! attacker-controlled oversized lengths before allocating. Serialization (postcard),
//! the derive macro, the message registry, and the tagged public format arrive in
//! later issues.
#![forbid(unsafe_code)]

mod error;
mod frame;

pub use error::CodecError;
pub use frame::{DEFAULT_MAX_FRAME_LEN, LENGTH_PREFIX_BYTES, LengthDelimited};
