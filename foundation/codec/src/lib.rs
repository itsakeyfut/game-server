//! Serialization / framing / message registry / versioning.
//!
//! Two layers so far:
//! - [`LengthDelimited`] **framing** — a capped `u32`-length-prefixed wire format that
//!   turns a byte stream into discrete frames and rejects attacker-controlled oversized
//!   lengths before allocating.
//! - [`Codec`] **serialization** — bytes ↔ typed messages, with [`PostcardCodec`] as the
//!   internal default (swappable at compile time).
//!
//! The derive macro, the message registry, and the tagged public format arrive in
//! later issues.
#![forbid(unsafe_code)]

mod error;
mod frame;
mod serialize;

pub use error::CodecError;
pub use frame::{DEFAULT_MAX_FRAME_LEN, LENGTH_PREFIX_BYTES, LengthDelimited};
pub use serialize::{Codec, PostcardCodec};
