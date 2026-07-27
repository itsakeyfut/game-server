//! Serialization / framing / message registry / versioning.
//!
//! - [`LengthDelimited`] **framing** — a capped `u32`-length-prefixed wire format that
//!   turns a byte stream into discrete frames and rejects attacker-controlled oversized
//!   lengths before allocating.
//! - [`Codec`] **serialization** — bytes ↔ typed messages, with [`PostcardCodec`] as the
//!   internal default (swappable at compile time).
//! - [`Packet`](trait@Packet) + `#[derive(Packet)]` — a message type declaring its wire
//!   metadata (id / channel / visibility).
//!
//! The message registry and the tagged public format arrive in later issues.
#![forbid(unsafe_code)]

mod error;
mod frame;
mod packet;
mod serialize;

pub use error::CodecError;
pub use frame::{DEFAULT_MAX_FRAME_LEN, LENGTH_PREFIX_BYTES, LengthDelimited};
pub use packet::{Packet, PacketId, Visibility};
pub use serialize::{Codec, PostcardCodec};

/// Re-exported from `gsf-transport` so a packet's channel is named from one crate.
pub use gsf_transport::ChannelKind;

/// The `#[derive(Packet)]` macro. Shares the name with the [`Packet`](trait@Packet)
/// trait (trait vs. macro namespace), the way `serde::Serialize` does.
pub use gsf_macros::Packet;
