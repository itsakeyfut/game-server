//! The `Packet` trait: a serde message that declares its wire metadata.
//!
//! A packet is a message type that, beyond being serde-serializable, declares three
//! things — a [`PacketId`] (used by the message registry), a [`ChannelKind`] (which
//! transport reliability channel it rides), and a [`Visibility`] (internal messages
//! use the compact default codec, public ones a tagged evolvable format).
//!
//! Declare a packet with the [`Packet`](macro@crate::Packet) derive, or implement the
//! trait by hand — the two are equivalent:
//!
//! ```
//! use gsf_codec::{ChannelKind, Packet, PacketId, Visibility};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Packet)]
//! #[packet(id = 1, channel = ReliableOrdered, visibility = Internal)]
//! struct Chat {
//!     text: String,
//! }
//!
//! assert_eq!(Chat::ID, PacketId(1));
//! assert_eq!(Chat::CHANNEL, ChannelKind::ReliableOrdered);
//! assert_eq!(Chat::VISIBILITY, Visibility::Internal);
//! ```

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ChannelKind;

/// A packet's stable identifier, mapped to its type by the message registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PacketId(pub u16);

/// Where a packet is sent, which picks its wire encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    /// Same-version internal / node-to-node / short-lived messaging — the compact
    /// default codec (postcard).
    Internal,
    /// Client-facing messages that must evolve across versions — a tagged format.
    Public,
}

/// A serde message that declares its wire metadata: a [`PacketId`], a [`ChannelKind`],
/// and a [`Visibility`].
///
/// Derive it with `#[derive(Packet)]` + `#[packet(id = …, channel = …, visibility = …)]`,
/// or implement it by hand — both produce the same associated constants.
pub trait Packet: Serialize + DeserializeOwned {
    /// This packet's stable identifier.
    const ID: PacketId;
    /// The reliability channel this packet is sent on.
    const CHANNEL: ChannelKind;
    /// Whether this packet is internal or public.
    const VISIBILITY: Visibility;
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    // The "explicit API": a packet defined by implementing the trait by hand, with no
    // derive macro involved.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Move {
        x: f32,
        y: f32,
    }

    impl Packet for Move {
        const ID: PacketId = PacketId(9);
        const CHANNEL: ChannelKind = ChannelKind::UnreliableSequenced;
        const VISIBILITY: Visibility = Visibility::Public;
    }

    #[test]
    fn hand_written_packet_should_expose_its_metadata() {
        assert_eq!(Move::ID, PacketId(9));
        assert_eq!(Move::CHANNEL, ChannelKind::UnreliableSequenced);
        assert_eq!(Move::VISIBILITY, Visibility::Public);
    }

    #[test]
    fn packet_id_should_compare_by_value() {
        assert_eq!(PacketId(1), PacketId(1));
        assert_ne!(PacketId(1), PacketId(2));
    }
}
