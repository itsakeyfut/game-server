//! Integration test for `#[derive(Packet)]` — exercised as an external crate would use
//! it (via `gsf-codec`, which defines the `Packet` trait and re-exports the derive).

use gsf_codec::{ChannelKind, Codec, Packet, PacketId, PostcardCodec, Visibility};
use serde::{Deserialize, Serialize};

/// A packet declared via the derive macro.
#[derive(Debug, PartialEq, Serialize, Deserialize, Packet)]
#[packet(id = 7, channel = ReliableOrdered, visibility = Internal)]
struct PlayCard {
    card: u8,
}

/// A second packet, exercising other channel / visibility variants.
#[derive(Debug, PartialEq, Serialize, Deserialize, Packet)]
#[packet(id = 100, channel = Unreliable, visibility = Public)]
struct Position {
    x: f32,
    y: f32,
}

/// An `enum` packet, exercising a non-struct target and the `ReliableUnordered` variant.
#[derive(Debug, PartialEq, Serialize, Deserialize, Packet)]
#[packet(id = 200, channel = ReliableUnordered, visibility = Internal)]
enum Event {
    Joined { who: u64 },
    Left(u64),
    Tick,
}

/// The same metadata as `PlayCard`, but written by hand (the explicit API — no macro).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct PlayCardManual {
    card: u8,
}

impl Packet for PlayCardManual {
    const ID: PacketId = PacketId(7);
    const CHANNEL: ChannelKind = ChannelKind::ReliableOrdered;
    const VISIBILITY: Visibility = Visibility::Internal;
}

#[test]
fn derived_packet_should_expose_declared_metadata() {
    assert_eq!(PlayCard::ID, PacketId(7));
    assert_eq!(PlayCard::CHANNEL, ChannelKind::ReliableOrdered);
    assert_eq!(PlayCard::VISIBILITY, Visibility::Internal);

    assert_eq!(Position::ID, PacketId(100));
    assert_eq!(Position::CHANNEL, ChannelKind::Unreliable);
    assert_eq!(Position::VISIBILITY, Visibility::Public);
}

#[test]
fn derived_and_hand_written_packets_should_be_equivalent() {
    assert_eq!(PlayCard::ID, PlayCardManual::ID);
    assert_eq!(PlayCard::CHANNEL, PlayCardManual::CHANNEL);
    assert_eq!(PlayCard::VISIBILITY, PlayCardManual::VISIBILITY);
}

#[test]
fn derived_packet_should_still_be_a_serde_message() {
    // `Packet: Serialize + DeserializeOwned`, so a packet round-trips through a codec.
    let codec = PostcardCodec;
    let card = PlayCard { card: 42 };
    let bytes = codec.encode(&card).unwrap();
    let back: PlayCard = codec.decode(&bytes).unwrap();
    assert_eq!(back, card);
}

#[test]
fn derived_enum_packet_should_expose_metadata_and_round_trip() {
    assert_eq!(Event::ID, PacketId(200));
    assert_eq!(Event::CHANNEL, ChannelKind::ReliableUnordered);
    assert_eq!(Event::VISIBILITY, Visibility::Internal);

    let codec = PostcardCodec;
    let event = Event::Left(9);
    let bytes = codec.encode(&event).unwrap();
    let back: Event = codec.decode(&bytes).unwrap();
    assert_eq!(back, event);
}
