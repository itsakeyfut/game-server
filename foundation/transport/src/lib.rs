//! Reliability-channel transport abstraction (TCP / WebSocket / QUIC / custom reliable-UDP / WebTransport).
//!
//! The transport hides *how* bytes move but is **honest about reliability**: every
//! [`Connection`] advertises the [`ChannelKind`]s it can actually deliver via
//! [`ChannelCapabilities`], and [`Connection::open_channel`] returns
//! [`TransportError::UnsupportedChannel`] for anything else rather than silently
//! degrading (e.g. handing back a lossy channel where an ordered one was asked for).
//!
//! - [`ChannelKind`] / [`ChannelCapabilities`] — the reliability taxonomy and the set
//!   a transport supports.
//! - [`Connection`] / [`Listener`] — the cold-path (setup / teardown) trait surface.
//! - [`Channel`] — the concrete, per-message hot path (no dynamic dispatch).
//! - [`memory_transport`] — an in-memory loopback reference implementation for tests.
//! - [`tcp`] — the TCP byte-transport (one reliable-ordered channel per connection),
//!   TLS-encrypted by default (`bind` / `connect`); plaintext is an explicit opt-out.
//! - [`ws`] — the WebSocket byte-transport, `wss://` (TLS) by default (`bind` / `connect`)
//!   with a plaintext `ws://` opt-out; further backends (QUIC, …) arrive in later issues.
//!
//! **Encrypted by default** (security §1.1): each socket backend's `bind` / `connect` take
//! a rustls config and encrypt; the loud `bind_plaintext` / `connect_plaintext` are the
//! only way to run unencrypted.
//!
//! Convention: the in-memory reference transport is exposed at the crate root as the
//! default, while each concrete wire backend lives in its own module ([`tcp`], [`ws`],
//! and future `quic`).
//!
//! ```
//! use bytes::Bytes;
//! use gsf_transport::{memory_transport, ChannelCapabilities, ChannelKind, Connection, Listener};
//!
//! # #[tokio::main]
//! # async fn main() {
//! // A transport that honestly provides only a reliable, ordered channel.
//! let caps = ChannelCapabilities::none().with(ChannelKind::ReliableOrdered);
//! let (mut listener, connector) = memory_transport(caps);
//!
//! let client = connector.connect().await.unwrap();
//! let server = listener.accept().await.unwrap();
//!
//! let client_ch = client.open_channel(ChannelKind::ReliableOrdered).await.unwrap();
//! let mut server_ch = server.open_channel(ChannelKind::ReliableOrdered).await.unwrap();
//!
//! client_ch.send(Bytes::from_static(b"ping")).await.unwrap();
//! assert_eq!(server_ch.recv().await, Some(Bytes::from_static(b"ping")));
//!
//! // Asking for a channel the transport never advertised is an error, not a downgrade.
//! assert!(client.open_channel(ChannelKind::Unreliable).await.is_err());
//! # }
//! ```
#![forbid(unsafe_code)]

mod channel;
mod connection;
mod error;
mod memory;
pub mod tcp;
mod tls;
pub mod ws;

pub use channel::{BackpressurePolicy, Channel, ChannelCapabilities, ChannelKind};
pub use connection::{Connection, Listener};
pub use error::TransportError;
pub use memory::{MemoryConnection, MemoryConnector, MemoryListener, memory_transport};

/// Re-export so callers build the TLS configs used by [`ws`] backends against the
/// exact rustls version this crate depends on.
pub use rustls;
