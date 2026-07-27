//! Errors produced by the transport abstraction.

use crate::channel::ChannelKind;

/// An error from opening a channel, accepting a connection, or moving bytes.
///
/// `#[non_exhaustive]` because concrete transports (TCP / WebSocket / QUIC, added
/// in later issues) will contribute backend-specific failure modes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// A [`ChannelKind`] was requested that the connection does not advertise.
    ///
    /// Reliability semantics are never silently downgraded: asking for a channel a
    /// transport cannot honestly provide is an error, not a best-effort substitute.
    #[error("channel kind {0:?} is not supported by this transport")]
    UnsupportedChannel(ChannelKind),

    /// A [`ChannelKind`] was opened that is already open on this connection.
    #[error("channel kind {0:?} is already open")]
    ChannelAlreadyOpen(ChannelKind),

    /// The connection or listener has been closed (peer gone, or a local `close`).
    #[error("connection closed")]
    ConnectionClosed,

    /// An underlying I/O error from a byte-transport backend.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A protocol- or handshake-level failure from a backend (e.g. the WebSocket or
    /// TLS handshake, or a malformed control frame).
    #[error("transport protocol error: {0}")]
    Protocol(String),
}
