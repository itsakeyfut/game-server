//! The `Connection` and `Listener` traits: the cold-path transport surface.

use async_trait::async_trait;

use crate::channel::{Channel, ChannelCapabilities, ChannelKind};
use crate::error::TransportError;

/// A live connection to a peer over some byte-transport backend.
///
/// A connection advertises which [`ChannelKind`]s it can provide via
/// [`capabilities`](Connection::capabilities); [`open_channel`](Connection::open_channel)
/// yields a concrete [`Channel`] for a supported kind, or errors for an unsupported one.
///
/// This is the cold path (setup / teardown), so it is a boxed async trait. The
/// per-message hot path lives on the concrete [`Channel`] and never dispatches
/// dynamically.
#[async_trait]
pub trait Connection: Send + Sync {
    /// The channel kinds this connection can provide.
    fn capabilities(&self) -> ChannelCapabilities;

    /// Open a channel of the given `kind`.
    ///
    /// # Errors
    /// - [`TransportError::UnsupportedChannel`] if `kind` is not in
    ///   [`capabilities`](Connection::capabilities).
    /// - [`TransportError::ChannelAlreadyOpen`] if this `kind` is already open.
    /// - [`TransportError::ConnectionClosed`] if the connection has been closed.
    async fn open_channel(&self, kind: ChannelKind) -> Result<Channel, TransportError>;

    /// Close the connection.
    ///
    /// After this returns, [`open_channel`](Connection::open_channel) fails with
    /// [`TransportError::ConnectionClosed`]. The fate of channels *already* opened
    /// is implementation-defined: a socket-backed transport tears them down at once,
    /// whereas the in-memory loopback leaves them live until dropped. Callers that
    /// need deterministic teardown of an open channel should drop the [`Channel`],
    /// which the peer observes as [`Channel::recv`] returning `None`.
    async fn close(&self);
}

/// The server side of a transport: accepts incoming [`Connection`]s.
#[async_trait]
pub trait Listener: Send {
    /// Wait for the next incoming connection.
    ///
    /// # Errors
    /// [`TransportError::ConnectionClosed`] once no more connections can arrive
    /// (every peer that could connect is gone).
    async fn accept(&mut self) -> Result<Box<dyn Connection>, TransportError>;
}
