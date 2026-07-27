//! In-memory loopback transport: the reference implementation of the abstraction.
//!
//! [`memory_transport`] returns a paired [`MemoryListener`] / [`MemoryConnector`].
//! Each [`MemoryConnector::connect`] mints a connected [`MemoryConnection`] pair —
//! one side returned to the caller, the other delivered to the listener — whose
//! channels are wired directly to each other. No socket, fully deterministic; it
//! exists to exercise the [`Connection`] / [`Channel`] contract in tests before the
//! real byte-transports (TCP / WebSocket) land.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

use crate::channel::{Channel, ChannelCapabilities, ChannelKind};
use crate::connection::{Connection, Listener};
use crate::error::TransportError;

/// Per-direction queue depth for in-memory channels.
const MEMORY_CHANNEL_CAPACITY: usize = 64;

/// Depth of a listener's pending-connection backlog.
const MEMORY_ACCEPT_BACKLOG: usize = 32;

/// Create an in-memory loopback transport advertising `capabilities`.
///
/// The returned [`MemoryConnector`] can be cloned to open many connections against
/// the single [`MemoryListener`].
#[must_use]
pub fn memory_transport(capabilities: ChannelCapabilities) -> (MemoryListener, MemoryConnector) {
    let (outgoing, incoming) = mpsc::channel(MEMORY_ACCEPT_BACKLOG);
    (
        MemoryListener { incoming },
        MemoryConnector {
            capabilities,
            outgoing,
        },
    )
}

/// The mutable state of a [`MemoryConnection`], guarded by an async mutex.
struct ConnectionState {
    /// Channels pre-wired at connect time, one per supported kind, removed on open.
    channels: HashMap<ChannelKind, Channel>,
    /// Set by [`Connection::close`]; makes further opens fail with `ConnectionClosed`.
    closed: bool,
}

/// A loopback [`Connection`]. Both peers hold one; their channels are wired to
/// each other.
///
/// `close` marks the connection closed and drops any channels not yet opened.
/// Channels already handed out by [`open_channel`](Connection::open_channel) are
/// owned by the caller and stay live until dropped — at which point the peer
/// observes closure via [`Channel::recv`] returning `None`. Real byte-transports
/// (TCP, #37) tear every channel down at once when the socket closes; this
/// loopback keeps the simpler ownership model, which is sufficient to exercise
/// the abstraction.
pub struct MemoryConnection {
    capabilities: ChannelCapabilities,
    state: Mutex<ConnectionState>,
}

impl MemoryConnection {
    /// Build a connected pair of loopback connections that both advertise `capabilities`.
    fn pair(capabilities: ChannelCapabilities) -> (MemoryConnection, MemoryConnection) {
        let mut client = HashMap::new();
        let mut server = HashMap::new();
        for kind in capabilities.kinds() {
            let (a, b) = Channel::pair(kind, MEMORY_CHANNEL_CAPACITY);
            client.insert(kind, a);
            server.insert(kind, b);
        }
        (
            MemoryConnection {
                capabilities,
                state: Mutex::new(ConnectionState {
                    channels: client,
                    closed: false,
                }),
            },
            MemoryConnection {
                capabilities,
                state: Mutex::new(ConnectionState {
                    channels: server,
                    closed: false,
                }),
            },
        )
    }
}

#[async_trait]
impl Connection for MemoryConnection {
    fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities
    }

    async fn open_channel(&self, kind: ChannelKind) -> Result<Channel, TransportError> {
        if !self.capabilities.supports(kind) {
            return Err(TransportError::UnsupportedChannel(kind));
        }
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(TransportError::ConnectionClosed);
        }
        state
            .channels
            .remove(&kind)
            .ok_or(TransportError::ChannelAlreadyOpen(kind))
    }

    async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        state.channels.clear();
    }
}

impl fmt::Debug for MemoryConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryConnection")
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

/// The server side of the in-memory transport; yields loopback connections from
/// the paired [`MemoryConnector`].
pub struct MemoryListener {
    incoming: mpsc::Receiver<MemoryConnection>,
}

#[async_trait]
impl Listener for MemoryListener {
    async fn accept(&mut self) -> Result<Box<dyn Connection>, TransportError> {
        match self.incoming.recv().await {
            Some(conn) => Ok(Box::new(conn)),
            None => Err(TransportError::ConnectionClosed),
        }
    }
}

impl fmt::Debug for MemoryListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryListener").finish_non_exhaustive()
    }
}

/// The client side of the in-memory transport. Each [`connect`](Self::connect)
/// mints a loopback connection whose peer is queued on the paired listener.
#[derive(Clone)]
pub struct MemoryConnector {
    capabilities: ChannelCapabilities,
    outgoing: mpsc::Sender<MemoryConnection>,
}

impl MemoryConnector {
    /// The channel kinds connections from this connector will advertise.
    #[must_use]
    pub fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities
    }

    /// Open a new loopback connection; its peer is delivered to the paired listener.
    ///
    /// # Errors
    /// [`TransportError::ConnectionClosed`] if the paired listener has been dropped.
    pub async fn connect(&self) -> Result<MemoryConnection, TransportError> {
        let (client, server) = MemoryConnection::pair(self.capabilities);
        self.outgoing
            .send(server)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        Ok(client)
    }
}

impl fmt::Debug for MemoryConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryConnector")
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn ordered_caps() -> ChannelCapabilities {
        ChannelCapabilities::none().with(ChannelKind::ReliableOrdered)
    }

    #[tokio::test]
    async fn channel_should_round_trip_reliable_ordered() {
        let (mut listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();
        let server = listener.accept().await.unwrap();

        let mut client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client_ch.send(Bytes::from_static(b"ping")).await.unwrap();
        assert_eq!(server_ch.recv().await, Some(Bytes::from_static(b"ping")));

        server_ch.send(Bytes::from_static(b"pong")).await.unwrap();
        assert_eq!(client_ch.recv().await, Some(Bytes::from_static(b"pong")));
    }

    #[tokio::test]
    async fn open_channel_should_support_multiple_kinds_independently() {
        let caps = ChannelCapabilities::none()
            .with(ChannelKind::ReliableOrdered)
            .with(ChannelKind::Unreliable);
        let (mut listener, connector) = memory_transport(caps);
        let client = connector.connect().await.unwrap();
        let server = listener.accept().await.unwrap();
        assert_eq!(client.capabilities(), caps);

        let mut ordered = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let unreliable = client.open_channel(ChannelKind::Unreliable).await.unwrap();
        let server_ordered = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_unreliable = server.open_channel(ChannelKind::Unreliable).await.unwrap();

        // Each kind is an independent pipe: traffic on one never crosses to the other.
        server_ordered
            .send(Bytes::from_static(b"ord"))
            .await
            .unwrap();
        unreliable.send(Bytes::from_static(b"unr")).await.unwrap();

        assert_eq!(ordered.recv().await, Some(Bytes::from_static(b"ord")));
        assert_eq!(
            server_unreliable.recv().await,
            Some(Bytes::from_static(b"unr"))
        );
        assert_eq!(ordered.kind(), ChannelKind::ReliableOrdered);
        assert_eq!(unreliable.kind(), ChannelKind::Unreliable);
    }

    #[tokio::test]
    async fn recv_should_return_none_when_peer_channel_dropped() {
        let (mut listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();
        let server = listener.accept().await.unwrap();

        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        drop(client_ch);
        assert_eq!(server_ch.recv().await, None);
    }

    #[tokio::test]
    async fn open_channel_should_error_for_unsupported_kind() {
        let (_listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();

        let err = client
            .open_channel(ChannelKind::Unreliable)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            TransportError::UnsupportedChannel(ChannelKind::Unreliable)
        ));
    }

    #[tokio::test]
    async fn open_channel_twice_should_error() {
        let (_listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();

        let _first = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let err = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            TransportError::ChannelAlreadyOpen(ChannelKind::ReliableOrdered)
        ));
    }

    #[tokio::test]
    async fn listener_should_accept_a_connected_peer() {
        let (mut listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();
        let server = listener.accept().await.unwrap();
        assert_eq!(server.capabilities(), ordered_caps());

        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client_ch.send(Bytes::from_static(b"hi")).await.unwrap();
        assert_eq!(server_ch.recv().await, Some(Bytes::from_static(b"hi")));
    }

    #[tokio::test]
    async fn send_should_error_when_receiver_dropped() {
        let (mut listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();
        let server = listener.accept().await.unwrap();

        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        drop(server_ch);
        let err = client_ch.send(Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }

    #[tokio::test]
    async fn accept_should_error_when_all_connectors_dropped() {
        let (mut listener, connector) = memory_transport(ordered_caps());
        drop(connector);
        let result = listener.accept().await;
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn connect_should_error_when_listener_dropped() {
        let (listener, connector) = memory_transport(ordered_caps());
        drop(listener);
        let err = connector.connect().await.unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }

    #[tokio::test]
    async fn open_channel_after_close_should_error_connection_closed() {
        let (_listener, connector) = memory_transport(ordered_caps());
        let client = connector.connect().await.unwrap();

        client.close().await;
        let err = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }
}
