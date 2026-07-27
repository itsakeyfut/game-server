//! TCP byte-transport: a single reliable-ordered channel per connection.
//!
//! TCP is a reliable, ordered byte stream, so a [`TcpConnection`] advertises exactly
//! [`ChannelKind::ReliableOrdered`] and hands out one [`Channel`] carrying the raw
//! bytes in order. It does **not** frame: turning that stream back into discrete
//! messages (length-prefix framing) is the codec's job, so a single
//! [`Channel::recv`] is not guaranteed to match a single peer send.
//!
//! Opening the channel splits the socket and spawns a reader and a writer task that
//! bridge it to the channel's bounded mpsc queues. The tasks are tied to the
//! channel's lifetime: dropping the [`Channel`] (or [`Connection::close`]) tears
//! them down, and a dropped [`TcpConnection`] leaves an open channel working.
//!
//! ```no_run
//! use bytes::Bytes;
//! use gsf_transport::{ChannelKind, Connection, Listener, tcp};
//!
//! # async fn run() -> Result<(), gsf_transport::TransportError> {
//! let mut listener = tcp::TcpListener::bind("127.0.0.1:4000").await?;
//! let client = tcp::connect("127.0.0.1:4000").await?;
//! let server = listener.accept().await?;
//!
//! let client_ch = client.open_channel(ChannelKind::ReliableOrdered).await?;
//! let mut server_ch = server.open_channel(ChannelKind::ReliableOrdered).await?;
//! client_ch.send(Bytes::from_static(b"hello")).await?;
//! let _bytes = server_ch.recv().await;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::channel::{Channel, ChannelCapabilities, ChannelKind};
use crate::connection::{Connection, Listener};
use crate::error::TransportError;

/// Per-read buffer size; also the largest chunk a single read yields. Bounds
/// per-read allocation regardless of peer behaviour (a resource cap).
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Bound on each direction's in-flight queue (backpressure seam, tuned in #39).
const CHANNEL_CAPACITY: usize = 64;

/// The channels a TCP connection can provide: reliable-ordered only.
const TCP_CAPS: ChannelCapabilities =
    ChannelCapabilities::none().with(ChannelKind::ReliableOrdered);

/// A TCP [`Connection`]: one reliable-ordered [`Channel`] over a `TcpStream`.
pub struct TcpConnection {
    peer: SocketAddr,
    state: Mutex<ConnState>,
}

/// Mutable connection state behind an async mutex.
struct ConnState {
    /// The socket, taken on the first `open_channel`; `None` once opened or closed.
    stream: Option<TcpStream>,
    /// Set by [`Connection::close`]; makes further opens fail with `ConnectionClosed`.
    closed: bool,
    /// Reader + writer task handles, aborted on `close` for eager teardown.
    tasks: Vec<JoinHandle<()>>,
}

impl TcpConnection {
    fn new(stream: TcpStream, peer: SocketAddr) -> Self {
        Self {
            peer,
            state: Mutex::new(ConnState {
                stream: Some(stream),
                closed: false,
                tasks: Vec::new(),
            }),
        }
    }

    /// The address of the peer this connection is talking to.
    #[must_use]
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

#[async_trait]
impl Connection for TcpConnection {
    fn capabilities(&self) -> ChannelCapabilities {
        TCP_CAPS
    }

    async fn open_channel(&self, kind: ChannelKind) -> Result<Channel, TransportError> {
        if !TCP_CAPS.supports(kind) {
            return Err(TransportError::UnsupportedChannel(kind));
        }
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(TransportError::ConnectionClosed);
        }
        let stream = state
            .stream
            .take()
            .ok_or(TransportError::ChannelAlreadyOpen(kind))?;

        let (read_half, write_half) = stream.into_split();
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (in_tx, in_rx) = mpsc::channel(CHANNEL_CAPACITY);

        state.tasks = vec![
            tokio::spawn(reader_loop(read_half, in_tx)),
            tokio::spawn(writer_loop(write_half, out_rx)),
        ];

        Ok(Channel::new(ChannelKind::ReliableOrdered, out_tx, in_rx))
    }

    async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        state.stream = None;
        // Eager teardown: aborting the tasks drops the socket halves (peer sees a
        // disconnect) and the channel's mpsc ends (local `send`/`recv` see closure).
        for task in state.tasks.drain(..) {
            task.abort();
        }
    }
}

impl fmt::Debug for TcpConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpConnection")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

/// Pump bytes from the socket into the channel's inbound queue until the peer
/// closes, the socket errors, or the [`Channel`] (holding the receiver) is dropped.
async fn reader_loop(mut read_half: OwnedReadHalf, in_tx: mpsc::Sender<Bytes>) {
    let mut buf = vec![0u8; READ_CHUNK_BYTES];
    loop {
        tokio::select! {
            // The Channel's receiver was dropped — no one is listening; stop reading.
            () = in_tx.closed() => break,
            result = read_half.read(&mut buf) => match result {
                Ok(0) => break,                       // peer half-closed (EOF)
                Ok(n) => {
                    if in_tx.send(Bytes::copy_from_slice(&buf[..n])).await.is_err() {
                        break;                        // receiver gone
                    }
                }
                Err(_) => break,                      // reset / I/O error — never panics
            },
        }
    }
}

/// Pump bytes from the channel's outbound queue to the socket until the [`Channel`]
/// (holding the sender) is dropped or a write fails.
async fn writer_loop(mut write_half: OwnedWriteHalf, mut out_rx: mpsc::Receiver<Bytes>) {
    while let Some(bytes) = out_rx.recv().await {
        if write_half.write_all(&bytes).await.is_err() {
            break;
        }
    }
}

/// A TCP [`Listener`] that yields a [`TcpConnection`] per accepted socket.
pub struct TcpListener {
    inner: TokioTcpListener,
}

impl TcpListener {
    /// Bind and start listening on `addr`.
    ///
    /// # Errors
    /// [`TransportError::Io`] if the socket cannot be bound.
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self, TransportError> {
        let inner = TokioTcpListener::bind(addr).await?;
        Ok(Self { inner })
    }

    /// The local address the listener is bound to.
    ///
    /// # Errors
    /// [`TransportError::Io`] if the address cannot be resolved.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.inner.local_addr()?)
    }
}

#[async_trait]
impl Listener for TcpListener {
    async fn accept(&mut self) -> Result<Box<dyn Connection>, TransportError> {
        let (stream, peer) = self.inner.accept().await?;
        stream.set_nodelay(true)?;
        Ok(Box::new(TcpConnection::new(stream, peer)))
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("local_addr", &self.inner.local_addr().ok())
            .finish_non_exhaustive()
    }
}

/// Dial `addr` and return the client-side [`TcpConnection`].
///
/// # Errors
/// [`TransportError::Io`] if the connection cannot be established.
pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<TcpConnection, TransportError> {
    let stream = TcpStream::connect(addr).await?;
    let peer = stream.peer_addr()?;
    stream.set_nodelay(true)?;
    Ok(TcpConnection::new(stream, peer))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    /// Guards every `recv`-await so a delivery/lifecycle regression fails fast
    /// instead of hanging the test (and CI) indefinitely.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Bind a listener on an ephemeral loopback port and dial it, returning the
    /// accepted (server) and dialing (client) connections.
    async fn connected_pair() -> (Box<dyn Connection>, TcpConnection) {
        let mut listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (server, client) = tokio::join!(listener.accept(), connect(addr));
        (server.unwrap(), client.unwrap())
    }

    /// Read from `ch` until `len` bytes have arrived, tolerating TCP re-chunking.
    async fn recv_exact(ch: &mut Channel, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let next = timeout(TEST_TIMEOUT, ch.recv())
                .await
                .expect("timed out waiting for bytes");
            match next {
                Some(bytes) => out.extend_from_slice(&bytes),
                None => break,
            }
        }
        out
    }

    /// Assert the channel observes closure (`recv` → `None`) within the timeout.
    async fn expect_closed(ch: &mut Channel) {
        let result = timeout(TEST_TIMEOUT, ch.recv())
            .await
            .expect("timed out waiting for close");
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn tcp_capabilities_should_be_reliable_ordered_only() {
        let (server, client) = connected_pair().await;
        let expected = ChannelCapabilities::none().with(ChannelKind::ReliableOrdered);
        assert_eq!(client.capabilities(), expected);
        assert_eq!(server.capabilities(), expected);
    }

    #[tokio::test]
    async fn tcp_channel_should_round_trip_reliable_ordered() {
        let (server, client) = connected_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client_ch.send(Bytes::from_static(b"ping")).await.unwrap();
        assert_eq!(recv_exact(&mut server_ch, 4).await, b"ping");

        server_ch.send(Bytes::from_static(b"pong")).await.unwrap();
        // client_ch must be mut to receive.
        let mut client_ch = client_ch;
        assert_eq!(recv_exact(&mut client_ch, 4).await, b"pong");
    }

    #[tokio::test]
    async fn tcp_recv_should_return_none_when_peer_disconnects() {
        let (server, client) = connected_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        drop(client_ch);
        expect_closed(&mut server_ch).await;
    }

    #[tokio::test]
    async fn tcp_channel_should_reassemble_message_larger_than_read_chunk() {
        let (server, client) = connected_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // Larger than one read buffer, so the reader must reassemble across reads;
        // index-derived bytes let us assert exact order and completeness.
        let payload: Vec<u8> = (0..READ_CHUNK_BYTES * 4).map(|i| i as u8).collect();
        client_ch.send(Bytes::from(payload.clone())).await.unwrap();

        assert_eq!(recv_exact(&mut server_ch, payload.len()).await, payload);
    }

    #[tokio::test]
    async fn tcp_channel_should_carry_both_directions_concurrently() {
        let (server, client) = connected_pair().await;
        let mut client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // Both peers send before either receives — exercises the independent
        // per-direction reader/writer tasks at once.
        client_ch.send(Bytes::from_static(b"c2s")).await.unwrap();
        server_ch.send(Bytes::from_static(b"s2c")).await.unwrap();

        assert_eq!(recv_exact(&mut server_ch, 3).await, b"c2s");
        assert_eq!(recv_exact(&mut client_ch, 3).await, b"s2c");
    }

    #[tokio::test]
    async fn tcp_open_channel_should_error_for_unsupported_kind() {
        let (_server, client) = connected_pair().await;
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
    async fn tcp_open_channel_twice_should_error() {
        let (_server, client) = connected_pair().await;
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
    async fn tcp_should_deliver_arbitrary_bytes_without_panic_on_abrupt_close() {
        let mut listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Raw peer that writes arbitrary bytes then hard-closes, bypassing our transport.
        let raw = tokio::spawn(async move {
            let mut sock = TcpStream::connect(addr).await.unwrap();
            sock.write_all(&[0xFF, 0x00, 0x99, 0x42, 0x13])
                .await
                .unwrap();
            // Dropping `sock` closes the connection.
        });
        let server = listener.accept().await.unwrap();
        raw.await.unwrap();

        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // The bytes arrive verbatim (no parsing, no panic), then the channel closes.
        let received = recv_exact(&mut server_ch, 5).await;
        assert_eq!(received, [0xFF, 0x00, 0x99, 0x42, 0x13]);
        expect_closed(&mut server_ch).await;
    }

    #[tokio::test]
    async fn tcp_close_should_tear_down_open_channel() {
        let (server, client) = connected_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client.close().await;

        // The peer observes the teardown, and local sends fail.
        expect_closed(&mut server_ch).await;
        let err = client_ch.send(Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }
}
