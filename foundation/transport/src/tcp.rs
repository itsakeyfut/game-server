//! TCP byte-transport: a single reliable-ordered channel per connection, **TLS-encrypted
//! by default** with an explicit plaintext opt-out.
//!
//! TCP is a reliable, ordered byte stream, so a [`TcpConnection`] advertises exactly
//! [`ChannelKind::ReliableOrdered`] and hands out one [`Channel`] carrying the raw
//! bytes in order. It does **not** frame: turning that stream back into discrete
//! messages (length-prefix framing) is the codec's job, so a single
//! [`Channel::recv`] is not guaranteed to match a single peer send.
//!
//! **Encrypted by default** (security §1.1): [`TcpListener::bind`] and [`connect`] take a
//! rustls [`ServerConfig`] / [`ClientConfig`] and encrypt; the loud
//! [`bind_plaintext`](TcpListener::bind_plaintext) / [`connect_plaintext`] are the only way
//! to run unencrypted. As with [`ws`](crate::ws), the caller owns the PKI and must install a
//! rustls crypto provider before building a config — this workspace uses ring, so call
//! `rustls::crypto::ring::default_provider().install_default()` once at startup.
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
//! // Plaintext opt-out shown for brevity; production uses `bind` / `connect` with a
//! // rustls config (see the encryption note above).
//! let mut listener = tcp::TcpListener::bind_plaintext("127.0.0.1:4000").await?;
//! let client = tcp::connect_plaintext("127.0.0.1:4000").await?;
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
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ServerConfig};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::channel::{Channel, ChannelCapabilities, ChannelKind};
use crate::connection::{Connection, Listener};
use crate::error::TransportError;
use crate::tls::MaybeTls;

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
    /// The socket (plaintext or TLS), taken on the first `open_channel`; `None` once
    /// opened or closed.
    stream: Option<MaybeTls>,
    /// Set by [`Connection::close`]; makes further opens fail with `ConnectionClosed`.
    closed: bool,
    /// Reader + writer task handles, aborted on `close` for eager teardown.
    tasks: Vec<JoinHandle<()>>,
}

impl TcpConnection {
    fn new(stream: MaybeTls, peer: SocketAddr) -> Self {
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

        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (in_tx, in_rx) = mpsc::channel(CHANNEL_CAPACITY);

        // Plaintext keeps `TcpStream::into_split`'s lock-free owned halves; a TLS stream
        // can't be split that way, so it uses `tokio::io::split`. The reader/writer loops
        // are generic over the half type, so both paths reuse the same logic.
        state.tasks = match stream {
            MaybeTls::Plain(tcp) => {
                let (read_half, write_half) = tcp.into_split();
                vec![
                    tokio::spawn(reader_loop(read_half, in_tx)),
                    tokio::spawn(writer_loop(write_half, out_rx)),
                ]
            }
            tls => {
                let (read_half, write_half) = tokio::io::split(tls);
                vec![
                    tokio::spawn(reader_loop(read_half, in_tx)),
                    tokio::spawn(writer_loop(write_half, out_rx)),
                ]
            }
        };

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
///
/// Generic over the read half so the same loop drives a plaintext `OwnedReadHalf` and a
/// TLS `ReadHalf<MaybeTls>`.
async fn reader_loop<R: AsyncRead + Unpin + Send>(mut read_half: R, in_tx: mpsc::Sender<Bytes>) {
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
/// (holding the sender) is dropped or a write fails. Generic over the write half (plaintext
/// `OwnedWriteHalf` or TLS `WriteHalf<MaybeTls>`).
async fn writer_loop<W: AsyncWrite + Unpin + Send>(
    mut write_half: W,
    mut out_rx: mpsc::Receiver<Bytes>,
) {
    while let Some(bytes) = out_rx.recv().await {
        if write_half.write_all(&bytes).await.is_err() {
            break;
        }
        // Coalesce any already-queued messages into one flush (batches write bursts).
        while let Ok(more) = out_rx.try_recv() {
            if write_half.write_all(&more).await.is_err() {
                return;
            }
        }
        // Flush before parking: plaintext is a no-op, but a TLS stream buffers records
        // internally, so without this the last records never reach the socket.
        if write_half.flush().await.is_err() {
            break;
        }
    }
    // Graceful close (the Channel was dropped, so `recv` returned `None`): shut the write
    // half down so a TLS stream sends `close_notify` and the peer can tell a clean close
    // from an abrupt one — parity with the `ws` transport's Close frame. (An eager
    // `Connection::close` aborts this task instead, which is an abrupt close by design.)
    let _ = write_half.shutdown().await;
}

/// A TCP [`Listener`] that yields a [`TcpConnection`] per accepted socket. TLS-encrypted
/// unless created with [`bind_plaintext`](TcpListener::bind_plaintext).
pub struct TcpListener {
    inner: TokioTcpListener,
    tls: Option<TlsAcceptor>,
}

impl TcpListener {
    /// Bind a TLS listener on `addr` using the caller's rustls server config. This is the
    /// encrypted default; use [`bind_plaintext`](Self::bind_plaintext) to opt out.
    ///
    /// A rustls crypto provider must already be installed (see the module docs).
    ///
    /// # Errors
    /// [`TransportError::Io`] if the socket cannot be bound.
    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
        config: Arc<ServerConfig>,
    ) -> Result<Self, TransportError> {
        let inner = TokioTcpListener::bind(addr).await?;
        Ok(Self {
            inner,
            tls: Some(TlsAcceptor::from(config)),
        })
    }

    /// Bind an **unencrypted** plaintext listener on `addr` — the explicit opt-out from the
    /// TLS default ([`bind`](Self::bind)).
    ///
    /// # Errors
    /// [`TransportError::Io`] if the socket cannot be bound.
    pub async fn bind_plaintext<A: ToSocketAddrs>(addr: A) -> Result<Self, TransportError> {
        let inner = TokioTcpListener::bind(addr).await?;
        Ok(Self { inner, tls: None })
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
        // The TLS handshake runs inline in `accept` (like `ws`), which serializes
        // handshakes across connections; off-loading it (slow-loris hardening) is tracked
        // separately (#70). A plaintext peer fails this handshake — that is how a TLS
        // listener rejects plaintext.
        let transport = match &self.tls {
            Some(acceptor) => MaybeTls::ServerTls(Box::new(acceptor.accept(stream).await?)),
            None => MaybeTls::Plain(stream),
        };
        Ok(Box::new(TcpConnection::new(transport, peer)))
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("local_addr", &self.inner.local_addr().ok())
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

/// Dial `addr` over TLS, verifying the server against `server_name`, and return the
/// client-side [`TcpConnection`]. This is the encrypted default; use [`connect_plaintext`]
/// to opt out.
///
/// `server_name` is the SNI / certificate name to verify (a `ToSocketAddrs` address discards
/// the host, so it is passed explicitly). A rustls crypto provider must already be installed
/// (see the module docs).
///
/// # Errors
/// - [`TransportError::Protocol`] if `server_name` is not a valid DNS name or IP.
/// - [`TransportError::Io`] if the TCP connection or TLS handshake fails.
pub async fn connect<A: ToSocketAddrs>(
    addr: A,
    server_name: &str,
    config: Arc<ClientConfig>,
) -> Result<TcpConnection, TransportError> {
    let stream = TcpStream::connect(addr).await?;
    let peer = stream.peer_addr()?;
    stream.set_nodelay(true)?;
    let name = ServerName::try_from(server_name.to_string())
        .map_err(|e| TransportError::Protocol(format!("invalid server name: {e}")))?;
    let tls = TlsConnector::from(config).connect(name, stream).await?;
    Ok(TcpConnection::new(MaybeTls::ClientTls(Box::new(tls)), peer))
}

/// Dial `addr` over an **unencrypted** plaintext connection — the explicit opt-out from the
/// TLS default ([`connect`]).
///
/// # Errors
/// [`TransportError::Io`] if the connection cannot be established.
pub async fn connect_plaintext<A: ToSocketAddrs>(addr: A) -> Result<TcpConnection, TransportError> {
    let stream = TcpStream::connect(addr).await?;
    let peer = stream.peer_addr()?;
    stream.set_nodelay(true)?;
    Ok(TcpConnection::new(MaybeTls::Plain(stream), peer))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::time::timeout;

    use super::*;

    /// Guards every `recv`-await so a delivery/lifecycle regression fails fast
    /// instead of hanging the test (and CI) indefinitely.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Install the ring crypto provider (idempotent — the caller normally does this once).
    fn install_ring() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    /// Build server + client rustls configs from a fresh self-signed `localhost` cert.
    fn tls_configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
        install_ring();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der: CertificateDer<'static> = cert.cert.der().clone();
        let key_der =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));

        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        (Arc::new(server), Arc::new(client))
    }

    /// A connected TLS pair; the client trusts the server's self-signed `localhost` cert.
    async fn tls_pair() -> (Box<dyn Connection>, TcpConnection) {
        let (server_cfg, client_cfg) = tls_configs();
        let mut listener = TcpListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // Connect to loopback but verify the "localhost" SNI name the cert was issued for.
        let (server, client) = timeout(TEST_TIMEOUT, async {
            tokio::join!(
                listener.accept(),
                connect(("127.0.0.1", port), "localhost", client_cfg)
            )
        })
        .await
        .expect("handshake timed out");
        (server.unwrap(), client.unwrap())
    }

    /// Bind a plaintext listener on an ephemeral loopback port and dial it, returning the
    /// accepted (server) and dialing (client) connections.
    async fn connected_pair() -> (Box<dyn Connection>, TcpConnection) {
        let mut listener = TcpListener::bind_plaintext("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (server, client) = tokio::join!(listener.accept(), connect_plaintext(addr));
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
        let mut listener = TcpListener::bind_plaintext("127.0.0.1:0").await.unwrap();
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

    #[tokio::test]
    async fn tcp_tls_channel_should_round_trip_over_tls() {
        let (server, client) = tls_pair().await;
        let mut client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client_ch.send(Bytes::from_static(b"secure")).await.unwrap();
        assert_eq!(recv_exact(&mut server_ch, 6).await, b"secure");

        server_ch.send(Bytes::from_static(b"hello!")).await.unwrap();
        assert_eq!(recv_exact(&mut client_ch, 6).await, b"hello!");
    }

    #[tokio::test]
    async fn tcp_tls_channel_should_reassemble_message_larger_than_read_chunk() {
        let (server, client) = tls_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // Larger than one read buffer, so the reader reassembles across reads and through
        // the TLS record layer; index-derived bytes assert exact order and completeness.
        let payload: Vec<u8> = (0..READ_CHUNK_BYTES * 4).map(|i| i as u8).collect();
        client_ch.send(Bytes::from(payload.clone())).await.unwrap();

        assert_eq!(recv_exact(&mut server_ch, payload.len()).await, payload);
    }

    #[tokio::test]
    async fn tcp_tls_listener_should_reject_a_plaintext_client() {
        let (server_cfg, _client_cfg) = tls_configs();
        let mut listener = TcpListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let addr = listener.local_addr().unwrap();

        // A plaintext client (bypassing TLS) writes non-TLS bytes; the server's TLS
        // handshake must reject it — this is how "encrypted by default" rejects plaintext.
        let plaintext_client = tokio::spawn(async move {
            if let Ok(mut sock) = TcpStream::connect(addr).await {
                let _ = sock.write_all(b"not a TLS ClientHello").await;
            }
        });

        let result = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("accept should not hang on a plaintext client");
        assert!(
            result.is_err(),
            "a TLS listener must reject a plaintext client"
        );
        let _ = plaintext_client.await;
    }

    #[tokio::test]
    async fn tcp_tls_connect_should_fail_when_server_cert_is_untrusted() {
        let (server_cfg, _trusted) = tls_configs();
        let mut listener = TcpListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // The server accepts in the background; its outcome doesn't matter here — this test
        // asserts the *client* rejects an untrusted server cert (server auth actually works).
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        install_ring();
        let untrusting = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let result = timeout(
            TEST_TIMEOUT,
            connect(("127.0.0.1", port), "localhost", untrusting),
        )
        .await
        .expect("connect should not hang");
        assert!(
            result.is_err(),
            "the client must reject a server certificate it does not trust"
        );
    }

    #[tokio::test]
    async fn tcp_connect_should_reject_an_invalid_server_name() {
        let (server_cfg, client_cfg) = tls_configs();
        let mut listener = TcpListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        // The server name is parsed (after the TCP connect); an invalid one is a Protocol
        // error, not a panic.
        let err = connect(("127.0.0.1", port), "not a valid name", client_cfg)
            .await
            .unwrap_err();
        assert!(matches!(err, TransportError::Protocol(_)));
    }

    #[tokio::test]
    async fn tcp_tls_channel_should_deliver_a_coalesced_burst_in_order() {
        let (server, client) = tls_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // Send a burst without awaiting the reader between sends, so several messages queue
        // and the writer's coalesce path (drain via `try_recv`, then one flush) runs. Order
        // and completeness must survive.
        let mut expected = Vec::new();
        for i in 0..50u8 {
            let msg = [i, i.wrapping_add(1), i.wrapping_add(2), i.wrapping_add(3)];
            client_ch.send(Bytes::copy_from_slice(&msg)).await.unwrap();
            expected.extend_from_slice(&msg);
        }
        assert_eq!(recv_exact(&mut server_ch, expected.len()).await, expected);
    }

    #[tokio::test]
    async fn tcp_tls_recv_should_return_none_when_peer_disconnects() {
        let (server, client) = tls_pair().await;
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
}
