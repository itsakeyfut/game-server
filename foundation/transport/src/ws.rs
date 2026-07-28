//! WebSocket byte-transport: a single reliable-ordered channel per connection,
//! **TLS `wss://` by default** with an explicit plaintext `ws://` opt-out.
//!
//! A [`WsConnection`] advertises exactly [`ChannelKind::ReliableOrdered`] and hands
//! out one [`Channel`] carrying the payloads of WebSocket **binary** messages in
//! order. WebSocket frames already preserve message boundaries, so — unlike the raw
//! TCP transport — a single [`Channel::recv`] here does line up with a single peer
//! send; callers still must not rely on that (the codec reframes).
//!
//! **Encrypted by default** (security §1.1): [`WsListener::bind`] and [`connect`] take a
//! rustls [`ServerConfig`] / [`ClientConfig`] and require `wss://`; the loud
//! [`bind_plaintext`](WsListener::bind_plaintext) / [`connect_plaintext`] are the only way
//! to run unencrypted `ws://`. Certificate and PKI management live with the caller; the
//! crate root re-exports `rustls` so configs are built against the exact same version. The
//! caller must also install a rustls crypto provider before building a config — this
//! workspace uses ring, so call `rustls::crypto::ring::default_provider().install_default()`
//! once at startup (or build configs with `ServerConfig::builder_with_provider`).
//!
//! Opening the channel splits the WebSocket and spawns a reader and a writer task,
//! bridging it to the channel's bounded mpsc queues — the same lifecycle as the TCP
//! transport (tasks are tied to the [`Channel`], not the [`Connection`]).
//!
//! ```no_run
//! use bytes::Bytes;
//! use gsf_transport::{ChannelKind, Connection, Listener, ws};
//!
//! # async fn run() -> Result<(), gsf_transport::TransportError> {
//! // Plaintext opt-out shown for brevity; production uses `bind` / `connect` with a
//! // rustls config (see the encryption note above).
//! let mut listener = ws::WsListener::bind_plaintext("127.0.0.1:9002").await?;
//! let client = ws::connect_plaintext("ws://127.0.0.1:9002").await?;
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
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ServerConfig};
use tokio::net::{TcpListener as TokioTcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{WebSocketStream, accept_async_with_config, client_async_with_config};

use crate::channel::{Channel, ChannelCapabilities, ChannelKind};
use crate::connection::{Connection, Listener};
use crate::error::TransportError;
use crate::tls::MaybeTls;

/// Bound on each direction's in-flight queue (backpressure seam, tuned in #39).
const CHANNEL_CAPACITY: usize = 64;

/// The channels a WebSocket connection can provide: reliable-ordered only.
const WS_CAPS: ChannelCapabilities = ChannelCapabilities::none().with(ChannelKind::ReliableOrdered);

/// Per-message resource caps (safe-by-default; tunable in #39). Far below
/// tungstenite's 64 MiB default so a single peer cannot force a huge allocation.
const MAX_WS_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_WS_FRAME_BYTES: usize = 1024 * 1024;

/// The WebSocket config applied to every connection: explicit, bounded message/frame sizes.
fn ws_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_FRAME_BYTES))
}

type WsSink = SplitSink<WebSocketStream<MaybeTls>, Message>;
type WsSource = SplitStream<WebSocketStream<MaybeTls>>;

/// Map a tungstenite error onto the transport's error type.
fn map_ws_err(err: tungstenite::Error) -> TransportError {
    match err {
        tungstenite::Error::Io(io) => TransportError::Io(io),
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            TransportError::ConnectionClosed
        }
        other => TransportError::Protocol(other.to_string()),
    }
}

/// A WebSocket [`Connection`]: one reliable-ordered [`Channel`] over a `ws`/`wss` socket.
pub struct WsConnection {
    peer: SocketAddr,
    state: Mutex<ConnState>,
}

/// Mutable connection state behind an async mutex.
struct ConnState {
    /// The handshaked WebSocket, taken on the first `open_channel`; `None` afterwards.
    ws: Option<WebSocketStream<MaybeTls>>,
    /// Set by [`Connection::close`]; makes further opens fail with `ConnectionClosed`.
    closed: bool,
    /// Reader + writer task handles, aborted on `close` for eager teardown.
    tasks: Vec<JoinHandle<()>>,
}

impl WsConnection {
    fn new(ws: WebSocketStream<MaybeTls>, peer: SocketAddr) -> Self {
        Self {
            peer,
            state: Mutex::new(ConnState {
                ws: Some(ws),
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
impl Connection for WsConnection {
    fn capabilities(&self) -> ChannelCapabilities {
        WS_CAPS
    }

    async fn open_channel(&self, kind: ChannelKind) -> Result<Channel, TransportError> {
        if !WS_CAPS.supports(kind) {
            return Err(TransportError::UnsupportedChannel(kind));
        }
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(TransportError::ConnectionClosed);
        }
        let ws = state
            .ws
            .take()
            .ok_or(TransportError::ChannelAlreadyOpen(kind))?;

        // `split` shares the WebSocket between the two tasks via a BiLock; it is
        // uncontended in practice (each task only ever touches its own half), unlike
        // the TCP transport's independent socket halves.
        let (sink, source) = ws.split();
        let (out_tx, out_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (in_tx, in_rx) = mpsc::channel(CHANNEL_CAPACITY);

        state.tasks = vec![
            tokio::spawn(reader_loop(source, in_tx)),
            tokio::spawn(writer_loop(sink, out_rx)),
        ];

        Ok(Channel::new(ChannelKind::ReliableOrdered, out_tx, in_rx))
    }

    async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        state.ws = None;
        // Eager teardown: aborting the tasks drops the WebSocket (peer sees a
        // disconnect) and the channel's mpsc ends (local `send`/`recv` see closure).
        for task in state.tasks.drain(..) {
            task.abort();
        }
    }
}

impl fmt::Debug for WsConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsConnection")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

/// Pump binary messages from the socket into the channel's inbound queue until the
/// peer closes, the socket errors, or the [`Channel`] (holding the receiver) drops.
async fn reader_loop(mut source: WsSource, in_tx: mpsc::Sender<Bytes>) {
    loop {
        tokio::select! {
            // The Channel's receiver was dropped — no one is listening; stop reading.
            () = in_tx.closed() => break,
            msg = source.next() => match msg {
                Some(Ok(Message::Binary(bytes))) => {
                    if in_tx.send(bytes).await.is_err() {
                        break;                              // receiver gone
                    }
                }
                Some(Ok(Message::Close(_))) | None => break, // peer closed / stream ended
                // A binary transport receiving a Text frame is a peer protocol
                // violation: close rather than silently discard (honest abstraction).
                Some(Ok(Message::Text(_))) => break,
                // Ping / Pong / raw Frame: control, not channel payload. tungstenite
                // auto-queues a Pong for Pings; we skip and keep reading.
                Some(Ok(_)) => continue,
                Some(Err(_)) => break,                       // malformed / protocol error — never panics
            },
        }
    }
}

/// Pump bytes from the channel's outbound queue to the socket as binary messages,
/// until the [`Channel`] (holding the sender) drops or a write fails.
async fn writer_loop(mut sink: WsSink, mut out_rx: mpsc::Receiver<Bytes>) {
    while let Some(bytes) = out_rx.recv().await {
        if sink.send(Message::Binary(bytes)).await.is_err() {
            return;
        }
    }
    // Graceful close: the channel was dropped, so send a WebSocket Close frame.
    let _ = sink.close().await;
}

/// A WebSocket [`Listener`]: yields a [`WsConnection`] per accepted socket. TLS-encrypted
/// unless created with [`bind_plaintext`](WsListener::bind_plaintext).
pub struct WsListener {
    inner: TokioTcpListener,
    tls: Option<TlsAcceptor>,
}

impl WsListener {
    /// Bind a TLS (`wss://`) listener on `addr` using the caller's rustls server config.
    /// This is the encrypted default; use [`bind_plaintext`](Self::bind_plaintext) to opt
    /// out.
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

    /// Bind an **unencrypted** plaintext (`ws://`) listener on `addr` — the explicit opt-out
    /// from the TLS default ([`bind`](Self::bind)).
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
impl Listener for WsListener {
    async fn accept(&mut self) -> Result<Box<dyn Connection>, TransportError> {
        let (stream, peer) = self.inner.accept().await?;
        stream.set_nodelay(true)?;
        let transport = match &self.tls {
            Some(acceptor) => MaybeTls::ServerTls(Box::new(acceptor.accept(stream).await?)),
            None => MaybeTls::Plain(stream),
        };
        let ws = accept_async_with_config(transport, Some(ws_config()))
            .await
            .map_err(map_ws_err)?;
        Ok(Box::new(WsConnection::new(ws, peer)))
    }
}

impl fmt::Debug for WsListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsListener")
            .field("local_addr", &self.inner.local_addr().ok())
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

/// Dial a TLS `wss://` URL using the caller's rustls client config. This is the encrypted
/// default; use [`connect_plaintext`] to opt out.
///
/// A rustls crypto provider must already be installed (see the module docs).
///
/// # Errors
/// [`TransportError`] if the TCP connection, TLS handshake, or WebSocket handshake fails.
pub async fn connect(url: &str, config: Arc<ClientConfig>) -> Result<WsConnection, TransportError> {
    connect_inner(url, Some(TlsConnector::from(config))).await
}

/// Dial an **unencrypted** plaintext `ws://` URL — the explicit opt-out from the TLS default
/// ([`connect`]).
///
/// # Errors
/// [`TransportError`] if the TCP connection or WebSocket handshake fails.
pub async fn connect_plaintext(url: &str) -> Result<WsConnection, TransportError> {
    connect_inner(url, None).await
}

async fn connect_inner(
    url: &str,
    tls: Option<TlsConnector>,
) -> Result<WsConnection, TransportError> {
    let uri: Uri = url
        .parse()
        .map_err(|e| TransportError::Protocol(format!("invalid url: {e}")))?;
    let host = uri
        .host()
        .ok_or_else(|| TransportError::Protocol("url has no host".to_string()))?;
    // `http`'s `host()` keeps the surrounding brackets on an IPv6 literal
    // (e.g. "[::1]"); strip them for both the TCP connect and the TLS SNI name.
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let port = uri
        .port_u16()
        .unwrap_or(if tls.is_some() { 443 } else { 80 });

    let tcp = TcpStream::connect((host.as_str(), port)).await?;
    let peer = tcp.peer_addr()?;
    tcp.set_nodelay(true)?;

    let transport = match tls {
        Some(connector) => {
            let server_name = ServerName::try_from(host)
                .map_err(|e| TransportError::Protocol(format!("invalid server name: {e}")))?;
            MaybeTls::ClientTls(Box::new(connector.connect(server_name, tcp).await?))
        }
        None => MaybeTls::Plain(tcp),
    };

    let (ws, _response) = client_async_with_config(url, transport, Some(ws_config()))
        .await
        .map_err(map_ws_err)?;
    Ok(WsConnection::new(ws, peer))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ClientConfig, RootCertStore};
    use tokio::io::AsyncWriteExt;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Install the ring crypto provider (idempotent — the caller normally does this).
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

    /// A connected plaintext (`ws://`) server/client pair on an ephemeral loopback port.
    async fn plaintext_pair() -> (Box<dyn Connection>, WsConnection) {
        let mut listener = WsListener::bind_plaintext("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        // Bound the handshake so a resolution/handshake hang fails fast, not hangs CI.
        let (server, client) = timeout(TEST_TIMEOUT, async {
            tokio::join!(listener.accept(), connect_plaintext(&url))
        })
        .await
        .expect("handshake timed out");
        (server.unwrap(), client.unwrap())
    }

    /// A connected TLS (`wss://`) pair; the client trusts the server's self-signed cert.
    async fn tls_pair() -> (Box<dyn Connection>, WsConnection) {
        let (server_cfg, client_cfg) = tls_configs();
        let mut listener = WsListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // Connect via the "localhost" SNI name the cert was issued for; TCP resolution
        // falls through to 127.0.0.1 where the server is bound.
        let url = format!("wss://localhost:{port}");
        let (server, client) = timeout(TEST_TIMEOUT, async {
            tokio::join!(listener.accept(), connect(&url, client_cfg))
        })
        .await
        .expect("handshake timed out");
        (server.unwrap(), client.unwrap())
    }

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

    async fn expect_closed(ch: &mut Channel) {
        let result = timeout(TEST_TIMEOUT, ch.recv())
            .await
            .expect("timed out waiting for close");
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn ws_capabilities_should_be_reliable_ordered_only() {
        let (server, client) = plaintext_pair().await;
        let expected = ChannelCapabilities::none().with(ChannelKind::ReliableOrdered);
        assert_eq!(client.capabilities(), expected);
        assert_eq!(server.capabilities(), expected);
    }

    #[tokio::test]
    async fn ws_channel_should_round_trip_over_plaintext() {
        let (server, client) = plaintext_pair().await;
        let mut client_ch = client
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
        assert_eq!(recv_exact(&mut client_ch, 4).await, b"pong");
    }

    #[tokio::test]
    async fn wss_channel_should_round_trip_over_tls() {
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
    async fn ws_should_preserve_message_boundaries() {
        let (server, client) = plaintext_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client_ch.send(Bytes::from_static(b"first")).await.unwrap();
        client_ch.send(Bytes::from_static(b"second")).await.unwrap();

        // Each binary message arrives as its own `recv` — WS frames carry boundaries.
        let first = timeout(TEST_TIMEOUT, server_ch.recv()).await.unwrap();
        let second = timeout(TEST_TIMEOUT, server_ch.recv()).await.unwrap();
        assert_eq!(first, Some(Bytes::from_static(b"first")));
        assert_eq!(second, Some(Bytes::from_static(b"second")));
    }

    #[tokio::test]
    async fn ws_channel_should_round_trip_a_large_binary_message() {
        let (server, client) = plaintext_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // Larger than tungstenite's read buffer (spans multiple socket reads) but
        // within the frame/message caps; index-derived bytes verify order + completeness.
        let payload: Vec<u8> = (0..256 * 1024).map(|i| i as u8).collect();
        client_ch.send(Bytes::from(payload.clone())).await.unwrap();
        assert_eq!(recv_exact(&mut server_ch, payload.len()).await, payload);
    }

    #[tokio::test]
    async fn ws_channel_should_carry_both_directions_concurrently() {
        let (server, client) = plaintext_pair().await;
        let mut client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // Both peers send before either receives — exercises both directions at once.
        client_ch.send(Bytes::from_static(b"c2s")).await.unwrap();
        server_ch.send(Bytes::from_static(b"s2c")).await.unwrap();

        assert_eq!(recv_exact(&mut server_ch, 3).await, b"c2s");
        assert_eq!(recv_exact(&mut client_ch, 3).await, b"s2c");
    }

    #[tokio::test]
    async fn ws_should_close_channel_when_peer_sends_text_frame() {
        let mut listener = WsListener::bind_plaintext("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{addr}");

        // A raw WebSocket client (bypassing our transport) that injects a Text frame,
        // which is a protocol violation for our binary transport.
        let raw_client = async {
            let tcp = TcpStream::connect(addr).await.unwrap();
            let (mut ws, _resp) = client_async_with_config(&url, tcp, Some(ws_config()))
                .await
                .unwrap();
            ws.send(Message::Text("not binary".into())).await.unwrap();
            ws // keep the socket open so the close is driven by our side
        };
        let (server, _raw) = tokio::join!(listener.accept(), raw_client);
        let server = server.unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        // The transport closes the channel (no panic, no silent discard).
        expect_closed(&mut server_ch).await;
    }

    #[tokio::test]
    async fn wss_recv_should_return_none_when_peer_disconnects() {
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

    #[tokio::test]
    async fn ws_recv_should_return_none_when_peer_disconnects() {
        let (server, client) = plaintext_pair().await;
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
    async fn ws_close_should_tear_down_open_channel() {
        let (server, client) = plaintext_pair().await;
        let client_ch = client
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();
        let mut server_ch = server
            .open_channel(ChannelKind::ReliableOrdered)
            .await
            .unwrap();

        client.close().await;

        expect_closed(&mut server_ch).await;
        let err = client_ch.send(Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }

    #[tokio::test]
    async fn ws_open_channel_should_error_for_unsupported_kind() {
        let (_server, client) = plaintext_pair().await;
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
    async fn ws_open_channel_twice_should_error() {
        let (_server, client) = plaintext_pair().await;
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
    async fn wss_connect_should_fail_when_server_cert_is_untrusted() {
        let (server_cfg, _trusted) = tls_configs();
        let mut listener = WsListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // The server accepts in the background; this test asserts the *client* rejects an
        // untrusted server cert (wss server auth actually works).
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        install_ring();
        let untrusting = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let url = format!("wss://localhost:{port}");
        let result = timeout(TEST_TIMEOUT, connect(&url, untrusting))
            .await
            .expect("connect should not hang");
        assert!(
            result.is_err(),
            "the wss client must reject a server certificate it does not trust"
        );
    }

    #[tokio::test]
    async fn wss_listener_should_reject_a_plaintext_client() {
        let (server_cfg, _client_cfg) = tls_configs();
        let mut listener = WsListener::bind("127.0.0.1:0", server_cfg).await.unwrap();
        let addr = listener.local_addr().unwrap();

        // A plaintext client (no TLS) writes a plaintext HTTP/ws upgrade; the server's TLS
        // handshake must reject it — this is how the wss default rejects plaintext.
        let plaintext_client = tokio::spawn(async move {
            if let Ok(mut sock) = TcpStream::connect(addr).await {
                let _ = sock
                    .write_all(b"GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n")
                    .await;
            }
        });

        let result = timeout(TEST_TIMEOUT, listener.accept())
            .await
            .expect("accept should not hang on a plaintext client");
        assert!(
            result.is_err(),
            "a wss listener must reject a plaintext client"
        );
        let _ = plaintext_client.await;
    }
}
