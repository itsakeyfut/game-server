//! Shared TLS stream plumbing for the socket byte-transports ([`tcp`](crate::tcp) and
//! [`ws`](crate::ws)).

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{client, server};

/// The byte stream under a socket transport: plaintext TCP, or a rustls TLS stream
/// (server- or client-side).
///
/// Erasing the concrete stream type here keeps the connection types monomorphic — a
/// `Box<dyn AsyncRead + AsyncWrite>` is not expressible (two non-auto traits), so an enum
/// that delegates the IO traits is the clean way to unify the variants. The TLS variants
/// are boxed because a `TlsStream` is much larger than a `TcpStream`.
pub(crate) enum MaybeTls {
    Plain(TcpStream),
    ServerTls(Box<server::TlsStream<TcpStream>>),
    ClientTls(Box<client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::ServerTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            MaybeTls::ClientTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::ServerTls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            MaybeTls::ClientTls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::ServerTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            MaybeTls::ClientTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::ServerTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            MaybeTls::ClientTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}
