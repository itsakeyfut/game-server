//! A minimal HTTP health server for Kubernetes probes.
//!
//! Serves just two endpoints so an orchestrator can route around a draining
//! instance — there is no general HTTP surface here (game traffic uses the
//! transport crate, not HTTP):
//!
//! - `GET /livez` (or `/healthz`) → `200` while the process is alive.
//! - `GET /readyz` → `200` normally, `503` once a [`Shutdown`] has been triggered
//!   (so the orchestrator stops sending new traffic during a drain).
//!
//! Requests are parsed defensively and **never panic** on malformed input; each
//! connection serves one request and closes.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::shutdown::Shutdown;

/// Serve health endpoints on `listener` until the task is dropped.
///
/// Runs for the life of the process (including during a drain, so `/readyz` can
/// report `503`); run it as a background task (`tokio::spawn`). Returns `Err` only
/// if accepting a connection fails.
pub async fn serve(listener: TcpListener, shutdown: Shutdown) -> io::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        let shutdown = shutdown.clone();
        // Health responses are tiny; handle each connection on its own task so a
        // slow client can't block probes on other connections.
        tokio::spawn(async move {
            let _ = handle_connection(stream, &shutdown).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream, shutdown: &Shutdown) -> io::Result<()> {
    // A health request is tiny; a single bounded read captures the request line.
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let (status, reason, body) = route(&buf[..n], shutdown);
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         content-type: text/plain; charset=utf-8\r\n\
         content-length: {}\r\n\
         connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

/// Map a raw request to `(status, reason, body)`.
fn route(request: &[u8], shutdown: &Shutdown) -> (u16, &'static str, &'static str) {
    let path = request_path(request);
    if path == Some(&b"/livez"[..]) || path == Some(&b"/healthz"[..]) {
        (200, "OK", "ok")
    } else if path == Some(&b"/readyz"[..]) {
        if shutdown.is_triggered() {
            (503, "Service Unavailable", "draining")
        } else {
            (200, "OK", "ready")
        }
    } else {
        (404, "Not Found", "not found")
    }
}

/// Extract the request-target from the first line (`METHOD SP TARGET SP VERSION`),
/// or `None` if it is malformed. Pure byte work — never panics.
fn request_path(request: &[u8]) -> Option<&[u8]> {
    let line_end = request
        .iter()
        .position(|&b| b == b'\r' || b == b'\n')
        .unwrap_or(request.len());
    let mut parts = request[..line_end].split(|&b| b == b' ');
    let _method = parts.next()?;
    let path = parts.next()?;
    if path.is_empty() { None } else { Some(path) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    async fn spawn_server(
        shutdown: Shutdown,
    ) -> (SocketAddr, tokio::task::JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let handle = tokio::spawn(serve(listener, shutdown));
        (addr, handle)
    }

    async fn http_get(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let request = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n");
        stream.write_all(request.as_bytes()).await.expect("write");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read");
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test]
    async fn livez_should_respond_ok() {
        let (addr, handle) = spawn_server(Shutdown::new()).await;
        let resp = http_get(addr, "/livez").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(resp.ends_with("ok"), "{resp}");
        handle.abort();
    }

    #[tokio::test]
    async fn readyz_should_respond_ok_when_running() {
        let (addr, handle) = spawn_server(Shutdown::new()).await;
        let resp = http_get(addr, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "{resp}");
        assert!(resp.ends_with("ready"), "{resp}");
        handle.abort();
    }

    #[tokio::test]
    async fn readyz_should_respond_unavailable_when_shutting_down() {
        let shutdown = Shutdown::new();
        let (addr, handle) = spawn_server(shutdown.clone()).await;
        shutdown.trigger();
        let resp = http_get(addr, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 503"), "{resp}");
        handle.abort();
    }

    #[tokio::test]
    async fn unknown_path_should_respond_not_found() {
        let (addr, handle) = spawn_server(Shutdown::new()).await;
        let resp = http_get(addr, "/nope").await;
        assert!(resp.starts_with("HTTP/1.1 404"), "{resp}");
        handle.abort();
    }

    #[tokio::test]
    async fn malformed_request_should_not_panic() {
        let (addr, handle) = spawn_server(Shutdown::new()).await;
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(b"\xff not-a-valid-request \x00")
            .await
            .expect("write");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read");
        // The server responds (404) rather than panicking or hanging.
        let resp = String::from_utf8_lossy(&response);
        assert!(resp.starts_with("HTTP/1.1 404"), "{resp}");
        handle.abort();
    }
}
