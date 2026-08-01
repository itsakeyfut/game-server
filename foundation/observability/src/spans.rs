//! Tracing-span constructors for the standard framework scopes (operability §1):
//! connection, room, and request.
//!
//! Each returns a [`tracing::Span`] with a consistent name and field set, so logs from
//! different subsystems share one vocabulary. Enter the span (`let _g = span.enter();`) or
//! attach it to a future (`fut.instrument(span)`) around the work it scopes.
//!
//! Arguments are std/primitive types (not other crates' id types) so this crate stays a leaf
//! that everything else can depend on — callers pass their own ids (`RoomId(u64).0`, …).

use std::net::SocketAddr;

use tracing::Span;

/// A span scoping a single connection, tagged with the remote `peer` address.
#[must_use]
pub fn connection_span(peer: SocketAddr) -> Span {
    tracing::info_span!("connection", %peer)
}

/// A span scoping activity within a room, tagged with the room instance `room` id.
#[must_use]
pub fn room_span(room: u64) -> Span {
    tracing::info_span!("room", room)
}

/// A span scoping a single request, tagged with the `request` (correlation) id.
#[must_use]
pub fn request_span(request: u64) -> Span {
    tracing::info_span!("request", request)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    /// A `MakeWriter` sink that appends to a shared buffer, so a scoped subscriber's output
    /// can be inspected without touching the process-global subscriber (parallel-safe).
    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("poisoned").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuf {
        type Writer = SharedBuf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run `f` under a scoped subscriber that renders spans, capturing the output.
    fn capture(f: impl FnOnce()) -> String {
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
            .with_writer(SharedBuf(buf.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        String::from_utf8(buf.lock().expect("poisoned").clone()).expect("utf-8")
    }

    #[test]
    fn connection_span_should_scope_events_with_peer() {
        let out = capture(|| {
            let addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
            let span = connection_span(addr);
            let _g = span.enter();
            tracing::info!("hello");
        });
        assert!(out.contains("connection"), "span name: {out:?}");
        assert!(out.contains("127.0.0.1:4000"), "peer field: {out:?}");
    }

    #[test]
    fn room_span_should_scope_events_with_room_id() {
        let out = capture(|| {
            let span = room_span(42);
            let _g = span.enter();
            tracing::info!("in room");
        });
        assert!(out.contains("room"), "span name: {out:?}");
        assert!(out.contains("42"), "room field: {out:?}");
    }

    #[test]
    fn request_span_should_scope_events_with_request_id() {
        let out = capture(|| {
            let span = request_span(7);
            let _g = span.enter();
            tracing::info!("handling");
        });
        assert!(out.contains("request"), "span name: {out:?}");
        assert!(out.contains("7"), "request field: {out:?}");
    }
}
