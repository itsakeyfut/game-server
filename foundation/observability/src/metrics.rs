//! Metrics: the pluggable exporter seam and the framework's key instruments.
//!
//! [`MetricsExporter`] is the plug point (a `/metrics` endpoint calls it) and
//! [`MetricsHandle`] holds one. [`Metrics`] is the concrete registry of the key runtime
//! instruments (connection/room counts, message & drop rates, request latency); it
//! implements [`MetricsExporter`] to render itself as Prometheus text. Emitting these from
//! real connections/rooms/requests is wired when the runtime is assembled.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A pluggable metrics exporter.
///
/// [`export`](MetricsExporter::export) returns metrics rendered in a text
/// exposition format — the shape a Prometheus `/metrics` endpoint serves.
pub trait MetricsExporter: Send + Sync + 'static {
    /// Render the current metrics as a text exposition body.
    fn export(&self) -> String;
}

/// Holds the optionally-installed [`MetricsExporter`].
///
/// The server owns a handle, later code plugs an exporter into it, and a
/// `/metrics` endpoint calls [`export`](MetricsHandle::export). It is an ordinary
/// value (no global state), so it is cheap to clone and trivial to test.
#[derive(Clone, Default)]
pub struct MetricsHandle {
    exporter: Option<Arc<dyn MetricsExporter>>,
}

impl MetricsHandle {
    /// Create an empty handle with no exporter installed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a handle with `exporter` already installed.
    #[must_use]
    pub fn with_exporter(exporter: Arc<dyn MetricsExporter>) -> Self {
        Self {
            exporter: Some(exporter),
        }
    }

    /// Install (or replace) the exporter.
    pub fn set_exporter(&mut self, exporter: Arc<dyn MetricsExporter>) {
        self.exporter = Some(exporter);
    }

    /// Whether an exporter is installed.
    #[must_use]
    pub fn is_installed(&self) -> bool {
        self.exporter.is_some()
    }

    /// Render the installed exporter's output, or `None` if none is installed.
    #[must_use]
    pub fn export(&self) -> Option<String> {
        self.exporter.as_ref().map(|e| e.export())
    }
}

impl fmt::Debug for MetricsHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsHandle")
            .field("installed", &self.is_installed())
            .finish()
    }
}

/// The framework's key runtime metrics (operability §1): connection / room gauges, message
/// and drop counters, and a request-latency summary.
///
/// Recorded from many tasks concurrently through `&self` (atomics), so share one behind an
/// [`Arc`] — [`shared`](Metrics::shared) does this. It implements [`MetricsExporter`], so
/// install `Arc<Metrics>` into a [`MetricsHandle`] and a `/metrics` endpoint renders it.
///
/// All counters use [`Ordering::Relaxed`]: the metrics are independent (there is no
/// happens-before invariant *between* them), and each is individually monotone/consistent —
/// the standard for metrics counters.
///
/// Overflow policy: **gauges saturate at 0** on an unpaired decrement, while **counters wrap
/// on overflow** — the Prometheus convention (`rate()` treats a wrap as a counter reset).
#[derive(Debug, Default)]
pub struct Metrics {
    connections: AtomicU64,
    rooms: AtomicU64,
    messages_total: AtomicU64,
    drops_total: AtomicU64,
    latency_nanos_total: AtomicU64,
    latency_count: AtomicU64,
}

impl Metrics {
    /// A fresh, zeroed registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A fresh registry behind an [`Arc`], ready to share across tasks.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Record a newly-opened connection (increments the connection gauge).
    pub fn inc_connections(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a closed connection (decrements the gauge, saturating at 0).
    pub fn dec_connections(&self) {
        dec_saturating(&self.connections);
    }

    /// Record a newly-created room (increments the room gauge).
    pub fn inc_rooms(&self) {
        self.rooms.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a closed room (decrements the gauge, saturating at 0).
    pub fn dec_rooms(&self) {
        dec_saturating(&self.rooms);
    }

    /// Record one processed message (message-rate counter).
    pub fn record_message(&self) {
        self.messages_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one dropped message (drop-rate counter).
    pub fn record_drop(&self) {
        self.drops_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Observe one request's handling latency (added to the latency summary).
    pub fn observe_latency(&self, latency: Duration) {
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        self.latency_nanos_total.fetch_add(nanos, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Current open connections.
    #[must_use]
    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }

    /// Current active rooms.
    #[must_use]
    pub fn rooms(&self) -> u64 {
        self.rooms.load(Ordering::Relaxed)
    }

    /// Total messages processed.
    #[must_use]
    pub fn messages_total(&self) -> u64 {
        self.messages_total.load(Ordering::Relaxed)
    }

    /// Total messages dropped.
    #[must_use]
    pub fn drops_total(&self) -> u64 {
        self.drops_total.load(Ordering::Relaxed)
    }

    /// Number of latency observations.
    #[must_use]
    pub fn latency_count(&self) -> u64 {
        self.latency_count.load(Ordering::Relaxed)
    }
}

impl MetricsExporter for Metrics {
    fn export(&self) -> String {
        // nanos → seconds for the Prometheus convention; the precision loss on a metric is
        // acceptable.
        let latency_seconds_total =
            self.latency_nanos_total.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        format!(
            "# HELP gsf_connections Currently open connections.\n\
             # TYPE gsf_connections gauge\n\
             gsf_connections {connections}\n\
             # HELP gsf_rooms Currently active rooms.\n\
             # TYPE gsf_rooms gauge\n\
             gsf_rooms {rooms}\n\
             # HELP gsf_messages_total Messages processed.\n\
             # TYPE gsf_messages_total counter\n\
             gsf_messages_total {messages}\n\
             # HELP gsf_drops_total Messages dropped.\n\
             # TYPE gsf_drops_total counter\n\
             gsf_drops_total {drops}\n\
             # HELP gsf_request_latency_seconds Request handling latency.\n\
             # TYPE gsf_request_latency_seconds summary\n\
             gsf_request_latency_seconds_sum {latency_seconds_total}\n\
             gsf_request_latency_seconds_count {latency_count}\n",
            connections = self.connections(),
            rooms = self.rooms(),
            messages = self.messages_total(),
            drops = self.drops_total(),
            latency_count = self.latency_count(),
        )
    }
}

/// Decrement a gauge, saturating at zero so an unpaired `dec` never wraps to `u64::MAX`.
fn dec_saturating(gauge: &AtomicU64) {
    // The closure is infallible, so `fetch_update` always returns `Ok`.
    let _ = gauge.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyExporter;
    impl MetricsExporter for DummyExporter {
        fn export(&self) -> String {
            "gsf_up 1\n".to_string()
        }
    }

    #[test]
    fn metrics_handle_should_return_none_without_exporter() {
        let handle = MetricsHandle::new();
        assert!(!handle.is_installed());
        assert_eq!(handle.export(), None);
    }

    #[test]
    fn metrics_handle_should_render_installed_exporter() {
        let mut handle = MetricsHandle::new();
        handle.set_exporter(Arc::new(DummyExporter));
        assert!(handle.is_installed());
        assert_eq!(handle.export().as_deref(), Some("gsf_up 1\n"));
    }

    #[test]
    fn metrics_handle_with_exporter_should_be_installed() {
        let handle = MetricsHandle::with_exporter(Arc::new(DummyExporter));
        assert_eq!(handle.export().as_deref(), Some("gsf_up 1\n"));
    }

    #[test]
    fn metrics_should_render_prometheus_text() {
        let metrics = Metrics::new();
        metrics.inc_connections();
        metrics.inc_connections();
        metrics.inc_rooms();
        metrics.record_message();
        metrics.record_message();
        metrics.record_message();
        metrics.record_drop();
        metrics.observe_latency(Duration::from_millis(250));

        let text = metrics.export();
        assert!(text.contains("gsf_connections 2\n"), "{text}");
        assert!(text.contains("gsf_rooms 1\n"), "{text}");
        assert!(text.contains("gsf_messages_total 3\n"), "{text}");
        assert!(text.contains("gsf_drops_total 1\n"), "{text}");
        assert!(
            text.contains("gsf_request_latency_seconds_sum 0.25\n"),
            "{text}"
        );
        assert!(
            text.contains("gsf_request_latency_seconds_count 1\n"),
            "{text}"
        );
        // Prometheus TYPE lines are present for the exposition format.
        assert!(text.contains("# TYPE gsf_connections gauge\n"), "{text}");
        assert!(
            text.contains("# TYPE gsf_messages_total counter\n"),
            "{text}"
        );
    }

    #[test]
    fn metrics_gauge_dec_should_saturate_at_zero() {
        let metrics = Metrics::new();
        metrics.dec_connections(); // unpaired dec on an empty gauge
        assert_eq!(metrics.connections(), 0); // saturated, not u64::MAX
        metrics.inc_connections();
        metrics.dec_connections();
        metrics.dec_connections();
        assert_eq!(metrics.connections(), 0);
    }

    #[test]
    fn metrics_shared_arc_should_aggregate_records() {
        let metrics = Metrics::shared();
        let clone = Arc::clone(&metrics);
        clone.record_message();
        clone.inc_rooms();
        // The original sees records made through the shared clone.
        assert_eq!(metrics.messages_total(), 1);
        assert_eq!(metrics.rooms(), 1);
    }

    #[test]
    fn metrics_installed_into_handle_should_export() {
        let metrics = Metrics::shared();
        metrics.record_message();
        let handle = MetricsHandle::with_exporter(metrics);
        let text = handle.export().expect("exporter installed");
        assert!(text.contains("gsf_messages_total 1\n"), "{text}");
    }

    #[test]
    fn metrics_should_count_exactly_under_concurrent_recording() {
        use std::thread;

        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 10_000;

        let metrics = Metrics::shared();
        thread::scope(|scope| {
            for _ in 0..THREADS {
                let metrics = Arc::clone(&metrics);
                scope.spawn(move || {
                    for _ in 0..PER_THREAD {
                        metrics.record_message();
                    }
                });
            }
        });
        // Atomic counting is exact under contention — no lost updates.
        assert_eq!(metrics.messages_total(), THREADS * PER_THREAD);
    }
}
