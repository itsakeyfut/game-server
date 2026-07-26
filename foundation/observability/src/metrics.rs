//! Metrics exporter hook.
//!
//! This is the *skeleton* seam for metrics: a pluggable [`MetricsExporter`] and a
//! [`MetricsHandle`] that holds one. A concrete exporter (e.g. Prometheus) and the
//! actual metric instrumentation (connection/room counts, tick time, latency, …)
//! land in later milestones; this issue only establishes the plug point.

use std::fmt;
use std::sync::Arc;

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
}
