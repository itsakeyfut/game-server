//! Structured-logging setup built on `tracing` / `tracing-subscriber`.
//!
//! [`init_logging`] installs a global subscriber that renders human-readable,
//! level-filtered logs (with source `file:line`) to stderr. The active level is
//! taken from the `RUST_LOG` environment variable, falling back to
//! [`LogConfig::default_level`] when it is unset.

use tracing_subscriber::EnvFilter;

/// Re-exported so callers can set [`LogConfig::default_level`] without importing
/// `tracing-subscriber` directly.
pub use tracing_subscriber::filter::LevelFilter;

/// Configuration for [`init_logging`].
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Level used when the `RUST_LOG` environment variable is not set.
    pub default_level: LevelFilter,
    /// Emit ANSI color codes (helpful in a dev terminal, noise in log files).
    pub ansi: bool,
    /// Include the source file and line number on each event.
    pub with_location: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            default_level: LevelFilter::INFO,
            ansi: true,
            with_location: true,
        }
    }
}

/// Error returned by [`init_logging`] when a global subscriber is already set.
#[derive(Debug, thiserror::Error)]
#[error("a global tracing subscriber is already installed")]
pub struct LogInitError(#[from] tracing::subscriber::SetGlobalDefaultError);

/// Build an [`EnvFilter`] from an optional `RUST_LOG`-style directive string,
/// defaulting to `default` when the string is absent or empty.
///
/// Directives are parsed leniently (`parse_lossy`): malformed entries are
/// dropped rather than causing a panic.
fn build_filter(rust_log: Option<&str>, default: LevelFilter) -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(default.into())
        .parse_lossy(rust_log.unwrap_or(""))
}

/// Install the global tracing subscriber.
///
/// Reads `RUST_LOG` for the filter (falling back to `config.default_level`).
/// Returns [`LogInitError`] — and never panics — if a global subscriber has
/// already been installed, so it is safe to call defensively.
pub fn init_logging(config: &LogConfig) -> Result<(), LogInitError> {
    let rust_log = std::env::var("RUST_LOG").ok();
    let filter = build_filter(rust_log.as_deref(), config.default_level);

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(config.ansi)
        .with_file(config.with_location)
        .with_line_number(config.with_location)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    /// A `MakeWriter`-compatible sink that appends to a shared buffer, so a
    /// scoped subscriber's output can be inspected in a test.
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("buffer poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Capture the output of `f` under a scoped (thread-local) subscriber using
    /// the given filter, so tests do not touch the process-global subscriber and
    /// stay parallel-safe.
    fn capture_with_writer(filter: EnvFilter, f: impl FnOnce()) -> String {
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let sink = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(move || SharedBuf(sink.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        String::from_utf8(buf.lock().expect("buffer poisoned").clone()).expect("utf-8")
    }

    #[test]
    fn logging_should_filter_below_configured_level() {
        let out = capture_with_writer(build_filter(None, LevelFilter::INFO), || {
            tracing::info!("info-visible");
            tracing::debug!("debug-hidden");
        });
        assert!(out.contains("info-visible"), "info should render: {out:?}");
        assert!(
            !out.contains("debug-hidden"),
            "debug should be filtered out at INFO: {out:?}"
        );
    }

    #[test]
    fn logging_should_emit_when_rust_log_lowers_level() {
        let out = capture_with_writer(build_filter(Some("debug"), LevelFilter::INFO), || {
            tracing::debug!("debug-now-visible");
        });
        assert!(
            out.contains("debug-now-visible"),
            "debug should render when RUST_LOG=debug: {out:?}"
        );
    }

    #[test]
    fn logging_should_err_when_already_installed() {
        // Regardless of prior global state, after this call a subscriber is
        // installed, so the next call must return Err (and must not panic).
        let _ = init_logging(&LogConfig::default());
        assert!(init_logging(&LogConfig::default()).is_err());
    }
}
