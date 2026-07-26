//! A process-wide shutdown notification.
//!
//! [`Shutdown`] is a clonable handle around a `watch` channel that starts
//! "running" and flips to "shutting down" exactly once. Components take a
//! [`ShutdownListener`] and `await` [`ShutdownListener::wait`] to be woken when a
//! drain begins. [`install_signal_handler`] wires OS signals (SIGTERM / SIGINT /
//! Ctrl-C) to [`Shutdown::trigger`].
//!
//! This is the skeleton the App-level drain orchestration (stop-accept → drain
//! rooms → notify sessions) builds on in a later milestone.

use std::io;
use std::sync::Arc;

use tokio::sync::watch;

/// A clonable handle that can start a shutdown and report whether one is in
/// progress. Cloning shares the same underlying signal.
#[derive(Clone)]
pub struct Shutdown {
    tx: Arc<watch::Sender<bool>>,
}

impl Shutdown {
    /// Create a shutdown handle in the "running" state.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx: Arc::new(tx) }
    }

    /// Begin shutting down. Idempotent — calling it again is a no-op.
    pub fn trigger(&self) {
        self.tx.send_replace(true);
    }

    /// Whether a shutdown has been triggered.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.tx.borrow()
    }

    /// Get a listener that can `await` the shutdown.
    #[must_use]
    pub fn subscribe(&self) -> ShutdownListener {
        ShutdownListener {
            rx: self.tx.subscribe(),
        }
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Shutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shutdown")
            .field("triggered", &self.is_triggered())
            .finish()
    }
}

/// A subscription to a [`Shutdown`]; `await` [`wait`](Self::wait) to be woken when
/// the shutdown starts.
#[derive(Clone)]
pub struct ShutdownListener {
    rx: watch::Receiver<bool>,
}

impl ShutdownListener {
    /// Whether a shutdown has been triggered.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolve once a shutdown has been triggered (immediately, if it already has).
    pub async fn wait(&mut self) {
        // `wait_for` returns immediately if the predicate already holds, and only
        // errors if the sender is dropped — in which case treat it as shut down.
        let _ = self.rx.wait_for(|&triggered| triggered).await;
    }
}

impl std::fmt::Debug for ShutdownListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownListener")
            .field("triggered", &self.is_triggered())
            .finish()
    }
}

/// Wait for an OS shutdown signal, then [`trigger`](Shutdown::trigger) `shutdown`.
///
/// On Unix this listens for both `SIGTERM` (Kubernetes' graceful-stop signal) and
/// `SIGINT` (Ctrl-C); on other platforms it waits for Ctrl-C. Intended to be run
/// as a background task (`tokio::spawn`).
pub async fn install_signal_handler(shutdown: Shutdown) -> io::Result<()> {
    wait_for_signal().await?;
    shutdown.trigger();
    Ok(())
}

#[cfg(unix)]
async fn wait_for_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_should_notify_listeners_when_triggered() {
        let shutdown = Shutdown::new();
        let mut listener = shutdown.subscribe();
        assert!(!listener.is_triggered());

        shutdown.trigger();

        // `wait` must resolve now that the shutdown has fired.
        listener.wait().await;
        assert!(listener.is_triggered());
    }

    #[tokio::test]
    async fn wait_should_resolve_immediately_if_already_triggered() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        let mut listener = shutdown.subscribe();
        listener.wait().await; // must not hang
        assert!(listener.is_triggered());
    }

    #[tokio::test]
    async fn wait_should_wake_a_pending_listener() {
        let shutdown = Shutdown::new();
        let mut listener = shutdown.subscribe();
        let waiter = tokio::spawn(async move {
            listener.wait().await;
            true
        });
        shutdown.trigger();
        assert!(waiter.await.expect("task"));
    }

    #[test]
    fn shutdown_should_be_idempotent() {
        let shutdown = Shutdown::new();
        assert!(!shutdown.is_triggered());
        shutdown.trigger();
        shutdown.trigger();
        assert!(shutdown.is_triggered());
    }

    #[test]
    fn clones_should_share_the_same_signal() {
        let a = Shutdown::new();
        let b = a.clone();
        a.trigger();
        assert!(b.is_triggered(), "clone observes the trigger");
    }
}
