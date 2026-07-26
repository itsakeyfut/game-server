//! Server-lifecycle primitives: graceful-shutdown signalling and health endpoints.
//!
//! This crate is the **skeleton** the full graceful-drain orchestration (stop
//! accepting connections → drain rooms → notify sessions) builds on in a later
//! milestone. It gives every server (game and cache) two shared, generic pieces:
//!
//! - [`Shutdown`] — a clonable notification that flips once, driven by
//!   [`install_signal_handler`] (SIGTERM / SIGINT / Ctrl-C).
//! - [`serve`] — a minimal HTTP health server exposing `/livez` and `/readyz`
//!   whose readiness reflects the shutdown state (for Kubernetes probes).
//!
//! ```no_run
//! use gsf_lifecycle::{Shutdown, install_signal_handler, serve};
//! use tokio::net::TcpListener;
//!
//! # async fn run() -> std::io::Result<()> {
//! let shutdown = Shutdown::new();
//! tokio::spawn(install_signal_handler(shutdown.clone()));
//!
//! let listener = TcpListener::bind("0.0.0.0:8080").await?;
//! tokio::spawn(serve(listener, shutdown.clone()));
//!
//! // ... run the server; components take `shutdown.subscribe()` to drain.
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

mod health;
mod shutdown;

pub use health::serve;
pub use shutdown::{Shutdown, ShutdownListener, install_signal_handler};
