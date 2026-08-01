//! Observability foundations: structured logging and a metrics exporter hook.
//!
//! Per the platform's operability policy, observability is built in by default.
//! This crate provides the skeleton:
//!
//! - [`init_logging`] installs a `tracing` subscriber with level filtering (from
//!   `RUST_LOG`) and source `file:line` locations.
//! - [`connection_span`] / [`room_span`] / [`request_span`] construct the standard tracing
//!   spans (operability §1) with a consistent name + field vocabulary.
//! - [`Metrics`] collects the key runtime metrics (connection / room counts, message &
//!   drop rates, request latency) via atomics; it implements [`MetricsExporter`], so
//!   installing an `Arc<Metrics>` into a [`MetricsHandle`] lets a `/metrics` endpoint render
//!   it as Prometheus text.
//!
//! Emitting these from real connections / rooms / requests is wired up when the runtime /
//! server is assembled; this crate provides the leaf-safe instruments and vocabulary.
//!
//! `tracing` is re-exported so downstream crates share the workspace-pinned
//! version of the logging macros.
#![forbid(unsafe_code)]

pub use tracing;

mod logging;
mod metrics;
mod spans;

pub use logging::{LevelFilter, LogConfig, LogInitError, init_logging};
pub use metrics::{Metrics, MetricsExporter, MetricsHandle};
pub use spans::{connection_span, request_span, room_span};
