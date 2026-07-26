//! Observability foundations: structured logging and a metrics exporter hook.
//!
//! Per the platform's operability policy, observability is built in by default.
//! This crate provides the skeleton:
//!
//! - [`init_logging`] installs a `tracing` subscriber with level filtering (from
//!   `RUST_LOG`) and source `file:line` locations.
//! - [`MetricsHandle`] / [`MetricsExporter`] are the pluggable seam for a metrics
//!   exporter (Prometheus-compatible), filled in by later milestones.
//!
//! `tracing` is re-exported so downstream crates share the workspace-pinned
//! version of the logging macros.
#![forbid(unsafe_code)]

pub use tracing;

mod logging;
mod metrics;

pub use logging::{LevelFilter, LogConfig, LogInitError, init_logging};
pub use metrics::{MetricsExporter, MetricsHandle};
