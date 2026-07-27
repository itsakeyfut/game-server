//! App + Plugin programming model; runtime; server facade.
//!
//! [`App`] is the center of a server: register [`Plugin`]s, then [`App::run`] builds,
//! starts, and stops them in **dependency order** (programming-model §1.1 / §1.6),
//! draining on a [`gsf_lifecycle`] shutdown signal.
#![forbid(unsafe_code)]

mod app;
mod plugin;

pub use app::App;
pub use plugin::{AppError, BoxError, Plugin, PluginId};
