//! `Service<Message>` pipeline and router.
//!
//! A [`Pipeline`] is a composable chain of [`Middleware`] (the framework's own Tower
//! analogy, runtime §3) that an inbound [`Request`] flows through — authentication,
//! rate-limiting, decoding, destination-room resolution — before reaching the
//! [`Endpoint`], the Room boundary. Build one with [`Pipeline::builder`].
#![forbid(unsafe_code)]

mod auth;
mod pipeline;
mod request;

pub use auth::AuthMiddleware;
pub use pipeline::{Endpoint, Middleware, Next, Pipeline, PipelineBuilder, PipelineError};
pub use request::{ConnectionId, Extensions, Request};
