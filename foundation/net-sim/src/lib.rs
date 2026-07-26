//! Deterministic, network-independent test harness: a fault-injecting virtual
//! network for proving the reliability layer and protocol correctness **without a
//! real network**.
//!
//! A [`VirtualLink`] carries opaque [`bytes::Bytes`] and, driven by a seeded
//! [`SimRng`], injects latency / loss / reorder / duplication / bandwidth exactly
//! as configured. Because the randomness is seeded and time is virtual
//! ([`SimInstant`]), **a failing scenario reproduces exactly**.
//!
//! This crate is intended as a **dev-dependency** of the transport / session /
//! protocol crates' reliability tests. The virtual clock that advances time and
//! drives timers is a follow-up task; here time is passed in explicitly.
//!
//! ```
//! use std::time::Duration;
//! use bytes::Bytes;
//! use gsf_net_sim::{LinkConfig, Probability, SimInstant, VirtualLink};
//!
//! let cfg = LinkConfig { latency: Duration::from_millis(50), loss: Probability::new(0.1), ..Default::default() };
//! let mut link = VirtualLink::new(cfg, 42); // seed
//! link.send(SimInstant::START, Bytes::from_static(b"hello"));
//! // ... advance time, then collect what has arrived:
//! let _delivered = link.deliver_due(SimInstant::from_start(Duration::from_millis(50)));
//! ```
#![forbid(unsafe_code)]

mod link;
mod rng;
mod time;

pub use link::{Bandwidth, LinkConfig, Probability, VirtualLink};
pub use rng::SimRng;
pub use time::SimInstant;
