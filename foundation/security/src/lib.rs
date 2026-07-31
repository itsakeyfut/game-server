//! Encryption, source validation, rate limiting, per-connection resource caps.
//!
//! Rate limiting & connection-flood protection (security §1.3), as pure, deterministic
//! engines — the caller supplies the clock (`now: Instant`) and synchronizes access, so they
//! never read wall-clock time and never panic (all arithmetic saturates):
//!
//! - [`TokenBucket`] — the core token-bucket primitive. Use one per connection for a
//!   **per-connection message rate**.
//! - [`KeyedRateLimiter`] — a **bounded** per-key limiter (evicts idle keys so it can't itself
//!   be a memory DoS). Use `KeyedRateLimiter<IpAddr>` for a **per-IP connection rate**.
//! - [`ConnectionLimiter`] — a cap on **un-established connections** (connection-flood).
//!
//! Wiring these into the transport accept loop / middleware pipeline / session is a later
//! integration step. Encryption / source validation / per-connection resource caps arrive in
//! their own issues.
#![forbid(unsafe_code)]

mod connection;
mod rate_limit;

pub use connection::ConnectionLimiter;
pub use rate_limit::{KeyedRateLimiter, RateLimit, TokenBucket};
