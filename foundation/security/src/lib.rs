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
//! Per-connection resource caps (security §1.3) reuse those primitives, one per dimension:
//! - **memory** — a [`ResourceQuota`] reserving message *bytes*.
//! - **in-flight message count** — a [`ResourceQuota`] reserving `1` per message.
//! - **bandwidth** — a byte-rate [`TokenBucket`] via [`try_acquire_n`](TokenBucket::try_acquire_n)
//!   (`RateLimit::per_second(bytes_per_sec, burst_bytes)`, charging the message length).
//!
//! Wiring these into the transport accept loop / middleware pipeline / session is a later
//! integration step. Encryption / source validation arrive in their own issues.
#![forbid(unsafe_code)]

mod connection;
mod quota;
mod rate_limit;

pub use connection::ConnectionLimiter;
pub use quota::ResourceQuota;
pub use rate_limit::{KeyedRateLimiter, RateLimit, TokenBucket};
