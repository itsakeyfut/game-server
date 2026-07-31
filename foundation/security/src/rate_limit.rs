//! Token-bucket rate limiting (security §1.3): a pure [`TokenBucket`] primitive and a
//! bounded per-key [`KeyedRateLimiter`].
//!
//! Everything here is a **pure, deterministic engine**: the caller supplies the clock as
//! `now: Instant`, so nothing reads wall-clock time and tests are fully reproducible (as with
//! the session heartbeat). All arithmetic saturates — clock skew or extreme configs never
//! panic.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// A rate limit: a sustained rate (expressed as the time to accrue one token) plus a `burst`
/// (the bucket capacity — the most tokens available at once).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// Time to accrue one token (the inverse of the rate).
    interval: Duration,
    /// Bucket capacity: the most tokens available at once (max burst).
    burst: u32,
}

impl RateLimit {
    /// A limit of `rate` events per second with a `burst` capacity. `rate` and `burst` are
    /// clamped to at least 1 (a zero rate or zero burst would throttle everything).
    #[must_use]
    pub fn per_second(rate: u32, burst: u32) -> Self {
        Self {
            interval: Duration::from_secs(1) / rate.max(1),
            burst: burst.max(1),
        }
    }

    /// A limit that accrues one token every `interval`, with a `burst` capacity. `burst` is
    /// clamped to at least 1.
    #[must_use]
    pub fn new(interval: Duration, burst: u32) -> Self {
        Self {
            interval,
            burst: burst.max(1),
        }
    }

    /// A fresh, full [`TokenBucket`] for this limit, starting at `now`.
    #[must_use]
    pub fn bucket(self, now: Instant) -> TokenBucket {
        TokenBucket::new(self, now)
    }
}

/// A single token bucket (security §1.3). Use one per connection for a per-connection message
/// rate; [`KeyedRateLimiter`] holds one per key for a per-source rate.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// Nanoseconds of accrued time per token (the emission interval).
    interval_nanos: u64,
    /// Capacity: `interval_nanos * burst` (saturating).
    cap_nanos: u64,
    /// Current budget, in nanoseconds of accrued time (`0..=cap_nanos`).
    budget_nanos: u64,
    /// When the budget was last refilled.
    last: Instant,
}

impl TokenBucket {
    /// A fresh, full bucket for `limit`, starting at `now`.
    #[must_use]
    pub fn new(limit: RateLimit, now: Instant) -> Self {
        // Clamp to at least 1ns per token so an extreme rate (`per_second` > 1e9, rounding
        // the interval to 0ns) or a zero `interval` still enforces the burst cap rather than
        // failing *open* (safe-by-default: throttle on doubt).
        let interval_nanos = duration_to_nanos(limit.interval).max(1);
        let cap_nanos = interval_nanos.saturating_mul(u64::from(limit.burst));
        Self {
            interval_nanos,
            cap_nanos,
            budget_nanos: cap_nanos, // starts full
            last: now,
        }
    }

    /// Try to consume one token at `now`. Returns `true` if a token was available (allow), or
    /// `false` if the bucket is empty (throttle).
    #[must_use = "the throttle decision must be enforced, not discarded"]
    pub fn try_acquire(&mut self, now: Instant) -> bool {
        self.budget_nanos = self.budget_at(now);
        self.last = now;
        if self.budget_nanos >= self.interval_nanos {
            self.budget_nanos -= self.interval_nanos;
            true
        } else {
            false
        }
    }

    /// Whether the bucket is fully replenished at `now` (idle — no tokens owed). A full bucket
    /// carries no state, so a keyed limiter can evict it without affecting fairness.
    #[must_use]
    pub fn is_replenished(&self, now: Instant) -> bool {
        self.budget_at(now) >= self.cap_nanos
    }

    /// The budget the bucket would have at `now`, without mutating it. `saturating_duration_since`
    /// tolerates clock skew (`now < last`).
    fn budget_at(&self, now: Instant) -> u64 {
        let elapsed = duration_to_nanos(now.saturating_duration_since(self.last));
        self.budget_nanos
            .saturating_add(elapsed)
            .min(self.cap_nanos)
    }
}

/// Nanoseconds of a `Duration` as `u64`, saturating (a duration past ~584 years — never
/// reached from real elapsed time — clamps instead of overflowing).
fn duration_to_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// A rate limiter keyed by some source (e.g. an `IpAddr` for a per-IP connection rate).
///
/// **Bounded**: it tracks at most `max_keys` buckets and evicts idle ones, so a flood of
/// distinct keys cannot exhaust memory — the limiter must not itself become a DoS vector.
#[derive(Debug, Clone)]
pub struct KeyedRateLimiter<K> {
    limit: RateLimit,
    max_keys: usize,
    buckets: HashMap<K, TokenBucket>,
}

impl<K: Eq + Hash + Clone> KeyedRateLimiter<K> {
    /// A limiter applying `limit` per key, tracking at most `max_keys` keys (clamped to at
    /// least 1).
    #[must_use]
    pub fn new(limit: RateLimit, max_keys: usize) -> Self {
        Self {
            limit,
            max_keys: max_keys.max(1),
            buckets: HashMap::new(),
        }
    }

    /// Charge one event to `key` at `now`. Returns `true` if allowed, or `false` if `key` is
    /// over its limit — or if the limiter is already tracking `max_keys` keys (**fail-closed**:
    /// a distinct-key flood is throttled and memory stays bounded).
    ///
    /// This is **O(1)**: idle keys are reclaimed by [`sweep`](Self::sweep) (call it
    /// periodically), not by scanning on the hot path — so a distinct-key flood cannot force
    /// an O(`max_keys`) table scan per request. While the limiter is full, new keys are denied
    /// until a `sweep` frees a slot.
    #[must_use = "the throttle decision must be enforced, not discarded"]
    pub fn check(&mut self, key: K, now: Instant) -> bool {
        if let Some(bucket) = self.buckets.get_mut(&key) {
            return bucket.try_acquire(now);
        }
        if self.buckets.len() >= self.max_keys {
            return false; // full — reclaim via `sweep`, not an O(n) scan here
        }
        let mut bucket = TokenBucket::new(self.limit, now);
        let allowed = bucket.try_acquire(now);
        self.buckets.insert(key, bucket);
        allowed
    }

    /// Drop every idle (fully replenished) bucket, bounding memory and freeing slots for new
    /// keys. Call this periodically (e.g. on a timer or every N events).
    pub fn sweep(&mut self, now: Instant) {
        self.buckets.retain(|_, bucket| !bucket.is_replenished(now));
    }

    /// The number of keys currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Whether no keys are currently tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A base instant; tests add explicit `Duration`s to it, so the relative timing is
    /// deterministic regardless of the wall clock.
    fn base() -> Instant {
        Instant::now()
    }

    #[test]
    fn token_bucket_should_allow_a_full_burst_then_throttle() {
        let t0 = base();
        let mut bucket = RateLimit::per_second(1, 5).bucket(t0);
        for _ in 0..5 {
            assert!(bucket.try_acquire(t0));
        }
        assert!(!bucket.try_acquire(t0)); // 6th at the same instant is throttled
    }

    #[test]
    fn token_bucket_should_refill_after_the_interval() {
        let t0 = base();
        // 10/sec, burst 1 → one token per 100ms.
        let mut bucket = RateLimit::per_second(10, 1).bucket(t0);
        assert!(bucket.try_acquire(t0));
        assert!(!bucket.try_acquire(t0)); // burst exhausted
        // Just before the interval: still throttled (no partial-token allow).
        assert!(!bucket.try_acquire(t0 + Duration::from_millis(99)));
        // At the interval: refilled.
        assert!(bucket.try_acquire(t0 + Duration::from_millis(100)));
    }

    #[test]
    fn token_bucket_should_cap_budget_at_burst_after_a_long_idle() {
        let t0 = base();
        let mut bucket = RateLimit::per_second(1, 3).bucket(t0);
        let later = t0 + Duration::from_secs(10_000);
        for _ in 0..3 {
            assert!(bucket.try_acquire(later));
        }
        assert!(!bucket.try_acquire(later)); // capped at burst=3 despite the long wait
    }

    #[test]
    fn token_bucket_should_not_panic_on_clock_skew() {
        let t0 = base();
        let mut bucket = RateLimit::per_second(5, 2).bucket(t0 + Duration::from_secs(1));
        // `now` earlier than `last` (skew): saturates to no refill, no panic.
        assert!(bucket.try_acquire(t0));
        assert!(bucket.try_acquire(t0));
        assert!(!bucket.try_acquire(t0));
    }

    #[test]
    fn keyed_should_isolate_keys() {
        let t0 = base();
        let mut limiter = KeyedRateLimiter::new(RateLimit::per_second(1, 1), 100);
        assert!(limiter.check("a", t0));
        assert!(!limiter.check("a", t0)); // "a" throttled
        assert!(limiter.check("b", t0)); // "b" is independent
    }

    #[test]
    fn keyed_new_key_first_request_should_be_allowed() {
        let t0 = base();
        let mut limiter = KeyedRateLimiter::new(RateLimit::per_second(1, 1), 100);
        assert!(limiter.check(1u32, t0));
    }

    #[test]
    fn keyed_should_bound_tracked_keys_and_deny_new_when_full() {
        let t0 = base();
        let mut limiter = KeyedRateLimiter::new(RateLimit::per_second(1, 1), 2);
        assert!(limiter.check(1u32, t0));
        assert!(limiter.check(2u32, t0));
        // At cap; keys 1 and 2 just consumed a token so neither is idle → a new key is denied.
        assert!(!limiter.check(3u32, t0));
        assert_eq!(limiter.len(), 2); // memory stays bounded
    }

    #[test]
    fn keyed_full_should_deny_new_key_until_sweep_frees_an_idle_slot() {
        let t0 = base();
        let mut limiter = KeyedRateLimiter::new(RateLimit::per_second(1, 1), 1);
        assert!(limiter.check(1u32, t0));
        // At cap: a new key is denied (no O(n) evict-on-insert on the hot path)...
        let later = t0 + Duration::from_secs(10);
        assert!(!limiter.check(2u32, later));
        // ...until `sweep` reclaims key 1's now-idle bucket.
        limiter.sweep(later);
        assert!(limiter.check(2u32, later));
        assert_eq!(limiter.len(), 1);
    }

    #[test]
    fn keyed_sweep_should_drop_idle_buckets_but_keep_busy_ones() {
        let t0 = base();
        let mut limiter = KeyedRateLimiter::new(RateLimit::per_second(1, 1), 100);
        assert!(limiter.check("idle", t0)); // "idle" last used at t0
        let sweep_at = t0 + Duration::from_secs(10);
        assert!(limiter.check("busy", sweep_at)); // "busy" used at the sweep instant
        assert_eq!(limiter.len(), 2);

        limiter.sweep(sweep_at);
        // "idle" (10s since last use) is replenished → dropped; "busy" (just used) survives.
        assert_eq!(limiter.len(), 1);
        assert!(!limiter.check("busy", sweep_at)); // the survivor is "busy" and still throttled
    }

    proptest::proptest! {
        #[test]
        fn token_bucket_should_allow_exactly_burst_at_a_fixed_instant(
            rate in 1u32..10_000, burst in 1u32..1_000,
        ) {
            let t0 = base();
            let mut bucket = RateLimit::per_second(rate, burst).bucket(t0);
            let mut allowed = 0u32;
            while bucket.try_acquire(t0) {
                allowed += 1;
                if allowed > burst + 5 { break; } // guard against a runaway (bug)
            }
            proptest::prop_assert_eq!(allowed, burst);
        }

        #[test]
        fn token_bucket_should_never_exceed_burst_after_any_elapsed(
            rate in 1u32..10_000, burst in 1u32..1_000, secs in 0u64..100_000,
        ) {
            let t0 = base();
            let mut bucket = RateLimit::per_second(rate, burst).bucket(t0);
            let later = t0 + Duration::from_secs(secs);
            let mut allowed = 0u32;
            while bucket.try_acquire(later) {
                allowed += 1;
                if allowed > burst + 5 { break; }
            }
            // The budget caps at `burst` no matter how long the bucket sat idle.
            proptest::prop_assert_eq!(allowed, burst);
        }

        #[test]
        fn keyed_len_should_never_exceed_max_keys(
            max_keys in 1usize..20,
            // Keys drawn from a space wider than `max_keys` so the at-cap path is exercised;
            // each op advances the clock by a small delta.
            ops in proptest::collection::vec((0u8..100, 0u64..3), 0..300),
        ) {
            let mut limiter = KeyedRateLimiter::new(RateLimit::per_second(2, 3), max_keys);
            let mut now = base();
            for (key, delta) in ops {
                now += Duration::from_secs(delta);
                let _ = limiter.check(key, now);
                // The core DoS-resistance invariant: memory stays bounded, always.
                proptest::prop_assert!(limiter.len() <= max_keys);
            }
        }
    }
}
