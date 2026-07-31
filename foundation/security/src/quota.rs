//! A bounded, weighted resource budget for per-connection caps (security §1.3).

/// A bounded, weighted resource budget: at most `max` units may be reserved at once.
///
/// Serves the per-connection **memory cap** (reserve message *bytes*) and the **in-flight
/// message-count cap** (reserve `1` per message) — one instance per dimension. Pure gauge:
/// reserve on admit, release on completion; each successful reserve of `amount` must be
/// released with the same `amount`. All arithmetic saturates, and it never reserves past the
/// cap — the "bounded, does not OOM" guarantee.
#[derive(Debug, Clone)]
pub struct ResourceQuota {
    max: u64,
    used: u64,
}

impl ResourceQuota {
    /// A quota permitting at most `max` units in use at once.
    #[must_use]
    pub fn new(max: u64) -> Self {
        Self { max, used: 0 }
    }

    /// Reserve `amount` units if they fit under the cap. Returns `true` if reserved, or
    /// `false` if it would exceed `max` (reject — the reservation is *not* made, so usage
    /// stays bounded and never grows past the cap). A single `amount` larger than `max` can
    /// never be reserved.
    #[must_use = "a rejected reservation must not be treated as granted"]
    pub fn try_reserve(&mut self, amount: u64) -> bool {
        // Comparing against the remaining headroom avoids ever forming `used + amount`
        // (which could overflow); `saturating_sub` keeps it safe even if the `used <= max`
        // invariant were ever violated.
        if amount <= self.max.saturating_sub(self.used) {
            self.used += amount;
            true
        } else {
            false
        }
    }

    /// Release `amount` previously-reserved units. Saturates at zero, so an over-release never
    /// underflows.
    pub fn release(&mut self, amount: u64) {
        self.used = self.used.saturating_sub(amount);
    }

    /// The units currently reserved.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.used
    }

    /// The units still available before hitting the cap.
    #[must_use]
    pub fn available(&self) -> u64 {
        self.max.saturating_sub(self.used)
    }

    /// The cap.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_quota_should_reserve_up_to_max_then_reject() {
        let mut quota = ResourceQuota::new(1000);
        assert!(quota.try_reserve(600));
        assert!(quota.try_reserve(400)); // exactly at max
        assert_eq!(quota.used(), 1000);
        assert_eq!(quota.available(), 0);
        assert!(!quota.try_reserve(1)); // one over the cap
    }

    #[test]
    fn resource_quota_should_reject_a_reservation_that_would_overflow_the_cap() {
        let mut quota = ResourceQuota::new(1000);
        assert!(quota.try_reserve(300));
        assert!(!quota.try_reserve(800)); // 300 + 800 > 1000 → rejected, not partially granted
        assert_eq!(quota.used(), 300); // unchanged
        assert!(quota.try_reserve(700)); // 300 + 700 == 1000 → fits
    }

    #[test]
    fn resource_quota_release_should_free_capacity() {
        let mut quota = ResourceQuota::new(1000);
        assert!(quota.try_reserve(1000));
        assert!(!quota.try_reserve(1));
        quota.release(400);
        assert_eq!(quota.available(), 400);
        assert!(quota.try_reserve(400));
    }

    #[test]
    fn resource_quota_release_past_used_should_saturate() {
        let mut quota = ResourceQuota::new(1000);
        assert!(quota.try_reserve(100));
        quota.release(500); // over-release — must not underflow/panic
        assert_eq!(quota.used(), 0);
        assert!(quota.try_reserve(1000));
    }

    #[test]
    fn resource_quota_amount_larger_than_max_should_be_rejected() {
        // The "does not OOM" guarantee: a single oversized reservation is never granted.
        let mut quota = ResourceQuota::new(1000);
        assert!(!quota.try_reserve(5000));
        assert_eq!(quota.used(), 0);
    }

    #[test]
    fn resource_quota_reserve_zero_should_always_succeed() {
        let mut quota = ResourceQuota::new(0);
        assert!(quota.try_reserve(0));
        assert!(!quota.try_reserve(1));
    }

    proptest::proptest! {
        #[test]
        fn resource_quota_used_should_never_exceed_max(
            max in 0u64..1_000_000,
            // Arbitrary interleaving of reserve (true) / release (false) with a weight.
            ops in proptest::collection::vec((proptest::bool::ANY, 0u64..2_000_000), 0..400),
        ) {
            let mut quota = ResourceQuota::new(max);
            for (reserve, amount) in ops {
                if reserve {
                    let _ = quota.try_reserve(amount);
                } else {
                    quota.release(amount);
                }
                // The core bound: usage never exceeds the cap, and `available` is consistent.
                proptest::prop_assert!(quota.used() <= max);
                proptest::prop_assert_eq!(quota.available(), max - quota.used());
            }
        }
    }
}
