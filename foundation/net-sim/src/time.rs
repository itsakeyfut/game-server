//! Virtual time for the simulator.
//!
//! [`SimInstant`] is a point in *simulated* time measured from the start of a run
//! (`t = 0`), decoupled from wall-clock time so scenarios are reproducible. The
//! virtual clock that advances time and drives timers lands in a follow-up task;
//! here we only need a comparable instant and the ability to schedule a delivery
//! at `now + delay`.

use std::time::Duration;

/// A point in simulated time, measured as a [`Duration`] since the start of the run.
///
/// Arithmetic saturates rather than panicking on overflow (library code must not
/// panic), which is harmless for the small horizons a test scenario uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SimInstant(Duration);

impl SimInstant {
    /// The start of the run, `t = 0`.
    pub const START: SimInstant = SimInstant(Duration::ZERO);

    /// Create an instant `elapsed` after the start of the run.
    #[must_use]
    pub const fn from_start(elapsed: Duration) -> Self {
        Self(elapsed)
    }

    /// Time elapsed since the start of the run.
    #[must_use]
    pub const fn since_start(self) -> Duration {
        self.0
    }

    /// This instant plus `delay` (saturating on overflow).
    #[must_use]
    pub fn saturating_add(self, delay: Duration) -> Self {
        Self(self.0.saturating_add(delay))
    }

    /// Non-negative duration from `earlier` to `self` (saturating; zero if `self`
    /// is before `earlier`).
    #[must_use]
    pub fn saturating_duration_since(self, earlier: SimInstant) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturating_add_should_advance_by_delay() {
        let t = SimInstant::START.saturating_add(Duration::from_millis(100));
        assert_eq!(t.since_start(), Duration::from_millis(100));
    }

    #[test]
    fn ordering_should_follow_elapsed_time() {
        let a = SimInstant::from_start(Duration::from_millis(10));
        let b = SimInstant::from_start(Duration::from_millis(20));
        assert!(a < b);
    }

    #[test]
    fn saturating_add_should_not_panic_on_overflow() {
        let t = SimInstant::from_start(Duration::MAX).saturating_add(Duration::MAX);
        assert_eq!(t.since_start(), Duration::MAX);
    }

    #[test]
    fn duration_since_should_saturate_to_zero_when_earlier_is_later() {
        let a = SimInstant::from_start(Duration::from_millis(5));
        let b = SimInstant::from_start(Duration::from_millis(10));
        assert_eq!(a.saturating_duration_since(b), Duration::ZERO);
        assert_eq!(b.saturating_duration_since(a), Duration::from_millis(5));
    }
}
