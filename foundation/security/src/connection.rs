//! A cap on un-established connections — connection-flood protection (security §1.3).

/// A bound on how many connections may be **un-established** (accepted but not yet through
/// their handshake/auth) at once — connection-flood protection (security §1.3).
///
/// Pure gauge: [`try_admit`](Self::try_admit) on accept (before the handshake), and
/// [`release`](Self::release) once the connection is established **or** closed. Each
/// successful `try_admit` must be paired with exactly one `release`.
///
/// Not `Copy`: it is a mutable gauge with a pairing invariant, so an implicit copy that
/// silently absorbed a `try_admit`/`release` would desync it from the shared original.
#[derive(Debug, Clone)]
pub struct ConnectionLimiter {
    max: usize,
    current: usize,
}

impl ConnectionLimiter {
    /// A limiter admitting at most `max` un-established connections at once.
    #[must_use]
    pub fn new(max: usize) -> Self {
        Self { max, current: 0 }
    }

    /// Try to admit one un-established connection. Returns `true` if admitted (below the
    /// cap), or `false` if the cap is reached (reject the connection — flood protection).
    #[must_use = "a rejected connection must not be allowed to proceed"]
    pub fn try_admit(&mut self) -> bool {
        if self.current < self.max {
            self.current += 1;
            true
        } else {
            false
        }
    }

    /// Release one admitted connection (it became established, or it closed). Saturates at
    /// zero, so an unpaired release never underflows.
    pub fn release(&mut self) {
        self.current = self.current.saturating_sub(1);
    }

    /// The number of un-established connections currently admitted.
    #[must_use]
    pub fn current(&self) -> usize {
        self.current
    }

    /// How many more connections can be admitted before hitting the cap.
    #[must_use]
    pub fn available(&self) -> usize {
        self.max.saturating_sub(self.current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_should_admit_up_to_max_then_reject() {
        let mut limiter = ConnectionLimiter::new(3);
        assert!(limiter.try_admit());
        assert!(limiter.try_admit());
        assert!(limiter.try_admit());
        assert!(!limiter.try_admit()); // at cap
        assert_eq!(limiter.current(), 3);
        assert_eq!(limiter.available(), 0);
    }

    #[test]
    fn connection_release_should_free_a_slot() {
        let mut limiter = ConnectionLimiter::new(2);
        assert!(limiter.try_admit());
        assert!(limiter.try_admit());
        assert!(!limiter.try_admit());
        limiter.release();
        assert_eq!(limiter.available(), 1);
        assert!(limiter.try_admit()); // slot freed
    }

    #[test]
    fn connection_release_past_zero_should_saturate() {
        let mut limiter = ConnectionLimiter::new(2);
        limiter.release(); // unpaired release — must not underflow/panic
        assert_eq!(limiter.current(), 0);
        assert!(limiter.try_admit());
    }

    proptest::proptest! {
        #[test]
        fn connection_current_should_never_exceed_max(
            max in 1usize..50,
            // An arbitrary interleaving of admit (true) and release (false), including
            // unpaired releases.
            ops in proptest::collection::vec(proptest::bool::ANY, 0..400),
        ) {
            let mut limiter = ConnectionLimiter::new(max);
            for admit in ops {
                if admit {
                    let _ = limiter.try_admit();
                } else {
                    limiter.release();
                }
                // Invariants: the cap is never exceeded, and `available` is consistent.
                proptest::prop_assert!(limiter.current() <= max);
                proptest::prop_assert_eq!(limiter.available(), max - limiter.current());
            }
        }
    }
}
