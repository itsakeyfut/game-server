//! A virtual clock decoupled from wall-time.
//!
//! [`SimClock`] holds a [`SimInstant`] that only moves when the caller advances
//! it, and fires timers scheduled against that virtual time. Because time is
//! driven explicitly (never read from the wall clock), **expiry / timeout
//! behaviour is fully reproducible**.
//!
//! It is the *timer* half of the simulation harness; the *packet* half is
//! [`VirtualLink`](crate::VirtualLink). A test driver advances to the next event —
//! `min(clock.next_deadline(), link.next_delivery())` — processes it, and repeats.
//!
//! ```
//! use std::time::Duration;
//! use gsf_net_sim::SimClock;
//!
//! let mut clock: SimClock<&str> = SimClock::new();
//! clock.schedule_after(Duration::from_secs(5), "timeout");
//! assert!(clock.advance(Duration::from_secs(4)).is_empty()); // not yet
//! assert_eq!(clock.advance(Duration::from_secs(1)), vec!["timeout"]); // fires at 5s
//! ```

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::time::Duration;

use crate::time::SimInstant;

/// A handle to a scheduled timer, for [`SimClock::cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(u64);

/// A scheduled timer, ordered by `(deadline, seq)` (its payload is not compared).
/// `seq` keeps equal deadlines firing in schedule order.
struct Timer<T> {
    deadline: SimInstant,
    seq: u64,
    payload: T,
}

impl<T> PartialEq for Timer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}
impl<T> Eq for Timer<T> {}
impl<T> PartialOrd for Timer<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Timer<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then(self.seq.cmp(&other.seq))
    }
}
impl<T> std::fmt::Debug for Timer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Timer")
            .field("deadline", &self.deadline)
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

/// A deterministic virtual clock with scheduled, cancellable timers carrying a
/// payload of type `T`.
pub struct SimClock<T> {
    now: SimInstant,
    // Min-ordered by (deadline, seq) via `Reverse`.
    timers: BinaryHeap<Reverse<Timer<T>>>,
    // Seqs of timers not yet fired or cancelled — the source of truth for whether
    // a popped/queried timer is still live (lazy cancellation).
    pending: HashSet<u64>,
    next_seq: u64,
}

impl<T> Default for SimClock<T> {
    fn default() -> Self {
        Self {
            now: SimInstant::START,
            timers: BinaryHeap::new(),
            pending: HashSet::new(),
            next_seq: 0,
        }
    }
}

impl<T> SimClock<T> {
    /// A fresh clock at `t = 0` with no timers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The current simulated time.
    #[must_use]
    pub fn now(&self) -> SimInstant {
        self.now
    }

    /// Schedule `payload` to fire at absolute time `deadline` (a deadline already
    /// in the past fires on the next advance).
    pub fn schedule_at(&mut self, deadline: SimInstant, payload: T) -> TimerId {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.pending.insert(seq);
        self.timers.push(Reverse(Timer {
            deadline,
            seq,
            payload,
        }));
        TimerId(seq)
    }

    /// Schedule `payload` to fire `delay` from [`now`](Self::now).
    pub fn schedule_after(&mut self, delay: Duration, payload: T) -> TimerId {
        let deadline = self.now.saturating_add(delay);
        self.schedule_at(deadline, payload)
    }

    /// Cancel a timer. Returns `true` if it was still pending (not yet fired or
    /// already cancelled).
    pub fn cancel(&mut self, id: TimerId) -> bool {
        self.pending.remove(&id.0)
    }

    /// The earliest pending timer's deadline, if any — lets a driver jump straight
    /// to the next event.
    #[must_use]
    pub fn next_deadline(&self) -> Option<SimInstant> {
        self.timers
            .iter()
            .filter(|t| self.pending.contains(&t.0.seq))
            .map(|t| t.0.deadline)
            .min()
    }

    /// Advance time to `instant` (never backwards) and return the payloads of
    /// every timer that has now expired, in `(deadline, seq)` order.
    pub fn advance_to(&mut self, instant: SimInstant) -> Vec<T> {
        self.now = self.now.max(instant);
        let mut fired = Vec::new();
        while self.timers.peek().is_some_and(|t| t.0.deadline <= self.now) {
            if let Some(Reverse(timer)) = self.timers.pop() {
                // Only fire if still pending; cancelled timers are dropped silently.
                if self.pending.remove(&timer.seq) {
                    fired.push(timer.payload);
                }
            }
        }
        fired
    }

    /// Advance time by `delta` (a tick) and return the expired timers' payloads.
    pub fn advance(&mut self, delta: Duration) -> Vec<T> {
        let target = self.now.saturating_add(delta);
        self.advance_to(target)
    }
}

impl<T> std::fmt::Debug for SimClock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimClock")
            .field("now", &self.now)
            .field("pending_timers", &self.pending.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn after(clock: &mut SimClock<&'static str>, secs: u64, label: &'static str) -> TimerId {
        clock.schedule_after(Duration::from_secs(secs), label)
    }

    #[test]
    fn clock_should_start_at_zero() {
        let clock: SimClock<()> = SimClock::new();
        assert_eq!(clock.now(), SimInstant::START);
    }

    #[test]
    fn clock_should_advance_by_delta() {
        let mut clock: SimClock<()> = SimClock::new();
        let _ = clock.advance(Duration::from_secs(3));
        assert_eq!(clock.now(), SimInstant::from_start(Duration::from_secs(3)));
    }

    #[test]
    fn timer_should_fire_at_its_deadline_not_before() {
        let mut clock: SimClock<&str> = SimClock::new();
        after(&mut clock, 5, "t");
        assert!(
            clock.advance(Duration::from_secs(4)).is_empty(),
            "not at 4s"
        );
        assert_eq!(clock.advance(Duration::from_secs(1)), vec!["t"], "at 5s");
    }

    #[test]
    fn timers_should_fire_in_deadline_order() {
        let mut clock: SimClock<&str> = SimClock::new();
        after(&mut clock, 3, "c");
        after(&mut clock, 1, "a");
        after(&mut clock, 2, "b");
        assert_eq!(
            clock.advance(Duration::from_secs(10)),
            vec!["a", "b", "c"],
            "sorted by deadline regardless of schedule order"
        );
    }

    #[test]
    fn timers_should_break_ties_by_schedule_order() {
        let mut clock: SimClock<&str> = SimClock::new();
        after(&mut clock, 1, "first");
        after(&mut clock, 1, "second");
        assert_eq!(
            clock.advance(Duration::from_secs(1)),
            vec!["first", "second"],
            "equal deadlines keep schedule order"
        );
    }

    #[test]
    fn cancelled_timer_should_not_fire() {
        let mut clock: SimClock<&str> = SimClock::new();
        let id = after(&mut clock, 5, "cancelled");
        after(&mut clock, 5, "kept");
        assert!(clock.cancel(id), "was pending");
        assert!(!clock.cancel(id), "already cancelled");
        assert_eq!(clock.advance(Duration::from_secs(10)), vec!["kept"]);
    }

    #[test]
    fn next_deadline_should_return_earliest_pending() {
        let mut clock: SimClock<&str> = SimClock::new();
        assert_eq!(clock.next_deadline(), None, "none when empty");
        after(&mut clock, 3, "c");
        let early = after(&mut clock, 1, "a");
        assert_eq!(
            clock.next_deadline(),
            Some(SimInstant::from_start(Duration::from_secs(1)))
        );
        clock.cancel(early);
        assert_eq!(
            clock.next_deadline(),
            Some(SimInstant::from_start(Duration::from_secs(3))),
            "skips cancelled"
        );
    }

    #[test]
    fn advance_to_should_not_move_time_backward() {
        let mut clock: SimClock<()> = SimClock::new();
        let _ = clock.advance(Duration::from_secs(10));
        let fired = clock.advance_to(SimInstant::from_start(Duration::from_secs(5)));
        assert!(fired.is_empty());
        assert_eq!(clock.now(), SimInstant::from_start(Duration::from_secs(10)));
    }

    #[test]
    fn schedule_after_should_be_relative_to_now() {
        let mut clock: SimClock<&str> = SimClock::new();
        let _ = clock.advance(Duration::from_secs(2));
        after(&mut clock, 3, "t"); // deadline = 2s + 3s = 5s
        assert!(clock.advance(Duration::from_secs(2)).is_empty(), "at 4s");
        assert_eq!(clock.advance(Duration::from_secs(1)), vec!["t"], "at 5s");
    }

    #[test]
    fn expiry_should_be_reproducible() {
        let run = || -> Vec<&'static str> {
            let mut clock: SimClock<&str> = SimClock::new();
            after(&mut clock, 3, "c");
            after(&mut clock, 1, "a");
            after(&mut clock, 2, "b");
            let mut out = clock.advance(Duration::from_secs(1));
            out.extend(clock.advance(Duration::from_secs(2)));
            out
        };
        assert_eq!(
            run(),
            run(),
            "identical schedule/advance ⇒ identical firing"
        );
        assert_eq!(run(), vec!["a", "b", "c"]);
    }

    proptest! {
        #[test]
        fn clock_should_fire_every_timer_exactly_once_in_order(
            deadlines in prop::collection::vec(0u64..1000, 0..50)
        ) {
            let mut clock: SimClock<usize> = SimClock::new();
            for (i, &d) in deadlines.iter().enumerate() {
                clock.schedule_at(SimInstant::from_start(Duration::from_millis(d)), i);
            }
            let fired = clock.advance_to(SimInstant::from_start(Duration::from_secs(10)));
            // every timer fired exactly once
            prop_assert_eq!(fired.len(), deadlines.len());
            // fired order matches sort by (deadline, schedule index)
            let mut expected: Vec<usize> = (0..deadlines.len()).collect();
            expected.sort_by_key(|&i| (deadlines[i], i));
            prop_assert_eq!(fired, expected);
        }
    }
}
