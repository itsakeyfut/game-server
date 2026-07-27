//! The connection liveness policy: heartbeat probing plus handshake and idle timeouts.
//!
//! [`Heartbeat`] is a **pure, sync policy engine**. Fed the current time via
//! [`poll`](Heartbeat::poll), it decides what a connection needs — wait, send a probe,
//! or disconnect (slow-loris or dead) — without owning a clock or a task. The async
//! runtime supplies the time (real or test), sleeps until the returned deadline, reads
//! the socket (which is [activity](Heartbeat::on_activity)), and executes the action;
//! on [`Disconnect`](HeartbeatAction::Disconnect) it drives the session's
//! [`time_out`](crate::Session::time_out). Keeping the policy time-injected makes it
//! deterministically testable (security §1.3: handshake / idle deadlines).
//!
//! ```
//! use std::time::{Duration, Instant};
//! use gsf_session::{Heartbeat, HeartbeatAction, HeartbeatConfig, TimeoutKind};
//!
//! let cfg = HeartbeatConfig {
//!     handshake_timeout: Duration::from_secs(10),
//!     heartbeat_interval: Duration::from_secs(15),
//!     idle_timeout: Duration::from_secs(60),
//! };
//! let t0 = Instant::now();
//! let hb = Heartbeat::new(cfg, t0);
//! // An unauthenticated peer that stalls past the handshake deadline is slow-loris.
//! assert_eq!(
//!     hb.poll(t0 + Duration::from_secs(10)),
//!     HeartbeatAction::Disconnect(TimeoutKind::Handshake),
//! );
//! ```

use std::time::{Duration, Instant};

/// Timeouts governing a connection's liveness.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    /// How long an unauthenticated connection may take to authenticate before it is
    /// treated as slow-loris.
    pub handshake_timeout: Duration,
    /// How long an authenticated connection may be silent before a heartbeat probe is
    /// sent. Should be shorter than [`idle_timeout`](Self::idle_timeout) so a probe
    /// precedes disconnection.
    pub heartbeat_interval: Duration,
    /// How long an authenticated connection may be silent (no activity, not even a probe
    /// response) before it is treated as dead.
    pub idle_timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

/// Why a connection reached its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutKind {
    /// The connection did not authenticate within the handshake deadline (slow-loris).
    Handshake,
    /// The connection went silent past the idle deadline (a dead connection).
    Idle,
}

/// What a connection needs at the polled instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatAction {
    /// Nothing is due until this instant; the runtime may sleep until then.
    Wait(Instant),
    /// The connection has been silent for `heartbeat_interval`; send a probe.
    SendHeartbeat,
    /// A deadline passed; disconnect the connection for this reason.
    Disconnect(TimeoutKind),
}

/// A per-connection liveness monitor: a pure, time-injected policy whose
/// [`poll`](Heartbeat::poll) decides whether to wait, probe, or disconnect.
#[derive(Debug, Clone)]
pub struct Heartbeat {
    config: HeartbeatConfig,
    established_at: Instant,
    last_activity: Instant,
    last_probe: Instant,
    authenticated: bool,
}

impl Heartbeat {
    /// Start monitoring a connection established at `now` (not yet authenticated).
    #[must_use]
    pub fn new(config: HeartbeatConfig, now: Instant) -> Self {
        Self {
            config,
            established_at: now,
            last_activity: now,
            last_probe: now,
            authenticated: false,
        }
    }

    /// Record inbound activity (any received bytes, including a heartbeat response),
    /// which resets the idle deadline.
    pub fn on_activity(&mut self, now: Instant) {
        self.last_activity = now;
    }

    /// Record that the connection authenticated: the handshake deadline no longer
    /// applies; idle-timeout and probing begin from `now`.
    pub fn on_authenticated(&mut self, now: Instant) {
        self.authenticated = true;
        self.last_activity = now;
        self.last_probe = now;
    }

    /// Record that a heartbeat probe was sent, so [`poll`](Self::poll) does not re-emit
    /// [`SendHeartbeat`](HeartbeatAction::SendHeartbeat) on every tick.
    pub fn on_heartbeat_sent(&mut self, now: Instant) {
        self.last_probe = now;
    }

    /// Decide what the connection needs at `now`.
    ///
    /// All comparisons use `saturating_duration_since`, so an out-of-order `now` (before
    /// a recorded instant) is treated as zero elapsed rather than panicking.
    #[must_use]
    pub fn poll(&self, now: Instant) -> HeartbeatAction {
        if !self.authenticated {
            return if now.saturating_duration_since(self.established_at)
                >= self.config.handshake_timeout
            {
                HeartbeatAction::Disconnect(TimeoutKind::Handshake)
            } else {
                HeartbeatAction::Wait(self.established_at + self.config.handshake_timeout)
            };
        }

        if now.saturating_duration_since(self.last_activity) >= self.config.idle_timeout {
            return HeartbeatAction::Disconnect(TimeoutKind::Idle);
        }

        // Probe after `heartbeat_interval` of silence, measured from the later of the
        // last activity and the last probe (so a sent probe doesn't re-fire every tick).
        let probe_base = self.last_activity.max(self.last_probe);
        if now.saturating_duration_since(probe_base) >= self.config.heartbeat_interval {
            HeartbeatAction::SendHeartbeat
        } else {
            let idle_deadline = self.last_activity + self.config.idle_timeout;
            let probe_at = probe_base + self.config.heartbeat_interval;
            HeartbeatAction::Wait(idle_deadline.min(probe_at))
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{Session, SessionEvent, SessionId, SessionState};

    fn config() -> HeartbeatConfig {
        HeartbeatConfig {
            handshake_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(60),
        }
    }

    #[test]
    fn unauthenticated_should_disconnect_on_handshake_deadline() {
        let t0 = Instant::now();
        let hb = Heartbeat::new(config(), t0);
        // Before the deadline: wait.
        assert!(matches!(
            hb.poll(t0 + Duration::from_secs(9)),
            HeartbeatAction::Wait(_)
        ));
        // At the deadline: slow-loris disconnect.
        assert_eq!(
            hb.poll(t0 + Duration::from_secs(10)),
            HeartbeatAction::Disconnect(TimeoutKind::Handshake)
        );
    }

    #[test]
    fn authenticated_should_disconnect_on_idle_deadline() {
        let t0 = Instant::now();
        let mut hb = Heartbeat::new(config(), t0);
        hb.on_authenticated(t0);
        // No activity for `idle_timeout` → dead connection.
        assert_eq!(
            hb.poll(t0 + Duration::from_secs(60)),
            HeartbeatAction::Disconnect(TimeoutKind::Idle)
        );
    }

    #[test]
    fn activity_should_reset_the_idle_deadline() {
        let t0 = Instant::now();
        let mut hb = Heartbeat::new(config(), t0);
        hb.on_authenticated(t0);
        // Activity at 50s pushes the idle deadline to 110s: still alive at 100s (a probe
        // may be due, but not a disconnect), dead only at 110s.
        hb.on_activity(t0 + Duration::from_secs(50));
        assert!(!matches!(
            hb.poll(t0 + Duration::from_secs(100)),
            HeartbeatAction::Disconnect(_)
        ));
        assert_eq!(
            hb.poll(t0 + Duration::from_secs(110)),
            HeartbeatAction::Disconnect(TimeoutKind::Idle)
        );
    }

    #[test]
    fn should_send_a_heartbeat_after_the_interval_and_not_respam() {
        let t0 = Instant::now();
        let mut hb = Heartbeat::new(config(), t0);
        hb.on_authenticated(t0);

        // Silent for `heartbeat_interval` → probe.
        assert_eq!(
            hb.poll(t0 + Duration::from_secs(15)),
            HeartbeatAction::SendHeartbeat
        );
        // After sending, do not re-emit until the next interval.
        hb.on_heartbeat_sent(t0 + Duration::from_secs(15));
        assert!(matches!(
            hb.poll(t0 + Duration::from_secs(20)),
            HeartbeatAction::Wait(_)
        ));
        assert_eq!(
            hb.poll(t0 + Duration::from_secs(30)),
            HeartbeatAction::SendHeartbeat
        );
    }

    #[test]
    fn deadline_boundary_should_fire_at_cap_not_before() {
        let t0 = Instant::now();

        // Handshake: `deadline - 1ns` waits, exactly `deadline` disconnects (>= not >).
        let hs = Heartbeat::new(config(), t0);
        assert!(matches!(
            hs.poll(t0 + Duration::from_secs(10) - Duration::from_nanos(1)),
            HeartbeatAction::Wait(_)
        ));
        assert_eq!(
            hs.poll(t0 + Duration::from_secs(10)),
            HeartbeatAction::Disconnect(TimeoutKind::Handshake)
        );

        // Idle: same boundary.
        let mut idle = Heartbeat::new(config(), t0);
        idle.on_authenticated(t0);
        assert!(matches!(
            idle.poll(t0 + Duration::from_secs(60) - Duration::from_nanos(1)),
            HeartbeatAction::SendHeartbeat | HeartbeatAction::Wait(_)
        ));
        assert_eq!(
            idle.poll(t0 + Duration::from_secs(60)),
            HeartbeatAction::Disconnect(TimeoutKind::Idle)
        );
    }

    #[test]
    fn poll_should_disconnect_the_session_on_timeout() {
        // The policy composes with the FSM: a Disconnect verdict drives `time_out`.
        let t0 = Instant::now();
        let hb = Heartbeat::new(config(), t0);
        let mut session = Session::new(SessionId(1));

        assert_eq!(
            hb.poll(t0 + Duration::from_secs(10)),
            HeartbeatAction::Disconnect(TimeoutKind::Handshake)
        );
        // The runtime, on that verdict, times the session out.
        assert_eq!(session.time_out().unwrap(), SessionEvent::Timeout);
        assert_eq!(session.state(), &SessionState::Disconnected);
    }

    proptest! {
        /// Any sequence of activity/probe/advance never panics; `Wait` is always in the
        /// future; and a connection frozen (no activity) past its idle deadline stays
        /// disconnected.
        #[test]
        fn any_sequence_of_activity_and_polls_should_never_panic_and_stay_monotone(
            secs in proptest::collection::vec(0u64..200, 0..40),
        ) {
            let t0 = Instant::now();
            let mut hb = Heartbeat::new(config(), t0);
            hb.on_authenticated(t0);

            for &s in &secs {
                let now = t0 + Duration::from_secs(s);
                match hb.poll(now) {
                    HeartbeatAction::Wait(until) => prop_assert!(until > now),
                    HeartbeatAction::SendHeartbeat => hb.on_heartbeat_sent(now),
                    HeartbeatAction::Disconnect(_) => {}
                }
            }

            // With no activity recorded, once past the idle deadline it is always dead.
            let dead = t0 + Duration::from_secs(60);
            prop_assert_eq!(hb.poll(dead), HeartbeatAction::Disconnect(TimeoutKind::Idle));
            prop_assert_eq!(
                hb.poll(dead + Duration::from_secs(100)),
                HeartbeatAction::Disconnect(TimeoutKind::Idle)
            );
        }
    }
}
