//! Integration test for epic E-M0-07: a request/reply round-trip over a lossy
//! simulated network, driven by the virtual clock with retransmission-on-timeout,
//! is testable and **reproducible** (same seeds ⇒ identical outcome).

use std::time::Duration;

use bytes::Bytes;
use gsf_net_sim::{LinkConfig, Probability, SimClock, SimInstant, VirtualLink};

const REQ: &[u8] = b"REQ";
const REP: &[u8] = b"REP";

/// Client A sends a request to server B over one lossy link; B replies over
/// another. A retransmits on a virtual-time timeout until the reply arrives.
/// Returns `(attempts, completion_time)` — a pure function of the seeds.
fn run_round_trip(seed: u64) -> (u32, SimInstant) {
    let link_cfg = || LinkConfig {
        latency: Duration::from_millis(50),
        loss: Probability::new(0.3),
        ..Default::default()
    };
    let mut a2b = VirtualLink::new(link_cfg(), seed);
    let mut b2a = VirtualLink::new(link_cfg(), seed ^ 0xA5A5_A5A5);
    let mut clock: SimClock<()> = SimClock::new();
    // RTO exceeds the 100ms round-trip so a successful exchange never triggers a
    // spurious retransmit; a lost packet is recovered when the timer fires.
    let rto = Duration::from_millis(200);

    let mut attempts = 0u32;
    a2b.send(clock.now(), Bytes::from_static(REQ));
    clock.schedule_after(rto, ());
    attempts += 1;

    loop {
        assert!(attempts < 1000, "round-trip failed to converge");

        // Jump to the next event across the clock and both links.
        let next = [
            clock.next_deadline(),
            a2b.next_delivery(),
            b2a.next_delivery(),
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("a pending timer or an in-flight packet always exists here");
        let fired = clock.advance_to(next);

        // Server: reply to each request that arrived.
        for _req in a2b.deliver_due(clock.now()) {
            b2a.send(clock.now(), Bytes::from_static(REP));
        }
        // Client: a reply completes the round-trip.
        if !b2a.deliver_due(clock.now()).is_empty() {
            return (attempts, clock.now());
        }
        // Timeout: retransmit and re-arm.
        if !fired.is_empty() {
            a2b.send(clock.now(), Bytes::from_static(REQ));
            clock.schedule_after(rto, ());
            attempts += 1;
        }
    }
}

#[test]
fn round_trip_should_complete_and_be_reproducible() {
    let first = run_round_trip(0x0D15_EA5E);
    let second = run_round_trip(0x0D15_EA5E);
    assert_eq!(
        first, second,
        "same seeds ⇒ identical (attempts, completion time)"
    );
    assert!(first.0 >= 1, "at least one attempt was made");
}

#[test]
fn round_trip_should_converge_across_seeds() {
    // Different seeds may need different attempt counts, but must all complete.
    for seed in [1u64, 2, 3, 7, 42, 0xBEEF, 0x1234_5678] {
        let (attempts, _t) = run_round_trip(seed);
        assert!(attempts >= 1, "seed {seed} completed");
    }
}
