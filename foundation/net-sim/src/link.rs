//! A one-directional virtual link that injects faults deterministically.
//!
//! [`VirtualLink`] carries opaque [`Bytes`] payloads and, driven by a seeded
//! [`SimRng`], applies loss / bandwidth / latency / reorder / duplication exactly
//! as configured. Delivery times are scheduled against [`SimInstant`]; the caller
//! advances time and calls [`VirtualLink::deliver_due`] to collect packets that
//! have arrived. Same seed + same config + same sends ⇒ identical deliveries.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use bytes::Bytes;

use crate::rng::SimRng;
use crate::time::SimInstant;

/// A probability in `[0, 1]`. Construction clamps out-of-range values (and maps
/// `NaN` to `0`), so it can never make [`SimRng::chance`] misbehave.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Probability(f64);

impl Probability {
    /// Never happens.
    pub const ZERO: Probability = Probability(0.0);
    /// Always happens.
    pub const ONE: Probability = Probability(1.0);

    /// Clamp `value` into `[0, 1]` (`NaN` becomes `0`).
    #[must_use]
    pub fn new(value: f64) -> Self {
        if value.is_nan() {
            Self(0.0)
        } else {
            Self(value.clamp(0.0, 1.0))
        }
    }

    /// The underlying probability, in `[0, 1]`.
    #[must_use]
    pub fn value(self) -> f64 {
        self.0
    }
}

/// A link bandwidth, in bytes per second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bandwidth(u64);

impl Bandwidth {
    /// A bandwidth of `bytes_per_second` (clamped to at least 1 to avoid a
    /// divide-by-zero / infinite-delay link).
    #[must_use]
    pub fn bytes_per_second(bytes_per_second: u64) -> Self {
        Self(bytes_per_second.max(1))
    }

    /// Time to serialize `bytes` onto the link.
    #[must_use]
    fn transmission_time(self, bytes: usize) -> Duration {
        let nanos = u128::from(bytes as u64) * 1_000_000_000u128 / u128::from(self.0);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// Fault parameters for a [`VirtualLink`]. [`Default`] is a perfect link (no faults).
#[derive(Debug, Clone, Default)]
pub struct LinkConfig {
    /// Base one-way propagation delay.
    pub latency: Duration,
    /// Probability that a packet is dropped.
    pub loss: Probability,
    /// Probability that a packet is delayed by an extra `uniform[0, latency)`.
    ///
    /// Reordering *is* delay variance, and its scale is [`latency`](Self::latency):
    /// a packet can arrive up to one extra latency late and so overtake later
    /// packets. **This means reordering only occurs when `latency > 0`** — with
    /// `latency == 0` the extra delay is `uniform[0, 0) == 0`, so `reorder` has no
    /// effect and delivery stays FIFO.
    pub reorder: Probability,
    /// Probability that a packet is delivered a second time. The duplicate is a
    /// pure observation-level copy: it does **not** re-consume link bandwidth.
    pub duplicate: Probability,
    /// Optional bandwidth cap; `None` is an infinitely fast link.
    pub bandwidth: Option<Bandwidth>,
}

/// A packet waiting to be delivered, ordered by `(deliver_at, seq)`. `seq` is a
/// per-link monotonic tiebreaker so equal delivery times keep a stable order.
struct InFlight {
    deliver_at: SimInstant,
    seq: u64,
    payload: Bytes,
}

impl PartialEq for InFlight {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.seq == other.seq
    }
}
impl Eq for InFlight {}
impl PartialOrd for InFlight {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for InFlight {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deliver_at
            .cmp(&other.deliver_at)
            .then(self.seq.cmp(&other.seq))
    }
}
impl std::fmt::Debug for InFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlight")
            .field("deliver_at", &self.deliver_at)
            .field("seq", &self.seq)
            .field("len", &self.payload.len())
            .finish()
    }
}

/// A deterministic, fault-injecting, one-directional in-memory link.
#[derive(Debug)]
pub struct VirtualLink {
    config: LinkConfig,
    rng: SimRng,
    // Min-ordered by delivery time (via `Reverse`). Unbounded: the caller is
    // expected to drain via `deliver_due`; `pending()` exposes the in-flight count.
    queue: BinaryHeap<Reverse<InFlight>>,
    // When the serialized link next becomes free (for the bandwidth model).
    busy_until: SimInstant,
    seq: u64,
}

impl VirtualLink {
    /// Create a link with the given fault config, seeded for reproducibility.
    #[must_use]
    pub fn new(config: LinkConfig, seed: u64) -> Self {
        Self {
            config,
            rng: SimRng::new(seed),
            queue: BinaryHeap::new(),
            busy_until: SimInstant::START,
            seq: 0,
        }
    }

    /// Offer `payload` to the link at simulated time `now`. It is dropped,
    /// delayed, reordered, and/or duplicated per the config; nothing is returned —
    /// deliveries are collected later via [`deliver_due`](Self::deliver_due).
    pub fn send(&mut self, now: SimInstant, payload: Bytes) {
        // rng draw order is fixed (loss → primary reorder → duplicate → dup
        // reorder) so a given seed always produces the same schedule.
        if self.rng.chance(self.config.loss) {
            return;
        }

        // Bandwidth: serialize onto the link, queueing behind earlier packets.
        let start = now.max(self.busy_until);
        let finish = match self.config.bandwidth {
            Some(bw) => start.saturating_add(bw.transmission_time(payload.len())),
            None => start,
        };
        self.busy_until = finish;

        self.schedule(finish, payload.clone());
        if self.rng.chance(self.config.duplicate) {
            self.schedule(finish, payload);
        }
    }

    /// Compute a delivery time and enqueue the packet.
    fn schedule(&mut self, finish: SimInstant, payload: Bytes) {
        let latency = self.config.latency;
        let reorder = self.config.reorder;
        let mut deliver_at = finish.saturating_add(latency);
        if self.rng.chance(reorder) {
            deliver_at = deliver_at.saturating_add(self.uniform_below(latency));
        }
        let seq = self.seq;
        // `seq` breaks ties for equal delivery times; it only wraps after 2^64
        // sends (unreachable for any real scenario), so it stays unique in practice.
        self.seq = self.seq.wrapping_add(1);
        self.queue.push(Reverse(InFlight {
            deliver_at,
            seq,
            payload,
        }));
    }

    /// A uniformly random `Duration` in `[0, max)`.
    fn uniform_below(&mut self, max: Duration) -> Duration {
        let nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(self.rng.below(nanos))
    }

    /// Pop and return every packet whose delivery time is `<= now`, in delivery
    /// order (`(deliver_at, seq)`).
    pub fn deliver_due(&mut self, now: SimInstant) -> Vec<Bytes> {
        let mut delivered = Vec::new();
        while self
            .queue
            .peek()
            .is_some_and(|item| item.0.deliver_at <= now)
        {
            if let Some(Reverse(item)) = self.queue.pop() {
                delivered.push(item.payload);
            }
        }
        delivered
    }

    /// The delivery time of the earliest in-flight packet, if any (lets a clock
    /// jump straight to the next event).
    #[must_use]
    pub fn next_delivery(&self) -> Option<SimInstant> {
        self.queue.peek().map(|item| item.0.deliver_at)
    }

    /// Number of packets still in flight.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn at(millis: u64) -> SimInstant {
        SimInstant::from_start(Duration::from_millis(millis))
    }

    fn pkt(byte: u8) -> Bytes {
        Bytes::from(vec![byte])
    }

    #[test]
    fn link_should_deliver_after_configured_latency() {
        let cfg = LinkConfig {
            latency: Duration::from_millis(100),
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 1);
        link.send(SimInstant::START, pkt(0));

        assert!(
            link.deliver_due(at(99)).is_empty(),
            "not due before latency"
        );
        assert_eq!(link.deliver_due(at(100)), vec![pkt(0)], "due at latency");
    }

    #[test]
    fn link_should_drop_all_when_loss_is_one() {
        let cfg = LinkConfig {
            loss: Probability::ONE,
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 1);
        for i in 0..100 {
            link.send(SimInstant::START, pkt(i));
        }
        assert_eq!(link.pending(), 0);
        assert!(link.deliver_due(at(10_000)).is_empty());
    }

    #[test]
    fn link_should_deliver_all_when_loss_is_zero() {
        let mut link = VirtualLink::new(LinkConfig::default(), 1);
        for i in 0..100 {
            link.send(SimInstant::START, pkt(i));
        }
        assert_eq!(link.deliver_due(SimInstant::START).len(), 100);
    }

    #[test]
    fn link_should_duplicate_each_packet_when_duplicate_is_one() {
        let cfg = LinkConfig {
            duplicate: Probability::ONE,
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 1);
        for i in 0..10 {
            link.send(SimInstant::START, pkt(i));
        }
        let delivered = link.deliver_due(at(10_000));
        assert_eq!(delivered.len(), 20, "each of 10 packets delivered twice");
        for i in 0..10 {
            let count = delivered.iter().filter(|b| b[0] == i).count();
            assert_eq!(count, 2, "packet {i} should appear twice");
        }
    }

    #[test]
    fn link_should_preserve_order_without_reorder() {
        let cfg = LinkConfig {
            latency: Duration::from_millis(100),
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 1);
        for i in 0..10 {
            link.send(SimInstant::START, pkt(i));
        }
        let delivered: Vec<u8> = link.deliver_due(at(200)).iter().map(|b| b[0]).collect();
        assert_eq!(
            delivered,
            (0..10).collect::<Vec<u8>>(),
            "FIFO when reorder=0"
        );
    }

    #[test]
    fn link_should_reorder_reproducibly_with_seed() {
        let run = |seed: u64| -> Vec<u8> {
            let cfg = LinkConfig {
                latency: Duration::from_millis(100),
                reorder: Probability::ONE,
                ..Default::default()
            };
            let mut link = VirtualLink::new(cfg, seed);
            for i in 0..10 {
                link.send(SimInstant::START, pkt(i));
            }
            link.deliver_due(at(10_000)).iter().map(|b| b[0]).collect()
        };
        let first = run(0xC0FFEE);
        let second = run(0xC0FFEE);
        assert_eq!(first, second, "same seed reproduces the same order");
        assert_ne!(
            first,
            (0..10).collect::<Vec<u8>>(),
            "reorder should perturb the send order"
        );
    }

    #[test]
    fn link_should_serialize_deliveries_under_bandwidth_limit() {
        // 1000 B/s: a 1000-byte packet takes 1s to serialize.
        let cfg = LinkConfig {
            bandwidth: Some(Bandwidth::bytes_per_second(1000)),
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 1);
        let big = Bytes::from(vec![0u8; 1000]);
        link.send(SimInstant::START, big.clone());
        link.send(SimInstant::START, big);

        // First finishes at 1s, second queues behind it and finishes at 2s.
        assert_eq!(link.deliver_due(at(1000)).len(), 1, "only the first by 1s");
        assert!(
            link.deliver_due(at(1999)).is_empty(),
            "second not yet by ~2s"
        );
        assert_eq!(link.deliver_due(at(2000)).len(), 1, "second by 2s");
    }

    #[test]
    fn link_should_not_reorder_when_latency_is_zero() {
        // Reorder scales with latency, so a zero-latency link cannot reorder even
        // at reorder=1.0 — delivery stays FIFO (documented coupling).
        let cfg = LinkConfig {
            latency: Duration::ZERO,
            reorder: Probability::ONE,
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 0xC0FFEE);
        for i in 0..10 {
            link.send(SimInstant::START, pkt(i));
        }
        let delivered: Vec<u8> = link.deliver_due(at(10_000)).iter().map(|b| b[0]).collect();
        assert_eq!(
            delivered,
            (0..10).collect::<Vec<u8>>(),
            "FIFO when latency=0"
        );
    }

    #[test]
    fn link_should_bound_reorder_delay_within_latency() {
        // With reorder=1.0 every delivery time is in [latency, 2*latency).
        let cfg = LinkConfig {
            latency: Duration::from_millis(100),
            reorder: Probability::ONE,
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 0xC0FFEE);
        for i in 0..10 {
            link.send(SimInstant::START, pkt(i));
        }
        assert!(
            link.deliver_due(at(99)).is_empty(),
            "never delivered before base latency"
        );
        assert_eq!(
            link.deliver_due(at(200)).len(),
            10,
            "reorder delay is bounded by one extra latency (< 2x)"
        );
    }

    #[test]
    fn next_delivery_should_be_none_when_empty() {
        let link = VirtualLink::new(LinkConfig::default(), 1);
        assert_eq!(link.next_delivery(), None);
    }

    #[test]
    fn next_delivery_should_return_earliest_and_advance() {
        let cfg = LinkConfig {
            latency: Duration::from_millis(100),
            ..Default::default()
        };
        let mut link = VirtualLink::new(cfg, 1);
        link.send(at(0), pkt(0)); // delivered at 100ms
        link.send(at(50), pkt(1)); // delivered at 150ms

        assert_eq!(link.next_delivery(), Some(at(100)), "earliest first");
        assert_eq!(link.deliver_due(at(100)).len(), 1);
        assert_eq!(
            link.next_delivery(),
            Some(at(150)),
            "advances after delivery"
        );
        assert_eq!(link.deliver_due(at(150)).len(), 1);
        assert_eq!(link.next_delivery(), None, "none once drained");
    }

    #[test]
    fn link_should_apply_partial_loss_reproducibly() {
        let count = |seed: u64| -> usize {
            let cfg = LinkConfig {
                loss: Probability::new(0.5),
                ..Default::default()
            };
            let mut link = VirtualLink::new(cfg, seed);
            for _ in 0..200 {
                link.send(SimInstant::START, pkt(0));
            }
            link.deliver_due(SimInstant::START).len()
        };
        let delivered = count(7);
        assert_eq!(delivered, count(7), "partial loss reproduces for a seed");
        assert!(
            delivered > 0 && delivered < 200,
            "loss=0.5 drops some but not all (got {delivered})"
        );
    }

    proptest! {
        #[test]
        fn link_should_be_deterministic_for_same_seed(seed in any::<u64>(), n in 1usize..40) {
            let run = |seed: u64| -> Vec<Bytes> {
                let cfg = LinkConfig {
                    latency: Duration::from_millis(50),
                    loss: Probability::new(0.3),
                    reorder: Probability::new(0.5),
                    duplicate: Probability::new(0.2),
                    bandwidth: Some(Bandwidth::bytes_per_second(10_000)),
                };
                let mut link = VirtualLink::new(cfg, seed);
                for i in 0..n {
                    link.send(SimInstant::START, Bytes::from(vec![i as u8]));
                }
                link.deliver_due(SimInstant::from_start(Duration::from_secs(60)))
            };
            prop_assert_eq!(run(seed), run(seed));
        }

        #[test]
        fn probability_new_should_clamp_out_of_range(x in any::<f64>()) {
            let p = Probability::new(x).value();
            prop_assert!((0.0..=1.0).contains(&p));
        }
    }
}
