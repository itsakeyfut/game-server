//! A tiny self-contained deterministic PRNG.
//!
//! Uses **SplitMix64** — a fixed, well-known algorithm — rather than the `rand`
//! crate, because the whole point of the simulator is that *a failing scenario
//! reproduces exactly*. `rand`'s `StdRng` / `SmallRng` explicitly do **not**
//! guarantee the same output across crate versions or platforms; SplitMix64 with
//! fixed constants always produces the same stream for a given seed.

use crate::link::Probability;

/// A seedable, reproducible PRNG (SplitMix64).
///
/// Exposed publicly (beyond [`VirtualLink`](crate::VirtualLink)'s internal use) so
/// test authors can draw their own reproducible values from the same stream — e.g.
/// to generate deterministic payloads alongside a seeded link.
#[derive(Debug, Clone)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Create a PRNG from a seed. The same seed always yields the same stream.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next 64-bit value (SplitMix64).
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform `f64` in `[0, 1)` (top 53 bits, the full mantissa).
    pub fn next_f64(&mut self) -> f64 {
        // 53-bit integer / 2^53 is exact in f64 and always < 1.0.
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    /// Uniform integer in `[0, n)`; returns 0 when `n == 0`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            // Modulo bias is negligible for the ranges a simulator uses.
            self.next_u64() % n
        }
    }

    /// Draw a boolean that is `true` with the given probability.
    ///
    /// Exact at the extremes: `Probability::ONE` is always `true`, `ZERO` always
    /// `false` (since `next_f64()` is in `[0, 1)`).
    pub fn chance(&mut self, p: Probability) -> bool {
        self.next_f64() < p.value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_should_be_reproducible_for_same_seed() {
        let mut a = SimRng::new(42);
        let mut b = SimRng::new(42);
        let seq_a: Vec<u64> = (0..100).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..100).map(|_| b.next_u64()).collect();
        assert_eq!(seq_a, seq_b);
    }

    #[test]
    fn rng_should_differ_for_different_seeds() {
        let mut a = SimRng::new(1);
        let mut b = SimRng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn next_f64_should_stay_in_unit_interval() {
        let mut rng = SimRng::new(7);
        for _ in 0..10_000 {
            let x = rng.next_f64();
            assert!((0.0..1.0).contains(&x), "out of range: {x}");
        }
    }

    #[test]
    fn below_should_return_zero_for_zero_bound() {
        let mut rng = SimRng::new(0);
        assert_eq!(rng.below(0), 0);
    }

    #[test]
    fn below_should_respect_upper_bound() {
        let mut rng = SimRng::new(99);
        for _ in 0..10_000 {
            assert!(rng.below(10) < 10);
        }
    }

    #[test]
    fn chance_should_be_exact_at_extremes() {
        let mut rng = SimRng::new(123);
        for _ in 0..1_000 {
            assert!(rng.chance(Probability::ONE));
            assert!(!rng.chance(Probability::ZERO));
        }
    }
}
