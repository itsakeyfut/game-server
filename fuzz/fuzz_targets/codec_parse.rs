#![no_main]
//! Skeleton fuzz target for codec / packet parsing.
//!
//! `foundation/codec` has no parsing yet, so this only exercises the fuzzing
//! harness end to end (so `cargo fuzz run` and the CI smoke work). In M1 the body
//! is wired to `gsf-codec` decoding of arbitrary bytes, asserting it **never
//! panics** on malformed / truncated input.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // TODO(M1): decode `data` via gsf-codec and assert no panic / no UB.
    let _ = data;
});
