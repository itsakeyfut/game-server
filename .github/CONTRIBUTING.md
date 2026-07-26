# Contributing to game-server

Thank you for your interest in contributing! No contribution is too small — bug reports,
documentation improvements, and typo fixes are all equally welcome.

If you're unsure where to start, feel free to open an issue and ask.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Prerequisites](#prerequisites)
- [Ways to Contribute](#ways-to-contribute)
- [Issue Labels](#issue-labels)
- [Reporting Bugs](#reporting-bugs)
- [Feature Requests](#feature-requests)
- [Pull Requests](#pull-requests)
- [Commit Messages](#commit-messages)
- [Code Style](#code-style)
- [Testing](#testing)
- [Documentation](#documentation)
- [Project Layout](#project-layout)
- [License](#license)

---

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

---

## Prerequisites

This is a **pure-Rust** workspace — there are no system libraries or native dependencies to install.

**Rust toolchain**

```sh
rustup component add rustfmt clippy
```

The repository pins its toolchain via `rust-toolchain.toml` (stable channel + `rustfmt`/`clippy`), so `rustup`
selects the right toolchain automatically when you build. The **MSRV (Minimum Supported Rust Version) is 1.95**
(rolling policy: latest stable minus two); CI verifies it.

That's it — `cargo build` works out of the box.

---

## Ways to Contribute

- **Bug reports** — Something panics, misbehaves, or violates a documented guarantee
- **Documentation** — Missing or incorrect rustdoc comments, examples, or guides
- **Examples** — Realistic usage under `game/examples/` (e.g. chat, card-game)
- **Feature work** — Transport backends, codec/protocol, session/room logic, genre layers, cache data structures, services
- **Testing** — Property tests, network-simulation scenarios, fuzz targets, `loom` interleavings
- **Performance** — Profiling and reducing per-connection overhead, hot-path allocations, or serialization cost

Looking for a starting point? Check issues labeled [`good first issue`](https://github.com/itsakeyfut/game-server/issues?q=is%3Aopen+label%3A%22good+first+issue%22) or [`help wanted`](https://github.com/itsakeyfut/game-server/issues?q=is%3Aopen+label%3A%22help+wanted%22).

---

## Issue Labels

Issues and PRs are organised with a prefix system. Each label belongs to one family:

| Prefix | Meaning | Examples |
|---|---|---|
| `T-` | **Type** of work | `T-Feat`, `T-Bug`, `T-Doc`, `T-Test`, `T-Perf`, `T-Refactor`, `T-Maintenance`, `T-Question`, `T-Tracking-Issue` |
| `C-` | **Crate / component** | `C-transport`, `C-codec`, `C-session`, `C-room`, `C-cache-store`, `C-client`, … |
| `A-` | **Area** (cross-cutting, non-crate) | `A-ci`, `A-compliance`, `A-perf` |
| `P-` | **Priority** | `P-High`, `P-Medium`, `P-Low` |
| `S-` | **Status** in the workflow | `S-Needs-Triage`, `S-Needs-Design`, `S-Ready-For-Implementation`, `S-In-Progress`, `S-Blocked` |

Typical labelling: one `T-*` (type) + one or more `C-*` (affected crate) + a milestone; `P-*`/`S-*` optional.
`T-Tracking-Issue` marks a milestone/feature epic.

Finding work to pick up:

- New to the project? Start with [`good first issue`](https://github.com/itsakeyfut/game-server/issues?q=is%3Aopen+label%3A%22good+first+issue%22) or [`help wanted`](https://github.com/itsakeyfut/game-server/issues?q=is%3Aopen+label%3A%22help+wanted%22).
- `S-Ready-For-Implementation` means the design is settled — safe to start an implementation PR. `S-Needs-Design`
  means it needs discussion first.

---

## Reporting Bugs

Before filing a bug, search existing issues to avoid duplicates.

A good bug report includes:

1. **Description** — What happened and what you expected to happen
2. **Minimal reproduction** — The smallest code or test that reproduces the issue
3. **Versions**:
   - `rustc --version`
   - The affected `gsf-*` crate version(s)
   - Operating system and architecture
4. **Error output** — Full error message or panic backtrace (`RUST_BACKTRACE=1`)

The [Bug Report](https://github.com/itsakeyfut/game-server/issues/new?template=bug_report.yml) form walks you
through these fields.

---

## Feature Requests

Open an issue describing:

- The use case or problem you're trying to solve, and who benefits
- The crate/component it touches and any API-shape ideas
- Alternatives you considered

For changes that touch multiple crates or the public API surface, please discuss in an issue **before** starting
implementation.

---

## Pull Requests

1. **Open an issue first** for any non-trivial change (new features, API changes, or significant refactors).
2. Fork the repository and create a **topic branch** off `main`, named for the work (e.g.
   `feat/issue-42-quic-backend`, `fix/issue-51-codec-overflow`).
3. Make your changes. Each commit should build and pass tests independently.
4. Run the full check suite (see [Code Style](#code-style) and [Testing](#testing)).
5. Push your branch and open a PR against `main`, following the pull-request template.
6. Add new commits to address review feedback — do not force-push during review.

**PRs without tests will not be merged.** If a change is genuinely hard to test automatically, explain why in
the PR description.

---

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/), a **single line** with the crate/component as
scope:

```
feat(transport): add QUIC reliability-channel backend
```

Guidelines:

- **Type**: one of `feat`, `fix`, `test`, `refactor`, `docs`, `chore`, `perf`.
- **Scope**: the touched crate/component — `transport`, `codec`, `session`, `room`, `app`, `cache-store`, `ci`, …
  For workspace-wide changes use `workspace`.
- Imperative mood ("add", "fix", "remove"); no trailing period; keep the line concise (≈ 70 chars).
- **One line only** — no body. Reference the issue in the PR description (`Closes #N` / `Fixes #N`), not the commit.

Examples: `fix(codec): reject oversized length prefix` · `chore(ci): pin actions to commit SHAs` ·
`test(net-sim): proptest link determinism`.

---

## Code Style

Before submitting, run:

```sh
cargo fmt --all
cargo clippy --all --all-features -- -D warnings   # must pass with zero warnings
cargo doc --all-features --no-deps                 # docs must compile
```

Conventions:

- `#![forbid(unsafe_code)]` is the baseline. Any `unsafe` needs a `// SAFETY:` comment and explicit review; it is
  the rare exception, not the norm.
- **No panics in library code.** Malformed / untrusted input must never panic (packet/codec parsing is fuzzed).
- Return `Result` and use `?`; **no `unwrap()` / `expect()` / `panic!` outside `#[cfg(test)]`**.
- Libraries use **`thiserror`** (not `anyhow`); each crate has its own `Error` type wrapping lower errors with
  `#[from]`.
- Structured logging via `tracing` (`key = value` fields), never `println!`.

---

## Testing

Run the full suite:

```sh
cargo test --all --all-features
```

Match the test technique to the layer ([`docs/specs` — operability §3](https://github.com/itsakeyfut/game-server), private):

- **Pure logic / data structures** → unit tests + **property tests (`proptest`)**.
- **Reliability / transport** → the deterministic, seeded **network-simulation harness** (`gsf-net-sim`) — never
  the real network, so a failing scenario reproduces exactly.
- **Packet / codec parsing** → **`cargo-fuzz`** (deserialization must never panic on malformed input).
- **Concurrency** → **`loom`** for message-ordering / interleaving coverage.

Name tests `feature_should_expected_result`. New features and bug fixes must come with tests.

---

## Documentation

Public API items must have rustdoc comments — at least a one-line summary, plus a short example when the usage
isn't obvious:

```rust
/// Serialize a message to the wire format.
///
/// # Example
///
/// ```
/// # use gsf_codec::encode;
/// let bytes = encode(&Ping { seq: 1 })?;
/// # Ok::<_, gsf_codec::Error>(())
/// ```
pub fn encode<M: Message>(msg: &M) -> Result<Bytes, Error> { /* ... */ }
```

(The example above is illustrative — APIs evolve; check the crate's current rustdoc.)

---

## Project Layout

A single Cargo workspace in three groups with **strict one-way dependencies**:

```
foundation/   generic networked-server toolkit (game-agnostic)
              transport · codec · macros · session · middleware ·
              actor-runtime · security · observability · net-sim
game/         Game Server Framework  (app · room · services · genre layers · client)
cache/        Cache Server           (store · server · protocol · client)
```

- **Foundation knows nothing of game or cache.** Game and Cache are sibling applications that share Foundation
  and never depend on each other.
- Define a type in the lowest applicable crate and re-export upward; never define the same type twice.
- `#![forbid(unsafe_code)]` is the baseline across the workspace.

---

## License

By contributing to this project, you agree that your contributions will be licensed under the same terms as the
project: **MIT OR Apache-2.0**.

See [LICENSE-MIT](../LICENSE-MIT) and [LICENSE-APACHE](../LICENSE-APACHE) for details.
