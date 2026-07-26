# cache/

The **Cache Server** — the second application built on `foundation/`, a sibling to the Game Server
Framework (`game/`). A game-optimized cache: an embeddable in-memory core (L1) plus a standalone
network server (L2), speaking a protocol layered on the Foundation codec.

Crate directories carry a provisional `gsf-*` prefix (colliding names — `server`, `client` — are
disambiguated with a `cache` segment).

| Directory | Crate | Responsibility |
|---|---|---|
| `store/` | `gsf-cache-store` | In-memory data-structure core (embeddable L1) |
| `server/` | `gsf-cache-server` | Standalone L2 server |
| `protocol/` | `gsf-cache-protocol` | Cache protocol (on top of the Foundation codec) |
| `client/` | `gsf-cache-client` | Cache client (embedded + standalone) |

## Dependency rules

- **Cache depends on `foundation/` only. It must never depend on `game/`.** Game and Cache are siblings
  that share Foundation and nothing else.
- Internal direction is one-way: `protocol → {codec, store}`, `server → {protocol, store}`,
  `client → protocol`.

`#![forbid(unsafe_code)]` is the baseline in every crate.
