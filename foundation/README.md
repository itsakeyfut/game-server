# foundation/

The **generic, game-agnostic networked-server toolkit** that both applications in this repo — the
Game Server Framework (`game/`) and the Cache Server (`cache/`) — are built on. Think of it as the
shared plumbing: transport, serialization, session lifecycle, the middleware pipeline, the actor
runtime, security, and observability. It knows nothing about games or caching.

Crate directories carry a provisional `gsf-*` prefix (the neutral name is still undecided).

| Directory | Crate | Responsibility |
|---|---|---|
| `transport/` | `gsf-transport` | Reliability-channel abstraction; TCP / WebSocket / QUIC / custom reliable-UDP / WebTransport |
| `codec/` | `gsf-codec` | Serialize / framing / message registry / versioning (postcard internal, tagged public) |
| `macros/` | `gsf-macros` | Derive / attribute macros; neutral-schema generation (proc-macro) |
| `session/` | `gsf-session` | Connection-lifecycle FSM; auth; heartbeat; reconnect / resume |
| `middleware/` | `gsf-middleware` | `Service<Message>` pipeline; router |
| `actor-runtime/` | `gsf-actor-runtime` | Actor / mailbox primitives |
| `security/` | `gsf-security` | Encryption; source validation; rate limiting; per-connection resource caps |
| `observability/` | `gsf-observability` | tracing logs / metrics / stats |

## Dependency rules

- **Foundation depends on nothing above it.** It must never reference `game/` or `cache/`.
- Internal direction is one-way and acyclic: `codec → macros`, `session → {transport, codec}`,
  `middleware → session`. `security` / `observability` / `actor-runtime` are cross-cutting leaves used
  by higher layers.
- Keeping this boundary clean is deliberate: Foundation is the split candidate for eventual extraction
  into its own published crates / repository once the API stabilizes (~1.0).

`#![forbid(unsafe_code)]` is the baseline in every crate.
