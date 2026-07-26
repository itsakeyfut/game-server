# game/

The **Game Server Framework** — one of the two applications built on `foundation/`. It provides the
App/Plugin programming model, the Room actor runtime, opt-in genre layers (turn-based / realtime /
deterministic / mmo), reference services, and a reference client. Game-specific logic (rules, combat,
abilities, items, AI) is written by the *user* on top of this; the framework itself stays game-neutral.

Crate directories carry a provisional `gsf-*` prefix.

| Directory | Crate | Responsibility |
|---|---|---|
| `app/` | `gsf-app` | App + Plugin programming model; runtime wiring; server façade |
| `room/` | `gsf-room` | Room actor runtime; event bus; opt-in tick |
| `services/` | `gsf-services` | Service traits + reference impls (matchmaking, ranking, chat, presence, …) |
| `turnbased/` | `gsf-turnbased` | Turn-based genre layer (opt-in) |
| `realtime/` | `gsf-realtime` | Realtime (action) genre layer (opt-in) |
| `deterministic/` | `gsf-deterministic` | Deterministic / rollback genre layer (opt-in) |
| `mmo/` | `gsf-mmo` | MMO genre layer (opt-in) |
| `runtime/` | `gsf-runtime` | Assembly of app / room / services / genre layers |
| `server/` | `gsf-server` | Server façade |
| `client/` | `gsf-client` | Reference client; basis for cross-language SDKs |

## Dependency rules

- **Game depends on `foundation/` only. It must never depend on `cache/`.** The two applications are
  siblings that share Foundation and nothing else. (A game *may* use the cache at runtime via
  `gsf-cache-client` as a consumer, but no compile-time edge into `cache/` is created here.)
- Internal direction is one-way: `room → app`, `services → room`, each genre layer → `{room, services}`,
  `runtime → {app, room, services}`, `server → runtime`, `client → foundation`.
- Genre layers are **opt-in** (feature-gated), so a project pulls in only the model it needs.

`#![forbid(unsafe_code)]` is the baseline in every crate.
