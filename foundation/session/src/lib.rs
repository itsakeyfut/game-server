//! Connection-lifecycle FSM: auth, heartbeat, reconnect / resume.
//!
//! The [`Session`] state machine drives a connection through
//! `Connecting → Authenticating → Authenticated → InRoom → Disconnected → Reconnecting`
//! (runtime §1); each transition emits a [`SessionEvent`] or returns a [`TransitionError`].
#![forbid(unsafe_code)]

mod fsm;

pub use fsm::{
    RoomId, Session, SessionEvent, SessionId, SessionState, SessionStateKind, TransitionError,
};
