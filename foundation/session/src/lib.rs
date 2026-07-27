//! Connection-lifecycle FSM: auth, heartbeat, reconnect / resume.
//!
//! The [`Session`] state machine drives a connection through
//! `Connecting → Authenticating → Authenticated → InRoom → Disconnected → Reconnecting`
//! (runtime §1); each transition emits a [`SessionEvent`] or returns a [`TransitionError`].
//! A pluggable [`Authenticator`] gates the `Authenticating → Authenticated` step via
//! [`authenticate_session`], rejecting an unauthenticated peer early. A [`Heartbeat`]
//! policy decides when to probe an idle connection or disconnect a dead / slow-loris one.
//! A [`ResumeRegistry`] restores a dropped session's identity on reconnect via
//! [`resume_session`], emitting `Resumed`.
#![forbid(unsafe_code)]

mod auth;
mod fsm;
mod heartbeat;
mod resume;

pub use auth::{
    AuthError, Authenticator, Credentials, HandshakeError, Identity, authenticate_session,
};
pub use fsm::{
    RoomId, Session, SessionEvent, SessionId, SessionState, SessionStateKind, TransitionError,
};
pub use heartbeat::{Heartbeat, HeartbeatAction, HeartbeatConfig, TimeoutKind};
pub use resume::{
    ResumeConfig, ResumeError, ResumeHandshakeError, ResumeRegistry, ResumeToken, resume_session,
};
