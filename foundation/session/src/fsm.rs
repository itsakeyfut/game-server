//! The connection-lifecycle finite state machine.
//!
//! A [`Session`] moves through `Connecting → Authenticating → Authenticated → InRoom →
//! Disconnected → Reconnecting` (runtime §1). States, events, and errors are all
//! enums — the machine is *expressed in the type system* — and every transition is a
//! named method that either returns the [`SessionEvent`] it emits or a
//! [`TransitionError`] for an illegal transition, leaving the state unchanged on error.
//!
//! ```
//! use gsf_session::{RoomId, Session, SessionEvent, SessionId};
//!
//! let mut session = Session::new(SessionId(1));
//! assert_eq!(session.begin_authentication().unwrap(), SessionEvent::AuthenticationStarted);
//! assert_eq!(session.authenticate().unwrap(), SessionEvent::Authenticated);
//! assert_eq!(
//!     session.bind_room(RoomId(7)).unwrap(),
//!     SessionEvent::RoomBound { room: RoomId(7) },
//! );
//! // An illegal transition is rejected and the state is left intact.
//! assert!(session.resume().is_err());
//! ```

/// A session's position-transparent identity (runtime §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

/// An opaque room-instance id a session is bound to (programming-model §1.1). The
/// concrete room lives in the game layer; foundation refers to it only by this id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoomId(pub u64);

/// The lifecycle state of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    /// The connection is established but not yet authenticating.
    Connecting,
    /// Authentication is in progress.
    Authenticating,
    /// Authenticated, but bound to no room.
    Authenticated,
    /// Authenticated and bound to one or more room instances.
    InRoom {
        /// The room instances this session is currently bound to (never empty in this
        /// state — the last unbind returns to [`Authenticated`](SessionState::Authenticated)).
        rooms: Vec<RoomId>,
    },
    /// The connection has dropped (grace period for resume is a higher layer's concern).
    Disconnected,
    /// A dropped session is attempting to resume its identity.
    Reconnecting,
}

impl SessionState {
    /// The data-less discriminant of this state, for diagnostics and matching.
    #[must_use]
    pub fn kind(&self) -> SessionStateKind {
        match self {
            SessionState::Connecting => SessionStateKind::Connecting,
            SessionState::Authenticating => SessionStateKind::Authenticating,
            SessionState::Authenticated => SessionStateKind::Authenticated,
            SessionState::InRoom { .. } => SessionStateKind::InRoom,
            SessionState::Disconnected => SessionStateKind::Disconnected,
            SessionState::Reconnecting => SessionStateKind::Reconnecting,
        }
    }
}

/// The data-less discriminant of a [`SessionState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionStateKind {
    /// See [`SessionState::Connecting`].
    Connecting,
    /// See [`SessionState::Authenticating`].
    Authenticating,
    /// See [`SessionState::Authenticated`].
    Authenticated,
    /// See [`SessionState::InRoom`].
    InRoom,
    /// See [`SessionState::Disconnected`].
    Disconnected,
    /// See [`SessionState::Reconnecting`].
    Reconnecting,
}

/// A lifecycle event emitted by a transition. Consumed by the event bus (game/room)
/// and the observability layer. `#[non_exhaustive]` — more events arrive with auth,
/// heartbeat, and the bus.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionEvent {
    /// Authentication has begun.
    AuthenticationStarted,
    /// The session authenticated successfully.
    Authenticated,
    /// The session was bound to a room instance.
    RoomBound {
        /// The room just bound.
        room: RoomId,
    },
    /// The session was unbound from a room instance.
    RoomUnbound {
        /// The room just unbound.
        room: RoomId,
    },
    /// The connection dropped.
    Disconnected,
    /// The connection was dropped by a heartbeat timeout.
    Timeout,
    /// The session began attempting to resume.
    ReconnectStarted,
    /// The session's identity was restored after a reconnect (runtime §1).
    Resumed,
}

/// An attempted transition that the current state does not permit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TransitionError {
    /// The transition is not legal from the current state.
    #[error("illegal transition `{transition}` from state {from:?}")]
    IllegalTransition {
        /// The state the session was in.
        from: SessionStateKind,
        /// The name of the attempted transition.
        transition: &'static str,
    },
    /// A room was bound that is already bound.
    #[error("room {room:?} is already bound")]
    RoomAlreadyBound {
        /// The room that was already bound.
        room: RoomId,
    },
    /// A room was unbound that is not bound.
    #[error("room {room:?} is not bound")]
    RoomNotBound {
        /// The room that was not bound.
        room: RoomId,
    },
}

/// A connection and its lifecycle state.
///
/// Transitions mutate the state in place and return the [`SessionEvent`] they emit;
/// an illegal transition returns a [`TransitionError`] and leaves the state unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    state: SessionState,
}

impl Session {
    /// Create a session in the initial [`Connecting`](SessionState::Connecting) state.
    #[must_use]
    pub fn new(id: SessionId) -> Self {
        Self {
            id,
            state: SessionState::Connecting,
        }
    }

    /// This session's identity.
    #[must_use]
    pub fn id(&self) -> SessionId {
        self.id
    }

    /// This session's current state.
    #[must_use]
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    fn illegal(&self, transition: &'static str) -> TransitionError {
        TransitionError::IllegalTransition {
            from: self.state.kind(),
            transition,
        }
    }

    /// `Connecting → Authenticating`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] unless in `Connecting`.
    pub fn begin_authentication(&mut self) -> Result<SessionEvent, TransitionError> {
        if self.state.kind() != SessionStateKind::Connecting {
            return Err(self.illegal("begin_authentication"));
        }
        self.state = SessionState::Authenticating;
        Ok(SessionEvent::AuthenticationStarted)
    }

    /// `Authenticating → Authenticated`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] unless in `Authenticating`.
    pub fn authenticate(&mut self) -> Result<SessionEvent, TransitionError> {
        if self.state.kind() != SessionStateKind::Authenticating {
            return Err(self.illegal("authenticate"));
        }
        self.state = SessionState::Authenticated;
        Ok(SessionEvent::Authenticated)
    }

    /// Bind a room instance: `Authenticated → InRoom`, or add to an existing `InRoom`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] unless in `Authenticated` or `InRoom`;
    /// [`TransitionError::RoomAlreadyBound`] if `room` is already bound.
    pub fn bind_room(&mut self, room: RoomId) -> Result<SessionEvent, TransitionError> {
        match &mut self.state {
            SessionState::InRoom { rooms } => {
                if rooms.contains(&room) {
                    return Err(TransitionError::RoomAlreadyBound { room });
                }
                rooms.push(room);
                return Ok(SessionEvent::RoomBound { room });
            }
            SessionState::Authenticated => {} // fall through: the borrow ends with the match
            other => {
                return Err(TransitionError::IllegalTransition {
                    from: other.kind(),
                    transition: "bind_room",
                });
            }
        }
        self.state = SessionState::InRoom { rooms: vec![room] };
        Ok(SessionEvent::RoomBound { room })
    }

    /// Unbind a room instance. Removing the last binding returns to `Authenticated`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] unless in `InRoom`;
    /// [`TransitionError::RoomNotBound`] if `room` is not bound.
    pub fn unbind_room(&mut self, room: RoomId) -> Result<SessionEvent, TransitionError> {
        let now_empty = match &mut self.state {
            SessionState::InRoom { rooms } => {
                let Some(pos) = rooms.iter().position(|r| *r == room) else {
                    return Err(TransitionError::RoomNotBound { room });
                };
                rooms.remove(pos);
                rooms.is_empty()
            }
            other => {
                return Err(TransitionError::IllegalTransition {
                    from: other.kind(),
                    transition: "unbind_room",
                });
            }
        };
        if now_empty {
            self.state = SessionState::Authenticated;
        }
        Ok(SessionEvent::RoomUnbound { room })
    }

    /// Drop the connection: any live state → `Disconnected`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] if already `Disconnected`.
    pub fn disconnect(&mut self) -> Result<SessionEvent, TransitionError> {
        if self.state.kind() == SessionStateKind::Disconnected {
            return Err(self.illegal("disconnect"));
        }
        self.state = SessionState::Disconnected;
        Ok(SessionEvent::Disconnected)
    }

    /// Drop the connection via heartbeat timeout: any live state → `Disconnected`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] if already `Disconnected`.
    pub fn time_out(&mut self) -> Result<SessionEvent, TransitionError> {
        if self.state.kind() == SessionStateKind::Disconnected {
            return Err(self.illegal("time_out"));
        }
        self.state = SessionState::Disconnected;
        Ok(SessionEvent::Timeout)
    }

    /// Begin a resume attempt: `Disconnected → Reconnecting`.
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] unless in `Disconnected`.
    pub fn begin_reconnect(&mut self) -> Result<SessionEvent, TransitionError> {
        if self.state.kind() != SessionStateKind::Disconnected {
            return Err(self.illegal("begin_reconnect"));
        }
        self.state = SessionState::Reconnecting;
        Ok(SessionEvent::ReconnectStarted)
    }

    /// Restore identity after a reconnect: `Reconnecting → Authenticated` (runtime §1;
    /// re-binding rooms is the state layer's job via `on_player_resume`).
    ///
    /// # Errors
    /// [`TransitionError::IllegalTransition`] unless in `Reconnecting`.
    pub fn resume(&mut self) -> Result<SessionEvent, TransitionError> {
        if self.state.kind() != SessionStateKind::Reconnecting {
            return Err(self.illegal("resume"));
        }
        self.state = SessionState::Authenticated;
        Ok(SessionEvent::Resumed)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn happy_path_should_transition_and_emit_the_expected_events() {
        let mut s = Session::new(SessionId(1));
        assert_eq!(s.state(), &SessionState::Connecting);

        assert_eq!(
            s.begin_authentication().unwrap(),
            SessionEvent::AuthenticationStarted
        );
        assert_eq!(s.state().kind(), SessionStateKind::Authenticating);

        assert_eq!(s.authenticate().unwrap(), SessionEvent::Authenticated);
        assert_eq!(s.state(), &SessionState::Authenticated);

        assert_eq!(
            s.bind_room(RoomId(7)).unwrap(),
            SessionEvent::RoomBound { room: RoomId(7) }
        );
        assert_eq!(
            s.state(),
            &SessionState::InRoom {
                rooms: vec![RoomId(7)]
            }
        );

        assert_eq!(s.disconnect().unwrap(), SessionEvent::Disconnected);
        assert_eq!(s.state(), &SessionState::Disconnected);

        assert_eq!(s.begin_reconnect().unwrap(), SessionEvent::ReconnectStarted);
        assert_eq!(s.state(), &SessionState::Reconnecting);

        assert_eq!(s.resume().unwrap(), SessionEvent::Resumed);
        assert_eq!(s.state(), &SessionState::Authenticated);
    }

    #[test]
    fn illegal_transitions_should_error_and_leave_state_unchanged() {
        // `authenticate` before authentication started.
        let mut s = Session::new(SessionId(1));
        let err = s.authenticate().unwrap_err();
        assert_eq!(
            err,
            TransitionError::IllegalTransition {
                from: SessionStateKind::Connecting,
                transition: "authenticate",
            }
        );
        assert_eq!(
            s.state(),
            &SessionState::Connecting,
            "state untouched on error"
        );

        // `bind_room` while only authenticating.
        s.begin_authentication().unwrap();
        assert!(s.bind_room(RoomId(1)).is_err());
        assert_eq!(s.state().kind(), SessionStateKind::Authenticating);

        // `resume` from a live (non-Reconnecting) state.
        s.authenticate().unwrap();
        assert!(s.resume().is_err());
        assert_eq!(s.state(), &SessionState::Authenticated);

        // `disconnect` twice: the second is illegal.
        s.disconnect().unwrap();
        assert!(s.disconnect().is_err());
        assert_eq!(s.state(), &SessionState::Disconnected);
    }

    #[test]
    fn in_room_should_track_zero_to_n_bindings() {
        let mut s = Session::new(SessionId(1));
        s.begin_authentication().unwrap();
        s.authenticate().unwrap();

        s.bind_room(RoomId(1)).unwrap();
        s.bind_room(RoomId(2)).unwrap();
        assert_eq!(
            s.state(),
            &SessionState::InRoom {
                rooms: vec![RoomId(1), RoomId(2)]
            }
        );

        // Duplicate bind is rejected.
        assert_eq!(
            s.bind_room(RoomId(1)).unwrap_err(),
            TransitionError::RoomAlreadyBound { room: RoomId(1) }
        );

        // Unbinding a non-bound room is rejected.
        assert_eq!(
            s.unbind_room(RoomId(9)).unwrap_err(),
            TransitionError::RoomNotBound { room: RoomId(9) }
        );

        // Unbind one, still InRoom; unbind the last, back to Authenticated.
        assert_eq!(
            s.unbind_room(RoomId(1)).unwrap(),
            SessionEvent::RoomUnbound { room: RoomId(1) }
        );
        assert_eq!(
            s.state(),
            &SessionState::InRoom {
                rooms: vec![RoomId(2)]
            }
        );
        s.unbind_room(RoomId(2)).unwrap();
        assert_eq!(s.state(), &SessionState::Authenticated);
    }

    #[test]
    fn time_out_should_disconnect_and_emit_timeout() {
        let mut s = Session::new(SessionId(1));
        s.begin_authentication().unwrap();
        assert_eq!(s.time_out().unwrap(), SessionEvent::Timeout);
        assert_eq!(s.state(), &SessionState::Disconnected);
    }

    #[derive(Debug, Clone)]
    enum Cmd {
        BeginAuth,
        Authenticate,
        BindRoom(u64),
        UnbindRoom(u64),
        Disconnect,
        TimeOut,
        BeginReconnect,
        Resume,
    }

    fn cmd_strategy() -> impl Strategy<Value = Cmd> {
        // A small room-id space so `bind`/`unbind` of the same room actually collide.
        prop_oneof![
            Just(Cmd::BeginAuth),
            Just(Cmd::Authenticate),
            (0u64..4).prop_map(Cmd::BindRoom),
            (0u64..4).prop_map(Cmd::UnbindRoom),
            Just(Cmd::Disconnect),
            Just(Cmd::TimeOut),
            Just(Cmd::BeginReconnect),
            Just(Cmd::Resume),
        ]
    }

    fn apply(session: &mut Session, cmd: &Cmd) -> Result<SessionEvent, TransitionError> {
        match cmd {
            Cmd::BeginAuth => session.begin_authentication(),
            Cmd::Authenticate => session.authenticate(),
            Cmd::BindRoom(r) => session.bind_room(RoomId(*r)),
            Cmd::UnbindRoom(r) => session.unbind_room(RoomId(*r)),
            Cmd::Disconnect => session.disconnect(),
            Cmd::TimeOut => session.time_out(),
            Cmd::BeginReconnect => session.begin_reconnect(),
            Cmd::Resume => session.resume(),
        }
    }

    proptest! {
        /// Any sequence of transitions must never panic, and a rejected transition must
        /// leave the state exactly as it was — the invariant a manager driving the FSM
        /// from arbitrary network events relies on.
        #[test]
        fn applying_any_transition_sequence_should_never_panic_and_keep_state_valid(
            cmds in proptest::collection::vec(cmd_strategy(), 0..40),
        ) {
            let mut session = Session::new(SessionId(1));
            for cmd in &cmds {
                let before = session.state().clone();
                if apply(&mut session, cmd).is_err() {
                    prop_assert_eq!(session.state(), &before);
                }
                // An `InRoom` state always holds at least one room.
                if let SessionState::InRoom { rooms } = session.state() {
                    prop_assert!(!rooms.is_empty());
                }
            }
        }
    }
}
