//! Pluggable authentication, folded into connection establishment.
//!
//! An [`Authenticator`] is the app-supplied policy that verifies a peer's
//! [`Credentials`] and yields an [`Identity`]. [`authenticate_session`] runs it as part
//! of the lifecycle handshake so an unauthenticated peer is **rejected early** — driven
//! straight to [`Disconnected`](crate::SessionState::Disconnected) before it can reach
//! any resource-consuming state (security §1.2). The identity model is
//! transport-independent (transport §1.1.1 A5): auth lives above the transport
//! abstraction, so the same hook serves TCP, WebSocket, and future QUIC.
//!
//! ```
//! use async_trait::async_trait;
//! use gsf_session::{
//!     authenticate_session, AuthError, Authenticator, Credentials, Identity, Session, SessionId,
//! };
//!
//! struct StaticToken(&'static [u8]);
//!
//! #[async_trait]
//! impl Authenticator for StaticToken {
//!     async fn authenticate(&self, creds: &Credentials) -> Result<Identity, AuthError> {
//!         if creds.token() == self.0 {
//!             Ok(Identity::new("player-1"))
//!         } else {
//!             Err(AuthError::Rejected { reason: "bad token".into() })
//!         }
//!     }
//! }
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let mut session = Session::new(SessionId(1));
//! let auth = StaticToken(b"secret");
//! let identity = authenticate_session(&mut session, &auth, &Credentials::new(*b"secret"))
//!     .await
//!     .unwrap();
//! assert_eq!(identity.subject(), "player-1");
//! # });
//! ```

use async_trait::async_trait;

use crate::{Session, TransitionError};

/// The credentials a peer presents during the auth handshake — an opaque token whose
/// concrete format is the [`Authenticator`]'s concern.
///
/// The token is a bearer secret, so [`Debug`] is redacted (never derived): logging or
/// tracing a `Credentials` must not leak the token (security §1, safe-by-default).
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    token: Vec<u8>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field(
                "token",
                &format_args!("<redacted {} bytes>", self.token.len()),
            )
            .finish()
    }
}

impl Credentials {
    /// Wrap a presented token.
    #[must_use]
    pub fn new(token: impl Into<Vec<u8>>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// The presented token bytes.
    #[must_use]
    pub fn token(&self) -> &[u8] {
        &self.token
    }
}

/// The authenticated principal a successful [`Authenticator`] returns — who the session
/// now acts as. Transport-independent (transport §1.1.1 A5), and distinct from a
/// [`SessionId`](crate::SessionId), which identifies the connection rather than the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    subject: String,
}

impl Identity {
    /// Build an identity for `subject` (the authenticated principal).
    #[must_use]
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
        }
    }

    /// The authenticated principal.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// Why an authentication attempt was rejected. `#[non_exhaustive]` — more reasons (e.g.
/// expired token, revoked session) arrive as the auth story grows.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// The credentials were not accepted.
    #[error("authentication rejected: {reason}")]
    Rejected {
        /// A human-readable rejection reason.
        reason: String,
    },
}

/// A failure of the auth handshake: either the credentials were rejected, or the
/// handshake was run on a session that was not in the expected state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HandshakeError {
    /// The authenticator rejected the credentials.
    #[error(transparent)]
    Auth(#[from] AuthError),
    /// The session was not in a state from which the handshake could run.
    #[error(transparent)]
    Transition(#[from] TransitionError),
}

/// A pluggable authentication policy (security §1.5). Supply your own token/session
/// validation; the framework runs it during connection establishment.
///
/// Typically stored behind an `Arc<dyn Authenticator>` and shared across connection
/// tasks, so implementors are `Send + Sync`.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Verify `credentials`, returning the authenticated [`Identity`] or an [`AuthError`].
    ///
    /// # Errors
    /// [`AuthError`] if the credentials are not accepted. Implementations may `await`
    /// I/O (e.g. a session-store or token-introspection lookup).
    async fn authenticate(&self, credentials: &Credentials) -> Result<Identity, AuthError>;
}

/// Run the authentication handshake for `session`, folding auth into connection
/// establishment (security §1.2).
///
/// On a [`Connecting`](crate::SessionState::Connecting) session this advances the FSM to
/// [`Authenticating`](crate::SessionState::Authenticating), runs `auth`, then on success
/// advances to [`Authenticated`](crate::SessionState::Authenticated) and returns the
/// [`Identity`]. On rejection it drives the session to
/// [`Disconnected`](crate::SessionState::Disconnected) — **rejected early**, before the
/// FSM can reach any room/resource-consuming state — and returns the error.
///
/// The `&mut Session` is held across the `.await`, so the state cannot change underneath
/// the handshake.
///
/// A **rejected** session lands in `Disconnected` and must be **discarded, not resumed**:
/// `Disconnected` is a shared state the reconnect layer (a later issue) treats as
/// resumable, so authorizing a resume is that layer's job — it must verify a resume
/// token, which a peer that never authenticated does not hold. This function never
/// re-promotes a session; do not route a rejected one back through reconnect.
///
/// **Cancellation:** this future does not clean up if dropped mid-`.await`. It begins in
/// `Authenticating` before awaiting the authenticator, so a cancelled handshake leaves
/// the session in `Authenticating` (never `Disconnected`). A handshake deadline (a later
/// issue) must therefore be enforced *inside* this flow (e.g. `select!` that also calls
/// `disconnect()` on timeout), **not** by wrapping the whole call in `tokio::time::timeout`
/// — an outer timeout would strand the session in `Authenticating` and defeat the
/// resource-recovery it exists for.
///
/// # Errors
/// [`HandshakeError::Transition`] if `session` is not `Connecting`;
/// [`HandshakeError::Auth`] if the authenticator rejects the credentials.
pub async fn authenticate_session(
    session: &mut Session,
    auth: &dyn Authenticator,
    credentials: &Credentials,
) -> Result<Identity, HandshakeError> {
    session.begin_authentication()?;
    match auth.authenticate(credentials).await {
        Ok(identity) => {
            session.authenticate()?;
            Ok(identity)
        }
        Err(rejected) => {
            // Reject early: drop to `Disconnected` before any resource use. `disconnect`
            // is always legal from `Authenticating`, so a returned event/error is moot.
            let _ = session.disconnect();
            Err(HandshakeError::Auth(rejected))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RoomId, SessionId, SessionState};

    /// Accepts exactly one token, mapping it to a fixed subject.
    struct AcceptToken {
        expected: &'static [u8],
        subject: &'static str,
    }

    #[async_trait]
    impl Authenticator for AcceptToken {
        async fn authenticate(&self, creds: &Credentials) -> Result<Identity, AuthError> {
            if creds.token() == self.expected {
                Ok(Identity::new(self.subject))
            } else {
                Err(AuthError::Rejected {
                    reason: "invalid token".to_string(),
                })
            }
        }
    }

    /// Rejects everything.
    struct RejectAll;

    #[async_trait]
    impl Authenticator for RejectAll {
        async fn authenticate(&self, _creds: &Credentials) -> Result<Identity, AuthError> {
            Err(AuthError::Rejected {
                reason: "closed".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn authenticate_session_should_accept_valid_credentials_and_reach_authenticated() {
        let mut session = Session::new(SessionId(1));
        let auth = AcceptToken {
            expected: b"secret",
            subject: "player-1",
        };

        let identity = authenticate_session(&mut session, &auth, &Credentials::new(*b"secret"))
            .await
            .unwrap();

        assert_eq!(identity, Identity::new("player-1"));
        assert_eq!(session.state(), &SessionState::Authenticated);
    }

    #[tokio::test]
    async fn authenticate_session_should_reject_invalid_credentials_early() {
        let mut session = Session::new(SessionId(1));
        let auth = AcceptToken {
            expected: b"secret",
            subject: "player-1",
        };

        let err = authenticate_session(&mut session, &auth, &Credentials::new(*b"wrong"))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            HandshakeError::Auth(AuthError::Rejected { .. })
        ));
        // Rejected early: never reached `Authenticated`.
        assert_eq!(session.state(), &SessionState::Disconnected);
    }

    #[tokio::test]
    async fn rejected_session_should_not_be_able_to_bind_a_room() {
        let mut session = Session::new(SessionId(1));

        let _ = authenticate_session(&mut session, &RejectAll, &Credentials::new(*b"x"))
            .await
            .unwrap_err();

        // An unauthenticated peer is `Disconnected` and cannot reach a resource state.
        assert_eq!(session.state(), &SessionState::Disconnected);
        assert!(session.bind_room(RoomId(1)).is_err());
    }

    #[tokio::test]
    async fn authenticate_session_should_error_on_a_non_connecting_session() {
        // Drive the session past `Connecting` first.
        let mut session = Session::new(SessionId(1));
        session.begin_authentication().unwrap();
        session.authenticate().unwrap();
        assert_eq!(session.state(), &SessionState::Authenticated);

        let auth = AcceptToken {
            expected: b"secret",
            subject: "player-1",
        };
        let err = authenticate_session(&mut session, &auth, &Credentials::new(*b"secret"))
            .await
            .unwrap_err();

        assert!(matches!(err, HandshakeError::Transition(_)));
        // The pre-existing state is left untouched.
        assert_eq!(session.state(), &SessionState::Authenticated);
    }

    #[tokio::test]
    async fn two_authenticator_impls_should_be_interchangeable() {
        // Both drive the same handshake through `&dyn Authenticator`, producing distinct
        // concrete outcomes (pluggability — not merely `is_ok`).
        let accept = AcceptToken {
            expected: b"ok",
            subject: "p",
        };

        // Accept impl: reaches `Authenticated` with the mapped identity.
        let mut accepted = Session::new(SessionId(1));
        let identity = authenticate_session(&mut accepted, &accept, &Credentials::new(*b"ok"))
            .await
            .unwrap();
        assert_eq!(identity, Identity::new("p"));
        assert_eq!(accepted.state(), &SessionState::Authenticated);

        // Reject impl: same call site, `&dyn Authenticator` — rejected to `Disconnected`.
        let mut rejected = Session::new(SessionId(2));
        let err = authenticate_session(&mut rejected, &RejectAll, &Credentials::new(*b"ok"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            HandshakeError::Auth(AuthError::Rejected { .. })
        ));
        assert_eq!(rejected.state(), &SessionState::Disconnected);
    }
}
