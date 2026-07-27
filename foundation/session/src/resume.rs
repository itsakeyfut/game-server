//! Reconnect / resume: restoring a session's identity across a disconnect.
//!
//! When an authenticated connection drops, the runtime keeps its identity resumable by
//! issuing an unforgeable [`ResumeToken`] into a [`ResumeRegistry`]; the client presents
//! that token when it reconnects, and [`resume_session`] verifies it, restores the
//! [`Identity`], and drives the FSM back to
//! [`Authenticated`](crate::SessionState::Authenticated), emitting
//! [`Resumed`](crate::SessionEvent::Resumed) (runtime §1's transport-layer half; the
//! state-layer replay is the Room's job). The token model is transport-independent
//! (transport §1.1.1 A5).
//!
//! Only an authenticated session is ever issued a token, so a peer that never
//! authenticated holds none and cannot resume. A token is a **bearer secret** — a
//! predictable or leaked one is a session takeover — so it is CSPRNG-random, redacted in
//! [`Debug`], expiring, and single-use.
//!
//! ```
//! use std::time::Instant;
//! use gsf_session::{resume_session, Identity, ResumeRegistry, Session, SessionId, SessionState};
//!
//! let now = Instant::now();
//! let mut registry = ResumeRegistry::default();
//!
//! // An authenticated session drops; the runtime parks its identity behind a token.
//! let token = registry.issue(Identity::new("player-1"), now);
//! let mut session = Session::new(SessionId(1));
//! session.begin_authentication().unwrap();
//! session.authenticate().unwrap();
//! session.disconnect().unwrap();
//!
//! // The client reconnects and presents the token: same identity restored.
//! let identity = resume_session(&mut session, &mut registry, &token, now).unwrap();
//! assert_eq!(identity, Identity::new("player-1"));
//! assert_eq!(session.state(), &SessionState::Authenticated);
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::{Identity, Session, TransitionError};

/// An unforgeable, single-use secret proving a reconnecting peer is a parked session.
///
/// 256 bits of CSPRNG entropy. [`Debug`] is redacted (never derived) — a leaked token is
/// a session takeover. Constructing one from bytes does not make it *valid*: only a token
/// a [`ResumeRegistry`] issued matches an entry.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResumeToken([u8; 32]);

impl ResumeToken {
    /// Rebuild a token from its wire bytes (e.g. what a reconnecting client presented).
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The token's bytes, for sending to the client.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for ResumeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the bytes: the token is a bearer secret.
        f.debug_tuple("ResumeToken").field(&"<redacted>").finish()
    }
}

/// Why a resume token could not be redeemed. `#[non_exhaustive]` — more reasons may come
/// with revocation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ResumeError {
    /// No such token — never issued, already redeemed, or forged.
    #[error("unknown resume token")]
    Unknown,
    /// The token was valid but its resume window has closed.
    #[error("resume token expired")]
    Expired,
}

/// A failure of the resume handshake: the token was rejected, or the session was not in a
/// state from which it could resume.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ResumeHandshakeError {
    /// The token was not accepted.
    #[error(transparent)]
    Resume(#[from] ResumeError),
    /// The session was not `Disconnected`, so it could not begin reconnecting.
    #[error(transparent)]
    Transition(#[from] TransitionError),
}

/// How long a parked session stays resumable.
#[derive(Debug, Clone, Copy)]
pub struct ResumeConfig {
    /// The resume window: a token expires this long after it is issued.
    pub ttl: Duration,
}

impl Default for ResumeConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
        }
    }
}

struct Record {
    identity: Identity,
    expires_at: Instant,
}

/// A store of outstanding resume tokens, each mapping to a parked [`Identity`] until it
/// expires (see the [module docs](crate)). Single-node; distributed resume is a later
/// concern (spec §2.2.6).
///
/// The registry only self-cleans on the paths that touch a token ([`redeem`](Self::redeem))
/// or on an explicit [`sweep`](Self::sweep); the runtime must call `sweep` periodically so
/// unredeemed entries cannot accumulate. An absolute per-connection resource cap is a
/// separate concern (security §1.3, a later issue) — issuance here is already gated on
/// authentication.
pub struct ResumeRegistry {
    ttl: Duration,
    entries: HashMap<ResumeToken, Record>,
}

impl Default for ResumeRegistry {
    fn default() -> Self {
        // Delegate to the config default (a real TTL) — deriving `Default` would zero the
        // `ttl` and expire every token instantly.
        Self::new(ResumeConfig::default())
    }
}

impl ResumeRegistry {
    /// A registry with the given config.
    #[must_use]
    pub fn new(config: ResumeConfig) -> Self {
        Self {
            ttl: config.ttl,
            entries: HashMap::new(),
        }
    }

    /// A registry whose tokens live for `ttl`.
    #[must_use]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: HashMap::new(),
        }
    }

    /// Park `identity` behind a fresh token, valid until `now + ttl`, and return the token.
    pub fn issue(&mut self, identity: Identity, now: Instant) -> ResumeToken {
        // 256 bits of CSPRNG entropy: unpredictable, so it cannot be forged. `random`
        // only fails on impossible OS-entropy loss.
        let token = ResumeToken(rand::random::<[u8; 32]>());
        self.entries.insert(
            token,
            Record {
                identity,
                expires_at: now + self.ttl,
            },
        );
        token
    }

    /// Redeem `token`, returning the parked [`Identity`]. The token is consumed whether or
    /// not it had expired (single-use: no replay).
    ///
    /// # Errors
    /// [`ResumeError::Unknown`] if the token is not outstanding; [`ResumeError::Expired`]
    /// if its resume window has closed.
    pub fn redeem(&mut self, token: &ResumeToken, now: Instant) -> Result<Identity, ResumeError> {
        match self.entries.remove(token) {
            None => Err(ResumeError::Unknown),
            Some(record) if now <= record.expires_at => Ok(record.identity),
            Some(_) => Err(ResumeError::Expired),
        }
    }

    /// Drop every expired entry, returning how many were removed. The runtime calls this
    /// periodically so unredeemed tokens cannot accumulate (a resource cap, security §1.3).
    pub fn sweep(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, record| now <= record.expires_at);
        before - self.entries.len()
    }

    /// The number of outstanding tokens.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no tokens are outstanding.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Run the resume handshake for a `Disconnected` `session`: verify `token`, and on success
/// drive the FSM `Disconnected → Reconnecting → Authenticated` (emitting
/// [`Resumed`](crate::SessionEvent::Resumed)), returning the restored [`Identity`].
///
/// On a bad or expired token the session is rolled back to
/// [`Disconnected`](crate::SessionState::Disconnected); a valid token is **not** consumed
/// if the session was not `Disconnected` to begin with.
///
/// # Errors
/// [`ResumeHandshakeError::Transition`] if `session` is not `Disconnected`;
/// [`ResumeHandshakeError::Resume`] if the token is rejected.
pub fn resume_session(
    session: &mut Session,
    registry: &mut ResumeRegistry,
    token: &ResumeToken,
    now: Instant,
) -> Result<Identity, ResumeHandshakeError> {
    session.begin_reconnect()?;
    match registry.redeem(token, now) {
        Ok(identity) => {
            session.resume()?;
            Ok(identity)
        }
        Err(rejected) => {
            // Roll the session back to `Disconnected`; `disconnect` is legal from
            // `Reconnecting`, so a returned event/error is moot.
            let _ = session.disconnect();
            Err(ResumeHandshakeError::Resume(rejected))
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{SessionEvent, SessionId, SessionState};

    /// A `Disconnected` session (the state a dropped connection is in).
    fn disconnected_session(id: u64) -> Session {
        let mut s = Session::new(SessionId(id));
        s.begin_authentication().unwrap();
        s.authenticate().unwrap();
        s.disconnect().unwrap();
        assert_eq!(s.state(), &SessionState::Disconnected);
        s
    }

    #[test]
    fn resume_should_restore_the_same_session() {
        let now = Instant::now();
        let mut registry = ResumeRegistry::with_ttl(Duration::from_secs(30));
        let token = registry.issue(Identity::new("player-1"), now);

        let mut session = disconnected_session(1);
        let identity = resume_session(&mut session, &mut registry, &token, now).unwrap();

        assert_eq!(identity, Identity::new("player-1"));
        assert_eq!(session.state(), &SessionState::Authenticated);
        // The token was consumed by a successful resume.
        assert!(registry.is_empty());
    }

    #[test]
    fn resume_should_emit_resumed() {
        // A fresh session driven through the resume path emits `Resumed` on the final step.
        let now = Instant::now();
        let mut session = disconnected_session(1);
        assert_eq!(
            session.begin_reconnect().unwrap(),
            SessionEvent::ReconnectStarted
        );
        assert_eq!(session.resume().unwrap(), SessionEvent::Resumed);
        let _ = now;
    }

    #[test]
    fn redeem_should_reject_an_unknown_token() {
        let now = Instant::now();
        let mut registry = ResumeRegistry::default();
        // A forged token (never issued) is not valid.
        let forged = ResumeToken::from_bytes([0xAB; 32]);
        assert_eq!(registry.redeem(&forged, now), Err(ResumeError::Unknown));
    }

    #[test]
    fn ttl_boundary_should_accept_at_expiry_and_reject_after() {
        let now = Instant::now();
        let ttl = Duration::from_secs(30);

        // Exactly at expiry: still valid, and restores the right identity.
        let mut reg = ResumeRegistry::with_ttl(ttl);
        let token = reg.issue(Identity::new("p"), now);
        assert_eq!(reg.redeem(&token, now + ttl), Ok(Identity::new("p")));

        // One tick past expiry: expired.
        let mut reg = ResumeRegistry::with_ttl(ttl);
        let token = reg.issue(Identity::new("p"), now);
        assert_eq!(
            reg.redeem(&token, now + ttl + Duration::from_nanos(1)),
            Err(ResumeError::Expired)
        );
    }

    #[test]
    fn redeem_should_be_one_time() {
        let now = Instant::now();
        let mut registry = ResumeRegistry::with_ttl(Duration::from_secs(30));
        let token = registry.issue(Identity::new("p"), now);

        assert!(registry.redeem(&token, now).is_ok());
        // A second redemption of the same token is rejected (replay protection).
        assert_eq!(registry.redeem(&token, now), Err(ResumeError::Unknown));
    }

    #[test]
    fn distinct_tokens_should_restore_their_own_identities() {
        // Fidelity of the token → identity map: each token restores its OWN identity, and
        // redeeming one does not affect the other.
        let now = Instant::now();
        let mut registry = ResumeRegistry::with_ttl(Duration::from_secs(30));
        let token_a = registry.issue(Identity::new("alice"), now);
        let token_b = registry.issue(Identity::new("bob"), now);

        assert_eq!(registry.redeem(&token_b, now), Ok(Identity::new("bob")));
        // `token_a` is untouched by `token_b`'s redemption.
        assert_eq!(registry.redeem(&token_a, now), Ok(Identity::new("alice")));
    }

    #[test]
    fn an_expired_token_should_be_consumed_by_redeeming_it() {
        // Documented: a token is consumed whether or not it had expired.
        let now = Instant::now();
        let ttl = Duration::from_secs(30);
        let mut registry = ResumeRegistry::with_ttl(ttl);
        let token = registry.issue(Identity::new("p"), now);

        let later = now + ttl + Duration::from_secs(1);
        assert_eq!(registry.redeem(&token, later), Err(ResumeError::Expired));
        // The expired token was consumed: a retry is now `Unknown`, not `Expired`.
        assert_eq!(registry.redeem(&token, later), Err(ResumeError::Unknown));
    }

    #[test]
    fn sweep_should_drop_only_expired_entries() {
        let now = Instant::now();
        let ttl = Duration::from_secs(30);
        let mut registry = ResumeRegistry::with_ttl(ttl);
        // `early` issued at `now` (expires at now+30); `late` issued 20s later (expires now+50).
        let early = registry.issue(Identity::new("early"), now);
        let late = registry.issue(Identity::new("late"), now + Duration::from_secs(20));

        // At now+40: only `early` has expired.
        assert_eq!(registry.sweep(now + Duration::from_secs(40)), 1);
        assert_eq!(
            registry.redeem(&early, now + Duration::from_secs(40)),
            Err(ResumeError::Unknown)
        );
        // `late` survived the sweep and still redeems.
        assert_eq!(
            registry.redeem(&late, now + Duration::from_secs(40)),
            Ok(Identity::new("late"))
        );
    }

    #[test]
    fn resume_session_should_reject_a_bad_token_and_stay_disconnected() {
        let now = Instant::now();
        let mut registry = ResumeRegistry::default();
        let mut session = disconnected_session(1);

        let forged = ResumeToken::from_bytes([0x00; 32]);
        let err = resume_session(&mut session, &mut registry, &forged, now).unwrap_err();

        assert!(matches!(
            err,
            ResumeHandshakeError::Resume(ResumeError::Unknown)
        ));
        // Rejected: rolled back, not restored.
        assert_eq!(session.state(), &SessionState::Disconnected);
    }

    #[test]
    fn resume_session_should_error_on_a_non_disconnected_session() {
        let now = Instant::now();
        let mut registry = ResumeRegistry::with_ttl(Duration::from_secs(30));
        let token = registry.issue(Identity::new("p"), now);

        // A connecting (not disconnected) session cannot resume.
        let mut session = Session::new(SessionId(1));
        let err = resume_session(&mut session, &mut registry, &token, now).unwrap_err();

        assert!(matches!(err, ResumeHandshakeError::Transition(_)));
        assert_eq!(session.state(), &SessionState::Connecting);
        // The valid token was not consumed.
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn sweep_should_drop_expired_entries() {
        let now = Instant::now();
        let mut registry = ResumeRegistry::with_ttl(Duration::from_secs(30));
        registry.issue(Identity::new("a"), now);
        registry.issue(Identity::new("b"), now);
        assert_eq!(registry.len(), 2);

        // Before expiry: nothing swept. After: both gone.
        assert_eq!(registry.sweep(now + Duration::from_secs(10)), 0);
        assert_eq!(registry.sweep(now + Duration::from_secs(31)), 2);
        assert!(registry.is_empty());
    }

    #[test]
    fn resume_token_debug_should_redact_the_secret() {
        let token = ResumeToken::from_bytes([0xAB; 32]);
        let shown = format!("{token:?}");
        assert!(shown.contains("redacted"));
        assert!(
            !shown.contains("171") && !shown.contains("ab"),
            "raw bytes must not appear"
        );
    }

    proptest! {
        /// Random issue / redeem / sweep sequences never panic; a successful redeem
        /// restores the token's OWN identity (fidelity) and only ever happens once per
        /// token (no replay). Each token maps to a distinct, indexed identity.
        #[test]
        fn any_issue_redeem_sweep_sequence_should_never_panic_and_stay_faithful(
            ops in prop::collection::vec(0u8..3, 0..60),
            offsets in prop::collection::vec(0u64..120, 0..60),
        ) {
            let base = Instant::now();
            let mut registry = ResumeRegistry::with_ttl(Duration::from_secs(30));
            let mut issued: Vec<ResumeToken> = Vec::new();
            let mut expected: std::collections::HashMap<ResumeToken, Identity> =
                std::collections::HashMap::new();
            let mut redeemed: Vec<ResumeToken> = Vec::new();
            let mut counter = 0u64;

            for (i, &op) in ops.iter().enumerate() {
                let offset = offsets.get(i).copied().unwrap_or(0);
                let now = base + Duration::from_secs(offset);
                match op {
                    0 => {
                        let identity = Identity::new(format!("p{counter}"));
                        counter += 1;
                        let token = registry.issue(identity.clone(), now);
                        expected.insert(token, identity);
                        issued.push(token);
                    }
                    1 => {
                        if !issued.is_empty() {
                            // Index (don't pop) so a token can be redeemed more than once,
                            // exercising the replay path.
                            let token = issued[(offset as usize) % issued.len()];
                            if let Ok(identity) = registry.redeem(&token, now) {
                                // Fidelity: the token's own identity, and a first-time
                                // success (a redeemed token never succeeds again).
                                prop_assert!(!redeemed.contains(&token));
                                prop_assert_eq!(&identity, &expected[&token]);
                                redeemed.push(token);
                            }
                        }
                    }
                    _ => {
                        let _ = registry.sweep(now);
                    }
                }
            }
        }
    }
}
