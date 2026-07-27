//! [`AuthMiddleware`] — the authentication stage of the pipeline.

use std::sync::Arc;

use async_trait::async_trait;
use gsf_session::{Authenticator, Credentials};

use crate::pipeline::{Middleware, Next, PipelineError};
use crate::request::Request;

/// Authenticates a request with a pluggable [`Authenticator`] (from `gsf-session`), then
/// lets it continue: on success the resulting [`Identity`](gsf_session::Identity) is
/// attached to the request's [`Extensions`](crate::Extensions) for downstream middleware
/// and the Room; on failure the message is rejected with [`PipelineError::Unauthorized`]
/// before it reaches any resource-consuming stage.
///
/// The presented [`Credentials`] are read from the request extensions (an upstream
/// connection stage attaches them).
pub struct AuthMiddleware {
    authenticator: Arc<dyn Authenticator>,
}

impl AuthMiddleware {
    /// Build the auth stage around a shared authenticator.
    #[must_use]
    pub fn new(authenticator: Arc<dyn Authenticator>) -> Self {
        Self { authenticator }
    }
}

#[async_trait]
impl Middleware for AuthMiddleware {
    async fn handle(&self, mut request: Request, next: Next<'_>) -> Result<(), PipelineError> {
        // Clone the credentials so the extensions borrow is released before we re-borrow
        // mutably to attach the identity.
        let credentials = request
            .extensions()
            .get::<Credentials>()
            .cloned()
            .ok_or_else(|| PipelineError::Unauthorized {
                reason: "no credentials presented".to_string(),
            })?;

        match self.authenticator.authenticate(&credentials).await {
            Ok(identity) => {
                request.extensions_mut().insert(identity);
                next.run(request).await
            }
            Err(rejected) => Err(PipelineError::Unauthorized {
                reason: rejected.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use gsf_session::{AuthError, Identity};

    use super::*;
    use crate::pipeline::{Endpoint, Pipeline};
    use crate::request::ConnectionId;

    /// Accepts one token; maps it to a fixed subject.
    struct AcceptToken(&'static [u8]);
    #[async_trait]
    impl Authenticator for AcceptToken {
        async fn authenticate(&self, creds: &Credentials) -> Result<Identity, AuthError> {
            if creds.token() == self.0 {
                Ok(Identity::new("player-1"))
            } else {
                Err(AuthError::Rejected {
                    reason: "bad token".into(),
                })
            }
        }
    }

    /// Records the identity the request carried when it reached the endpoint.
    struct CaptureIdentity(Arc<Mutex<Option<Identity>>>);
    #[async_trait]
    impl Endpoint for CaptureIdentity {
        async fn deliver(&self, request: Request) -> Result<(), PipelineError> {
            *self.0.lock().unwrap() = request.extensions().get::<Identity>().cloned();
            Ok(())
        }
    }

    fn request_with_token(token: &[u8]) -> Request {
        let mut request = Request::new(ConnectionId(1), Bytes::new());
        request.extensions_mut().insert(Credentials::new(token));
        request
    }

    #[tokio::test]
    async fn auth_middleware_should_attach_identity_on_success() {
        let captured = Arc::new(Mutex::new(None));
        let pipeline = Pipeline::builder()
            .layer(AuthMiddleware::new(Arc::new(AcceptToken(b"secret"))))
            .build(CaptureIdentity(Arc::clone(&captured)));

        pipeline
            .dispatch(request_with_token(b"secret"))
            .await
            .unwrap();

        assert_eq!(*captured.lock().unwrap(), Some(Identity::new("player-1")));
    }

    #[tokio::test]
    async fn auth_middleware_should_reject_without_valid_credentials() {
        let captured = Arc::new(Mutex::new(None));
        let pipeline = Pipeline::builder()
            .layer(AuthMiddleware::new(Arc::new(AcceptToken(b"secret"))))
            .build(CaptureIdentity(Arc::clone(&captured)));

        let err = pipeline
            .dispatch(request_with_token(b"wrong"))
            .await
            .unwrap_err();

        assert!(matches!(err, PipelineError::Unauthorized { .. }));
        // Rejected early: the endpoint was never reached.
        assert_eq!(*captured.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn auth_middleware_should_reject_when_no_credentials_present() {
        let captured = Arc::new(Mutex::new(None));
        let pipeline = Pipeline::builder()
            .layer(AuthMiddleware::new(Arc::new(AcceptToken(b"secret"))))
            .build(CaptureIdentity(Arc::clone(&captured)));

        // No credentials attached.
        let request = Request::new(ConnectionId(1), Bytes::new());
        let err = pipeline.dispatch(request).await.unwrap_err();
        assert!(matches!(err, PipelineError::Unauthorized { .. }));
    }
}
