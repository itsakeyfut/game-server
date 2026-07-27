//! The composable `Service<Message>` pipeline: a chain of [`Middleware`] a [`Request`]
//! flows through before reaching the [`Endpoint`] (the Room boundary).
//!
//! This is the framework's own "Tower analogy" (runtime §3) — a custom, dyn-friendly
//! onion chain rather than the `tower` crate. Each [`Middleware`] either calls
//! [`Next::run`] to continue (optionally observing the result on the way back out) or
//! returns early to short-circuit. The pipeline is fire-and-forward: a game response is
//! out-of-band (the Room's `ctx.reply`), so dispatch yields `Result<(), PipelineError>`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::request::Request;

/// One stage of the pipeline. Order-composable and unit-testable in isolation with a
/// mock [`Next`].
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Handle `request`, calling [`next.run`](Next::run) to pass it downstream or
    /// returning without calling it to short-circuit the chain.
    ///
    /// # Errors
    /// A [`PipelineError`] rejects the message (and, unless the middleware swallows it,
    /// propagates back out through the chain).
    async fn handle(&self, request: Request, next: Next<'_>) -> Result<(), PipelineError>;
}

/// The terminal of the pipeline — the **Room boundary** where a fully-processed message
/// is delivered (the real implementation resolves the destination room and posts to its
/// mailbox; that is a later issue).
#[async_trait]
pub trait Endpoint: Send + Sync {
    /// Deliver a fully-processed `request`.
    ///
    /// # Errors
    /// A [`PipelineError`] if delivery is refused.
    async fn deliver(&self, request: Request) -> Result<(), PipelineError>;
}

/// The continuation of the chain handed to a [`Middleware`]: the remaining middleware and
/// the terminal [`Endpoint`].
pub struct Next<'a> {
    remaining: &'a [Arc<dyn Middleware>],
    endpoint: &'a Arc<dyn Endpoint>,
}

impl Next<'_> {
    /// Pass `request` to the next middleware, or to the [`Endpoint`] if this is the end of
    /// the chain.
    ///
    /// # Errors
    /// Whatever the downstream middleware or endpoint returns.
    pub async fn run(self, request: Request) -> Result<(), PipelineError> {
        match self.remaining.split_first() {
            Some((head, tail)) => {
                let next = Next {
                    remaining: tail,
                    endpoint: self.endpoint,
                };
                head.handle(request, next).await
            }
            None => self.endpoint.deliver(request).await,
        }
    }
}

/// A built pipeline: an ordered chain of [`Middleware`] terminating at an [`Endpoint`].
/// Build one with [`Pipeline::builder`].
pub struct Pipeline {
    middleware: Vec<Arc<dyn Middleware>>,
    endpoint: Arc<dyn Endpoint>,
}

impl Pipeline {
    /// Start building a pipeline.
    #[must_use]
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::default()
    }

    /// Send `request` through the whole chain to the endpoint.
    ///
    /// # Errors
    /// Whatever a middleware or the endpoint returns.
    pub async fn dispatch(&self, request: Request) -> Result<(), PipelineError> {
        let next = Next {
            remaining: &self.middleware,
            endpoint: &self.endpoint,
        };
        next.run(request).await
    }
}

/// Builds a [`Pipeline`] by layering [`Middleware`] in order, then choosing an [`Endpoint`].
#[derive(Default)]
pub struct PipelineBuilder {
    middleware: Vec<Arc<dyn Middleware>>,
}

impl PipelineBuilder {
    /// Add a middleware to the end of the chain (runs after those already added).
    #[must_use]
    pub fn layer(mut self, middleware: impl Middleware + 'static) -> Self {
        self.middleware.push(Arc::new(middleware));
        self
    }

    /// Finish the pipeline with the terminal `endpoint` (the Room boundary).
    #[must_use]
    pub fn build(self, endpoint: impl Endpoint + 'static) -> Pipeline {
        Pipeline {
            middleware: self.middleware,
            endpoint: Arc::new(endpoint),
        }
    }
}

/// Why a message was rejected somewhere in the pipeline. `#[non_exhaustive]` — more
/// standard rejections (rate-limited, malformed, no-route) arrive with their middleware.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PipelineError {
    /// Authentication failed or was absent.
    #[error("unauthorized: {reason}")]
    Unauthorized {
        /// Why authentication was refused.
        reason: String,
    },
    /// A middleware rejected the message.
    #[error("rejected by `{middleware}`: {reason}")]
    Rejected {
        /// The middleware that rejected.
        middleware: &'static str,
        /// Why it rejected.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;

    use super::*;
    use crate::request::ConnectionId;

    type Log = Arc<Mutex<Vec<String>>>;

    fn request() -> Request {
        Request::new(ConnectionId(1), Bytes::new())
    }

    /// Logs when it runs, then continues down the chain.
    struct Trace {
        tag: &'static str,
        log: Log,
    }
    #[async_trait]
    impl Middleware for Trace {
        async fn handle(&self, request: Request, next: Next<'_>) -> Result<(), PipelineError> {
            self.log.lock().unwrap().push(format!("enter:{}", self.tag));
            let result = next.run(request).await;
            self.log.lock().unwrap().push(format!("leave:{}", self.tag));
            result
        }
    }

    /// Rejects without calling `next` (short-circuit).
    struct Reject;
    #[async_trait]
    impl Middleware for Reject {
        async fn handle(&self, _request: Request, _next: Next<'_>) -> Result<(), PipelineError> {
            Err(PipelineError::Rejected {
                middleware: "Reject",
                reason: "always".into(),
            })
        }
    }

    /// A mock endpoint that records the request's connection.
    struct RecordingEndpoint {
        log: Log,
    }
    #[async_trait]
    impl Endpoint for RecordingEndpoint {
        async fn deliver(&self, request: Request) -> Result<(), PipelineError> {
            self.log
                .lock()
                .unwrap()
                .push(format!("deliver:{}", request.connection().0));
            Ok(())
        }
    }

    #[tokio::test]
    async fn message_should_flow_through_the_chain_to_the_endpoint() {
        let log: Log = Log::default();
        let pipeline = Pipeline::builder()
            .layer(Trace {
                tag: "A",
                log: Arc::clone(&log),
            })
            .layer(Trace {
                tag: "B",
                log: Arc::clone(&log),
            })
            .build(RecordingEndpoint {
                log: Arc::clone(&log),
            });

        pipeline.dispatch(request()).await.unwrap();

        // Onion order: enter A, enter B, deliver, leave B, leave A.
        assert_eq!(
            *log.lock().unwrap(),
            ["enter:A", "enter:B", "deliver:1", "leave:B", "leave:A"]
        );
    }

    #[tokio::test]
    async fn a_middleware_that_short_circuits_should_stop_the_chain() {
        let log: Log = Log::default();
        let pipeline = Pipeline::builder()
            .layer(Trace {
                tag: "A",
                log: Arc::clone(&log),
            })
            .layer(Reject)
            .layer(Trace {
                tag: "C",
                log: Arc::clone(&log),
            })
            .build(RecordingEndpoint {
                log: Arc::clone(&log),
            });

        let err = pipeline.dispatch(request()).await.unwrap_err();
        assert!(matches!(err, PipelineError::Rejected { .. }));

        // A entered (and unwound), but C and the endpoint were never reached.
        let events = log.lock().unwrap().clone();
        assert_eq!(events, ["enter:A", "leave:A"]);
        assert!(!events.iter().any(|e| e == "enter:C"));
        assert!(!events.iter().any(|e| e.starts_with("deliver")));
    }

    #[tokio::test]
    async fn a_pipeline_with_no_middleware_should_reach_the_endpoint() {
        let log: Log = Log::default();
        let pipeline = Pipeline::builder().build(RecordingEndpoint {
            log: Arc::clone(&log),
        });
        pipeline.dispatch(request()).await.unwrap();
        assert_eq!(*log.lock().unwrap(), ["deliver:1"]);
    }

    /// Inserts a marker into the extensions for a downstream middleware to read.
    struct Enrich;
    #[async_trait]
    impl Middleware for Enrich {
        async fn handle(&self, mut request: Request, next: Next<'_>) -> Result<(), PipelineError> {
            request.extensions_mut().insert(42u32);
            next.run(request).await
        }
    }

    /// Rejects unless the extension marker is present (proving upstream enrichment ran).
    struct RequireMarker {
        log: Log,
    }
    #[async_trait]
    impl Middleware for RequireMarker {
        async fn handle(&self, request: Request, next: Next<'_>) -> Result<(), PipelineError> {
            match request.extensions().get::<u32>() {
                Some(&42) => {
                    self.log.lock().unwrap().push("saw-marker".into());
                    next.run(request).await
                }
                _ => Err(PipelineError::Rejected {
                    middleware: "RequireMarker",
                    reason: "missing marker".into(),
                }),
            }
        }
    }

    #[tokio::test]
    async fn middleware_should_enrich_the_request_for_downstream() {
        let log: Log = Log::default();
        let pipeline = Pipeline::builder()
            .layer(Enrich)
            .layer(RequireMarker {
                log: Arc::clone(&log),
            })
            .build(RecordingEndpoint {
                log: Arc::clone(&log),
            });
        pipeline.dispatch(request()).await.unwrap();
        assert_eq!(*log.lock().unwrap(), ["saw-marker", "deliver:1"]);
    }
}
