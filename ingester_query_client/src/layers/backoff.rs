//! Backoff layer.

use std::ops::ControlFlow;

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};

use crate::{
    error::DynError,
    error_classifier::{is_upstream_error, ErrorClassifier},
    layer::{Layer, QueryResponse},
};

/// Backoff layer.
///
/// This will retry and backoff if the initial request fails. This will NOT handle errors that occur during streaming.
/// The reason is that a) the majority of the errors occurs during the initial response and b) retrying during streaming
/// is generally impossible since parts of the stream might already be consumed (e.g. converted, aggregated, send back
/// to the user, ...).
#[derive(Debug)]
pub struct BackoffLayer<L>
where
    L: Layer,
{
    config: BackoffConfig,
    inner: L,
    should_retry: ErrorClassifier,
}

impl<L> BackoffLayer<L>
where
    L: Layer,
{
    /// Create new backoff wrapper.
    pub fn new(inner: L, config: BackoffConfig) -> Self {
        Self::new_with_classifier(inner, config, ErrorClassifier::new(is_upstream_error))
    }

    fn new_with_classifier(inner: L, config: BackoffConfig, should_retry: ErrorClassifier) -> Self {
        Self {
            config,
            inner,
            should_retry,
        }
    }
}

#[async_trait]
impl<L> Layer for BackoffLayer<L>
where
    L: Layer,
{
    type Request = L::Request;
    type ResponseMetadata = L::ResponseMetadata;
    type ResponsePayload = L::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        Backoff::new(&self.config)
            .retry_with_backoff("ingester request", || async {
                match self.inner.query(request.clone()).await {
                    Ok(res) => ControlFlow::Break(Ok(res)),
                    Err(e) if self.should_retry.matches(&e) => ControlFlow::Continue(e),
                    Err(e) => ControlFlow::Break(Err(e)),
                }
            })
            .await
            .map_err(DynError::new)?
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use crate::{
        error_classifier::{test_error_classifier, TestError},
        layers::testing::{TestLayer, TestResponse},
    };

    use super::*;

    #[tokio::test]
    async fn test_ok() {
        TestCase {
            responses: [TestResponse::ok(())],
            outcome: Outcome::Ok,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_early_error_no_retry() {
        TestCase {
            responses: [TestResponse::err(DynError::new(TestError::NO_RETRY))],
            outcome: Outcome::EarlyError,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_early_error_retry() {
        TestCase {
            responses: [
                TestResponse::err(DynError::new(TestError::RETRY)),
                TestResponse::ok(()),
            ],
            outcome: Outcome::Ok,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_late_error_no_retry() {
        TestCase {
            responses: [TestResponse::ok(()).with_err_payload(DynError::new(TestError::NO_RETRY))],
            outcome: Outcome::LateError,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_late_error_flagged_retry_but_its_too_late() {
        // NOTE: even though the underlying error is marked as "retry", we won't retry because the error occurs during
        // the streaming phase.
        TestCase {
            responses: [TestResponse::ok(()).with_err_payload(DynError::new(TestError::RETRY))],
            outcome: Outcome::LateError,
        }
        .run()
        .await;
    }

    enum Outcome {
        Ok,
        EarlyError,
        LateError,
    }

    struct TestCase<const N: usize> {
        responses: [TestResponse<(), ()>; N],
        outcome: Outcome,
    }

    impl<const N: usize> TestCase<N> {
        async fn run(self) {
            let Self { responses, outcome } = self;

            let l = TestLayer::<(), (), ()>::default();
            for resp in responses {
                l.mock_response(resp);
            }

            let l = BackoffLayer::new_with_classifier(
                l,
                BackoffConfig::default(),
                test_error_classifier(),
            );

            match outcome {
                Outcome::Ok => {
                    let resp = l.query(()).await.unwrap();
                    resp.payload.try_collect::<Vec<_>>().await.unwrap();
                }
                Outcome::EarlyError => {
                    l.query(()).await.unwrap_err();
                }
                Outcome::LateError => {
                    let resp = l.query(()).await.unwrap();
                    resp.payload.try_collect::<Vec<_>>().await.unwrap_err();
                }
            }
        }
    }
}
