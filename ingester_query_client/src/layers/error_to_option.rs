//! Convenience layer to convert errors to option results.

use async_trait::async_trait;
use futures::StreamExt;

use crate::{
    error::DynError,
    error_classifier::{error_shall_be_ignored, ErrorClassifier},
    layer::{Layer, QueryResponse},
};

/// Convert [ignorable](error_shall_be_ignored) errors to `None` for initial response and empties paylad stream.
///
/// This will NOT touch the payload stream since this is usually too late anyways (data might already be processed or
/// even be streamed back to the user).
#[derive(Debug)]
pub struct ErrorToOptionLayer<L>
where
    L: Layer,
{
    inner: L,
    classifier: ErrorClassifier,
}

impl<L> ErrorToOptionLayer<L>
where
    L: Layer,
{
    /// Create new error-to-option wrapper.
    pub fn new(inner: L) -> Self {
        Self::new_with_classifier(inner, ErrorClassifier::new(error_shall_be_ignored))
    }

    fn new_with_classifier(inner: L, classifier: ErrorClassifier) -> Self {
        Self { inner, classifier }
    }
}

#[async_trait]
impl<L> Layer for ErrorToOptionLayer<L>
where
    L: Layer,
{
    type Request = L::Request;
    type ResponseMetadata = Option<L::ResponseMetadata>;
    type ResponsePayload = L::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        match self.inner.query(request).await {
            Ok(QueryResponse { metadata, payload }) => Ok(QueryResponse {
                metadata: Some(metadata),
                payload,
            }),
            Err(e) if self.classifier.matches(&e) => Ok(QueryResponse {
                metadata: None,
                payload: futures::stream::empty().boxed(),
            }),
            Err(e) => Err(e),
        }
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
        let l = TestLayer::<(), (), ()>::default();
        l.mock_response(TestResponse::ok(()).with_ok_payload(()));
        let l = ErrorToOptionLayer::new_with_classifier(l, test_error_classifier());

        let resp = l.query(()).await.unwrap();
        let payload = resp.payload.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(payload.len(), 1);
    }

    #[tokio::test]
    async fn test_err_ignore() {
        let l = TestLayer::<(), (), ()>::default();
        l.mock_response(TestResponse::err(DynError::new(TestError::RETRY)));
        let l = ErrorToOptionLayer::new_with_classifier(l, test_error_classifier());

        let resp = l.query(()).await.unwrap();
        let payload = resp.payload.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(payload.len(), 0);
    }

    #[tokio::test]
    async fn test_err_no_ignore() {
        let l = TestLayer::<(), (), ()>::default();
        l.mock_response(TestResponse::err(DynError::new(TestError::NO_RETRY)));
        let l = ErrorToOptionLayer::new_with_classifier(l, test_error_classifier());

        l.query(()).await.unwrap_err();
    }
}
