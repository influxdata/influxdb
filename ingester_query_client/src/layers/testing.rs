//! Testing layer.
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::Barrier;

use crate::{
    error::DynError,
    layer::{Layer, QueryResponse},
};

/// Simplified version of [`QueryResponse`] for testing.
#[derive(Debug)]
pub struct TestResponse<ResponseMetadata, ResponsePayload>
where
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    barriers: Vec<Arc<Barrier>>,
    res: Result<(ResponseMetadata, Vec<Result<ResponsePayload, DynError>>), DynError>,
}

impl<ResponseMetadata, ResponsePayload> TestResponse<ResponseMetadata, ResponsePayload>
where
    ResponseMetadata: Clone + Debug + Default + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    /// Create OK response w/o any payload.
    pub fn ok(md: ResponseMetadata) -> Self {
        Self {
            barriers: vec![],
            res: Ok((md, vec![])),
        }
    }

    /// Create ERR response w/o any payload.
    pub fn err(e: DynError) -> Self {
        Self {
            barriers: vec![],
            res: Err(e),
        }
    }

    /// Add OK payload response.
    ///
    /// # Panic
    /// This is only legal for [`ok`](Self::ok) responses.
    pub fn with_ok_payload(mut self, payload: ResponsePayload) -> Self {
        self.res.as_mut().expect("ok status").1.push(Ok(payload));
        self
    }

    /// Add Err payload response.
    ///
    /// # Panic
    /// This is only legal for [`ok`](Self::ok) responses.
    pub fn with_err_payload(mut self, e: DynError) -> Self {
        self.res.as_mut().expect("ok status").1.push(Err(e));
        self
    }

    /// Add barrier to initial response (i.e. BEFORE the metadata returns).
    ///
    /// Multiple barriers can be stacked, e.g. to check if the response is pending and then block it.
    pub fn with_initial_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.barriers.push(barrier);
        self
    }
}

/// [`Layer`] for testing.
#[derive(Debug)]
pub struct TestLayer<Request, ResponseMetadata, ResponsePayload>
where
    Request: Clone + Debug + Send + Sync + 'static,
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    requests: Mutex<Vec<Request>>,
    responses: Mutex<Vec<TestResponse<ResponseMetadata, ResponsePayload>>>,
}

impl<Request, ResponseMetadata, ResponsePayload> Default
    for TestLayer<Request, ResponseMetadata, ResponsePayload>
where
    Request: Clone + Debug + Send + Sync + 'static,
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            requests: Default::default(),
            responses: Default::default(),
        }
    }
}

impl<Request, ResponseMetadata, ResponsePayload>
    TestLayer<Request, ResponseMetadata, ResponsePayload>
where
    Request: Clone + Debug + Send + Sync + 'static,
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    /// Get capctured requests.
    ///
    /// This does clone the current state. It does NOT reset/forget the captured requests.
    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().expect("not poisoned").clone()
    }

    /// Mock new response.
    pub fn mock_response(&self, response: TestResponse<ResponseMetadata, ResponsePayload>) {
        self.responses.lock().expect("not poisoned").push(response);
    }
}

impl<Request, ResponseMetadata, ResponsePayload> Drop
    for TestLayer<Request, ResponseMetadata, ResponsePayload>
where
    Request: Clone + Debug + Send + Sync + 'static,
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let responses = self.responses.lock().expect("not poisoned");
            if !responses.is_empty() {
                panic!("responses left");
            }
        }
    }
}

#[async_trait]
impl<Request, ResponseMetadata, ResponsePayload> Layer
    for TestLayer<Request, ResponseMetadata, ResponsePayload>
where
    Request: Clone + Debug + Send + Sync + 'static,
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    type Request = Request;
    type ResponseMetadata = ResponseMetadata;
    type ResponsePayload = ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        self.requests.lock().expect("not poisoned").push(request);

        let maybe_response = {
            let mut guard = self.responses.lock().expect("not poisoned");
            (!guard.is_empty()).then(|| guard.remove(0))
        };

        // assert AFTER dropping the lock guard
        let TestResponse { barriers, res } = maybe_response.expect("no response left");

        for barrier in barriers {
            barrier.wait().await;
        }

        res.map(|(metadata, payload)| QueryResponse {
            metadata,
            payload: futures::stream::iter(payload).boxed(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_impl;

    use super::*;

    #[test]
    #[should_panic(expected = "responses left")]
    fn test_response_left() {
        let l = TestLayer::<(), (), ()>::default();
        l.mock_response(TestResponse::ok(()));
    }

    #[test]
    #[should_panic(expected = "foo")]
    fn test_response_left_no_double_panic() {
        let l = TestLayer::<(), (), ()>::default();
        l.mock_response(TestResponse::ok(()));
        panic!("foo");
    }

    #[tokio::test]
    #[should_panic(expected = "no response left")]
    async fn test_no_response_left() {
        let l = TestLayer::<(), (), ()>::default();
        l.query(()).await.ok();
    }

    assert_impl!(
        default_test_layer_is_layer,
        TestLayer<(), (), ()>,
        crate::layer::Layer
    );
}
