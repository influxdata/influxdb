//! Layer that reconnects on error.

use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
    task::Poll,
};

use async_trait::async_trait;
use futures::StreamExt;

use crate::{
    error::DynError,
    error_classifier::{is_upstream_error, ErrorClassifier},
    layer::{Layer, QueryResponse},
};

/// Layer that reconnects on error.
#[derive(Debug)]
pub struct ReconnectOnErrorLayer<L>
where
    L: Layer,
{
    inner: Arc<ReconnectOnErrorLayerInner<L>>,
}

impl<L> ReconnectOnErrorLayer<L>
where
    L: Layer,
{
    /// Create new reconnect layer.
    pub fn new<F>(constructor: F) -> Self
    where
        F: Fn() -> L + Send + Sync + 'static,
    {
        Self::new_with_classifier(constructor, ErrorClassifier::new(is_upstream_error))
    }

    fn new_with_classifier<F>(constructor: F, should_reconnect: ErrorClassifier) -> Self
    where
        F: Fn() -> L + Send + Sync + 'static,
    {
        let current_connection = Mutex::new(Arc::new(Connection {
            inner: constructor(),
            gen: 0,
        }));
        Self {
            inner: Arc::new(ReconnectOnErrorLayerInner {
                constructor: Box::new(constructor),
                current_connection,
                should_reconnect,
            }),
        }
    }
}

#[async_trait]
impl<L> Layer for ReconnectOnErrorLayer<L>
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
        let inner = Arc::clone(&self.inner);
        let connection = {
            let guard = inner.current_connection.lock().expect("not poisoned");
            Arc::clone(&guard)
        };

        match connection.inner.query(request).await {
            Ok(QueryResponse {
                metadata,
                mut payload,
            }) => Ok(QueryResponse {
                metadata,
                payload: futures::stream::poll_fn(move |cx| {
                    let res = payload.poll_next_unpin(cx);

                    match &res {
                        Poll::Ready(Some(Ok(_))) => {}
                        Poll::Ready(Some(Err(e))) => {
                            inner.maybe_reconnect(connection.gen, e);
                        }
                        Poll::Ready(None) => {}
                        Poll::Pending => {}
                    }

                    res
                })
                .boxed(),
            }),
            Err(e) => {
                inner.maybe_reconnect(connection.gen, &e);
                Err(e)
            }
        }
    }
}

struct ReconnectOnErrorLayerInner<L>
where
    L: Layer,
{
    constructor: Box<dyn Fn() -> L + Send + Sync>,
    current_connection: Mutex<Arc<Connection<L>>>,
    should_reconnect: ErrorClassifier,
}

impl<L> Debug for ReconnectOnErrorLayerInner<L>
where
    L: Layer,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectOnErrorLayerInner")
            .field("constructor", &"<CONSTRUCTOR>")
            .field("current_connection", &self.current_connection)
            .field("should_reconnect", &self.should_reconnect)
            .finish()
    }
}

impl<L> ReconnectOnErrorLayerInner<L>
where
    L: Layer,
{
    fn maybe_reconnect(&self, gen: u64, e: &DynError) {
        // check error before locking guard
        if !self.should_reconnect.matches(e) {
            return;
        }

        let mut guard = self.current_connection.lock().expect("not poisoned");
        if guard.gen != gen {
            // do not reconnect if we already did in the meantime
            return;
        }
        *guard = Arc::new(Connection {
            inner: (self.constructor)(),
            gen: guard.gen + 1,
        });
    }
}

#[derive(Debug)]
struct Connection<L>
where
    L: Layer,
{
    inner: L,
    gen: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use futures::{stream::FuturesOrdered, TryStreamExt};
    use tokio::sync::Barrier;

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
            reconnect: false,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_early_error_no_reconnect() {
        TestCase {
            responses: [TestResponse::err(DynError::new(TestError::NO_RETRY))],
            outcome: Outcome::EarlyError,
            reconnect: false,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_early_error_reconnect() {
        TestCase {
            responses: [TestResponse::err(DynError::new(TestError::RETRY))],
            outcome: Outcome::EarlyError,
            reconnect: true,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_late_error_no_reconnect() {
        TestCase {
            responses: [TestResponse::ok(()).with_err_payload(DynError::new(TestError::NO_RETRY))],
            outcome: Outcome::LateError,
            reconnect: false,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_late_error_reconnect() {
        TestCase {
            responses: [TestResponse::ok(()).with_err_payload(DynError::new(TestError::RETRY))],
            outcome: Outcome::LateError,
            reconnect: true,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn test_no_double_reconnect() {
        let barrier = Arc::new(Barrier::new(2));

        TestCase {
            responses: [
                TestResponse::ok(())
                    .with_initial_barrier(Arc::clone(&barrier))
                    .with_err_payload(DynError::new(TestError::RETRY)),
                TestResponse::ok(())
                    .with_initial_barrier(Arc::clone(&barrier))
                    .with_err_payload(DynError::new(TestError::RETRY)),
            ],
            outcome: Outcome::LateError,
            reconnect: true,
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
        reconnect: bool,
    }

    impl<const N: usize> TestCase<N> {
        async fn run(self) {
            let Self {
                responses,
                outcome,
                reconnect,
            } = self;

            let l = Arc::new(TestLayer::<(), (), ()>::default());
            let n = responses.len();
            for resp in responses {
                l.mock_response(resp);
            }

            let connected_count = Arc::new(AtomicU64::new(0));
            let connected_captured = Arc::clone(&connected_count);
            let l = ReconnectOnErrorLayer::new_with_classifier(
                move || {
                    connected_captured.fetch_add(1, Ordering::SeqCst);
                    Arc::clone(&l)
                },
                test_error_classifier(),
            );

            assert_eq!(connected_count.load(Ordering::SeqCst), 1);

            let mut futs = (0..n)
                .map(|_| async {
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
                })
                .collect::<FuturesOrdered<_>>();
            while futs.next().await.is_some() {}

            if reconnect {
                assert_eq!(connected_count.load(Ordering::SeqCst), 2);
            } else {
                assert_eq!(connected_count.load(Ordering::SeqCst), 1);
            }
        }
    }
}
