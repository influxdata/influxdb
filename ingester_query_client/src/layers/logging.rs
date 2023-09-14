//! Logging layer.

use std::{sync::Arc, task::Poll};

use async_trait::async_trait;
use futures::StreamExt;
use observability_deps::tracing::{debug, error};

use crate::{
    error::DynError,
    layer::{Layer, QueryResponse},
};

/// Logging layer.
#[derive(Debug)]
pub struct LoggingLayer<L>
where
    L: Layer,
{
    addr: Arc<str>,
    inner: L,
}

impl<L> LoggingLayer<L>
where
    L: Layer,
{
    /// Create new logging layer.
    pub fn new(inner: L, addr: Arc<str>) -> Self {
        Self { addr, inner }
    }
}

#[async_trait]
impl<L> Layer for LoggingLayer<L>
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
        debug!(addr = self.addr.as_ref(), ?request, "ingester request",);

        match self.inner.query(request).await {
            Ok(QueryResponse {
                metadata,
                mut payload,
            }) => {
                debug!(addr = self.addr.as_ref(), "ingester response stream start",);

                let addr_captured = Arc::clone(&self.addr);

                Ok(QueryResponse {
                    metadata,
                    payload: futures::stream::poll_fn(move |cx| {
                        let res = payload.poll_next_unpin(cx);

                        match &res {
                            Poll::Ready(Some(Ok(_))) => {
                                debug!(
                                    addr = addr_captured.as_ref(),
                                    "ingester response stream response",
                                );
                            }
                            Poll::Ready(Some(Err(e))) => {
                                error!(
                                    addr=addr_captured.as_ref(),
                                    %e,
                                    "ingester response stream error",
                                );
                            }
                            Poll::Ready(None) => {
                                debug!(
                                    addr = addr_captured.as_ref(),
                                    "ingester response stream end",
                                );
                            }
                            Poll::Pending => {}
                        }

                        res
                    })
                    .boxed(),
                })
            }
            Err(e) => {
                error!(
                    addr=self.addr.as_ref(),
                    %e,
                    "failed ingester request",
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use test_helpers::tracing::TracingCapture;

    use crate::layers::testing::{TestLayer, TestResponse};

    use super::*;

    #[tokio::test]
    async fn test() {
        let l = TestLayer::<(), (), ()>::default();
        l.mock_response(TestResponse::err(DynError::from("error 1")));
        l.mock_response(
            TestResponse::ok(())
                .with_ok_payload(())
                .with_err_payload(DynError::from("error 2")),
        );
        l.mock_response(TestResponse::ok(()).with_ok_payload(()).with_ok_payload(()));
        let l = LoggingLayer::new(l, "foo.bar".into());

        let capture = TracingCapture::new();

        l.query(()).await.unwrap_err();
        l.query(())
            .await
            .unwrap()
            .payload
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        l.query(())
            .await
            .unwrap()
            .payload
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            capture.to_string(),
            [
                r#"level = DEBUG; message = ingester request; addr = "foo.bar"; request = (); "#,
                r#"level = ERROR; message = failed ingester request; addr = "foo.bar"; e = error 1; "#,
                r#"level = DEBUG; message = ingester request; addr = "foo.bar"; request = (); "#,
                r#"level = DEBUG; message = ingester response stream start; addr = "foo.bar"; "#,
                r#"level = DEBUG; message = ingester response stream response; addr = "foo.bar"; "#,
                r#"level = ERROR; message = ingester response stream error; addr = "foo.bar"; e = error 2; "#,
                r#"level = DEBUG; message = ingester request; addr = "foo.bar"; request = (); "#,
                r#"level = DEBUG; message = ingester response stream start; addr = "foo.bar"; "#,
                r#"level = DEBUG; message = ingester response stream response; addr = "foo.bar"; "#,
                r#"level = DEBUG; message = ingester response stream response; addr = "foo.bar"; "#,
                r#"level = DEBUG; message = ingester response stream end; addr = "foo.bar"; "#,
            ].join("\n")
        );
    }
}
