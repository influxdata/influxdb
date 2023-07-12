//! Abstraction over RPC client

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use generated_types::influxdata::iox::ingester::v1::{
    write_service_client::WriteServiceClient, WriteRequest,
};
use thiserror::Error;
use trace::ctx::SpanContext;
use trace_http::ctx::format_jaeger_trace_context;

/// Request errors returned by [`WriteClient`] implementations.
#[derive(Debug, Error)]
pub enum RpcWriteClientError {
    /// The upstream connection is not established (lazy connection
    /// establishment).
    #[error("upstream {0} is not connected")]
    UpstreamNotConnected(String),

    /// The upstream ingester returned an error response.
    #[error("upstream ingester error: {0}")]
    Upstream(#[from] tonic::Status),

    /// The client is misconfigured and has produced an invalid request metadata key.
    #[error("misconfigured client producing invalid metadata key: {0}")]
    MisconfiguredMetadataKey(#[from] tonic::metadata::errors::InvalidMetadataKey),

    /// The client is misconfigured and has produced an invalid request metadata value.
    #[error("misconfigured client producing invalid metadata value: {0}")]
    MisconfiguredMetadataValue(#[from] tonic::metadata::errors::InvalidMetadataValue),
}

/// An abstract RPC client that pushes `op` to an opaque receiver.
#[async_trait]
pub(super) trait WriteClient: Send + Sync + std::fmt::Debug {
    /// Write `op` and wait for a response.
    async fn write(
        &self,
        op: WriteRequest,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), RpcWriteClientError>;
}

#[async_trait]
impl<T> WriteClient for Arc<T>
where
    T: WriteClient,
{
    async fn write(
        &self,
        op: WriteRequest,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), RpcWriteClientError> {
        (**self).write(op, span_ctx).await
    }
}

#[derive(Debug)]
pub(crate) struct TracePropagatingWriteClient<'a> {
    inner: WriteServiceClient<tonic::transport::Channel>,
    trace_context_header_name: &'a str,
}

impl<'a> TracePropagatingWriteClient<'a> {
    pub(crate) fn new(
        inner: WriteServiceClient<tonic::transport::Channel>,
        trace_context_header_name: &'a str,
    ) -> Self {
        Self {
            inner,
            trace_context_header_name,
        }
    }
}

#[async_trait]
impl<'a> WriteClient for TracePropagatingWriteClient<'a> {
    async fn write(
        &self,
        op: WriteRequest,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), RpcWriteClientError> {
        let req = decorate_request_with_span_context(
            tonic::Request::new(op),
            self.trace_context_header_name,
            span_ctx,
        )?;
        WriteServiceClient::write(&mut self.inner.clone(), req).await?;
        Ok(())
    }
}

fn decorate_request_with_span_context<T>(
    mut req: tonic::Request<T>,
    trace_context_header_name: &str,
    span_ctx: Option<SpanContext>,
) -> Result<tonic::Request<T>, RpcWriteClientError> {
    if let Some(span_ctx) = span_ctx {
        req.metadata_mut().insert(
            tonic::metadata::MetadataKey::from_bytes(trace_context_header_name.as_bytes())?,
            tonic::metadata::MetadataValue::try_from(&format_jaeger_trace_context(&span_ctx))?,
        );
    };
    Ok(req)
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use trace::{RingBufferTraceCollector, TraceCollector};
    use trace_http::ctx::TraceHeaderParser;

    use super::*;

    const ARBITRARY_TRACE_CONTEXT_HEADER_NAME: &str = "bananas";

    #[test]
    fn span_context_can_be_parsed_from_write_request() {
        // Initialise a trace context to bundle into the request.
        let trace_collector = Arc::new(RingBufferTraceCollector::new(5));
        let trace_observer: Arc<dyn TraceCollector> = Arc::new(Arc::clone(&trace_collector));
        let req_ctx = SpanContext::new(Arc::clone(&trace_observer));
        let req_span = req_ctx.child("request span");

        // Decorate the request with the context.
        let req = decorate_request_with_span_context(
            tonic::Request::new(WriteRequest::default()),
            ARBITRARY_TRACE_CONTEXT_HEADER_NAME,
            Some(req_span.ctx),
        )
        .expect("must be able to decorate request");

        // Parse the headers as it would be done by the middleware layer.
        let header_parser = TraceHeaderParser::new()
            .with_jaeger_trace_context_header_name(ARBITRARY_TRACE_CONTEXT_HEADER_NAME);
        let headers = req.metadata().clone().into_headers();
        let got_ctx = header_parser
            .parse(Some(trace_observer).as_ref(), &headers)
            .expect("must be able to parse a span context");

        // Ensure that the parsed context shares the trace ID and has the
        // request span as its parent.
        assert_matches!(got_ctx, Some(got_ctx) => {
            assert_eq!(got_ctx.trace_id, req_ctx.trace_id);
            assert_eq!(got_ctx.parent_span_id, Some(req_ctx.span_id));
        });
    }
}

/// Mocks for testing
pub mod mock {
    use super::*;
    use parking_lot::Mutex;
    use std::{fmt::Debug, iter, sync::Arc};

    struct State {
        calls: Vec<WriteRequest>,
        ret: Box<dyn Iterator<Item = Result<(), RpcWriteClientError>> + Send + Sync>,
        returned_oks: usize,
    }

    /// A mock implementation of the [`WriteClient`] for testing purposes.
    ///
    /// An instance yielded by the [`Default`] implementation will always return
    /// [`Ok(())`] for write calls.
    pub struct MockWriteClient {
        state: Mutex<State>,
    }

    impl Debug for MockWriteClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockWriteClient").finish()
        }
    }

    impl Default for MockWriteClient {
        fn default() -> Self {
            Self {
                state: Mutex::new(State {
                    calls: Default::default(),
                    ret: Box::new(iter::repeat_with(|| Ok(()))),
                    returned_oks: 0,
                }),
            }
        }
    }

    impl MockWriteClient {
        /// Retrieve the requests that this mock received.
        pub fn calls(&self) -> Vec<WriteRequest> {
            self.state.lock().calls.clone()
        }

        /// Retrieve the number of times this mock returned [`Ok`] to a write
        /// request.
        pub fn success_count(&self) -> usize {
            self.state.lock().returned_oks
        }

        /// Read values off of the provided iterator and return them for calls
        /// to [`Self::write()`].
        #[cfg(test)]
        pub(crate) fn with_ret<T, U>(self, ret: T) -> Self
        where
            T: IntoIterator<IntoIter = U>,
            U: Iterator<Item = Result<(), RpcWriteClientError>> + Send + Sync + 'static,
        {
            self.state.lock().ret = Box::new(ret.into_iter());
            self
        }
    }

    #[async_trait]
    impl WriteClient for Arc<MockWriteClient> {
        async fn write(
            &self,
            op: WriteRequest,
            _span_ctx: Option<SpanContext>,
        ) -> Result<(), RpcWriteClientError> {
            let mut guard = self.state.lock();
            guard.calls.push(op);

            let ret = guard.ret.next().expect("no mock response");

            if ret.is_ok() {
                guard.returned_oks += 1;
            }

            ret
        }
    }
}
