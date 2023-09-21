//! Serialize requests.
use crate::{
    error::DynError,
    interface::Request,
    layer::{Layer, QueryResponse},
};
use async_trait::async_trait;
use http::{header::InvalidHeaderValue, HeaderName, HeaderValue};
use ingester_query_grpc::{influxdata::iox::ingester::v2 as proto, FieldViolation};
use observability_deps::tracing::warn;
use snafu::{ResultExt, Snafu};
use trace::span::Span;
use trace_http::ctx::format_jaeger_trace_context;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("cannot serialize request: {source}"))]
    RequestSerialization {
        #[snafu(source(from(FieldViolation, Box::new)))]
        source: Box<FieldViolation>,
    },

    #[snafu(display("cannot serialize trace header: {source}"))]
    TraceHeader { source: InvalidHeaderValue },
}

impl From<Error> for DynError {
    fn from(e: Error) -> Self {
        Self::new(e)
    }
}

/// Serialize data for gRPC.
///
/// Note if the filters is are "complicated" to be serialized simply
/// ask for all the data from the ingester. More details:
/// <https://github.com/apache/arrow-datafusion/issues/3968>
#[derive(Debug)]
pub struct SerializeLayer<L>
where
    L: Layer<Request = (proto::QueryRequest, Vec<(HeaderName, HeaderValue)>)>,
{
    /// Header that transmits the trace context.
    trace_context_header_name: HeaderName,

    /// Inner layer.
    inner: L,
}

impl<L> SerializeLayer<L>
where
    L: Layer<Request = (proto::QueryRequest, Vec<(HeaderName, HeaderValue)>)>,
{
    /// Create new serialization layer.
    pub fn new(inner: L, trace_context_header_name: HeaderName) -> Self {
        Self {
            trace_context_header_name,
            inner,
        }
    }

    /// Get inner layer.
    pub fn inner(&self) -> &L {
        &self.inner
    }
}

#[async_trait]
impl<L> Layer for SerializeLayer<L>
where
    L: Layer<Request = (proto::QueryRequest, Vec<(HeaderName, HeaderValue)>)>,
{
    type Request = (Request, Option<Span>);
    type ResponseMetadata = L::ResponseMetadata;
    type ResponsePayload = L::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        let (request, span) = request;

        let request = encode_request(request).context(RequestSerializationSnafu)?;

        let mut headers = Vec::with_capacity(1);
        if let Some(span) = span {
            headers.push((
                self.trace_context_header_name.clone(),
                format_jaeger_trace_context(&span.ctx)
                    .try_into()
                    .context(TraceHeaderSnafu)?,
            ));
        }

        self.inner.query((request, headers)).await
    }
}

/// Encode request to gRPC.
fn encode_request(request: Request) -> Result<proto::QueryRequest, FieldViolation> {
    let Request {
        namespace_id,
        table_id,
        columns,
        mut filters,
    } = request;

    let filters = loop {
        match proto::Filters::try_from(filters.clone()) {
            Ok(filters) => {
                break filters;
            }
            Err(e) => {
                match (
                    e.field.strip_prefix("expr."),
                    SerializeFailureReason::extract_from_description(&e.description),
                ) {
                    (Some(idx), Some(reason)) => {
                        let idx: usize = idx.parse().expect("invalid error");
                        let filter = filters.remove(idx);
                        warn!(?filter, ?reason, "Cannot serialize predicate, stripping it",);
                        continue;
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
        }
    };

    Ok(proto::QueryRequest {
        namespace_id: namespace_id.get(),
        table_id: table_id.get(),
        columns,
        filters: Some(filters),
    })
}

#[derive(Debug)]
enum SerializeFailureReason {
    RecursionLimit,
    NotSupported,
}

impl SerializeFailureReason {
    fn extract_from_description(description: &str) -> Option<Self> {
        if description.contains("recursion limit reached") {
            Some(Self::RecursionLimit)
        } else if description.contains("not supported") {
            Some(Self::NotSupported)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::{NamespaceId, TableId};
    use datafusion::{
        logical_expr::LogicalPlanBuilder,
        prelude::{col, exists, lit, when, Expr},
    };

    use crate::{
        layers::testing::{TestLayer, TestResponse},
        testing::span,
    };

    use super::*;

    #[tokio::test]
    async fn test_ok_happy_path() {
        let filters = vec![col("x").eq(lit(1))];

        let l = TestLayer::<_, (), ()>::default();
        l.mock_response(TestResponse::ok(()));

        let l = SerializeLayer::new(l, "tracing-header".try_into().unwrap());

        l.query((
            Request {
                namespace_id: NamespaceId::new(1337),
                table_id: TableId::new(1),
                columns: vec![String::from("a"), String::from("b")],
                filters: filters.clone(),
            },
            Some(span()),
        ))
        .await
        .unwrap();

        let requests = l.inner().requests();
        assert_eq!(requests.len(), 1);
        let (request, headers) = requests.into_iter().next().unwrap();
        assert_eq!(
            request,
            proto::QueryRequest {
                namespace_id: 1337,
                table_id: 1,
                columns: vec![String::from("a"), String::from("b")],
                filters: Some(filters.try_into().unwrap()),
            },
        );
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers[0].0,
            HeaderName::try_from("tracing-header").unwrap()
        );
    }

    #[test]
    fn test_serialize_nasty_expressions() {
        // see https://github.com/influxdata/influxdb_iox/issues/5974

        // we need more stack space so this doesn't overflow in dev builds
        std::thread::Builder::new()
            .stack_size(10_000_000)
            .spawn(|| {
                let expr_simple = col("a").eq(lit(1));
                let expr_and = expr_deeply_nested_and();
                let expr_complex = expr_deeply_nested_complex();
                let expr_cannot = expr_cannot_serialize();

                let filters_in = vec![
                    expr_simple.clone(),
                    expr_and.clone(),
                    expr_complex,
                    expr_cannot,
                    expr_simple.clone(),
                ];
                let filters_out = vec![expr_simple.clone(), expr_and, expr_simple];

                let l = TestLayer::<_, (), ()>::default();
                l.mock_response(TestResponse::ok(()));
                let l = SerializeLayer::new(l, "tracing-header".try_into().unwrap());

                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(async {
                        l.query((
                            Request {
                                namespace_id: NamespaceId::new(1337),
                                table_id: TableId::new(1),
                                columns: vec![],
                                filters: filters_in,
                            },
                            None,
                        ))
                        .await
                        .unwrap();
                    });

                let requests = l.inner().requests();
                assert_eq!(requests.len(), 1);
                let (request, headers) = requests.into_iter().next().unwrap();
                assert_eq!(
                    request,
                    proto::QueryRequest {
                        namespace_id: 1337,
                        table_id: 1,
                        columns: vec![],
                        filters: Some(filters_out.try_into().unwrap()),
                    },
                );
                assert_eq!(headers.len(), 0);
            })
            .expect("spawning thread")
            .join()
            .expect("joining thread");
    }

    fn expr_deeply_nested_and() -> Expr {
        let expr_base = col("a").lt(lit(5i32));
        (0..100).fold(expr_base.clone(), |expr, _| expr.and(expr_base.clone()))
    }

    fn expr_deeply_nested_complex() -> Expr {
        (0..100).fold(lit(false), |expr, _| when(lit(true), expr).end().unwrap())
    }

    fn expr_cannot_serialize() -> Expr {
        let subquery = Arc::new(LogicalPlanBuilder::empty(true).build().unwrap());
        exists(subquery)
    }
}
