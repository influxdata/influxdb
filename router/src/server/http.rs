//! HTTP service implementations for `router`.

pub mod write;

use std::{str::Utf8Error, time::Instant};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use hashbrown::HashMap;
use hyper::{header::CONTENT_ENCODING, Body, Method, Request, Response, StatusCode};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, U64Counter};
use mutable_batch::MutableBatch;
use mutable_batch_lp::LinesConverter;
use observability_deps::tracing::*;
use thiserror::Error;
use tokio::sync::{Semaphore, TryAcquireError};
use trace::ctx::SpanContext;

use self::write::{
    multi_tenant::MultiTenantExtractError, single_tenant::SingleTenantExtractError, WriteParams,
    WriteRequestUnifier,
};
use crate::{
    dml_handlers::{
        client::RpcWriteClientError, DmlError, DmlHandler, PartitionError, RetentionError,
        RpcWriteError, SchemaError,
    },
    namespace_resolver::NamespaceResolver,
};

/// Errors returned by the `router` HTTP request handler.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found")]
    NoHandler,

    /// A delete request was rejected (not supported).
    #[error("deletes are not supported")]
    DeletesUnsupported,

    /// An error parsing a single-tenant HTTP request.
    #[error(transparent)]
    SingleTenantError(#[from] SingleTenantExtractError),

    /// An error parsing a multi-tenant HTTP request.
    #[error(transparent)]
    MultiTenantError(#[from] MultiTenantExtractError),

    /// The request body content is not valid utf8.
    #[error("body content is not valid utf8: {0}")]
    NonUtf8Body(Utf8Error),

    /// The `Content-Encoding` header is invalid and cannot be read.
    #[error("invalid content-encoding header: {0}")]
    NonUtf8ContentHeader(hyper::header::ToStrError),

    /// The specified `Content-Encoding` is not acceptable.
    #[error("unacceptable content-encoding: {0}")]
    InvalidContentEncoding(String),

    /// The client disconnected.
    #[error("client disconnected")]
    ClientHangup(hyper::Error),

    /// The client sent a request body that exceeds the configured maximum.
    #[error("max request size ({0} bytes) exceeded")]
    RequestSizeExceeded(usize),

    /// Decoding a gzip-compressed stream of data failed.
    #[error("error decoding gzip stream: {0}")]
    InvalidGzip(std::io::Error),

    /// Failure to decode the provided line protocol.
    #[error("failed to parse line protocol: {0}")]
    ParseLineProtocol(mutable_batch_lp::Error),

    /// An error returned from the [`DmlHandler`].
    #[error("dml handler error: {0}")]
    DmlHandler(#[from] DmlError),

    /// An error that occurs when attempting to map the user-provided namespace
    /// name into a [`NamespaceId`].
    ///
    /// [`NamespaceId`]: data_types::NamespaceId
    #[error(transparent)]
    NamespaceResolver(#[from] crate::namespace_resolver::Error),

    /// The router is currently servicing the maximum permitted number of
    /// simultaneous requests.
    #[error("this service is overloaded, please try again later")]
    RequestLimit,

    /// The request has no authentication, but authorization is configured.
    #[error("authentication required")]
    Unauthenticated,

    /// The provided authorization is not sufficient to perform the request.
    #[error("access denied")]
    Forbidden,
}

impl Error {
    /// Convert the error into an appropriate [`StatusCode`] to be returned to
    /// the end user.
    pub fn as_status_code(&self) -> StatusCode {
        match self {
            Error::NoHandler => StatusCode::NOT_FOUND,
            Error::DeletesUnsupported => StatusCode::NOT_IMPLEMENTED,
            Error::ClientHangup(_) => StatusCode::BAD_REQUEST,
            Error::InvalidGzip(_) => StatusCode::BAD_REQUEST,
            Error::NonUtf8ContentHeader(_) => StatusCode::BAD_REQUEST,
            Error::NonUtf8Body(_) => StatusCode::BAD_REQUEST,
            Error::ParseLineProtocol(_) => StatusCode::BAD_REQUEST,
            Error::RequestSizeExceeded(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Error::InvalidContentEncoding(_) => {
                // https://www.rfc-editor.org/rfc/rfc7231#section-6.5.13
                StatusCode::UNSUPPORTED_MEDIA_TYPE
            }
            Error::DmlHandler(err) => StatusCode::from(err),
            // Error from the namespace resolver is 4xx if autocreation is disabled, 5xx otherwise
            Error::NamespaceResolver(crate::namespace_resolver::Error::Create(
                crate::namespace_resolver::ns_autocreation::NamespaceCreationError::Reject(_),
            )) => StatusCode::BAD_REQUEST,
            Error::NamespaceResolver(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::RequestLimit => StatusCode::SERVICE_UNAVAILABLE,
            Error::Unauthenticated => StatusCode::UNAUTHORIZED,
            Error::Forbidden => StatusCode::FORBIDDEN,
            Error::SingleTenantError(e) => StatusCode::from(e),
            Error::MultiTenantError(e) => StatusCode::from(e),
        }
    }
}

impl From<&DmlError> for StatusCode {
    fn from(e: &DmlError) -> Self {
        match e {
            DmlError::NamespaceNotFound(_) => StatusCode::NOT_FOUND,

            DmlError::Schema(SchemaError::ServiceLimit(_)) => {
                // https://docs.influxdata.com/influxdb/cloud/account-management/limits/#api-error-responses
                StatusCode::BAD_REQUEST
            }
            DmlError::Schema(SchemaError::Conflict(_)) => StatusCode::BAD_REQUEST,
            DmlError::Schema(SchemaError::UnexpectedCatalogError(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }

            DmlError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::Partition(PartitionError::BatchWrite(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::Partition(PartitionError::Partitioner(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            DmlError::Retention(RetentionError::OutsideRetention { .. }) => StatusCode::FORBIDDEN,
            DmlError::RpcWrite(RpcWriteError::Client(RpcWriteClientError::Upstream(_))) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            DmlError::RpcWrite(RpcWriteError::Client(
                RpcWriteClientError::MisconfiguredMetadataKey(_),
            )) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::RpcWrite(RpcWriteError::Client(
                RpcWriteClientError::MisconfiguredMetadataValue(_),
            )) => StatusCode::INTERNAL_SERVER_ERROR,
            DmlError::RpcWrite(RpcWriteError::Client(
                RpcWriteClientError::UpstreamNotConnected(_),
            )) => StatusCode::SERVICE_UNAVAILABLE,
            DmlError::RpcWrite(RpcWriteError::Timeout(_)) => StatusCode::GATEWAY_TIMEOUT,
            DmlError::RpcWrite(
                RpcWriteError::NoHealthyUpstreams
                | RpcWriteError::NotEnoughReplicas
                | RpcWriteError::PartialWrite { .. },
            ) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// This type is responsible for servicing requests to the `router` HTTP
/// endpoint.
///
/// Requests to some paths may be handled externally by the caller - the IOx
/// server runner framework takes care of implementing the heath endpoint,
/// metrics, pprof, etc.
#[derive(Debug)]
pub struct HttpDelegate<D, N, T = SystemProvider> {
    max_request_bytes: usize,
    time_provider: T,
    namespace_resolver: N,
    dml_handler: D,
    write_request_mode_handler: Box<dyn WriteRequestUnifier>,

    // A request limiter to restrict the number of simultaneous requests this
    // router services.
    //
    // This allows the router to drop a portion of requests when experiencing an
    // unusual flood of requests (i.e. due to peer routers crashing and
    // depleting the available instances in the pool) in order to preserve
    // overall system availability, instead of OOMing or otherwise failing.
    request_sem: Semaphore,

    write_metric_lines: U64Counter,
    http_line_protocol_parse_duration: DurationHistogram,
    write_metric_fields: U64Counter,
    write_metric_tables: U64Counter,
    write_metric_body_size: U64Counter,
    request_limit_rejected: U64Counter,
}

impl<D, N> HttpDelegate<D, N, SystemProvider> {
    /// Initialise a new [`HttpDelegate`] passing valid requests to the
    /// specified `dml_handler`.
    ///
    /// HTTP request bodies are limited to `max_request_bytes` in size,
    /// returning an error if exceeded.
    pub fn new(
        max_request_bytes: usize,
        max_requests: usize,
        namespace_resolver: N,
        dml_handler: D,
        metrics: &metric::Registry,
        write_request_mode_handler: Box<dyn WriteRequestUnifier>,
    ) -> Self {
        let write_metric_lines = metrics
            .register_metric::<U64Counter>(
                "http_write_lines",
                "cumulative number of line protocol lines successfully routed",
            )
            .recorder(&[]);
        let write_metric_fields = metrics
            .register_metric::<U64Counter>(
                "http_write_fields",
                "cumulative number of line protocol fields successfully routed",
            )
            .recorder(&[]);
        let write_metric_tables = metrics
            .register_metric::<U64Counter>(
                "http_write_tables",
                "cumulative number of tables in each write request",
            )
            .recorder(&[]);
        let write_metric_body_size = metrics
            .register_metric::<U64Counter>(
                "http_write_body_bytes",
                "cumulative byte size of successfully routed (decompressed) line protocol write requests",
            )
            .recorder(&[]);
        let request_limit_rejected = metrics
            .register_metric::<U64Counter>(
                "http_request_limit_rejected",
                "number of HTTP requests rejected due to exceeding parallel request limit",
            )
            .recorder(&[]);
        let http_line_protocol_parse_duration = metrics
            .register_metric::<DurationHistogram>(
                "http_line_protocol_parse_duration",
                "write latency of line protocol parsing",
            )
            .recorder(&[]);

        Self {
            max_request_bytes,
            time_provider: SystemProvider::default(),
            namespace_resolver,
            write_request_mode_handler,
            dml_handler,
            request_sem: Semaphore::new(max_requests),
            write_metric_lines,
            http_line_protocol_parse_duration,
            write_metric_fields,
            write_metric_tables,
            write_metric_body_size,
            request_limit_rejected,
        }
    }
}

impl<D, N, T> HttpDelegate<D, N, T>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>, WriteOutput = ()>,
    N: NamespaceResolver,
    T: TimeProvider,
{
    /// Routes `req` to the appropriate handler, if any, returning the handler
    /// response.
    pub async fn route(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        // Acquire and hold a permit for the duration of this request, or return
        // a 503 if the existing requests have already exhausted the allocation.
        //
        // By dropping requests at the routing stage, before the request buffer
        // is read/decompressed, this limit can efficiently shed load to avoid
        // unnecessary memory pressure (the resource this request limit usually
        // aims to protect.)
        let _permit = match self.request_sem.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                error!("simultaneous request limit exceeded - dropping request");
                self.request_limit_rejected.inc(1);
                return Err(Error::RequestLimit);
            }
            Err(e) => panic!("request limiter error: {e}"),
        };

        // Route the request to a handler.
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/write") => {
                let dml_info = self.write_request_mode_handler.parse_v1(&req).await?;
                self.write_handler(req, dml_info).await
            }
            (&Method::POST, "/api/v2/write") => {
                let dml_info = self.write_request_mode_handler.parse_v2(&req).await?;
                self.write_handler(req, dml_info).await
            }
            (&Method::POST, "/api/v2/delete") => return Err(Error::DeletesUnsupported),
            _ => return Err(Error::NoHandler),
        }
        .map(|_summary| {
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        })
    }

    async fn write_handler(
        &self,
        req: Request<Body>,
        write_info: WriteParams,
    ) -> Result<(), Error> {
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        trace!(
            namespace=%write_info.namespace,
            "processing write request"
        );

        // Read the HTTP body and convert it to a str.
        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        // The time, in nanoseconds since the epoch, to assign to any points that don't
        // contain a timestamp
        let default_time = self.time_provider.now().timestamp_nanos();
        let start_instant = Instant::now();

        let mut converter = LinesConverter::new(default_time);
        converter.set_timestamp_base(write_info.precision.timestamp_base());
        let (batches, stats) = match converter.write_lp(body).and_then(|_| converter.finish()) {
            Ok(v) => v,
            Err(mutable_batch_lp::Error::EmptyPayload) => {
                debug!("nothing to write");
                return Ok(());
            }
            Err(e) => return Err(Error::ParseLineProtocol(e)),
        };

        let num_tables = batches.len();
        let duration = start_instant.elapsed();
        self.http_line_protocol_parse_duration.record(duration);
        debug!(
            num_lines=stats.num_lines,
            num_fields=stats.num_fields,
            num_tables,
            precision=?write_info.precision,
            body_size=body.len(),
            namespace=%write_info.namespace,
            duration=?duration,
            "routing write",
        );

        // Retrieve the namespace schema for this namespace.
        let namespace_schema = self
            .namespace_resolver
            .get_namespace_schema(&write_info.namespace)
            .await?;

        self.dml_handler
            .write(&write_info.namespace, namespace_schema, batches, span_ctx)
            .await
            .map_err(Into::into)?;

        self.write_metric_lines.inc(stats.num_lines as _);
        self.write_metric_fields.inc(stats.num_fields as _);
        self.write_metric_tables.inc(num_tables as _);
        self.write_metric_body_size.inc(body.len() as _);

        Ok(())
    }

    /// Parse the request's body into raw bytes, applying the configured size
    /// limits and decoding any content encoding.
    async fn read_body(&self, req: hyper::Request<Body>) -> Result<Bytes, Error> {
        let encoding = req
            .headers()
            .get(&CONTENT_ENCODING)
            .map(|v| v.to_str().map_err(Error::NonUtf8ContentHeader))
            .transpose()?;
        let ungzip = match encoding {
            None | Some("identity") => false,
            Some("gzip") => true,
            Some(v) => return Err(Error::InvalidContentEncoding(v.to_string())),
        };

        let mut payload = req.into_body();

        let mut body = BytesMut::new();
        while let Some(chunk) = payload.next().await {
            let chunk = chunk.map_err(Error::ClientHangup)?;
            // limit max size of in-memory payload
            if (body.len() + chunk.len()) > self.max_request_bytes {
                return Err(Error::RequestSizeExceeded(self.max_request_bytes));
            }
            body.extend_from_slice(&chunk);
        }
        let body = body.freeze();

        // If the body is not compressed, return early.
        if !ungzip {
            return Ok(body);
        }

        // Unzip the gzip-encoded content
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most max_request_bytes bytes to prevent a decompression bomb
        // based DoS.
        //
        // In order to detect if the entire stream ahs been read, or truncated,
        // read an extra byte beyond the limit and check the resulting data
        // length - see the max_request_size_truncation test.
        let mut decoder = decoder.take(self.max_request_bytes as u64 + 1);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .map_err(Error::InvalidGzip)?;

        // If the length is max_size+1, the body is at least max_size+1 bytes in
        // length, and possibly longer, but truncated.
        if decoded_data.len() > self.max_request_bytes {
            return Err(Error::RequestSizeExceeded(self.max_request_bytes));
        }

        Ok(decoded_data.into())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, iter, sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{
        NamespaceId, NamespaceName, NamespaceNameError, OrgBucketMappingError, TableId,
    };
    use flate2::{write::GzEncoder, Compression};
    use hyper::header::HeaderValue;
    use metric::{Attributes, Metric};
    use mutable_batch::column::ColumnData;
    use mutable_batch_lp::LineWriteError;
    use test_helpers::timeout::FutureTimeout;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{
        dml_handlers::{
            mock::{MockDmlHandler, MockDmlHandlerCall},
            CachedServiceProtectionLimit,
        },
        namespace_resolver::{mock::MockNamespaceResolver, NamespaceCreationError},
        server::http::write::{
            mock::{MockUnifyingParseCall, MockWriteRequestUnifier},
            multi_tenant::MultiTenantRequestUnifier,
            v1::V1WriteParseError,
            v2::V2WriteParseError,
            Precision,
        },
    };

    const MAX_BYTES: usize = 1024;
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);
    static NAMESPACE_NAME: &str = "bananas_test";

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str, value: Option<u64>) {
        let counter = metrics
            .get_instrument::<Metric<U64Counter>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to get observer")
            .fetch();

        if let Some(want) = value {
            assert_eq!(want, counter, "metric does not have expected value");
        } else {
            assert!(counter > 0, "metric {name} did not record any values");
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    //
    // The following tests assert "cloud2" or "multi-tenant" mode only, as this
    // is the default operating mode.
    //
    // Tests of the single-tenant behaviour differs from the established norms,
    // and can be found alongside the single-tenant implementation.
    //
    ////////////////////////////////////////////////////////////////////////////

    // Generate two HTTP handler tests - one for a plain request and one with a
    // gzip-encoded body (and appropriate header), asserting the handler return
    // value & write op.
    macro_rules! test_http_handler {
        (
            $name:ident,
            uri = $uri:expr,                                // Request URI
            body = $body:expr,                              // Request body content
            dml_write_handler = $dml_write_handler:expr,    // DML write handler response (if called)
            dml_delete_handler = $dml_delete_handler:expr,  // DML delete handler response (if called)
            want_result = $want_result:pat,                 // Expected handler return value (as pattern)
            want_dml_calls = $($want_dml_calls:tt )+        // assert_matches slice pattern for expected DML calls
        ) => {
            // Generate the three test cases by feed the same inputs, but varying
            // the encoding.
            test_http_handler!(
                $name,
                encoding=plain,
                uri = $uri,
                body = $body,
                dml_write_handler = $dml_write_handler,
                dml_delete_handler = $dml_delete_handler,
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
            test_http_handler!(
                $name,
                encoding=identity,
                uri = $uri,
                body = $body,
                dml_write_handler = $dml_write_handler,
                dml_delete_handler = $dml_delete_handler,
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
            test_http_handler!(
                $name,
                encoding=gzip,
                uri = $uri,
                body = $body,
                dml_write_handler = $dml_write_handler,
                dml_delete_handler = $dml_delete_handler,
                want_result = $want_result,
                want_dml_calls = $($want_dml_calls)+
            );
        };
        // Actual test body generator.
        (
            $name:ident,
            encoding = $encoding:tt,
            uri = $uri:expr,
            body = $body:expr,
            dml_write_handler = $dml_write_handler:expr,
            dml_delete_handler = $dml_delete_handler:expr,
            want_result = $want_result:pat,
            want_dml_calls = $($want_dml_calls:tt )+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_http_handler_ $name _ $encoding>]() {
                    let body = $body;

                    // Optionally generate a fragment of code to encode the body
                    let body = test_http_handler!(encoding=$encoding, body);

                    #[allow(unused_mut)]
                    let mut request = Request::builder()
                        .uri($uri)
                        .method("POST")
                        .body(Body::from(body))
                        .unwrap();

                    // Optionally modify request to account for the desired
                    // encoding
                    test_http_handler!(encoding_header=$encoding, request);

                    let mock_namespace_resolver = MockNamespaceResolver::default()
                        .with_mapping(NAMESPACE_NAME, NAMESPACE_ID);
                    let dml_handler = Arc::new(MockDmlHandler::default()
                        .with_write_return($dml_write_handler)
                    );
                    let metrics = Arc::new(metric::Registry::default());
                    let delegate = HttpDelegate::new(
                        MAX_BYTES,
                        100,
                        mock_namespace_resolver,
                        Arc::clone(&dml_handler),
                        &metrics,
                        Box::<crate::server::http::write::multi_tenant::MultiTenantRequestUnifier>::default(),
                    );

                    let got = delegate.route(request).await;
                    assert_matches!(got, $want_result);

                    // All successful responses should have a NO_CONTENT code
                    // and metrics should be recorded.
                    if let Ok(v) = got {
                        assert_eq!(v.status(), StatusCode::NO_CONTENT);
                        if $uri.contains("/api/v2/write") {
                            assert_metric_hit(&metrics, "http_write_lines", None);
                            assert_metric_hit(&metrics, "http_write_fields", None);
                            assert_metric_hit(&metrics, "http_write_tables", None);
                            assert_metric_hit(&metrics, "http_write_body_bytes", Some($body.len() as _));
                        } else {
                            assert_metric_hit(&metrics, "http_delete_body_bytes", Some($body.len() as _));
                        }
                    }

                    let calls = dml_handler.calls();
                    assert_matches!(calls.as_slice(), $($want_dml_calls)+);
                }
            }
        };
        (encoding=plain, $body:ident) => {
            $body
        };
        (encoding=identity, $body:ident) => {
            $body
        };
        (encoding=gzip, $body:ident) => {{
            // Apply gzip compression to the body
            let mut e = GzEncoder::new(Vec::new(), Compression::default());
            e.write_all(&$body).unwrap();
            e.finish().expect("failed to compress test body")
        }};
        (encoding_header=plain, $request:ident) => {};
        (encoding_header=identity, $request:ident) => {{
            // Set the identity content encoding
            $request
                .headers_mut()
                .insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
        }};
        (encoding_header=gzip, $request:ident) => {{
            // Set the gzip content encoding
            $request
                .headers_mut()
                .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }};
    }

    // Wrapper over test_http_handler specifically for write requests.
    macro_rules! test_write_handler {
        (
            $name:ident,
            query_string = $query_string:expr,   // Request URI query string
            body = $body:expr,                   // Request body content
            dml_handler = $dml_handler:expr,     // DML write handler response (if called)
            want_result = $want_result:pat,
            want_dml_calls = $($want_dml_calls:tt )+
        ) => {
            paste::paste! {
                test_http_handler!(
                    [<write_ $name>],
                    uri = format!("https://bananas.example/api/v2/write{}", $query_string),
                    body = $body,
                    dml_write_handler = $dml_handler,
                    dml_delete_handler = [],
                    want_result = $want_result,
                    want_dml_calls = $($want_dml_calls)+
                );
            }
        };
    }

    test_write_handler!(
        ok,
        query_string = "?org=bananas&bucket=test",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [
            MockDmlHandlerCall::Write { namespace, .. }
        ] => {
            assert_eq!(namespace, NAMESPACE_NAME);
        }
    );

    test_write_handler!(
        ok_precision_s,
        query_string = "?org=bananas&bucket=test&precision=s",
        body = "platanos,tag1=A,tag2=B val=42i 1647622847".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [
            MockDmlHandlerCall::Write { namespace, namespace_schema, write_input, .. }
        ] => {
            assert_eq!(namespace, NAMESPACE_NAME);
            assert_eq!(namespace_schema.id, NAMESPACE_ID);

            let table = write_input.get("platanos").expect("table not found");
            let ts = table.timestamp_summary().expect("no timestamp summary");
            assert_eq!(Some(1647622847000000000), ts.stats.min);
        }
    );

    test_write_handler!(
        ok_precision_ms,
        query_string = "?org=bananas&bucket=test&precision=ms",
        body = "platanos,tag1=A,tag2=B val=42i 1647622847000".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [
            MockDmlHandlerCall::Write { namespace, namespace_schema, write_input, .. }
        ] => {
            assert_eq!(namespace, NAMESPACE_NAME);
            assert_eq!(namespace_schema.id, NAMESPACE_ID);

            let table = write_input.get("platanos").expect("table not found");
            let ts = table.timestamp_summary().expect("no timestamp summary");
            assert_eq!(Some(1647622847000000000), ts.stats.min);
        }
    );

    test_write_handler!(
        ok_precision_us,
        query_string = "?org=bananas&bucket=test&precision=us",
        body = "platanos,tag1=A,tag2=B val=42i 1647622847000000".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [
            MockDmlHandlerCall::Write { namespace, namespace_schema, write_input, .. }
        ] => {
            assert_eq!(namespace, NAMESPACE_NAME);
            assert_eq!(namespace_schema.id, NAMESPACE_ID);

            let table = write_input.get("platanos").expect("table not found");
            let ts = table.timestamp_summary().expect("no timestamp summary");
            assert_eq!(Some(1647622847000000000), ts.stats.min);
        }
    );

    test_write_handler!(
        ok_precision_ns,
        query_string = "?org=bananas&bucket=test&precision=ns",
        body = "platanos,tag1=A,tag2=B val=42i 1647622847000000000".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [
            MockDmlHandlerCall::Write { namespace, namespace_schema, write_input, .. }
        ] => {
            assert_eq!(namespace, NAMESPACE_NAME);
            assert_eq!(namespace_schema.id, NAMESPACE_ID);

            let table = write_input.get("platanos").expect("table not found");
            let ts = table.timestamp_summary().expect("no timestamp summary");
            assert_eq!(Some(1647622847000000000), ts.stats.min);
        }
    );

    test_write_handler!(
        precision_overflow,
        // SECONDS, so multiplies the provided timestamp by 1,000,000,000
        query_string = "?org=bananas&bucket=test&precision=s",
        body = "platanos,tag1=A,tag2=B val=42i 1647622847000000000".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::ParseLineProtocol(_)),
        want_dml_calls = []
    );

    test_write_handler!(
        no_query_params,
        query_string = "",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::MultiTenantError(
            MultiTenantExtractError::ParseV2Request(V2WriteParseError::NoQueryParams)
        )),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        no_org_bucket,
        query_string = "?",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::NoOrgBucketSpecified
            )
        )),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        empty_org_bucket,
        query_string = "?org=&bucket=",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::NoOrgBucketSpecified
            )
        )),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        invalid_org_bucket,
        query_string = format!("?org=test&bucket={}", "A".repeat(1000)),
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::MultiTenantError(
            MultiTenantExtractError::InvalidOrgAndBucket(
                OrgBucketMappingError::InvalidNamespaceName(
                    NamespaceNameError::LengthConstraint { .. }
                )
            )
        )),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        invalid_line_protocol,
        query_string = "?org=bananas&bucket=test",
        body = "not line protocol".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::ParseLineProtocol(_)),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        non_utf8_body,
        query_string = "?org=bananas&bucket=test",
        body = vec![0xc3, 0x28],
        dml_handler = [Ok(())],
        want_result = Err(Error::NonUtf8Body(_)),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        max_request_size_truncation,
        query_string = "?org=bananas&bucket=test",
        body = {
            // Generate a LP string in the form of:
            //
            //  bananas,A=AAAAAAAAAA(repeated)... B=42
            //                                  ^
            //                                  |
            //                         MAX_BYTES boundary
            //
            // So that reading MAX_BYTES number of bytes produces the string:
            //
            //  bananas,A=AAAAAAAAAA(repeated)...
            //
            // Effectively trimming off the " B=42" suffix.
            let body = "bananas,A=";
            iter::once(body)
                .chain(iter::repeat("A").take(MAX_BYTES - body.len()))
                .chain(iter::once(" B=42\n"))
                .flat_map(|s| s.bytes())
                .collect::<Vec<u8>>()
        },
        dml_handler = [Ok(())],
        want_result = Err(Error::RequestSizeExceeded(_)),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        db_not_found,
        query_string = "?org=bananas&bucket=test",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Err(DmlError::NamespaceNotFound(NAMESPACE_NAME.to_string()))],
        want_result = Err(Error::DmlHandler(DmlError::NamespaceNotFound(_))),
        want_dml_calls = [MockDmlHandlerCall::Write { namespace, .. }] => {
            assert_eq!(namespace, NAMESPACE_NAME);
        }
    );

    test_write_handler!(
        dml_handler_error,
        query_string = "?org=bananas&bucket=test",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Err(DmlError::Internal("💣".into()))],
        want_result = Err(Error::DmlHandler(DmlError::Internal(_))),
        want_dml_calls = [MockDmlHandlerCall::Write { namespace, .. }] => {
            assert_eq!(namespace, NAMESPACE_NAME);
        }
    );

    test_write_handler!(
        field_upsert_within_batch,
        query_string = "?org=bananas&bucket=test",
        body = "test field=1u 100\ntest field=2u 100".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [
            MockDmlHandlerCall::Write { namespace, namespace_schema, write_input, .. }
        ] => {
            assert_eq!(namespace, NAMESPACE_NAME);
            assert_eq!(namespace_schema.id, NAMESPACE_ID);
            let table = write_input.get("test").expect("table not in write");
            let col = table.column("field").expect("column missing");
            assert_matches!(col.data(), ColumnData::U64(data, _) => {
                // Ensure both values are recorded, in the correct order.
                assert_eq!(data.as_slice(), [1, 2]);
            });
        }
    );

    test_write_handler!(
        column_named_time,
        query_string = "?org=bananas&bucket=test",
        body = "test field=1u,time=42u 100".as_bytes(),
        dml_handler = [],
        want_result = Err(_),
        want_dml_calls = []
    );

    test_http_handler!(
        not_found,
        uri = "https://bananas.example/wat",
        body = "".as_bytes(),
        dml_write_handler = [],
        dml_delete_handler = [],
        want_result = Err(Error::NoHandler),
        want_dml_calls = []
    );

    // https://github.com/influxdata/influxdb_iox/issues/4326
    mod issue4326 {
        use super::*;

        test_write_handler!(
            duplicate_fields_same_value,
            query_string = "?org=bananas&bucket=test",
            body = "whydo InputPower=300i,InputPower=300i".as_bytes(),
            dml_handler = [Ok(())],
            want_result = Ok(_),
            want_dml_calls = [MockDmlHandlerCall::Write { namespace, write_input, .. }] => {
                assert_eq!(namespace, NAMESPACE_NAME);
                let table = write_input.get("whydo").expect("table not in write");
                let col = table.column("InputPower").expect("column missing");
                assert_matches!(col.data(), ColumnData::I64(data, _) => {
                    // Ensure the duplicate values are coalesced.
                    assert_eq!(data.as_slice(), [300]);
                });
            }
        );

        test_write_handler!(
            duplicate_fields_different_value,
            query_string = "?org=bananas&bucket=test",
            body = "whydo InputPower=300i,InputPower=42i".as_bytes(),
            dml_handler = [Ok(())],
            want_result = Ok(_),
            want_dml_calls = [MockDmlHandlerCall::Write { namespace, write_input, .. }] => {
                assert_eq!(namespace, NAMESPACE_NAME);
                let table = write_input.get("whydo").expect("table not in write");
                let col = table.column("InputPower").expect("column missing");
                assert_matches!(col.data(), ColumnData::I64(data, _) => {
                    // Last value wins
                    assert_eq!(data.as_slice(), [42]);
                });
            }
        );

        test_write_handler!(
            duplicate_fields_different_type,
            query_string = "?org=bananas&bucket=test",
            body = "whydo InputPower=300i,InputPower=4.2".as_bytes(),
            dml_handler = [],
            want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: LineWriteError::ConflictedFieldTypes { .. },
                ..
            })),
            want_dml_calls = []
        );

        test_write_handler!(
            duplicate_tags_same_value,
            query_string = "?org=bananas&bucket=test",
            body = "whydo,InputPower=300i,InputPower=300i field=42i".as_bytes(),
            dml_handler = [],
            want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: LineWriteError::DuplicateTag { .. },
                ..
            })),
            want_dml_calls = []
        );

        test_write_handler!(
            duplicate_tags_different_value,
            query_string = "?org=bananas&bucket=test",
            body = "whydo,InputPower=300i,InputPower=42i field=42i".as_bytes(),
            dml_handler = [],
            want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: LineWriteError::DuplicateTag { .. },
                ..
            })),
            want_dml_calls = []
        );

        test_write_handler!(
            duplicate_tags_different_type,
            query_string = "?org=bananas&bucket=test",
            body = "whydo,InputPower=300i,InputPower=4.2 field=42i".as_bytes(),
            dml_handler = [],
            want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: LineWriteError::DuplicateTag { .. },
                ..
            })),
            want_dml_calls = []
        );

        test_write_handler!(
            duplicate_is_tag_and_field,
            query_string = "?org=bananas&bucket=test",
            body = "whydo,InputPower=300i InputPower=300i".as_bytes(),
            dml_handler = [],
            want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: LineWriteError::MutableBatch {
                    source: mutable_batch::writer::Error::TypeMismatch { .. }
                },
                ..
            })),
            want_dml_calls = []
        );

        test_write_handler!(
            duplicate_is_tag_and_field_different_types,
            query_string = "?org=bananas&bucket=test",
            body = "whydo,InputPower=300i InputPower=30.0".as_bytes(),
            dml_handler = [],
            want_result = Err(Error::ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: LineWriteError::MutableBatch {
                    source: mutable_batch::writer::Error::TypeMismatch { .. }
                },
                ..
            })),
            want_dml_calls = []
        );
    }

    #[derive(Debug, Error)]
    enum MockError {
        #[error("bad stuff")]
        Terrible,
    }

    // This test ensures the request limiter drops requests once the configured
    // number of simultaneous requests are being serviced.
    #[tokio::test]
    async fn test_request_limit_enforced() {
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping("bananas", NamespaceId::new(42));

        let dml_handler = Arc::new(MockDmlHandler::default());
        let metrics = Arc::new(metric::Registry::default());
        let delegate = Arc::new(HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            &metrics,
            Box::new(
                MockWriteRequestUnifier::default().with_ret(iter::repeat_with(|| {
                    Ok(WriteParams {
                        namespace: NamespaceName::new(NAMESPACE_NAME).unwrap(),
                        precision: Precision::default(),
                    })
                })),
            ),
        ));

        // Use a channel to hold open the request.
        //
        // This causes the request handler to block reading the request body
        // until tx is dropped and the body stream ends, completing the body and
        // unblocking the request handler.
        let (body_1_tx, rx) = tokio::sync::mpsc::channel(1);
        let request_1 = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::wrap_stream(ReceiverStream::new(rx)))
            .unwrap();

        // Spawn the first request and push at least 2 body chunks through tx.
        //
        // Spawning and writing through tx will avoid any race between which
        // request handler task is scheduled first by ensuring this request is
        // being actively read from - the first send() could fill the channel
        // buffer of 1, and therefore successfully returning from the second
        // send() MUST indicate the stream is being read by the handler (and
        // therefore the task has spawned and the request is actively being
        // serviced).
        let req_1 = tokio::spawn({
            let delegate = Arc::clone(&delegate);
            async move { delegate.route(request_1).await }
        });
        body_1_tx
            .send(Ok("cpu "))
            .await
            .expect("req1 closed channel");
        body_1_tx
            .send(Ok("field=1i"))
            // Never hang if there is no handler reading this request
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect("req1 closed channel");

        //
        // At this point we can be certain that request 1 is being actively
        // serviced, and the HTTP server is in a state that should cause the
        // immediate drop of any subsequent requests.
        //

        assert_metric_hit(&metrics, "http_request_limit_rejected", Some(0));

        // Retain this tx handle for the second request and use it to prove the
        // request dropped before anything was read from the body - the request
        // should error _before_ anything is sent over tx, and subsequently
        // attempting to send something over tx after the error should fail with
        // a "channel closed" error.
        let (body_2_tx, rx) = tokio::sync::mpsc::channel::<Result<&'static str, MockError>>(1);
        let request_2 = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::wrap_stream(ReceiverStream::new(rx)))
            .unwrap();

        // Attempt to service request 2.
        //
        // This should immediately return without requiring any body chunks to
        // be sent through tx.
        let err = delegate
            .route(request_2)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect_err("second request should be rejected");
        assert_matches!(err, Error::RequestLimit);

        // Ensure the "rejected requests" metric was incremented
        assert_metric_hit(&metrics, "http_request_limit_rejected", Some(1));

        // Prove the dropped request body is not being read:
        body_2_tx
            .send(Ok("wat"))
            .await
            .expect_err("channel should be closed");

        // Cause the first request handler to bail, releasing request capacity
        // back to the router.
        body_1_tx
            .send(Err(MockError::Terrible))
            .await
            .expect("req1 closed channel");
        // Wait for the handler to return to avoid any races.
        let req_1 = req_1
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect("request 1 handler should not panic")
            .expect_err("request should fail");
        assert_matches!(req_1, Error::ClientHangup(_));

        // And submit a third request that should be serviced now there's no
        // concurrent request being handled.
        let request_3 = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::from(""))
            .unwrap();
        delegate
            .route(request_3)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .expect("empty write should succeed");

        // And the request rejected metric must remain unchanged
        assert_metric_hit(&metrics, "http_request_limit_rejected", Some(1));
    }

    /// Assert the router rejects writes to the V1 endpoint when in
    /// "multi-tenant" mode.
    #[tokio::test]
    async fn test_multi_tenant_denies_v1_writes() {
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping(NAMESPACE_NAME, NamespaceId::new(42));

        let dml_handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));
        let metrics = Arc::new(metric::Registry::default());
        let delegate = HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            &metrics,
            Box::<MultiTenantRequestUnifier>::default(),
        );

        let request = Request::builder()
            .uri("https://bananas.example/write")
            .method("POST")
            .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Err(Error::NoHandler));
    }

    /// Assert the router delegates request parsing to the
    /// [`WriteRequestUnifier`] implementation.
    ///
    /// By validating request parsing is delegated, behavioural tests for each
    /// implementation can be implemented directly against those implementations
    /// (instead of putting them all here).
    #[tokio::test]
    async fn test_delegate_to_write_request_unifier_ok() {
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping(NAMESPACE_NAME, NamespaceId::new(42));

        let request_unifier = Arc::new(MockWriteRequestUnifier::default().with_ret(
            iter::repeat_with(|| {
                Ok(WriteParams {
                    namespace: NamespaceName::new(NAMESPACE_NAME).unwrap(),
                    precision: Precision::default(),
                })
            }),
        ));

        let dml_handler =
            Arc::new(MockDmlHandler::default().with_write_return([Ok(()), Ok(()), Ok(())]));
        let metrics = Arc::new(metric::Registry::default());
        let delegate = HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            &metrics,
            Box::new(Arc::clone(&request_unifier)),
        );

        // A route miss does not invoke the parser
        let request = Request::builder()
            .uri("https://bananas.example/wat")
            .method("POST")
            .body(Body::from(""))
            .unwrap();
        let got = delegate.route(request).await;
        assert_matches!(got, Err(Error::NoHandler));

        // V1 write parsing is delegated
        let request = Request::builder()
            .uri("https://bananas.example/write")
            .method("POST")
            .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
            .unwrap();
        let got = delegate.route(request).await;
        assert_matches!(got, Ok(_));

        // Only one call was received, and it should be v1.
        assert_matches!(
            request_unifier.calls().as_slice(),
            [MockUnifyingParseCall::V1]
        );

        // V2 write parsing is delegated
        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write")
            .method("POST")
            .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
            .unwrap();
        let got = delegate.route(request).await;
        assert_matches!(got, Ok(_));

        // Both call were received.
        assert_matches!(
            request_unifier.calls().as_slice(),
            [MockUnifyingParseCall::V1, MockUnifyingParseCall::V2]
        );
    }

    // The display text of Error gets passed through `ioxd_router::IoxHttpErrorAdaptor` then
    // `ioxd_common::http::error::HttpApiError` as the JSON "message" value in error response
    // bodies. These are fixture tests to document error messages that users might see when
    // making requests to `/api/v2/write`.
    macro_rules! check_errors {
        (
            $((                   // This macro expects a list of tuples, each specifying:
                $variant:ident        // - One of the error enum variants
                $(($data:expr))?,     // - If needed, an expression to construct the variant's data
                $msg:expr $(,)?       // - The string expected for `Display`ing this variant
            )),*,
        ) => {
            // Generate code that contains all possible error variants, to ensure a compiler error
            // if any errors are not explicitly covered in this test.
            #[test]
            fn all_error_variants_are_checked() {
                #[allow(dead_code)]
                fn all_documented(ensure_all_error_variants_are_checked: Error) {
                    #[allow(unreachable_patterns)]
                    match ensure_all_error_variants_are_checked {
                        $(Error::$variant { .. } => {},)*
                        // If this test doesn't compile because of a non-exhaustive pattern,
                        // a variant needs to be added to the `check_errors!` call with the
                        // expected `to_string()` text.
                    }
                }
            }

            // A test that covers all errors given to this macro.
            #[tokio::test]
            async fn error_messages_match() {
                // Generate an assert for each error given to this macro.
                $(
                    let e = Error::$variant $(($data))?;
                    assert_eq!(e.to_string(), $msg);
                )*
            }

            #[test]
            fn print_out_error_text() {
                println!("{}", concat!($(stringify!($variant), "\t", $msg, "\n",)*),)
            }
        };
    }

    check_errors! {
        (
            NoHandler,
            "not found",
        ),

        (
            DeletesUnsupported,
            "deletes are not supported",
        ),

        (
            NonUtf8Body(std::str::from_utf8(&[0, 159]).unwrap_err()),
            "body content is not valid utf8: invalid utf-8 sequence of 1 bytes from index 1",
        ),

        (
            NonUtf8ContentHeader({
                hyper::header::HeaderValue::from_bytes(&[159]).unwrap().to_str().unwrap_err()
            }),
            "invalid content-encoding header: failed to convert header to a str",
        ),

        (
            InvalidContentEncoding("[invalid content encoding value]".into()),
            "unacceptable content-encoding: [invalid content encoding value]",
        ),

        (
            ClientHangup({
                let url = "wrong://999.999.999.999:999999".parse().unwrap();
                hyper::Client::new().get(url).await.unwrap_err()
            }),
            "client disconnected",
        ),

        (
            RequestSizeExceeded(1337),
            "max request size (1337 bytes) exceeded",
        ),

        (
            InvalidGzip(std::io::Error::new(std::io::ErrorKind::Other, "[io Error]")),
            "error decoding gzip stream: [io Error]",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::LineProtocol {
                source: influxdb_line_protocol::Error::FieldSetMissing,
                line: 42,
            }),
            "failed to parse line protocol: \
            error parsing line 42 (1-based): No fields were provided",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: mutable_batch_lp::LineWriteError::DuplicateTag {
                    name: "host".into(),
                },
                line: 42,
            }),
            "failed to parse line protocol: \
            error writing line 42: \
            the tag 'host' is specified more than once with conflicting values",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::Write {
                source: mutable_batch_lp::LineWriteError::ConflictedFieldTypes {
                    name: "bananas".into(),
                },
                line: 42,
            }),
            "failed to parse line protocol: \
            error writing line 42: \
            the field 'bananas' is specified more than once with conflicting types",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::EmptyPayload),
            "failed to parse line protocol: empty write payload",
        ),

        (
            ParseLineProtocol(mutable_batch_lp::Error::TimestampOverflow),
            "failed to parse line protocol: timestamp overflows i64",
        ),

        (
            DmlHandler(DmlError::NamespaceNotFound("[namespace name]".into())),
            "dml handler error: namespace [namespace name] does not exist",
        ),

        (
            NamespaceResolver({
                let e = iox_catalog::interface::Error::NameExists { name: "[name]".into() };
                crate::namespace_resolver::Error::Lookup(e)
            }),
            "failed to resolve namespace ID: \
             name [name] already exists",
        ),

        (
            NamespaceResolver(
                crate::namespace_resolver::Error::Create(NamespaceCreationError::Reject("bananas".to_string()))
            ),
            "rejecting write due to non-existing namespace: bananas",
        ),

        (
            RequestLimit,
            "this service is overloaded, please try again later",
        ),

        (
            Unauthenticated,
            "authentication required",
        ),

        (
            Forbidden,
            "access denied",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(CachedServiceProtectionLimit::Column {
                table_name: "bananas".to_string(),
                existing_column_count: 42,
                merged_column_count: 4242,
                max_columns_per_table: 24,
            })))),
            "dml handler error: service limit reached: couldn't create columns in table `bananas`; table contains 42 \
            existing columns, applying this write would result in 4242 columns, limit is 24",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(CachedServiceProtectionLimit::Table {
                existing_table_count: 42,
                merged_table_count: 4242,
                table_count_limit: 24,
            })))),
            "dml handler error: service limit reached: couldn't create new table; namespace contains 42 existing \
            tables, applying this write would result in 4242 tables, limit is 24",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(iox_catalog::interface::Error::ColumnCreateLimitError {
                column_name: "bananas".to_string(),
                table_id: TableId::new(42),
            })))),
            "dml handler error: service limit reached: couldn't create column bananas in table 42; limit reached on \
            namespace",
        ),

        (
            DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(Box::new(iox_catalog::interface::Error::TableCreateLimitError {
                table_name: "bananas".to_string(),
                namespace_id: NamespaceId::new(42),
            })))),
            "dml handler error: service limit reached: couldn't create table bananas; limit reached on namespace 42",
        ),

        // A single-tenant namespace parsing error
        (
            SingleTenantError(SingleTenantExtractError::InvalidNamespace(NamespaceNameError::LengthConstraint{name: "bananas".to_string()})),
            "namespace name bananas length must be between 1 and 64 characters",
        ),
        // A single-tenant v1 parsing error
        (
            SingleTenantError(SingleTenantExtractError::ParseV1Request(V1WriteParseError::NoQueryParams)),
            "no db destination provided",
        ),
        // A single-tenant v2 parsing error
        (
            SingleTenantError(SingleTenantExtractError::ParseV2Request(V2WriteParseError::NoQueryParams)),
            "no org/bucket destination provided",
        ),

        // A multi-tenant namespace parsing error
        (
            MultiTenantError(MultiTenantExtractError::InvalidOrgAndBucket(OrgBucketMappingError::InvalidNamespaceName(NamespaceNameError::LengthConstraint{name: "bananas".to_string()}))),
            "invalid namespace name: namespace name bananas length must be between 1 and 64 characters",
        ),
        (
            MultiTenantError(MultiTenantExtractError::InvalidOrgAndBucket(OrgBucketMappingError::NoOrgBucketSpecified)),
            "missing org/bucket value",
        ),

        // There is no multi-tenant support for v1 parsing, so no errors!

        // A multi-tenant v2 parsing error
        (
            MultiTenantError(MultiTenantExtractError::ParseV2Request(V2WriteParseError::NoQueryParams)),
            "no org/bucket destination provided",
        ),
    }
}
