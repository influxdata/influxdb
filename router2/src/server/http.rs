//! HTTP service implementations for `router2`.

use std::str::Utf8Error;

use bytes::{Bytes, BytesMut};
use data_types::names::{org_and_bucket_to_database, OrgBucketMappingError};

use futures::StreamExt;
use hyper::{header::CONTENT_ENCODING, Body, Method, Request, Response, StatusCode};
use observability_deps::tracing::*;
use predicate::delete_predicate::{parse_delete_predicate, parse_http_delete_request};
use serde::Deserialize;
use thiserror::Error;
use time::{SystemProvider, TimeProvider};
use trace::ctx::SpanContext;

use crate::dml_handler::{DmlError, DmlHandler};

/// Errors returned by the `router2` HTTP request handler.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found")]
    NoHandler,

    /// An error with the org/bucket in the request.
    #[error(transparent)]
    InvalidOrgBucket(#[from] OrgBucketError),

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

    /// Failure to parse the request delete predicate.
    #[error("failed to parse delete predicate: {0}")]
    ParseDelete(#[from] predicate::delete_predicate::Error),

    /// An error returned from the [`DmlHandler`].
    #[error("dml handler error: {0}")]
    DmlHandler(#[from] DmlError),
}

impl Error {
    /// Convert the error into an appropriate [`StatusCode`] to be returned to
    /// the end user.
    pub fn as_status_code(&self) -> StatusCode {
        match self {
            Error::NoHandler | Error::DmlHandler(DmlError::DatabaseNotFound(_)) => {
                StatusCode::NOT_FOUND
            }
            Error::InvalidOrgBucket(_) => StatusCode::BAD_REQUEST,
            Error::ClientHangup(_) => StatusCode::BAD_REQUEST,
            Error::InvalidGzip(_) => StatusCode::BAD_REQUEST,
            Error::NonUtf8ContentHeader(_) => StatusCode::BAD_REQUEST,
            Error::NonUtf8Body(_) => StatusCode::BAD_REQUEST,
            Error::InvalidContentEncoding(_) => {
                // https://www.rfc-editor.org/rfc/rfc7231#section-6.5.13
                StatusCode::UNSUPPORTED_MEDIA_TYPE
            }
            Error::RequestSizeExceeded(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Error::ParseLineProtocol(_) => StatusCode::BAD_REQUEST,
            Error::DmlHandler(DmlError::Internal(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseDelete(_) => StatusCode::BAD_REQUEST,
        }
    }
}

/// Errors returned when decoding the organisation / bucket information from a
/// HTTP request and deriving the database name from it.
#[derive(Debug, Error)]
pub enum OrgBucketError {
    /// The request contains no org/bucket destination information.
    #[error("no org/bucket destination provided")]
    NotSpecified,

    /// The request contains invalid parameters.
    #[error("failed to deserialise org/bucket in request: {0}")]
    DecodeFail(#[from] serde::de::value::Error),

    /// The provided org/bucket could not be converted into a database name.
    #[error(transparent)]
    MappingFail(#[from] OrgBucketMappingError),
}

#[derive(Debug, Deserialize)]
/// Org & bucket identifiers for a DML operation.
pub struct OrgBucketInfo {
    org: String,
    bucket: String,
}

impl<T> TryFrom<&Request<T>> for OrgBucketInfo {
    type Error = OrgBucketError;

    fn try_from(req: &Request<T>) -> Result<Self, Self::Error> {
        let query = req.uri().query().ok_or(OrgBucketError::NotSpecified)?;
        let got: OrgBucketInfo = serde_urlencoded::from_str(query)?;

        // An empty org or bucket is not acceptable.
        if got.org.is_empty() || got.bucket.is_empty() {
            return Err(OrgBucketError::NotSpecified);
        }

        Ok(got)
    }
}

/// This type is responsible for servicing requests to the `router2` HTTP
/// endpoint.
///
/// Requests to some paths may be handled externally by the caller - the IOx
/// server runner framework takes care of implementing the heath endpoint,
/// metrics, pprof, etc.
#[derive(Debug, Default)]
pub struct HttpDelegate<D, T = SystemProvider> {
    max_request_bytes: usize,
    time_provider: T,
    dml_handler: D,
}

impl<D> HttpDelegate<D, SystemProvider> {
    /// Initialise a new [`HttpDelegate`] passing valid requests to the
    /// specified `dml_handler`.
    ///
    /// HTTP request bodies are limited to `max_request_bytes` in size,
    /// returning an error if exceeded.
    pub fn new(max_request_bytes: usize, dml_handler: D) -> Self {
        Self {
            max_request_bytes,
            time_provider: SystemProvider::default(),
            dml_handler,
        }
    }
}

impl<D, T> HttpDelegate<D, T>
where
    D: DmlHandler,
    T: TimeProvider,
{
    /// Routes `req` to the appropriate handler, if any, returning the handler
    /// response.
    pub async fn route(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/api/v2/write") => self.write_handler(req).await,
            (&Method::POST, "/api/v2/delete") => self.delete_handler(req).await,
            _ => return Err(Error::NoHandler),
        }
        .map(|_| response_no_content())
    }

    async fn write_handler(&self, req: Request<Body>) -> Result<(), Error> {
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let account = OrgBucketInfo::try_from(&req)?;
        let namespace = org_and_bucket_to_database(&account.org, &account.bucket)
            .map_err(OrgBucketError::MappingFail)?;

        trace!(org=%account.org, bucket=%account.bucket, %namespace, "processing write request");

        // Read the HTTP body and convert it to a str.
        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        // The time, in nanoseconds since the epoch, to assign to any points that don't
        // contain a timestamp
        let default_time = self.time_provider.now().timestamp_nanos();

        let (batches, stats) = match mutable_batch_lp::lines_to_batches_stats(body, default_time) {
            Ok(v) => v,
            Err(mutable_batch_lp::Error::EmptyPayload) => {
                debug!("nothing to write");
                return Ok(());
            }
            Err(e) => return Err(Error::ParseLineProtocol(e)),
        };

        self.dml_handler
            .write(namespace, batches, stats, body.len(), span_ctx)
            .await?;

        Ok(())
    }

    async fn delete_handler(&self, req: Request<Body>) -> Result<(), Error> {
        let span_ctx: Option<SpanContext> = req.extensions().get().cloned();

        let account = OrgBucketInfo::try_from(&req)?;
        let namespace = org_and_bucket_to_database(&account.org, &account.bucket)
            .map_err(OrgBucketError::MappingFail)?;

        trace!(org=%account.org, bucket=%account.bucket, %namespace, "processing delete request");

        // Read the HTTP body and convert it to a str.
        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        // Parse and extract table name (which can be empty), start, stop, and predicate
        let parsed_delete = parse_http_delete_request(body)?;
        let predicate = parse_delete_predicate(
            &parsed_delete.start_time,
            &parsed_delete.stop_time,
            &parsed_delete.predicate,
        )?;

        self.dml_handler
            .delete(namespace, parsed_delete.table_name, predicate, span_ctx)
            .await?;

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
            None => false,
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

fn response_no_content() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::{io::Write, iter, sync::Arc};

    use assert_matches::assert_matches;

    use flate2::{write::GzEncoder, Compression};
    use hyper::header::HeaderValue;

    use crate::dml_handler::mock::{MockDmlHandler, MockDmlHandlerCall};

    use super::*;

    const MAX_BYTES: usize = 1024;

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
            // Generate the two test cases by feed the same inputs, but varying
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
                    let want_body_len = body.len();

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

                    let dml_handler = Arc::new(MockDmlHandler::default()
                        .with_write_return($dml_write_handler)
                        .with_delete_return($dml_delete_handler)
                    );
                    let delegate = HttpDelegate::new(MAX_BYTES, Arc::clone(&dml_handler));

                    let got = delegate.route(request).await;
                    assert_matches!(got, $want_result);

                    // All successful responses should have a NO_CONTENT code
                    if let Ok(v) = got {
                        assert_eq!(v.status(), StatusCode::NO_CONTENT);
                    }

                    let calls = dml_handler.calls();
                    assert_matches!(calls.as_slice(), $($want_dml_calls)+);

                    // If this was a write op, ensure the body length matches
                    // the above input.
                    if let Some(MockDmlHandlerCall::Write{body_len, ..}) = calls.get(0) {
                        assert_eq!(*body_len, want_body_len);
                    }
                }
            }
        };
        (encoding=plain, $body:ident) => {
            $body
        };
        (encoding=gzip, $body:ident) => {{
            // Apply gzip compression to the body
            let mut e = GzEncoder::new(Vec::new(), Compression::default());
            e.write_all(&$body).unwrap();
            e.finish().expect("failed to compress test body")
        }};
        (encoding_header=plain, $request:ident) => {};
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

    // Wrapper over test_http_handler specifically for delete requests.
    macro_rules! test_delete_handler {
        (
            $name:ident,
            query_string = $query_string:expr,   // Request URI query string
            body = $body:expr,                   // Request body content
            dml_handler = $dml_handler:expr,     // DML delete handler response (if called)
            want_result = $want_result:pat,
            want_dml_calls = $($want_dml_calls:tt )+
        ) => {
            paste::paste! {
                test_http_handler!(
                    [<delete_ $name>],
                    uri = format!("https://bananas.example/api/v2/delete{}", $query_string),
                    body = $body,
                    dml_write_handler = [],
                    dml_delete_handler = $dml_handler,
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
        want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, "bananas_test");
        }
    );

    test_write_handler!(
        no_query_params,
        query_string = "",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        no_org_bucket,
        query_string = "?",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::DecodeFail(_))),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        empty_org_bucket,
        query_string = "?org=&bucket=",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
        want_dml_calls = [] // None
    );

    test_write_handler!(
        invalid_org_bucket,
        query_string = format!("?org=test&bucket={}", "A".repeat(1000)),
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::MappingFail(_))),
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
        dml_handler = [Err(DmlError::DatabaseNotFound("bananas_test".to_string()))],
        want_result = Err(Error::DmlHandler(DmlError::DatabaseNotFound(_))),
        want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, "bananas_test");
        }
    );

    test_write_handler!(
        dml_handler_error,
        query_string = "?org=bananas&bucket=test",
        body = "platanos,tag1=A,tag2=B val=42i 123456".as_bytes(),
        dml_handler = [Err(DmlError::Internal("💣".into()))],
        want_result = Err(Error::DmlHandler(DmlError::Internal(_))),
        want_dml_calls = [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, "bananas_test");
        }
    );

    test_delete_handler!(
        ok,
        query_string = "?org=bananas&bucket=test",
        body = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"_measurement=its_a_table and location=Boston"}"#.as_bytes(),
        dml_handler = [Ok(())],
        want_result = Ok(_),
        want_dml_calls = [MockDmlHandlerCall::Delete{namespace, table, predicate}] => {
            assert_eq!(table, "its_a_table");
            assert_eq!(namespace, "bananas_test");
            assert!(!predicate.exprs.is_empty());
        }
    );

    test_delete_handler!(
        invalid_delete_body,
        query_string = "?org=bananas&bucket=test",
        body = r#"{wat}"#.as_bytes(),
        dml_handler = [],
        want_result = Err(Error::ParseDelete(_)),
        want_dml_calls = []
    );

    test_delete_handler!(
        no_query_params,
        query_string = "",
        body = "".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
        want_dml_calls = [] // None
    );

    test_delete_handler!(
        no_org_bucket,
        query_string = "?",
        body = "".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::DecodeFail(_))),
        want_dml_calls = [] // None
    );

    test_delete_handler!(
        empty_org_bucket,
        query_string = "?org=&bucket=",
        body = "".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::NotSpecified)),
        want_dml_calls = [] // None
    );

    test_delete_handler!(
        invalid_org_bucket,
        query_string = format!("?org=test&bucket={}", "A".repeat(1000)),
        body = "".as_bytes(),
        dml_handler = [Ok(())],
        want_result = Err(Error::InvalidOrgBucket(OrgBucketError::MappingFail(_))),
        want_dml_calls = [] // None
    );

    test_delete_handler!(
        non_utf8_body,
        query_string = "?org=bananas&bucket=test",
        body = vec![0xc3, 0x28],
        dml_handler = [Ok(())],
        want_result = Err(Error::NonUtf8Body(_)),
        want_dml_calls = [] // None
    );

    test_delete_handler!(
        db_not_found,
        query_string = "?org=bananas&bucket=test",
        body = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"_measurement=its_a_table and location=Boston"}"#.as_bytes(),
        dml_handler = [Err(DmlError::DatabaseNotFound("bananas_test".to_string()))],
        want_result = Err(Error::DmlHandler(DmlError::DatabaseNotFound(_))),
        want_dml_calls = [MockDmlHandlerCall::Delete{namespace, table, predicate}] => {
            assert_eq!(table, "its_a_table");
            assert_eq!(namespace, "bananas_test");
            assert!(!predicate.exprs.is_empty());
        }
    );

    test_delete_handler!(
        dml_handler_error,
        query_string = "?org=bananas&bucket=test",
        body = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"_measurement=its_a_table and location=Boston"}"#.as_bytes(),
        dml_handler = [Err(DmlError::Internal("💣".into()))],
        want_result = Err(Error::DmlHandler(DmlError::Internal(_))),
        want_dml_calls = [MockDmlHandlerCall::Delete{namespace, table, predicate}] => {
            assert_eq!(table, "its_a_table");
            assert_eq!(namespace, "bananas_test");
            assert!(!predicate.exprs.is_empty());
        }
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
}
