//! HTTP API service implementations for `server`

use crate::{query_executor, QueryKind};
use crate::{CommonServerState, QueryExecutor};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use authz::http::AuthorizationHeaderExtension;
use authz::Authorizer;
use bytes::{Bytes, BytesMut};
use data_types::NamespaceName;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use hyper::header::ACCEPT;
use hyper::header::AUTHORIZATION;
use hyper::header::CONTENT_ENCODING;
use hyper::header::CONTENT_TYPE;
use hyper::http::HeaderValue;
use hyper::HeaderMap;
use hyper::{Body, Method, Request, Response, StatusCode};
use influxdb3_process::{INFLUXDB3_GIT_HASH_SHORT, INFLUXDB3_VERSION};
use influxdb3_write::catalog::Error as CatalogError;
use influxdb3_write::last_cache::{self, CreateCacheArguments};
use influxdb3_write::persister::TrackedMemoryArrowWriter;
use influxdb3_write::write_buffer::Error as WriteBufferError;
use influxdb3_write::BufferedWriteRequest;
use influxdb3_write::Precision;
use influxdb3_write::WriteBuffer;
use iox_http::write::single_tenant::SingleTenantRequestUnifier;
use iox_http::write::v1::V1_NAMESPACE_RP_SEPARATOR;
use iox_http::write::{WriteParseError, WriteRequestUnifier};
use iox_query_influxql_rewrite as rewrite;
use iox_query_params::StatementParams;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, info};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::convert::Infallible;
use std::fmt::Debug;
use std::pin::Pin;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use unicode_segmentation::UnicodeSegmentation;

mod v1;

#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found")]
    NoHandler,

    /// The request body content is not valid utf8.
    #[error("body content is not valid utf8: {0}")]
    NonUtf8Body(Utf8Error),

    /// The `Content-Encoding` header is invalid and cannot be read.
    #[error("invalid content-encoding header: {0}")]
    NonUtf8ContentEncodingHeader(hyper::header::ToStrError),

    /// The `Content-Type` header is invalid and cannot be read.
    #[error("invalid content-type header: {0}")]
    NonUtf8ContentTypeHeader(hyper::header::ToStrError),

    /// The specified `Content-Encoding` is not acceptable.
    #[error("unacceptable content-encoding: {0}")]
    InvalidContentEncoding(String),

    /// The specified `Content-Type` is not acceptable.
    #[error("unacceptable content-type, expected: {expected}")]
    InvalidContentType { expected: mime::Mime },

    /// The client disconnected.
    #[error("client disconnected")]
    ClientHangup(hyper::Error),

    /// The client sent a request body that exceeds the configured maximum.
    #[error("max request size ({0} bytes) exceeded")]
    RequestSizeExceeded(usize),

    /// Decoding a gzip-compressed stream of data failed.
    #[error("error decoding gzip stream: {0}")]
    InvalidGzip(std::io::Error),

    #[error("invalid mime type ({0})")]
    InvalidMimeType(String),

    /// NamespaceName validation error.
    #[error("error validating namespace name: {0}")]
    InvalidNamespaceName(#[from] data_types::NamespaceNameError),

    /// Failure to decode the provided line protocol.
    #[error("failed to parse line protocol: {0}")]
    ParseLineProtocol(influxdb_line_protocol::Error),

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

    /// The HTTP request method is not supported for this resource
    #[error("unsupported method")]
    UnsupportedMethod,

    /// Hyper serving error
    #[error("error serving http: {0}")]
    ServingHttp(#[from] hyper::Error),

    /// Missing parameters for query
    #[error("missing query parameters 'db' and 'q'")]
    MissingQueryParams,

    /// MIssing parameters for write
    #[error("missing query parameter 'db'")]
    MissingWriteParams,

    #[error("the mime type specified was not valid UTF8: {0}")]
    NonUtf8MimeType(#[from] FromUtf8Error),

    /// Serde decode error
    #[error("serde error: {0}")]
    Serde(#[from] serde_urlencoded::de::Error),

    /// Arrow error
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Hyper error
    #[error("hyper http error: {0}")]
    Hyper(#[from] hyper::http::Error),

    /// WriteBuffer error
    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] influxdb3_write::write_buffer::Error),

    /// Persister error
    #[error("persister error: {0}")]
    Persister(#[from] influxdb3_write::persister::Error),

    // ToStrError
    #[error("to str error: {0}")]
    ToStr(#[from] hyper::header::ToStrError),

    // SerdeJsonError
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    // Influxdb3 Write
    #[error("serde json error: {0}")]
    Influxdb3Write(#[from] influxdb3_write::Error),

    #[error("datafusion error: {0}")]
    Datafusion(#[from] DataFusionError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("query error: {0}")]
    Query(#[from] query_executor::Error),

    #[error(transparent)]
    DbName(#[from] ValidateDbNameError),

    #[error("partial write of line protocol occurred")]
    PartialLpWrite(BufferedWriteRequest),

    #[error("error in InfluxQL statement: {0}")]
    InfluxqlRewrite(#[from] rewrite::Error),

    #[error("must provide only one InfluxQl statement per query")]
    InfluxqlSingleStatement,

    #[error("must specify a 'db' parameter, or provide the database in the InfluxQL query")]
    InfluxqlNoDatabase,

    #[error(
        "provided a database in both the parameters ({param_db}) and \
        query string ({query_db}) that do not match, if providing a query \
        that specifies the database, you can omit the 'database' parameter \
        from your request"
    )]
    InfluxqlDatabaseMismatch { param_db: String, query_db: String },

    #[error("v1 query API error: {0}")]
    V1Query(#[from] v1::QueryError),

    #[error("last cache error: {0}")]
    LastCache(#[from] last_cache::Error),

    #[error("provided database name does not exist")]
    DatabaseDoesNotExist,

    #[error("provided table name does not exist for database")]
    TableDoesNotExist,
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("the request was not authorized")]
    Unauthorized,
    #[error("the request was not in the form of 'Authorization: Bearer <token>'")]
    MalformedRequest,
    #[error("requestor is forbidden from requested resource")]
    Forbidden,
    #[error("to str error: {0}")]
    ToStr(#[from] hyper::header::ToStrError),
}

#[derive(Debug, Serialize)]
struct ErrorMessage<T: Serialize> {
    error: String,
    data: Option<T>,
}

impl Error {
    /// Convert this error into an HTTP [`Response`]
    fn into_response(self) -> Response<Body> {
        match self {
            Self::WriteBuffer(WriteBufferError::CatalogUpdateError(
                err @ (CatalogError::TooManyDbs
                | CatalogError::TooManyColumns
                | CatalogError::TooManyTables),
            )) => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: err.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(body)
                    .unwrap()
            }
            Self::WriteBuffer(WriteBufferError::ParseError(err)) => {
                let err = ErrorMessage {
                    error: "parsing failed for write_lp endpoint".into(),
                    data: Some(err),
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::DbName(e) => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: e.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::PartialLpWrite(data) => {
                let err = ErrorMessage {
                    error: "partial write of line protocol occurred".into(),
                    data: Some(data.invalid_lines),
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::UnsupportedMethod => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: self.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = Body::from(serialized);
                Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(body)
                    .unwrap()
            }
            Self::SerdeJson(_) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(self.to_string()))
                .unwrap(),
            Self::LastCache(ref lc_err) => match lc_err {
                last_cache::Error::InvalidCacheSize
                | last_cache::Error::CacheAlreadyExists { .. }
                | last_cache::Error::KeyColumnDoesNotExist { .. }
                | last_cache::Error::InvalidKeyColumn
                | last_cache::Error::ValueColumnDoesNotExist { .. } => Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(lc_err.to_string()))
                    .unwrap(),
                // This variant should not be encountered by the API, as it is thrown during
                // query execution and would be captured there, but avoiding a catch-all arm here
                // in case new variants are added to the enum:
                last_cache::Error::CacheDoesNotExist => Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from(self.to_string()))
                    .unwrap(),
            },
            _ => {
                let body = Body::from(self.to_string());
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(body)
                    .unwrap()
            }
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub(crate) struct HttpApi<W, Q, T> {
    common_state: CommonServerState,
    write_buffer: Arc<W>,
    time_provider: Arc<T>,
    pub(crate) query_executor: Arc<Q>,
    max_request_bytes: usize,
    authorizer: Arc<dyn Authorizer>,
    legacy_write_param_unifier: SingleTenantRequestUnifier,
}

impl<W, Q, T> HttpApi<W, Q, T> {
    pub(crate) fn new(
        common_state: CommonServerState,
        time_provider: Arc<T>,
        write_buffer: Arc<W>,
        query_executor: Arc<Q>,
        max_request_bytes: usize,
        authorizer: Arc<dyn Authorizer>,
    ) -> Self {
        let legacy_write_param_unifier = SingleTenantRequestUnifier::new(Arc::clone(&authorizer));
        Self {
            common_state,
            time_provider,
            write_buffer,
            query_executor,
            max_request_bytes,
            authorizer,
            legacy_write_param_unifier,
        }
    }
}

impl<W, Q, T> HttpApi<W, Q, T>
where
    W: WriteBuffer,
    Q: QueryExecutor,
    T: TimeProvider,
    Error: From<<Q as QueryExecutor>::Error>,
{
    async fn write_lp(&self, req: Request<Body>) -> Result<Response<Body>> {
        let query = req.uri().query().ok_or(Error::MissingWriteParams)?;
        let params: WriteParams = serde_urlencoded::from_str(query)?;
        self.write_lp_inner(params, req, false, false).await
    }

    async fn write_v3(&self, req: Request<Body>) -> Result<Response<Body>> {
        let query = req.uri().query().ok_or(Error::MissingWriteParams)?;
        let params: WriteParams = serde_urlencoded::from_str(query)?;
        self.write_lp_inner(params, req, false, true).await
    }

    async fn write_lp_inner(
        &self,
        params: WriteParams,
        req: Request<Body>,
        accept_rp: bool,
        use_v3: bool,
    ) -> Result<Response<Body>> {
        validate_db_name(&params.db, accept_rp)?;
        info!("write_lp to {}", params.db);

        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        let database = NamespaceName::new(params.db)?;

        let default_time = self.time_provider.now();

        let result = if use_v3 {
            self.write_buffer
                .write_lp_v3(
                    database,
                    body,
                    default_time,
                    params.accept_partial,
                    params.precision,
                )
                .await?
        } else {
            self.write_buffer
                .write_lp(
                    database,
                    body,
                    default_time,
                    params.accept_partial,
                    params.precision,
                )
                .await?
        };

        if result.invalid_lines.is_empty() {
            Ok(Response::new(Body::empty()))
        } else {
            Err(Error::PartialLpWrite(result))
        }
    }

    async fn query_sql(&self, req: Request<Body>) -> Result<Response<Body>> {
        let QueryRequest {
            database,
            query_str,
            format,
            params,
        } = self.extract_query_request::<String>(req).await?;

        info!(%database, %query_str, ?format, "handling query_sql");

        let stream = self
            .query_executor
            .query(&database, &query_str, params, QueryKind::Sql, None, None)
            .await?;

        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(record_batch_stream_to_body(stream, format).await?)
            .map_err(Into::into)
    }

    async fn query_influxql(&self, req: Request<Body>) -> Result<Response<Body>> {
        let QueryRequest {
            database,
            query_str,
            format,
            params,
        } = self.extract_query_request::<Option<String>>(req).await?;

        info!(?database, %query_str, ?format, "handling query_influxql");

        let stream = self
            .query_influxql_inner(database, &query_str, params)
            .await?;

        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(record_batch_stream_to_body(stream, format).await?)
            .map_err(Into::into)
    }

    fn health(&self) -> Result<Response<Body>> {
        let response_body = "OK";
        Ok(Response::new(Body::from(response_body.to_string())))
    }

    fn ping(&self) -> Result<Response<Body>> {
        #[derive(Debug, Serialize)]
        struct PingResponse<'a> {
            version: &'a str,
            revision: &'a str,
        }

        let body = serde_json::to_string(&PingResponse {
            version: &INFLUXDB3_VERSION,
            revision: INFLUXDB3_GIT_HASH_SHORT,
        })
        .unwrap();

        Ok(Response::new(Body::from(body)))
    }

    fn handle_metrics(&self) -> Result<Response<Body>> {
        let mut body: Vec<u8> = Default::default();
        let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut body);
        self.common_state.metrics.report(&mut reporter);

        Ok(Response::new(Body::from(body)))
    }

    /// Parse the request's body into raw bytes, applying the configured size
    /// limits and decoding any content encoding.
    async fn read_body(&self, req: hyper::Request<Body>) -> Result<Bytes> {
        let encoding = req
            .headers()
            .get(&CONTENT_ENCODING)
            .map(|v| v.to_str().map_err(Error::NonUtf8ContentEncodingHeader))
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

    async fn authorize_request(&self, req: &mut Request<Body>) -> Result<(), AuthorizationError> {
        // Extend the request with the authorization token; this is used downstream in some
        // APIs, such as write, that need the full header value to authorize a request.
        let auth_header = req.headers().get(AUTHORIZATION).cloned();
        req.extensions_mut()
            .insert(AuthorizationHeaderExtension::new(auth_header));

        let auth = if let Some(p) = extract_v1_auth_token(req) {
            Some(p)
        } else {
            // We won't need the authorization header anymore and we don't want to accidentally log it.
            // Take it out so we can use it and not log it later by accident.
            req.headers_mut()
                .remove(AUTHORIZATION)
                .map(validate_auth_header)
                .transpose()?
        };

        // Currently we pass an empty permissions list, but in future we may be able to derive
        // the permissions based on the incoming request
        let permissions = self.authorizer.permissions(auth, &[]).await?;

        // Extend the request with the permissions, which may be useful in future
        req.extensions_mut().insert(permissions);

        Ok(())
    }

    async fn extract_query_request<D: DeserializeOwned>(
        &self,
        req: Request<Body>,
    ) -> Result<QueryRequest<D, QueryFormat, StatementParams>> {
        let header_format = QueryFormat::try_from_headers(req.headers())?;
        let request = match *req.method() {
            Method::GET => {
                let query = req.uri().query().ok_or(Error::MissingQueryParams)?;
                let r = serde_urlencoded::from_str::<QueryRequest<D, Option<QueryFormat>, String>>(
                    query,
                )?;
                QueryRequest {
                    database: r.database,
                    query_str: r.query_str,
                    format: r.format,
                    params: r.params.map(|s| serde_json::from_str(&s)).transpose()?,
                }
            }
            Method::POST => {
                let body = self.read_body(req).await?;
                serde_json::from_slice(body.as_ref())?
            }
            _ => return Err(Error::UnsupportedMethod),
        };

        Ok(QueryRequest {
            database: request.database,
            query_str: request.query_str,
            format: request.format.unwrap_or(header_format),
            params: request.params,
        })
    }

    /// Inner function for performing InfluxQL queries
    ///
    /// This is used by both the `/api/v3/query_influxql` and `/api/v1/query`
    /// APIs.
    async fn query_influxql_inner(
        &self,
        database: Option<String>,
        query_str: &str,
        params: Option<StatementParams>,
    ) -> Result<SendableRecordBatchStream> {
        let mut statements = rewrite::parse_statements(query_str)?;

        if statements.len() != 1 {
            return Err(Error::InfluxqlSingleStatement);
        }
        let statement = statements.pop().unwrap();

        let database = match (database, statement.resolve_dbrp()) {
            (None, None) => None,
            (None, Some(db)) | (Some(db), None) => Some(db),
            (Some(p), Some(q)) => {
                if p == q {
                    Some(p)
                } else {
                    return Err(Error::InfluxqlDatabaseMismatch {
                        param_db: p,
                        query_db: q,
                    });
                }
            }
        };

        if statement.statement().is_show_databases() {
            self.query_executor.show_databases()
        } else if statement.statement().is_show_retention_policies() {
            self.query_executor
                .show_retention_policies(database.as_deref(), None)
                .await
        } else {
            let Some(database) = database else {
                return Err(Error::InfluxqlNoDatabase);
            };

            self.query_executor
                .query(
                    &database,
                    // TODO - implement an interface that takes the statement directly,
                    // so we don't need to double down on the parsing
                    &statement.to_statement().to_string(),
                    params,
                    QueryKind::InfluxQl,
                    None,
                    None,
                )
                .await
        }
        .map_err(Into::into)
    }

    async fn configure_last_cache_create(&self, req: Request<Body>) -> Result<Response<Body>> {
        let LastCacheCreateRequest {
            db,
            table,
            name,
            key_columns,
            value_columns,
            count,
            ttl,
        } = self.read_body_json(req).await?;

        let Some(db_schema) = self.write_buffer.catalog().db_schema(&db) else {
            return Err(Error::DatabaseDoesNotExist);
        };

        let Some(tbl_schema) = db_schema.get_table_schema(&table) else {
            return Err(Error::TableDoesNotExist);
        };

        match self
            .write_buffer
            .last_cache()
            .create_cache(CreateCacheArguments {
                db_name: db,
                tbl_name: table,
                schema: tbl_schema.clone(),
                cache_name: name,
                count,
                ttl: ttl.map(Duration::from_secs),
                key_columns,
                value_columns,
            })? {
            Some(cache_name) => Response::builder()
                .status(StatusCode::CREATED)
                .header(CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
                .body(Body::from(
                    serde_json::to_string(&LastCacheCreatedResponse { cache_name }).unwrap(),
                ))
                .map_err(Into::into),
            None => Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .map_err(Into::into),
        }
    }

    /// Delete a last cache entry with the given [`LastCacheDeleteRequest`] parameters
    ///
    /// This will first attempt to parse the parameters from the URI query string, if a query string
    /// is provided, but if not, will attempt to parse them from the request body as JSON.
    async fn configure_last_cache_delete(&self, req: Request<Body>) -> Result<Response<Body>> {
        let LastCacheDeleteRequest { db, table, name } = if let Some(query) = req.uri().query() {
            serde_urlencoded::from_str(query)?
        } else {
            self.read_body_json(req).await?
        };

        self.write_buffer
            .last_cache()
            .delete_cache(&db, &table, &name)?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap())
    }

    async fn read_body_json<ReqBody: DeserializeOwned>(
        &self,
        req: hyper::Request<Body>,
    ) -> Result<ReqBody> {
        if !json_content_type(req.headers()) {
            return Err(Error::InvalidContentType {
                expected: mime::APPLICATION_JSON,
            });
        }
        let bytes = self.read_body(req).await?;
        serde_json::from_slice(&bytes).map_err(Into::into)
    }
}

/// Check that the content type is application/json
fn json_content_type(headers: &HeaderMap) -> bool {
    let content_type = if let Some(content_type) = headers.get(CONTENT_TYPE) {
        content_type
    } else {
        return false;
    };

    let content_type = if let Ok(content_type) = content_type.to_str() {
        content_type
    } else {
        return false;
    };

    let mime = if let Ok(mime) = content_type.parse::<mime::Mime>() {
        mime
    } else {
        return false;
    };

    let is_json_content_type = mime.type_() == "application"
        && (mime.subtype() == "json" || mime.suffix().map_or(false, |name| name == "json"));

    is_json_content_type
}

#[derive(Debug, Deserialize)]
struct V1AuthParameters {
    #[serde(rename = "p")]
    password: Option<String>,
}

/// Extract the authentication token for v1 API requests, which may use the `p` query
/// parameter to pass the authentication token.
fn extract_v1_auth_token(req: &mut Request<Body>) -> Option<Vec<u8>> {
    req.uri()
        .path_and_query()
        .and_then(|pq| match pq.path() {
            "/query" | "/write" => pq.query(),
            _ => None,
        })
        .map(serde_urlencoded::from_str::<V1AuthParameters>)
        .transpose()
        .ok()
        .flatten()
        .and_then(|params| params.password)
        .map(String::into_bytes)
}

fn validate_auth_header(header: HeaderValue) -> Result<Vec<u8>, AuthorizationError> {
    // Split the header value into two parts
    let mut header = header.to_str()?.split(' ');

    // Check that the header is the 'Bearer' auth scheme
    let bearer = header.next().ok_or(AuthorizationError::MalformedRequest)?;
    if bearer != "Bearer" {
        return Err(AuthorizationError::MalformedRequest);
    }

    // Get the token that we want to hash to check the request is valid
    let token = header.next().ok_or(AuthorizationError::MalformedRequest)?;

    // There should only be two parts the 'Bearer' scheme and the actual
    // token, error otherwise
    if header.next().is_some() {
        return Err(AuthorizationError::MalformedRequest);
    }

    Ok(token.as_bytes().to_vec())
}

impl From<authz::Error> for AuthorizationError {
    fn from(auth_error: authz::Error) -> Self {
        match auth_error {
            authz::Error::Forbidden => Self::Forbidden,
            _ => Self::Unauthorized,
        }
    }
}

/// Validate a database name
///
/// A valid name:
/// - Starts with a letter or a number
/// - Is ASCII not UTF-8
/// - Contains only letters, numbers, underscores or hyphens
/// - if `accept_rp` is true, then a single slash ('/') is allowed, separating the
///   the database name from the retention policy name, e.g., '<db_name>/<rp_name>'
fn validate_db_name(name: &str, accept_rp: bool) -> Result<(), ValidateDbNameError> {
    if name.is_empty() {
        return Err(ValidateDbNameError::Empty);
    }
    let mut is_first_char = true;
    let mut rp_seperator_found = false;
    let mut last_char = None;
    for grapheme in name.graphemes(true) {
        if grapheme.as_bytes().len() > 1 {
            // In the case of a unicode we need to handle multibyte chars
            return Err(ValidateDbNameError::InvalidChar);
        }
        let char = grapheme.as_bytes()[0] as char;
        if !is_first_char {
            match (accept_rp, rp_seperator_found, char) {
                (true, true, V1_NAMESPACE_RP_SEPARATOR) => {
                    return Err(ValidateDbNameError::InvalidRetentionPolicy)
                }
                (true, false, V1_NAMESPACE_RP_SEPARATOR) => {
                    rp_seperator_found = true;
                }
                (false, _, char)
                    if !(char.is_ascii_alphanumeric() || char == '_' || char == '-') =>
                {
                    return Err(ValidateDbNameError::InvalidChar)
                }
                _ => (),
            }
        } else {
            if !char.is_ascii_alphanumeric() {
                return Err(ValidateDbNameError::InvalidStartChar);
            }
            is_first_char = false;
        }
        last_char.replace(char);
    }

    if last_char.is_some_and(|c| c == '/') {
        return Err(ValidateDbNameError::InvalidRetentionPolicy);
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ValidateDbNameError {
    #[error(
        "invalid character in database name: must be ASCII, \
        containing only letters, numbers, underscores, or hyphens"
    )]
    InvalidChar,
    #[error("db name did not start with a number or letter")]
    InvalidStartChar,
    #[error(
        "db name with invalid retention policy, if providing a \
        retention policy name, must be of form '<db_name>/<rp_name>'"
    )]
    InvalidRetentionPolicy,
    #[error("db name cannot be empty")]
    Empty,
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryRequest<D, F, P> {
    #[serde(rename = "db")]
    pub(crate) database: D,
    #[serde(rename = "q")]
    pub(crate) query_str: String,
    pub(crate) format: F,
    pub(crate) params: Option<P>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueryFormat {
    Parquet,
    Csv,
    Pretty,
    Json,
}

impl QueryFormat {
    fn as_content_type(&self) -> &str {
        match self {
            Self::Parquet => "application/vnd.apache.parquet",
            Self::Csv => "text/csv",
            Self::Pretty => "text/plain; charset=utf-8",
            Self::Json => "application/json",
        }
    }

    fn try_from_headers(headers: &HeaderMap) -> Result<Self> {
        match headers.get(ACCEPT).map(HeaderValue::as_bytes) {
            // Accept Headers use the MIME types maintained by IANA here:
            // https://www.iana.org/assignments/media-types/media-types.xhtml
            // Note parquet hasn't been accepted yet just Arrow, but there
            // is the possibility it will be:
            // https://issues.apache.org/jira/browse/PARQUET-1889
            Some(b"application/vnd.apache.parquet") => Ok(Self::Parquet),
            Some(b"text/csv") => Ok(Self::Csv),
            Some(b"text/plain") => Ok(Self::Pretty),
            Some(b"application/json" | b"*/*") | None => Ok(Self::Json),
            Some(mime_type) => match String::from_utf8(mime_type.to_vec()) {
                Ok(s) => Err(Error::InvalidMimeType(s)),
                Err(e) => Err(Error::NonUtf8MimeType(e)),
            },
        }
    }
}

async fn record_batch_stream_to_body(
    stream: Pin<Box<dyn RecordBatchStream + Send>>,
    format: QueryFormat,
) -> Result<Body, Error> {
    fn to_json(batches: Vec<RecordBatch>) -> Result<Bytes> {
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        for batch in batches {
            writer.write(&batch)?;
        }

        writer.finish()?;

        Ok(Bytes::from(writer.into_inner()))
    }

    fn to_csv(batches: Vec<RecordBatch>) -> Result<Bytes> {
        let mut writer = arrow_csv::writer::Writer::new(Vec::new());
        for batch in batches {
            writer.write(&batch)?;
        }

        Ok(Bytes::from(writer.into_inner()))
    }

    fn to_pretty(batches: Vec<RecordBatch>) -> Result<Bytes> {
        Ok(Bytes::from(format!(
            "{}",
            pretty::pretty_format_batches(&batches)?
        )))
    }

    fn to_parquet(batches: Vec<RecordBatch>) -> Result<Bytes> {
        let mut bytes = Vec::new();
        let mem_pool = Arc::new(UnboundedMemoryPool::default());
        let mut writer =
            TrackedMemoryArrowWriter::try_new(&mut bytes, batches[0].schema(), mem_pool)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        Ok(Bytes::from(bytes))
    }

    let batches = stream.try_collect::<Vec<RecordBatch>>().await?;

    match format {
        QueryFormat::Pretty => to_pretty(batches),
        QueryFormat::Parquet => to_parquet(batches),
        QueryFormat::Csv => to_csv(batches),
        QueryFormat::Json => to_json(batches),
    }
    .map(Body::from)
}

// This is a hack around the fact that bool default is false not true
const fn true_fn() -> bool {
    true
}
#[derive(Debug, Deserialize)]
pub(crate) struct WriteParams {
    pub(crate) db: String,
    #[serde(default = "true_fn")]
    pub(crate) accept_partial: bool,
    #[serde(default)]
    pub(crate) precision: Precision,
}

impl From<iox_http::write::WriteParams> for WriteParams {
    fn from(legacy: iox_http::write::WriteParams) -> Self {
        Self {
            db: legacy.namespace.to_string(),
            // legacy behaviour was to not accept partial:
            accept_partial: false,
            precision: legacy.precision.into(),
        }
    }
}

/// Request definition for the `POST /api/v3/configure/last_cache` API
#[derive(Debug, Deserialize)]
struct LastCacheCreateRequest {
    db: String,
    table: String,
    name: Option<String>,
    key_columns: Option<Vec<String>>,
    value_columns: Option<Vec<String>>,
    count: Option<usize>,
    ttl: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LastCacheCreatedResponse {
    cache_name: String,
}

/// Request definition for the `DELETE /api/v3/configure/last_cache` API
#[derive(Debug, Deserialize)]
struct LastCacheDeleteRequest {
    db: String,
    table: String,
    name: String,
}

pub(crate) async fn route_request<W: WriteBuffer, Q: QueryExecutor, T: TimeProvider>(
    http_server: Arc<HttpApi<W, Q, T>>,
    mut req: Request<Body>,
) -> Result<Response<Body>, Infallible>
where
    Error: From<<Q as QueryExecutor>::Error>,
{
    if let Err(e) = http_server.authorize_request(&mut req).await {
        match e {
            AuthorizationError::Unauthorized => {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .unwrap())
            }
            AuthorizationError::MalformedRequest => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("{\"error\":\
                        \"Authorization header was malformed and should be in the form 'Authorization: Bearer <token>'\"\
                    }"))
                    .unwrap());
            }
            AuthorizationError::Forbidden => {
                return Ok(Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Body::empty())
                    .unwrap())
            }
            // We don't expect this to happen, but if the header is messed up
            // better to handle it then not at all
            AuthorizationError::ToStr(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap())
            }
        }
    }
    debug!(request = ?req,"Processing request");

    let method = req.method().clone();
    let uri = req.uri().clone();
    let content_length = req.headers().get("content-length").cloned();

    let response = match (method.clone(), uri.path()) {
        (Method::POST, "/write") => {
            let params = match http_server.legacy_write_param_unifier.parse_v1(&req).await {
                Ok(p) => p.into(),
                Err(e) => return Ok(legacy_write_error_to_response(e)),
            };

            http_server.write_lp_inner(params, req, true, false).await
        }
        (Method::POST, "/api/v2/write") => {
            let params = match http_server.legacy_write_param_unifier.parse_v2(&req).await {
                Ok(p) => p.into(),
                Err(e) => return Ok(legacy_write_error_to_response(e)),
            };

            http_server.write_lp_inner(params, req, false, false).await
        }
        (Method::POST, "/api/v3/write") => http_server.write_v3(req).await,
        (Method::POST, "/api/v3/write_lp") => http_server.write_lp(req).await,
        (Method::GET | Method::POST, "/api/v3/query_sql") => http_server.query_sql(req).await,
        (Method::GET | Method::POST, "/api/v3/query_influxql") => {
            http_server.query_influxql(req).await
        }
        (Method::GET, "/query") => http_server.v1_query(req).await,
        (Method::GET, "/health" | "/api/v1/health") => http_server.health(),
        (Method::GET | Method::POST, "/ping") => http_server.ping(),
        (Method::GET, "/metrics") => http_server.handle_metrics(),
        (Method::POST, "/api/v3/configure/last_cache") => {
            http_server.configure_last_cache_create(req).await
        }
        (Method::DELETE, "api/v3/configure/last_cache") => {
            http_server.configure_last_cache_delete(req).await
        }
        _ => {
            let body = Body::from("not found");
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(body)
                .unwrap())
        }
    };

    // TODO: Move logging to TraceLayer
    match response {
        Ok(response) => {
            debug!(?response, "Successfully processed request");
            Ok(response)
        }
        Err(error) => {
            error!(%error, %method, %uri, ?content_length, "Error while handling request");
            Ok(error.into_response())
        }
    }
}

fn legacy_write_error_to_response(e: WriteParseError) -> Response<Body> {
    let err: ErrorMessage<()> = ErrorMessage {
        error: e.to_string(),
        data: None,
    };
    let serialized = serde_json::to_string(&err).unwrap();
    let body = Body::from(serialized);
    let status = match e {
        WriteParseError::NotImplemented => StatusCode::NOT_FOUND,
        WriteParseError::SingleTenantError(e) => StatusCode::from(&e),
        WriteParseError::MultiTenantError(e) => StatusCode::from(&e),
    };
    Response::builder().status(status).body(body).unwrap()
}

#[cfg(test)]
mod tests {
    use super::validate_db_name;
    use super::ValidateDbNameError;

    macro_rules! assert_validate_db_name {
        ($name:literal, $accept_rp:literal, $expected:pat) => {
            let actual = validate_db_name($name, $accept_rp);
            assert!(matches!(&actual, $expected), "got: {actual:?}",);
        };
    }

    #[test]
    fn test_validate_db_name() {
        assert_validate_db_name!("foo/bar", false, Err(ValidateDbNameError::InvalidChar));
        assert!(validate_db_name("foo/bar", true).is_ok());
        assert_validate_db_name!(
            "foo/bar/baz",
            true,
            Err(ValidateDbNameError::InvalidRetentionPolicy)
        );
        assert_validate_db_name!(
            "foo/",
            true,
            Err(ValidateDbNameError::InvalidRetentionPolicy)
        );
        assert_validate_db_name!("foo/bar", false, Err(ValidateDbNameError::InvalidChar));
        assert_validate_db_name!("foo/bar/baz", false, Err(ValidateDbNameError::InvalidChar));
        assert_validate_db_name!("_foo", false, Err(ValidateDbNameError::InvalidStartChar));
        assert_validate_db_name!("", false, Err(ValidateDbNameError::Empty));
    }
}
