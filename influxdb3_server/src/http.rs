//! HTTP API service implementations for `server`

use crate::{CommonServerState, all_paths};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use authz::http::AuthorizationHeaderExtension;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use bytes::{Bytes, BytesMut};
use chrono::DateTime;
use data_types::NamespaceName;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::FutureExt;
use futures::{StreamExt, TryStreamExt};
use http::header::ACCESS_CONTROL_ALLOW_ORIGIN;
use http_body_util::BodyExt;
use hyper::HeaderMap;
use hyper::header::AUTHORIZATION;
use hyper::header::CONTENT_ENCODING;
use hyper::header::CONTENT_TYPE;
use hyper::http::HeaderValue;
use hyper::{Method, StatusCode};
use influxdb_influxql_parser::select::GroupByClause;
use influxdb_influxql_parser::statement::Statement;
use influxdb3_authz::{
    AuthProvider, AuthenticatorError, NoAuthAuthenticator, ResourceAuthorizationError,
};
use influxdb3_cache::distinct_cache;
use influxdb3_cache::last_cache;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::HardDeletionTime;
use influxdb3_catalog::log::FieldDataType;
use influxdb3_internal_api::query_executor::{QueryExecutor, QueryExecutorError};
use influxdb3_process::{
    INFLUXDB3_BUILD, INFLUXDB3_GIT_HASH_SHORT, INFLUXDB3_VERSION, ProcessUuidWrapper,
};
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_processing_engine::manager::ProcessingEngineError;
use influxdb3_types::http::*;
use influxdb3_write::BufferedWriteRequest;
use influxdb3_write::Precision;
use influxdb3_write::WriteBuffer;
use influxdb3_write::persister::TrackedMemoryArrowWriter;
use influxdb3_write::write_buffer::Error as WriteBufferError;
use iox_http::write::single_tenant::SingleTenantRequestUnifier;
use iox_http::write::v1::V1_NAMESPACE_RP_SEPARATOR;
use iox_http::write::{WriteParseError, WriteRequestUnifier};
use iox_http_util::{
    Request, Response, ResponseBody, ResponseBuilder, bytes_to_response_body, empty_response_body,
    stream_results_to_response_body,
};
use iox_query_influxql_rewrite as rewrite;
use iox_query_params::StatementParams;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, error, info, trace, warn};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::task::Poll;
use std::{convert::Infallible, time::Duration};
use thiserror::Error;
use trace::ctx::SpanContext;
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;

mod v1;

pub(crate) const UNKNOWN_VAL: &str = "unknown";

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
    ClientHangup(Box<dyn StdError + Send + Sync>),

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
    #[error("missing query parameters 'db'")]
    MissingDeleteDatabaseParams,

    /// Missing parameters for query
    #[error("missing query parameters 'db' and 'q'")]
    MissingQueryParams,

    /// Missing the `q` parameter in the v1 /query API
    #[error("missing query parameter 'q'")]
    MissingQueryV1Params,

    /// MIssing parameters for write
    #[error("missing query parameter 'db'")]
    MissingWriteParams,

    #[error("the mime type specified was not valid UTF8: {0}")]
    NonUtf8MimeType(#[from] FromUtf8Error),

    /// Serde decode error
    #[error("serde error: {0}")]
    SerdeUrlDecoding(#[from] serde_urlencoded::de::Error),

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
    Persister(#[from] influxdb3_write::persister::PersisterError),

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
    Query(#[from] QueryExecutorError),

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

    #[error("Operation with object store failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error("Python plugins not enabled on this server")]
    PythonPluginsNotEnabled,

    #[error("Plugin error: {0}")]
    Plugin(#[from] influxdb3_processing_engine::plugins::PluginError),

    #[error("Processing engine error: {0}")]
    ProcessingEngine(#[from] influxdb3_processing_engine::manager::ProcessingEngineError),

    #[error(transparent)]
    Influxdb3TypesHttp(#[from] influxdb3_types::http::Error),

    #[error("The following Database does not exist: {0}")]
    MissingDb(String),

    #[error("The following Database Table does not exist: {0}")]
    MissingTable(String),

    #[error("Cannot parse the given human time: {0}")]
    ParsingHumanTime(#[source] humantime::DurationError),

    #[error("Cannot parse the timestamp: {0}")]
    ParsingTimestamp(#[from] chrono::ParseError),

    #[error("Timestamp is out of range")]
    TimestampOutOfRange,

    #[error("Authorization error: {0}")]
    ResourceAuthorization(#[from] ResourceAuthorizationError),

    #[error("Authentication error: {0}")]
    Authentication(#[from] AuthenticatorError),

    #[error("Current node mode does not use the processing engine")]
    NoProcessingEngine,

    #[error(transparent)]
    LegacyWriteParse(#[from] WriteParseError),
}

#[derive(Debug, Error)]
pub(crate) enum AuthenticationError {
    #[error("the request was not authenticated")]
    Unauthenticated,
    #[error(
        "Authorization header was malformed, the request was not in the form of 'Authorization: <auth-scheme> <token>', supported auth-schemes are Bearer, Token and Basic"
    )]
    MalformedRequest,
    #[error("requestor is forbidden from requested resource")]
    Forbidden,
    #[error("to str error: {0}")]
    ToStr(#[from] hyper::header::ToStrError),
}

/// The /v2/write API expects errors to be JSON formatted like so:
///
/// ```json
/// {"code": "<code>", "message": "<detailed message>"}
/// ```
///
/// `code` can be one of:
/// * `invalid`
/// * `unauthorized`
/// * `not found`
/// * `request too large`
/// * `internal error`
///
/// See: <https://docs.influxdata.com/influxdb3/clustered/api/v2/#tag/Write>
///
/// This type implements `IntoResponse` to ensure that errors conform to this structure using the
/// existing `Error` type.
#[derive(Debug)]
struct V2WriteApiError(Error);

impl V2WriteApiError {
    fn to_code(&self) -> V2WriteErrorCode {
        match &self.0 {
            Error::NonUtf8Body(_)
            | Error::NonUtf8ContentEncodingHeader(_)
            | Error::NonUtf8ContentTypeHeader(_)
            | Error::InvalidContentEncoding(_)
            | Error::InvalidContentType { .. }
            | Error::InvalidGzip(_)
            | Error::InvalidMimeType(_)
            | Error::InvalidNamespaceName(_)
            | Error::ParseLineProtocol(_)
            | Error::RequestLimit
            | Error::Forbidden
            | Error::UnsupportedMethod
            | Error::NonUtf8MimeType(_)
            | Error::SerdeUrlDecoding(_)
            | Error::WriteBuffer(_)
            | Error::DbName(_)
            | Error::LegacyWriteParse(_)
            | Error::Authentication(_) => V2WriteErrorCode::Invalid,
            Error::Catalog(e) => match e {
                CatalogError::AlreadyExists
                | CatalogError::InvalidConfiguration { .. }
                | CatalogError::InvalidColumnType { .. }
                | CatalogError::ReservedColumn(_)
                | CatalogError::TooManyColumns(_)
                | CatalogError::TooManyTagColumns(_)
                | CatalogError::TooManyTables(_)
                | CatalogError::TooManyDbs(_)
                | CatalogError::TooManyFields { .. }
                | CatalogError::FieldTypeMismatch { .. }
                | CatalogError::SeriesKeyMismatch { .. }
                | CatalogError::DuplicateColumn { .. } => V2WriteErrorCode::Invalid,
                CatalogError::NotFound
                | CatalogError::DatabaseNotFound { .. }
                | CatalogError::TableNotFound { .. } => V2WriteErrorCode::NotFound,
                _ => V2WriteErrorCode::InternalError,
            },
            Error::Unauthenticated => V2WriteErrorCode::Unauthorized,
            Error::ResourceAuthorization(ResourceAuthorizationError::Unauthorized) => {
                V2WriteErrorCode::Unauthorized
            }
            Error::RequestSizeExceeded(_) => V2WriteErrorCode::RequestTooLarge,
            _ => V2WriteErrorCode::InternalError,
        }
    }
}

#[derive(Debug)]
enum V2WriteErrorCode {
    Invalid,
    Unauthorized,
    NotFound,
    RequestTooLarge,
    InternalError,
}

impl V2WriteErrorCode {
    fn to_str(&self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Unauthorized => "unauthorized",
            Self::NotFound => "not found",
            Self::RequestTooLarge => "request too large",
            Self::InternalError => "internal error",
        }
    }

    fn to_status_code(&self) -> StatusCode {
        match self {
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl Serialize for V2WriteErrorCode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_str())
    }
}

#[derive(Debug, Serialize)]
struct V2ErrorResponse {
    code: V2WriteErrorCode,
    message: String,
}

impl IntoResponse for V2WriteApiError {
    fn into_response(self) -> Response {
        let code = self.to_code();
        let status = code.to_status_code();
        let message = if let V2WriteErrorCode::Unauthorized = code {
            // Ensure an opaque error message for 401 errors.
            warn!(
                error = self.0.to_string(),
                "unauthorized access attempt to /v2/write API"
            );
            "unauthorized access".to_string()
        } else {
            self.0.to_string()
        };
        let response = V2ErrorResponse { code, message };
        let body = bytes_to_response_body(serde_json::to_vec(&response).unwrap());
        ResponseBuilder::new().status(status).body(body).unwrap()
    }
}

#[derive(Debug, Serialize)]
struct ErrorMessage<T: Serialize> {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for CatalogError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => ResponseBuilder::new()
                .status(StatusCode::NOT_FOUND)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::AlreadyExists | Self::AlreadyDeleted => ResponseBuilder::new()
                .status(StatusCode::CONFLICT)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::InvalidConfiguration { .. }
            | Self::InvalidDistinctCacheColumnType
            | Self::InvalidLastCacheKeyColumnType
            | Self::ReservedColumn(_)
            | Self::DuplicateColumn { .. }
            | Self::InvalidColumnType { .. } => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::TooManyColumns(_)
            | Self::TooManyTables(_)
            | Self::TooManyDbs(_)
            | Self::TooManyTagColumns(_)
            | Self::TooManyFields { .. } => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: self.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(body)
                    .unwrap()
            }
            _ => {
                let body = bytes_to_response_body(self.to_string());
                ResponseBuilder::new()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(body)
                    .unwrap()
            }
        }
    }
}

impl IntoResponse for Error {
    /// Convert this error into an HTTP [`Response`]
    fn into_response(self) -> Response {
        debug!(error = ?self, "API error");
        match self {
            Self::Catalog(err @ CatalogError::CannotDeleteOperatorToken) => ResponseBuilder::new()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(bytes_to_response_body(err.to_string()))
                .unwrap(),
            Self::Catalog(err @ CatalogError::TokenNameAlreadyExists { .. }) => {
                ResponseBuilder::new()
                    .status(StatusCode::CONFLICT)
                    .body(bytes_to_response_body(err.to_string()))
                    .unwrap()
            }
            Self::Catalog(err) | Self::WriteBuffer(WriteBufferError::CatalogUpdateError(err)) => {
                err.into_response()
            }
            Self::Query(err @ QueryExecutorError::MethodNotImplemented(_)) => {
                ResponseBuilder::new()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(bytes_to_response_body(err.to_string()))
                    .unwrap()
            }
            Self::WriteBuffer(err @ WriteBufferError::DatabaseNotFound { db_name: _ }) => {
                ResponseBuilder::new()
                    .status(StatusCode::NOT_FOUND)
                    .body(bytes_to_response_body(err.to_string()))
                    .unwrap()
            }
            Self::WriteBuffer(
                err @ WriteBufferError::TableNotFound {
                    db_name: _,
                    table_name: _,
                },
            ) => ResponseBuilder::new()
                .status(StatusCode::NOT_FOUND)
                .body(bytes_to_response_body(err.to_string()))
                .unwrap(),
            Self::WriteBuffer(err @ WriteBufferError::DatabaseExists(_)) => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(err.to_string()))
                .unwrap(),
            Self::WriteBuffer(WriteBufferError::ParseError(err)) => {
                let err = ErrorMessage {
                    error: "parsing failed for write_lp endpoint".into(),
                    data: Some(err),
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::WriteBuffer(err @ WriteBufferError::EmptyWrite) => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(err.to_string()))
                .unwrap(),
            Self::WriteBuffer(err @ WriteBufferError::ColumnDoesNotExist(_)) => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: err.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::WriteBuffer(err @ WriteBufferError::DatabaseDeleted(_)) => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: err.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::WriteBuffer(WriteBufferError::LastCacheError(ref lc_err)) => match lc_err {
                last_cache::Error::InvalidCacheSize
                | last_cache::Error::CacheAlreadyExists { .. }
                | last_cache::Error::ColumnDoesNotExistByName { .. }
                | last_cache::Error::ColumnDoesNotExistById { .. }
                | last_cache::Error::KeyColumnDoesNotExist { .. }
                | last_cache::Error::KeyColumnDoesNotExistByName { .. }
                | last_cache::Error::InvalidKeyColumn { .. }
                | last_cache::Error::ValueColumnDoesNotExist { .. } => ResponseBuilder::new()
                    .status(StatusCode::BAD_REQUEST)
                    .body(bytes_to_response_body(lc_err.to_string()))
                    .unwrap(),
                last_cache::Error::CacheDoesNotExist => ResponseBuilder::new()
                    .status(StatusCode::NOT_FOUND)
                    .body(bytes_to_response_body(self.to_string()))
                    .unwrap(),
            },
            Self::WriteBuffer(WriteBufferError::DistinctCacheError(ref mc_err)) => match mc_err {
                distinct_cache::ProviderError::Cache(cache_err) => match cache_err {
                    distinct_cache::CacheError::EmptyColumnSet
                    | distinct_cache::CacheError::NonTagOrStringColumn { .. }
                    | distinct_cache::CacheError::ConfigurationMismatch { .. } => {
                        ResponseBuilder::new()
                            .status(StatusCode::BAD_REQUEST)
                            .body(bytes_to_response_body(mc_err.to_string()))
                            .unwrap()
                    }
                    distinct_cache::CacheError::Unexpected(_) => ResponseBuilder::new()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(bytes_to_response_body(mc_err.to_string()))
                        .unwrap(),
                },
                distinct_cache::ProviderError::CacheNotFound => ResponseBuilder::new()
                    .status(StatusCode::NOT_FOUND)
                    .body(bytes_to_response_body(mc_err.to_string()))
                    .unwrap(),
                distinct_cache::ProviderError::Unexpected(_) => ResponseBuilder::new()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(bytes_to_response_body(mc_err.to_string()))
                    .unwrap(),
            },
            Self::DbName(e) => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: e.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::BAD_REQUEST)
                    .body(body)
                    .unwrap()
            }
            Self::PartialLpWrite(data) => {
                let limit_hit = data.invalid_lines.iter().any(|err| {
                    err.error_message
                        .starts_with("Update to schema would exceed number of")
                        || err
                            .error_message
                            .starts_with("Adding a new database would exceed limit of")
                });
                let err = ErrorMessage {
                    error: "partial write of line protocol occurred".into(),
                    data: Some(data.invalid_lines),
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(if limit_hit {
                        StatusCode::UNPROCESSABLE_ENTITY
                    } else {
                        StatusCode::BAD_REQUEST
                    })
                    .body(body)
                    .unwrap()
            }
            Self::UnsupportedMethod => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: self.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(body)
                    .unwrap()
            }
            Self::Query(QueryExecutorError::DatabaseNotFound { .. }) => {
                let err: ErrorMessage<()> = ErrorMessage {
                    error: self.to_string(),
                    data: None,
                };
                let serialized = serde_json::to_string(&err).unwrap();
                let body = bytes_to_response_body(serialized);
                ResponseBuilder::new()
                    .status(StatusCode::NOT_FOUND)
                    .body(body)
                    .unwrap()
            }
            Self::SerdeJson(_) => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::InvalidContentEncoding(_) => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::InvalidContentType { .. } => ResponseBuilder::new()
                .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::SerdeUrlDecoding(_) => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::ParsingHumanTime(_) => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::MissingQueryParams
            | Self::MissingQueryV1Params
            | Self::MissingWriteParams
            | Self::MissingDeleteDatabaseParams => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            Self::ParsingTimestamp(_) | Self::TimestampOutOfRange => ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body(self.to_string()))
                .unwrap(),
            _ => {
                let body = bytes_to_response_body(self.to_string());
                ResponseBuilder::new()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(body)
                    .unwrap()
            }
        }
    }
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct HttpApi {
    common_state: CommonServerState,
    write_buffer: Arc<dyn WriteBuffer>,
    processing_engine: Arc<ProcessingEngineManagerImpl>,
    time_provider: Arc<dyn TimeProvider>,
    pub(crate) query_executor: Arc<dyn QueryExecutor>,
    max_request_bytes: usize,
    authorizer: Arc<dyn AuthProvider>,
    legacy_write_param_unifier: SingleTenantRequestUnifier,
}

/// Wrapper for HttpApi used by the recovery endpoint that includes a shutdown token
pub(crate) struct RecoveryHttpApi {
    http_api: Arc<HttpApi>,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl RecoveryHttpApi {
    pub(crate) fn new(
        http_api: Arc<HttpApi>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            http_api,
            cancellation_token,
        }
    }
}

impl HttpApi {
    pub fn new(
        common_state: CommonServerState,
        time_provider: Arc<dyn TimeProvider>,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        processing_engine: Arc<ProcessingEngineManagerImpl>,
        max_request_bytes: usize,
        authorizer: Arc<dyn AuthProvider>,
    ) -> Self {
        // there is a global authentication setup, passing in auth provider just does the same
        // check twice. So, instead we pass in a NoAuthAuthenticator to avoid authenticating twice.
        let legacy_write_param_unifier =
            SingleTenantRequestUnifier::new(Arc::clone(&NoAuthAuthenticator.upcast()));
        Self {
            common_state,
            time_provider,
            write_buffer,
            query_executor,
            max_request_bytes,
            authorizer,
            legacy_write_param_unifier,
            processing_engine,
        }
    }
}

impl HttpApi {
    async fn write_lp(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().ok_or(Error::MissingWriteParams)?;
        let params: WriteParams = serde_urlencoded::from_str(query)?;
        self.write_lp_inner(params, req, false).await
    }

    async fn write_lp_inner(
        &self,
        params: WriteParams,
        req: Request,
        accept_rp: bool,
    ) -> Result<Response> {
        validate_db_name(&params.db, accept_rp)?;
        let body = self.read_body(req).await?;
        let body = std::str::from_utf8(&body).map_err(Error::NonUtf8Body)?;

        let database = NamespaceName::new(params.db)?;

        let default_time = self.time_provider.now();

        let result = self
            .write_buffer
            .write_lp(
                database,
                body,
                default_time,
                params.accept_partial.unwrap_or(true),
                params.precision.unwrap_or(Precision::Auto),
                params.no_sync.unwrap_or(false),
            )
            .await?;

        let num_lines = result.line_count;
        let payload_size = body.len();
        self.common_state
            .telemetry_store
            .add_write_metrics(num_lines, payload_size);

        if result.invalid_lines.is_empty() {
            ResponseBuilder::new()
                .status(StatusCode::NO_CONTENT)
                .body(empty_response_body())
                .map_err(Into::into)
        } else {
            Err(Error::PartialLpWrite(result))
        }
    }

    pub(crate) async fn create_admin_token(&self, _req: Request) -> Result<Response, Error> {
        let catalog = self.write_buffer.catalog();
        let (token_info, token) = catalog.create_admin_token(false).await?;

        let response = CreateTokenWithPermissionsResponse::from_token_info(token_info, token);
        let body = serde_json::to_vec(&response)?;

        let body = ResponseBuilder::new()
            .status(StatusCode::CREATED)
            .header(CONTENT_TYPE, "json")
            .body(bytes_to_response_body(body));

        Ok(body?)
    }

    pub(crate) async fn create_named_admin_token(&self, req: Request) -> Result<Response, Error> {
        let token_request: CreateNamedAdminTokenRequest = self.read_body_json(req).await?;
        let catalog = self.write_buffer.catalog();
        let (token_info, token) = catalog
            .create_named_admin_token_with_permission(
                token_request.token_name,
                token_request.expiry_secs,
            )
            .await?;

        let response = CreateTokenWithPermissionsResponse::from_token_info(token_info, token);
        let body = serde_json::to_vec(&response)?;

        let body = ResponseBuilder::new()
            .status(StatusCode::CREATED)
            .header(CONTENT_TYPE, "json")
            .body(bytes_to_response_body(body));

        Ok(body?)
    }

    pub(crate) async fn regenerate_admin_token(&self, _req: Request) -> Result<Response, Error> {
        let catalog = self.write_buffer.catalog();
        let (token_info, token) = catalog.create_admin_token(true).await?;

        let response = CreateTokenWithPermissionsResponse::from_token_info(token_info, token);
        let body = serde_json::to_vec(&response)?;

        let body = ResponseBuilder::new()
            .status(StatusCode::CREATED)
            .header(CONTENT_TYPE, "json")
            .body(bytes_to_response_body(body));

        Ok(body?)
    }

    async fn query_sql(&self, req: Request) -> Result<Response> {
        let QueryRequest {
            database,
            query_str,
            format,
            params,
        } = self.extract_query_request::<String>(req).await?;

        info!(%database, %query_str, ?format, "handling query_sql");

        let span_ctx = Some(SpanContext::new_with_optional_collector(
            self.common_state.trace_collector(),
        ));

        let stream = self
            .query_executor
            .query_sql(&database, &query_str, params, span_ctx, None)
            .await?;

        ResponseBuilder::new()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(record_batch_stream_to_body(stream, format).await?)
            .map_err(Into::into)
    }

    async fn query_influxql(&self, req: Request) -> Result<Response> {
        let QueryRequest {
            database,
            query_str,
            format,
            params,
        } = self.extract_query_request::<Option<String>>(req).await?;

        info!(?database, %query_str, ?format, "handling query_influxql");
        let (stream, _) = self
            .query_influxql_inner(database, &query_str, params)
            .await?;

        ResponseBuilder::new()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(record_batch_stream_to_body(stream, format).await?)
            .map_err(Into::into)
    }

    fn health(&self) -> Result<Response> {
        let response_body = "OK";
        Ok(Response::new(bytes_to_response_body(
            response_body.to_string(),
        )))
    }

    fn ping(&self) -> Result<Response> {
        let process_uuid = ProcessUuidWrapper::new();
        let body = serde_json::to_string(&PingResponse {
            version: INFLUXDB3_VERSION.to_string(),
            revision: INFLUXDB3_GIT_HASH_SHORT.to_string(),
            process_id: *process_uuid.get(),
        })?;

        // InfluxDB 1.x used time-based UUIDs.
        let request_id = Uuid::now_v7().as_hyphenated().to_string();

        ResponseBuilder::new()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .header("Request-Id", request_id.clone())
            .header("X-Influxdb-Build", INFLUXDB3_BUILD.to_string())
            .header("X-Influxdb-Version", INFLUXDB3_VERSION.to_string())
            .header("X-Request-Id", request_id)
            .body(bytes_to_response_body(body))
            .map_err(Into::into)
    }

    fn handle_metrics(&self) -> Result<Response> {
        let mut body: Vec<u8> = Default::default();
        let mut reporter = metric_exporters::PrometheusTextEncoder::new(&mut body);
        self.common_state.metrics.report(&mut reporter);

        Ok(Response::new(bytes_to_response_body(body)))
    }

    /// Parse the request's body into raw bytes, applying the configured size
    /// limits and decoding any content encoding.
    async fn read_body(&self, req: Request) -> Result<Bytes> {
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
        while let Some(frame) = payload.frame().await {
            let frame = frame.map_err(Error::ClientHangup)?;
            if let Some(chunk) = frame.data_ref() {
                // limit max size of in-memory payload
                if (body.len() + chunk.len()) > self.max_request_bytes {
                    return Err(Error::RequestSizeExceeded(self.max_request_bytes));
                }
                body.extend_from_slice(chunk);
            }
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

    async fn authenticate_request(&self, req: &mut Request) -> Result<(), AuthenticationError> {
        // Extend the request with the authorization token; this is used downstream in some
        // APIs, such as write, that need the full header value to authorize a request.
        let auth_header = req.headers().get(AUTHORIZATION).cloned();
        req.extensions_mut()
            .insert(AuthorizationHeaderExtension::new(auth_header.clone()));

        let auth_token = if let Some(p) = extract_v1_auth_token(req) {
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
        let token_id = self
            .authorizer
            .authenticate(auth_token.clone())
            .await
            .map_err(|e| {
                error!(?e, "cannot authenticate token");
                AuthenticationError::Unauthenticated
            })?;

        // Extend the request with the token, which can be looked up later in authorization
        req.extensions_mut().insert(token_id);

        Ok(())
    }

    async fn extract_query_request<D: DeserializeOwned>(
        &self,
        req: Request,
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
    ) -> Result<(SendableRecordBatchStream, Option<GroupByClause>)> {
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

        let statement = statement.to_statement();
        let group_by = match &statement {
            Statement::Select(select_statement) => select_statement.group_by.clone(),
            _ => None,
        };

        let span_ctx = Some(SpanContext::new_with_optional_collector(
            self.common_state.trace_collector(),
        ));

        let stream = if statement.is_show_databases() {
            self.query_executor.show_databases(true)?
        } else if statement.is_show_retention_policies() {
            self.query_executor
                .show_retention_policies(database.as_deref(), None)
                .await?
        } else {
            let Some(database) = database else {
                return Err(Error::InfluxqlNoDatabase);
            };

            self.query_executor
                .query_influxql(&database, query_str, statement, params, span_ctx, None)
                .await?
        };

        Ok((stream, group_by))
    }

    /// Create a new distinct value cache given the [`DistinctCacheCreateRequest`] arguments in the request
    /// body.
    ///
    /// If the result is to create a cache that already exists, with the same configuration, this
    /// will respond with a 204 NOT CREATED. If an existing cache would be overwritten with a
    /// different configuration, that is a 400 BAD REQUEST
    async fn configure_distinct_cache_create(&self, req: Request) -> Result<Response> {
        let args = self.read_body_json(req).await?;
        info!(?args, "create distinct value cache request");
        let DistinctCacheCreateRequest {
            db,
            table,
            name,
            columns,
            max_cardinality,
            max_age,
        } = args;
        match self
            .write_buffer
            .catalog()
            .create_distinct_cache(
                &db,
                &table,
                name.as_deref(),
                &columns,
                max_cardinality,
                max_age,
            )
            .await
        {
            Ok(batch) => ResponseBuilder::new()
                .status(StatusCode::CREATED)
                .body(bytes_to_response_body(serde_json::to_vec(&batch)?))
                .map_err(Into::into),
            Err(error) => Err(error.into()),
        }
    }

    /// Delete a distinct value cache entry with the given [`DistinctCacheDeleteRequest`] parameters
    ///
    /// The parameters must be passed in either the query string or the body of the request as JSON.
    async fn configure_distinct_cache_delete(&self, req: Request) -> Result<Response> {
        let DistinctCacheDeleteRequest { db, table, name } = if let Some(query) = req.uri().query()
        {
            serde_urlencoded::from_str(query)?
        } else {
            self.read_body_json(req).await?
        };

        self.write_buffer
            .catalog()
            .delete_distinct_cache(&db, &table, &name)
            .await?;

        ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())
            .map_err(Into::into)
    }

    async fn configure_last_cache_create(&self, req: Request) -> Result<Response> {
        let LastCacheCreateRequest {
            db,
            table,
            name,
            key_columns,
            value_columns,
            count,
            ttl,
        } = self.read_body_json(req).await?;
        match self
            .write_buffer
            .catalog()
            .create_last_cache(
                &db,
                &table,
                name.as_deref(),
                key_columns.as_deref(),
                value_columns.as_deref(),
                count,
                ttl,
            )
            .await
        {
            Ok(batch) => ResponseBuilder::new()
                .status(StatusCode::CREATED)
                .body(bytes_to_response_body(serde_json::to_vec(&batch)?))
                .map_err(Into::into),
            Err(error) => Err(error.into()),
        }
    }

    /// Delete a last cache entry with the given [`LastCacheDeleteRequest`] parameters
    ///
    /// This will first attempt to parse the parameters from the URI query string, if a query string
    /// is provided, but if not, will attempt to parse them from the request body as JSON.
    async fn configure_last_cache_delete(&self, req: Request) -> Result<Response> {
        let LastCacheDeleteRequest { db, table, name } = if let Some(query) = req.uri().query() {
            serde_urlencoded::from_str(query)?
        } else {
            self.read_body_json(req).await?
        };

        self.write_buffer
            .catalog()
            .delete_last_cache(&db, &table, &name)
            .await?;

        ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())
            .map_err(Into::into)
    }

    async fn configure_processing_engine_trigger(&self, req: Request) -> Result<Response> {
        let ProcessingEngineTriggerCreateRequest {
            db,
            plugin_filename,
            trigger_name,
            trigger_settings,
            trigger_specification,
            trigger_arguments,
            disabled,
        } = if let Some(query) = req.uri().query() {
            serde_urlencoded::from_str(query)?
        } else {
            self.read_body_json(req).await?
        };
        debug!(%db, %plugin_filename, %trigger_name, %trigger_specification, %disabled, "configure_processing_engine_trigger");
        let plugin_filename = self
            .processing_engine
            .validate_plugin_filename(&plugin_filename)
            .await?;
        self.write_buffer
            .catalog()
            .create_processing_engine_trigger(
                &db,
                &trigger_name,
                self.processing_engine.node_id(),
                plugin_filename,
                &trigger_specification,
                trigger_settings,
                &trigger_arguments,
                disabled,
            )
            .await?;
        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())?)
    }

    async fn delete_processing_engine_trigger(&self, req: Request) -> Result<Response> {
        let ProcessingEngineTriggerDeleteRequest {
            db,
            trigger_name,
            force,
        } = if let Some(query) = req.uri().query() {
            serde_urlencoded::from_str(query)?
        } else {
            self.read_body_json(req).await?
        };
        self.write_buffer
            .catalog()
            .delete_processing_engine_trigger(&db, &trigger_name, force)
            .await?;
        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())?)
    }

    async fn disable_processing_engine_trigger(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().unwrap_or("");
        let ProcessingEngineTriggerIdentifier { db, trigger_name } =
            serde_urlencoded::from_str(query)?;
        match self
            .write_buffer
            .catalog()
            .disable_processing_engine_trigger(&db, &trigger_name)
            .await
        {
            Ok(_) | Err(CatalogError::TriggerAlreadyDisabled) => Ok(ResponseBuilder::new()
                .status(StatusCode::OK)
                .body(empty_response_body())?),
            Err(error) => Err(error.into()),
        }
    }

    async fn enable_processing_engine_trigger(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().unwrap_or("");
        let ProcessingEngineTriggerIdentifier { db, trigger_name } =
            serde_urlencoded::from_str(query)?;
        match self
            .write_buffer
            .catalog()
            .enable_processing_engine_trigger(&db, &trigger_name)
            .await
        {
            Ok(_) | Err(CatalogError::TriggerAlreadyEnabled) => Ok(ResponseBuilder::new()
                .status(StatusCode::OK)
                .body(empty_response_body())?),
            Err(error) => Err(error.into()),
        }
    }

    async fn install_plugin_environment_packages(&self, req: Request) -> Result<Response> {
        let ProcessingEngineInstallPackagesRequest { packages } =
            if let Some(query) = req.uri().query() {
                serde_urlencoded::from_str(query)?
            } else {
                self.read_body_json(req).await?
            };
        let manager = self.processing_engine.get_environment_manager();
        manager
            .install_packages(packages)
            .map_err(ProcessingEngineError::from)?;

        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())?)
    }

    async fn install_plugin_environment_requirements(&self, req: Request) -> Result<Response> {
        let ProcessingEngineInstallRequirementsRequest {
            requirements_location,
        } = if let Some(query) = req.uri().query() {
            serde_urlencoded::from_str(query)?
        } else {
            self.read_body_json(req).await?
        };
        info!(
            "installing plugin environment requirements from {}",
            requirements_location
        );
        let manager = self.processing_engine.get_environment_manager();
        manager
            .install_requirements(requirements_location)
            .map_err(ProcessingEngineError::from)?;

        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())?)
    }

    async fn show_databases(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().unwrap_or("");
        let ShowDatabasesRequest {
            format,
            show_deleted,
        } = serde_urlencoded::from_str(query)?;
        let stream = self.query_executor.show_databases(show_deleted)?;
        ResponseBuilder::new()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, format.as_content_type())
            .body(record_batch_stream_to_body(stream, format).await?)
            .map_err(Into::into)
    }

    async fn create_database(&self, req: Request) -> Result<Response> {
        let CreateDatabaseRequest {
            db,
            retention_period,
        } = self.read_body_json(req).await?;
        validate_db_name(&db, false)?;
        self.write_buffer
            .catalog()
            .create_database_opts(
                &db,
                influxdb3_catalog::catalog::CreateDatabaseOptions { retention_period },
            )
            .await?;
        Ok(Response::new(empty_response_body()))
    }

    /// Endpoint for testing a plugin that will be trigger on WAL writes.
    async fn test_processing_engine_wal_plugin(&self, req: Request) -> Result<Response> {
        let request: influxdb3_types::http::WalPluginTestRequest = self.read_body_json(req).await?;

        let output = self
            .processing_engine
            .test_wal_plugin(request, Arc::clone(&self.query_executor))
            .await?;
        let body = serde_json::to_string(&output)?;

        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(bytes_to_response_body(body))?)
    }

    async fn test_processing_engine_schedule_plugin(&self, req: Request) -> Result<Response> {
        let request: influxdb3_types::http::SchedulePluginTestRequest =
            self.read_body_json(req).await?;

        let output = self
            .processing_engine
            .test_schedule_plugin(request, Arc::clone(&self.query_executor))
            .await?;
        let body = serde_json::to_string(&output)?;

        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(bytes_to_response_body(body))?)
    }

    async fn processing_engine_request_plugin(
        &self,
        trigger_path: &str,
        req: Request,
    ) -> Result<Response> {
        use hashbrown::HashMap;

        // pull out the query params into a hashmap
        let uri = req.uri();
        let query_str = uri.query().unwrap_or("");

        let parsed_url = url::Url::parse(&format!("http://influxdata.com?{query_str}")).unwrap();
        let params: HashMap<String, String> = parsed_url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        // pull out the request headers into a hashmap
        let headers = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect();

        // pull out the request body
        let body = self.read_body(req).await?;

        match self
            .processing_engine
            .request_trigger(trigger_path, params, headers, body)
            .await
        {
            Ok(response) => Ok(response),
            Err(
                influxdb3_processing_engine::manager::ProcessingEngineError::RequestTriggerNotFound,
            ) => {
                let body = "{error: \"not found\"}";
                Ok(ResponseBuilder::new()
                    .status(StatusCode::NOT_FOUND)
                    .body(bytes_to_response_body(body))?)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn update_database(&self, req: Request) -> Result<Response> {
        let update_req = self.read_body_json::<UpdateDatabaseRequest>(req).await?;

        match update_req.retention_period {
            Some(duration) => {
                self.write_buffer
                    .catalog()
                    .set_retention_period_for_database(&update_req.db, duration)
                    .await?;
            }
            None => {
                self.write_buffer
                    .catalog()
                    .clear_retention_period_for_database(&update_req.db)
                    .await?;
            }
        }

        Ok(Response::new(empty_response_body()))
    }

    async fn delete_database(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().unwrap_or("");
        let delete_req = serde_urlencoded::from_str::<DeleteDatabaseRequest>(query)?;

        let hard_delete_time = match delete_req.hard_delete_at.unwrap_or_default() {
            influxdb3_types::http::HardDeletionTime::Never => HardDeletionTime::Never,
            influxdb3_types::http::HardDeletionTime::Now => HardDeletionTime::Now,
            influxdb3_types::http::HardDeletionTime::Default => HardDeletionTime::Default,
            influxdb3_types::http::HardDeletionTime::Timestamp(ts) => {
                let time = Time::from_datetime(DateTime::parse_from_rfc3339(&ts)?.to_utc());
                if time < self.time_provider.now() {
                    HardDeletionTime::Now
                } else {
                    HardDeletionTime::Timestamp(time)
                }
            }
        };

        self.write_buffer
            .catalog()
            .soft_delete_database(&delete_req.db, hard_delete_time)
            .await?;
        Ok(Response::new(empty_response_body()))
    }

    async fn create_table(&self, req: Request) -> Result<Response> {
        let CreateTableRequest {
            db,
            table,
            tags,
            fields,
        } = self.read_body_json(req).await?;
        validate_db_name(&db, false)?;
        self.write_buffer
            .catalog()
            .create_table(
                &db,
                &table,
                &tags,
                &fields
                    .into_iter()
                    .map(|field| (field.name, field.r#type.into()))
                    .collect::<Vec<(String, FieldDataType)>>(),
            )
            .await?;
        Ok(Response::new(empty_response_body()))
    }

    async fn delete_table(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().unwrap_or("");
        let delete_req = serde_urlencoded::from_str::<DeleteTableRequest>(query)?;

        let hard_delete_time = match delete_req.hard_delete_at.unwrap_or_default() {
            influxdb3_types::http::HardDeletionTime::Never => HardDeletionTime::Never,
            influxdb3_types::http::HardDeletionTime::Now => HardDeletionTime::Now,
            influxdb3_types::http::HardDeletionTime::Default => HardDeletionTime::Default,
            influxdb3_types::http::HardDeletionTime::Timestamp(ts) => {
                let time = Time::from_datetime(DateTime::parse_from_rfc3339(&ts)?.to_utc());
                if time < self.time_provider.now() {
                    HardDeletionTime::Now
                } else {
                    HardDeletionTime::Timestamp(time)
                }
            }
        };

        self.write_buffer
            .catalog()
            .soft_delete_table(&delete_req.db, &delete_req.table, hard_delete_time)
            .await?;
        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())
            .unwrap())
    }

    async fn delete_token(&self, req: Request) -> Result<Response> {
        let query = req.uri().query().unwrap_or("");
        let delete_req = serde_urlencoded::from_str::<TokenDeleteRequest>(query)?;
        self.write_buffer
            .catalog()
            .delete_token(&delete_req.token_name)
            .await?;
        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(empty_response_body())
            .unwrap())
    }

    async fn read_body_json<ReqBody: DeserializeOwned>(&self, req: Request) -> Result<ReqBody> {
        if !json_content_type(req.headers()) {
            return Err(Error::InvalidContentType {
                expected: mime::APPLICATION_JSON,
            });
        }
        let bytes = self.read_body(req).await?;
        serde_json::from_slice(&bytes).map_err(Into::into)
    }

    pub(crate) async fn set_retention_period_for_database(
        &self,
        req: Request,
    ) -> Result<Response, Error> {
        #[derive(Deserialize)]
        struct SetRetentionPeriod {
            db: String,
            duration: String,
        }

        let query = req.uri().query().unwrap_or("");
        let create_req = serde_urlencoded::from_str::<SetRetentionPeriod>(query)?;
        let catalog = self.write_buffer.catalog();

        let duration: Duration = create_req
            .duration
            .parse::<humantime::Duration>()
            .map_err(Error::ParsingHumanTime)?
            .into();

        catalog
            .set_retention_period_for_database(create_req.db.as_str(), duration)
            .await?;

        let body = ResponseBuilder::new()
            .status(StatusCode::NO_CONTENT)
            .body(empty_response_body());

        Ok(body?)
    }

    pub(crate) async fn clear_retention_period_for_database(
        &self,
        req: Request,
    ) -> Result<Response, Error> {
        #[derive(Deserialize)]
        struct ClearRetentionPeriod {
            db: String,
        }

        let query = req.uri().query().unwrap_or("");
        let catalog = self.write_buffer.catalog();
        let delete_req = serde_urlencoded::from_str::<ClearRetentionPeriod>(query)?;

        catalog
            .clear_retention_period_for_database(delete_req.db.as_str())
            .await?;

        let body = ResponseBuilder::new()
            .status(StatusCode::NO_CONTENT)
            .body(empty_response_body());

        Ok(body?)
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

    mime.type_() == "application"
        && (mime.subtype() == "json" || mime.suffix().is_some_and(|name| name == "json"))
}

#[derive(Debug, Deserialize)]
struct V1AuthParameters {
    #[serde(rename = "p")]
    password: Option<String>,
}

/// Extract the authentication token for v1 API requests, which may use the `p` query
/// parameter to pass the authentication token.
fn extract_v1_auth_token(req: &mut Request) -> Option<Vec<u8>> {
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

fn validate_auth_header(header: HeaderValue) -> Result<Vec<u8>, AuthenticationError> {
    // Split the header value into two parts
    let mut header = header.to_str()?.split(' ');

    // Check that the header is the 'Bearer' or 'Token' auth scheme
    let auth_scheme = header.next().ok_or(AuthenticationError::MalformedRequest)?;
    if auth_scheme != "Bearer" && auth_scheme != "Token" && auth_scheme != "Basic" {
        return Err(AuthenticationError::MalformedRequest);
    }

    // Get the token that we want to hash to check the request is valid
    let token = header.next().ok_or(AuthenticationError::MalformedRequest)?;

    // There should only be two parts the 'Bearer' scheme and the actual
    // token, error otherwise
    if header.next().is_some() {
        return Err(AuthenticationError::MalformedRequest);
    }

    let token = if auth_scheme == "Basic" {
        token_part_as_bytes(token)?
    } else {
        token.as_bytes().to_vec()
    };

    Ok(token)
}

fn token_part_as_bytes(token: &str) -> Result<Vec<u8>, AuthenticationError> {
    let decoded = B64_STANDARD.decode(token).map_err(|err| {
        error!(?err, "cannot decode basic auth token");
        AuthenticationError::MalformedRequest
    })?;

    let token_parts_string = String::from_utf8(decoded).map_err(|err| {
        error!(?err, "cannot decode basic auth token to string");
        AuthenticationError::MalformedRequest
    })?;

    let token_parts = token_parts_string.rsplit_once(":");
    let (username, token_part) = token_parts.ok_or_else(|| {
        error!("cannot find token part in decoded basic auth token");
        AuthenticationError::MalformedRequest
    })?;

    if username.contains(":") {
        error!("token username/password contains ':' which is not allowed");
        return Err(AuthenticationError::MalformedRequest);
    }

    Ok(token_part.as_bytes().to_vec())
}

impl From<authz::Error> for AuthenticationError {
    fn from(auth_error: authz::Error) -> Self {
        match auth_error {
            authz::Error::Forbidden { .. } => Self::Forbidden,
            _ => Self::Unauthenticated,
        }
    }
}

/// Validate a database name
///
/// A valid name:
/// - Starts with a letter or a number
/// - Is ASCII not UTF-8
/// - Contains only letters, numbers, underscores or hyphens
/// - if `accept_rp` is true, then a single slash ('/') is allowed, separating
///   the database name from the retention policy name, e.g., '<db_name>/<rp_name>'
fn validate_db_name(name: &str, accept_rp: bool) -> Result<(), ValidateDbNameError> {
    if name.is_empty() {
        return Err(ValidateDbNameError::Empty);
    }
    let mut is_first_char = true;
    let mut rp_seperator_found = false;
    let mut last_char = None;
    for grapheme in name.graphemes(true) {
        if grapheme.len() > 1 {
            // In the case of a unicode we need to handle multibyte chars
            return Err(ValidateDbNameError::InvalidChar);
        }
        let char = grapheme.as_bytes()[0] as char;
        if !is_first_char {
            match (accept_rp, rp_seperator_found, char) {
                (true, true, V1_NAMESPACE_RP_SEPARATOR) => {
                    return Err(ValidateDbNameError::InvalidRetentionPolicy);
                }
                (true, false, V1_NAMESPACE_RP_SEPARATOR) => {
                    rp_seperator_found = true;
                }
                (false, _, char)
                    if !(char.is_ascii_alphanumeric() || char == '_' || char == '-') =>
                {
                    return Err(ValidateDbNameError::InvalidChar);
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

#[derive(Clone, Copy, Debug, thiserror::Error)]
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

async fn record_batch_stream_to_body(
    mut stream: Pin<Box<dyn RecordBatchStream + Send>>,
    format: QueryFormat,
) -> Result<ResponseBody, Error> {
    match format {
        QueryFormat::Pretty => {
            let batches = stream.try_collect::<Vec<RecordBatch>>().await?;
            Ok(bytes_to_response_body(Bytes::from(format!(
                "{}",
                pretty::pretty_format_batches(&batches)?
            ))))
        }
        QueryFormat::Parquet => {
            // Grab the first batch so that we can get the schema
            let Some(batch) = stream.next().await.transpose()? else {
                return Ok(empty_response_body());
            };
            let schema = batch.schema();

            let mut bytes = Vec::new();
            let mem_pool = Arc::new(UnboundedMemoryPool::default());
            let mut writer = TrackedMemoryArrowWriter::try_new(&mut bytes, schema, mem_pool)?;

            // Write the first batch we got and then continue writing batches
            writer.write(batch)?;
            while let Some(batch) = stream.next().await.transpose()? {
                writer.write(batch)?;
            }
            writer.close()?;
            Ok(bytes_to_response_body(Bytes::from(bytes)))
        }
        QueryFormat::Csv => {
            struct CsvFuture {
                first_poll: bool,
                stream: Pin<Box<dyn RecordBatchStream + Send>>,
            }

            impl Future for CsvFuture {
                type Output = Option<Result<Bytes, DataFusionError>>;

                fn poll(
                    mut self: Pin<&mut Self>,
                    ctx: &mut std::task::Context<'_>,
                ) -> Poll<Self::Output> {
                    match self.stream.poll_next_unpin(ctx) {
                        Poll::Ready(Some(batch)) => {
                            let batch = match batch {
                                Ok(batch) => batch,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            let mut writer = if self.first_poll {
                                self.first_poll = false;
                                arrow_csv::Writer::new(Vec::new())
                            } else {
                                arrow_csv::WriterBuilder::new()
                                    .with_header(false)
                                    .build(Vec::new())
                            };
                            if let Err(err) = writer.write(&batch) {
                                Poll::Ready(Some(Err(err.into())))
                            } else {
                                Poll::Ready(Some(Ok(Bytes::from(writer.into_inner()))))
                            }
                        }
                        Poll::Ready(None) => Poll::Ready(None),
                        Poll::Pending => Poll::Pending,
                    }
                }
            }

            let mut future = CsvFuture {
                first_poll: true,
                stream,
            };
            Ok(stream_results_to_response_body(futures::stream::poll_fn(
                move |ctx| future.poll_unpin(ctx),
            )))
        }
        QueryFormat::Json => {
            struct JsonFuture {
                state: State,
                stream: Pin<Box<dyn RecordBatchStream + Send>>,
            }

            enum State {
                FirstPoll,
                Body,
                Done,
            }

            #[derive(Default, Debug)]
            struct JsonMiddle;

            impl arrow_json::writer::JsonFormat for JsonMiddle {
                fn start_row<W: std::io::Write>(
                    &self,
                    writer: &mut W,
                    _is_first_row: bool,
                ) -> std::result::Result<(), arrow_schema::ArrowError> {
                    writer.write_all(b",")?;

                    Ok(())
                }
            }

            impl Future for JsonFuture {
                type Output = Option<Result<Bytes, DataFusionError>>;

                fn poll(
                    mut self: Pin<&mut Self>,
                    ctx: &mut std::task::Context<'_>,
                ) -> Poll<Self::Output> {
                    match self.stream.poll_next_unpin(ctx) {
                        Poll::Ready(Some(batch)) => {
                            let batch = match batch {
                                Ok(batch) => batch,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            match self.state {
                                State::FirstPoll => {
                                    let mut writer = arrow_json::ArrayWriter::new(Vec::new());

                                    // If we have nothing in this batch just skip it.
                                    // The initial '[' will be added either on the first non-empty batch or as '[]' at the end.
                                    if batch.num_rows() == 0 {
                                        Poll::Ready(Some(Ok(Bytes::new())))
                                    } else if let Err(err) = writer.write(&batch) {
                                        Poll::Ready(Some(Err(err.into())))
                                    } else {
                                        self.state = State::Body;
                                        Poll::Ready(Some(Ok(Bytes::from(writer.into_inner()))))
                                    }
                                }
                                State::Body => {
                                    let mut writer = arrow_json::WriterBuilder::new()
                                        .build::<Vec<_>, JsonMiddle>(Vec::new());
                                    if let Err(err) = writer.write(&batch) {
                                        Poll::Ready(Some(Err(err.into())))
                                    } else {
                                        Poll::Ready(Some(Ok(Bytes::from(writer.into_inner()))))
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                        Poll::Ready(None) => {
                            match self.state {
                                // We've never written anything so just close the
                                // stream with an empty array
                                State::FirstPoll => {
                                    self.state = State::Done;
                                    Poll::Ready(Some(Ok(Bytes::from("[]"))))
                                }
                                // We have written to this before so we should
                                // write the final bytes to close the object
                                State::Body => {
                                    self.state = State::Done;
                                    Poll::Ready(Some(Ok(Bytes::from("]"))))
                                }
                                State::Done => Poll::Ready(None),
                            }
                        }
                        Poll::Pending => Poll::Pending,
                    }
                }
            }

            let mut future = JsonFuture {
                state: State::FirstPoll,
                stream,
            };
            Ok(stream_results_to_response_body(futures::stream::poll_fn(
                move |ctx| future.poll_unpin(ctx),
            )))
        }
        QueryFormat::JsonLines => {
            let stream = futures::stream::poll_fn(move |ctx| match stream.poll_next_unpin(ctx) {
                Poll::Ready(Some(batch)) => {
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    let mut writer = arrow_json::LineDelimitedWriter::new(Vec::new());
                    if let Err(err) = writer.write(&batch) {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    if let Err(err) = writer.finish() {
                        Poll::Ready(Some(Err(err.into())))
                    } else {
                        Poll::Ready(Some(Ok(Bytes::from(writer.into_inner()))))
                    }
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            });
            Ok(stream_results_to_response_body(stream))
        }
    }
}

/// This is used to trigger a shutdown when it's dropped.
#[derive(Clone)]
struct ShutdownTrigger {
    token: tokio_util::sync::CancellationToken,
}

impl ShutdownTrigger {
    fn new(token: tokio_util::sync::CancellationToken) -> Self {
        Self { token }
    }
}

impl Drop for ShutdownTrigger {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

pub(crate) async fn route_admin_token_recovery_request(
    recovery_api: Arc<RecoveryHttpApi>,
    req: hyper::Request<hyper::body::Incoming>,
) -> Result<Response, Infallible> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    trace!(request = ?req,"Processing request");
    let content_length = req.headers().get("content-length").cloned();

    // Convert incoming request to iox_http_util request
    let (parts, body) = req.into_parts();
    let collected = match http_body_util::BodyExt::collect(body).await {
        Ok(collected) => collected,
        Err(e) => {
            error!("Failed to collect request body: {}", e);
            return Ok(ResponseBuilder::new()
                .status(StatusCode::BAD_REQUEST)
                .body(bytes_to_response_body("Failed to read request body"))
                .unwrap());
        }
    };
    let bytes = collected.to_bytes();
    let iox_body = iox_http_util::bytes_to_request_body(bytes);
    let req = iox_http_util::Request::from_parts(parts, iox_body);

    let response = match (method.clone(), uri.path()) {
        (Method::POST, all_paths::API_V3_CONFIGURE_ADMIN_TOKEN_REGENERATE) => {
            info!("Regenerating admin token without password through token recovery API request");
            let result = recovery_api.http_api.regenerate_admin_token(req).await;

            // If token regeneration was successful, trigger shutdown of the recovery endpoint
            if let Ok(response) = result {
                info!("Admin token regenerated successfully, shutting down recovery endpoint");
                let cancellation_token = recovery_api.cancellation_token.clone();
                let mut res_builder = ResponseBuilder::new();
                let extensions = res_builder.extensions_mut().unwrap();
                let shutdown_trigger = ShutdownTrigger::new(cancellation_token);
                extensions.insert(shutdown_trigger);

                Ok(res_builder
                    .status(response.status())
                    .body(response.into_body())
                    .unwrap())
            } else {
                result
            }
        }
        _ => {
            let body = bytes_to_response_body("not found");
            Ok(ResponseBuilder::new()
                .status(StatusCode::NOT_FOUND)
                .body(body)
                .unwrap())
        }
    };

    match response {
        Ok(mut response) => {
            response
                .headers_mut()
                .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            debug!(?response, "Successfully processed request");
            Ok(response)
        }
        Err(error) => {
            error!(%error, %method, path = uri.path(), ?content_length, "Error while handling request");
            Ok(error.into_response())
        }
    }
}

pub(crate) async fn route_request(
    http_server: Arc<HttpApi>,
    mut req: Request,
    started_without_auth: bool,
    paths_without_authz: &'static Vec<&'static str>,
) -> Result<Response, Infallible> {
    let method = req.method().clone();
    let uri = req.uri().clone();

    // Handle CORS Preflight Checks by allowing everything by default
    // and allowing the check to be cached by the browser. This is useful
    // for people wanting to query the DB directly from a browser rather
    // than from a server. We're permissive about what works with CORS
    // so we don't need to check the incoming request, just respond with
    // the following headers. We do this before the API token checks as
    // the browser will not send a request with an auth header for CORS.
    if let Method::OPTIONS = method {
        info!(?uri, "preflight request");
        return Ok(ResponseBuilder::new()
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "*")
            .header("Access-Control-Allow-Headers", "*")
            .header("Access-Control-Max-Age", "86400")
            .status(204)
            .body(empty_response_body())
            .expect("Able to always create a valid response type for CORS"));
    }

    if started_without_auth && uri.path().starts_with(all_paths::API_V3_CONFIGURE_TOKEN) {
        return Ok(ResponseBuilder::new()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(bytes_to_response_body(Bytes::from(
                "endpoint disabled, started without auth",
            )))
            .unwrap());
    }

    let path = uri.path();
    // admin token creation should be allowed without authentication
    // and any endpoints that are disabled
    if path == all_paths::API_V3_CONFIGURE_ADMIN_TOKEN || paths_without_authz.contains(&path) {
        trace!(?uri, "not authenticating request");
    } else {
        trace!(?uri, "authenticating request");
        if let Some(authentication_error) = authenticate(&http_server, &mut req).await {
            return authentication_error;
        }
    }

    trace!(request = ?req,"Processing request");
    let content_length = req.headers().get("content-length").cloned();
    // Extract database name from query parameters before req is consumed
    // This is ok for now as write endpoint expects db in query param
    let db = extract_db_from_query_param(&uri);

    // Extract client IP from headers (check common reverse proxy headers first)
    let client_ip = extract_client_ip(&req);

    let response = match (method.clone(), path) {
        (Method::DELETE, all_paths::API_V3_CONFIGURE_TOKEN) => http_server.delete_token(req).await,
        (Method::POST, all_paths::API_V3_CONFIGURE_ADMIN_TOKEN) => {
            http_server.create_admin_token(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_ADMIN_TOKEN_REGENERATE) => {
            http_server.regenerate_admin_token(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_NAMED_ADMIN_TOKEN) => {
            http_server.create_named_admin_token(req).await
        }
        (Method::POST, all_paths::API_LEGACY_WRITE) => {
            let params = match http_server.legacy_write_param_unifier.parse_v1(&req).await {
                Ok(p) => p.into(),
                Err(e) => return Ok(legacy_write_error_to_response(e)),
            };

            http_server.write_lp_inner(params, req, true).await
        }
        (Method::POST, all_paths::API_V2_WRITE) => {
            let params = match http_server
                .legacy_write_param_unifier
                .parse_v2(&req)
                .await
                .map_err(Error::LegacyWriteParse)
                .map_err(V2WriteApiError)
            {
                Ok(p) => p.into(),
                Err(e) => return Ok(e.into_response()),
            };
            match http_server
                .write_lp_inner(params, req, false)
                .await
                .map_err(V2WriteApiError)
            {
                Ok(r) => Ok(r),
                Err(e) => return Ok(e.into_response()),
            }
        }
        (Method::POST, all_paths::API_V3_WRITE) => http_server.write_lp(req).await,
        (Method::GET | Method::POST, all_paths::API_V3_QUERY_SQL) => {
            http_server.query_sql(req).await
        }
        (Method::GET | Method::POST, all_paths::API_V3_QUERY_INFLUXQL) => {
            http_server.query_influxql(req).await
        }
        (Method::GET | Method::POST, all_paths::API_V1_QUERY) => http_server.v1_query(req).await,
        (Method::GET, all_paths::API_V3_HEALTH | all_paths::API_V1_HEALTH) => http_server.health(),
        (Method::GET | Method::POST, all_paths::API_PING) => http_server.ping(),
        (Method::GET, all_paths::API_METRICS) => http_server.handle_metrics(),
        (Method::GET | Method::POST, path) if path.starts_with(all_paths::API_V3_ENGINE) => {
            let path = path.strip_prefix(all_paths::API_V3_ENGINE).unwrap();
            http_server
                .processing_engine_request_plugin(path, req)
                .await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_DISTINCT_CACHE) => {
            http_server.configure_distinct_cache_create(req).await
        }
        (Method::DELETE, all_paths::API_V3_CONFIGURE_DISTINCT_CACHE) => {
            http_server.configure_distinct_cache_delete(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_LAST_CACHE) => {
            http_server.configure_last_cache_create(req).await
        }
        (Method::DELETE, all_paths::API_V3_CONFIGURE_LAST_CACHE) => {
            http_server.configure_last_cache_delete(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_DISABLE) => {
            http_server.disable_processing_engine_trigger(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_ENABLE) => {
            http_server.enable_processing_engine_trigger(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_TRIGGER) => {
            http_server.configure_processing_engine_trigger(req).await
        }
        (Method::DELETE, all_paths::API_V3_CONFIGURE_PROCESSING_ENGINE_TRIGGER) => {
            http_server.delete_processing_engine_trigger(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_PLUGIN_INSTALL_PACKAGES) => {
            http_server.install_plugin_environment_packages(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_PLUGIN_INSTALL_REQUIREMENTS) => {
            http_server
                .install_plugin_environment_requirements(req)
                .await
        }
        (Method::GET, all_paths::API_V3_CONFIGURE_DATABASE) => {
            http_server.show_databases(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_DATABASE) => {
            http_server.create_database(req).await
        }
        (Method::PUT, all_paths::API_V3_CONFIGURE_DATABASE) => {
            http_server.update_database(req).await
        }
        (Method::DELETE, all_paths::API_V3_CONFIGURE_DATABASE) => {
            http_server.delete_database(req).await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_TABLE) => http_server.create_table(req).await,
        (Method::DELETE, all_paths::API_V3_CONFIGURE_TABLE) => http_server.delete_table(req).await,
        (Method::POST, all_paths::API_V3_TEST_WAL_ROUTE) => {
            http_server.test_processing_engine_wal_plugin(req).await
        }
        (Method::POST, all_paths::API_V3_TEST_PLUGIN_ROUTE) => {
            http_server
                .test_processing_engine_schedule_plugin(req)
                .await
        }
        (Method::POST, all_paths::API_V3_CONFIGURE_DATABASE_RETENTION_PERIOD) => {
            http_server.set_retention_period_for_database(req).await
        }

        (Method::DELETE, all_paths::API_V3_CONFIGURE_DATABASE_RETENTION_PERIOD) => {
            http_server.clear_retention_period_for_database(req).await
        }
        _ => {
            let body = bytes_to_response_body("not found");
            Ok(ResponseBuilder::new()
                .status(StatusCode::NOT_FOUND)
                .body(body)
                .unwrap())
        }
    };

    // TODO: Move logging to TraceLayer
    match response {
        Ok(mut response) => {
            response
                .headers_mut()
                .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            debug!(?response, "Successfully processed request");
            Ok(response)
        }
        Err(error) => {
            let ip = client_ip.as_deref().unwrap_or(UNKNOWN_VAL);
            match db.as_ref() {
                Some(db) => {
                    error!(%error, %method, path = uri.path(), ?content_length, database = %db, client_ip = %ip, "Error while handling request")
                }
                None => {
                    error!(%error, %method, path = uri.path(), ?content_length, client_ip = %ip, "Error while handling request")
                }
            }
            Ok(error.into_response())
        }
    }
}

fn extract_client_ip<T>(req: &http::Request<T>) -> Option<String> {
    req.headers()
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next()) // Take first IP if multiple
        .map(|s| s.trim().to_string())
        .or_else(|| {
            req.headers()
                .get("x-real-ip")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
        })
        .or_else(|| {
            // Fall back to socket address from request extensions
            req.extensions()
                .get::<Option<std::net::SocketAddr>>()
                .map(|socket_addr| {
                    socket_addr
                        .map(|addr| addr.ip().to_string())
                        .unwrap_or_else(|| UNKNOWN_VAL.to_string())
                })
        })
}

fn extract_db_from_query_param(uri: &http::Uri) -> Option<String> {
    uri.query()
        .and_then(|query| serde_urlencoded::from_str::<Vec<(String, String)>>(query).ok())
        .and_then(|params| params.into_iter().find(|(k, _)| k == "db"))
        .map(|(_, v)| v)
}

async fn authenticate(
    http_server: &Arc<HttpApi>,
    req: &mut Request,
) -> Option<std::result::Result<Response, Infallible>> {
    if let Err(e) = http_server.authenticate_request(req).await {
        match e {
            AuthenticationError::Unauthenticated => {
                return Some(Ok(ResponseBuilder::new()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(empty_response_body())
                    .unwrap()));
            }
            AuthenticationError::MalformedRequest => {
                return Some(Ok(ResponseBuilder::new()
                    .status(StatusCode::BAD_REQUEST)
                    .body(bytes_to_response_body(format!(r#"{{"error": "{e}"}}"#)))
                    .unwrap()));
            }
            AuthenticationError::Forbidden => {
                return Some(Ok(ResponseBuilder::new()
                    .status(StatusCode::FORBIDDEN)
                    .body(empty_response_body())
                    .unwrap()));
            }
            // We don't expect this to happen, but if the header is messed up
            // better to handle it then not at all
            AuthenticationError::ToStr(_) => {
                return Some(Ok(ResponseBuilder::new()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(empty_response_body())
                    .unwrap()));
            }
        }
    }
    None
}

fn legacy_write_error_to_response(e: WriteParseError) -> Response {
    let err: ErrorMessage<()> = ErrorMessage {
        error: e.to_string(),
        data: None,
    };
    let serialized = serde_json::to_string(&err).unwrap();
    let body = bytes_to_response_body(serialized);
    let status = match e {
        WriteParseError::NotImplemented => StatusCode::NOT_FOUND,
        WriteParseError::SingleTenantError(e) => StatusCode::from(&e),
        WriteParseError::MultiTenantError(e) => StatusCode::from(&e),
    };
    ResponseBuilder::new().status(status).body(body).unwrap()
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue, header::ACCEPT};
    use http::{Request, Uri};

    use super::{extract_client_ip, extract_db_from_query_param};
    use crate::http::AuthenticationError;

    use super::QueryFormat;
    use super::ValidateDbNameError;
    use super::record_batch_stream_to_body;
    use super::token_part_as_bytes;
    use super::validate_db_name;
    use arrow_array::{Int32Array, RecordBatch, record_batch};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use iox_http_util::read_body_bytes_for_tests;
    use pretty_assertions::assert_eq;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::str;
    use std::sync::Arc;

    macro_rules! assert_validate_db_name {
        ($name:literal, $accept_rp:literal, $expected:pat) => {
            let actual = validate_db_name($name, $accept_rp);
            assert!(matches!(&actual, $expected), "got: {actual:?}",);
        };
    }

    #[test]
    fn test_try_from_headers_default_browser_accept_headers_to_json() {
        let mut map = HeaderMap::new();
        map.append(
            ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        let format = QueryFormat::try_from_headers(&map).unwrap();
        assert!(matches!(format, QueryFormat::Json));
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

    #[tokio::test]
    async fn test_json_output_empty() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(None), QueryFormat::Json)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "[]");
    }

    #[tokio::test]
    async fn test_json_output_one_record() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(1)), QueryFormat::Json)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "[{\"a\":1}]");
    }

    #[tokio::test]
    async fn test_json_output_all_empties() {
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(
                make_record_stream_with_sizes(vec![0, 0, 0]),
                QueryFormat::Json,
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "[]");
    }

    #[tokio::test]
    async fn test_empty_present_mixture() {
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(
                make_record_stream_with_sizes(vec![0, 0, 1, 1, 0, 1, 0]),
                QueryFormat::Json,
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(
            str::from_utf8(bytes.as_ref()).unwrap(),
            "[{\"a\":1},{\"a\":1},{\"a\":1}]"
        );
    }
    #[tokio::test]
    async fn test_json_output_three_records() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(3)), QueryFormat::Json)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            str::from_utf8(bytes.as_ref()).unwrap(),
            "[{\"a\":1},{\"a\":1},{\"a\":1}]"
        );
    }
    #[tokio::test]
    async fn test_json_output_five_records() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(5)), QueryFormat::Json)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            str::from_utf8(bytes.as_ref()).unwrap(),
            "[{\"a\":1},{\"a\":1},{\"a\":1},{\"a\":1},{\"a\":1}]"
        );
    }

    #[tokio::test]
    async fn test_jsonl_output_empty() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(None), QueryFormat::JsonLines)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "");
    }

    #[tokio::test]
    async fn test_jsonl_output_one_record() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(1)), QueryFormat::JsonLines)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "{\"a\":1}\n");
    }
    #[tokio::test]
    async fn test_jsonl_output_three_records() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(3)), QueryFormat::JsonLines)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            str::from_utf8(bytes.as_ref()).unwrap(),
            "{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n"
        );
    }
    #[tokio::test]
    async fn test_jsonl_output_five_records() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(5)), QueryFormat::JsonLines)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            str::from_utf8(bytes.as_ref()).unwrap(),
            "{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n{\"a\":1}\n"
        );
    }
    #[tokio::test]
    async fn test_csv_output_empty() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(None), QueryFormat::Csv)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "");
    }

    #[tokio::test]
    async fn test_csv_output_one_record() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(1)), QueryFormat::Csv)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "a\n1\n");
    }
    #[tokio::test]
    async fn test_csv_output_three_records() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(3)), QueryFormat::Csv)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(str::from_utf8(bytes.as_ref()).unwrap(), "a\n1\n1\n1\n");
    }
    #[tokio::test]
    async fn test_csv_output_five_records() {
        // Turn RecordBatches into a Body and then collect into Bytes to assert
        // their validity
        let bytes = read_body_bytes_for_tests(
            record_batch_stream_to_body(make_record_stream(Some(5)), QueryFormat::Csv)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            str::from_utf8(bytes.as_ref()).unwrap(),
            "a\n1\n1\n1\n1\n1\n"
        );
    }

    #[test]
    fn test_basic_auth_token_valid() {
        let token_bytes =
            token_part_as_bytes(
                "PHVzZXJuYW1lPjphcGl2M19Ka2Fsdi1JUEtxSlIyUDdRVDBuMjhnRnBmMWlFd0stZVo3cTZoWHF5enJKdTBrRVBCLVZFODhlR1hUVHo5R0tod0ttMzgtNnFreWtLUGRoTmVkdVM5Zw==")
            .expect("base64 encoded string to be valid");
        let token_string = String::from_utf8_lossy(&token_bytes);
        assert_eq!(
            token_string,
            "apiv3_Jkalv-IPKqJR2P7QT0n28gFpf1iEwK-eZ7q6hXqyzrJu0kEPB-VE88eGXTTz9GKhwKm38-6qkykKPdhNeduS9g"
        );
    }

    #[test]
    fn test_basic_auth_token_invalid() {
        let invalid_token = token_part_as_bytes(
            "YXBpdjNfSmthbHYtSVBLcUpSMlA3UVQwbjI4Z0ZwZjFpRXdLLWVaN3E2aFhxeXpySnUwa0VQQi1WRTg4ZUdYVFR6OUdLaHdLbTM4LTZxa3lrS1BkaE5lZHVTOWc=",
        );
        assert!(matches!(
            invalid_token,
            Err(AuthenticationError::MalformedRequest)
        ));
    }

    #[test]
    fn test_basic_auth_token_should_not_allow_colon_in_username() {
        //  echo -n "foo:bar:$TOKEN" | base64 -w 0
        let invalid_token = token_part_as_bytes(
            "Zm9vOmJhcjphcGl2M19Ka2Fsdi1JUEtxSlIyUDdRVDBuMjhnRnBmMWlFd0stZVo3cTZoWHF5enJKdTBrRVBCLVZFODhlR1hUVHo5R0tod0ttMzgtNnFreWtLUGRoTmVkdVM5Zw==",
        );
        assert!(matches!(
            invalid_token,
            Err(AuthenticationError::MalformedRequest)
        ));
    }

    #[test]
    fn test_client_ip_extraction_from_socket_address() {
        use hyper::Request;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // Create a request with socket address in extensions
        let mut req = Request::builder()
            .uri("http://example.com/api/v3/write_lp?db=test")
            .body(())
            .unwrap();

        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 8080);
        req.extensions_mut().insert(Some(socket_addr));

        // Extract client IP - should get socket address since no headers
        let client_ip = extract_client_ip(&req);

        assert_eq!(client_ip, Some("192.168.1.100".to_string()));
    }

    #[test]
    fn test_client_ip_extraction_prefers_headers_over_socket() {
        use hyper::Request;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // Create a request with both headers and socket address
        let mut req = Request::builder()
            .uri("http://example.com/api/v3/write_lp?db=test")
            .header("x-forwarded-for", "10.0.0.1")
            .body(())
            .unwrap();

        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 8080);
        req.extensions_mut().insert(Some(socket_addr));

        // Extract client IP - should prefer header over socket
        let client_ip = extract_client_ip(&req);

        assert_eq!(client_ip, Some("10.0.0.1".to_string()));
    }

    fn make_record_stream(records: Option<usize>) -> SendableRecordBatchStream {
        match records {
            None => make_record_stream_with_sizes(vec![]),
            Some(num) => make_record_stream_with_sizes(vec![1; num]),
        }
    }

    fn make_record_stream_with_sizes(batch_sizes: Vec<usize>) -> SendableRecordBatchStream {
        let batch = record_batch!(("a", Int32, [1])).unwrap();
        let schema = batch.schema();

        // If there are no sizes, return empty stream
        if batch_sizes.is_empty() {
            let stream = futures::stream::iter(Vec::new());
            let adapter = RecordBatchStreamAdapter::new(schema, stream);
            return Box::pin(adapter);
        }

        let batches = batch_sizes
            .into_iter()
            .map(|size| {
                if size == 0 {
                    // Create an empty batch
                    Ok(RecordBatch::new_empty(Arc::clone(&schema)))
                } else {
                    // Create a batch with 'size' rows, all with value 1
                    Ok(RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int32Array::from_iter_values(vec![1; size]))],
                    )?)
                }
            })
            .collect::<Vec<_>>();

        let stream = futures::stream::iter(batches);
        let adapter = RecordBatchStreamAdapter::new(schema, stream);
        Box::pin(adapter)
    }

    #[test]
    fn test_extract_db_from_query_param() {
        // Test with valid db parameter
        let uri = Uri::try_from("http://example.com/api/v3/write?db=mydb").unwrap();
        assert_eq!(extract_db_from_query_param(&uri), Some("mydb".to_string()));

        // Test with db parameter among other parameters
        let uri =
            Uri::try_from("http://example.com/api/v3/write?foo=bar&db=testdb&baz=qux").unwrap();
        assert_eq!(
            extract_db_from_query_param(&uri),
            Some("testdb".to_string())
        );

        // Test with empty db parameter
        let uri = Uri::try_from("http://example.com/api/v3/write?db=").unwrap();
        assert_eq!(extract_db_from_query_param(&uri), Some("".to_string()));

        // Test without db parameter
        let uri = Uri::try_from("http://example.com/api/v3/write?foo=bar").unwrap();
        assert_eq!(extract_db_from_query_param(&uri), None);

        // Test with no query parameters
        let uri = Uri::try_from("http://example.com/api/v3/write").unwrap();
        assert_eq!(extract_db_from_query_param(&uri), None);

        // Test with URL encoded db name
        let uri = Uri::try_from("http://example.com/api/v3/write?db=my%20database").unwrap();
        assert_eq!(
            extract_db_from_query_param(&uri),
            Some("my database".to_string())
        );

        // Test with special characters in db name
        let uri = Uri::try_from("http://example.com/api/v3/write?db=db-name_123").unwrap();
        assert_eq!(
            extract_db_from_query_param(&uri),
            Some("db-name_123".to_string())
        );

        // Test with multiple db parameters (should return first one)
        let uri = Uri::try_from("http://example.com/api/v3/write?db=first&db=second").unwrap();
        assert_eq!(extract_db_from_query_param(&uri), Some("first".to_string()));
    }

    #[test]
    fn test_extract_client_ip() {
        // Test with x-forwarded-for header (single IP)
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-forwarded-for", "192.168.1.100")
            .body("")
            .unwrap();
        assert_eq!(extract_client_ip(&req), Some("192.168.1.100".to_string()));

        // Test with x-forwarded-for header (multiple IPs, should take first)
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-forwarded-for", "10.0.0.1, 172.16.0.1, 192.168.1.1")
            .body("")
            .unwrap();
        assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

        // Test with x-forwarded-for header with spaces
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-forwarded-for", "  10.0.0.1  ,  172.16.0.1  ")
            .body("")
            .unwrap();
        assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

        // Test with x-real-ip header
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-real-ip", "192.168.1.50")
            .body("")
            .unwrap();
        assert_eq!(extract_client_ip(&req), Some("192.168.1.50".to_string()));

        // Test with both headers (x-forwarded-for takes precedence)
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-forwarded-for", "10.0.0.1")
            .header("x-real-ip", "192.168.1.50")
            .body("")
            .unwrap();
        assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

        // Test with socket address in extensions (IPv4)
        let mut req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .body("")
            .unwrap();
        let socket_addr = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        req.extensions_mut().insert(socket_addr);
        assert_eq!(extract_client_ip(&req), Some("127.0.0.1".to_string()));

        // Test with socket address in extensions (IPv6)
        let mut req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .body("")
            .unwrap();
        let socket_addr = Some(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
            8080,
        ));
        req.extensions_mut().insert(socket_addr);
        assert_eq!(extract_client_ip(&req), Some("::1".to_string()));

        // Test with None socket address in extensions
        let mut req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .body("")
            .unwrap();
        let socket_addr: Option<SocketAddr> = None;
        req.extensions_mut().insert(socket_addr);
        assert_eq!(extract_client_ip(&req), Some("unknown".to_string()));

        // Test with no headers and no socket address
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .body("")
            .unwrap();
        assert_eq!(extract_client_ip(&req), None);

        // Test header precedence: x-forwarded-for > x-real-ip > socket
        let mut req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-forwarded-for", "10.0.0.1")
            .header("x-real-ip", "192.168.1.50")
            .body("")
            .unwrap();
        let socket_addr = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        req.extensions_mut().insert(socket_addr);
        assert_eq!(extract_client_ip(&req), Some("10.0.0.1".to_string()));

        // Test with empty x-forwarded-for header
        let req = Request::builder()
            .uri("http://example.com/api/v3/write")
            .header("x-forwarded-for", "")
            .body("")
            .unwrap();
        // Should return empty string since the header exists but is empty
        assert_eq!(extract_client_ip(&req), Some("".to_string()));
    }
}
