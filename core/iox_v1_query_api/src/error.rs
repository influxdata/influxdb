use std::fmt::Debug;

use datafusion::error::DataFusionError;
use iox_query_influxql_rewrite as rewrite;
use thiserror::Error;

/// Error type for the v1 API
///
/// This is used to catch errors that occur during the streaming process.
/// [`anyhow::Error`] is used as a catch-all because if anything fails during
/// that process it will result in a 500 INTERNAL ERROR.
#[derive(Debug, thiserror::Error)]
#[error("unexpected query error: {0}")]
pub struct QueryError(#[from] pub anyhow::Error);

#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found: {0}")]
    NoHandler(String),

    #[error("authorization failure: {0}")]
    AuthorizationFailure(String),

    #[error("invalid mime type ({0})")]
    InvalidMimeType(String),

    /// Missing parameters for query
    #[error("missing query parameters 'db' and 'q'")]
    MissingQueryParams,

    #[error("error decoding multipart file upload: {0}")]
    MultipartFile(String),

    #[error("Invalid UTF8: {message} {error}")]
    Utf8 {
        message: &'static str,
        error: String,
    },

    /// Serde decode error
    #[error("error decoding params from url: {0}")]
    SerdeUrlDecoding(#[from] serde_urlencoded::de::Error),

    // SerdeJsonError
    #[error("error decoding query body: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("datafusion error: {0}")]
    Datafusion(#[from] DataFusionError),

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

    #[error(
        "provided a retention policy in both the parameters ({param_rp}) and \
        query string ({query_rp}) that do not match, if providing a query \
        that specifies the retention_policy, you can omit the 'rp' parameter \
        from your request"
    )]
    InfluxqlRetentionPolicyMismatch { param_rp: String, query_rp: String },

    #[error("error reading field from body: {name} -- {error}")]
    FieldRead { name: &'static str, error: String },

    #[error("Cannot retrieve database: {0}")]
    Database(DataFusionError),

    #[error("Database {0} not found")]
    DatabaseNotFound(String),

    #[error("v1 query API error: {0}")]
    V1Query(#[from] QueryError),
}

#[derive(Debug, Clone)]
pub enum HttpError {
    NotFound(String),
    Unauthorized(String),
    Invalid(String),
    InternalError(String),
}

impl From<Error> for HttpError {
    fn from(e: Error) -> Self {
        use Error::*;
        use HttpError::*;
        match e {
            NoHandler(_) => NotFound(e.to_string()),
            InvalidMimeType(_)
            | MissingQueryParams
            | InfluxqlSingleStatement
            | InfluxqlNoDatabase
            | Database(_)
            | DatabaseNotFound(_)
            | InfluxqlDatabaseMismatch { .. }
            | InfluxqlRetentionPolicyMismatch { .. }
            | MultipartFile(_)
            | SerdeUrlDecoding(_)
            | SerdeJson(_)
            | Utf8 { .. }
            | FieldRead { .. }
            | InfluxqlRewrite(_) => Invalid(e.to_string()),
            Datafusion(_) | V1Query(_) => InternalError(e.to_string()),
            AuthorizationFailure(_) => Unauthorized(e.to_string()),
        }
    }
}
