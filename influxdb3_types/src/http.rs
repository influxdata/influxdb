mod enterprise;
pub use enterprise::*;
use influxdb3_catalog::log::TriggerSettings;

use crate::write::Precision;
use hashbrown::HashMap;
use hyper::HeaderMap;
use hyper::header::ACCEPT;
use hyper::http::HeaderValue;
pub use influxdb3_catalog::log::{
    FieldDataType, LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality,
};
use iox_query_params::StatementParams;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid mime type ({0})")]
    InvalidMimeType(String),

    #[error("the mime type specified was not valid UTF8: {0}")]
    NonUtf8MimeType(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PingResponse {
    pub version: String,
    pub revision: String,
}

impl PingResponse {
    /// Get the `version` from the response
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the `revision` from the response
    pub fn revision(&self) -> &str {
        &self.revision
    }
}

/// Request definition for the `POST /api/v3/configure/distinct_cache` API
#[derive(Debug, Deserialize, Serialize)]
pub struct DistinctCacheCreateRequest {
    /// The name of the database associated with the cache
    pub db: String,
    /// The name of the table associated with the cache
    pub table: String,
    /// The name of the cache. If not provided, the cache name will be generated from the table
    /// name and selected column names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The columns to create the cache on.
    // TODO: this should eventually be made optional, so that if not provided, the columns used will
    // correspond to the series key columns for the table, i.e., the tags. See:
    // https://github.com/influxdata/influxdb/issues/25585
    pub columns: Vec<String>,
    /// The maximumn number of distinct value combinations to hold in the cache
    #[serde(default)]
    pub max_cardinality: MaxCardinality,
    /// The duration in seconds that entries will be kept in the cache before being evicted
    #[serde(default)]
    pub max_age: MaxAge,
}

/// Resposne definition for the `POST /api/v3/configure/distinct_cache` API
#[derive(Debug, Deserialize, Serialize)]
pub struct DistinctCacheCreatedResponse {
    /// The id of the table the cache was created on
    pub table_id: u32,
    /// The name of the table the cache was created on
    pub table_name: String,
    /// The name of the created cache
    pub cache_name: String,
    /// The columns in the cache
    pub column_ids: Vec<u32>,
    /// The maximum number of unique value combinations the cache will hold
    pub max_cardinality: usize,
    /// The maximum age for entries in the cache
    pub max_age_seconds: u64,
}

/// Request definition for the `DELETE /api/v3/configure/distinct_cache` API
#[derive(Debug, Deserialize, Serialize)]
pub struct DistinctCacheDeleteRequest {
    pub db: String,
    pub table: String,
    pub name: String,
}

/// Request definition for the `POST /api/v3/configure/last_cache` API
#[derive(Debug, Deserialize, Serialize)]
pub struct LastCacheCreateRequest {
    pub db: String,
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_columns: Option<Vec<String>>,
    #[serde(default)]
    pub count: LastCacheSize,
    #[serde(default)]
    pub ttl: LastCacheTtl,
}

/// Request definition for the `DELETE /api/v3/configure/last_cache` API
#[derive(Debug, Deserialize, Serialize)]
pub struct LastCacheDeleteRequest {
    pub db: String,
    pub table: String,
    pub name: String,
}

/// Request definition for the `POST /api/v3/configure/processing_engine_plugin` API
#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEnginePluginCreateRequest {
    pub db: String,
    pub plugin_name: String,
    pub file_name: String,
    pub plugin_type: String,
}

/// Request definition for the `DELETE /api/v3/configure/processing_engine_plugin` API
#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEnginePluginDeleteRequest {
    pub db: String,
    pub plugin_name: String,
}

/// Request definition for the `POST /api/v3/configure/processing_engine_trigger` API
#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEngineTriggerCreateRequest {
    pub db: String,
    pub plugin_filename: String,
    pub trigger_name: String,
    pub trigger_settings: TriggerSettings,
    pub trigger_specification: String,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

/// Request definition for the `DELETE /api/v3/configure/processing_engine_trigger` API
#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEngineTriggerDeleteRequest {
    pub db: String,
    pub trigger_name: String,
    #[serde(default)]
    pub force: bool,
}

/// Request definition for the `POST /api/v3/configure/plugin_environment/install_packages` API
#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEngineInstallPackagesRequest {
    pub packages: Vec<String>,
}

/// Request definition for the `POST /api/v3/configure/plugin_environment/install_requirements` API
#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEngineInstallRequirementsRequest {
    pub requirements_location: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessingEngineTriggerIdentifier {
    pub db: String,
    pub trigger_name: String,
}

/// Request definition for the `POST /api/v3/plugin_test/wal` API
#[derive(Debug, Deserialize, Serialize)]
pub struct WalPluginTestRequest {
    pub filename: String,
    pub database: String,
    pub input_lp: String,
    pub input_arguments: Option<HashMap<String, String>>,
}

/// Response definition for the `POST /api/v3/plugin_test/wal` API
#[derive(Debug, Deserialize, Serialize)]
pub struct WalPluginTestResponse {
    pub log_lines: Vec<String>,
    pub database_writes: HashMap<String, Vec<String>>,
    pub errors: Vec<String>,
}

/// Request definition for the `POST /api/v3/plugin_test/schedule` API
#[derive(Debug, Deserialize, Serialize)]
pub struct SchedulePluginTestRequest {
    pub filename: String,
    pub database: String,
    pub schedule: Option<String>,
    pub input_arguments: Option<HashMap<String, String>>,
}

/// Response definition for the `POST /api/v3/plugin_test/schedule` API
#[derive(Debug, Deserialize, Serialize)]
pub struct SchedulePluginTestResponse {
    pub trigger_time: Option<String>,
    pub log_lines: Vec<String>,
    pub database_writes: HashMap<String, Vec<String>>,
    pub errors: Vec<String>,
}

/// Request definition for the `GET /api/v3/configure/database` API
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct ShowDatabasesRequest {
    pub format: QueryFormat,
    #[serde(default)]
    pub show_deleted: bool,
}

/// Request definition for the `POST /api/v3/configure/database` API
#[derive(Debug, Deserialize, Serialize)]
pub struct CreateDatabaseRequest {
    pub db: String,
}

/// Request definition for the `DELETE /api/v3/configure/database` API
#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteDatabaseRequest {
    pub db: String,
}

/// Request definition for the `POST /api/v3/configure/table` API
#[derive(Debug, Deserialize, Serialize)]
pub struct CreateTableRequest {
    pub db: String,
    pub table: String,
    pub tags: Vec<String>,
    pub fields: Vec<CreateTableField>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateTableField {
    pub name: String,
    pub r#type: FieldType,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Utf8,
    Int64,
    UInt64,
    Float64,
    Bool,
}

impl From<FieldType> for FieldDataType {
    fn from(f: FieldType) -> Self {
        match f {
            FieldType::Utf8 => Self::String,
            FieldType::Int64 => Self::Integer,
            FieldType::UInt64 => Self::UInteger,
            FieldType::Float64 => Self::Float,
            FieldType::Bool => Self::Boolean,
        }
    }
}

/// Request definition for the `DELETE /api/v3/configure/table` API
#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteTableRequest {
    pub db: String,
    pub table: String,
}

pub type ClientQueryRequest = QueryRequest<String, Option<QueryFormat>, StatementParams>;

/// Request definition for the `POST /api/v3/query_sql` and `POST /api/v3/query_influxql` APIs
#[derive(Debug, Deserialize, Serialize)]
pub struct QueryRequest<D, F, P> {
    #[serde(rename = "db")]
    pub database: D,
    #[serde(rename = "q")]
    pub query_str: String,
    pub format: F,
    pub params: Option<P>,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryFormat {
    Parquet,
    Csv,
    Pretty,
    Json,
    #[serde(alias = "jsonl")]
    JsonLines,
}

impl QueryFormat {
    pub fn as_content_type(&self) -> &str {
        match self {
            Self::Parquet => "application/vnd.apache.parquet",
            Self::Csv => "text/csv",
            Self::Pretty => "text/plain; charset=utf-8",
            Self::Json => "application/json",
            Self::JsonLines => "application/jsonl",
        }
    }

    pub fn try_from_headers(headers: &HeaderMap) -> std::result::Result<Self, Error> {
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
                Ok(s) => {
                    if s.contains("text/html") || s.contains("*/*") {
                        return Ok(Self::Json);
                    }
                    Err(Error::InvalidMimeType(s))
                }
                Err(e) => Err(Error::NonUtf8MimeType(e)),
            },
        }
    }
}

/// The URL parameters of the request to the `/api/v3/write_lp` API
#[derive(Debug, Deserialize, Serialize)]
pub struct WriteParams {
    pub db: String,
    pub precision: Option<Precision>,
    pub accept_partial: Option<bool>,
    pub no_sync: Option<bool>,
}

impl From<iox_http::write::WriteParams> for WriteParams {
    fn from(legacy: iox_http::write::WriteParams) -> Self {
        Self {
            db: legacy.namespace.to_string(),
            // legacy behaviour was to not accept partial:
            accept_partial: Some(false),
            precision: Some(legacy.precision.into()),
            no_sync: Some(false),
        }
    }
}
