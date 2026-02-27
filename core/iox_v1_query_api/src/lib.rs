use workspace_hack as _;

// A large majority of the code in this file was copied from the Monolith
// project and then adapted to fit our format/needs.
use std::{collections::HashMap, fmt::Debug, num::ParseIntError, str::ParseBoolError};

use bytes::Bytes;
use error::Error;
use iox_http_util::Request;
use serde::Deserialize;
use types::Precision;
use types::Statement;

mod error;
pub use error::HttpError;
mod handler;
pub use handler::V1HttpHandler;
mod response;
mod types;
mod value;

const DEFAULT_CHUNK_SIZE: usize = 10_000;

type Result<T, E = Error> = std::result::Result<T, E>;
type StatementFuture = Box<dyn Future<Output = std::result::Result<Statement, Error>> + Send>;
/// Enum representing the query format for the v1/query API.
///
/// The original API supports CSV, JSON, and "pretty" JSON formats.
#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryFormat {
    Csv,
    #[default]
    Json,
    JsonPretty,
    MsgPack,
}

impl QueryFormat {
    /// Returns the content type as a string slice for the query format.
    ///
    /// Maps the `QueryFormat` variants to their corresponding MIME types as strings.
    /// This is useful for setting the `Content-Type` header in HTTP responses.
    pub fn as_content_type(&self) -> &str {
        match self {
            Self::Csv => "application/csv",
            Self::Json | Self::JsonPretty => "application/json",
            Self::MsgPack => "application/x-msgpack",
        }
    }

    /// Extracts the [`QueryFormat`] from an HTTP [`Request`].
    ///
    /// Parses the HTTP request to determine the desired query format. The `pretty`
    /// parameter indicates if the pretty format is requested via a query parameter.
    /// The function inspects the `Accept` header of the request to determine the
    /// format, defaulting to JSON if no specific format is requested. If the format
    /// is invalid or non-UTF8, an error is returned.
    pub fn from_bytes(mime_type: Option<&[u8]>, pretty: bool) -> Result<Self> {
        match mime_type {
            Some(b"application/csv" | b"text/csv") => Ok(Self::Csv),
            Some(b"application/x-msgpack") => Ok(Self::MsgPack),
            Some(b"application/json" | b"*/*") | None => {
                // If no specific format is requested via the Accept header,
                // and the 'pretty' parameter is true, use the pretty JSON format.
                // Otherwise, default to the regular JSON format.
                if pretty {
                    Ok(Self::JsonPretty)
                } else {
                    Ok(Self::Json)
                }
            }
            Some(mime_type) => match std::str::from_utf8(mime_type) {
                Ok(s) => Err(Error::InvalidMimeType(s.to_owned())),
                Err(e) => Err(Error::Utf8 {
                    message: "mime type",
                    error: e.to_string(),
                }),
            },
        }
    }
}

/// Query parameters for the v1/query API
///
/// The original API supports a `u` parameter, for "username", as well as a `p`,
/// for "password". The password is extracted upstream, and username is ignored.
#[derive(Debug, Default, Deserialize)]
pub struct QueryParams {
    /// Chunk the response into chunks of size `chunk_size`, or 10,000, or by series
    pub chunked: Option<bool>,
    /// Define the number of records that will go into a chunk
    pub chunk_size: Option<usize>,
    /// Database to perform the query against
    ///
    /// This is optional because the query string may specify the database
    #[serde(rename = "db")]
    pub database: Option<String>,
    /// Retention Policy to perform the query against
    ///
    /// This is optional because the query string may specify the rp
    #[serde(rename = "rp")]
    pub retention_policy: Option<String>,
    /// Map timestamps to UNIX epoch time, with the given precision
    pub epoch: Option<Precision>,
    /// Format the JSON outputted in pretty format
    pub pretty: Option<bool>,
    /// The InfluxQL query string
    #[serde(rename = "q")]
    pub query: Option<String>,
    /// Params for parameterized queries
    pub params: Option<String>,
}

impl QueryParams {
    /// Extract [`QueryParams`] from an HTTP [`Request`]
    pub fn from_request_query_string(req: &Request) -> Result<Self> {
        let query = req.uri().query().ok_or(Error::MissingQueryParams)?;
        let mut params: Self = serde_urlencoded::from_str(query).map_err(Error::from)?;

        // For other request types we need to know if the value was set or not,
        // so we have to unwrap_or_default here rather than on QueryParams directly.
        params.chunked = Some(params.chunked.unwrap_or_default());
        params.pretty = Some(params.pretty.unwrap_or_default());

        Ok(params)
    }

    pub fn from_bytes_form_urlencoded(bytes: &Bytes) -> Result<Self> {
        serde_urlencoded::from_bytes(bytes).map_err(Into::into)
    }

    pub fn from_hashmap_multipart(fields: HashMap<String, String>) -> Result<Self> {
        let mut this = Self::default();

        if let Some(chunked) = fields.get("chunked") {
            let b = chunked
                .trim()
                .parse()
                .map_err(|e: ParseBoolError| Error::FieldRead {
                    name: "chunked",
                    error: e.to_string(),
                })?;
            this.chunked = Some(b);
        }

        if let Some(chunk_size) = fields.get("chunk_size") {
            let u = chunk_size
                .trim()
                .parse()
                .map_err(|e: ParseIntError| Error::FieldRead {
                    name: "chunk_size",
                    error: e.to_string(),
                })?;
            this.chunk_size = Some(u);
        }

        if let Some(epoch) = fields.get("epoch") {
            let e = epoch.trim();
            let e = serde_json::from_str(e).map_err(Error::from)?;
            this.epoch = Some(e)
        }

        if let Some(pretty) = fields.get("pretty") {
            let p = pretty
                .trim()
                .parse()
                .map_err(|e: ParseBoolError| Error::FieldRead {
                    name: "pretty",
                    error: e.to_string(),
                })?;
            this.pretty = Some(p);
        }

        this.database = fields.get("db").cloned();
        this.retention_policy = fields.get("rp").cloned();
        this.query = fields.get("q").cloned();
        this.params = fields.get("params").cloned();

        Ok(this)
    }

    pub fn merge(&mut self, lower_precedence: Self) {
        if self.chunked.is_none() {
            self.chunked = lower_precedence.chunked;
        }

        if self.chunk_size.is_none() {
            self.chunk_size = lower_precedence.chunk_size;
        }

        if self.database.is_none() {
            self.database = lower_precedence.database;
        }

        if self.retention_policy.is_none() {
            self.retention_policy = lower_precedence.retention_policy;
        }

        if self.epoch.is_none() {
            self.epoch = lower_precedence.epoch;
        }

        if self.pretty.is_none() {
            self.pretty = lower_precedence.pretty;
        }

        if self.query.is_none() {
            self.query = lower_precedence.query;
        }

        if self.params.is_none() {
            self.params = lower_precedence.params;
        }
    }
}
