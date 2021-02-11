//! Error types returned by a client.
//!
//! A request to an IOx server can fail in three main ways:
//!
//!  - an HTTP transport error (network error)
//!  - a known API handler error response
//!  - an unknown error returned by the server
//!
//! The first is converted into a [`HttpError`] and contains details of the
//! failed request.
//!
//! The second case is when the "business logic" of the API handler returns a
//! defined, meaningful error to the client. Examples of this include "the
//! database name is invalid" or "the database already exists". These are mapped
//! to per-handler error types (see [`CreateDatabaseError`] as an example).
//!
//! The last case is a generic error returned by the IOx server. These become
//! [`ServerErrorResponse`] instances and contain the error string, optional
//! error code and HTTP status code sent by the server.
//!
//! If using the Arrow Flight API, errors from gRPC requests will be converted
//! into a [`GrpcError`] containing details of the failed request.

use thiserror::Error;

mod http_error;
pub use http_error::*;

mod client_error;
pub use client_error::*;

mod server_error_response;
pub use server_error_response::*;

mod create_database;
pub use create_database::*;

#[cfg(feature = "flight")]
mod grpc_error;
#[cfg(feature = "flight")]
pub use grpc_error::*;

#[cfg(feature = "flight")]
mod grpc_query_error;
#[cfg(feature = "flight")]
pub use grpc_query_error::*;

/// Constants used in API error codes.
///
/// Expressing this as a enum prevents reuse of discriminants, and as they're
/// effectively consts this uses UPPER_SNAKE_CASE.
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub enum ApiErrorCode {
    /// An unknown/unhandled error
    UNKNOWN = 100,

    /// The database name in the request is invalid.
    DB_INVALID_NAME = 101,

    /// The database referenced already exists.
    DB_ALREADY_EXISTS = 102,

    /// The database referenced does not exist.
    DB_NOT_FOUND = 103,
}

impl From<ApiErrorCode> for u32 {
    fn from(v: ApiErrorCode) -> Self {
        v as u32
    }
}
/// `Error` defines the generic error type for API methods that do not have
/// specific error types.
#[derive(Debug, Error)]
pub enum Error {
    /// A non-application HTTP request/response error has occurred.
    #[error("http request/response error: {0}")]
    HttpError(#[from] HttpError),

    /// The IOx server has responded with an error.
    #[error("error response from server: {0}")]
    ServerError(#[from] ServerErrorResponse),
}

/// Convert errors from the underlying HTTP client into `HttpError` instances.
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::HttpError(err.into())
    }
}
