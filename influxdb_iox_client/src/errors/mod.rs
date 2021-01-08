//! Error types returned by the [`Client`][crate::Client].
use snafu::Snafu;

mod http_error;
pub use http_error::*;

mod server_error_response;
pub use server_error_response::*;

mod create_database;
pub use create_database::*;

/// A generic HTTP request/response error.
#[derive(Debug, Snafu)]
pub enum RequestError {
    /// The HTTP request failed, or the response returned invalid data.
    #[snafu(display("http request error: {}", source))]
    HttpRequestError {
        /// The underlying HTTP error.
        source: HttpError,
    },

    /// The response contained an unexpected HTTP status code.
    #[snafu(display("response contains unexpected status code {}", code))]
    UnexpectedStatusCode {
        /// The HTTP status code returned.
        code: u16,
    },

    /// The server understood the HTTP request but rejected it due to semantic
    /// errors.
    #[snafu(display("error response from server: {}", source))]
    BadRequest {
        /// The server-generated error message (if any)
        source: ServerErrorResponse,
    },
}

impl From<reqwest::Error> for RequestError {
    fn from(v: reqwest::Error) -> Self {
        RequestError::HttpRequestError { source: v.into() }
    }
}
