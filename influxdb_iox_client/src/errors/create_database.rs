use thiserror::Error;

use super::{ApiErrorCode, ClientError, HttpError, ServerErrorResponse};

/// Error responses when creating a new IOx database.
#[derive(Debug, Error)]
pub enum CreateDatabaseError {
    /// The database name contains an invalid character.
    #[error("the database name contains an invalid character")]
    InvalidName,

    /// The database being created already exists.
    #[error("a database with the requested name already exists")]
    AlreadyExists,

    /// An unknown server error occurred.
    ///
    /// The error string contains the error string returned by the server.
    #[error(transparent)]
    ServerError(ServerErrorResponse),

    /// A non-application HTTP request/response error occurred.
    #[error(transparent)]
    HttpError(#[from] HttpError),

    /// An error occurred in the client.
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

/// Convert a [`ServerErrorResponse`] into a [`CreateDatabaseError`].
///
/// This conversion plucks any errors with API error codes that are applicable
/// to [`CreateDatabaseError`] types, and everything else becomes a
/// `ServerError`.
impl From<ServerErrorResponse> for CreateDatabaseError {
    fn from(err: ServerErrorResponse) -> Self {
        match err.error_code() {
            Some(c) if c == ApiErrorCode::DB_INVALID_NAME as u32 => Self::InvalidName,
            Some(c) if c == ApiErrorCode::DB_ALREADY_EXISTS as u32 => Self::AlreadyExists,
            _ => Self::ServerError(err),
        }
    }
}

/// Convert errors from the underlying HTTP client into `HttpError` instances.
impl From<reqwest::Error> for CreateDatabaseError {
    fn from(err: reqwest::Error) -> Self {
        Self::HttpError(err.into())
    }
}
