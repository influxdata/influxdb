use hyper::StatusCode;

use crate::server::data::DataError;

/// Error type for the server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error in the keyspace layer.
    #[error("Keyspace error: {0}")]
    Keyspace(String),
    /// Error in the precondition layer.
    #[error("Precondition error: {0}")]
    Precondition(String),
    /// Error in the data layer.
    #[error("Data error: {0}")]
    Data(#[from] DataError),

    /// Error with warming.
    #[error("Warming error: {0}")]
    Warming(String),
    /// Cache miss.
    #[error("Cache miss")]
    CacheMiss,
    /// Bad request from the user.
    #[error("Bad Request: {0}")]
    BadRequest(String),
    /// Object does not exist.
    #[error("Bad Request: object location does not exist in catalog or object store")]
    DoesNotExist,
    /// Error due to server shutdown.
    #[error("Server shutdown")]
    ServerShutdown,
}

impl Error {
    /// Return the HTTP status code for this error.
    ///
    /// Should match the handling, per code, in the [client](crate::client::object_store::DataCacheObjectStore).
    pub fn code(&self) -> StatusCode {
        match self {
            // If errors here, have the client return an error.
            Self::BadRequest(_)
            | Self::DoesNotExist
            | Self::Data(DataError::BadRequest(_))
            | Self::Data(DataError::DoesNotExist) => StatusCode::BAD_REQUEST,
            Self::Precondition(_) => StatusCode::PRECONDITION_FAILED,
            // If errors below here, result in the client using the fallback.
            Self::CacheMiss => StatusCode::NOT_FOUND,
            Self::Keyspace(_) | Self::Warming(_) | Self::Data(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::ServerShutdown => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}
