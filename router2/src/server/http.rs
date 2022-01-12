//! HTTP service implementations for `router2`.

use hyper::{Body, Request, Response, StatusCode};
use thiserror::Error;

/// Errors returned by the `router2` HTTP request handler.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested path has no registered handler.
    #[error("not found")]
    NotFound,
}

impl Error {
    /// Convert the error into an appropriate [`StatusCode`] to be returned to
    /// the end user.
    pub fn as_status_code(&self) -> StatusCode {
        match self {
            Error::NotFound => StatusCode::NOT_FOUND,
        }
    }
}

/// This type is responsible for servicing requests to the `router2` HTTP
/// endpoint.
///
/// Requests to some paths may be handled externally by the caller - the IOx
/// server runner framework takes care of implementing the heath endpoint,
/// metrics, pprof, etc.
#[derive(Debug, Default)]
pub struct HttpDelegate;

impl HttpDelegate {
    /// Routes `req` to the appropriate handler, if any, returning the handler
    /// response.
    pub fn route(&self, _req: Request<Body>) -> Result<Response<Body>, Error> {
        unimplemented!()
    }
}
