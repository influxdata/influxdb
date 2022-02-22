//! HTTP service implementations for `compactor`.

use crate::handler::CompactorHandler;
use hyper::{Body, Request, Response, StatusCode};
use std::sync::Arc;
use thiserror::Error;

/// Errors returned by the HTTP request handler.
#[derive(Debug, Error, Copy, Clone)]
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

/// This type is responsible for servicing requests to the `compactor` HTTP
/// endpoint.
///
/// Requests to some paths may be handled externally by the caller - the IOx
/// server runner framework takes care of implementing the heath endpoint,
/// metrics, pprof, etc.
#[derive(Debug, Default)]
pub struct HttpDelegate<I: CompactorHandler> {
    #[allow(dead_code)]
    compactor_handler: Arc<I>,
}

impl<I: CompactorHandler> HttpDelegate<I> {
    /// Initialise a new [`HttpDelegate`] passing valid requests to the
    /// specified `compactor_handler`.
    pub fn new(compactor_handler: Arc<I>) -> Self {
        Self { compactor_handler }
    }

    /// Routes `req` to the appropriate handler, if any, returning the handler
    /// response.
    pub fn route(&self, _req: Request<Body>) -> Result<Response<Body>, Error> {
        unimplemented!()
    }
}
