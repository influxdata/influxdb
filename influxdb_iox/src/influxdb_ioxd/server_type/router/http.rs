use hyper::{Body, Method, Request, Response};
use snafu::Snafu;

use crate::influxdb_ioxd::server_type::RouteError;

use super::RouterServerType;

#[derive(Debug, Snafu)]
pub enum ApplicationError {
    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },
}

impl RouteError for ApplicationError {
    fn response(&self) -> http::Response<hyper::Body> {
        match self {
            ApplicationError::RouteNotFound { .. } => self.not_found(),
        }
    }
}

#[allow(clippy::match_single_binding)]
pub async fn route_request(
    _server_type: &RouterServerType,
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let method = req.method().clone();
    let uri = req.uri().clone();

    match (method, uri.path()) {
        (method, path) => Err(ApplicationError::RouteNotFound {
            method,
            path: path.to_string(),
        }),
    }
}
