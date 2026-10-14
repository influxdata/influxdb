//! Operator socket HTTP handlers
//!
//! This module provides HTTP handlers for the operator control socket (Unix Domain Socket).
//! These endpoints bypass authentication - authorization is via filesystem permissions on the socket.
//!
//! The handlers reuse the same implementation as the main HTTP API endpoints, just without
//! the authentication middleware.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use hyper::StatusCode;
use iox_http_util::{Response, ResponseBuilder, bytes_to_response_body};
use observability_deps::tracing::trace;

use super::{HttpApi, IntoResponse};
use crate::all_paths;

/// Route requests from the operator control socket.
///
/// These requests bypass authentication - access control is via socket permissions.
/// Only a limited set of read-only operations are available through this endpoint.
pub async fn route_operator_request(
    http_api: Arc<HttpApi>,
    req: hyper::Request<hyper::body::Incoming>,
) -> Result<Response, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    trace!(request = ?req, "Processing operator socket request");

    let response = match (method, path.as_str()) {
        (Method::GET, all_paths::API_METRICS) => http_api.handle_metrics(),
        (Method::GET, all_paths::API_V3_HEALTH | all_paths::API_V1_HEALTH) => http_api.health(),
        _ => Ok(not_found_response()),
    };

    let response = match response {
        Ok(response) => response,
        Err(e) => e.into_response(),
    };

    Ok(response)
}

/// Create a 404 Not Found response
fn not_found_response() -> Response {
    ResponseBuilder::new()
        .status(StatusCode::NOT_FOUND)
        .body(bytes_to_response_body(Bytes::from("not found")))
        .unwrap()
}
