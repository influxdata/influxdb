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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use router::server::RouterServer;
    use time::SystemProvider;

    use crate::influxdb_ioxd::{
        http::test_utils::{assert_health, assert_metrics, TestServer},
        server_type::common_state::CommonServerState,
    };

    use super::*;

    #[tokio::test]
    async fn test_health() {
        assert_health(test_server().await).await;
    }

    #[tokio::test]
    async fn test_metrics() {
        assert_metrics(test_server().await).await;
    }

    async fn test_server() -> TestServer<RouterServerType> {
        let common_state = CommonServerState::for_testing();
        let time_provider = Arc::new(SystemProvider::new());
        let server =
            Arc::new(RouterServer::new(None, common_state.trace_collector(), time_provider).await);
        let server_type = Arc::new(RouterServerType::new(server, &common_state));
        TestServer::new(server_type)
    }
}
