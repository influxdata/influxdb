use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use iox_http_util::BoxError;
use observability_deps::tracing::error;
use tonic::{Code, Status};
use tower::Service;

use crate::http::{HttpApi, route_request};
use crate::is_grpc_request;

#[derive(Clone)]
pub(crate) struct UnifiedService<S> {
    http_api: Arc<HttpApi>,
    grpc_service: S,
    without_auth: bool,
    paths_without_authz: &'static Vec<&'static str>,
}

impl<S> UnifiedService<S> {
    pub(crate) fn new(
        http_api: Arc<HttpApi>,
        grpc_service: S,
        without_auth: bool,
        paths_without_authz: &'static Vec<&'static str>,
    ) -> Self {
        Self {
            http_api,
            grpc_service,
            without_auth,
            paths_without_authz,
        }
    }
}

impl<S, B> Service<Request<Incoming>> for UnifiedService<S>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
{
    type Response = Response<iox_http_util::ResponseBody>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        // Check if this is a gRPC request
        let is_grpc = is_grpc_request(&req);

        if is_grpc {
            let mut grpc_service = self.grpc_service.clone();
            Box::pin(async move {
                // Call the gRPC service
                let response = match grpc_service.call(req).await {
                    Ok(response) => response,
                    Err(_) => {
                        error!("gRPC service error occurred");

                        // Convert service errors to gRPC responses, this maintains
                        // the connection instead of dropping it
                        let status = Status::new(Code::Internal, "Service error");
                        let response = status.into_http();
                        let (parts, body) = response.into_parts();
                        let body = BodyExt::map_err(body, |err| err.into()).boxed_unsync();
                        return Ok(Response::from_parts(parts, body));
                    }
                };

                // Convert the grpc body to our unified body type
                let (parts, grpc_body) = response.into_parts();

                // Convert BoxBody to UnsyncBoxBody, need to use http_body_util to convert
                // the tonic BoxBody to our UnsyncBoxBody
                let body = BodyExt::map_err(grpc_body, |e| e.into()).boxed_unsync();

                Ok(Response::from_parts(parts, body))
            })
        } else {
            // Convert Incoming body to iox_http_util Request
            let http_api = Arc::clone(&self.http_api);
            let without_auth = self.without_auth;
            let paths_without_authz = self.paths_without_authz;

            Box::pin(async move {
                // Convert hyper::Request<Incoming> to iox_http_util::Request
                let (parts, body) = req.into_parts();

                // Collect the body
                let collected = match body.collect().await {
                    Ok(collected) => collected,
                    Err(e) => {
                        // Convert body collection error to HTTP response
                        return Ok(http::Response::builder()
                            .status(http::StatusCode::BAD_REQUEST)
                            .body(iox_http_util::bytes_to_response_body(format!(
                                "Failed to read request body: {e}"
                            )))
                            .unwrap());
                    }
                };
                let bytes = collected.to_bytes();

                // Create iox_http_util request body
                let iox_body = iox_http_util::bytes_to_request_body(bytes);
                let iox_request = iox_http_util::Request::from_parts(parts, iox_body);

                // NB: We haven't created another tower service to wrap the HTTP 1 handler function
                //     and then tried to multiplex with GRPC (HTTP 2). Instead the routing happens
                //     directly here
                //
                //     route_request returns Result<_, Infallible> so unwrap is
                //     safe
                let response =
                    route_request(http_api, iox_request, without_auth, paths_without_authz)
                        .await
                        .unwrap();

                Ok(response)
            })
        }
    }
}
