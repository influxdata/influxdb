use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response, Version, header};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use tower::Service;

use crate::errors::ServiceError;
use crate::http::{HttpApi, route_request};
use crate::unified_service::body::UnifiedBody;

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
    type Response = Response<UnifiedBody>;
    type Error = ServiceError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.grpc_service
            .poll_ready(cx)
            .map_err(|e| ServiceError(e.into().to_string()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        // Check if this is a gRPC request
        let is_grpc = req.version() == Version::HTTP_2
            && req
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|ct| ct.to_str().ok())
                .map(|ct| ct.starts_with("application/grpc"))
                .unwrap_or(false);

        if is_grpc {
            let mut grpc_service = self.grpc_service.clone();
            Box::pin(async move {
                let response = grpc_service
                    .call(req)
                    .await
                    .map_err(|e| ServiceError(e.into().to_string()))?;

                // Convert the grpc body to our unified body type
                let (parts, grpc_body) = response.into_parts();

                // Convert BoxBody to UnsyncBoxBody, need to use http_body_util to convert
                // the tonic BoxBody to our UnsyncBoxBody
                let body = BodyExt::map_err(grpc_body, |e| ServiceError(e.into().to_string()))
                    .boxed_unsync();
                let unified_body = UnifiedBody::Grpc { body };

                Ok(Response::from_parts(parts, unified_body))
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
                let collected = body
                    .collect()
                    .await
                    .map_err(|e| ServiceError(e.to_string()))?;
                let bytes = collected.to_bytes();

                // Create iox_http_util request body
                let iox_body = iox_http_util::bytes_to_request_body(bytes);
                let iox_request = iox_http_util::Request::from_parts(parts, iox_body);

                // Route the request
                let response =
                    route_request(http_api, iox_request, without_auth, paths_without_authz)
                        .await
                        .map_err(|e| ServiceError(e.to_string()))?;

                Ok(response.map(|body| UnifiedBody::Http { body }))
            })
        }
    }
}
