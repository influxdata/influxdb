//! Code for generating a hybrid REST/gRPC service
//!
//! This module houses the [`HybridService`] which is used to serve HTTP
//! traffic, for both a REST API and a gRPC API on the same port.
//!
//! This is accomplished via the [`hybrid`] method, which accepts a REST
//! service in the form of `hyper`'s [`MakeServiceFn`][make-svc-fn], as well
//! as a gRPC service from `tonic`, and produces a [`HybridMakeService`] that
//! can be served with `hyper`'s [`Server`][hyper-server].
//!
//! Recommended reading: [Combining Axum, Hyper, Tonic, and Tower for hybrid web/gRPC apps][article]
//!
//! [make-svc-fn]: https://docs.rs/hyper/0.14.28/src/hyper/service/make.rs.html#149-151
//! [hyper-server]: https://docs.rs/hyper/0.14.28/hyper/server/struct.Server.html
//! [article]: https://www.fpcomplete.com/blog/axum-hyper-tonic-tower-part1/
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Future;
use hyper::HeaderMap;
use hyper::{Body, Request, Response, body::HttpBody};
use pin_project_lite::pin_project;
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Generate a [`HybridMakeService`]
pub(crate) fn hybrid<MakeRest, Grpc>(
    make_rest: MakeRest,
    grpc: Grpc,
) -> HybridMakeService<MakeRest, Grpc> {
    HybridMakeService { make_rest, grpc }
}

/// A hybrid of a "make service", i.e., a service that accepts connection info and returns
/// a service that will serve a request over that connection as its output, and a gRPC service
///
/// Can be used with `hyper`'s [`Server`][hyper-server] to serve both kinds of requests
/// on a single port.
///
/// [hyper-server]: https://docs.rs/hyper/0.14.28/hyper/server/struct.Server.html
pub(crate) struct HybridMakeService<MakeRest, Grpc> {
    make_rest: MakeRest,
    grpc: Grpc,
}

impl<ConnInfo, MakeRest, Grpc> Service<ConnInfo> for HybridMakeService<MakeRest, Grpc>
where
    MakeRest: Service<ConnInfo>,
    Grpc: Clone,
{
    type Response = HybridService<MakeRest::Response, Grpc>;
    type Error = MakeRest::Error;
    type Future = HybridMakeServiceFuture<MakeRest::Future, Grpc>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.make_rest.poll_ready(cx)
    }

    fn call(&mut self, conn_info: ConnInfo) -> Self::Future {
        HybridMakeServiceFuture {
            rest_future: self.make_rest.call(conn_info),
            grpc: Some(self.grpc.clone()),
        }
    }
}

pin_project! {
    /// A future that builds a new `Service` for serving REST requests or gRPC requests
    pub(crate) struct HybridMakeServiceFuture<RestFuture, Grpc> {
        #[pin]
        rest_future: RestFuture,
        grpc: Option<Grpc>,
    }
}

impl<RestFuture, Rest, RestError, Grpc> Future for HybridMakeServiceFuture<RestFuture, Grpc>
where
    RestFuture: Future<Output = Result<Rest, RestError>>,
{
    type Output = Result<HybridService<Rest, Grpc>, RestError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.rest_future.poll(cx) {
            Poll::Ready(Ok(rest)) => Poll::Ready(Ok(HybridService {
                rest,
                grpc: this.grpc.take().expect("future polled after execution"),
            })),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The service that can serve both gRPC and REST HTTP Requests
pub(crate) struct HybridService<Rest, Grpc> {
    rest: Rest,
    grpc: Grpc,
}

impl<Rest, Grpc, RestBody, GrpcBody> Service<Request<Body>> for HybridService<Rest, Grpc>
where
    Rest: Service<Request<Body>, Response = Response<RestBody>>,
    Grpc: Service<Request<Body>, Response = Response<GrpcBody>>,
    Rest::Error: Into<BoxError>,
    Grpc::Error: Into<BoxError>,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = BoxError;
    type Future = HybridFuture<Rest::Future, Grpc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.rest.poll_ready(cx) {
            Poll::Ready(Ok(())) => match self.grpc.poll_ready(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
                Poll::Pending => Poll::Pending,
            },
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    /// When calling the service, gRPC is served if the HTTP request version is HTTP/2
    /// and if the Content-Type is "application/grpc"; otherwise, the request is served
    /// as a REST request
    fn call(&mut self, req: Request<Body>) -> Self::Future {
        match (
            req.version(),
            req.headers().get(hyper::header::CONTENT_TYPE),
        ) {
            (hyper::Version::HTTP_2, Some(hv))
                if hv.as_bytes().starts_with(b"application/grpc") =>
            {
                HybridFuture::Grpc {
                    grpc_future: self.grpc.call(req),
                }
            }
            _ => HybridFuture::Rest {
                rest_future: self.rest.call(req),
            },
        }
    }
}

pin_project! {
    /// A hybrid HTTP body that will be used in the response type for the
    /// [`HybridFuture`], i.e., the output of the [`HybridService`]
    #[project = HybridBodyProj]
    pub enum HybridBody<RestBody, GrpcBody> {
        Rest {
            #[pin]
            rest_body: RestBody
        },
        Grpc {
            #[pin]
            grpc_body: GrpcBody
        },
    }
}

impl<RestBody, GrpcBody> HttpBody for HybridBody<RestBody, GrpcBody>
where
    RestBody: HttpBody + Send + Unpin,
    GrpcBody: HttpBody<Data = RestBody::Data> + Send + Unpin,
    RestBody::Error: Into<BoxError>,
    GrpcBody::Error: Into<BoxError>,
{
    type Data = RestBody::Data;
    type Error = BoxError;

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Rest { rest_body } => rest_body.is_end_stream(),
            Self::Grpc { grpc_body } => grpc_body.is_end_stream(),
        }
    }

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        match self.project() {
            HybridBodyProj::Rest { rest_body } => rest_body.poll_data(cx).map_err(Into::into),
            HybridBodyProj::Grpc { grpc_body } => grpc_body.poll_data(cx).map_err(Into::into),
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        match self.project() {
            HybridBodyProj::Rest { rest_body } => rest_body.poll_trailers(cx).map_err(Into::into),
            HybridBodyProj::Grpc { grpc_body } => grpc_body.poll_trailers(cx).map_err(Into::into),
        }
    }
}

pin_project! {
    /// A future that accepts an HTTP request as input and returns an HTTP
    /// response as output for the [`HybridService`]
    #[project = HybridFutureProj]
    pub enum HybridFuture<RestFuture, GrpcFuture> {
        Rest {
            #[pin]
            rest_future: RestFuture,
        },
        Grpc {
            #[pin]
            grpc_future: GrpcFuture,
        }
    }
}

impl<RestFuture, GrpcFuture, RestBody, GrpcBody, RestError, GrpcError> Future
    for HybridFuture<RestFuture, GrpcFuture>
where
    RestFuture: Future<Output = Result<Response<RestBody>, RestError>>,
    GrpcFuture: Future<Output = Result<Response<GrpcBody>, GrpcError>>,
    RestError: Into<BoxError>,
    GrpcError: Into<BoxError>,
{
    type Output = Result<Response<HybridBody<RestBody, GrpcBody>>, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            HybridFutureProj::Rest { rest_future } => match rest_future.poll(cx) {
                Poll::Ready(Ok(res)) => {
                    Poll::Ready(Ok(res.map(|rest_body| HybridBody::Rest { rest_body })))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
                Poll::Pending => Poll::Pending,
            },
            HybridFutureProj::Grpc { grpc_future } => match grpc_future.poll(cx) {
                Poll::Ready(Ok(res)) => {
                    Poll::Ready(Ok(res.map(|grpc_body| HybridBody::Grpc { grpc_body })))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
