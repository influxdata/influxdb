use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Future;
use hyper::HeaderMap;
use hyper::{body::HttpBody, Body, Request, Response};
use pin_project_lite::pin_project;
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub(crate) fn hybrid<MakeRest, Grpc>(
    make_rest: MakeRest,
    grpc: Grpc,
) -> HybridMakeService<MakeRest, Grpc> {
    HybridMakeService { make_rest, grpc }
}

pub struct HybridMakeService<MakeRest, Grpc> {
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
    pub struct HybridMakeServiceFuture<RestFuture, Grpc> {
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

pub struct HybridService<Rest, Grpc> {
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
