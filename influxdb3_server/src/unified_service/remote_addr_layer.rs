use std::net::SocketAddr;
use std::task::{Context, Poll};

use http::Request;
use tower::{Layer, Service};

/// A middleware layer that inserts the remote address into request extensions
#[derive(Clone)]
pub(crate) struct RemoteAddrLayer {
    remote_addr: Option<SocketAddr>,
}

impl RemoteAddrLayer {
    pub(crate) fn new(remote_addr: SocketAddr) -> Self {
        Self {
            remote_addr: Some(remote_addr),
        }
    }
}

impl<S> Layer<S> for RemoteAddrLayer {
    type Service = RemoteAddrService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RemoteAddrService {
            inner,
            remote_addr: self.remote_addr,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RemoteAddrService<S> {
    inner: S,
    remote_addr: Option<SocketAddr>,
}

impl<S, B> Service<Request<B>> for RemoteAddrService<S>
where
    S: Service<Request<B>>,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        // Insert the remote address into request extensions
        req.extensions_mut().insert(self.remote_addr);
        self.inner.call(req)
    }
}
