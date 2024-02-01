#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::ready;
use http::{Request, Response};
use http_body::SizeHint;
use parking_lot::Mutex;
use pin_project::pin_project;
use tower::{Layer, Service};

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

// re-export public types
pub use http::HeaderMap;

/// Layer that installs [`Trailers`] as a [request extension](Request::extensions).
#[derive(Debug, Clone, Default)]
#[allow(missing_copy_implementations)]
pub struct TrailerLayer;

impl<S> Layer<S> for TrailerLayer {
    type Service = TrailerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TrailerService { service }
    }
}

#[derive(Debug, Clone)]
pub struct TrailerService<S> {
    service: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TrailerService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: http_body::Body,
{
    type Response = Response<WrappedBody<ResBody>>;
    type Error = S::Error;
    type Future = WrappedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        let trailers = Trailers::new();
        let callbacks = trailers.callbacks.clone();
        let existing = request.extensions_mut().insert(trailers);
        assert!(
            existing.is_none(),
            "trailer layer/service installed multiple times"
        );

        WrappedFuture {
            callbacks,
            inner: self.service.call(request),
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct WrappedFuture<F> {
    callbacks: SharedCallbacks,
    #[pin]
    inner: F,
}

impl<F, ResBody, Error> Future for WrappedFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, Error>>,
    ResBody: http_body::Body,
{
    type Output = Result<Response<WrappedBody<ResBody>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result: Result<Response<ResBody>, Error> =
            ready!(self.as_mut().project().inner.poll(cx));

        match result {
            Ok(response) => Poll::Ready(Ok(response.map(|body| WrappedBody {
                callbacks: self.callbacks.clone(),
                inner: body,
            }))),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct WrappedBody<B> {
    callbacks: SharedCallbacks,
    #[pin]
    inner: B,
}

impl<B: http_body::Body> http_body::Body for WrappedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_data(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        self.as_mut().project().inner.poll_data(cx)
    }

    fn poll_trailers(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::header::HeaderMap>, Self::Error>> {
        let result: Result<Option<http::header::HeaderMap>, Self::Error> =
            ready!(self.as_mut().project().inner.poll_trailers(cx));

        let res = match result {
            Ok(trailers) => {
                let mut trailers = trailers.unwrap_or_default();

                for callback in self.callbacks.0.lock().iter() {
                    callback(&mut trailers);
                }

                Ok((!trailers.is_empty()).then_some(trailers))
            }
            Err(e) => Err(e),
        };
        Poll::Ready(res)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

type TrailerCallback = Box<dyn for<'a> Fn(&'a mut HeaderMap) + Send>;

#[derive(Clone, Default)]
struct SharedCallbacks(Arc<Mutex<Vec<TrailerCallback>>>);

impl std::fmt::Debug for SharedCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedCallbacks").field(&"...").finish()
    }
}

/// Handle to manage trailers of a HTTP response.
#[derive(Clone, Debug)]
pub struct Trailers {
    callbacks: SharedCallbacks,
}

impl Trailers {
    /// Private constructor.
    ///
    /// It is pointless / a potential bug to construct this type outside this crate, because it will NOT be hooked up
    /// into the layer.
    fn new() -> Self {
        Self {
            callbacks: Default::default(),
        }
    }

    /// Register callback that is called when the trailers are sent.
    pub fn add_callback<F>(&self, f: F)
    where
        for<'a> F: Fn(&'a mut HeaderMap) + Send + 'static,
    {
        let mut guard = self.callbacks.0.lock();
        guard.push(Box::new(f));
    }
}
