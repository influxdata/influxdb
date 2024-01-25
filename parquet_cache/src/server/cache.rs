use std::{pin::Pin, task::Poll};

use futures::{ready, Future};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use tokio::sync::OnceCell;
use tower::{Layer, Service};

use super::response::PinnedFuture;

pub type FinalResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response<Body>, super::error::Error>> + Send>>;

/// Cache Service
#[derive(Debug, Clone)]
pub struct CacheService<S: Clone> {
    inner: S,
    initialize_once: OnceCell<()>,
}

impl<S> CacheService<S>
where
    S: Service<Request<Body>, Future = PinnedFuture> + Clone + Send + Sync + 'static,
{
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            initialize_once: Default::default(),
        }
    }

    pub async fn prewarm(&mut self) -> Result<(), super::error::Error> {
        // TODO:
        // 0. (already done): LruCacheManager::new() => should have cache policy.
        // 1. (already done): Keyspace::poll_ready() => should have the keyspace.
        // 2. TODO(optional): may have persisted state from previous LruCacheManager, to reduce catalog load
        // 3. GET list of obj_keys from catalog.
        //      * Query limits based on cache policy.
        //      * Use slower prewarming, paginated catalog queries, prioritized cache insertion.
        // 4. for key in list => self.call(<`/write-hint` request for key>)
        //      * inner KeyspaceService will filter by key hash
        //      * inner DataService will filter by cache eviction policy
        //      * inner WriteService will handle write-back

        // 5. message to inner that prewarming is done.
        let req = Request::builder()
            .method(Method::PATCH)
            .uri("/warmed")
            .body(Body::empty())
            .expect("should create prewarm PATCH /warmed req");
        self.inner
            .call(req)
            .await
            .map_err(|e| super::error::Error::Warming(e.to_string()))?;

        Ok(())
    }
}

impl<S> Service<Request<Body>> for CacheService<S>
where
    S: Service<Request<Body>, Future = PinnedFuture> + Clone + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = super::error::Error;
    type Future = FinalResponseFuture;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // wait for inner service to receive requests
        let _ = ready!(self.inner.poll_ready(cx));

        // initialize once (which issues a request to inner service)
        let mut this = self.clone();
        Box::pin(async move {
            self.initialize_once
                .get_or_try_init(|| this.prewarm())
                .await
        })
        .as_mut()
        .poll(cx)
        .map_ok(|_| ())
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            match inner.call(req).await {
                Ok(resp) => match Response::builder().status(resp.code()).body(resp.into()) {
                    Ok(resp) => Ok(resp),
                    Err(e) => Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(e.to_string().into())
                        .expect("should build error response")),
                },
                Err(e) => Err(e),
            }
        })
    }
}

pub struct BuildCacheService;

impl<S> Layer<S> for BuildCacheService
where
    S: Service<Request<Body>, Future = PinnedFuture> + Clone + Send + Sync + 'static,
{
    type Service = CacheService<S>;

    fn layer(&self, service: S) -> Self::Service {
        CacheService::new(service)
    }
}
