//! This crate provides an abstraction to construct (connected) tonic gRPC clients
//! out of connection addresses, generic on the type of the service.
//!
//! # Examples
//!
//! See [`CachingConnectionManager`].
//!

use cache_loader_async::backing::{CacheBacking, HashMapBacking, TtlCacheBacking};
use cache_loader_async::cache_api::{self, LoadingCache};
use futures::Future;
use observability_deps::tracing::debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A connection manager knows how to obtain a T client given a connection string.
#[tonic::async_trait]
pub trait ConnectionManager<T, E = Error> {
    async fn remote_server(&self, connect: String) -> Result<T, E>;
}

/// A Caching ConnectionManager implementation.
///
/// It caches connected gRPC clients of type T (not operations performed over the connection).
/// Each cache access returns a clone of the tonic gRPC client. Cloning clients is cheap
/// and allows them to communicate through the same channel, see
/// <https://docs.rs/tonic/0.4.2/tonic/client/index.html#concurrent-usage>
///
/// The `CachingConnectionManager` implements a blocking cache-loading mechanism, that is, it guarantees that once a
/// connection request for a given connection string is in flight, subsequent cache access requests
/// get enqueued and wait for the first connection to complete instead of spawning each an
/// outstanding connection request and thus suffer from the thundering herd problem.
///
/// It also supports an optional expiry mechanism based on TTL, see [`CachingConnectionManager::builder()`].
///
/// # Examples
///
/// ```rust
/// # use std::time::Duration;
/// # use grpc_router::connection_manager::{ConnectionManager, CachingConnectionManager};
/// #
/// # type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;
/// #
/// # #[derive(Clone)]
/// # struct FooClient;
/// # impl FooClient {
/// #     async fn connect(_: String) -> Result<FooClient, tonic::transport::Error> { Ok(Self{})}
/// #     async fn foo(&self) -> Result<()> { Ok(()) }
/// # }
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// #
/// let connection_manager = CachingConnectionManager::builder()
///     .with_ttl(Duration::from_secs(60))
///     .with_make_client(|dst| Box::pin(FooClient::connect(dst)))
///     .build();
/// let client: FooClient = connection_manager
///     .remote_server("http://localhost:1234".to_string())
///     .await?;
/// client.foo().await?;
/// #
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct CachingConnectionManager<T>
where
    T: Clone + Send + 'static,
{
    cache: LoadingCache<String, T, Arc<Error>>,
}

pub type ClientFuture<T> = Pin<Box<dyn Future<Output = Result<T, tonic::transport::Error>> + Send>>;

impl<T> CachingConnectionManager<T>
where
    T: Clone + Send + 'static,
{
    /// Returns a [`CachingConnectionManagerBuilder`] for configuring and building a [`Self`].
    pub fn builder() -> CachingConnectionManagerBuilder<T> {
        CachingConnectionManagerBuilder::new()
    }
}

#[tonic::async_trait]
impl<T> ConnectionManager<T> for CachingConnectionManager<T>
where
    T: Clone + Send + 'static,
{
    async fn remote_server(&self, connect: String) -> Result<T, Error> {
        let cached = self.cache.get_with_meta(connect.to_string()).await?;
        debug!(was_cached=%cached.cached, %connect, "getting remote connection");
        Ok(cached.result)
    }
}

/// Builder for [`CachingConnectionManager`].
///
/// Can be constructed with [`CachingConnectionManager::builder`].
///
/// A CachingConnectionManager can choose which backing store `B` to use.
/// We provide a helper method [`Self::with_ttl`] to use the [`TtlCacheBacking`] backing store
///
/// The `F` type parameter encodes the client constructor set with [`Self::with_make_client`].
/// By default the `F` parameter is `()` which causes the [`Self::build`] method to not be available.
#[derive(Debug)]
pub struct CachingConnectionManagerBuilder<T, B = HashMapBacking<String, CacheEntry<T>>, F = ()>
where
    T: Clone + Send + 'static,
{
    backing: B,
    make_client: F,
    _phantom: PhantomData<T>,
}

impl<T> Default for CachingConnectionManagerBuilder<T>
where
    T: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CachingConnectionManagerBuilder<T>
where
    T: Clone + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            backing: HashMapBacking::new(),
            make_client: (),
            _phantom: Default::default(),
        }
    }
}

type CacheEntry<T> = cache_api::CacheEntry<T, Arc<Error>>;

// This needs to be a separate impl block because they place different bounds on the type parameters.
impl<T, B, F> CachingConnectionManagerBuilder<T, B, F>
where
    T: Clone + Send + 'static,
    B: CacheBacking<String, CacheEntry<T>> + Send + 'static,
{
    /// Use a cache backing store that expires entries once they are older than `ttl`.
    pub fn with_ttl(
        self,
        ttl: Duration,
    ) -> CachingConnectionManagerBuilder<T, TtlCacheBacking<String, CacheEntry<T>>, F> {
        self.with_backing(TtlCacheBacking::new(ttl))
    }

    /// Use a custom cache backing store.
    pub fn with_backing<B2>(self, backing: B2) -> CachingConnectionManagerBuilder<T, B2, F>
    where
        B2: CacheBacking<String, CacheEntry<T>>,
    {
        CachingConnectionManagerBuilder {
            backing,
            make_client: self.make_client,
            _phantom: Default::default(),
        }
    }

    pub fn with_make_client<F2>(self, make_client: F2) -> CachingConnectionManagerBuilder<T, B, F2>
    where
        F2: Fn(String) -> ClientFuture<T> + Copy + Send + Sync + 'static,
    {
        CachingConnectionManagerBuilder {
            backing: self.backing,
            make_client,
            _phantom: Default::default(),
        }
    }
}

// This needs to be a separate impl block because they place different bounds on the type parameters.
impl<T, B, F> CachingConnectionManagerBuilder<T, B, F>
where
    T: Clone + Send + 'static,
    B: CacheBacking<String, CacheEntry<T>> + Send + 'static,
    F: Fn(String) -> ClientFuture<T> + Copy + Send + Sync + 'static,
{
    /// Builds a [`CachingConnectionManager`].
    pub fn build(self) -> CachingConnectionManager<T> {
        let make_client = self.make_client;
        let cache = LoadingCache::with_backing(self.backing, move |connect| async move {
            (make_client)(connect)
                .await
                .map_err(|e| Arc::new(Box::new(e) as _))
        });
        CachingConnectionManager { cache }
    }
}
