//! How to load new cache entries.
use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use std::future::Future;

pub mod metrics;

/// Loader for missing [`Cache`](crate::driver::Cache) entries.
#[async_trait]
pub trait Loader: std::fmt::Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Send + 'static;

    /// Cache value.
    type V: Send + 'static;

    /// Load value for given key.
    async fn load(&self, k: Self::K) -> Self::V;
}

/// Simple-to-use wrapper for async functions to act as a [`Loader`].
pub struct FunctionLoader<K, V> {
    loader: Box<dyn (Fn(K) -> BoxFuture<'static, V>) + Send + Sync>,
}

impl<K, V> FunctionLoader<K, V> {
    /// Create loader from function.
    pub fn new<T, F>(loader: T) -> Self
    where
        T: Fn(K) -> F + Send + Sync + 'static,
        F: Future<Output = V> + Send + 'static,
    {
        let loader = Box::new(move |k| loader(k).boxed());
        Self { loader }
    }
}

impl<K, V> std::fmt::Debug for FunctionLoader<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionLoader").finish_non_exhaustive()
    }
}

#[async_trait]
impl<K, V> Loader for FunctionLoader<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    type K = K;
    type V = V;

    async fn load(&self, k: Self::K) -> Self::V {
        (self.loader)(k).await
    }
}
