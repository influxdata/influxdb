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

    /// Extra data needed when loading a missing entry. Specify `()` if not needed.
    type Extra: Send + 'static;

    /// Cache value.
    type V: Send + 'static;

    /// Load value for given key, using the extra data if needed.
    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V;
}

/// Simple-to-use wrapper for async functions to act as a [`Loader`].
pub struct FunctionLoader<K, V, Extra> {
    loader: Box<dyn (Fn(K, Extra) -> BoxFuture<'static, V>) + Send + Sync>,
}

impl<K, V, Extra> FunctionLoader<K, V, Extra> {
    /// Create loader from function.
    pub fn new<T, F>(loader: T) -> Self
    where
        T: Fn(K, Extra) -> F + Send + Sync + 'static,
        F: Future<Output = V> + Send + 'static,
    {
        let loader = Box::new(move |k, extra| loader(k, extra).boxed());
        Self { loader }
    }
}

impl<K, V, Extra> std::fmt::Debug for FunctionLoader<K, V, Extra> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionLoader").finish_non_exhaustive()
    }
}

#[async_trait]
impl<K, V, Extra> Loader for FunctionLoader<K, V, Extra>
where
    K: Send + 'static,
    V: Send + 'static,
    Extra: Send + 'static,
{
    type K = K;
    type V = V;
    type Extra = Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        (self.loader)(k, extra).await
    }
}
