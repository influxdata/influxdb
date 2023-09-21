//! How to load new cache entries.
use async_trait::async_trait;
use std::{fmt::Debug, future::Future, hash::Hash, marker::PhantomData, sync::Arc};

pub mod batch;
pub mod metrics;

#[cfg(test)]
pub(crate) mod test_util;

/// Loader for missing [`Cache`](crate::cache::Cache) entries.
#[async_trait]
pub trait Loader: std::fmt::Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Debug + Hash + Send + 'static;

    /// Extra data needed when loading a missing entry. Specify `()` if not needed.
    type Extra: Debug + Send + 'static;

    /// Cache value.
    type V: Debug + Send + 'static;

    /// Load value for given key, using the extra data if needed.
    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V;
}

#[async_trait]
impl<K, V, Extra> Loader for Box<dyn Loader<K = K, V = V, Extra = Extra>>
where
    K: Debug + Hash + Send + 'static,
    V: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    type K = K;
    type V = V;
    type Extra = Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        self.as_ref().load(k, extra).await
    }
}

#[async_trait]
impl<K, V, Extra, L> Loader for Arc<L>
where
    K: Debug + Hash + Send + 'static,
    V: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
    L: Loader<K = K, V = V, Extra = Extra>,
{
    type K = K;
    type V = V;
    type Extra = Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        self.as_ref().load(k, extra).await
    }
}

/// Simple-to-use wrapper for async functions to act as a [`Loader`].
///
/// # Typing
/// Semantically this wrapper has only one degree of freedom: `T`, which is the async loader function. However until
/// [`fn_traits`] are stable, there is no way to extract the parameters and return value from a function via associated
/// types. So we need to add additional type parametes for the special `Fn(...) -> ...` handling.
///
/// It is likely that `T` will be a closure, e.g.:
///
/// ```
/// use cache_system::loader::FunctionLoader;
///
/// let my_loader = FunctionLoader::new(|k: u8, _extra: ()| async move {
///     format!("{k}")
/// });
/// ```
///
/// There is no way to spell out the exact type of `my_loader` in the above example, because  the closure has an
/// anonymous type. If you need the type signature of [`FunctionLoader`], you have to
/// [erase the type](https://en.wikipedia.org/wiki/Type_erasure) by putting the [`FunctionLoader`] it into a [`Box`],
/// e.g.:
///
/// ```
/// use cache_system::loader::{Loader, FunctionLoader};
///
/// let my_loader = FunctionLoader::new(|k: u8, _extra: ()| async move {
///     format!("{k}")
/// });
/// let m_loader: Box<dyn Loader<K = u8, V = String, Extra = ()>> = Box::new(my_loader);
/// ```
///
///
/// [`fn_traits`]: https://doc.rust-lang.org/beta/unstable-book/library-features/fn-traits.html
pub struct FunctionLoader<T, F, K, Extra>
where
    T: Fn(K, Extra) -> F + Send + Sync + 'static,
    F: Future + Send + 'static,
    K: Debug + Send + 'static,
    F::Output: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    loader: T,
    _phantom: PhantomData<dyn Fn() -> (F, K, Extra) + Send + Sync + 'static>,
}

impl<T, F, K, Extra> FunctionLoader<T, F, K, Extra>
where
    T: Fn(K, Extra) -> F + Send + Sync + 'static,
    F: Future + Send + 'static,
    K: Debug + Send + 'static,
    F::Output: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    /// Create loader from function.
    pub fn new(loader: T) -> Self {
        Self {
            loader,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, K, Extra> std::fmt::Debug for FunctionLoader<T, F, K, Extra>
where
    T: Fn(K, Extra) -> F + Send + Sync + 'static,
    F: Future + Send + 'static,
    K: Debug + Send + 'static,
    F::Output: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionLoader").finish_non_exhaustive()
    }
}

#[async_trait]
impl<T, F, K, Extra> Loader for FunctionLoader<T, F, K, Extra>
where
    T: Fn(K, Extra) -> F + Send + Sync + 'static,
    F: Future + Send + 'static,
    K: Debug + Hash + Send + 'static,
    F::Output: Debug + Send + 'static,
    Extra: Debug + Send + 'static,
{
    type K = K;
    type V = F::Output;
    type Extra = Extra;

    async fn load(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        (self.loader)(k, extra).await
    }
}
