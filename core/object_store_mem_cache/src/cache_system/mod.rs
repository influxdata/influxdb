//! Cache System.
//!
//! # Design
//! There are the following components:
//!
//! - [`Cache`]: The actual cache that maps keys to [shared futures] that return results.
//! - [`Hook`]: Can react to state changes of the cache state. Implements logging but also size limitations.
//! - [`Reactor`]: Drives pruning / garbage-collection decisions of the [`Cache`]. Implemented as background task so
//!   users don't need to drive that themselves.
//!
//! ```text
//!    +-------+                         +------+
//!    | Cache |----(informs & asks)---->| Hook |
//!    +-------+                         +------+
//!        ^                                |
//!        |                                |
//!        |                                |
//! (drives pruning)                        |
//!        |                                |
//!        |                                |
//!        |                                |
//!   +---------+                           |
//!   | Reactor |<-------(informs)----------+
//!   +---------+
//! ```
//!
//!
//! [`Cache`]: self::Cache
//! [`Hook`]: self::hook::Hook
//! [`Reactor`]: self::reactor::Reactor
//! [shared futures]: futures::future::Shared
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::{fmt::Debug, hash::Hash};

use std::sync::Arc;

/// Provides size estimations for an immutable object.
pub trait HasSize {
    /// Size in bytes.
    fn size(&self) -> usize;
}

impl HasSize for &'static str {
    fn size(&self) -> usize {
        // no dynamic allocation
        0
    }
}

impl HasSize for str {
    fn size(&self) -> usize {
        self.len()
    }
}

impl HasSize for data_types::ObjectStoreId {
    fn size(&self) -> usize {
        // no dynamic allocation
        0
    }
}

impl HasSize for object_store::path::Path {
    fn size(&self) -> usize {
        self.as_ref().len()
    }
}

impl HasSize for Bytes {
    fn size(&self) -> usize {
        self.len()
    }
}

impl<T> HasSize for Arc<T>
where
    T: HasSize + ?Sized,
{
    fn size(&self) -> usize {
        std::mem::size_of_val(self.as_ref()) + self.as_ref().size()
    }
}

impl HasSize for () {
    fn size(&self) -> usize {
        0
    }
}

/// Dynamic error type.
pub type DynError = Arc<dyn std::error::Error + Send + Sync>;

impl HasSize for DynError {
    fn size(&self) -> usize {
        self.to_string().len()
    }
}

/// Trait for checking if a value is currently in use.
///
/// This is useful for determining if a cache entry can be
/// safely evicted, or if it is still needed.
#[async_trait]
pub trait InUse {
    /// Check if a value is currently in use.
    ///
    /// This accepts a mutable reference so one can use [`Arc::get_mut`] which is thread synchronized (in constrast to
    /// [`Arc::strong_count`], see <https://github.com/rust-lang/rust/issues/117485>).
    fn in_use(&mut self) -> bool;
}

#[async_trait]
impl<T> InUse for Arc<T>
where
    T: InUse + ?Sized,
{
    /// For arc'ed references (e.g. `Arc<V>` or nested arc'ed values within V),
    /// check the strong count and the inner usage,
    /// since an inner Arc may still be held.
    fn in_use(&mut self) -> bool {
        // do NOT use `strong_count` as it's ordering is implemented as "relaxed", see
        // https://github.com/rust-lang/rust/issues/117485
        Self::get_mut(self)
            .map(|inner| inner.in_use())
            .unwrap_or(true)
    }
}

impl InUse for str {
    fn in_use(&mut self) -> bool {
        false
    }
}

impl InUse for &str {
    fn in_use(&mut self) -> bool {
        false
    }
}

impl InUse for Bytes {
    fn in_use(&mut self) -> bool {
        !self.is_unique()
    }
}

/// Result type for cache requests.
///
/// The value (`V`) must be cloneable, so that they can be shared between multiple consumers of the cache.
pub type CacheRequestResult<V> = Result<V, DynError>;

impl<T> HasSize for CacheRequestResult<T>
where
    T: HasSize,
{
    fn size(&self) -> usize {
        match self {
            Ok(o) => o.size(),
            Err(e) => e.size(),
        }
    }
}

/// Type of the function that is used to fetch a value for a key.
type CacheFn<V> = Box<dyn FnOnce() -> BoxFuture<'static, CacheRequestResult<V>> + Send>;

/// Trait for asynchronously dropping a value.
pub trait AsyncDrop: Send + Sync {
    /// Return a future which drops a value.
    fn async_drop(self) -> impl Future<Output = ()> + Send;
}

impl<T> AsyncDrop for Arc<T>
where
    T: AsyncDrop,
{
    /// Drop an `Arc<T>`.
    ///
    /// Note that if this reference is still used elsewhere,
    /// it will not drop the inner value. This is expected behavior
    /// and can be compensated by checking the [`InUse`] status
    /// of items prior to calling async drop.
    async fn async_drop(self) {
        // Try to extract the inner value if we're the only owner,
        // otherwise we can't drop the inner value asynchronously
        if let Ok(inner) = Self::try_unwrap(self) {
            inner.async_drop().await
        } else {
            unreachable!(
                "InUse check should have already been performed for {}",
                std::any::type_name::<T>(),
            );
        }
    }
}

impl<T> AsyncDrop for Vec<T>
where
    T: AsyncDrop,
{
    /// Drop all values in the vector.
    async fn async_drop(self)
    where
        Self: Sized,
    {
        let mut unordered = self
            .into_iter()
            .map(|v| T::async_drop(v))
            .collect::<FuturesUnordered<_>>();

        while unordered.next().await.is_some() {}
    }
}

impl AsyncDrop for Arc<str> {
    async fn async_drop(self) {}
}

impl AsyncDrop for &str {
    async fn async_drop(self) {}
}

impl AsyncDrop for Bytes {
    async fn async_drop(self) {
        drop(self);
    }
}

/// Re-export for callers of [`Cache::get_or_fetch`].
pub use object_store_metrics::cache_state::{CacheState, CacheStateKind};

#[async_trait]
pub trait Cache<K, V, D>: Send + Sync + Debug
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + HasSize + AsyncDrop + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Get an existing key or start a new fetch process.
    ///
    /// Fetching is driven by a background tokio task and will make progress even when you do not poll the resulting
    /// future.
    ///
    /// Returns a [`CacheState`] that contains either the cached value or a future that resolves to the value.
    /// If data is loading, the early access data (`D`) is included in the [`CacheState`].
    ///
    /// The early access data (`D`) may be used by metrics, logging, or other purposes.
    fn get_or_fetch(&self, k: &K, f: CacheFn<V>, d: D, size_hint: usize) -> CacheState<V, D>;

    /// Get the cached value and return `None` if was not cached.
    ///
    /// Entries that are currently being loaded also result in `None`.
    fn get(&self, k: &K) -> Option<CacheRequestResult<V>>;

    /// Get number of entries in the cache.
    fn len(&self) -> usize;

    /// Return true if the cache is empty
    fn is_empty(&self) -> bool;

    /// Prune entries that failed to fetch or that weren't used since the last [`prune`](Self::prune) call.
    fn prune(&self);
}

pub mod hook;
pub mod loader;
pub mod reactor;
pub mod s3_fifo_cache;
pub mod utils;

#[cfg(test)]
pub(crate) mod test_utils;
