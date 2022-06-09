//! Top-level trait ([`Cache`]) that provides a fully functional cache.
//!
//! Caches usually combine a [backend](crate::backend) with a [loader](crate::loader). The easiest way to achieve that
//! is to use [`CacheDriver`](crate::cache::driver::CacheDriver). Caches might also wrap inner caches to provide certain
//! extra functionality like metrics.
use std::{fmt::Debug, hash::Hash};

use async_trait::async_trait;

pub mod driver;
pub mod metrics;

#[cfg(test)]
mod test_util;

/// Status of a [`Cache`] GET request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheGetStatus {
    /// The requested entry was present in the storage backend.
    Hit,

    /// The requested entry was NOT present in the storage backend and the loader had no previous query running.
    Miss,

    /// The requested entry was NOT present in the storage backend, but there was already a loader query running for
    /// this particular key.
    MissAlreadyLoading,
}

/// High-level cache implementation.
///
/// # Concurrency
///
/// Multiple cache requests for different keys can run at the same time. When data is requested for
/// the same key the underlying loader will only be polled once, even when the requests are made
/// while the loader is still running.
///
/// # Cancellation
///
/// Canceling a [`get`](Self::get) request will NOT cancel the underlying loader. The data will
/// still be cached.
///
/// # Panic
///
/// If the underlying loader panics, all currently running [`get`](Self::get) requests will panic.
/// The data will NOT be cached.
#[async_trait]
pub trait Cache: Debug + Send + Sync + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Debug + Ord + Send + 'static;

    /// Cache value.
    type V: Clone + Debug + Send + 'static;

    /// Extra data that is provided during loading but that is NOT part of the cache key.
    type Extra: Debug + Send + 'static;

    /// Get value from cache.
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend and no loader query is running yet.
    async fn get(&self, k: Self::K, extra: Self::Extra) -> Self::V {
        self.get_with_status(k, extra).await.0
    }

    /// Get value from cache and the status.
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend and no loader query is running yet.
    async fn get_with_status(&self, k: Self::K, extra: Self::Extra) -> (Self::V, CacheGetStatus);

    /// Side-load an entry into the cache.
    ///
    /// This will also complete a currently running request for this key.
    async fn set(&self, k: Self::K, v: Self::V);
}
