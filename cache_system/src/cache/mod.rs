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

/// Status of a [`Cache`] [GET](Cache::get_with_status) request.
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

impl CacheGetStatus {
    /// Get human and machine readable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::MissAlreadyLoading => "miss_already_loading",
        }
    }
}

/// Status of a [`Cache`] [PEEK](Cache::peek_with_status) request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePeekStatus {
    /// The requested entry was present in the storage backend.
    Hit,

    /// The requested entry was NOT present in the storage backend, but there was already a loader query running for
    /// this particular key.
    MissAlreadyLoading,
}

impl CachePeekStatus {
    /// Get human and machine redable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::MissAlreadyLoading => "miss_already_loading",
        }
    }
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

    /// Extra data that is provided during [`GET`](Self::get) but that is NOT part of the cache key.
    type GetExtra: Debug + Send + 'static;

    /// Extra data that is provided during [`PEEK`](Self::peek) but that is NOT part of the cache key.
    type PeekExtra: Debug + Send + 'static;

    /// Get value from cache.
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend and no loader query is running yet.
    async fn get(&self, k: Self::K, extra: Self::GetExtra) -> Self::V {
        self.get_with_status(k, extra).await.0
    }

    /// Get value from cache and the [status](CacheGetStatus).
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend and no loader query is running yet.
    async fn get_with_status(&self, k: Self::K, extra: Self::GetExtra)
        -> (Self::V, CacheGetStatus);

    /// Peek value from cache.
    ///
    /// In contrast to [`get`](Self::get) this will only return a value if there is a stored value or the value loading
    /// is already in progress. This will NOT start a new loading task.
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend and no loader query is running yet.
    async fn peek(&self, k: Self::K, extra: Self::PeekExtra) -> Option<Self::V> {
        self.peek_with_status(k, extra).await.map(|(v, _status)| v)
    }

    /// Peek value from cache and the [status](CachePeekStatus).
    ///
    /// In contrast to [`get_with_status`](Self::get_with_status) this will only return a value if there is a stored
    /// value or the value loading is already in progress. This will NOT start a new loading task.
    ///
    /// Note that `extra` is only used if the key is missing from the storage backend and no loader query is running yet.
    async fn peek_with_status(
        &self,
        k: Self::K,
        extra: Self::PeekExtra,
    ) -> Option<(Self::V, CachePeekStatus)>;

    /// Side-load an entry into the cache.
    ///
    /// This will also complete a currently running request for this key.
    async fn set(&self, k: Self::K, v: Self::V);
}

#[async_trait]
impl<K, V, GetExtra, PeekExtra> Cache
    for Box<dyn Cache<K = K, V = V, GetExtra = GetExtra, PeekExtra = PeekExtra>>
where
    K: Clone + Eq + Hash + Debug + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    GetExtra: Debug + Send + 'static,
    PeekExtra: Debug + Send + 'static,
{
    type K = K;
    type V = V;
    type GetExtra = GetExtra;
    type PeekExtra = PeekExtra;

    async fn get_with_status(
        &self,
        k: Self::K,
        extra: Self::GetExtra,
    ) -> (Self::V, CacheGetStatus) {
        self.as_ref().get_with_status(k, extra).await
    }

    async fn peek_with_status(
        &self,
        k: Self::K,
        extra: Self::PeekExtra,
    ) -> Option<(Self::V, CachePeekStatus)> {
        self.as_ref().peek_with_status(k, extra).await
    }

    async fn set(&self, k: Self::K, v: Self::V) {
        self.as_ref().set(k, v).await
    }
}
