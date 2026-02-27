//! Consistent cache system used by the catalog service
//!
//! # Design
//!
//! The catalog service needs to be able to service queries without needing to communicate
//! with its underlying backing store. This serves the dual purpose of reducing load on this
//! backing store, and also returning results in a more timely manner.
//!
//! This caching must be transparent to the users of the catalog service, and therefore must not
//! introduce eventually consistent behaviour, or other consistency effects.
//!
//! As such this crate provides a strongly-consistent, distributed key-value cache.
//!
//! In order to keep things simple, this only provides a mapping from [`CacheKey`] to opaque
//! binary payloads, with no support for structured payloads.
//!
//! This avoids:
//!
//! * Complex replicated state machines
//! * Forward compatibility challenges where newer data can't roundtrip through older servers
//!
//! While providing the following benefits:
//!
//! * Simple to introspect, debug and reason about
//! * Predictable and easily quantifiable memory usage
//!
//! However, it does have the following implications:
//!
//! * Care must be taken to ensure that parsing of the cached payloads does not become a bottleneck
//! * Large values (> 1MB) should be avoided, as updates will resend the entire value
//!
//! ## Components
//!
//! This crate is broken into multiple parts
//!
//! * [`CatalogCache`] provides a local key value store
//! * [`CatalogCacheService`] exposes this [`CatalogCache`] over an HTTP API
//! * [`CatalogCacheClient`] communicates with a remote [`CatalogCacheService`]
//! * [`QuorumCatalogCache`] combines the above into a strongly-consistent distributed cache
//!
//! [`CatalogCache`]: local::CatalogCache
//! [`CatalogCacheClient`]: api::client::CatalogCacheClient
//! [`CatalogCacheService`]: api::server::CatalogCacheService
//! [`QuorumCatalogCache`]: api::quorum::QuorumCatalogCache

#![warn(missing_docs)]

use std::sync::Arc;

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
#[cfg(test)]
use data_types as _;
use workspace_hack as _;

use bytes::Bytes;
use std::sync::atomic::AtomicBool;

pub mod api;
pub mod local;

/// The types of catalog cache key
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum CacheKey {
    /// Root key
    Root,
    /// A catalog namespace
    Namespace(i64),
    /// A catalog table
    Table(i64),
    /// A catalog partition
    Partition(i64),
}

impl CacheKey {
    /// Variant as string.
    ///
    /// This can be used for logging and metrics.
    pub fn variant(&self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Namespace(_) => "namespace",
            Self::Table(_) => "table",
            Self::Partition(_) => "partition",
        }
    }

    /// Untyped ID.
    pub fn id(&self) -> Option<i64> {
        match self {
            Self::Root => None,
            Self::Namespace(id) => Some(*id),
            Self::Table(id) => Some(*id),
            Self::Partition(id) => Some(*id),
        }
    }
}

/// A value stored in [`CatalogCache`](local::CatalogCache)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CacheValue {
    /// The data stored for this cache value. If `None`, the cache value
    /// is considered deleted and needs to be refreshed from the backing
    /// store.
    data: Option<Bytes>,
    /// The generation of this cache data
    generation: u64,
    /// The optional etag
    etag: Option<Arc<str>>,
}

impl CacheValue {
    /// Create a new [`CacheValue`] with the provided `data` and `generation`
    pub fn new(data: Bytes, generation: u64) -> Self {
        // HACK: `Bytes` is a view-based type and may reference and underlying larger buffer. Maybe that causes
        //        https://github.com/influxdata/influxdb_iox/issues/13765 . So we "unshare" the buffer by
        //        round-tripping it through an owned type.
        let data = Bytes::from(data.to_vec());

        Self {
            data: Some(data),
            generation,
            etag: None,
        }
    }

    /// Create a new [`CacheValue`] with no data and the provided
    /// `generation`.
    pub fn new_empty(generation: u64) -> Self {
        Self {
            data: None,
            generation,
            etag: None,
        }
    }

    /// Sets the etag
    pub fn with_etag(self, etag: impl Into<Arc<str>>) -> Self {
        Self {
            etag: Some(etag.into()),
            ..self
        }
    }

    /// Sets the etag
    pub fn with_etag_opt(self, etag: Option<Arc<str>>) -> Self {
        Self { etag, ..self }
    }

    /// The data stored for this cache
    #[inline]
    pub fn data(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    /// The size of the data stored for this cache value.
    #[inline]
    pub fn size(&self) -> usize {
        let Self {
            data,
            // generation is not heap-allocated
            generation: _,
            etag,
        } = self;

        data.as_ref().map_or(0, |d| d.len()) + etag.as_ref().map_or(0, |e| e.len())
    }

    /// The generation of this cache data
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The etag if any
    #[inline]
    pub fn etag(&self) -> Option<&Arc<str>> {
        self.etag.as_ref()
    }
}

/// Combines a [`CacheValue`] with an [`AtomicBool`] for the purposes of NRU-eviction
#[derive(Debug)]
struct CacheEntry {
    /// The value of this cache entry
    value: CacheValue,
    /// An atomic flag that is set to `true` by `CatalogCache::get` and
    /// cleared by `CatalogCache::evict_unused`
    used: AtomicBool,
}

impl From<CacheValue> for CacheEntry {
    fn from(value: CacheValue) -> Self {
        Self {
            value,
            // Values start used to prevent racing with `evict_unused`
            used: AtomicBool::new(true),
        }
    }
}
