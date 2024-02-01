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
//!
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use bytes::Bytes;
use std::sync::atomic::AtomicBool;

pub mod api;
pub mod local;

/// The types of catalog cache key
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum CacheKey {
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
            Self::Namespace(_) => "namespace",
            Self::Table(_) => "table",
            Self::Partition(_) => "partition",
        }
    }

    /// Untyped ID.
    pub fn id(&self) -> i64 {
        match self {
            Self::Namespace(id) => *id,
            Self::Table(id) => *id,
            Self::Partition(id) => *id,
        }
    }
}

/// A value stored in [`CatalogCache`](local::CatalogCache)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CacheValue {
    /// The data stored for this cache
    data: Bytes,
    /// The generation of this cache data
    generation: u64,
}

impl CacheValue {
    /// Create a new [`CacheValue`] with the provided `data` and `generation`
    pub fn new(data: Bytes, generation: u64) -> Self {
        Self { data, generation }
    }

    /// The data stored for this cache
    pub fn data(&self) -> &Bytes {
        &self.data
    }

    /// The generation of this cache data
    pub fn generation(&self) -> u64 {
        self.generation
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
