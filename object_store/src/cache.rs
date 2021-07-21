//! This module contains a trait and implementation for caching object storage objects
//! in the local filesystem. In the case of the disk backed object store implementation,
//! it yields locations to its files for cache locations and no-ops any cache modifications.

use crate::path::Path;
use crate::ObjectStore;
use async_trait::async_trait;
use snafu::Snafu;
use std::sync::Arc;

/// Result for the cache
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for Cache related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("unable to evict '{}' from the local cache", name))]
    UnableToEvict { name: String },
}

/// Defines an LRU cache with local file locations for objects from object store.
#[async_trait]
pub trait Cache {
    /// Evicts an object from the local filesystem cache.
    fn evict(&self, path: &Path) -> Result<()>;

    /// Returns the local filesystem path for the given object. If it isn't present, this
    /// will get the object from object storage and write it to the local filesystem cache.
    /// If the cache is over its limit, it will evict other cached objects based on an LRU
    /// policy.
    async fn fs_path_or_cache(&self, path: &Path, store: Arc<ObjectStore>) -> Result<&str>;

    /// The size in bytes of all files in the cache.
    fn size(&self) -> u64;

    /// The user configured limit in bytes for all files in the cache.
    fn limit(&self) -> u64;
}

/// Implementation of the local file system cache that keeps the LRU stats and
/// performs any evictions to load new objects in.
#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct LocalFSCache {}

#[async_trait]
impl Cache for LocalFSCache {
    fn evict(&self, _path: &Path) -> Result<()> {
        todo!()
    }

    async fn fs_path_or_cache(&self, _path: &Path, _store: Arc<ObjectStore>) -> Result<&str> {
        todo!()
    }

    fn size(&self) -> u64 {
        todo!()
    }

    fn limit(&self) -> u64 {
        todo!()
    }
}
