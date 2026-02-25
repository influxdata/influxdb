//! Memory cache implementation for [`ObjectStore`](object_store::ObjectStore)

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use clap as _;
#[cfg(test)]
use rand as _;
use workspace_hack as _;

pub mod buffer_channel;
pub mod cache_system;
pub mod object_store_cache_tests;
pub mod object_store_helpers;
pub mod store;

pub use store::{MemCacheObjectStore, MemCacheObjectStoreParams};
