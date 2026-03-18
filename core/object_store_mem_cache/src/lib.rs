//! Memory cache implementation for [`ObjectStore`](object_store::ObjectStore)

#[cfg(test)]
use clap as _;
#[cfg(test)]
use rand as _;

pub mod buffer_channel;
pub mod cache_system;
pub mod object_store_cache_tests;
pub mod object_store_helpers;
pub mod store;

pub use store::{MemCacheObjectStore, MemCacheObjectStoreParams};
