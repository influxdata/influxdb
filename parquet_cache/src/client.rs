//! Contains the cache client.

/// Interface for the object store. Consumed by Iox components.
pub mod object_store;
/// Interface for write hinting. Consumed by Iox components.
pub mod write_hints;

/// Connection to remote data cache. Used by the ObjectStore cache impl.
pub(crate) mod cache_connector;
pub(crate) mod http;
pub(crate) mod keyspace;
pub(crate) mod request;

/// Mocks used for internal testing
#[cfg(test)]
pub(crate) mod mock;
