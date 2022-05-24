/// Errors for the client
pub mod error;

/// Client for health checking API
pub mod health;

/// Client for delete API
pub mod delete;

/// Client for namespace API
pub mod namespace;

/// Client for schema API
pub mod schema;

/// Client for write API
pub mod write;

#[cfg(feature = "flight")]
/// Client for query API (based on Arrow flight)
pub mod flight;

/// Client for testing purposes.
pub mod test;

/// Client for fetching write info
pub mod write_info;

/// Client for interacting with a remote catalog
pub mod catalog;

/// Client for interacting with a remote object store
pub mod store;
