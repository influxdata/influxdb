/// Client for health checking API
pub mod health;

/// Client for management API
pub mod management;

/// Client for write API
pub mod write;

/// Client for long running operations API
pub mod operations;

#[cfg(feature = "flight")]
/// Client for query API (based on Arrow flight)
pub mod flight;
