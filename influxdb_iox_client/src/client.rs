/// Client for the gRPC health checking API
pub mod health;

/// Client for the management API
pub mod management;

#[cfg(feature = "flight")]
/// Client for the flight API
pub mod flight;
