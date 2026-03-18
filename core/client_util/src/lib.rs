//! Shared InfluxDB IOx API client functionality

#![warn(missing_docs)]

/// Builder for constructing connections for use with the various gRPC clients
pub mod connection;

/// Helper to set client headers.
pub mod tower;

/// Namespace <--> org/bucket utilities
pub mod namespace_translation;
