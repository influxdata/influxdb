//! Shared InfluxDB IOx API client functionality

#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

/// Builder for constructing connections for use with the various gRPC clients
pub mod connection;

/// Helper to set client headers.
pub mod tower;

/// Namespace <--> org/bucket utilities
pub mod namespace_translation;
