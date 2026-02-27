//! An InfluxDB IOx API client.

#![warn(missing_docs)]

#[cfg(all(not(feature = "format"), test))]
use insta as _;

pub use generated_types::{
    Code, IntoRequest, Request, Response, Status, google, metadata, protobuf_type_url,
    protobuf_type_url_eq, transport::Body,
};

pub use client::*;

pub use client_util::connection;
pub use client_util::namespace_translation;

#[cfg(feature = "format")]
/// Output formatting utilities
pub mod format;

mod client;
