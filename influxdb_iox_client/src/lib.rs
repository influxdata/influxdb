//! An InfluxDB IOx API client.
#![deny(
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

pub use generated_types::{google, protobuf_type_url, protobuf_type_url_eq};

pub use client::*;

pub use client_util::connection;
pub use client_util::namespace_translation;

#[cfg(feature = "format")]
/// Output formatting utilities
pub mod format;

mod client;
