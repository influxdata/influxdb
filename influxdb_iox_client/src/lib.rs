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
    clippy::future_not_send
)]
#![allow(clippy::missing_docs_in_private_items)]

pub use generated_types::{google, protobuf_type_url, protobuf_type_url_eq};

pub use client::*;

pub use client_util::connection;

#[cfg(feature = "format")]
/// Output formatting utilities
pub mod format;

mod client;

