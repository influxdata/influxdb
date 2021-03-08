//! An InfluxDB IOx API client.
#![deny(rust_2018_idioms, missing_debug_implementations, unreachable_pub)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr
)]
#![allow(clippy::missing_docs_in_private_items)]

pub use client::{health, management, write};

#[cfg(feature = "flight")]
pub use client::flight;

/// Builder for constructing connections for use with the various gRPC clients
pub mod connection;

mod client;
