//! An InfluxDB IOx API client.
#![deny(rust_2018_idioms, missing_debug_implementations, unreachable_pub)]
#![warn(missing_docs, clippy::todo, clippy::dbg_macro)]
#![allow(clippy::missing_docs_in_private_items)]

mod builder;
pub use builder::*;

mod client;
pub use client::*;

// can't combine these into one statement that uses `{}` because of this bug in
// the `unreachable_pub` lint: https://github.com/rust-lang/rust/issues/64762
#[cfg(feature = "flight")]
pub use client::FlightClient;
#[cfg(feature = "flight")]
pub use client::PerformQuery;

pub mod errors;
