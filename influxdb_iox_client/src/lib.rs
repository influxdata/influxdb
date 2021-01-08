//! An IOx HTTP API client.
#![deny(rust_2018_idioms, missing_debug_implementations, unreachable_pub)]
#![warn(missing_docs, clippy::todo, clippy::dbg_macro)]
#![allow(clippy::missing_docs_in_private_items)]

mod builder;
pub use builder::*;

mod client;
pub use client::*;

pub mod errors;
