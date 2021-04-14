//! An InfluxDB v2.0 API client.

#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod client;
pub mod common;
// pub use client::RequestError::*;
pub use client::*;

pub mod api;
pub mod models;
