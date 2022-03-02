//! IOx Query Server Implementation.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
#![allow(dead_code)]

pub use client_util::connection;

mod cache;
mod chunk;
mod database;
/// Flight client to the ingester to request in-memory data.
pub mod flight;
pub mod handler;
mod namespace;
mod poison;
pub mod server;

#[cfg(test)]
mod test_util;
