//! IOx compactor implementation.
//!

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

pub mod compact;
pub mod garbage_collector;
pub mod handler;
pub mod query;
pub mod server;
pub mod utils;
