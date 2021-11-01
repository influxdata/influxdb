//! Contains an in memory mutable buffer that stores incoming data in
//! a structure that is designed to be quickly appended to as well as queried

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

// TODO: Remove chunk module
mod chunk;
pub use chunk::*;
