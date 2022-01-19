//! IOx ingester implementation.
//! Design doc: https://docs.google.com/document/d/14NlzBiWwn0H37QxnE0k3ybTU58SKyUZmdgYpVw6az0Q/edit#
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

#[allow(dead_code)]
pub mod data;
pub mod server;
