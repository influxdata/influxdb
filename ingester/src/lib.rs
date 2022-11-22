//! IOx ingester implementation.
//! Design doc: <https://docs.google.com/document/d/14NlzBiWwn0H37QxnE0k3ybTU58SKyUZmdgYpVw6az0Q/edit#>
//!

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

mod arcmap;
mod buffer_tree;
pub(crate) mod compact;
pub mod data;
mod deferred_load;
mod dml_sink;
pub mod handler;
mod job;
pub mod lifecycle;
mod poison;
pub mod querier_handler;
pub(crate) mod query_adaptor;
mod sequence_range;
pub mod server;
pub(crate) mod stream_handler;
#[cfg(test)]
pub(crate) mod test_util;
