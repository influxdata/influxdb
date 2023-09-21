//! Code to convert between binary write format and [`mutable_batch::MutableBatch`]

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use data_types as _;
#[cfg(test)]
use mutable_batch_lp as _;
use workspace_hack as _;

pub mod decode;
pub mod encode;
