//! Client to query the ingester.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
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

pub mod composition;
pub mod error;
pub mod error_classifier;
pub mod interface;
pub mod layer;
pub mod layers;
pub mod testing;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;
