//! IOx sharder implementation.
//!
//! Given a table and a namespace, assign a consistent shard from the set of shards.

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
#[cfg(test)]
use rand as _;
use workspace_hack as _;

mod r#trait;
pub use r#trait::*;

mod round_robin;
pub use round_robin::*;

mod jumphash;
pub use jumphash::*;

#[allow(missing_docs)]
pub mod mock;
