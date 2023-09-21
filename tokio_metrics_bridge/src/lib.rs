//! Integrates tokio runtime stats into the IOx metric system.
//!
//! This is NOT called `tokio-metrics` since this name is already taken.
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

#[cfg(not(tokio_unstable))]
mod not_tokio_unstable {
    use metric as _;
    use parking_lot as _;
    use tokio as _;
    use workspace_hack as _;
}

#[cfg(tokio_unstable)]
mod bridge;
#[cfg(tokio_unstable)]
pub use bridge::*;
