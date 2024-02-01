//! Crate for common types and utilities related to InfluxDB
//! query/statement parameters.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies,
    missing_debug_implementations,
    unreachable_pub
)]

mod params;

pub use params::*;

use workspace_hack as _;
