//! Building blocks for [`clap`]-driven configs.
//!
//! They can easily be re-used using `#[clap(flatten)]`.
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

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod catalog_dsn;
pub mod compactor;
pub mod compactor_scheduler;
pub mod garbage_collector;
pub mod gossip;
pub mod ingester;
pub mod ingester_address;
pub mod object_store;
pub mod querier;
pub mod router;
pub mod run_config;
pub mod single_tenant;
pub mod socket_addr;
