//! Building blocks for [`clap`]-driven configs.
//!
//! They can easily be re-used using `#[clap(flatten)]`.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]
pub mod catalog_dsn;
pub mod compactor;
pub mod compactor2;
pub mod garbage_collector;
pub mod ingest_replica;
pub mod ingester;
pub mod ingester2;
pub mod ingester_address;
pub mod object_store;
pub mod querier;
pub mod router;
pub mod router2;
pub mod run_config;
pub mod socket_addr;
pub mod write_buffer;
