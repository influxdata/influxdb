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
pub mod ingester;
pub mod object_store;
pub mod querier;
pub mod run_config;
pub mod socket_addr;
pub mod write_buffer;
