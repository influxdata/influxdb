//! Building blocks for [`clap`]-driven configs.
//!
//! They can easily be re-used using `#[clap(flatten)]`.
pub mod catalog_dsn;
pub mod compactor;
pub mod ingester;
pub mod object_store;
pub mod run_config;
pub mod server_id;
pub mod socket_addr;
pub mod write_buffer;
