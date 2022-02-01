//! Building blocks for [`clap`]-driven configs.
//!
//! They can easily be re-used using `#[clap(flatten)]`.
pub mod boolean_flag;
pub mod catalog_dsn;
pub mod object_store;
pub mod run_config;
pub mod server_id;
pub mod socket_addr;
pub mod write_buffer;
