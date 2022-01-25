//! IOx router role implementation.
//!
//! An IOx router is responsible for:
//!
//! * Creating IOx namespaces & synchronising them within the catalog.
//! * Handling writes:
//!     * Receiving IOx write/delete requests via HTTP and gRPC endpoints.
//!     * Enforcing schema validation & synchronising it within the catalog.
//!     * Applying sharding logic.
//!     * Push resulting operations into the appropriate kafka topics.
//!

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
    clippy::future_not_send
)]
#![allow(clippy::missing_docs_in_private_items)]

pub mod dml_handler;
pub mod sequencer;
pub mod server;
pub mod sharded_write_buffer;
pub mod sharder;
