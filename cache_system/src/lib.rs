//! Flexible and modular cache system.
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

pub mod addressable_heap;
pub mod backend;
pub mod cache;
mod cancellation_safe_future;
pub mod loader;
pub mod resource_consumption;
