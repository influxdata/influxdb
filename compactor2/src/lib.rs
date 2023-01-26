//! The compactor.
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

pub mod compactor;
mod components;
pub mod config;
mod driver;
mod error;
mod partition_info;

#[cfg(test)]
mod compactor_tests;

#[cfg(test)]
mod test_util;
