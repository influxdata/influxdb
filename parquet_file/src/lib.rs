#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod catalog;
pub mod chunk;
pub mod metadata;
pub mod storage;
pub mod table;

mod storage_testing;
#[cfg(test)]
pub mod utils;
