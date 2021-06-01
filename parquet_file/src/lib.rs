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
pub mod cleanup;
pub mod metadata;
pub mod rebuild;
pub mod storage;
pub mod table;
pub mod test_utils;

mod storage_testing;
