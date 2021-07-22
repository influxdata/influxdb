#![deny(broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod access;
pub mod atomic_instant;
pub mod guard;
pub mod once;
pub mod schema;
pub mod selection;
