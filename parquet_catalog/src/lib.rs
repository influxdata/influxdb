#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod cleanup;
pub mod core;
pub mod dump;
pub mod interface;
mod internals;
pub mod prune;
pub mod rebuild;
pub mod test_helpers;

pub use crate::internals::proto_io::Error as ProtoIOError;
pub use crate::internals::proto_parse::Error as ProtoParseError;
