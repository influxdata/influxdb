//! DataFusion User Defined Functions (UDF/ UDAF) for IOx
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
pub mod group_by;

/// Regular Expressions
pub mod regex;

/// Flux selector expressions
pub mod selectors;

/// Time window and groupin
pub mod window;
