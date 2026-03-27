//! Startup utilities for InfluxDB 3.
//!
//! Provides functionality needed during early startup, before the tracing
//! system is initialized:
//! - [`early_logging`]: Log-like output for pre-tracing startup messages
//! - [`env_compat`]: Backwards-compatible environment variable aliasing
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod early_logging;
pub mod env_compat;
