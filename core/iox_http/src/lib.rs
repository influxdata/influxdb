//! `iox_http`
//!
//! Core crate for defining shared HTTP API functionality for services providing HTTP APIs.
//!
//! For lower-level, more general HTTP types and functions suitable for use in any HTTP-related
//! context, see the `iox_http_util` crate.

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod client;
pub use client::hyper0_client;
pub mod write;
