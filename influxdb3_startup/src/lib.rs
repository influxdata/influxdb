//! Startup utilities for InfluxDB 3.
//!
//! Provides functionality needed during node startup:
//! - [`early_logging`]: Log-like output for pre-tracing startup messages
//! - [`env_compat`]: Backwards-compatible environment variable aliasing
//! - [`phase`]: Per-phase progress tracking for the serve path
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod early_logging;
pub mod env_compat;
pub mod phase;

pub use phase::{
    NoopStartupPhaseObserver, PhaseGuard, StartupPhase, StartupPhaseObserver, StartupPhases,
};
