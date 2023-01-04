#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

//! Arrow Flight implementations
//!
//! The goal is to upstream much/all of this to back to arrow-rs, but
//! it is included in the IOx codebase initially for development
//! convenience (so we can develop directly on main without forks of
//! upstream repos)

pub mod error;
pub use error::FlightError;

mod client;
pub use client::{
    DecodedFlightData, DecodedPayload, FlightClient, FlightDataStream, FlightRecordBatchStream,
};

pub mod flightsql;
pub use flightsql::FlightSqlClient;

/// Reexport all of arrow_flight so this crate can masquarade as
/// `arrow-flight` in the IOx codebase (as the aim is to publish this
/// all upstream)
pub use arrow_flight::*;

/// Publically reexport prost used by flight
pub use prost;

/// Publically reexport tonic used by flight
pub use tonic;
