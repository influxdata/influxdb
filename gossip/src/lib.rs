//! A work-in-progress, simple gossip primitive for metadata distribution
//! between IOx nodes.
//!
//! # Transport
//!
//! Prefer small payloads where possible, and expect loss of some messages -
//! this primitive provides *best effort* delivery.
//!
//! This implementation sends unicast UDP frames between peers, with support for
//! both control frames & user payloads. The maximum message size is 65,507
//! bytes, but a packet this large is fragmented into smaller (at most
//! MTU-sized) packets and is at greater risk of being dropped due to a lost
//! fragment.
//!
//! # Security
//!
//! Messages exchanged between peers are unauthenticated and connectionless -
//! it's trivial to forge a message appearing to come from a different peer, or
//! include malicious payloads.
//!
//! The security model of this implementation expects the peers to be running in
//! a trusted environment, secure from malicious users.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    missing_docs
)]

mod builder;
mod dispatcher;
mod handle;
mod peers;
mod proto;
mod reactor;
pub(crate) mod seed;

use std::time::Duration;

/// Work around the unused_crate_dependencies false positives for test deps.
#[cfg(test)]
use test_helpers as _;

pub use builder::*;
pub use dispatcher::*;
pub use handle::*;

/// The maximum duration of time allotted to performing a DNS resolution against
/// a seed/peer address.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Defines the interval between PING frames sent to all configured seed peers.
const SEED_PING_INTERVAL: std::time::Duration = Duration::from_secs(15);
