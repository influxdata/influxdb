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
//! bytes ([`MAX_USER_PAYLOAD_BYTES`] for application-level payloads), but a
//! packet this large is fragmented into smaller (at most MTU-sized) packets and
//! is at greater risk of being dropped due to a lost fragment.
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
mod metric;
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

/// The maximum payload size allowed.
///
/// Attempting to send a serialised packet (inclusive of control frames/fields)
/// in excess of this amount will result in an error.
const MAX_FRAME_BYTES: usize = 1024 * 10;

/// The frame header overhead for user payloads.
const USER_PAYLOAD_OVERHEAD: usize = 22;

/// The maximum allowed byte size of user payloads.
///
/// Sending payloads of this size is discouraged as it leads to fragmentation of
/// the message and increases the chance of the message being undelivered /
/// dropped. Smaller is always better for UDP transports!
pub const MAX_USER_PAYLOAD_BYTES: usize = MAX_FRAME_BYTES - USER_PAYLOAD_OVERHEAD;

#[cfg(test)]
#[allow(clippy::assertions_on_constants)]
mod tests {
    use super::*;

    #[test]
    fn test_max_msg_size() {
        assert!(MAX_FRAME_BYTES < 65_536, "cannot exceed UDP maximum");
    }

    #[test]
    fn test_max_user_payload_size() {
        assert_eq!(
            MAX_USER_PAYLOAD_BYTES, 10_218,
            "applications may depend on this value not changing"
        );
    }
}
