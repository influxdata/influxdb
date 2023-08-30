#![doc = include_str!("../README.md")]
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
#![allow(clippy::default_constructed_unit_structs)]

mod builder;
mod dispatcher;
mod handle;
mod metric;
mod peers;
mod proto;
mod reactor;
pub(crate) mod seed;
mod topic_set;

use std::time::Duration;

/// Work around the unused_crate_dependencies false positives for test deps.
#[cfg(test)]
use test_helpers as _;
use workspace_hack as _;

pub use builder::*;
pub use dispatcher::*;
pub use handle::*;
pub use peers::Identity;

/// The maximum duration of time allotted to performing a DNS resolution against
/// a seed/peer address.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Defines the interval between PING frames sent to all configured seed peers.
const SEED_PING_INTERVAL: Duration = Duration::from_secs(60);

/// How often a PING frame should be sent to a known peer.
///
/// This value and [`MAX_PING_UNACKED`] defines the approximate duration of time
/// until a dead peer is removed from the peer list.
const PEER_PING_INTERVAL: Duration = Duration::from_secs(30);

/// The maximum payload size allowed.
///
/// Attempting to send a serialised packet (inclusive of control frames/fields)
/// in excess of this amount will result in an error.
const MAX_FRAME_BYTES: usize = 1024 * 10;

/// The frame header overhead for user payloads.
const USER_PAYLOAD_OVERHEAD: usize = 33;

/// The maximum allowed byte size of user payloads.
///
/// Sending payloads of this size is discouraged as it leads to fragmentation of
/// the message and increases the chance of the message being undelivered /
/// dropped. Smaller is always better for UDP transports!
pub const MAX_USER_PAYLOAD_BYTES: usize = MAX_FRAME_BYTES - USER_PAYLOAD_OVERHEAD;

/// The number of PING messages sent to a peer without a response (of any kind)
/// before the peer is considered dead and removed from the peer list.
///
/// Increasing this value does not affect correctness - messages will be sent to
/// this peer for longer before being marked as dead, and the (small amount of)
/// RAM used by this peer will be held for longer.
///
/// This value should be large so that the occasional dropped frame does not
/// cause a peer to be spuriously marked as "dead" - doing so would cause it to
/// miss broadcast frames until it is re-discovered.
const MAX_PING_UNACKED: usize = 10;

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
            MAX_USER_PAYLOAD_BYTES, 10_207,
            "applications may depend on this value not changing"
        );
    }
}
