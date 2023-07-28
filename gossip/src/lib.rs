//! A simple gossip & broadcast primitive for best-effort metadata distribution
//! between IOx nodes.
//!
//! # Peers
//!
//! Peers are uniquely identified by their self-reported "identity" UUID. A
//! unique UUID is generated for each gossip instance, ensuring the identity
//! changes across restarts of the underlying node.
//!
//! An identity is associated with an immutable socket address used for peer
//! communication.
//!
//! # Transport
//!
//! Prefer small payloads where possible, and expect loss of some messages -
//! this primitive provides *best effort* delivery.
//!
//! This implementation sends unicast UDP frames between peers, with support for
//! both control frames & user payloads. The maximum UDP message size is 65,507
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
//!
//! # Peer Exchange
//!
//! When a gossip instance is initialised, it advertises itself to the set of
//! user-provided "seed" peers - other gossip instances with fixed, known
//! addresses. The peer then bootstraps the peer list from these seed peers.
//!
//! Peers are discovered through PONG messages from peers, which contain the
//! list of peers the sender has successfully communicated with.
//!
//! On receipt of a PONG frame, a node will send PING frames to all newly
//! discovered peers without adding the peer to its local peer list. Once the
//! discovered peer responds with a PONG, the peer is added to the peer list.
//! This acts as a liveness check, ensuring a node only adds peers it can
//! communicate with to its peer list.
//!
//! ```text
//!                             ┌──────────┐
//!                             │   Seed   │
//!                             └──────────┘
//!                                 ▲   │
//!                                 │   │
//!                            (1)  │   │   (2)
//!                           PING  │   │  PONG
//!                                 │   │    (contains Peer A)
//!                                 │   ▼
//!                             ┌──────────┐
//!                             │  Local   │
//!                             └──────────┘
//!                                 ▲   │
//!                                 │   │
//!                            (4)  │   │   (3)
//!                           PONG  │   │  PING
//!                                 │   │
//!                                 │   ▼
//!                             ┌──────────┐
//!                             │  Peer A  │
//!                             └──────────┘
//! ```
//!
//! The above illustrates this process when the "local" node joins:
//!
//!   1. Send PING messages to all configured seeds
//!   2. Receive a PONG response containing the list of all known peers
//!   3. Send PING frames to all discovered peers - do not add to peer list
//!   4. Receive PONG frames from discovered peers - add to peer list
//!
//! The peer addresses sent during PEX rounds contain the advertised peer
//! identity and the socket address the PONG sender discovered.
//!
//! # Dead Peer Removal
//!
//! All peers are periodically sent a PING frame, and a per-peer counter is
//! incremented. If a message of any sort is received (including the PONG
//! response to the soliciting PING), the peer's counter is reset to 0.
//!
//! Once a peer's counter reaches [`MAX_PING_UNACKED`], indicating a number of
//! PINGs have been sent without receiving any response, the peer is removed
//! from the node's peer list.
//!
//! Dead peers age out of the cluster once all nodes perform the above routine.
//! If a peer dies, it is still sent in PONG messages as part of PEX until it is
//! removed from the sender's peer list, but the receiver of the PONG will not
//! add it to the node's peer list unless it successfully commutates, ensuring
//! dead peers are not propagated.
//!
//! Ageing out dead peers is strictly an optimisation (and not for correctness).
//! A dead peer consumes a tiny amount of RAM, but also will have frames
//! dispatched to it - over time, as the number of dead peers accumulates, this
//! would cause the number of UDP frames sent per broadcast to increase,
//! needlessly increasing gossip traffic.
//!
//! This process is heavily biased towards reliability/deliverability and is too
//! slow for use as a general peer health check.

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

use std::time::Duration;

/// Work around the unused_crate_dependencies false positives for test deps.
#[cfg(test)]
use test_helpers as _;
use workspace_hack as _;

pub use builder::*;
pub use dispatcher::*;
pub use handle::*;

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
const USER_PAYLOAD_OVERHEAD: usize = 22;

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
            MAX_USER_PAYLOAD_BYTES, 10_218,
            "applications may depend on this value not changing"
        );
    }
}
