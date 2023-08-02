//! A handle to publish gossip messages to other peers.

use crate::namespace_cache::ChangeStats;

/// Application-level event notifiers.
pub trait SchemaEventHandle {
    /// Observe a new schema diff.
    fn observe_diff(&self, c: &ChangeStats);
}

/// A handle to the [`gossip`] subsystem propagating schema change
/// notifications.
#[derive(Debug)]
pub struct Handle(gossip::GossipHandle);

impl SchemaEventHandle for Handle {
    fn observe_diff(&self, _c: &ChangeStats) {
        unreachable!()
    }
}
