//! Abstractions decoupling application schema gossiping from the underlying
//! transport.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use observability_deps::tracing::error;

/// An abstract best-effort broadcast primitive, sending an opaque payload to
/// all peers.
#[async_trait]
pub trait SchemaBroadcast: Send + Sync + Debug {
    /// Broadcast `payload` to all peers, blocking until the message is enqueued
    /// for processing.
    async fn broadcast(&self, payload: Vec<u8>);
}

#[async_trait]
impl SchemaBroadcast for Arc<gossip::GossipHandle> {
    async fn broadcast(&self, payload: Vec<u8>) {
        if let Err(e) = gossip::GossipHandle::broadcast(self, payload).await {
            error!(error=%e, "failed to broadcast payload");
        }
    }
}
