//! Abstractions decoupling application schema gossiping from the underlying
//! transport.

use std::fmt::Debug;

use generated_types::influxdata::iox::gossip::v1::schema_message::Event;
use gossip_schema::handle::SchemaTx;

/// An abstract best-effort broadcast primitive, sending an opaque payload to
/// all peers.
pub trait SchemaBroadcast: Send + Sync + Debug {
    /// Broadcast `payload` to all peers, blocking until the message is enqueued
    /// for processing.
    fn broadcast(&self, payload: Event);
}

impl SchemaBroadcast for SchemaTx {
    fn broadcast(&self, payload: Event) {
        SchemaTx::broadcast(self, payload)
    }
}
