//! Gossip event dispatcher & handler implementations for routers.
//!
//! This sub-system is composed of the following primary components:
//!
//! * [`gossip`] crate: provides the gossip transport, the [`GossipHandle`], and
//!   the [`Dispatcher`]. This crate operates on raw bytes.
//!
//! * The outgoing [`SchemaChangeObserver`]: a router-specific wrapper over the
//!   underlying [`GossipHandle`]. This type translates the application calls
//!   into protobuf [`Msg`], and serialises them into bytes, sending them over
//!   the underlying [`gossip`] impl.
//!
//! * The incoming [`GossipMessageDispatcher`]: deserialises the incoming bytes
//!   from the gossip [`Dispatcher`] into [`Msg`] and passes them off to the
//!   [`NamespaceSchemaGossip`] implementation for processing.
//!
//! * The incoming [`NamespaceSchemaGossip`]: processes [`Msg`] received from
//!   peers, applying them to the local cache state if necessary.
//!
//! ```text
//!         ┌────────────────────────────────────────────────────┐
//!         │                   NamespaceCache                   │
//!         └────────────────────────────────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                   diff                        diff
//!                     │                           │
//!                     │              ┌─────────────────────────┐
//!                     │              │  NamespaceSchemaGossip  │
//!                     │              └─────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                     │     Application types     │
//!                     │                           │
//!                     ▼                           │
//!         ┌──────────────────────┐   ┌─────────────────────────┐
//!         │ SchemaChangeObserver │   │ GossipMessageDispatcher │
//!         └──────────────────────┘   └─────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                     │   Encoded Protobuf bytes  │
//!                     │                           │
//!                     │                           │
//!        ┌ Gossip  ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─
//!                     ▼                           │             │
//!        │    ┌──────────────┐          ┌──────────────────┐
//!             │ GossipHandle │          │    Dispatcher    │    │
//!        │    └──────────────┘          └──────────────────┘
//!                                                               │
//!        └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ```
//!
//! [`GossipHandle`]: gossip::GossipHandle
//! [`Dispatcher`]: gossip::Dispatcher
//! [`SchemaChangeObserver`]: schema_change_observer::SchemaChangeObserver
//! [`Msg`]: generated_types::influxdata::iox::gossip::v1::gossip_message::Msg
//! [`GossipMessageDispatcher`]: dispatcher::GossipMessageDispatcher
//! [`NamespaceSchemaGossip`]: namespace_cache::NamespaceSchemaGossip

pub mod dispatcher;
pub mod namespace_cache;
pub mod schema_change_observer;
pub mod traits;

#[cfg(test)]
mod mock_schema_broadcast;
