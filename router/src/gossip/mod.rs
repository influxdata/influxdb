//! Gossip event dispatcher & handler implementations for routers.
//!
//! This sub-system is composed of the following primary components:
//!
//! * [`gossip`] crate: provides the gossip transport, the [`GossipHandle`], and
//!   the [`Dispatcher`]. This crate operates on raw bytes.
//!
//! * The [`SchemaChangeObserver`]: a router-specific wrapper over the underlying
//!   [`GossipHandle`]. This type translates the application calls into protobuf
//!   [`Msg`], and serialises them into bytes for the underlying [`gossip`]
//!   impl.
//!
//! * The [`GossipMessageDispatcher`]: deserialises the incoming bytes from the
//!   gossip [`Dispatcher`] into [`Msg`] and passes them off to the
//!   [`GossipMessageHandler`] implementation for processing.
//!
//! ```text
//!                   event                      handler
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
//! [`GossipMessageHandler`]: dispatcher::GossipMessageHandler

pub mod dispatcher;
pub mod namespace_cache;
pub mod schema_change_observer;
pub mod traits;

#[cfg(test)]
mod mock_schema_broadcast;
