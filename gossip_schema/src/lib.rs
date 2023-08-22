//! Schema [gossip] event dispatcher & handler implementations.
//!
//! This sub-system is composed of the following primary components:
//!
//! * [`gossip`] crate: provides the gossip transport, the [`GossipHandle`], and
//!   the [`Dispatcher`]. This crate operates on raw bytes.
//!
//! * The outgoing [`SchemaTx`]: a schema-specific wrapper over the underlying
//!   [`GossipHandle`]. This type translates the protobuf [`Event`] from the
//!   application layer into raw serialised bytes, sending them over the
//!   underlying [`gossip`] impl.
//!
//! * The incoming [`SchemaRx`]: deserialises the incoming bytes from the gossip
//!   [`Dispatcher`] into [`Event`] and passes them off to the
//!   [`SchemaEventHandler`] implementation for processing.
//!
//! Users of this crate should implement the [`SchemaEventHandler`] trait to
//! receive change events, and push events into the [`SchemaTx`] to broadcast
//! changes to peers.
//!
//! ```text
//!          ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                                                               │
//!          │                    Application
//!                                                               │
//!          └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                      │                           ▲
//!                      │                           │
//!                      │                           │
//!                      │       Schema Event        │
//!                      │                           │
//!                      ▼                           │
//!          ┌──────────────────────┐   ┌─────────────────────────┐
//!          │       SchemaTx       │   │        SchemaRx         │
//!          └──────────────────────┘   └─────────────────────────┘
//!                      │                           ▲
//!                      │                           │
//!                      │       Encoded bytes       │
//!                      │                           │
//!                      │                           │
//!         ┌ Gossip  ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─
//!                      ▼                           │             │
//!         │    ┌──────────────┐          ┌──────────────────┐
//!              │ GossipHandle │          │    Dispatcher    │    │
//!         │    └──────────────┘          └──────────────────┘
//!                                                                │
//!         └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ```
//!
//! # Operation-Based CRDT / Differentials
//!
//! The messages exchanged between peers are schema change differentials
//! observed by the sender - the messages form the update messages of an
//! operation-based CRDT, as the schemas are additive only (append-only sets).
//!
//! # Best Effort
//!
//! This underlying gossip subsystem is designed to provide best effort delivery
//! of messages, and therefore best-effort delivery of schema change events,
//! without any ordering or delivery guarantees.
//!
//! This crate does NOT provide any eventual consistency guarantees of schema
//! data.
//!
//! [`GossipHandle`]: gossip::GossipHandle
//! [`Dispatcher`]: gossip::Dispatcher
//! [`SchemaTx`]: handle::SchemaTx
//! [`Event`]:
//!     generated_types::influxdata::iox::gossip::v1::schema_message::Event
//! [`SchemaRx`]: dispatcher::SchemaRx
//! [`SchemaEventHandler`]: dispatcher::SchemaEventHandler

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    missing_docs
)]
#![allow(clippy::default_constructed_unit_structs)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod dispatcher;
pub mod handle;

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use data_types::partition_template::{
        test_table_partition_override, NamespacePartitionTemplateOverride, PARTITION_BY_DAY_PROTO,
    };
    use generated_types::influxdata::iox::gossip::v1::{
        schema_message::Event, Column as GossipColumn, NamespaceCreated, TableCreated, TableUpdated,
    };
    use gossip::Builder;
    use test_helpers::{maybe_start_logging, timeout::FutureTimeout};
    use tokio::{net::UdpSocket, sync::mpsc};

    use crate::{
        dispatcher::{SchemaEventHandler, SchemaRx},
        handle::SchemaTx,
    };

    /// Bind a UDP socket on a random port and return it alongside the socket
    /// address.
    async fn random_udp() -> (UdpSocket, SocketAddr) {
        // Bind a UDP socket to a random port
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("failed to bind UDP socket");
        let addr = socket.local_addr().expect("failed to read local addr");

        (socket, addr)
    }

    #[derive(Debug)]
    struct Peer {
        tx: SchemaTx,
        rx: mpsc::Receiver<Event>,
    }

    #[derive(Debug)]
    struct MockSchemaEventHandler(mpsc::Sender<Event>);

    impl MockSchemaEventHandler {
        fn new() -> (Self, mpsc::Receiver<Event>) {
            let (tx, rx) = mpsc::channel(10);
            (Self(tx), rx)
        }
    }

    #[async_trait]
    impl SchemaEventHandler for Arc<MockSchemaEventHandler> {
        async fn handle(&self, event: Event) {
            self.0.send(event).await.unwrap();
        }
    }

    async fn new_node_pair() -> (Peer, Peer) {
        let metrics = Arc::new(metric::Registry::default());

        let (a_socket, a_addr) = random_udp().await;
        let (handler, a_rx) = MockSchemaEventHandler::new();
        let a_store = Arc::new(handler);
        let a_dispatcher = SchemaRx::new(Arc::clone(&a_store), 100);

        let (b_socket, b_addr) = random_udp().await;
        let (handler, b_rx) = MockSchemaEventHandler::new();
        let b_store = Arc::new(handler);
        let b_dispatcher = SchemaRx::new(Arc::clone(&b_store), 100);

        // Initialise both gossip reactors
        let addrs = vec![a_addr.to_string(), b_addr.to_string()];
        let a = Builder::new(addrs.clone(), a_dispatcher, Arc::clone(&metrics)).build(a_socket);
        let b = Builder::new(addrs, b_dispatcher, Arc::clone(&metrics)).build(b_socket);

        // Wait for peer discovery to occur
        async {
            loop {
                if a.get_peers().await.len() == 1 && b.get_peers().await.len() == 1 {
                    break;
                }
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        let a = Peer {
            tx: SchemaTx::new(a),
            rx: a_rx,
        };

        let b = Peer {
            tx: SchemaTx::new(b),
            rx: b_rx,
        };

        (a, b)
    }

    // Broadcast a new namespace from node A and check it's instantiated into
    // the SchemaStore in node B.
    //
    // This is an integration test of the various schema gossip components.
    #[tokio::test]
    async fn test_new_namespace() {
        maybe_start_logging();

        let (node_a, mut node_b) = new_node_pair().await;

        let want = Event::NamespaceCreated(NamespaceCreated {
            namespace_name: "bananas".to_string(),
            namespace_id: 4242,
            partition_template: Some(
                NamespacePartitionTemplateOverride::try_from((**PARTITION_BY_DAY_PROTO).clone())
                    .unwrap()
                    .as_proto()
                    .unwrap()
                    .clone(),
            ),
            max_columns_per_table: 1,
            max_tables: 2,
            retention_period_ns: Some(1234),
        });

        // Broadcast the event from A
        node_a.tx.broadcast(want.clone());

        // Receive it from B
        let got = node_b
            .rx
            .recv()
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .unwrap();

        // Ensuring the content is identical
        assert_eq!(got, want);
    }

    // As above, but ensuring default partition templates propagate correctly.
    #[tokio::test]
    async fn test_namespace_default_partition_templates() {
        maybe_start_logging();

        let (node_a, mut node_b) = new_node_pair().await;

        let want = Event::NamespaceCreated(NamespaceCreated {
            namespace_name: "bananas".to_string(),
            namespace_id: 4242,
            partition_template: None,
            max_columns_per_table: 1,
            max_tables: 2,
            retention_period_ns: Some(1234),
        });

        // Broadcast the event from A
        node_a.tx.broadcast(want.clone());

        // Receive it from B
        let got = node_b
            .rx
            .recv()
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .unwrap();

        // Ensuring the content is identical
        assert_eq!(got, want);
    }

    /// Add a new table and column to an existing namespace.
    #[tokio::test]
    async fn test_new_table() {
        maybe_start_logging();

        let (node_a, mut node_b) = new_node_pair().await;
        let want = Event::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: "platanos".to_string(),
                table_id: 4242,
                columns: vec![GossipColumn {
                    name: "c2".to_string(),
                    column_id: 12345,
                    column_type: data_types::ColumnType::U64 as _,
                }],
            }),
            partition_template: test_table_partition_override(vec![
                data_types::partition_template::TemplatePart::TagValue("bananatastic"),
            ])
            .as_proto()
            .cloned(),
        });

        // Broadcast the event from A
        node_a.tx.broadcast(want.clone());

        // Receive it from B
        let got = node_b
            .rx
            .recv()
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .unwrap();

        // Ensuring the content is identical
        assert_eq!(got, want);
    }

    /// Add a new column to an existing table
    #[tokio::test]
    async fn test_new_column() {
        maybe_start_logging();

        let (node_a, mut node_b) = new_node_pair().await;
        let want = Event::TableUpdated(TableUpdated {
            table_name: "platanos".to_string(),
            namespace_name: "bananas".to_string(),
            table_id: 4242,
            columns: vec![GossipColumn {
                name: "c2".to_string(),
                column_id: 12345,
                column_type: data_types::ColumnType::U64 as _,
            }],
        });

        // Broadcast the event from A
        node_a.tx.broadcast(want.clone());

        // Receive it from B
        let got = node_b
            .rx
            .recv()
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .unwrap();

        // Ensuring the content is identical
        assert_eq!(got, want);
    }
}
