//! Parquet file creation notifications over [gossip].
//!
//! This sub-system is composed of the following primary components:
//!
//! * [`gossip`] crate: provides the gossip transport, the [`GossipHandle`], and
//!   the [`Dispatcher`]. This crate operates on raw bytes.
//!
//! * The outgoing [`ParquetFileTx`]: a parquet file focussed wrapper over the
//!   underlying [`GossipHandle`]. This type translates the protobuf
//!   [`ParquetFile`] from the application layer into raw serialised bytes,
//!   sending them over the underlying [`gossip`] impl.
//!
//! * The incoming [`ParquetFileRx`]: deserialises the incoming bytes from the
//!   gossip [`Dispatcher`] into [`ParquetFile`] and passes them off to the
//!   [`ParquetFileEventHandler`] implementation for processing.
//!
//! Users of this crate should implement the [`ParquetFileEventHandler`] trait
//! to receive change events, and push events into the [`ParquetFileTx`] to
//! broadcast changes to peers.
//!
//! ```text
//!         ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                                                              │
//!         │                    Application
//!                                                              │
//!         └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//!                     │                           ▲
//!                     │                           │
//!                     │                           │
//!                     │        ParquetFile        │
//!                     │                           │
//!                     ▼                           │
//!         ┌──────────────────────┐   ┌─────────────────────────┐
//!         │    ParquetFileTx     │   │      ParquetFileRx      │
//!         └──────────────────────┘   └─────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                     │       Encoded bytes       │
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
//! # Best Effort
//!
//! This underlying gossip subsystem is designed to provide best effort delivery
//! of messages, and therefore best-effort delivery of parquet creation events,
//! without any ordering or delivery guarantees.
//!
//! This crate does NOT provide any eventual consistency guarantees.
//!
//! [`ParquetFileTx`]: tx::ParquetFileTx
//! [`ParquetFileRx`]: rx::ParquetFileRx
//! [`ParquetFileEventHandler`]: rx::ParquetFileEventHandler
//! [`GossipHandle`]: gossip::GossipHandle
//! [`Dispatcher`]: gossip::Dispatcher
//! [`ParquetFile`]: generated_types::influxdata::iox::catalog::v1::ParquetFile

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

pub mod rx;
pub mod tx;

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use generated_types::influxdata::iox::catalog::v1::{
        partition_identifier, ParquetFile, PartitionIdentifier,
    };
    use gossip::Builder;
    use test_helpers::{maybe_start_logging, timeout::FutureTimeout};
    use tokio::{net::UdpSocket, sync::mpsc};

    use crate::{
        rx::{ParquetFileEventHandler, ParquetFileRx},
        tx::ParquetFileTx,
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
        tx: ParquetFileTx<ParquetFile>,
        rx: mpsc::Receiver<ParquetFile>,
    }

    #[derive(Debug)]
    struct MockParquetEventHandler(mpsc::Sender<ParquetFile>);

    impl MockParquetEventHandler {
        fn new() -> (Self, mpsc::Receiver<ParquetFile>) {
            let (tx, rx) = mpsc::channel(10);
            (Self(tx), rx)
        }
    }

    #[async_trait]
    impl ParquetFileEventHandler for Arc<MockParquetEventHandler> {
        async fn handle(&self, event: ParquetFile) {
            self.0.send(event).await.unwrap();
        }
    }

    async fn new_node_pair() -> (Peer, Peer) {
        let metrics = Arc::new(metric::Registry::default());

        let (a_socket, a_addr) = random_udp().await;
        let (handler, a_rx) = MockParquetEventHandler::new();
        let a_store = Arc::new(handler);
        let a_dispatcher = ParquetFileRx::new(Arc::clone(&a_store), 100);

        let (b_socket, b_addr) = random_udp().await;
        let (handler, b_rx) = MockParquetEventHandler::new();
        let b_store = Arc::new(handler);
        let b_dispatcher = ParquetFileRx::new(Arc::clone(&b_store), 100);

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
            tx: ParquetFileTx::new(a),
            rx: a_rx,
        };

        let b = Peer {
            tx: ParquetFileTx::new(b),
            rx: b_rx,
        };

        (a, b)
    }

    /// Ensure a ParquetFile can be round-tripped through the gossip layer.
    ///
    /// This acts as an integration test, covering the serialisation of parquet
    /// file messages, passing into the gossip layer, topic assignment &
    /// decoding, deserialisation of the parquet file message, and handling by
    /// the new file event delegate abstraction defined by this crate.
    #[tokio::test]
    async fn test_round_trip() {
        maybe_start_logging();

        let (node_a, mut node_b) = new_node_pair().await;
        let want = ParquetFile {
            id: 42,
            namespace_id: 4242,
            table_id: 24,
            partition_identifier: Some(PartitionIdentifier {
                id: Some(partition_identifier::Id::HashId(vec![1, 2, 3, 4])),
            }),
            object_store_id: "bananas".to_string(),
            min_time: 1,
            max_time: 100,
            to_delete: Some(0),
            file_size_bytes: 424242,
            row_count: 4242111,
            compaction_level: 4200,
            created_at: 12344321,
            column_set: vec![1, 2, 3, 4, 5],
            max_l0_created_at: 123455555,
        };

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
