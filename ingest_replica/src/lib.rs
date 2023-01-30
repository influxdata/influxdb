//! IOx Ingest Replica implementation
//!
//! The Ingest Replica serves as an in memory queryable buffer of data from one or more ingesters
//! that are persisting data. It provides horizontal scalability of query workloads on the data in
//! ingesters that has yet to be persisted to Parquet files. It also ensures that the write path
//! and the query path have failure isolation so that an outage in one won't create an outage in
//! the other.

#![allow(dead_code)] // Until ingest_replica is used.
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
    missing_docs
)]

mod buffer;
mod cache;
mod grpc;
mod query;
mod query_adaptor;

use crate::cache::CacheError;
use crate::{buffer::Buffer, cache::SchemaCache, grpc::GrpcDelegate};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use async_trait::async_trait;
use data_types::sequence_number_set::SequenceNumberSet;
use data_types::{NamespaceId, PartitionId, SequenceNumber, TableId, TRANSITION_SHARD_INDEX};
use generated_types::influxdata::iox::ingester::v1::replication_service_server::{
    ReplicationService, ReplicationServiceServer,
};
use hashbrown::HashMap;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use mutable_batch::MutableBatch;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// An error returned by the `ReplicationBuffer`.
#[derive(Debug, Error)]
pub enum BufferError {
    /// An error from the mutable batch sent to a buffer.
    #[error("mutable batch error: {0}")]
    MutableBatch(#[from] mutable_batch::Error),
}

/// Acquire opaque handles to the IngestReplica RPC service implementations.
///
/// This trait serves as the public crate API boundary - callers external to the
/// IngestReplica crate utilise this abstraction to acquire type erased handles to
/// the RPC service implementations, hiding internal IngestReplica implementation
/// details & types.
///
/// Callers can mock out this trait or decorate the returned implementation in
/// order to simulate or modify the behaviour of an ingest_replica in their own tests.
pub trait IngestReplicaRpcInterface: Send + Sync + std::fmt::Debug {
    /// The type of the [`ReplicationService`] implementation.
    type ReplicationHandler: ReplicationService;
    /// The type of the [`FlightService`] implementation.
    type FlightHandler: FlightService;

    /// Acquire an opaque handle to the IngestReplica's [`ReplicationService`] RPC
    /// handler implementation.
    fn replication_service(&self) -> ReplicationServiceServer<Self::ReplicationHandler>;

    /// Acquire an opaque handle to the Ingester's Arrow Flight
    /// [`FlightService`] RPC handler implementation, allowing at most
    /// `max_simultaneous_requests` queries to be running at any one time.
    fn query_service(
        &self,
        max_simultaneous_requests: usize,
    ) -> FlightServiceServer<Self::FlightHandler>;
}

/// Alias for the `TableId` to `MutableBatch` hashmap of data received in write and partition
/// buffer requests.
pub(crate) type TableIdToMutableBatch = HashMap<i64, MutableBatch>;

/// ReplicationBuffer can receive data from the replication protocol to get buffers of partition
/// data, individual write requests, and persistence notification to evict data from the buffer.
#[async_trait]
pub(crate) trait ReplicationBuffer: Send + Sync {
    /// Apply an individual write request to the buffer. Can write many rows into many partitions.
    async fn apply_write(
        &self,
        namespace_id: NamespaceId,
        table_batches: TableIdToMutableBatch,
        ingester_id: Uuid,
        sequence_number: SequenceNumber,
    ) -> Result<(), BufferError>;

    /// Apply a persist operation to the buffer, which should clear out the data from the given
    /// partition.
    async fn apply_persist(
        &self,
        ingester_id: Uuid,
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: PartitionId,
        sequence_set: SequenceNumberSet,
    ) -> Result<(), BufferError>;

    /// Append an entire partition buffer to the buffer. It should be able to evict this entire
    /// buffer in one operation when it later receives a persist operation that has a SequenceSet
    /// that is a superset of the one sent here.
    async fn append_partition_buffer(
        &self,
        ingester_id: Uuid,
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: PartitionId,
        sequence_set: SequenceNumberSet,
        table_batches: TableIdToMutableBatch,
    ) -> Result<(), BufferError>;
}

/// Errors that occur during initialisation of an `ingest_replica` instance.
#[derive(Debug, Error)]
pub enum InitError {
    /// An error occurred trying to warm the schema cache
    #[error("failed to pre-warm schema cache: {0}")]
    WarmCache(#[from] CacheError),
}

/// Initialise a new `ingest_replica` instance, returning the gRPC service handler
/// implementations to be bound by the caller.
#[allow(clippy::too_many_arguments)]
pub async fn new(
    catalog: Arc<dyn Catalog>,
    _ingesters: Vec<String>,
    exec: Arc<Executor>,
    metrics: Arc<metric::Registry>,
) -> Result<impl IngestReplicaRpcInterface, InitError> {
    // Create the transition shard.
    let mut txn = catalog
        .start_transaction()
        .await
        .expect("start transaction");
    let topic = txn
        .topics()
        .create_or_get("iox-shared")
        .await
        .expect("get topic");
    let transition_shard = txn
        .shards()
        .create_or_get(&topic, TRANSITION_SHARD_INDEX)
        .await
        .expect("create transition shard");
    txn.commit().await.expect("commit transition shard");

    let schema_cache = Arc::new(SchemaCache::new(Arc::clone(&catalog), transition_shard.id));
    schema_cache.warm().await?;

    let buffer = Arc::new(Buffer::new(schema_cache, exec));

    // TODO: connect to the remote ingesters and subscribe to their data, receiving the
    //       PartitionBufferResponses into the buffer. Note that the ReplicationService in this
    //       GrpcDelegate must be running before the requests are sent as the ingester will
    //       immediately start sending replciate requests.

    Ok(GrpcDelegate::new(Arc::clone(&buffer), metrics))
}
