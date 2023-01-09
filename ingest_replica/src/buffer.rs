//! In memory queryable buffer of data sent from one or more ingesters. It evicts data from the
//! buffer when persist requests are sent in.

use crate::{
    cache::SchemaCache,
    query::{response::QueryResponse, QueryError, QueryExec},
    BufferError, ReplicationBuffer, TableIdToMutableBatch,
};
use async_trait::async_trait;
use data_types::{
    sequence_number_set::SequenceNumberSet, NamespaceId, PartitionId, SequenceNumber, TableId,
};
use iox_query::exec::Executor;
use std::sync::Arc;
use trace::span::Span;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Buffer {
    _schema_cache: Arc<SchemaCache>,
    _exec: Arc<Executor>,
}

impl Buffer {
    pub(crate) fn new(_schema_cache: Arc<SchemaCache>, _exec: Arc<Executor>) -> Self {
        Self {
            _schema_cache,
            _exec,
        }
    }

    pub(crate) async fn apply_write(
        &self,
        _namespace_id: NamespaceId,
        _table_batches: TableIdToMutableBatch,
        _ingester_id: Uuid,
        _sequence_number: SequenceNumber,
    ) -> Result<(), BufferError> {
        panic!("unimplemented")
    }
}

#[async_trait]
impl ReplicationBuffer for Buffer {
    async fn apply_write(
        &self,
        namespace_id: NamespaceId,
        table_batches: TableIdToMutableBatch,
        ingester_id: Uuid,
        sequence_number: SequenceNumber,
    ) -> Result<(), BufferError> {
        self.apply_write(namespace_id, table_batches, ingester_id, sequence_number)
            .await
    }

    async fn apply_persist(
        &self,
        _ingester_id: Uuid,
        _namespace_id: NamespaceId,
        _table_id: TableId,
        _partition_id: PartitionId,
        _sequence_set: SequenceNumberSet,
    ) -> Result<(), BufferError> {
        panic!("unimplemented")
    }

    async fn append_partition_buffer(
        &self,
        _ingester_id: Uuid,
        _namespace_id: NamespaceId,
        _table_id: TableId,
        _partition_id: PartitionId,
        _sequence_set: SequenceNumberSet,
        _table_batches: TableIdToMutableBatch,
    ) -> Result<(), BufferError> {
        panic!("unimplemented")
    }
}

#[async_trait]
impl QueryExec for Buffer {
    type Response = QueryResponse;

    async fn query_exec(
        &self,
        _namespace_id: NamespaceId,
        _table_id: TableId,
        _columns: Vec<String>,
        _span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        panic!("unimplemented");
    }
}
