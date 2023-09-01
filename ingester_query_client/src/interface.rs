//! High-level interface.
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use data_types::{NamespaceId, TableId, TimestampMinMax, TransitionPartitionId};
use datafusion::prelude::Expr;
use std::{fmt::Debug, sync::Arc};
use trace::span::Span;
use uuid::Uuid;

use crate::layer::Layer;

/// Request to an ingester.
#[derive(Debug, Clone)]
pub struct Request {
    /// Namespace to search
    pub namespace_id: NamespaceId,

    /// Table that should be queried.
    pub table_id: TableId,

    /// Columns the query service is interested in.
    pub columns: Vec<String>,

    /// Predicate for filtering.
    pub filters: Vec<Expr>,
}

/// Response from the ingester.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseMetadata {
    /// Ingester UUID
    pub ingester_uuid: Uuid,

    /// Number of persisted parquet files for this ingester.
    pub persist_counter: i64,

    /// Serialized table schema.
    pub table_schema: SchemaRef,

    /// Ingester partitions.
    pub partitions: Vec<ResponsePartition>,
}

/// Partition metadata as part of a [`QueryResponse`].
///
///
/// [`QueryResponse`]: crate::layer::QueryResponse
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsePartition {
    /// Partition ID.
    pub id: TransitionPartitionId,

    /// Timestamp min and max.
    pub t_min_max: TimestampMinMax,

    /// Partition schema.
    ///
    /// This is always a projection of the [table schema](ResponseMetadata::table_schema).
    pub schema: SchemaRef,
}

/// Data payload in the [`QueryResponse`].
///
///
/// [`QueryResponse`]: crate::layer::QueryResponse
#[derive(Debug, Clone, PartialEq)]
pub struct ResponsePayload {
    /// Associated partition.
    ///
    /// This is always one of the partitions present in [`ResponseMetadata::partitions`].
    pub partition_id: TransitionPartitionId,

    /// Record batch data.
    ///
    /// This is always a projection of the [partition schema](ResponsePartition::schema).
    pub batch: RecordBatch,
}

/// High-level client.
pub type IngesterClient = Arc<
    dyn Layer<
        Request = (Request, Option<Span>),
        ResponseMetadata = ResponseMetadata,
        ResponsePayload = ResponsePayload,
    >,
>;

#[cfg(test)]
mod tests {
    use crate::assert_impl;

    use super::*;

    assert_impl!(ingester_client_is_send, IngesterClient, Send);
    assert_impl!(ingester_client_is_sync, IngesterClient, Sync);
}
