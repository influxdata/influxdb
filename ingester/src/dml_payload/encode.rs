//! This module houses encode helpers for the ingester internal DML types
use data_types::NamespaceId;
use generated_types::influxdata::pbdata::v1::DatabaseBatch;
use mutable_batch_pb::encode::encode_batch;

use super::write::WriteOperation;

/// Encodes a [`WriteOperation`] for `namespace` into the [`DatabaseBatch`]
/// wire format.
pub fn encode_write_op(namespace: NamespaceId, op: &WriteOperation) -> DatabaseBatch {
    DatabaseBatch {
        database_id: namespace.get(),
        partition_key: op.partition_key().to_string(),
        table_batches: op
            .tables()
            .map(|(table_id, batch)| encode_batch(table_id.get(), batch.partitioned_data().data()))
            .collect(),
    }
}
