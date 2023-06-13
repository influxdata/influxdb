use data_types::{NamespaceId, PartitionKey, SequenceNumber, TableId};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use trace::ctx::SpanContext;

/// The set of operations which the ingester can derive and process from wire
/// requests
pub enum IngestOp {
    Write(WriteOperation),
}

/// A decoded representation of the data contained by an RPC write
/// represented by an [`IngestOp::Write`]
pub struct WriteOperation {
    namespace: NamespaceId,

    tables: HashMap<TableId, TableData>,
    partition_key: PartitionKey,

    span_context: Option<SpanContext>,
}

impl WriteOperation {
    /// Construct a new [`WriteOperation`] from the provided details.
    ///
    /// # Panic
    ///
    /// Panics if
    ///
    /// - `tables` is empty
    pub fn new(
        namespace: NamespaceId,
        tables: HashMap<TableId, TableData>,
        partition_key: PartitionKey,
        span_context: Option<SpanContext>,
    ) -> Self {
        assert_ne!(tables.len(), 0);

        Self {
            namespace,
            tables,
            partition_key,
            span_context,
        }
    }
}

/// A container for all data for an individual table as part of a write
/// operation
pub struct TableData {
    table: TableId,
    /// The partitioned data for `table` in the write. Currently data is
    /// partitioned in a way that each table has a single partition of
    // data associated with it per write
    partitioned_data: PartitionedData,
}

/// Partitioned data belonging to a write, sequenced individually from
/// other [`PartitionedData`]
pub struct PartitionedData {
    sequence_number: SequenceNumber,
    data: MutableBatch,
}
