use data_types::{NamespaceId, PartitionKey, SequenceNumber, TableId};
use dml::{DmlOperation, DmlWrite};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use trace::ctx::SpanContext;

/// The set of operations which the ingester can derive and process from wire
/// requests
pub enum IngestOp {
    Write(WriteOperation),
}

impl From<DmlOperation> for IngestOp {
    fn from(value: DmlOperation) -> Self {
        match value {
            DmlOperation::Write(w) => Self::Write(WriteOperation::from(w)),
            DmlOperation::Delete(_) => {
                panic!("no corresponding ingest operation exists for DML delete")
            }
        }
    }
}

impl IngestOp {
    // TODO(savage): Consider removing the use of these and requiring users to
    //  match on the op type.
    pub fn namespace(&self) -> NamespaceId {
        match self {
            Self::Write(w) => w.namespace,
        }
    }
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

// TODO(savage): Temporary [`From`] implementation to assist in switchover
// within ingester code. This should be removed in favour of constructing all
// [`WriteOperation`]s directly
impl From<DmlWrite> for WriteOperation {
    fn from(dml_write: DmlWrite) -> Self {
        let namespace_id = dml_write.namespace_id();
        let partition_key = dml_write.partition_key().to_owned();
        let sequence_number = dml_write
            .meta()
            .sequence()
            .expect("tried to create write operation from unsequenced DML write");
        let span_context = dml_write.meta().span_context().map(SpanContext::to_owned);

        Self::new(
            namespace_id,
            dml_write
                .into_tables()
                .map(|(table, data)| {
                    (
                        table,
                        TableData {
                            table,
                            partitioned_data: PartitionedData {
                                sequence_number,
                                data,
                            },
                        },
                    )
                })
                .collect(),
            partition_key,
            span_context,
        )
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

impl TableData {
    pub fn new(table: TableId, partitioned_data: PartitionedData) -> Self {
        Self {
            table,
            partitioned_data,
        }
    }
}

/// Partitioned data belonging to a write, sequenced individually from
/// other [`PartitionedData`]
pub struct PartitionedData {
    sequence_number: SequenceNumber,
    data: MutableBatch,
}

impl PartitionedData {
    pub fn new(sequence_number: SequenceNumber, data: MutableBatch) -> Self {
        Self {
            sequence_number,
            data,
        }
    }
}
