use data_types::{NamespaceId, PartitionKey, SequenceNumber, TableId};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use trace::ctx::SpanContext;

/// The set of operations which the ingester can derive and process from wire
/// requests
#[derive(Clone, Debug)]
pub enum IngestOp {
    /// A write for ingest
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
    // TODO(savage): Consider removing the getters at the top level and
    // requiring consumers to match on the op type

    /// The namespace which the ingest operation is for
    pub fn namespace(&self) -> NamespaceId {
        match self {
            Self::Write(w) => w.namespace,
        }
    }

    /// An optional tracing context associated with the ingest operation
    pub fn span_context(&self) -> Option<&SpanContext> {
        match self {
            Self::Write(w) => w.span_context.as_ref(),
        }
    }
}

/// A decoded representation of the data contained by an RPC write
/// represented by an [`IngestOp::Write`]
#[derive(Debug, Clone)]
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

    /// The namespace which the write is
    pub fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    /// The partition key derived for the write operation
    pub fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Returns an by-reference iterator over the per-table write data
    /// contained in the operation
    pub fn tables(&self) -> impl Iterator<Item = (&TableId, &TableData)> {
        self.tables.iter()
    }

    /// Consumes `self`, returning an iterator over the per-table write
    /// data contained in the operation
    pub fn into_tables(self) -> impl Iterator<Item = (TableId, TableData)> {
        self.tables.into_iter()
    }
}

// TODO(savage): Temporary `From` implementation to assist in switchover
// within ingester code. This is deeply inefficient.
impl From<&WriteOperation> for DmlWrite {
    fn from(value: &WriteOperation) -> Self {
        let sequence_number = value
            .tables
            .values()
            .next()
            .expect("converting empty write operation")
            .partitioned_data
            .sequence_number;
        Self::new(
            value.namespace,
            value
                .tables
                .iter()
                .map(|(table_id, data)| (*table_id, data.partitioned_data.data.clone()))
                .collect(),
            value.partition_key.clone(),
            DmlMeta::sequenced(
                sequence_number,
                iox_time::Time::MAX,
                value.span_context.clone(),
                0,
            ),
        )
    }
}

// TODO(savage): Temporary `From` implementation to assist in switchover
// within ingester code. This should be removed in favour of constructing all
// [`WriteOperation`]s directly
impl From<DmlWrite> for WriteOperation {
    fn from(dml_write: DmlWrite) -> Self {
        let namespace_id = dml_write.namespace_id();
        let partition_key = dml_write.partition_key().clone();
        let sequence_number = dml_write
            .meta()
            .sequence()
            .expect("tried to create write operation from unsequenced DML write");
        let span_context = dml_write.meta().span_context().map(SpanContext::clone);

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
#[derive(Debug, Clone)]
pub struct TableData {
    table: TableId,
    // The partitioned data for `table` in the write. Currently data is
    // partitioned in a way that each table has a single partition of
    // data associated with it per write
    partitioned_data: PartitionedData,
}

impl TableData {
    /// Constructs a new set of table associated data
    pub fn new(table: TableId, partitioned_data: PartitionedData) -> Self {
        Self {
            table,
            partitioned_data,
        }
    }

    /// Returns the [`TableId`] which the data is for
    pub fn table(&self) -> TableId {
        self.table
    }

    /// Returns a reference to the [`PartitionedData`] for the table
    pub fn partitioned_data(&self) -> &PartitionedData {
        &self.partitioned_data
    }

    /// Consumes `self`, returning the [`PartitionedData`] for the table
    pub fn into_partitioned_data(self) -> PartitionedData {
        self.partitioned_data
    }
}

/// Partitioned data belonging to a write, sequenced individually from
/// other [`PartitionedData`]
#[derive(Debug, Clone)]
pub struct PartitionedData {
    sequence_number: SequenceNumber,
    data: MutableBatch,
}

impl PartitionedData {
    /// Creates a new set of partitioned data, assigning it a [`SequenceNumber`]
    pub fn new(sequence_number: SequenceNumber, data: MutableBatch) -> Self {
        Self {
            sequence_number,
            data,
        }
    }

    /// Returns the [`SequenceNumber`] assigned
    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }

    /// Consumes `self`, returning the data
    pub fn data(self) -> MutableBatch {
        self.data
    }
}
