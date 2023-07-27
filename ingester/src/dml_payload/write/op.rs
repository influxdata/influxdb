use data_types::{NamespaceId, PartitionKey, TableId};
use hashbrown::HashMap;
use trace::ctx::SpanContext;

use super::table_data::TableData;

/// A decoded representation of the data contained by an RPC write
/// represented by an [`crate::dml_payload::IngestOp::Write`]
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

    /// Do NOT remove this test annotation. This constructor exists to by-pass
    /// safety invariant assertions for testing code only.
    #[cfg(test)]
    pub(crate) fn new_empty_invalid(namespace: NamespaceId, partition_key: PartitionKey) -> Self {
        Self {
            namespace,
            tables: Default::default(),
            partition_key,
            span_context: None,
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

    /// An optional tracing context associated with the [`WriteOperation`]
    pub fn span_context(&self) -> Option<&SpanContext> {
        self.span_context.as_ref()
    }
}
