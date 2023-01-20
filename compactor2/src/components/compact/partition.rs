//! Information of a partition for compaction

use std::sync::Arc;

use data_types::{NamespaceId, PartitionId, PartitionKey, Table, TableSchema};
use schema::sort::SortKey;
use snafu::Snafu;

/// Compaction errors.
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Not implemented"))]
    NotImplemented,
}

/// Information of a partition for compaction
pub struct PartitionInfo {
    /// the partition
    pub partition_id: PartitionId,

    /// Namespace ID
    pub namespace_id: NamespaceId,

    /// Table.
    pub table: Arc<Table>,

    // Table schema
    pub table_schema: Arc<TableSchema>,

    // /// Counts of the number of columns of each type, used for estimating arrow size
    // pub column_type_counts: Vec<ColumnTypeCount>,
    /// Sort key of the partition
    pub sort_key: Option<SortKey>,

    /// partition_key
    pub partition_key: PartitionKey,
}

impl PartitionInfo {
    /// Create PartitionInfo for a paquert file
    pub fn new(
        partition_id: PartitionId,
        namespace_id: NamespaceId,
        table: Arc<Table>,
        table_schema: Arc<TableSchema>,
        sort_key: Option<SortKey>,
        partition_key: PartitionKey,
    ) -> Self {
        Self {
            partition_id,
            namespace_id,
            table,
            table_schema,
            sort_key,
            partition_key,
        }
    }
}
