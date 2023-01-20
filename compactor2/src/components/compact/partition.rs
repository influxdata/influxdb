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

    /// Namespace name
    pub namespace_name: String,

    /// Table.
    pub table: Arc<Table>,

    // Table schema
    pub table_schema: Arc<TableSchema>,

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
        namespace_name: String,
        table: Arc<Table>,
        table_schema: Arc<TableSchema>,
        sort_key: Option<SortKey>,
        partition_key: PartitionKey,
    ) -> Self {
        Self {
            partition_id,
            namespace_id,
            namespace_name,
            table,
            table_schema,
            sort_key,
            partition_key,
        }
    }
}
