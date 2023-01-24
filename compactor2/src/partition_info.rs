//! Information of a partition for compaction

use std::sync::Arc;

use data_types::{NamespaceId, PartitionId, PartitionKey, Table, TableSchema};
use schema::sort::SortKey;

/// Information of a partition for compaction
#[derive(Debug, PartialEq, Eq)]
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
