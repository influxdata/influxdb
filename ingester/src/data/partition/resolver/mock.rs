//! A mock [`PartitionProvider`] to inject [`PartitionData`] for tests.

use std::collections::HashMap;

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, ShardId, TableId};
use parking_lot::Mutex;

use crate::data::{partition::PartitionData, table::TableName};

use super::r#trait::PartitionProvider;

/// A mock [`PartitionProvider`] for testing that returns pre-initialised
/// [`PartitionData`] for configured `(key, shard, table)` triplets.
#[derive(Debug, Default)]
pub(crate) struct MockPartitionProvider {
    partitions: Mutex<HashMap<(PartitionKey, ShardId, TableId), PartitionData>>,
}

impl MockPartitionProvider {
    /// A builder helper for [`Self::insert()`].
    #[must_use]
    pub(crate) fn with_partition(mut self, data: PartitionData) -> Self {
        self.insert(data);
        self
    }

    /// Add `data` to the mock state, returning it when asked for the specified
    /// `(key, shard, table)` triplet.
    pub(crate) fn insert(&mut self, data: PartitionData) {
        assert!(
            self.partitions
                .lock()
                .insert(
                    (
                        data.partition_key().clone(),
                        data.shard_id(),
                        data.table_id()
                    ),
                    data
                )
                .is_none(),
            "overwriting an existing mock PartitionData"
        );
    }

    /// Returns true if all mock values have been consumed.
    pub fn is_empty(&self) -> bool {
        self.partitions.lock().is_empty()
    }
}

#[async_trait]
impl PartitionProvider for MockPartitionProvider {
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: TableName,
    ) -> PartitionData {
        let p = self
            .partitions
            .lock()
            .remove(&(partition_key.clone(), shard_id, table_id))
            .unwrap_or_else(|| {
                panic!("no partition data for mock ({partition_key:?}, {shard_id:?}, {table_id:?})")
            });

        assert_eq!(p.namespace_id(), namespace_id);
        assert_eq!(p.table_name().to_string(), table_name.to_string());
        p
    }
}
