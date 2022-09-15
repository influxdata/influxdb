//! A mock [`PartitionProvider`] to inject [`PartitionData`] for tests.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::{PartitionKey, ShardId, TableId};
use parking_lot::Mutex;

use crate::data::partition::PartitionData;

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
    pub(crate) fn with_partition(
        mut self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
        data: PartitionData,
    ) -> Self {
        self.insert(partition_key, shard_id, table_id, data);
        self
    }

    /// Add `data` to the mock state, returning it when asked for the specified
    /// `(key, shard, table)` triplet.
    pub(crate) fn insert(
        &mut self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
        data: PartitionData,
    ) {
        assert!(
            self.partitions
                .lock()
                .insert((partition_key, shard_id, table_id), data)
                .is_none(),
            "overwriting an existing mock PartitionData"
        );
    }
}

#[async_trait]
impl PartitionProvider for MockPartitionProvider {
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
        table_name: Arc<str>,
    ) -> PartitionData {
        let p = self
            .partitions
            .lock()
            .remove(&(partition_key.clone(), shard_id, table_id))
            .unwrap_or_else(|| {
                panic!("no partition data for mock ({partition_key:?}, {shard_id:?}, {table_id:?})")
            });

        assert_eq!(*p.table_name(), *table_name);
        p
    }
}
