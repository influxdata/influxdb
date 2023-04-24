use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, ShardId, TableId};
use parking_lot::Mutex;

use crate::{
    buffer_tree::{namespace::NamespaceName, partition::PartitionData, table::TableName},
    deferred_load::DeferredLoad,
};

/// An infallible resolver of [`PartitionData`] for the specified table and
/// partition key, returning an initialised [`PartitionData`] buffer for it.
#[async_trait]
pub(crate) trait PartitionProvider: Send + Sync + Debug {
    /// Return an initialised [`PartitionData`] for a given `(partition_key,
    /// table_id)` tuple.
    ///
    /// NOTE: the constructor for [`PartitionData`] is NOT `pub` and SHOULD NOT
    /// be `pub` so this trait is effectively sealed.
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
        transition_shard_id: ShardId,
    ) -> Arc<Mutex<PartitionData>>;
}

#[async_trait]
impl<T> PartitionProvider for Arc<T>
where
    T: PartitionProvider,
{
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
        transition_shard_id: ShardId,
    ) -> Arc<Mutex<PartitionData>> {
        (**self)
            .get_partition(
                partition_key,
                namespace_id,
                namespace_name,
                table_id,
                table_name,
                transition_shard_id,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use data_types::{PartitionId, ShardId, TRANSITION_SHARD_ID};

    use super::*;
    use crate::{
        buffer_tree::partition::{resolver::mock::MockPartitionProvider, SortKeyState},
        test_util::{
            PartitionDataBuilder, ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_ID,
            ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID, DEFER_NAMESPACE_NAME_1_SEC,
            DEFER_TABLE_NAME_1_SEC,
        },
    };

    #[tokio::test]
    async fn test_arc_impl() {
        let data = PartitionDataBuilder::new().build();

        let mock = Arc::new(MockPartitionProvider::default().with_partition(data));

        let got = mock
            .get_partition(
                ARBITRARY_PARTITION_KEY.clone(),
                ARBITRARY_NAMESPACE_ID,
                Arc::clone(&*DEFER_NAMESPACE_NAME_1_SEC),
                ARBITRARY_TABLE_ID,
                Arc::clone(&*DEFER_TABLE_NAME_1_SEC),
                TRANSITION_SHARD_ID,
            )
            .await;
        assert_eq!(got.lock().partition_id(), ARBITRARY_PARTITION_ID);
        assert_eq!(got.lock().namespace_id(), ARBITRARY_NAMESPACE_ID);
        assert_eq!(
            got.lock().namespace_name().to_string(),
            DEFER_NAMESPACE_NAME_1_SEC.to_string()
        );
        assert_eq!(
            got.lock().table_name().to_string(),
            DEFER_TABLE_NAME_1_SEC.to_string()
        );
    }
}
