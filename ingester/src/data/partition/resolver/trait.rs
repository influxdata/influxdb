use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, ShardId, TableId};

use crate::data::{partition::PartitionData, table::TableName};

/// An infallible resolver of [`PartitionData`] for the specified shard, table,
/// and partition key, returning an initialised [`PartitionData`] buffer for it.
#[async_trait]
pub trait PartitionProvider: Send + Sync + Debug {
    /// Return an initialised [`PartitionData`] for a given `(partition_key,
    /// shard_id, table_id)` tuple.
    ///
    /// NOTE: the constructor for [`PartitionData`] is NOT `pub` and SHOULD NOT
    /// be `pub` so this trait is effectively sealed.
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: TableName,
    ) -> PartitionData;
}

#[async_trait]
impl<T> PartitionProvider for Arc<T>
where
    T: PartitionProvider,
{
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: TableName,
    ) -> PartitionData {
        (**self)
            .get_partition(partition_key, shard_id, namespace_id, table_id, table_name)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::PartitionId;

    use crate::data::partition::{resolver::MockPartitionProvider, SortKeyState};

    use super::*;

    #[tokio::test]
    async fn test_arc_impl() {
        let key = PartitionKey::from("bananas");
        let shard_id = ShardId::new(42);
        let namespace_id = NamespaceId::new(1234);
        let table_id = TableId::new(24);
        let table_name = TableName::from("platanos");
        let partition = PartitionId::new(4242);
        let data = PartitionData::new(
            partition,
            "bananas".into(),
            shard_id,
            namespace_id,
            table_id,
            table_name.clone(),
            SortKeyState::Provided(None),
            None,
        );

        let mock = Arc::new(MockPartitionProvider::default().with_partition(data));

        let got = mock
            .get_partition(key, shard_id, namespace_id, table_id, table_name.clone())
            .await;
        assert_eq!(got.partition_id(), partition);
        assert_eq!(got.namespace_id(), namespace_id);
        assert_eq!(got.table_name().to_string(), table_name.to_string());
    }
}
