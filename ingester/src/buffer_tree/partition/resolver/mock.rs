//! A mock [`PartitionProvider`] to inject [`PartitionData`] for tests.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, TableId};
use parking_lot::Mutex;

use super::r#trait::PartitionProvider;
use crate::{
    buffer_tree::{namespace::NamespaceName, partition::PartitionData, table::TableMetadata},
    deferred_load::{self, DeferredLoad},
};

/// A mock [`PartitionProvider`] for testing that returns pre-initialised
/// [`PartitionData`] for configured `(key, table)` tuples.
#[derive(Debug, Default)]
pub(crate) struct MockPartitionProvider {
    partitions: Mutex<HashMap<(PartitionKey, TableId), PartitionData>>,
}

impl MockPartitionProvider {
    /// A builder helper for [`Self::insert()`].
    #[must_use]
    pub(crate) fn with_partition(mut self, data: PartitionData) -> Self {
        self.insert(data);
        self
    }

    /// Add `data` to the mock state, returning it when asked for the specified
    /// `(key, table)` tuple.
    pub(crate) fn insert(&mut self, data: PartitionData) {
        assert!(
            self.partitions
                .lock()
                .insert((data.partition_key().clone(), data.table_id()), data)
                .is_none(),
            "overwriting an existing mock PartitionData"
        );
    }

    /// Returns true if all mock values have been consumed.
    pub(crate) fn is_empty(&self) -> bool {
        self.partitions.lock().is_empty()
    }
}

#[async_trait]
impl PartitionProvider for MockPartitionProvider {
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table: Arc<DeferredLoad<TableMetadata>>,
    ) -> Arc<Mutex<PartitionData>> {
        let p = self
            .partitions
            .lock()
            .remove(&(partition_key.clone(), table_id))
            .unwrap_or_else(|| {
                panic!("no partition data for mock ({partition_key:?}, {table_id:?})")
            });

        assert_eq!(p.namespace_id(), namespace_id);

        let actual_namespace_name = p.namespace_name().to_string();
        let expected_namespace_name = namespace_name.get().await.to_string();
        assert!(
            (actual_namespace_name.as_str() == expected_namespace_name)
                || (actual_namespace_name == deferred_load::UNRESOLVED_DISPLAY_STRING),
            "unexpected namespace name: {actual_namespace_name}. \
            expected {expected_namespace_name} or {}",
            deferred_load::UNRESOLVED_DISPLAY_STRING,
        );

        let actual_table_name = p.table().to_string();
        let expected_table_name = table.get().await.name().to_string();
        assert!(
            (actual_table_name.as_str() == expected_table_name)
                || (actual_table_name == deferred_load::UNRESOLVED_DISPLAY_STRING),
            "unexpected table name: {actual_table_name}. \
            expected {expected_table_name} or {}",
            deferred_load::UNRESOLVED_DISPLAY_STRING,
        );

        Arc::new(Mutex::new(p))
    }
}
