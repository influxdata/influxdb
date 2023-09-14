//! A mock [`PartitionProvider`] to inject [`PartitionData`] for tests.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, PartitionKey, TableId};
use parking_lot::Mutex;

use super::r#trait::PartitionProvider;
use crate::{
    buffer_tree::{
        namespace::NamespaceName, partition::PartitionData, table::metadata::TableMetadata,
    },
    deferred_load::DeferredLoad,
    test_util::PartitionDataBuilder,
};

/// A mock [`PartitionProvider`] for testing that returns pre-initialised
/// [`PartitionData`] for configured `(key, table)` tuples.
#[derive(Debug, Default)]
pub(crate) struct MockPartitionProvider {
    partitions: Mutex<HashMap<(PartitionKey, TableId), PartitionDataBuilder>>,
}

impl MockPartitionProvider {
    /// Add a [`PartitionDataBuilder`] that will be used to construct a
    /// [`PartitionData`] for mock calls.
    ///
    /// The provided builder will be used to construct a [`PartitionData`] when
    /// asked for a partition with the same table ID & partition key as
    /// configured in the builder.
    #[must_use]
    pub(crate) fn with_partition(self, data: PartitionDataBuilder) -> Self {
        assert!(
            self.partitions
                .lock()
                .insert((data.partition_key().clone(), data.table_id()), data)
                .is_none(),
            "overwriting an existing mock PartitionData"
        );

        self
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

        let mut p = p.with_namespace_id(namespace_id);

        // If the test provided a namespace/table loader, use it, otherwise default to the
        // one provided in this call.
        if p.namespace_loader().is_none() {
            p = p.with_namespace_loader(namespace_name);
        }
        if p.table_loader().is_none() {
            p = p.with_table_loader(table);
        }

        Arc::new(Mutex::new(p.build()))
    }
}
