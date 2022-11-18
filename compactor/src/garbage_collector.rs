//! Clean up parquet files from object storage and their associated entries in the catalog that are
//! no longer needed because they've been compacted and they're old enough to no longer be used by
//! any queriers.

use data_types::Timestamp;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use object_store::DynObjectStore;
use parquet_file::ParquetFilePath;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while deleting catalog records {}", source))]
    DeletingCatalogRecords {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Error(s) while deleting object store files: {:#?}", sources))]
    DeletingObjectStoreFiles { sources: Vec<object_store::Error> },
}

/// A specialized `Result` for garbage collection errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Information needed to clean up old parquet files from object storage and their entries in the
/// catalog
pub struct GarbageCollector {
    /// Object store where parquet files should be cleaned up
    object_store: Arc<DynObjectStore>,
    /// The global catalog for parquet files
    catalog: Arc<dyn Catalog>,
    /// Time provider for all activities in this garbage collector
    pub time_provider: Arc<dyn TimeProvider>,
}

impl GarbageCollector {
    /// Initialize the Garbage Collector
    pub fn new(catalog: Arc<dyn Catalog>, object_store: Arc<DynObjectStore>) -> Self {
        let time_provider = catalog.time_provider();

        Self {
            catalog,
            object_store,
            time_provider,
        }
    }

    /// Perform a pass of garbage collection, querying the catalog for all files marked to be
    /// deleted earlier than the specified time. Remove the catalog entries, then remove the
    /// associated object store files.
    /// Meant to be invoked in a background loop.
    pub async fn cleanup(&self, older_than: Timestamp) -> Result<()> {
        let deleted_catalog_records = self
            .catalog
            .repositories()
            .await
            .parquet_files()
            .delete_old(older_than)
            .await
            .context(DeletingCatalogRecordsSnafu)?;

        let mut object_store_errors = Vec::with_capacity(deleted_catalog_records.len());

        for catalog_record in deleted_catalog_records {
            let path = ParquetFilePath::new(
                catalog_record.namespace_id,
                catalog_record.table_id,
                catalog_record.shard_id,
                catalog_record.partition_id,
                catalog_record.object_store_id,
            );
            let path = path.object_store_path();

            if let Err(e) = self.object_store.delete(&path).await {
                object_store_errors.push(e);
            }
        }

        if object_store_errors.is_empty() {
            Ok(())
        } else {
            DeletingObjectStoreFilesSnafu {
                sources: object_store_errors,
            }
            .fail()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, ParquetFile, ParquetFileParams, SequenceNumber,
        ShardIndex,
    };
    use futures::{StreamExt, TryStreamExt};
    use iox_tests::util::TestCatalog;
    use std::time::Duration;
    use uuid::Uuid;

    /// Test helper to put an empty object store file at the expected location for a parquet file
    /// tracked by the catalog, but without having to process data because the garbage collector
    /// isn't concerned with the contents of the file, only its existence or lack thereof
    async fn put_object_store_file(
        catalog_record: &ParquetFile,
        object_store: Arc<DynObjectStore>,
    ) {
        let bytes = "arbitrary".into();

        let path = ParquetFilePath::new(
            catalog_record.namespace_id,
            catalog_record.table_id,
            catalog_record.shard_id,
            catalog_record.partition_id,
            catalog_record.object_store_id,
        );
        let path = path.object_store_path();

        object_store.put(&path, bytes).await.unwrap();
    }

    #[tokio::test]
    async fn nothing_to_delete_is_success() {
        let catalog = TestCatalog::new();
        let gc = GarbageCollector::new(
            Arc::clone(&catalog.catalog),
            Arc::clone(&catalog.object_store),
        );
        let older_than = Timestamp::from(gc.time_provider.now() + Duration::from_secs(100));

        gc.cleanup(older_than).await.unwrap();
    }

    #[tokio::test]
    async fn leave_undeleted_files_alone() {
        let catalog = TestCatalog::new();
        let gc = GarbageCollector::new(
            Arc::clone(&catalog.catalog),
            Arc::clone(&catalog.object_store),
        );
        let older_than = Timestamp::from(gc.time_provider.now() + Duration::from_secs(100));

        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create("gc_leave_undeleted_files_alone", None, topic.id, pool.id)
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            created_at: Timestamp::new(1),
            compaction_level: CompactionLevel::Initial,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = txn
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        put_object_store_file(&parquet_file, Arc::clone(&catalog.object_store)).await;

        txn.commit().await.unwrap();

        gc.cleanup(older_than).await.unwrap();

        assert_eq!(
            catalog
                .catalog
                .repositories()
                .await
                .parquet_files()
                .count()
                .await
                .unwrap(),
            1
        );

        let list = catalog.object_store.list(None).await.unwrap();
        let obj_store_paths: Vec<_> = list.try_collect().await.unwrap();
        assert_eq!(obj_store_paths.len(), 1);
    }

    #[tokio::test]
    async fn leave_too_new_files_alone() {
        let catalog = TestCatalog::new();
        let gc = GarbageCollector::new(
            Arc::clone(&catalog.catalog),
            Arc::clone(&catalog.object_store),
        );
        let older_than = Timestamp::from(gc.time_provider.now() - Duration::from_secs(100));

        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create("gc_leave_too_new_files_alone", None, topic.id, pool.id)
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            created_at: Timestamp::new(1),
            compaction_level: CompactionLevel::Initial,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = txn
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        put_object_store_file(&parquet_file, Arc::clone(&catalog.object_store)).await;

        txn.parquet_files()
            .flag_for_delete(parquet_file.id)
            .await
            .unwrap();

        txn.commit().await.unwrap();

        gc.cleanup(older_than).await.unwrap();

        assert_eq!(
            catalog
                .catalog
                .repositories()
                .await
                .parquet_files()
                .count()
                .await
                .unwrap(),
            1
        );

        let list = catalog.object_store.list(None).await.unwrap();
        let obj_store_paths: Vec<_> = list.try_collect().await.unwrap();
        assert_eq!(obj_store_paths.len(), 1);
    }

    #[tokio::test]
    async fn remove_old_enough_files() {
        let catalog = TestCatalog::new();
        let gc = GarbageCollector::new(
            Arc::clone(&catalog.catalog),
            Arc::clone(&catalog.object_store),
        );
        let older_than = Timestamp::from(gc.time_provider.now() + Duration::from_secs(100));

        let mut txn = catalog.catalog.start_transaction().await.unwrap();
        let topic = txn.topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create("gc_remove_old_enough_files", None, topic.id, pool.id)
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = txn
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition = txn
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            created_at: Timestamp::new(1),
            compaction_level: CompactionLevel::Initial,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = txn
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        put_object_store_file(&parquet_file, Arc::clone(&catalog.object_store)).await;

        txn.parquet_files()
            .flag_for_delete(parquet_file.id)
            .await
            .unwrap();

        txn.commit().await.unwrap();

        gc.cleanup(older_than).await.unwrap();

        assert_eq!(
            catalog
                .catalog
                .repositories()
                .await
                .parquet_files()
                .count()
                .await
                .unwrap(),
            0
        );
        let mut list = catalog.object_store.list(None).await.unwrap();
        assert!(list.next().await.is_none());
    }
}
