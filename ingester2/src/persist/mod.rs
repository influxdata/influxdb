pub(crate) mod backpressure;
pub(super) mod compact;
mod context;
pub(crate) mod drain_buffer;
pub(crate) mod handle;
pub(crate) mod hot_partitions;
pub mod queue;
mod worker;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{CompactionLevel, ParquetFile, PartitionKey, ShardId};
    use dml::DmlOperation;
    use futures::TryStreamExt;
    use iox_catalog::{
        interface::{get_schema_by_id, Catalog},
        mem::MemCatalog,
        validate_or_insert_schema,
    };
    use iox_query::exec::Executor;
    use lazy_static::lazy_static;
    use object_store::{memory::InMemory, ObjectMeta, ObjectStore};
    use parking_lot::Mutex;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use test_helpers::{maybe_start_logging, timeout::FutureTimeout};

    use crate::{
        buffer_tree::{
            namespace::name_resolver::mock::MockNamespaceNameProvider,
            partition::{resolver::CatalogPartitionResolver, PartitionData, SortKeyState},
            post_write::mock::MockPostWriteObserver,
            table::name_resolver::mock::MockTableNameProvider,
            BufferTree,
        },
        dml_sink::DmlSink,
        ingest_state::IngestState,
        persist::queue::PersistQueue,
        test_util::{make_write_op, populate_catalog},
        TRANSITION_SHARD_INDEX,
    };

    use super::handle::PersistHandle;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);

    lazy_static! {
        static ref EXEC: Arc<Executor> = Arc::new(Executor::new_testing());
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("bananas");
    }

    /// Generate a [`PartitionData`] containing one write, and populate the
    /// catalog such that the schema is set (by validating the schema) and the
    /// partition entry exists (by driving the buffer tree to create it).
    async fn partition_with_write(catalog: Arc<dyn Catalog>) -> Arc<Mutex<PartitionData>> {
        // Create the namespace in the catalog and it's the schema
        let (_shard_id, namespace_id, table_id) = populate_catalog(
            &*catalog,
            TRANSITION_SHARD_INDEX,
            NAMESPACE_NAME,
            TABLE_NAME,
        )
        .await;

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::default()),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            Arc::new(CatalogPartitionResolver::new(Arc::clone(&catalog))),
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
            TRANSITION_SHARD_ID,
        );

        let write = make_write_op(
            &PARTITION_KEY,
            namespace_id,
            TABLE_NAME,
            table_id,
            0,
            r#"bananas,region=Asturias temp=35 4242424242"#,
        );

        let mut repos = catalog
            .repositories()
            .with_timeout_panic(Duration::from_secs(1))
            .await;

        let schema = get_schema_by_id(namespace_id, &mut *repos)
            .await
            .expect("failed to find namespace schema");

        // Insert the schema elements into the catalog
        validate_or_insert_schema(
            write.tables().map(|(_id, data)| (TABLE_NAME, data)),
            &schema,
            &mut *repos,
        )
        .await
        .expect("populating schema elements failed");

        drop(repos); // Don't you love this testing-only deadlock bug? #3859

        // Apply the write
        buf.apply(DmlOperation::Write(write))
            .await
            .expect("failed to apply write to buffer");

        // And finally return the namespace
        buf.partitions().next().unwrap()
    }

    /// A complete integration test of the persistence system components.
    #[tokio::test]
    async fn test_persist_integration() {
        maybe_start_logging();

        let object_storage: Arc<dyn ObjectStore> = Arc::new(InMemory::default());
        let storage = ParquetStorage::new(Arc::clone(&object_storage), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ingest_state = Arc::new(IngestState::default());

        // Initialise the persist system.
        let handle = PersistHandle::new(
            1,
            2,
            Arc::clone(&ingest_state),
            Arc::clone(&EXEC),
            storage,
            Arc::clone(&catalog),
            &metrics,
        );
        assert!(ingest_state.read().is_ok());

        // Generate a partition with data
        let partition = partition_with_write(Arc::clone(&catalog)).await;
        let table_id = partition.lock().table_id();
        let partition_id = partition.lock().partition_id();
        let namespace_id = partition.lock().namespace_id();
        assert_matches!(partition.lock().sort_key(), SortKeyState::Provided(None));

        // Transition it to "persisting".
        let data = partition
            .lock()
            .mark_persisting()
            .expect("partition with write should transition to persisting");

        // Enqueue the persist job
        let notify = handle.enqueue(Arc::clone(&partition), data).await;
        assert!(ingest_state.read().is_ok());

        // Wait for the persist to complete.
        notify
            .with_timeout(Duration::from_secs(10))
            .await
            .expect("timeout waiting for completion notification")
            .expect("worker task failed");

        // Assert the partition persistence count increased, an indication that
        // mark_persisted() was called.
        assert_eq!(partition.lock().completed_persistence_count(), 1);

        // Assert the sort key was also updated
        assert_matches!(partition.lock().sort_key(), SortKeyState::Provided(Some(p)) => {
            assert_eq!(p.to_columns().collect::<Vec<_>>(), &["region", "time"]);
        });

        // Ensure a file was made visible in the catalog
        let files = catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_partition_not_to_delete(partition_id)
            .await
            .expect("query for parquet files failed");

        // Validate a single file was inserted with the expected properties.
        let (object_store_id, file_size_bytes) = assert_matches!(&*files, &[ParquetFile {
                namespace_id: got_namespace_id,
                table_id: got_table_id,
                partition_id: got_partition_id,
                object_store_id,
                max_sequence_number,
                row_count,
                compaction_level,
                file_size_bytes,
                ..
            }] =>
            {
                assert_eq!(got_namespace_id, namespace_id);
                assert_eq!(got_table_id, table_id);
                assert_eq!(got_partition_id, partition_id);

                assert_eq!(row_count, 1);
                assert_eq!(compaction_level, CompactionLevel::Initial);

                assert_eq!(max_sequence_number.get(), 0); // Unused, dummy value

                (object_store_id, file_size_bytes)
            }
        );

        // Validate the file exists in object storage.
        let files: Vec<ObjectMeta> = object_storage
            .list(None)
            .await
            .expect("listing object storage failed")
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to list object store files");

        assert_matches!(
            &*files,
            &[ObjectMeta {
                ref location,
                size,
                ..
            }] => {
                let want_path = format!("{object_store_id}.parquet");
                assert!(location.as_ref().ends_with(&want_path));
                assert_eq!(size, file_size_bytes as usize);
            }
        )
    }
}
