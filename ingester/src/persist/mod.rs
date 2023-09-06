//! The persistence subsystem; abstractions, types, and implementation.

pub(crate) mod backpressure;
pub(super) mod compact;
pub(crate) mod completion_observer;
mod context;
pub(crate) mod drain_buffer;
pub(crate) mod file_metrics;
pub(crate) mod handle;
pub(crate) mod hot_partitions;
pub mod queue;
mod worker;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{CompactionLevel, ParquetFile, SortedColumnSet};
    use futures::TryStreamExt;
    use iox_catalog::{
        interface::{get_schema_by_id, Catalog, SoftDeletedRows},
        mem::MemCatalog,
        validate_or_insert_schema,
    };
    use iox_query::exec::Executor;
    use lazy_static::lazy_static;
    use metric::{Attributes, DurationHistogram, Metric, U64Counter, U64Gauge};
    use object_store::{memory::InMemory, ObjectMeta, ObjectStore};
    use parking_lot::Mutex;
    use parquet_file::{
        storage::{ParquetStorage, StorageId},
        ParquetFilePath,
    };
    use test_helpers::{maybe_start_logging, timeout::FutureTimeout};

    use crate::{
        buffer_tree::{
            partition::{resolver::CatalogPartitionResolver, PartitionData, SortKeyState},
            post_write::mock::MockPostWriteObserver,
            BufferTree,
        },
        dml_payload::IngestOp,
        dml_sink::DmlSink,
        ingest_state::IngestState,
        persist::handle::PersistHandle,
        persist::{completion_observer::mock::MockCompletionObserver, queue::PersistQueue},
        test_util::{
            make_write_op, populate_catalog, ARBITRARY_NAMESPACE_NAME,
            ARBITRARY_NAMESPACE_NAME_PROVIDER, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_PROVIDER,
        },
    };

    lazy_static! {
        static ref EXEC: Arc<Executor> = Arc::new(Executor::new_testing());
    }

    /// Generate a [`PartitionData`] containing one write, and populate the
    /// catalog such that the schema is set (by validating the schema) and the
    /// partition entry exists (by driving the buffer tree to create it).
    async fn partition_with_write(catalog: Arc<dyn Catalog>) -> Arc<Mutex<PartitionData>> {
        // Create the namespace in the catalog and it's the schema
        let (namespace_id, table_id) =
            populate_catalog(&*catalog, &ARBITRARY_NAMESPACE_NAME, &ARBITRARY_TABLE_NAME).await;

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::clone(&*ARBITRARY_NAMESPACE_NAME_PROVIDER),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            Arc::new(CatalogPartitionResolver::new(Arc::clone(&catalog))),
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
        );

        let write = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            namespace_id,
            &ARBITRARY_TABLE_NAME,
            table_id,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );

        let mut repos = catalog
            .repositories()
            .with_timeout_panic(Duration::from_secs(1))
            .await;

        let schema = get_schema_by_id(namespace_id, &mut *repos, SoftDeletedRows::AllRows)
            .await
            .expect("failed to find namespace schema");

        // Insert the schema elements into the catalog
        validate_or_insert_schema(
            write
                .tables()
                .map(|(_id, data)| (&***ARBITRARY_TABLE_NAME, data.partitioned_data().data())),
            &schema,
            &mut *repos,
        )
        .await
        .expect("populating schema elements failed");

        drop(repos); // Don't you love this testing-only deadlock bug? #3859

        // Apply the write
        buf.apply(IngestOp::Write(write))
            .await
            .expect("failed to apply write to buffer");

        // And finally return the namespace
        buf.partitions().next().unwrap()
    }

    #[track_caller]
    pub(super) fn assert_metric_gauge(metrics: &metric::Registry, name: &'static str, value: u64) {
        let v = metrics
            .get_instrument::<Metric<U64Gauge>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(v, value, "metric {name} had value {v} want {value}");
    }

    #[track_caller]
    pub(super) fn assert_metric_counter(
        metrics: &metric::Registry,
        name: &'static str,
        value: u64,
    ) {
        let v = metrics
            .get_instrument::<Metric<U64Counter>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(v, value, "metric {name} had value {v} want {value}");
    }

    #[track_caller]
    pub(super) fn assert_metric_histogram(
        metrics: &metric::Registry,
        name: &'static str,
        hits: u64,
    ) {
        let v = metrics
            .get_instrument::<Metric<DurationHistogram>>(name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch()
            .sample_count();

        assert_eq!(v, hits, "metric {name} had {v} samples want {hits}");
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
        let completion_observer = Arc::new(MockCompletionObserver::default());

        // Initialise the persist system.
        let handle = PersistHandle::new(
            1,
            2,
            Arc::clone(&ingest_state),
            Arc::new(Executor::new_testing()),
            storage,
            Arc::clone(&catalog),
            Arc::clone(&completion_observer),
            &metrics,
        );
        assert!(ingest_state.read().is_ok());

        // Generate a partition with data
        let partition = partition_with_write(Arc::clone(&catalog)).await;
        let table_id = partition.lock().table_id();
        let partition_id = partition.lock().partition_id().clone();
        let namespace_id = partition.lock().namespace_id();
        assert_matches!(partition.lock().sort_key(), SortKeyState::Provided(None));

        // Transition it to "persisting".
        let data = partition
            .lock()
            .mark_persisting()
            .expect("partition with write should transition to persisting");

        // Assert the starting metric values.
        assert_metric_histogram(&metrics, "ingester_persist_active_duration", 0);
        assert_metric_histogram(&metrics, "ingester_persist_enqueue_duration", 0);

        // Enqueue the persist job
        let notify = handle.enqueue(Arc::clone(&partition), data).await;
        assert!(ingest_state.read().is_ok());

        assert_metric_counter(&metrics, "ingester_persist_enqueued_jobs", 1);

        // Wait for the persist to complete.
        notify
            .with_timeout(Duration::from_secs(10))
            .await
            .expect("timeout waiting for completion notification")
            .expect("worker task failed");

        // Assert the notification observer saw this persist operation finish.
        assert_matches!(&completion_observer.calls().as_slice(), &[n] => {
            assert_eq!(n.namespace_id(), namespace_id);
            assert_eq!(n.table_id(), table_id);
            assert_eq!(n.partition_id(), &partition_id);
            assert_eq!(n.sequence_numbers().len(), 1);
        });

        // And that metrics recorded the enqueue & completion
        assert_metric_histogram(&metrics, "ingester_persist_active_duration", 1);
        assert_metric_histogram(&metrics, "ingester_persist_enqueue_duration", 1);

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
            .list_by_partition_not_to_delete(&partition_id)
            .await
            .expect("query for parquet files failed");

        // Validate a single file was inserted with the expected properties.
        let (object_store_id, file_size_bytes) = assert_matches!(&*files, [ParquetFile {
                namespace_id: got_namespace_id,
                table_id: got_table_id,
                partition_id: got_partition_id,
                object_store_id,
                row_count,
                compaction_level,
                file_size_bytes,
                created_at,
                max_l0_created_at,
                ..
            }] =>
            {
                assert_eq!(created_at.get(), max_l0_created_at.get());

                assert_eq!(got_namespace_id, &namespace_id);
                assert_eq!(got_table_id, &table_id);
                assert_eq!(got_partition_id, &partition_id);

                assert_eq!(*row_count, 1);
                assert_eq!(compaction_level, &CompactionLevel::Initial);

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
                assert_eq!(size, *file_size_bytes as usize);
            }
        )
    }

    /// An integration test covering concurrent catalog sort key updates,
    /// discovered at persist time.
    #[tokio::test]
    async fn test_persist_integration_concurrent_sort_key_update() {
        maybe_start_logging();

        let object_storage: Arc<dyn ObjectStore> = Arc::new(InMemory::default());
        let storage = ParquetStorage::new(Arc::clone(&object_storage), StorageId::from("iox"));
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ingest_state = Arc::new(IngestState::default());
        let completion_observer = Arc::new(MockCompletionObserver::default());

        // Initialise the persist system.
        let handle = PersistHandle::new(
            1,
            2,
            Arc::clone(&ingest_state),
            Arc::new(Executor::new_testing()),
            storage,
            Arc::clone(&catalog),
            Arc::clone(&completion_observer),
            &metrics,
        );
        assert!(ingest_state.read().is_ok());

        // Generate a partition with data
        let partition = partition_with_write(Arc::clone(&catalog)).await;
        let table_id = partition.lock().table_id();
        let partition_id = partition.lock().partition_id().clone();
        let namespace_id = partition.lock().namespace_id();
        assert_matches!(partition.lock().sort_key(), SortKeyState::Provided(None));

        // Transition it to "persisting".
        let data = partition
            .lock()
            .mark_persisting()
            .expect("partition with write should transition to persisting");

        // Update the sort key in the catalog, causing the persist job to
        // discover the change during the persist.
        let updated_partition = catalog
            .repositories()
            .await
            .partitions()
            .cas_sort_key(
                &partition_id,
                None,
                // must use column names that exist in the partition data
                &["region"],
                // column id of region
                &SortedColumnSet::from([2]),
            )
            .await
            .expect("failed to set catalog sort key");
        // Test: sort_key_ids after updating
        assert_eq!(
            updated_partition.sort_key_ids(),
            Some(&SortedColumnSet::from([2]))
        );

        // Enqueue the persist job
        let notify = handle.enqueue(Arc::clone(&partition), data).await;
        assert!(ingest_state.read().is_ok());

        // Wait for the persist to complete.
        notify
            .with_timeout(Duration::from_secs(10))
            .await
            .expect("timeout waiting for completion notification")
            .expect("worker task failed");

        // Assert the notification observer was invoked exactly once, with the
        // successful persist output.
        assert_matches!(&completion_observer.calls().as_slice(), &[n] => {
            assert_eq!(n.namespace_id(), namespace_id);
            assert_eq!(n.table_id(), table_id);
            assert_eq!(n.partition_id(), &partition_id);
            assert_eq!(n.sequence_numbers().len(), 1);
        });

        // And that despite the persist job effectively running twice (to handle
        // the sort key conflict) the metrics should record 1 persist job start
        // & completion
        assert_metric_histogram(&metrics, "ingester_persist_active_duration", 1);
        assert_metric_histogram(&metrics, "ingester_persist_enqueue_duration", 1);

        // Assert the partition persistence count increased, an indication that
        // mark_persisted() was called.
        assert_eq!(partition.lock().completed_persistence_count(), 1);

        // Assert the sort key was also updated, adding the new columns (time) to the
        // end of the concurrently updated catalog sort key.
        assert_matches!(partition.lock().sort_key(), SortKeyState::Provided(Some(p)) => {
            // Before there is only ["region"] (manual sort key update above). Now ["region", "time"]
            assert_eq!(p.to_columns().collect::<Vec<_>>(), &["region", "time"]);
        });

        // Ensure a file was made visible in the catalog
        let files = catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_partition_not_to_delete(&partition_id)
            .await
            .expect("query for parquet files failed");

        // Validate a single file was inserted with the expected properties.
        let (object_store_id, file_size_bytes) = assert_matches!(&*files, [ParquetFile {
                namespace_id: got_namespace_id,
                table_id: got_table_id,
                partition_id: got_partition_id,
                object_store_id,
                row_count,
                compaction_level,
                file_size_bytes,
                created_at,
                max_l0_created_at,
                ..
            }] =>
            {
                assert_eq!(created_at.get(), max_l0_created_at.get());

                assert_eq!(got_namespace_id, &namespace_id);
                assert_eq!(got_table_id, &table_id);
                assert_eq!(got_partition_id, &partition_id);

                assert_eq!(*row_count, 1);
                assert_eq!(compaction_level, &CompactionLevel::Initial);

                (object_store_id, file_size_bytes)
            }
        );

        // Validate the files exists in object storage.
        let files: Vec<ObjectMeta> = object_storage
            .list(None)
            .await
            .expect("listing object storage failed")
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to list object store files");

        // Two files should have been uploaded - first the one that observed the
        // concurrent sort key update, and then the resorted file using the
        // observed sort key.
        assert_eq!(files.len(), 2, "expected two uploaded files");

        // Ensure the catalog record points at a valid file in object storage.
        let want_path =
            ParquetFilePath::new(namespace_id, table_id, &partition_id, *object_store_id)
                .object_store_path();
        let file = files
            .into_iter()
            .find(|f| f.location == want_path)
            .expect("did not find final file in object storage");

        assert_eq!(file.size, *file_size_bytes as usize);
    }
}
