use super::*;
use crate::Precision;
use crate::test_helpers::TestWriter;
use crate::write_buffer::validator::WriteValidator;
use datafusion_util::config::register_iox_object_store;
use executor::{DedicatedExecutor, register_current_runtime_for_io};
use influxdb3_catalog::resource::CatalogResource;
use influxdb3_wal::{Gen1Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
use iox_query::exec::{ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use parquet::arrow::arrow_reader;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::num::NonZeroUsize;

#[tokio::test]
async fn snapshot_works_with_not_all_columns_in_buffer() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let metrics = Arc::new(metric::Registry::default());

    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            // Default to 1gb
            mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new_testing(),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
    register_current_runtime_for_io();

    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let catalog = Arc::new(
        Catalog::new(
            "hosta",
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "hosta",
        time_provider,
        None,
    ));
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    let queryable_buffer_args = QueryableBufferArgs {
        executor: Arc::clone(&exec),
        catalog: Arc::clone(&catalog),
        persister: Arc::clone(&persister),
        last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap(),
        distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap(),
        persisted_files: Arc::new(PersistedFiles::new(None)),
        parquet_cache: None,
    };
    let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

    let db = data_types::NamespaceName::new("testdb").unwrap();

    // create the initial write with two tags
    let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog)).unwrap();
    let lp = format!(
        "foo,t2=a,t1=b f1=1i {}",
        time_provider.now().timestamp_nanos()
    );

    let lines = val
        .v1_parse_lines_and_catalog_updates(&lp, false, time_provider.now(), Precision::Nanosecond)
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_1m());
    let batch: WriteBatch = lines.into();
    let wal_contents = WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns: batch.min_time_ns,
        max_timestamp_ns: batch.max_time_ns,
        wal_file_number: WalFileSequenceNumber::new(1),
        ops: vec![WalOp::Write(batch)],
        snapshot: None,
    };
    let end_time =
        wal_contents.max_timestamp_ns + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

    // write the lp into the buffer
    queryable_buffer.notify(Arc::new(wal_contents)).await;

    // now force a snapshot, persisting the data to parquet file. Also, buffer up a new write
    let snapshot_sequence_number = SnapshotSequenceNumber::new(1);
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number,
        end_time_marker: end_time,
        first_wal_sequence_number: WalFileSequenceNumber::new(1),
        last_wal_sequence_number: WalFileSequenceNumber::new(2),
        forced: false,
    };

    // create another write, this time with only one tag, in a different gen1 block
    let lp = "foo,t2=b f1=1i 240000000000";
    let val = WriteValidator::initialize(db, Arc::clone(&catalog)).unwrap();

    let lines = val
        .v1_parse_lines_and_catalog_updates(lp, false, time_provider.now(), Precision::Nanosecond)
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_1m());
    let batch: WriteBatch = lines.into();
    let wal_contents = WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns: batch.min_time_ns,
        max_timestamp_ns: batch.max_time_ns,
        wal_file_number: WalFileSequenceNumber::new(2),
        ops: vec![WalOp::Write(batch)],
        snapshot: None,
    };
    let end_time =
        wal_contents.max_timestamp_ns + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

    let details = queryable_buffer
        .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
        .await;
    let _details = details.await.unwrap();

    // validate we have a single persisted file
    let db = catalog.db_schema("testdb").unwrap();
    let table = db.table_definition("foo").unwrap();
    assert_eq!(
        table.sort_key,
        SortKey::from_columns(vec!["t2", "t1", "time"])
    );
    let files = queryable_buffer
        .persisted_files
        .get_files(db.id, table.table_id);
    assert_eq!(files.len(), 1);

    // now force another snapshot, persisting the data to parquet file
    let snapshot_sequence_number = SnapshotSequenceNumber::new(2);
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number,
        end_time_marker: end_time,
        first_wal_sequence_number: WalFileSequenceNumber::new(3),
        last_wal_sequence_number: WalFileSequenceNumber::new(3),
        forced: false,
    };
    queryable_buffer
        .notify_and_snapshot(
            Arc::new(WalContents {
                persist_timestamp_ms: 0,
                min_timestamp_ns: 0,
                max_timestamp_ns: 0,
                wal_file_number: WalFileSequenceNumber::new(3),
                ops: vec![],
                snapshot: Some(snapshot_details),
            }),
            snapshot_details,
        )
        .await
        .await
        .unwrap();

    // validate we have two persisted files
    let files = queryable_buffer
        .persisted_files
        .get_files(db.id, table.table_id);
    assert_eq!(files.len(), 2);

    // Verify the `iox::series::key` metadata is present in the parquet file
    {
        let path = Path::parse(&files[0].path).expect("path should be parseable");
        let res = object_store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let metadata =
            arrow_reader::ArrowReaderMetadata::load(&res, ArrowReaderOptions::new()).unwrap();
        let schema: Schema = Schema::try_from(Arc::clone(metadata.schema())).unwrap();
        let primary_key = schema.primary_key();
        assert_eq!(primary_key, &["t2", "t1", "time"]);
    }
}

/// This test validates that buffer replay ignores data from deleted tables.
#[tokio::test]
async fn snapshot_skips_deleted_table() {
    // Setup test infrastructure
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let metrics = Arc::new(metric::Registry::default());
    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            mem_pool_size: 1024 * 1024 * 1024,
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new_testing(),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
    register_current_runtime_for_io();

    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let catalog = Arc::new(
        Catalog::new(
            "hosta",
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap(),
    );

    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "hosta",
        Arc::clone(&time_provider) as _,
        None,
    ));

    let queryable_buffer_args = QueryableBufferArgs {
        executor: Arc::clone(&exec),
        catalog: Arc::clone(&catalog),
        persister: Arc::clone(&persister),
        last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap(),
        distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider) as _,
            Arc::clone(&catalog),
        )
        .await
        .unwrap(),
        persisted_files: Arc::new(PersistedFiles::new(None)),
        parquet_cache: None,
    };
    let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

    let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
    let lines1 = writer
        .write_lp_to_write_batch("table1,tag=a value=1i 1000", 0)
        .await;
    let lines2 = writer
        .write_lp_to_write_batch("table2,tag=b value=2i 2000", 0)
        .await;

    let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
    let db_id = db_schema.id();
    let table1_id = db_schema.table_name_to_id("table1").unwrap();
    let table2_id = db_schema.table_name_to_id("table2").unwrap();

    // Soft delete the second table
    use influxdb3_catalog::catalog::HardDeletionTime;
    catalog
        .soft_delete_table(TestWriter::DB_NAME, "table2", HardDeletionTime::Now)
        .await
        .unwrap();

    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        end_time_marker: 1000,
        first_wal_sequence_number: WalFileSequenceNumber::new(1),
        last_wal_sequence_number: WalFileSequenceNumber::new(2),
        forced: false,
    };

    let wal_contents = influxdb3_wal::create::wal_contents_with_snapshot(
        (0, 100, 1),
        [
            influxdb3_wal::create::write_batch_op(lines1),
            influxdb3_wal::create::write_batch_op(lines2),
        ],
        snapshot_details,
    );

    queryable_buffer
        .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
        .await
        .await
        .unwrap();

    // Verify only table1 has persisted files
    let files1 = queryable_buffer.persisted_files.get_files(db_id, table1_id);
    assert_eq!(files1.len(), 1, "Should have 1 persisted file for table1");

    let files2 = queryable_buffer.persisted_files.get_files(db_id, table2_id);
    assert_eq!(
        files2.len(),
        0,
        "Soft deleted table should not have persisted files"
    );
}
