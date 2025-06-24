use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

use arrow_array::RecordBatch;
use data_types::NamespaceName;
use datafusion::assert_batches_sorted_eq;
use futures::TryStreamExt;
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_server::query_executor::{CreateQueryExecutorArgs, QueryExecutorImpl};
use influxdb3_shutdown::ShutdownManager;
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::{
    Precision, WriteBuffer,
    persister::Persister,
    write_buffer::{N_SNAPSHOTS_TO_LOAD_ON_START, WriteBufferImpl, WriteBufferImplArgs},
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{MockProvider, Time, TimeProvider};
use metric::Registry;
use object_store::{ObjectStore, memory::InMemory};
use parquet_file::storage::{ParquetStorage, StorageId};

/// Test that when duplicates are written into the write buffer, the query results do not contain
/// duplicate rows
#[test_log::test(tokio::test)]
async fn test_deduplicate_rows_in_write_buffer_memory() {
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // use a large snapshot size so that queries are served only by in-memory buffer chunks:
        snapshot_size: 100,
        ..Default::default()
    };
    let service = TestService::setup(wal_config).await;

    // do the same writes every time:
    let writes = [
        TestWrite::lp("bar val=1", 1),
        TestWrite::lp("bar val=2", 2),
        TestWrite::lp("bar val=3", 3),
    ];

    // query results should always be the same:
    let expected = [
        "+---------------------+-----+",
        "| time                | val |",
        "+---------------------+-----+",
        "| 1970-01-01T00:00:01 | 1.0 |",
        "| 1970-01-01T00:00:02 | 2.0 |",
        "| 1970-01-01T00:00:03 | 3.0 |",
        "+---------------------+-----+",
    ];

    // do the writes several times:
    for _ in 0..5 {
        service.do_writes("foo", writes.clone()).await;
        let batches = service.query_sql("foo", "select * from bar").await;
        assert_batches_sorted_eq!(expected, &batches);
    }

    // check the explain plan:
    let batches = service.query_sql("foo", "explain select * from bar").await;
    let plan = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    // checking that the DeduplicateExec is above the RecordBatchesExec in the query plan:
    insta::assert_snapshot!(plan);
}

/// Test that when duplicates are written into the write buffer, across snapshot points, i.e., so
/// that there are duplicate rows contained across multiple parquet files, the query results do not
/// contain duplicate rows
#[test_log::test(tokio::test)]
async fn test_deduplicate_rows_in_write_buffer_parquet() {
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // uses a small snapshot size to get parquet persisted early, and therefore have queries
        // serve chunks from both in-memory buffer and persisted parquet
        snapshot_size: 1,
        ..Default::default()
    };
    let service = TestService::setup(wal_config).await;

    // do the same 3 writes every time:
    let writes = [
        TestWrite::lp("bar val=1", 1),
        TestWrite::lp("bar val=2", 2),
        TestWrite::lp("bar val=3", 3),
    ];

    // query result should always be the same:
    let expected = [
        "+---------------------+-----+",
        "| time                | val |",
        "+---------------------+-----+",
        "| 1970-01-01T00:00:01 | 1.0 |",
        "| 1970-01-01T00:00:02 | 2.0 |",
        "| 1970-01-01T00:00:03 | 3.0 |",
        "+---------------------+-----+",
    ];

    // do the writes and query several times:
    for i in 1..5 {
        // write then wait for a snapshot to ensure parquet
        // is persisted:
        service.do_writes("foo", writes.clone()).await;
        service.wait_for_snapshot_sequence(i).await;

        let batches = service.query_sql("foo", "select * from bar").await;
        assert_batches_sorted_eq!(expected, &batches);
    }

    // check the query plan to ensure that deduplication is taking place:
    let batches = service.query_sql("foo", "explain select * from bar").await;
    let plan = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    // looking to see that the `DeduplicateExec` is above the `ParquetExec` to ensure data from
    // multiple parquet files is deduplicated:
    insta::assert_snapshot!(plan);
}

#[test_log::test(tokio::test)]
async fn test_deduplicate_rows_in_write_buffer_both_memory_and_parquet() {
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // uses a small snapshot size to get parquet persisted early, and therefore have queries
        // serve chunks from both in-memory buffer and persisted parquet
        snapshot_size: 1,
        ..Default::default()
    };
    let service = TestService::setup(wal_config).await;

    service
        .do_writes(
            "foo",
            [
                TestWrite::lp("bar value=false", 1),
                TestWrite::lp("bar value=false", 2),
                TestWrite::lp("bar value=false", 3),
            ],
        )
        .await;

    service.wait_for_snapshot_sequence(1).await;

    let batches = service.query_sql("foo", "select * from bar").await;
    assert_batches_sorted_eq!(
        [
            "+---------------------+-------+",
            "| time                | value |",
            "+---------------------+-------+",
            "| 1970-01-01T00:00:01 | false |", // note that this is `false`
            "| 1970-01-01T00:00:02 | false |",
            "| 1970-01-01T00:00:03 | false |",
            "+---------------------+-------+",
        ],
        &batches
    );

    // write a duplicate row, but don't trigger a snapshot, so subsequent query will be served
    // by chunks in both memory buffer and persisted parquet; this also flips the `value` to `true`
    // to show that the most recent written line appears in the query result:
    service
        .do_writes("foo", [TestWrite::lp("bar value=true", 1)])
        .await;

    let batches = service.query_sql("foo", "select * from bar").await;
    assert_batches_sorted_eq!(
        [
            "+---------------------+-------+",
            "| time                | value |",
            "+---------------------+-------+",
            "| 1970-01-01T00:00:01 | true  |", // note that this is now `true`
            "| 1970-01-01T00:00:02 | false |",
            "| 1970-01-01T00:00:03 | false |",
            "+---------------------+-------+",
        ],
        &batches
    );

    let batches = service.query_sql("foo", "explain select * from bar").await;
    // There should be a union of a ParquetExec and RecordBatchesExec that feeds into a
    // DeduplicateExec in the query plan, i.e., there is both in-memory buffer chunks and persisted
    // parquet chunks:
    let plan = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    insta::assert_snapshot!(plan);
}

struct TestService {
    query_executor: Arc<dyn QueryExecutor>,
    write_buffer: Arc<dyn WriteBuffer>,
    _time_provider: Arc<dyn TimeProvider>,
    _metrics: Arc<Registry>,
    _object_store: Arc<dyn ObjectStore>,
}

impl TestService {
    async fn setup(wal_config: WalConfig) -> Self {
        let node_id = "test-node";
        let object_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metrics = Arc::new(Registry::new());
        let catalog = Arc::new(
            Catalog::new(
                node_id,
                Arc::clone(&object_store) as _,
                Arc::clone(&time_provider) as _,
                Arc::clone(&metrics),
            )
            .await
            .unwrap(),
        );
        let parquet_store =
            ParquetStorage::new(Arc::clone(&object_store) as _, StorageId::from("influxdb3"));
        let exec = Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                mem_pool_size: usize::MAX,
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
                heap_memory_limit: None,
            },
            DedicatedExecutor::new_testing(),
        ));
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store) as _,
            node_id,
            Arc::clone(&time_provider) as _,
        ));
        let write_buffer: Arc<dyn WriteBuffer> = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog: Arc::clone(&catalog),
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                .await
                .unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider) as _,
                Arc::clone(&catalog),
            )
            .await
            .unwrap(),
            time_provider: Arc::clone(&time_provider) as _,
            executor: Arc::clone(&exec),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::clone(&metrics),
            snapshotted_wal_files_to_keep: 100,
            query_file_limit: None,
            shutdown: ShutdownManager::new_testing().register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: Some(1),
        })
        .await
        .unwrap();
        let sys_events_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider) as _));
        let telemetry_store =
            TelemetryStore::new_without_background_runners(None, Arc::clone(&catalog) as _);
        let mut datafusion_config = HashMap::new();
        datafusion_config.insert(
            "iox.hint_known_object_size_to_object_store".to_string(),
            false.to_string(),
        );
        let query_executor = Arc::new(QueryExecutorImpl::new(CreateQueryExecutorArgs {
            catalog: Arc::clone(&catalog),
            write_buffer: Arc::clone(&write_buffer) as _,
            exec: Arc::clone(&exec),
            metrics: Arc::clone(&metrics),
            datafusion_config: Arc::new(datafusion_config),
            query_log_size: 10,
            telemetry_store: Arc::clone(&telemetry_store),
            sys_events_store: Arc::clone(&sys_events_store),
            started_with_auth: false,
            time_provider: Arc::clone(&time_provider) as _,
        }));

        Self {
            query_executor,
            write_buffer,
            _time_provider: time_provider,
            _metrics: metrics,
            _object_store: object_store,
        }
    }
}

#[derive(Clone)]
struct TestWrite<LP> {
    lp: LP,
    time_seconds: i64,
}

impl<LP> TestWrite<LP>
where
    LP: AsRef<str>,
{
    fn lp(lp: LP, time_seconds: i64) -> Self {
        Self { lp, time_seconds }
    }
}

impl TestService {
    async fn do_writes<LP: AsRef<str>>(
        &self,
        db: &'static str,
        writes: impl IntoIterator<Item = TestWrite<LP>>,
    ) {
        for w in writes {
            self.write_buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    false,
                    Precision::Nanosecond,
                    false,
                )
                .await
                .unwrap();
        }
    }

    async fn query_sql(&self, db: &str, query_str: &str) -> Vec<RecordBatch> {
        let stream = self
            .query_executor
            .query_sql(db, query_str, None, None, None)
            .await
            .unwrap();
        stream.try_collect().await.unwrap()
    }

    async fn wait_for_snapshot_sequence(&self, wait_for: u64) {
        let mut count = 0;
        loop {
            let last = self
                .write_buffer
                .wal()
                .last_snapshot_sequence_number()
                .await
                .as_u64();
            if last >= wait_for {
                break;
            }
            count += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if count > 100 {
                panic!("waited too long for a snapshot with sequence {wait_for}");
            }
        }
    }
}
