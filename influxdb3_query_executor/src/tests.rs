use super::CreateQueryExecutorArgs;
use crate::QueryExecutorImpl;
use arrow::array::RecordBatch;
use data_types::NamespaceName;
use datafusion::assert_batches_sorted_eq;
use futures::TryStreamExt;
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider,
    parquet_cache::test_cached_obj_store_and_oracle,
};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_shutdown::ShutdownManager;
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::{
    Bufferer, WriteBuffer,
    persister::Persister,
    write_buffer::{
        N_SNAPSHOTS_TO_LOAD_ON_START, WriteBufferImpl, WriteBufferImplArgs,
        persisted_files::PersistedFiles,
    },
};
use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{MockProvider, Time, TimeProvider};
use metric::Registry;
use object_store::{ObjectStore, local::LocalFileSystem};
use parquet_file::storage::{ParquetStorage, StorageId};
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn make_exec(object_store: Arc<dyn ObjectStore>) -> Arc<Executor> {
    let metrics = Arc::new(metric::Registry::default());

    let parquet_store = ParquetStorage::new(
        Arc::clone(&object_store),
        StorageId::from("test_exec_storage"),
    );

    // Register the object store with both the StorageId and the URL used by DataSourceExecInput
    let mut object_stores = vec![(parquet_store.id(), Arc::clone(parquet_store.object_store()))];
    // Also register with the URL scheme that DataSourceExecInput uses
    object_stores.push((StorageId::from("influxdb3"), Arc::clone(&object_store)));

    Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: object_stores.into_iter().collect(),
            metric_registry: Arc::clone(&metrics),
            // Default to 1gb
            mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new_testing(),
    ))
}

pub(crate) async fn setup(
    query_file_limit: Option<usize>,
    started_with_auth: bool,
) -> (
    Arc<dyn WriteBuffer>,
    QueryExecutorImpl,
    Arc<MockProvider>,
    Arc<SysEventStore>,
) {
    // Set up QueryExecutor
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
        object_store,
        Arc::clone(&time_provider) as _,
        Default::default(),
    );
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "test_host",
        Arc::clone(&time_provider) as _,
        None,
    ));
    let exec = make_exec(Arc::clone(&object_store));
    let node_id = Arc::from("sample-host-id");
    let catalog = Arc::new(
        Catalog::new(
            node_id,
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let shutdown = ShutdownManager::new_testing();
    let write_buffer_impl = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap(),
        distinct_cache: DistinctCacheProvider::new_from_catalog(
            Arc::<MockProvider>::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap(),
        time_provider: Arc::<MockProvider>::clone(&time_provider),
        executor: Arc::clone(&exec),
        wal_config: WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        },
        parquet_cache: Some(parquet_cache),
        metric_registry: Default::default(),
        snapshotted_wal_files_to_keep: 1,
        query_file_limit,
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        shutdown: shutdown.register(),
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let persisted_files: Arc<PersistedFiles> = Arc::clone(&write_buffer_impl.persisted_files());
    let processing_engine_metrics_provider: Arc<Catalog> = Arc::clone(&write_buffer_impl.catalog());
    let telemetry_store = TelemetryStore::new_without_background_runners(
        Some(persisted_files),
        processing_engine_metrics_provider,
    );
    let sys_events_store = Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
        &time_provider,
    )));
    let write_buffer: Arc<dyn WriteBuffer> = write_buffer_impl;
    let metrics = Arc::new(Registry::new());
    let mut datafusion_config = HashMap::new();
    // NB: need to prevent iox_query from injecting a size hint. It currently does so using a
    // bit of a hack, and then strips it out with an additional object store layer. Instead of
    // adding the additional layer, we just avoid using the size hint with this configuration.
    datafusion_config.insert(
        "iox.hint_known_object_size_to_object_store".to_string(),
        false.to_string(),
    );
    let datafusion_config = Arc::new(datafusion_config);
    let query_executor = QueryExecutorImpl::new(CreateQueryExecutorArgs {
        catalog: write_buffer.catalog(),
        write_buffer: Arc::clone(&write_buffer),
        exec,
        metrics,
        datafusion_config,
        query_log_size: 10,
        telemetry_store,
        sys_events_store: Arc::clone(&sys_events_store),
        started_with_auth,
        time_provider: Arc::clone(&time_provider) as _,
        processing_engine: None,
    });

    (
        write_buffer,
        query_executor,
        time_provider,
        sys_events_store,
    )
}

#[test_log::test(tokio::test)]
async fn system_parquet_files_success() {
    let (write_buffer, query_executor, time_provider, _) = setup(None, true).await;
    // Perform some writes to multiple tables
    let db_name = "test_db";
    // perform writes over time to generate WAL files and some snapshots
    // the time provider is bumped to trick the system into persisting files:
    for i in 0..10 {
        let time = i * 10;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
                Time::from_timestamp_nanos(time),
                false,
                influxdb3_write::Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        time_provider.set(Time::from_timestamp(time + 1, 0).unwrap());
    }

    // bump time again and sleep briefly to ensure time to persist things
    time_provider.set(Time::from_timestamp(20, 0).unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;

    struct TestCase<'a> {
        query: &'a str,
        expected: &'a [&'a str],
    }

    let test_cases = [
        TestCase {
            query: "\
                SELECT table_name, size_bytes, row_count, min_time, max_time \
                FROM system.parquet_files \
                WHERE table_name = 'cpu'",
            expected: &[
                "+------------+------------+-----------+----------+----------+",
                "| table_name | size_bytes | row_count | min_time | max_time |",
                "+------------+------------+-----------+----------+----------+",
                "| cpu        | 2089       | 3         | 0        | 20       |",
                "| cpu        | 2089       | 3         | 30       | 50       |",
                "| cpu        | 2089       | 3         | 60       | 80       |",
                "+------------+------------+-----------+----------+----------+",
            ],
        },
        TestCase {
            query: "\
                SELECT table_name, size_bytes, row_count, min_time, max_time \
                FROM system.parquet_files \
                WHERE table_name = 'mem'",
            expected: &[
                "+------------+------------+-----------+----------+----------+",
                "| table_name | size_bytes | row_count | min_time | max_time |",
                "+------------+------------+-----------+----------+----------+",
                "| mem        | 2089       | 3         | 0        | 20       |",
                "| mem        | 2089       | 3         | 30       | 50       |",
                "| mem        | 2089       | 3         | 60       | 80       |",
                "+------------+------------+-----------+----------+----------+",
            ],
        },
        TestCase {
            query: "\
                SELECT table_name, size_bytes, row_count, min_time, max_time \
                FROM system.parquet_files",
            expected: &[
                "+------------+------------+-----------+----------+----------+",
                "| table_name | size_bytes | row_count | min_time | max_time |",
                "+------------+------------+-----------+----------+----------+",
                "| cpu        | 2089       | 3         | 0        | 20       |",
                "| cpu        | 2089       | 3         | 30       | 50       |",
                "| cpu        | 2089       | 3         | 60       | 80       |",
                "| mem        | 2089       | 3         | 0        | 20       |",
                "| mem        | 2089       | 3         | 30       | 50       |",
                "| mem        | 2089       | 3         | 60       | 80       |",
                "+------------+------------+-----------+----------+----------+",
            ],
        },
        TestCase {
            query: "\
                SELECT table_name, size_bytes, row_count, min_time, max_time \
                FROM system.parquet_files \
                LIMIT 4",
            expected: &[
                "+------------+------------+-----------+----------+----------+",
                "| table_name | size_bytes | row_count | min_time | max_time |",
                "+------------+------------+-----------+----------+----------+",
                "| cpu        | 2089       | 3         | 0        | 20       |",
                "| cpu        | 2089       | 3         | 30       | 50       |",
                "| cpu        | 2089       | 3         | 60       | 80       |",
                "| mem        | 2089       | 3         | 60       | 80       |",
                "+------------+------------+-----------+----------+----------+",
            ],
        },
    ];

    for t in test_cases {
        let batch_stream = query_executor
            .query_sql(db_name, t.query, None, None, None)
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        assert_batches_sorted_eq!(t.expected, &batches);
    }
}

#[test_log::test(tokio::test)]
async fn query_file_limits_default() {
    let (write_buffer, query_executor, time_provider, _) = setup(None, true).await;
    // Perform some writes to multiple tables
    let db_name = "test_db";
    // perform writes over time to generate WAL files and some snapshots
    // the time provider is bumped to trick the system into persisting files:
    for i in 0..1298 {
        let time = i * 10;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
                Time::from_timestamp_nanos(time),
                false,
                influxdb3_write::Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        time_provider.set(Time::from_timestamp(time + 1, 0).unwrap());
    }

    // bump time again and sleep briefly to ensure time to persist things
    time_provider.set(Time::from_timestamp(20, 0).unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;

    struct TestCase<'a> {
        query: &'a str,
        expected: &'a [&'a str],
    }

    let test_cases = [
        TestCase {
            query: "\
                SELECT COUNT(*) \
                FROM system.parquet_files \
                WHERE table_name = 'cpu'",
            expected: &[
                "+----------+",
                "| count(*) |",
                "+----------+",
                "| 432      |",
                "+----------+",
            ],
        },
        TestCase {
            query: "\
                SELECT Count(host) \
                FROM cpu",
            expected: &[
                "+-----------------+",
                "| count(cpu.host) |",
                "+-----------------+",
                "| 1298            |",
                "+-----------------+",
            ],
        },
    ];

    for t in test_cases {
        let batch_stream = query_executor
            .query_sql(db_name, t.query, None, None, None)
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        assert_batches_sorted_eq!(t.expected, &batches);
    }

    // put us over the parquet limit
    let time = 12990;
    let _ = write_buffer
        .write_lp(
            NamespaceName::new(db_name).unwrap(),
            "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
            Time::from_timestamp_nanos(time),
            false,
            influxdb3_write::Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

    time_provider.set(Time::from_timestamp(time + 1, 0).unwrap());

    // bump time again and sleep briefly to ensure time to persist things
    time_provider.set(Time::from_timestamp(20, 0).unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;

    match query_executor
        .query_sql(db_name, "SELECT COUNT(host) FROM CPU", None, None, None)
        .await {
        Ok(_) => panic!("expected to exceed parquet file limit, yet query succeeded"),
        Err(err) => assert_eq!(err.to_string(), "error while planning query: External error: Query would scan 432 Parquet files, exceeding the file limit. InfluxDB 3 Core caps file access to prevent performance degradation and memory issues. Use a narrower time range, or increase the limit with --query-file-limit (this may cause slower queries or instability).\n\nTo remove this limitation, upgrade to InfluxDB 3 Enterprise, which automatically compacts files for efficient querying across any time range. Free for non-commercial and home use, and free trials for commercial evaluation: https://www.influxdata.com/downloads".to_string())
    }

    // Make sure if we specify a smaller time range that queries will still work
    query_executor
        .query_sql(
            db_name,
            "SELECT COUNT(host) FROM CPU WHERE time < '1970-01-01T00:00:00.000000010Z'",
            None,
            None,
            None,
        )
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn query_file_limits_configured() {
    let (write_buffer, query_executor, time_provider, _) = setup(Some(3), true).await;
    // Perform some writes to multiple tables
    let db_name = "test_db";
    // perform writes over time to generate WAL files and some snapshots
    // the time provider is bumped to trick the system into persisting files:
    for i in 0..11 {
        let time = i * 10;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
                Time::from_timestamp_nanos(time),
                false,
                influxdb3_write::Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        time_provider.set(Time::from_timestamp(time + 1, 0).unwrap());
    }

    // bump time again and sleep briefly to ensure time to persist things
    time_provider.set(Time::from_timestamp(20, 0).unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;

    struct TestCase<'a> {
        query: &'a str,
        expected: &'a [&'a str],
    }

    let test_cases = [
        TestCase {
            query: "\
                SELECT COUNT(*) \
                FROM system.parquet_files \
                WHERE table_name = 'cpu'",
            expected: &[
                "+----------+",
                "| count(*) |",
                "+----------+",
                "| 3        |",
                "+----------+",
            ],
        },
        TestCase {
            query: "\
                SELECT Count(host) \
                FROM cpu",
            expected: &[
                "+-----------------+",
                "| count(cpu.host) |",
                "+-----------------+",
                "| 11              |",
                "+-----------------+",
            ],
        },
    ];

    for t in test_cases {
        let batch_stream = query_executor
            .query_sql(db_name, t.query, None, None, None)
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        assert_batches_sorted_eq!(t.expected, &batches);
    }

    // put us over the parquet limit
    let time = 120;
    let _ = write_buffer
        .write_lp(
            NamespaceName::new(db_name).unwrap(),
            "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
            Time::from_timestamp_nanos(time),
            false,
            influxdb3_write::Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

    time_provider.set(Time::from_timestamp(time + 1, 0).unwrap());

    // bump time again and sleep briefly to ensure time to persist things
    time_provider.set(Time::from_timestamp(20, 0).unwrap());
    tokio::time::sleep(Duration::from_millis(500)).await;

    match query_executor
        .query_sql(db_name, "SELECT COUNT(host) FROM CPU", None, None, None)
        .await {
        Ok(_) => panic!("expected to exceed parquet file limit, yet query succeeded"),
        Err(err) => assert_eq!(err.to_string(), "error while planning query: External error: Query would scan 3 Parquet files, exceeding the file limit. InfluxDB 3 Core caps file access to prevent performance degradation and memory issues. Use a narrower time range, or increase the limit with --query-file-limit (this may cause slower queries or instability).\n\nTo remove this limitation, upgrade to InfluxDB 3 Enterprise, which automatically compacts files for efficient querying across any time range. Free for non-commercial and home use, and free trials for commercial evaluation: https://www.influxdata.com/downloads".to_string())
    }

    // Make sure if we specify a smaller time range that queries will still work
    query_executor
        .query_sql(
            db_name,
            "SELECT COUNT(host) FROM CPU WHERE time < '1970-01-01T00:00:00.000000010Z'",
            None,
            None,
            None,
        )
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn test_token_permissions_sys_table_query_wrong_db_name() {
    let (write_buffer, query_exec, _, _) = setup(None, true).await;
    write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "\
        cpu,host=a,region=us-east usage=250\n\
        mem,host=a,region=us-east usage=150000\n\
        ",
            Time::from_timestamp_nanos(100),
            false,
            influxdb3_write::Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

    // create an admin token
    write_buffer
        .catalog()
        .create_admin_token(false)
        .await
        .unwrap();

    let query = "select token_id, name, created_at, expiry, permissions, description, created_by_token_id, updated_at, updated_by_token_id FROM system.tokens";

    let stream = query_exec
        // `foo` is present but `system.tokens` is only available in `_internal` db
        .query_sql("foo", query, None, None, None)
        .await;
    assert!(stream.is_err());
}

#[test_log::test(tokio::test)]
async fn test_token_permissions_sys_table_query_with_admin_token() {
    let (write_buffer, query_exec, _, _) = setup(None, true).await;

    // create an admin token
    write_buffer
        .catalog()
        .create_admin_token(false)
        .await
        .unwrap();

    let query = "select token_id, name, created_at, expiry, permissions, description, created_by_token_id, updated_at, updated_by_token_id FROM system.tokens";

    let stream = query_exec
        .query_sql("_internal", query, None, None, None)
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_batches_sorted_eq!(
        [
            "+----------+--------+---------------------+--------+-------------+-------------+---------------------+------------+---------------------+",
            "| token_id | name   | created_at          | expiry | permissions | description | created_by_token_id | updated_at | updated_by_token_id |",
            "+----------+--------+---------------------+--------+-------------+-------------+---------------------+------------+---------------------+",
            "| 0        | _admin | 1970-01-01T00:00:00 |        | *:*:*       |             |                     |            |                     |",
            "+----------+--------+---------------------+--------+-------------+-------------+---------------------+------------+---------------------+",
        ],
        &batches
    );
}

#[test_log::test(tokio::test)]
async fn test_token_permissions_sys_table_query_with_auth() {
    let (write_buffer, query_exec, _, _) = setup(None, true).await;

    // create an admin token
    write_buffer
        .catalog()
        .create_admin_token(false)
        .await
        .unwrap();

    let query = "select * FROM system.tokens";

    let stream = query_exec
        .query_sql("_internal", query, None, None, None)
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    // schema should contain hash when started with auth
    for batch in batches {
        assert!(
            batch
                .schema()
                .fields
                .iter()
                .any(|field| field.name() == "hash")
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_token_permissions_sys_table_query_without_auth() {
    let (write_buffer, query_exec, _, _) = setup(None, false).await;

    // create an admin token
    write_buffer
        .catalog()
        .create_admin_token(false)
        .await
        .unwrap();

    let query = "select * FROM system.tokens";

    let stream = query_exec
        .query_sql("_internal", query, None, None, None)
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    // schema should _not_ contain hash when started without auth
    for batch in batches {
        assert!(
            !batch
                .schema()
                .fields
                .iter()
                .any(|field| field.name() == "hash")
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_influxql_time_filter_double_group_by() {
    use influxdb3_write::test_helpers::do_write;
    use iox_query_influxql_rewrite as rewrite;

    let (wb, qe, tp, ..) = setup(None, false).await;
    do_write(wb.as_ref(), "test_db", "test_table,t1=a val=1", tp.now()).await;

    for (query_str, expected) in [
        (
            "select * from test_table where time < 1337ms",
            vec![
                "+------------------+---------------------+----+-----+",
                "| iox::measurement | time                | t1 | val |",
                "+------------------+---------------------+----+-----+",
                "| test_table       | 1970-01-01T00:00:00 | a  | 1.0 |",
                "+------------------+---------------------+----+-----+",
            ],
        ),
        (
            "select sum(val) from test_table where time < 1337ms group by time(1d)",
            vec![
                "+------------------+---------------------+-----+",
                "| iox::measurement | time                | sum |",
                "+------------------+---------------------+-----+",
                "| test_table       | 1970-01-01T00:00:00 | 1.0 |",
                "+------------------+---------------------+-----+",
            ],
        ),
        (
            "\
            select \
                sum(a)/sum(b) as foo \
            from (\
                select \
                    sum(val) as a, \
                    last(val) as b, \
                    time \
                from test_table \
                where time >= 0ms \
                    and time <= 1337 \
                group by time(1d)\
            ) group by time(1d)\
            limit 1", // NB(tjh): add a limit because otherwise the result is massive
            vec![
                "+------------------+---------------------+-----+",
                "| iox::measurement | time                | foo |",
                "+------------------+---------------------+-----+",
                "| test_table       | 1970-01-01T00:00:00 | 1.0 |",
                "+------------------+---------------------+-----+",
            ],
        ),
    ] {
        let statement = rewrite::parse_statements(query_str)
            .unwrap()
            .pop()
            .unwrap()
            .to_statement();
        let stream = qe
            .query_influxql("test_db", query_str, statement, None, None, None)
            .await
            .expect("query should work");
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_batches_sorted_eq!(expected, &batches);
    }
}
