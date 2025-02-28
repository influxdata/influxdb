mod common;

use crate::common::build_parquet_cache_prefetcher;
use arrow_util::assert_batches_sorted_eq;
use datafusion_util::config::register_iox_object_store;
use executor::DedicatedExecutor;
use futures::FutureExt;
use futures::StreamExt;
use hashbrown::HashMap;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_config::EnterpriseConfig;
use influxdb3_enterprise_buffer::WriteBufferEnterprise;
use influxdb3_enterprise_buffer::modes::read_write::{CreateReadWriteModeArgs, ReadWriteMode};
use influxdb3_enterprise_buffer::replica::ReplicationConfig;
use influxdb3_enterprise_compactor::producer::CompactionCleaner;
use influxdb3_enterprise_compactor::producer::{CompactedDataProducer, CompactedDataProducerArgs};
use influxdb3_enterprise_compactor::{
    consumer::CompactedDataConsumer, sys_events::CompactionEventStore,
};
use influxdb3_enterprise_data_layout::CompactionConfig;
use influxdb3_sys_events::SysEventStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::persister::Persister;
use influxdb3_write::test_helpers::WriteBufferTester;
use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
use influxdb3_write::{Precision, WriteBuffer};
use iox_query::exec::{Executor, ExecutorConfig};
use iox_time::{MockProvider, SystemProvider, Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use observability_deps::tracing::info;
use parquet_file::storage::{ParquetStorage, StorageId};
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "flakey test as the 208 data point for some reason isn't always there (see comment below)"]
async fn two_writers_gen1_compaction() {
    let metrics = Arc::new(metric::Registry::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    let exec = make_exec_and_register_runtime(Arc::clone(&object_store), Arc::clone(&metrics));

    let node1_id = "node1";
    let node2_id = "node2";

    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // small snapshot size will have parquet written out after 3 WAL periods:
        snapshot_size: 1,
    };

    let node1_persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node1_id,
        Arc::clone(&time_provider),
    ));
    let node1_catalog = Arc::new(node1_persister.load_or_create_catalog().await.unwrap());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&node1_catalog)).unwrap();
    let distinct_cache = DistinctCacheProvider::new_from_catalog(
        Arc::clone(&time_provider),
        Arc::clone(&node1_catalog),
    )
    .unwrap();

    let node2_persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node2_id,
        Arc::clone(&time_provider),
    ));
    let node2_catalog = node2_persister.load_or_create_catalog().await.unwrap();
    let sys_events_store: Arc<dyn CompactionEventStore> =
        Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let node2_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&node2_persister),
        catalog: Arc::new(node2_catalog),
        last_cache: Arc::clone(&last_cache),
        distinct_cache: Arc::clone(&distinct_cache),
        time_provider: Arc::clone(&time_provider),
        executor: Arc::clone(&exec),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::clone(&metrics),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
    })
    .await
    .unwrap();

    let compactor_id = "compact".into();
    let compaction_config =
        CompactionConfig::new(&[2], Duration::from_secs(120)).with_per_file_row_limit(10);
    let obj_store = Arc::new(InMemory::new());
    let parquet_cache_prefetcher = build_parquet_cache_prefetcher(&obj_store);

    let compaction_producer = CompactedDataProducer::new(CompactedDataProducerArgs {
        span_ctx: None,
        compactor_id,
        node_ids: vec!["node1".to_string(), "node2".to_string()],
        compaction_config,
        enterprise_config: Default::default(),
        datafusion_config: Default::default(),
        object_store,
        object_store_url: node1_persister.object_store_url().clone(),
        executor: Arc::clone(&exec),
        parquet_cache_prefetcher,
        sys_events_store: Arc::clone(&sys_events_store),
        time_provider: Arc::clone(&time_provider),
    })
    .await
    .unwrap();

    let read_write_mode = Arc::new(
        WriteBufferEnterprise::read_write(CreateReadWriteModeArgs {
            node_id: node1_id.into(),
            persister: Arc::clone(&node1_persister),
            catalog: Arc::clone(&node1_catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::new(SystemProvider::new()),
            executor: Arc::clone(&exec),
            wal_config,
            metric_registry: Arc::clone(&metrics),
            replication_config: Some(ReplicationConfig::new(
                Duration::from_millis(10),
                vec![node2_id.to_string()],
            )),
            parquet_cache: None,
            compacted_data: Some(Arc::clone(&compaction_producer.compacted_data)),
            snapshotted_wal_files_to_keep: 10,
        })
        .await
        .unwrap(),
    );

    let compacted_data = Arc::clone(&compaction_producer.compacted_data);

    // run the compactor on the DataFusion executor, but don't drop it
    let _t = exec
        .executor()
        .spawn(async move {
            compaction_producer
                .run_compaction_loop(Duration::from_millis(10), Duration::from_millis(30))
                .await;
        })
        .boxed();

    // each call to do_writes will force a snapshot. We want to do two for each node,
    // which will then trigger a compaction. We also want to do one more snapshot each
    // so that we'll have non-compacted files show up in this query too.
    do_writes(read_write_mode.as_ref(), node1_id, 0, 1).await;
    do_writes(node2_buffer.as_ref(), node2_id, 0, 2).await;
    do_writes(read_write_mode.as_ref(), node1_id, 1, 1).await;
    do_writes(node2_buffer.as_ref(), node2_id, 1, 2).await;
    do_writes(read_write_mode.as_ref(), node1_id, 2, 1).await;
    do_writes(node2_buffer.as_ref(), node2_id, 2, 2).await;

    // wait for two compactions to happen
    let mut count = 0;
    loop {
        let db_schema = compacted_data
            .compacted_catalog
            .db_schema("test_db")
            .unwrap();
        let table_id = db_schema.table_name_to_id("m1").unwrap();
        if let Some(detail) = compacted_data.compaction_detail(db_schema.id, table_id) {
            if detail.sequence_number.as_u64() > 1 {
                // we should have a single compacted generation
                assert_eq!(
                    detail.compacted_generations.len(),
                    1,
                    "should have a single generation. compaction details: {:?}",
                    detail
                );
                // we should have 1 leftover gen1 file from node1, which is the trailing chunk
                // from the 120 time block.
                assert_eq!(
                    detail.leftover_gen1_files.len(),
                    1,
                    "should have one leftover gen1 file. details: {:?}",
                    detail
                );
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        count += 1;
        if count > 30 {
            panic!("compaction did not happen");
        }
    }

    // query and make sure all the data is there
    let ctx = exec.new_context();
    let batches = read_write_mode
        .get_record_batches_unchecked("test_db", "m1", &ctx)
        .await;

    // I don't know why this test is flakey at this point. It has something to do with the last
    // row that gets written from node 2, which is this:
    //             "| 208.0 | 1970-01-01T00:02:00.000000208Z | node2 |",
    // That row should be in the write buffer in the same buffer chunk as 206 and 207, but it's not.
    // I think this may be something unrelated to compaction and more related to how the write
    // buffer works. But this definitely deserves deeper investigation.

    assert_batches_sorted_eq!(
        [
            "+-------+--------------------------------+---------+",
            "| f1    | time                           | w       |",
            "+-------+--------------------------------+---------+",
            "| 103.0 | 1970-01-01T00:01:00.000000103Z | node1 |",
            "| 104.0 | 1970-01-01T00:01:00.000000104Z | node1 |",
            "| 105.0 | 1970-01-01T00:01:00.000000105Z | node1 |",
            "| 106.0 | 1970-01-01T00:01:00.000000106Z | node2 |",
            "| 107.0 | 1970-01-01T00:01:00.000000107Z | node2 |",
            "| 108.0 | 1970-01-01T00:01:00.000000108Z | node2 |",
            "| 203.0 | 1970-01-01T00:02:00.000000203Z | node1 |",
            "| 204.0 | 1970-01-01T00:02:00.000000204Z | node1 |",
            "| 205.0 | 1970-01-01T00:02:00.000000205Z | node1 |",
            "| 206.0 | 1970-01-01T00:02:00.000000206Z | node2 |",
            "| 207.0 | 1970-01-01T00:02:00.000000207Z | node2 |",
            "| 208.0 | 1970-01-01T00:02:00.000000208Z | node2 |",
            "| 3.0   | 1970-01-01T00:00:00.000000003Z | node1 |",
            "| 4.0   | 1970-01-01T00:00:00.000000004Z | node1 |",
            "| 5.0   | 1970-01-01T00:00:00.000000005Z | node1 |",
            "| 6.0   | 1970-01-01T00:00:00.000000006Z | node2 |",
            "| 7.0   | 1970-01-01T00:00:00.000000007Z | node2 |",
            "| 8.0   | 1970-01-01T00:00:00.000000008Z | node2 |",
            "+-------+--------------------------------+---------+",
        ],
        &batches
    );
}

#[test_log::test(tokio::test)]
async fn compact_consumer_picks_up_latest_summary() {
    // setup
    let metrics = Arc::new(metric::Registry::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let exec = make_exec_and_register_runtime(Arc::clone(&object_store), Arc::clone(&metrics));

    // create two write buffers to write data that will be compacted:
    let mut write_buffers = HashMap::new();
    for node_id in ["spock", "tuvok"] {
        let b = setup_write_buffer(
            node_id,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Arc::clone(&exec),
            Arc::clone(&metrics),
        )
        .await;
        write_buffers.insert(node_id, b);
    }

    // make a bnch of writes to them:
    for i in 0..10 {
        do_writes(&write_buffers["spock"], "spock", i, i).await;
        do_writes(&write_buffers["tuvok"], "tuvok", i + 100, i).await;
    }

    // setup a compaction producer to do the compaciton:
    let compactor_id = Arc::<str>::from("com");
    let sys_events_store: Arc<dyn CompactionEventStore> =
        Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let compaction_config =
        CompactionConfig::new(&[2], Duration::from_secs(120)).with_per_file_row_limit(10);
    let persister = Persister::new(
        Arc::clone(&object_store),
        compactor_id.as_ref(),
        Arc::clone(&time_provider),
    );
    let compaction_producer = CompactedDataProducer::new(CompactedDataProducerArgs {
        span_ctx: None,
        compactor_id: Arc::clone(&compactor_id),
        node_ids: vec!["spock".to_string(), "tuvok".to_string()],
        compaction_config,
        enterprise_config: Default::default(),
        datafusion_config: Default::default(),
        object_store: Arc::clone(&object_store),
        object_store_url: persister.object_store_url().clone(),
        executor: Arc::clone(&exec),
        parquet_cache_prefetcher: None,
        sys_events_store: Arc::clone(&sys_events_store),
        time_provider: Arc::clone(&time_provider),
    })
    .await
    .unwrap();

    // run the compactor on the DataFusion executor, but don't drop the future:
    let compacted_data = Arc::clone(&compaction_producer.compacted_data);
    let _t = exec
        .executor()
        .spawn(async move {
            compaction_producer
                .run_compaction_loop(Duration::from_millis(10), Duration::from_millis(30))
                .await;
        })
        .boxed();

    // setup a compaction consumer on which we want to check for updated summaries:
    let consumer = Arc::new(
        CompactedDataConsumer::new(
            Arc::clone(&compactor_id),
            Arc::clone(&object_store),
            None,
            Arc::clone(&sys_events_store),
        )
        .await
        .unwrap(),
    );

    // spin off a task to refresh the compaction consumer:
    let consumer_cloned = Arc::clone(&consumer);
    tokio::spawn(async move {
        consumer_cloned
            .poll_in_background(Duration::from_millis(10))
            .await
    });

    // wait for some compactions to happen by checking the producer:
    let mut count = 0;
    loop {
        let db_schema = compacted_data
            .compacted_catalog
            .db_schema("test_db")
            .unwrap();
        let table_id = db_schema.table_name_to_id("m1").unwrap();
        if let Some(detail) = compacted_data.compaction_detail(db_schema.id, table_id) {
            if detail.sequence_number.as_u64() > 1 {
                // we should have some compacted generations:
                assert!(
                    !detail.compacted_generations.is_empty(),
                    "should have compacted generations. compaction details: {:?}",
                    detail
                );
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        count += 1;
        if count > 30 {
            panic!("compaction did not happen");
        }
    }

    // now wait for the compactor consumer to update to a new summary:
    // in the reproducer, this will panic, see:
    // https://github.com/influxdata/influxdb_pro/issues/293
    let mut count = 0;
    loop {
        let summary = consumer.compacted_data.compaction_summary();
        info!(?summary, "compaction summary");
        if summary.compaction_sequence_number.as_u64() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        count += 1;
        if count > 30 {
            panic!("The compaction consumer's summary never incremented");
        }
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
/// Test that old files are deleted as compaction happens
async fn compaction_cleanup() {
    let metrics = Arc::new(metric::Registry::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // small snapshot size will have parquet written out after 3 WAL periods:
        snapshot_size: 1,
    };
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
        },
        DedicatedExecutor::new_testing(),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    let compactor_id = "compactor";

    let compactor_persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        compactor_id,
        Arc::clone(&time_provider),
    ));
    let compactor_catalog = Arc::new(compactor_persister.load_or_create_catalog().await.unwrap());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&compactor_catalog)).unwrap();
    let distinct_cache = DistinctCacheProvider::new_from_catalog(
        Arc::clone(&time_provider),
        Arc::clone(&compactor_catalog),
    )
    .unwrap();

    let node_id = "host";

    let compaction_config = CompactionConfig::new(&[2], Duration::from_secs(120));
    let generation_levels = compaction_config.compaction_levels();
    let obj_store = Arc::new(InMemory::new());
    let parquet_cache_prefetcher = build_parquet_cache_prefetcher(&obj_store);

    let sys_events_store: Arc<dyn CompactionEventStore> =
        Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let compaction_producer = CompactedDataProducer::new(CompactedDataProducerArgs {
        span_ctx: None,
        compactor_id: compactor_id.into(),
        node_ids: vec![node_id.to_string()],
        compaction_config,
        enterprise_config: Arc::new(EnterpriseConfig::default()),
        datafusion_config: Arc::new(std::collections::HashMap::new()),
        object_store: Arc::clone(&object_store),
        object_store_url: compactor_persister.object_store_url().clone(),
        executor: Arc::clone(&exec),
        parquet_cache_prefetcher,
        sys_events_store: Arc::clone(&sys_events_store),
        time_provider: Arc::clone(&time_provider),
    })
    .await
    .unwrap();

    let writer_persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node_id,
        Arc::clone(&time_provider),
    ));
    let writer_catalog = Arc::new(writer_persister.load_or_create_catalog().await.unwrap());

    let read_write_mode = Arc::new(
        WriteBufferEnterprise::read_write(CreateReadWriteModeArgs {
            node_id: node_id.into(),
            persister: Arc::clone(&writer_persister),
            catalog: Arc::clone(&writer_catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::new(SystemProvider::new()),
            executor: Arc::clone(&exec),
            wal_config,
            metric_registry: Arc::clone(&metrics),
            replication_config: None,
            parquet_cache: None,
            compacted_data: Some(Arc::clone(&compaction_producer.compacted_data)),
            snapshotted_wal_files_to_keep: 0,
        })
        .await
        .unwrap(),
    );

    // run the the DataFusion executor, but don't drop it and setup the initial data
    let ex = exec.executor();

    for i in 0..20 {
        do_writes(read_write_mode.as_ref(), node_id, i, 1).await;
    }

    // Make sure all of the parquet files are persisted by the write buffer
    tokio::time::sleep(Duration::from_secs(2)).await;
    async fn list_files(
        prefix: Option<&str>,
        object_store: Arc<dyn ObjectStore>,
    ) -> BTreeSet<String> {
        object_store
            .list(prefix.map(|p| p.into()).as_ref())
            .fold(
                Ok(BTreeSet::new()),
                |acc: Result<BTreeSet<String>, object_store::Error>, file| async move {
                    let mut acc = acc?;
                    acc.insert(file?.location.to_string());
                    Ok(acc)
                },
            )
            .await
            .unwrap()
    }

    let compaction_producer = Arc::new(compaction_producer);
    // Compact the data 4 times. Beyond this no compaction runs for the input data
    for _ in 0..4 {
        let com_pro = Arc::clone(&compaction_producer);
        let gen_levels = generation_levels.clone();
        let sys_events = Arc::clone(&sys_events_store);
        ex.spawn(async move {
            com_pro
                .plan_and_run_compaction(&gen_levels, sys_events)
                .await
        })
        .await
        .unwrap()
        .unwrap();
    }

    let to_delete = mem::take(&mut *compaction_producer.to_delete.lock());

    ex.spawn(
        CompactionCleaner::new(Arc::clone(&object_store), to_delete, Duration::from_secs(0))
            .data_deletion(),
    )
    .await
    .unwrap();

    let compactor_c = list_files(Some("compactor/c"), Arc::clone(&object_store)).await;
    let compactor_cd = list_files(Some("compactor/cd"), Arc::clone(&object_store)).await;
    let compactor_cs = list_files(Some("compactor/cs"), Arc::clone(&object_store)).await;

    assert_eq!(
        compactor_c,
        BTreeSet::from([
            "compactor/c/4d/7fa/0e/27.json".into(),
            "compactor/c/4d/7fa/0e/35.parquet".into(),
            "compactor/c/81/7d4/7a/23.json".into(),
            "compactor/c/81/7d4/7a/27.parquet".into(),
            "compactor/c/8a/b13/30/25.json".into(),
            "compactor/c/8a/b13/30/31.parquet".into(),
            "compactor/c/a3/f42/4a0/21.json".into(),
            "compactor/c/a3/f42/4a0/23.parquet".into(),
            "compactor/c/ea/abb/43d/20.json".into(),
            "compactor/c/ea/abb/43d/21.parquet".into(),
        ]),
    );
    assert_eq!(
        compactor_cd,
        BTreeSet::from(["compactor/cd/1/1/18446744073709551607.json".into(),]),
    );
    assert_eq!(
        compactor_cs,
        BTreeSet::from(["compactor/cs/18446744073709551607.json".into()]),
    );

    // query and make sure all the data is there
    let ctx = exec.new_context();
    let batches = read_write_mode
        .get_record_batches_unchecked("test_db", "m1", &ctx)
        .await;
    assert_batches_sorted_eq!(
        [
            "+--------+--------------------------------+------+",
            "| f1     | time                           | w    |",
            "+--------+--------------------------------+------+",
            "| 1003.0 | 1970-01-01T00:10:00.000001003Z | host |",
            "| 1004.0 | 1970-01-01T00:10:00.000001004Z | host |",
            "| 1005.0 | 1970-01-01T00:10:00.000001005Z | host |",
            "| 103.0  | 1970-01-01T00:01:00.000000103Z | host |",
            "| 104.0  | 1970-01-01T00:01:00.000000104Z | host |",
            "| 105.0  | 1970-01-01T00:01:00.000000105Z | host |",
            "| 1103.0 | 1970-01-01T00:11:00.000001103Z | host |",
            "| 1104.0 | 1970-01-01T00:11:00.000001104Z | host |",
            "| 1105.0 | 1970-01-01T00:11:00.000001105Z | host |",
            "| 1203.0 | 1970-01-01T00:12:00.000001203Z | host |",
            "| 1204.0 | 1970-01-01T00:12:00.000001204Z | host |",
            "| 1205.0 | 1970-01-01T00:12:00.000001205Z | host |",
            "| 1303.0 | 1970-01-01T00:13:00.000001303Z | host |",
            "| 1304.0 | 1970-01-01T00:13:00.000001304Z | host |",
            "| 1305.0 | 1970-01-01T00:13:00.000001305Z | host |",
            "| 1403.0 | 1970-01-01T00:14:00.000001403Z | host |",
            "| 1404.0 | 1970-01-01T00:14:00.000001404Z | host |",
            "| 1405.0 | 1970-01-01T00:14:00.000001405Z | host |",
            "| 1503.0 | 1970-01-01T00:15:00.000001503Z | host |",
            "| 1504.0 | 1970-01-01T00:15:00.000001504Z | host |",
            "| 1505.0 | 1970-01-01T00:15:00.000001505Z | host |",
            "| 1603.0 | 1970-01-01T00:16:00.000001603Z | host |",
            "| 1604.0 | 1970-01-01T00:16:00.000001604Z | host |",
            "| 1605.0 | 1970-01-01T00:16:00.000001605Z | host |",
            "| 1703.0 | 1970-01-01T00:17:00.000001703Z | host |",
            "| 1704.0 | 1970-01-01T00:17:00.000001704Z | host |",
            "| 1705.0 | 1970-01-01T00:17:00.000001705Z | host |",
            "| 1803.0 | 1970-01-01T00:18:00.000001803Z | host |",
            "| 1804.0 | 1970-01-01T00:18:00.000001804Z | host |",
            "| 1805.0 | 1970-01-01T00:18:00.000001805Z | host |",
            "| 1903.0 | 1970-01-01T00:19:00.000001903Z | host |",
            "| 1904.0 | 1970-01-01T00:19:00.000001904Z | host |",
            "| 1905.0 | 1970-01-01T00:19:00.000001905Z | host |",
            "| 203.0  | 1970-01-01T00:02:00.000000203Z | host |",
            "| 204.0  | 1970-01-01T00:02:00.000000204Z | host |",
            "| 205.0  | 1970-01-01T00:02:00.000000205Z | host |",
            "| 3.0    | 1970-01-01T00:00:00.000000003Z | host |",
            "| 303.0  | 1970-01-01T00:03:00.000000303Z | host |",
            "| 304.0  | 1970-01-01T00:03:00.000000304Z | host |",
            "| 305.0  | 1970-01-01T00:03:00.000000305Z | host |",
            "| 4.0    | 1970-01-01T00:00:00.000000004Z | host |",
            "| 403.0  | 1970-01-01T00:04:00.000000403Z | host |",
            "| 404.0  | 1970-01-01T00:04:00.000000404Z | host |",
            "| 405.0  | 1970-01-01T00:04:00.000000405Z | host |",
            "| 5.0    | 1970-01-01T00:00:00.000000005Z | host |",
            "| 503.0  | 1970-01-01T00:05:00.000000503Z | host |",
            "| 504.0  | 1970-01-01T00:05:00.000000504Z | host |",
            "| 505.0  | 1970-01-01T00:05:00.000000505Z | host |",
            "| 603.0  | 1970-01-01T00:06:00.000000603Z | host |",
            "| 604.0  | 1970-01-01T00:06:00.000000604Z | host |",
            "| 605.0  | 1970-01-01T00:06:00.000000605Z | host |",
            "| 703.0  | 1970-01-01T00:07:00.000000703Z | host |",
            "| 704.0  | 1970-01-01T00:07:00.000000704Z | host |",
            "| 705.0  | 1970-01-01T00:07:00.000000705Z | host |",
            "| 803.0  | 1970-01-01T00:08:00.000000803Z | host |",
            "| 804.0  | 1970-01-01T00:08:00.000000804Z | host |",
            "| 805.0  | 1970-01-01T00:08:00.000000805Z | host |",
            "| 903.0  | 1970-01-01T00:09:00.000000903Z | host |",
            "| 904.0  | 1970-01-01T00:09:00.000000904Z | host |",
            "| 905.0  | 1970-01-01T00:09:00.000000905Z | host |",
            "+--------+--------------------------------+------+",
        ],
        &batches
    );
}

fn make_exec_and_register_runtime(
    object_store: Arc<dyn ObjectStore>,
    metrics: Arc<Registry>,
) -> Arc<Executor> {
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
        },
        DedicatedExecutor::new_testing(),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
    exec
}

async fn setup_write_buffer(
    node_id: &str,
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    executor: Arc<Executor>,
    metric_registry: Arc<Registry>,
) -> WriteBufferEnterprise<ReadWriteMode> {
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node_id,
        Arc::clone(&time_provider),
    ));
    let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .unwrap();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // small snapshot size will have parquet written out after 3 WAL periods:
        snapshot_size: 1,
    };
    WriteBufferEnterprise::read_write(CreateReadWriteModeArgs {
        node_id: node_id.into(),
        persister,
        catalog,
        last_cache,
        distinct_cache,
        time_provider,
        executor,
        wal_config,
        metric_registry,
        replication_config: None,
        parquet_cache: None,
        compacted_data: None,
        snapshotted_wal_files_to_keep: 10,
    })
    .await
    .unwrap()
}

async fn do_writes(
    buffer: &(impl WriteBuffer + ?Sized),
    writer: &str,
    minute: i64,
    writer_offet: i64,
) {
    let db = data_types::NamespaceName::new("test_db").unwrap();

    let number_of_writes = 3;
    let writer_offset = writer_offet * number_of_writes;

    for i in 0..number_of_writes {
        let val = i + writer_offset + (100 * minute);
        let time = (minute * 60 * 1_000_000_000) + val;
        let data = format!("m1,w={} f1={} {}", writer, val, time);
        buffer
            .write_lp(
                db.clone(),
                &data,
                Time::from_timestamp_nanos(time),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();
    }
}
