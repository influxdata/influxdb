mod common;

use crate::common::build_parquet_cache_prefetcher;
use arrow_array::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use datafusion::execution::context::SessionContext;
use datafusion_util::config::register_iox_object_store;
use executor::DedicatedExecutor;
use futures::FutureExt;
use hashbrown::HashMap;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_cache::meta_cache::MetaCacheProvider;
use influxdb3_enterprise_buffer::modes::read_write::{CreateReadWriteModeArgs, ReadWriteMode};
use influxdb3_enterprise_buffer::replica::ReplicationConfig;
use influxdb3_enterprise_buffer::WriteBufferEnterprise;
use influxdb3_enterprise_compactor::producer::{CompactedDataProducer, CompactedDataProducerArgs};
use influxdb3_enterprise_compactor::{
    consumer::CompactedDataConsumer, sys_events::CompactionEventStore,
};
use influxdb3_enterprise_data_layout::CompactionConfig;
use influxdb3_sys_events::SysEventStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
use influxdb3_write::{ChunkContainer, Precision, WriteBuffer};
use iox_query::exec::{Executor, ExecutorConfig};
use iox_query::QueryChunk;
use iox_time::{MockProvider, SystemProvider, Time, TimeProvider};
use metric::Registry;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use observability_deps::tracing::info;
use parquet_file::storage::{ParquetStorage, StorageId};
use pretty_assertions::assert_eq;
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

    let writer1_id = "writer1";
    let writer2_id = "writer2";

    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // small snapshot size will have parquet written out after 3 WAL periods:
        snapshot_size: 1,
    };

    let writer1_persister = Arc::new(Persister::new(Arc::clone(&object_store), writer1_id));
    let writer1_catalog = Arc::new(writer1_persister.load_or_create_catalog().await.unwrap());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&writer1_catalog)).unwrap();
    let meta_cache = MetaCacheProvider::new_from_catalog(
        Arc::clone(&time_provider),
        Arc::clone(&writer1_catalog),
    )
    .unwrap();

    let writer2_persister = Arc::new(Persister::new(Arc::clone(&object_store), writer2_id));
    let writer2_catalog = writer2_persister.load_or_create_catalog().await.unwrap();
    let sys_events_store: Arc<dyn CompactionEventStore> =
        Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let writer2_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&writer2_persister),
        catalog: Arc::new(writer2_catalog),
        last_cache: Arc::clone(&last_cache),
        meta_cache: Arc::clone(&meta_cache),
        time_provider: Arc::clone(&time_provider),
        executor: Arc::clone(&exec),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::clone(&metrics),
        plugin_dir: None,
    })
    .await
    .unwrap();

    let compactor_id = "compact".into();
    let compaction_config =
        CompactionConfig::new(&[2], Duration::from_secs(120)).with_per_file_row_limit(10);
    let obj_store = Arc::new(InMemory::new());
    let parquet_cache_prefetcher = build_parquet_cache_prefetcher(&obj_store);

    let compaction_producer = CompactedDataProducer::new(CompactedDataProducerArgs {
        compactor_id,
        hosts: vec!["writer1".to_string(), "writer2".to_string()],
        compaction_config,
        enterprise_config: Default::default(),
        datafusion_config: Default::default(),
        object_store,
        object_store_url: writer1_persister.object_store_url().clone(),
        executor: Arc::clone(&exec),
        parquet_cache_prefetcher,
        sys_events_store: Arc::clone(&sys_events_store),
    })
    .await
    .unwrap();

    let read_write_mode = Arc::new(
        WriteBufferEnterprise::read_write(CreateReadWriteModeArgs {
            host_id: writer1_id.into(),
            persister: Arc::clone(&writer1_persister),
            catalog: Arc::clone(&writer1_catalog),
            last_cache,
            meta_cache,
            time_provider: Arc::new(SystemProvider::new()),
            executor: Arc::clone(&exec),
            wal_config,
            metric_registry: Arc::clone(&metrics),
            replication_config: Some(ReplicationConfig::new(
                Duration::from_millis(10),
                vec![writer2_id.to_string()],
            )),
            parquet_cache: None,
            compacted_data: Some(Arc::clone(&compaction_producer.compacted_data)),
            plugin_dir: None,
        })
        .await
        .unwrap(),
    );

    let compaction_producer = Arc::new(compaction_producer);
    let compaction_producer_clone = Arc::clone(&compaction_producer);

    // run the compactor on the DataFusion executor, but don't drop it
    let _t = exec
        .executor()
        .spawn(async move {
            compaction_producer_clone
                .run_compaction_loop(Duration::from_millis(10))
                .await;
        })
        .boxed();

    // each call to do_writes will force a snapshot. We want to do two for each writer,
    // which will then trigger a compaction. We also want to do one more snapshot each
    // so that we'll have non-compacted files show up in this query too.
    do_writes(read_write_mode.as_ref(), writer1_id, 0, 1).await;
    do_writes(writer2_buffer.as_ref(), writer2_id, 0, 2).await;
    do_writes(read_write_mode.as_ref(), writer1_id, 1, 1).await;
    do_writes(writer2_buffer.as_ref(), writer2_id, 1, 2).await;
    do_writes(read_write_mode.as_ref(), writer1_id, 2, 1).await;
    do_writes(writer2_buffer.as_ref(), writer2_id, 2, 2).await;

    // wait for two compactions to happen
    let mut count = 0;
    loop {
        let db_schema = compaction_producer
            .compacted_data
            .compacted_catalog
            .db_schema("test_db")
            .unwrap();
        let table_id = db_schema.table_name_to_id("m1").unwrap();
        if let Some(detail) = compaction_producer
            .compacted_data
            .compaction_detail(db_schema.id, table_id)
        {
            if detail.sequence_number.as_u64() > 1 {
                // we should have a single compacted generation
                assert_eq!(
                    detail.compacted_generations.len(),
                    1,
                    "should have a single generation. compaction details: {:?}",
                    detail
                );
                // we should have 1 leftover gen1 file from writer1, which is the trailing chunk
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
    let chunks = read_write_mode
        .get_table_chunks("test_db", "m1", &[], None, &ctx.inner().state())
        .unwrap();

    // I don't know why this test is flakey at this point. It has something to do with the last
    // row that gets written from writer 2, which is this:
    //             "| 208.0 | 1970-01-01T00:02:00.000000208Z | writer2 |",
    // That row should be in the write buffer in the same buffer chunk as 206 and 207, but it's not.
    // I think this may be something unrelated to compaction and more related to how the write
    // buffer works. But this definitely deserves deeper investigation.

    let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
    assert_batches_sorted_eq!(
        [
            "+-------+--------------------------------+---------+",
            "| f1    | time                           | w       |",
            "+-------+--------------------------------+---------+",
            "| 103.0 | 1970-01-01T00:01:00.000000103Z | writer1 |",
            "| 104.0 | 1970-01-01T00:01:00.000000104Z | writer1 |",
            "| 105.0 | 1970-01-01T00:01:00.000000105Z | writer1 |",
            "| 106.0 | 1970-01-01T00:01:00.000000106Z | writer2 |",
            "| 107.0 | 1970-01-01T00:01:00.000000107Z | writer2 |",
            "| 108.0 | 1970-01-01T00:01:00.000000108Z | writer2 |",
            "| 203.0 | 1970-01-01T00:02:00.000000203Z | writer1 |",
            "| 204.0 | 1970-01-01T00:02:00.000000204Z | writer1 |",
            "| 205.0 | 1970-01-01T00:02:00.000000205Z | writer1 |",
            "| 206.0 | 1970-01-01T00:02:00.000000206Z | writer2 |",
            "| 207.0 | 1970-01-01T00:02:00.000000207Z | writer2 |",
            "| 208.0 | 1970-01-01T00:02:00.000000208Z | writer2 |",
            "| 3.0   | 1970-01-01T00:00:00.000000003Z | writer1 |",
            "| 4.0   | 1970-01-01T00:00:00.000000004Z | writer1 |",
            "| 5.0   | 1970-01-01T00:00:00.000000005Z | writer1 |",
            "| 6.0   | 1970-01-01T00:00:00.000000006Z | writer2 |",
            "| 7.0   | 1970-01-01T00:00:00.000000007Z | writer2 |",
            "| 8.0   | 1970-01-01T00:00:00.000000008Z | writer2 |",
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
    for host_id in ["spock", "tuvok"] {
        let b = setup_write_buffer(
            host_id,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Arc::clone(&exec),
            Arc::clone(&metrics),
        )
        .await;
        write_buffers.insert(host_id, b);
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
    let persister = Persister::new(Arc::clone(&object_store), compactor_id.as_ref());
    let compaction_producer = Arc::new(
        CompactedDataProducer::new(CompactedDataProducerArgs {
            compactor_id: Arc::clone(&compactor_id),
            hosts: vec!["spock".to_string(), "tuvok".to_string()],
            compaction_config,
            enterprise_config: Default::default(),
            datafusion_config: Default::default(),
            object_store: Arc::clone(&object_store),
            object_store_url: persister.object_store_url().clone(),
            executor: Arc::clone(&exec),
            parquet_cache_prefetcher: None,
            sys_events_store: Arc::clone(&sys_events_store),
        })
        .await
        .unwrap(),
    );

    // run the compactor on the DataFusion executor, but don't drop the future:
    let compaction_producer_cloned = Arc::clone(&compaction_producer);
    let _t = exec
        .executor()
        .spawn(async move {
            compaction_producer_cloned
                .run_compaction_loop(Duration::from_millis(10))
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
        let db_schema = compaction_producer
            .compacted_data
            .compacted_catalog
            .db_schema("test_db")
            .unwrap();
        let table_id = db_schema.table_name_to_id("m1").unwrap();
        if let Some(detail) = compaction_producer
            .compacted_data
            .compaction_detail(db_schema.id, table_id)
        {
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
    host_id: &str,
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    executor: Arc<Executor>,
    metric_registry: Arc<Registry>,
) -> WriteBufferEnterprise<ReadWriteMode> {
    let persister = Arc::new(Persister::new(Arc::clone(&object_store), host_id));
    let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap();
    let meta_cache =
        MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .unwrap();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        // small snapshot size will have parquet written out after 3 WAL periods:
        snapshot_size: 1,
    };
    WriteBufferEnterprise::read_write(CreateReadWriteModeArgs {
        host_id: host_id.into(),
        persister,
        catalog,
        last_cache,
        meta_cache,
        time_provider,
        executor,
        wal_config,
        metric_registry,
        replication_config: None,
        parquet_cache: None,
        compacted_data: None,
        plugin_dir: None,
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
            )
            .await
            .unwrap();
    }
}

async fn chunks_to_record_batches(
    chunks: Vec<Arc<dyn QueryChunk>>,
    ctx: &SessionContext,
) -> Vec<RecordBatch> {
    let mut batches = vec![];
    for chunk in chunks {
        batches.append(&mut chunk.data().read_to_batches(chunk.schema(), ctx).await);
    }
    batches
}
