mod common;

use arrow_array::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use datafusion::execution::context::SessionContext;
use datafusion_util::config::register_iox_object_store;
use executor::DedicatedExecutor;
use futures::FutureExt;
use influxdb3_config::ProConfig;
use influxdb3_pro_buffer::modes::read_write::ReadWriteArgs;
use influxdb3_pro_buffer::replica::ReplicationConfig;
use influxdb3_pro_buffer::WriteBufferPro;
use influxdb3_pro_compactor::Compactor;
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::CompactionConfig;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::last_cache::LastCacheProvider;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::WriteBufferImpl;
use influxdb3_write::{ChunkContainer, Precision, WriteBuffer};
use iox_query::exec::{Executor, ExecutorConfig};
use iox_query::QueryChunk;
use iox_time::{SystemProvider, Time};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::common::build_parquet_cache_prefetcher;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "flakey test as it depends on the timing of other writer to read"]
async fn two_writers_gen1_compaction() {
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

    let writer1_id = "writer1";
    let writer2_id = "writer2";

    let writer1_persister = Arc::new(Persister::new(Arc::clone(&object_store), writer1_id));
    let writer1_catalog = Arc::new(writer1_persister.load_or_create_catalog().await.unwrap());
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&writer1_catalog)).unwrap();

    let writer2_persister = Arc::new(Persister::new(Arc::clone(&object_store), writer2_id));
    let writer2_catalog = writer2_persister.load_or_create_catalog().await.unwrap();
    let writer2_buffer = WriteBufferImpl::new(
        Arc::clone(&writer2_persister),
        Arc::new(writer2_catalog),
        Arc::clone(&last_cache),
        Arc::new(SystemProvider::new()),
        Arc::clone(&exec),
        wal_config,
        None,
    )
    .await
    .unwrap();

    let compactor_id = "compact";
    let compaction_config = CompactionConfig::new(&[2], Duration::from_secs(120), 10);
    let compacted_data = CompactedData::load_compacted_data(
        compactor_id,
        compaction_config,
        Arc::clone(&object_store),
        Arc::clone(&writer1_catalog),
    )
    .await
    .unwrap();

    let read_write_mode = Arc::new(
        WriteBufferPro::read_write(ReadWriteArgs {
            host_id: writer1_id.into(),
            persister: Arc::clone(&writer1_persister),
            catalog: Arc::clone(&writer1_catalog),
            last_cache,
            time_provider: Arc::new(SystemProvider::new()),
            executor: Arc::clone(&exec),
            wal_config,
            metric_registry: Arc::clone(&metrics),
            replication_config: Some(ReplicationConfig::new(
                Duration::from_millis(10),
                vec![writer2_id.to_string()],
            )),
            parquet_cache: None,
            compacted_data: Some(Arc::clone(&compacted_data)),
        })
        .await
        .unwrap(),
    );
    let obj_store = Arc::new(InMemory::new());
    let parquet_cache_prefetcher = build_parquet_cache_prefetcher(&obj_store);

    let compactor = Compactor::new(
        Arc::clone(&compacted_data),
        Arc::clone(&writer1_catalog),
        writer1_persister.object_store_url().clone(),
        Arc::clone(&exec),
        parquet_cache_prefetcher,
        Arc::new(RwLock::new(ProConfig::default())),
    )
    .await
    .unwrap();

    // run the compactor on the DataFusion executor, but don't drop it
    let _t = exec.executor().spawn(compactor.compact()).boxed();

    let mut snapshot_notify = compacted_data.snapshot_notification_receiver();
    // each call to do_writes will force a snapshot. We want to do two for each writer,
    // which will then trigger a compaction. We also want to do one more snapshot each
    // so that we'll have non-compacted files show up in this query too.
    do_writes(read_write_mode.as_ref(), writer1_id, 0, 1).await;
    let _ = snapshot_notify.recv().await;
    do_writes(&writer2_buffer, writer2_id, 0, 2).await;
    let _ = snapshot_notify.recv().await;
    do_writes(read_write_mode.as_ref(), writer1_id, 1, 1).await;
    let _ = snapshot_notify.recv().await;
    do_writes(&writer2_buffer, writer2_id, 1, 2).await;
    let _ = snapshot_notify.recv().await;
    do_writes(read_write_mode.as_ref(), writer1_id, 2, 1).await;
    let _ = snapshot_notify.recv().await;
    do_writes(&writer2_buffer, writer2_id, 2, 2).await;
    let _ = snapshot_notify.recv().await;

    // wait for a compaction to happen
    let mut count = 0;
    loop {
        let (db_id, db_schema) = writer1_catalog.db_schema_and_id("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("m1").unwrap();
        if let Some(detail) = compacted_data.get_last_compaction_detail(db_id, table_id) {
            if detail.sequence_number.as_u64() > 1 {
                // we should have a single compacted generation
                assert_eq!(
                    detail.compacted_generations.len(),
                    1,
                    "should have a single generation. compaction details: {:?}",
                    detail
                );
                // nothing should be leftover as it should all now exist in gen3
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

    // we need to wait for the replica to read writer2's last writes to ensure they
    // show up. Not the best way to handle this, but it'll work for now. Can fix up if
    // it ends up being too flakey.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // query and make sure all the data is there
    let ctx = exec.new_context();
    let chunks = read_write_mode
        .get_table_chunks("test_db", "m1", &[], None, &ctx.inner().state())
        .unwrap();

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
