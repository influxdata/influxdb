use arrow_array::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use datafusion::execution::context::SessionContext;
use datafusion_util::config::register_iox_object_store;
use executor::DedicatedExecutor;
use futures::FutureExt;
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
use influxdb3_write::{Bufferer, ChunkContainer, Precision, WriteBuffer};
use iox_query::exec::{Executor, ExecutorConfig};
use iox_query::QueryChunk;
use iox_time::{SystemProvider, Time};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let last_cache =
        Arc::new(LastCacheProvider::new_from_catalog(Arc::clone(&writer1_catalog)).unwrap());

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

    let compactor = Compactor::new(
        vec![writer1_id.to_string(), writer2_id.to_string()],
        Arc::clone(&compacted_data),
        Arc::clone(&writer1_catalog),
        writer1_persister.object_store_url().clone(),
        Arc::clone(&exec),
        read_write_mode.watch_persisted_snapshots(),
    )
    .await
    .unwrap();

    // run the compactor on the DataFusion executor, but don't drop it
    let _t = exec.executor().spawn(compactor.compact()).boxed();

    let mut snapshot_notify = read_write_mode.watch_persisted_snapshots();

    // each call to do_writes will force a snapshot. We want to do two for each writer,
    // which will then trigger a compaction. We also want to do one more snapshot each
    // so that we'll have non-compacted files show up in this query too.
    snapshot_notify.mark_unchanged();
    do_writes(read_write_mode.as_ref(), writer1_id, 0).await;
    snapshot_notify.changed().await.unwrap();
    snapshot_notify.mark_unchanged();
    do_writes(&writer2_buffer, writer2_id, 1).await;
    snapshot_notify.changed().await.unwrap();
    snapshot_notify.mark_unchanged();
    do_writes(read_write_mode.as_ref(), writer1_id, 2).await;
    snapshot_notify.changed().await.unwrap();
    snapshot_notify.mark_unchanged();
    do_writes(&writer2_buffer, writer2_id, 3).await;
    snapshot_notify.changed().await.unwrap();
    snapshot_notify.mark_unchanged();

    // wait for a compaction to happen
    let mut count = 0;
    loop {
        let (db_id, db_schema) = writer1_catalog.db_schema_and_id("test_db").unwrap();
        let table_id = db_schema.table_name_to_id("m1").unwrap();
        if let Some(detail) = compacted_data.get_last_compaction_detail(db_id, table_id) {
            if detail.sequence_number.as_u64() > 1 {
                // we should have a compacted generation
                assert_eq!(detail.compacted_generations.len(), 1);
                // nothing should be leftover as it should all now exist in gen3
                assert_eq!(detail.leftover_gen1_files.len(), 0);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        count += 1;
        if count > 20 {
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
            "+------+--------------------------------+---------+",
            "| f1   | time                           | w       |",
            "+------+--------------------------------+---------+",
            "| 0.0  | 1970-01-01T00:00:00Z           | writer1 |",
            "| 1.0  | 1970-01-01T00:00:00.000000001Z | writer1 |",
            "| 10.0 | 1970-01-01T00:04:30.000000001Z | writer2 |",
            "| 11.0 | 1970-01-01T00:04:30.000000002Z | writer2 |",
            "| 2.0  | 1970-01-01T00:00:00.000000002Z | writer1 |",
            "| 3.0  | 1970-01-01T00:01:30Z           | writer2 |",
            "| 4.0  | 1970-01-01T00:01:30.000000001Z | writer2 |",
            "| 5.0  | 1970-01-01T00:01:30.000000002Z | writer2 |",
            "| 6.0  | 1970-01-01T00:03:00Z           | writer1 |",
            "| 7.0  | 1970-01-01T00:03:00.000000001Z | writer1 |",
            "| 8.0  | 1970-01-01T00:03:00.000000002Z | writer1 |",
            "| 9.0  | 1970-01-01T00:04:30Z           | writer2 |",
            "+------+--------------------------------+---------+",
        ],
        &batches
    );
}

async fn do_writes(buffer: &(impl WriteBuffer + ?Sized), writer: &str, start_num: i64) {
    let db = data_types::NamespaceName::new("test_db").unwrap();

    let number_of_writes = 3;
    let offset = start_num * number_of_writes;

    for i in 0..number_of_writes {
        let num = (offset * 30 * 1_000_000_000) + i;
        let data = format!("m1,w={} f1={} {}", writer, offset + i, num);
        buffer
            .write_lp(
                db.clone(),
                &data,
                Time::from_timestamp_nanos(num),
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
