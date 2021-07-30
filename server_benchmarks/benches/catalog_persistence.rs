use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode};
use data_types::database_rules::LifecycleRules;
use object_store::{ObjectStore, ThrottleConfig};
use query::QueryChunk;
use server::{db::test_helpers::write_lp, utils::TestDb};
use std::{
    convert::TryFrom,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    runtime::{Handle, Runtime},
    sync::Mutex,
    task::block_in_place,
};

/// Checkpoint interval for preserved catalog.
const CHECKPOINT_INTERVAL: u64 = 10;

/// Number of chunks simulated for persistence.
///
/// Ideally this value is NOT divisible by [`CHECKPOINT_INTERVAL`], so that there are some transactions after the last
/// checkpoint.
const N_CHUNKS: u32 = 109;

/// Number of tags for the test table.
const N_TAGS: usize = 10;

/// Number of fields for the test table.
const N_FIELDS: usize = 10;

/// Run all benchmarks that test catalog persistence.
fn benchmark_catalog_persistence(c: &mut Criterion) {
    let object_store = create_throttled_store();
    let setup_done = Mutex::new(false);

    // benchmark reading the catalog
    let mut group = c.benchmark_group("catalog");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(10));

    group.bench_function("catalog_restore", |b| {
        b.to_async(Runtime::new().unwrap()).iter_batched(
            || {
                // Threaded runtime is already running.
                block_in_place(|| {
                    Handle::current().block_on(setup(Arc::clone(&object_store), &setup_done));
                });
            },
            |_| async {
                let db = create_persisted_db(Arc::clone(&object_store)).await.db;

                // test that data is actually loaded
                let partition_key = "1970-01-01T00";
                let table_name = "cpu";
                let chunk_id = 1;
                assert!(db
                    .table_summary(table_name, partition_key, chunk_id)
                    .is_some());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Persist a database to the given object store with [`N_CHUNKS`] chunks.
async fn setup(object_store: Arc<ObjectStore>, done: &Mutex<bool>) {
    let mut guard = done.lock().await;
    if *guard {
        return;
    }

    let db = create_persisted_db(object_store).await.db;
    let lp = create_lp(N_TAGS, N_FIELDS);
    let partition_key = "1970-01-01T00";

    for _ in 0..N_CHUNKS {
        let table_names = write_lp(&db, &lp).await;

        for table_name in &table_names {
            let chunk = db
                .rollover_partition(table_name, partition_key)
                .await
                .unwrap()
                .unwrap();

            db.move_chunk_to_read_buffer(table_name, partition_key, chunk.id())
                .await
                .unwrap();

            let chunk = db
                .persist_partition(
                    table_name,
                    partition_key,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();

            db.unload_read_buffer(table_name, partition_key, chunk.id())
                .unwrap();
        }
    }

    *guard = true;
}

/// Create a persisted database and load its catalog.
#[inline(never)]
async fn create_persisted_db(object_store: Arc<ObjectStore>) -> TestDb {
    TestDb::builder()
        .object_store(object_store)
        .lifecycle_rules(LifecycleRules {
            catalog_transactions_until_checkpoint: NonZeroU64::try_from(CHECKPOINT_INTERVAL)
                .unwrap(),
            late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
            ..Default::default()
        })
        .build()
        .await
}

/// Create line protocol for a single entry with `n_tags` tags and `n_fields` fields.
///
/// The table is `"cpu"` and the timestamp is `0`.
fn create_lp(n_tags: usize, n_fields: usize) -> String {
    let mut lp = "cpu".to_string();
    for i in 0..n_tags {
        lp.push_str(&format!(",tag_{}=x", i));
    }
    lp.push(' ');
    for i in 0..n_fields {
        if i > 0 {
            lp.push(',')
        }
        lp.push_str(&format!("field_{}=1", i));
    }
    lp.push_str(" 0");
    lp
}

/// Create object store with somewhat realistic operation latencies.
fn create_throttled_store() -> Arc<ObjectStore> {
    let config = ThrottleConfig {
        // for every call: assume a 100ms latency
        wait_delete_per_call: Duration::from_millis(100),
        wait_get_per_call: Duration::from_millis(100),
        wait_list_per_call: Duration::from_millis(100),
        wait_list_with_delimiter_per_call: Duration::from_millis(100),
        wait_put_per_call: Duration::from_millis(100),

        // for list operations: assume we need 1 call per 1k entries at 100ms
        wait_list_per_entry: Duration::from_millis(100) / 1_000,
        wait_list_with_delimiter_per_entry: Duration::from_millis(100) / 1_000,

        // for upload/download: assume 1GByte/s
        wait_get_per_byte: Duration::from_secs(1) / 1_000_000_000,
        wait_put_per_byte: Duration::from_secs(1) / 1_000_000_000,
    };

    Arc::new(ObjectStore::new_in_memory_throttled(config))
}

criterion_group!(benches, benchmark_catalog_persistence);
criterion_main!(benches);
