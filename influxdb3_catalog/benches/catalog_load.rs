//! Benchmarks for reconstructing a catalog from persisted v3 catalog files.
//!
//! These run against a *real* set of catalog files rather than a synthetic
//! fixture, because the cost of a cold-start load is dominated by the shape of
//! the persisted data (record mix, database/table/column counts) and that
//! shape is hard to fabricate faithfully. The fixture is checked in under
//! `benches/fixtures/`, so the benchmark is self-contained:
//!
//! ```console
//! cargo bench -p influxdb3_catalog --bench catalog_load
//! ```
//!
//! To measure a different catalog instead, point [`FIXTURE_ENV`] at any
//! `catalog/v3` directory — one containing a `snapshot` file beside a `logs/`
//! directory of `*.catalog` files.
//!
//! Two groups, split by cost so the cheap measurements stay cheap:
//!
//! | Benchmark                        | Measures                                        |
//! |----------------------------------|-------------------------------------------------|
//! | `catalog_decode/snapshot/crc32`  | payload checksum only                           |
//! | `catalog_decode/snapshot/parse`  | header + CRC + record framing, bodies untouched |
//! | `catalog_load/snapshot/apply`    | decoding record bodies into `InnerCatalog`      |
//! | `catalog_load/logs/replay`       | applying the log files written after the snapshot|
//! | `catalog_load/full`              | the public API against an in-memory object store |
//!
//! The `catalog_load` group scales with catalog size, so it uses criterion's
//! minimum sample count to stay quick on catalogs much larger than this one.
//! On the checked-in fixture the whole benchmark takes about a minute. Filter
//! to one group when that is more than you need:
//!
//! ```console
//! cargo bench -p influxdb3_catalog --bench catalog_load -- catalog_decode
//! ```

use std::cell::{Cell, OnceCell};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use influxdb3_catalog::catalog::versions::v3::inner::InnerCatalog;
use influxdb3_catalog::catalog::{Catalog, CatalogArgs, CatalogLimits, CatalogSequenceNumber};
use influxdb3_catalog::format::apply::{RestorePreload, apply_records};
use influxdb3_catalog::format::{CatalogFile, view};
use iox_time::{SystemProvider, TimeProvider};
use object_store::memory::InMemory;
use object_store::{ObjectStore, path::Path as ObjPath};
use uuid::Uuid;

/// Environment variable overriding which `catalog/v3` directory to load from.
const FIXTURE_ENV: &str = "INFLUXDB3_CATALOG_BENCH_DIR";

/// The checked-in fixture, used when [`FIXTURE_ENV`] is unset. See the
/// `README.md` beside it for provenance and for how to capture another one.
const DEFAULT_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/benches/fixtures/catalog-22k-tables"
);

/// Object-store prefix the fixture is staged under. Arbitrary — the loader
/// only needs the prefix it writes to match the prefix it reads from.
const PREFIX: &str = "bench-node";

/// The raw bytes of a persisted catalog, as they sit in object storage.
struct Fixture {
    snapshot: Bytes,
    /// Log files paired with their sequence number, ascending.
    logs: Vec<(u64, Bytes)>,
}

impl Fixture {
    /// Read every catalog file under `dir`, which must be a `catalog/v3`
    /// directory (a `snapshot` file beside a `logs/` directory).
    fn load(dir: &Path) -> std::io::Result<Self> {
        let snapshot = Bytes::from(std::fs::read(dir.join("snapshot"))?);

        let mut logs = Vec::new();
        for entry in std::fs::read_dir(dir.join("logs"))? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("catalog") {
                continue;
            }
            // Log file names are the zero-padded sequence number.
            let Some(sequence) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            logs.push((sequence, Bytes::from(std::fs::read(&path)?)));
        }
        logs.sort_unstable_by_key(|(sequence, _)| *sequence);

        Ok(Self { snapshot, logs })
    }

    /// The log files a load actually replays: those written after the
    /// snapshot was taken. Logs at or below the snapshot's sequence are
    /// already folded into it and are skipped by `load_catalog`.
    fn logs_after(&self, snapshot_sequence: u64) -> impl Iterator<Item = &(u64, Bytes)> {
        self.logs
            .iter()
            .filter(move |(sequence, _)| *sequence > snapshot_sequence)
    }

    /// Stage the fixture into a fresh in-memory object store, laid out where
    /// `CatalogFilePath` expects to find it.
    async fn to_object_store(&self) -> Arc<dyn ObjectStore> {
        let store = InMemory::new();
        store
            .put(
                &ObjPath::from(format!("{PREFIX}/catalog/v3/snapshot")),
                self.snapshot.clone().into(),
            )
            .await
            .expect("put snapshot");
        for (sequence, bytes) in &self.logs {
            store
                .put(
                    &ObjPath::from(format!("{PREFIX}/catalog/v3/logs/{sequence:020}.catalog")),
                    bytes.clone().into(),
                )
                .await
                .expect("put log");
        }
        Arc::new(store)
    }
}

/// Parse `bytes` as a catalog file, panicking with context on failure — a
/// malformed fixture is a setup error, not something to measure.
fn parse(bytes: &Bytes, what: &str) -> CatalogFile {
    CatalogFile::read_from(&mut Cursor::new(bytes.as_ref()))
        .unwrap_or_else(|e| panic!("failed to parse {what}: {e}"))
}

/// An `InnerCatalog` with the snapshot applied and nothing else — the state
/// log replay starts from.
fn catalog_from_snapshot(snapshot: &CatalogFile) -> InnerCatalog {
    let mut inner = InnerCatalog::new(
        Arc::from(PREFIX),
        Uuid::from_u128(snapshot.header.catalog_uuid),
    );
    apply_records(
        &snapshot.records,
        &mut inner,
        CatalogSequenceNumber::new(snapshot.header.sequence_number),
        RestorePreload::empty(),
    )
    .expect("apply snapshot records");
    inner
}

/// The end-to-end load under test: the same public entry point a node calls
/// on startup.
async fn load_catalog(store: Arc<dyn ObjectStore>) -> Arc<Catalog> {
    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
    Catalog::load_or_create(
        PREFIX,
        None,
        store,
        time_provider,
        Default::default(),
        Arc::new(CatalogLimits::none()),
        CatalogArgs::default(),
    )
    .await
    .expect("load catalog")
}

/// Print the shape of the fixture, so a benchmark run is self-describing:
/// the numbers here explain the timings below them.
///
/// Deliberately cheap — everything here is derived from the file bytes. The
/// loaded database/table counts are reported separately, from a catalog the
/// `full` benchmark builds anyway, rather than by loading a second time.
fn describe(fixture: &Fixture, snapshot: &CatalogFile) {
    let replayed: Vec<_> = fixture
        .logs_after(snapshot.header.sequence_number)
        .collect();
    let replayed_records: usize = replayed
        .iter()
        .map(|(_, bytes)| parse(bytes, "log").records.len())
        .sum();

    println!("catalog fixture:");
    println!(
        "  snapshot: {} bytes, sequence {}, {} records, group_count {}",
        fixture.snapshot.len(),
        snapshot.header.sequence_number,
        snapshot.records.len(),
        snapshot.header.group_count,
    );
    println!(
        "  logs:     {} files total, {} replayed on top of the snapshot ({} bytes, \
         {replayed_records} records)",
        fixture.logs.len(),
        replayed.len(),
        replayed.iter().map(|(_, b)| b.len()).sum::<usize>(),
    );

    // Counts come from the shared view helper; body sizes are summed here so
    // both columns are keyed on the same record id.
    let mut body_bytes = BTreeMap::<_, usize>::new();
    for record in &snapshot.records {
        *body_bytes.entry(record.id()).or_default() += record.data.len();
    }
    let records = view::all_records(snapshot);
    let mut histogram = view::histogram(&records);
    histogram.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.count));
    println!("  snapshot record histogram:");
    for entry in histogram.iter().take(15) {
        println!(
            "    {:>9}  {:<32} {:>12} body bytes",
            entry.count,
            entry.name.unwrap_or("<unknown>"),
            body_bytes.get(&entry.id).copied().unwrap_or_default(),
        );
    }
}

/// Read the fixture named by [`FIXTURE_ENV`], falling back to the checked-in
/// one. A missing or malformed fixture is a hard error: silently benchmarking
/// nothing would be worse than failing.
fn fixture() -> Fixture {
    let dir = std::env::var_os(FIXTURE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_FIXTURE));
    Fixture::load(&dir)
        .unwrap_or_else(|e| panic!("failed to read catalog fixture at {}: {e}", dir.display()))
}

/// Cheap, byte-oriented work: everything up to but not including decoding
/// record bodies.
fn bench_catalog_decode(c: &mut Criterion) {
    let fixture = fixture();
    let snapshot = parse(&fixture.snapshot, "snapshot");

    let mut group = c.benchmark_group("catalog_decode");
    group.throughput(Throughput::Bytes(fixture.snapshot.len() as u64));

    // Checksum alone, to separate integrity verification from decoding. The
    // payload follows the fixed-size header, so hashing the whole file is
    // within a rounding error of hashing just the payload.
    group.bench_function("snapshot/crc32", |b| {
        b.iter(|| black_box(crc32fast::hash(black_box(fixture.snapshot.as_ref()))));
    });

    // Header validation + CRC + splitting the payload into records. Record
    // bodies stay as opaque `Bytes` slices here.
    group.bench_function("snapshot/parse", |b| {
        b.iter(|| black_box(parse(&fixture.snapshot, "snapshot")));
    });

    group.finish();
    black_box(snapshot);
}

/// The expensive half: decoding record bodies and folding them into the
/// in-memory catalog. Scales with the number of databases, tables and columns.
fn bench_catalog_load(c: &mut Criterion) {
    let fixture = fixture();
    let snapshot = parse(&fixture.snapshot, "snapshot");

    describe(&fixture, &snapshot);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let mut group = c.benchmark_group("catalog_load");
    // A single iteration is on the order of seconds for a large catalog, so
    // take criterion's minimum sample count and let it size the run itself.
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10));

    group.throughput(Throughput::Bytes(fixture.snapshot.len() as u64));
    group.bench_function("snapshot/apply", |b| {
        b.iter_batched(
            || {
                InnerCatalog::new(
                    Arc::from(PREFIX),
                    Uuid::from_u128(snapshot.header.catalog_uuid),
                )
            },
            |mut inner| {
                apply_records(
                    &snapshot.records,
                    &mut inner,
                    CatalogSequenceNumber::new(snapshot.header.sequence_number),
                    RestorePreload::empty(),
                )
                .expect("apply snapshot records");
                inner
            },
            BatchSize::PerIteration,
        );
    });

    // Log replay on top of the snapshot: the tail of a cold start. The base
    // state is built once and cloned per iteration, so rebuilding it — itself
    // the benchmark above — stays out of the per-iteration setup path. It is
    // built lazily because criterion calls the routine below once per sample
    // but skips it entirely when the benchmark is filtered out; eager
    // construction would charge every filtered run a full snapshot apply.
    let replayed: Vec<CatalogFile> = fixture
        .logs_after(snapshot.header.sequence_number)
        .map(|(_, bytes)| parse(bytes, "log"))
        .collect();
    let base: OnceCell<InnerCatalog> = OnceCell::new();
    group.throughput(Throughput::Bytes(
        fixture
            .logs_after(snapshot.header.sequence_number)
            .map(|(_, b)| b.len())
            .sum::<usize>() as u64,
    ));
    group.bench_function("logs/replay", |b| {
        let base = base.get_or_init(|| catalog_from_snapshot(&snapshot));
        b.iter_batched(
            || base.clone(),
            |mut inner| {
                for file in &replayed {
                    apply_records(
                        &file.records,
                        &mut inner,
                        CatalogSequenceNumber::new(file.header.sequence_number),
                        RestorePreload::empty(),
                    )
                    .expect("apply log records");
                }
                inner
            },
            BatchSize::PerIteration,
        );
    });
    drop(base);

    // End-to-end through the public API, against an in-memory object store so
    // the measurement is CPU-bound rather than a measure of the local disk.
    let total_bytes =
        (fixture.snapshot.len() + fixture.logs.iter().map(|(_, b)| b.len()).sum::<usize>()) as u64;
    group.throughput(Throughput::Bytes(total_bytes));
    // Observed from inside the benchmark so the loaded shape can be reported
    // without paying for a load that exists only to be described.
    let loaded_shape = Cell::new(None);
    group.bench_function("full", |b| {
        b.iter_batched(
            // A fresh store per iteration keeps iterations independent: a load
            // may rewrite a legacy-layout snapshot, which would otherwise
            // change the work the next iteration does.
            || runtime.block_on(fixture.to_object_store()),
            |store| {
                let catalog = runtime.block_on(load_catalog(store));
                loaded_shape.set(Some((catalog.database_count(), catalog.table_count())));
                catalog
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();

    if let Some((databases, tables)) = loaded_shape.get() {
        println!("catalog fixture loaded: {databases} databases, {tables} tables");
    }
}

criterion_group!(decode, bench_catalog_decode);
criterion_group!(load, bench_catalog_load);
criterion_main!(decode, load);
