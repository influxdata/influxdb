//! Profiling harness for catalog load.
//!
//! Repeatedly reconstructs a catalog from a real set of persisted v3 catalog
//! files, printing a per-round timing. Unlike the criterion benchmark in
//! `benches/catalog_load.rs`, this is a plain binary with a single-threaded,
//! predictable call stack — the shape a sampling profiler wants.
//!
//! ```console
//! cargo build -p influxdb3_catalog --example catalog_load_profile --profile bench
//! target/release/examples/catalog_load_profile [dir] [rounds] [phase]
//! ```
//!
//! `dir` is a `catalog/v3` directory — a `snapshot` file beside a `logs/`
//! directory of `*.catalog` files. It defaults to the same checked-in fixture
//! the criterion benchmark uses, so the binary runs with no arguments.
//!
//! `phase` selects what each round does, so a profile can be narrowed to one
//! stage: `full` (default, the public load API), `apply` (decode record bodies
//! into the catalog, skipping object-store and framing work), or `parse`
//! (framing and checksums only).
//!
//! Attach a profiler while it runs, e.g. with the macOS sampler:
//!
//! ```console
//! sample <pid> 30 -f /tmp/catalog_load.sample
//! ```
//!
//! or under Instruments:
//!
//! ```console
//! cargo instruments -t "Time Profiler" -p influxdb3_catalog \
//!     --example catalog_load_profile --profile bench -- "" 20 apply
//! ```

use std::hint::black_box;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use influxdb3_catalog::catalog::versions::v3::inner::InnerCatalog;
use influxdb3_catalog::catalog::{Catalog, CatalogArgs, CatalogLimits, CatalogSequenceNumber};
use influxdb3_catalog::format::CatalogFile;
use influxdb3_catalog::format::apply::{RestorePreload, apply_records};
use iox_time::{SystemProvider, TimeProvider};
use object_store::memory::InMemory;
use object_store::{ObjectStore, path::Path as ObjPath};
use uuid::Uuid;

const PREFIX: &str = "profile-node";

/// The checked-in fixture shared with `benches/catalog_load.rs`, used when no
/// directory is given on the command line.
const DEFAULT_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/benches/fixtures/catalog-22k-tables"
);

/// What a single round measures.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The public load API, object store included.
    Full,
    /// Record-body decode and catalog mutation only.
    Apply,
    /// File framing and checksum verification only.
    Parse,
}

impl Phase {
    fn from_arg(s: &str) -> Option<Self> {
        match s {
            "full" => Some(Self::Full),
            "apply" => Some(Self::Apply),
            "parse" => Some(Self::Parse),
            _ => None,
        }
    }
}

/// The raw bytes of a persisted catalog, as they sit in object storage.
struct Fixture {
    snapshot: Bytes,
    /// Log files paired with their sequence number, ascending.
    logs: Vec<(u64, Bytes)>,
}

impl Fixture {
    fn load(dir: &Path) -> std::io::Result<Self> {
        let snapshot = Bytes::from(std::fs::read(dir.join("snapshot"))?);
        let mut logs = Vec::new();
        for entry in std::fs::read_dir(dir.join("logs"))? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("catalog") {
                continue;
            }
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

fn parse_file(bytes: &Bytes) -> CatalogFile {
    CatalogFile::read_from(&mut Cursor::new(bytes.as_ref())).expect("parse catalog file")
}

fn main() {
    let mut args = std::env::args().skip(1);
    // An empty first argument selects the default too, so the later positional
    // arguments stay reachable without naming a path.
    let dir = args
        .next()
        .filter(|d| !d.is_empty())
        .map_or_else(|| PathBuf::from(DEFAULT_FIXTURE), PathBuf::from);
    let rounds: usize = args
        .next()
        .map(|s| s.parse().expect("rounds must be a number"))
        .unwrap_or(10);
    let phase = args
        .next()
        .map(|s| Phase::from_arg(&s).expect("phase must be one of: full, apply, parse"))
        .unwrap_or(Phase::Full);

    let fixture = Fixture::load(&dir)
        .unwrap_or_else(|e| panic!("failed to read catalog fixture at {}: {e}", dir.display()));
    let snapshot = parse_file(&fixture.snapshot);
    let catalog_uuid = Uuid::from_u128(snapshot.header.catalog_uuid);
    let snapshot_sequence = CatalogSequenceNumber::new(snapshot.header.sequence_number);

    println!(
        "pid {}: {} snapshot records, {} log files, {rounds} rounds",
        std::process::id(),
        snapshot.records.len(),
        fixture.logs.len(),
    );

    // Single-threaded on purpose: the load is sequential, and a current-thread
    // runtime keeps the profile free of idle worker stacks.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    // Stage once. Loading does not mutate the store for a snapshot already in
    // the current single-group layout, so rounds stay independent.
    let store = runtime.block_on(fixture.to_object_store());

    for round in 1..=rounds {
        let start = Instant::now();
        match phase {
            Phase::Full => {
                let catalog = runtime.block_on(async {
                    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
                    Catalog::load_or_create(
                        PREFIX,
                        None,
                        Arc::clone(&store),
                        time_provider,
                        Default::default(),
                        Arc::new(CatalogLimits::none()),
                        CatalogArgs::default(),
                    )
                    .await
                    .expect("load catalog")
                });
                let elapsed = start.elapsed();
                println!(
                    "round {round}: {elapsed:?} ({} databases, {} tables)",
                    catalog.database_count(),
                    catalog.table_count(),
                );
            }
            Phase::Apply => {
                let mut inner = InnerCatalog::new(Arc::from(PREFIX), catalog_uuid);
                apply_records(
                    &snapshot.records,
                    &mut inner,
                    snapshot_sequence,
                    RestorePreload::empty(),
                )
                .expect("apply snapshot records");
                let elapsed = start.elapsed();
                black_box(inner);
                println!("round {round}: {elapsed:?}");
            }
            Phase::Parse => {
                black_box(parse_file(&fixture.snapshot));
                let elapsed = start.elapsed();
                println!("round {round}: {elapsed:?}");
            }
        }
    }
}
