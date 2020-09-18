use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use delorean_line_parser as line_parser;
use delorean_storage_interface::Database;
use delorean_wal::{Entry, WalBuilder};
use delorean_write_buffer::database::{restore_partitions_from_wal, Db};
use std::{fs, path::Path};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn benchmark_restore_partitions(c: &mut Criterion) {
    let (entries, line_count) = generate_entries().expect("Unable to create benchmarking entries");
    assert_eq!(line_count, 1000);

    let mut group = c.benchmark_group("wal-restoration");
    group.throughput(Throughput::Elements(line_count as u64));
    group.bench_function("restore_partitions_from_wal", |b| {
        b.iter(|| {
            let entries = entries.clone().into_iter().map(Ok);
            let (partitions, partition_id, _stats) = restore_partitions_from_wal(entries).unwrap();
            assert_eq!(partitions.len(), 1);
            assert_eq!(partition_id, 1);
        })
    });
    group.finish();
}

#[tokio::main]
async fn generate_entries() -> Result<(Vec<Entry>, usize)> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let tmp_dir = delorean_test_helpers::tmp_dir()?;

    let mut wal_dir = tmp_dir.as_ref().to_owned();
    let lines = crate_root.join("../tests/fixtures/lineproto/metrics.lp");
    let lines = fs::read_to_string(lines)?;
    let lines: Vec<_> = line_parser::parse_lines(&lines).collect::<Result<_, _>>()?;
    let db = Db::try_with_wal("mydb", &mut wal_dir).await?;
    db.write_lines(&lines).await?;

    let wal_builder = WalBuilder::new(wal_dir);
    let entries = wal_builder.entries()?.collect::<Result<_, _>>()?;

    Ok((entries, lines.len()))
}

criterion_group!(benches, benchmark_restore_partitions);

criterion_main!(benches);
