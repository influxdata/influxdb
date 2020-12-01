use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use influxdb_line_protocol as line_parser;
use query::TSDatabase;
use wal::{Entry, WalBuilder};
use write_buffer::{restore_partitions_from_wal, Db};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

const DAY_1_NS: i64 = 1_600_000_000_000_000_000;
const DAY_2_NS: i64 = 1_500_000_000_000_000_000;
const DAY_3_NS: i64 = 1_400_000_000_000_000_000;
const DAYS_NS: &[i64] = &[DAY_1_NS, DAY_2_NS, DAY_3_NS];

fn benchmark_restore_single_entry_single_partition(common_create_entries: &mut Criterion) {
    let (entries, line_count) =
        generate_single_entry_single_partition().expect("Unable to create benchmarking entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(line_count, 1000);

    let mut group = common_create_entries.benchmark_group("wal-restoration");
    group.throughput(Throughput::Elements(line_count as u64));
    group.bench_function("restore_single_entry_single_partition", |b| {
        b.iter(|| {
            let entries = entries.clone().into_iter().map(Ok);
            let (partitions, _stats) = restore_partitions_from_wal(entries).unwrap();
            assert_eq!(partitions.len(), 1);
        })
    });
    group.finish();
}

#[tokio::main]
async fn generate_single_entry_single_partition() -> Result<(Vec<Entry>, usize)> {
    common_create_entries(|add_entry| {
        add_entry(create_line_protocol(1000, DAY_1_NS));
    })
    .await
}

fn benchmark_restore_multiple_entry_multiple_partition(common_create_entries: &mut Criterion) {
    let (entries, line_count) = generate_multiple_entry_multiple_partition()
        .expect("Unable to create benchmarking entries");
    assert_eq!(entries.len(), 12);
    assert_eq!(line_count, 12000);

    let mut group = common_create_entries.benchmark_group("wal-restoration");
    group.throughput(Throughput::Elements(line_count as u64));
    group.bench_function("restore_multiple_entry_multiple_partition", |b| {
        b.iter(|| {
            let entries = entries.clone().into_iter().map(Ok);
            let (partitions, _stats) = restore_partitions_from_wal(entries).unwrap();
            assert_eq!(partitions.len(), 3);
        })
    });
    group.finish();
}

#[tokio::main]
async fn generate_multiple_entry_multiple_partition() -> Result<(Vec<Entry>, usize)> {
    common_create_entries(|add_entry| {
        for &day_ns in DAYS_NS {
            for _ in 0..4 {
                add_entry(create_line_protocol(1000, day_ns))
            }
        }
    })
    .await
}

async fn common_create_entries(
    mut f: impl FnMut(&mut dyn FnMut(String)),
) -> Result<(Vec<Entry>, usize)> {
    let tmp_dir = test_helpers::tmp_dir()?;
    let mut wal_dir = tmp_dir.as_ref().to_owned();
    let db = Db::try_with_wal("mydb", &mut wal_dir).await?;

    let mut lp_entries = Vec::new();
    f(&mut |entry| lp_entries.push(entry));

    let mut total_lines = 0;
    for lp_entry in lp_entries {
        let lines: Vec<_> = line_parser::parse_lines(&lp_entry).collect::<Result<_, _>>()?;
        db.write_lines(&lines).await?;
        total_lines += lines.len();
    }

    let wal_builder = WalBuilder::new(wal_dir);
    let entries = wal_builder.entries()?.collect::<Result<_, _>>()?;

    Ok((entries, total_lines))
}

fn create_line_protocol(entries: usize, timestamp_ns: i64) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    for _ in 0..entries {
        writeln!(
            s,
            "processes,host=my.awesome.computer.example.com blocked=0i,zombies=0i,stopped=0i,running=42i,sleeping=999i,total=1041i,unknown=0i,idle=0i {timestamp}",
            timestamp = timestamp_ns,
        ).unwrap();
    }
    s
}

criterion_group!(
    benches,
    benchmark_restore_single_entry_single_partition,
    benchmark_restore_multiple_entry_multiple_partition,
);

criterion_main!(benches);
