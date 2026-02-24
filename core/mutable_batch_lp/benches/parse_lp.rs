// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use std::fmt::Display;
use std::{fmt::Write, path::PathBuf};

use criterion::{
    BatchSize, BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use mutable_batch_lp::LinesConverter;

struct BenchRun {
    tables: usize,
    rows: usize,
    columns: usize,
}

impl Display for BenchRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}_tables_{}_rows_{}_columns",
            self.tables, self.rows, self.columns
        )
    }
}

impl From<&BenchRun> for String {
    fn from(value: &BenchRun) -> Self {
        value.to_string()
    }
}

// Generate line protocol for a single partition.
fn make_lp(scenario: &BenchRun) -> String {
    let mut buf = String::new();

    assert!(scenario.columns > 0);

    for table in 0..scenario.tables {
        let table_name = format!("bananas{table}");
        for i in 0..scenario.rows {
            // Generate a single row containing the specified number of columns
            // expressed as unique unsigned integers.
            buf.push_str(&table_name);
            buf.push(' ');

            // Write N-1 columns (which have a trailing "," separator)
            for j in 0..(scenario.columns - 1) {
                write!(buf, "c{j}={j}i,").unwrap();
            }

            // Write the final column and timestamp.
            writeln!(buf, "c_end={i}i {i}").unwrap();
        }
    }

    buf
}

/// Read the cached line protocol for this benchmark run, or generate (and
/// cache) it if it does not exist.
fn get_cached_lp(scenario: &BenchRun) -> String {
    let cache_name = String::from(scenario);
    let cache_dir = env!("CARGO_TARGET_TMPDIR");

    let mut path = PathBuf::default();
    path.push(cache_dir);
    path.push(cache_name);

    match std::fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => {
            // Regenerate LP
            let lp = make_lp(scenario);
            std::fs::write(path, &lp).expect("failed to cache benchmark line protocol");
            lp
        }
    }
}

fn parse_lp_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_lp");

    bench_scenario(
        &mut group,
        BenchRun {
            tables: 10,
            rows: 10000,
            columns: 1,
        },
    );
    bench_scenario(
        &mut group,
        BenchRun {
            tables: 10,
            rows: 10000,
            columns: 10,
        },
    );
    bench_scenario(
        &mut group,
        BenchRun {
            tables: 10,
            rows: 10000,
            columns: 100,
        },
    );

    group.finish();
}

fn bench_scenario(group: &mut BenchmarkGroup<'_, WallTime>, scenario: BenchRun) {
    let lp = get_cached_lp(&scenario);

    group.throughput(Throughput::Bytes(lp.len() as _));
    group.bench_function(&scenario, |b| {
        b.iter_batched(
            || LinesConverter::new(42),
            |mut converter| {
                converter.write_lp(&lp).unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(benches, parse_lp_benchmarks);
criterion_main!(benches);
