use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;

static LINES: &str = include_str!("line-protocol.txt");

fn line_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("line_parser");

    // group.throughput(Throughput::Elements(LINES.lines().count() as u64));
    group.throughput(Throughput::Bytes(LINES.len() as u64));
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("all lines", |b| {
        b.iter(|| {
            let lines = delorean::line_parser::parse(LINES).unwrap();
            assert_eq!(582, lines.len());
        })
    });

    group.finish();
}

criterion_group!(benches, line_parser);

criterion_main!(benches);
