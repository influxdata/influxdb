use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mutable_batch_lp::LinesConverter;

fn bench_write_line(c: &mut Criterion) {
    // Read the text_fixtures/metrics.lp data set, containing 1,000 lines of LP.
    let lp = std::fs::read_to_string(format!(
        "{}/../test_fixtures/lineproto/metrics.lp",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("reading test fixture failed");

    let lines = lp.chars().filter(|&c| c == '\n').count();
    assert_eq!(lines, 1000); // Perf would vary if the fixture changed

    let mut group = c.benchmark_group("parse_lp");
    group.throughput(Throughput::Elements(lines as _));
    group.bench_function("metrics.lp", |b| {
        b.iter_batched(
            || LinesConverter::new(42),
            |mut converter| {
                converter.write_lp(&lp).unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(benches, bench_write_line);
criterion_main!(benches);
