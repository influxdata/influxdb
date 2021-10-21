use std::io::Read;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use flate2::read::GzDecoder;

use mutable_batch_lp::lines_to_batch;

fn generate_lp_bytes() -> Bytes {
    let raw = include_bytes!("../../tests/fixtures/lineproto/read_filter.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);

    let mut buffer = Vec::new();
    gz.read_to_end(&mut buffer).unwrap();
    buffer.into()
}

pub fn write_lp(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_lp");
    let lp_bytes = generate_lp_bytes();
    for count in &[1, 2, 3, 4, 5] {
        group.throughput(Throughput::Bytes(lp_bytes.len() as u64 * *count as u64));
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| {
                for _ in 0..*count {
                    lines_to_batch(std::str::from_utf8(&lp_bytes).unwrap(), 0).unwrap();
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, write_lp);
criterion_main!(benches);
