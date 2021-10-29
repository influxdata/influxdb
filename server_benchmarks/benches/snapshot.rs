use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flate2::read::GzDecoder;
use mutable_buffer::MBChunk;
use std::io::Read;

#[inline]
fn snapshot_chunk(chunk: &MBChunk) {
    let _ = chunk.snapshot();
}

fn chunk(count: usize) -> MBChunk {
    let raw = include_bytes!("../../test_fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    let mut chunk = mutable_buffer::test_helpers::write_lp_to_new_chunk(&lp);

    for _ in 1..count {
        mutable_buffer::test_helpers::write_lp_to_chunk(&lp, &mut chunk);
    }

    chunk
}

pub fn snapshot_mb(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_mb");
    for count in &[1, 2, 3, 4, 5] {
        let chunk = chunk(*count as _);
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| snapshot_chunk(&chunk));
        });
    }
    group.finish();
}

criterion_group!(benches, snapshot_mb);
criterion_main!(benches);
