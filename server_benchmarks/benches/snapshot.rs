use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use entry::test_helpers::{hour_partitioner, lp_to_entries};
use flate2::read::GzDecoder;
use mutable_buffer::{ChunkMetrics, MBChunk};
use std::io::Read;

#[inline]
fn snapshot_chunk(chunk: &MBChunk) {
    let _ = chunk.snapshot();
}

fn chunk(count: usize) -> MBChunk {
    let mut chunk: Option<MBChunk> = None;

    let raw = include_bytes!("../../tests/fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    for _ in 0..count {
        for entry in lp_to_entries(&lp, &hour_partitioner()) {
            for write in entry.partition_writes().iter().flatten() {
                for batch in write.table_batches() {
                    match chunk {
                        Some(ref mut c) => {
                            c.write_table_batch(batch, None).unwrap();
                        }
                        None => {
                            chunk = Some(
                                MBChunk::new(ChunkMetrics::new_unregistered(), batch, None)
                                    .unwrap(),
                            );
                        }
                    }
                }
            }
        }
    }

    chunk.expect("Must write at least one table batch to create a chunk")
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
