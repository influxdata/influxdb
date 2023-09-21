use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use data_types::{NamespaceId, TableId};
use dml::DmlWrite;
use generated_types::influxdata::pbdata::v1::DatabaseBatch;
use mutable_batch::MutableBatch;
use mutable_batch_lp::lines_to_batches;
use mutable_batch_tests::benchmark_lp;
use prost::Message;

fn generate_pbdata_bytes() -> Vec<(String, (usize, Bytes))> {
    benchmark_lp()
        .into_iter()
        .map(|(bench, lp)| {
            let batches = lines_to_batches(&lp, 0).unwrap();
            let data = batches
                .into_iter()
                .enumerate()
                .map(|(idx, (_table_name, batch))| (TableId::new(idx as _), batch))
                .collect();

            let write = DmlWrite::new(
                NamespaceId::new(42),
                data,
                "bananas".into(),
                Default::default(),
            );
            let database_batch = mutable_batch_pb::encode::encode_write(42, &write);

            let mut bytes = BytesMut::new();
            database_batch.encode(&mut bytes).unwrap();

            (bench, (lp.len(), bytes.freeze()))
        })
        .collect()
}

pub fn write_pb(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_pb");
    for (bench, (lp_bytes, pbdata_bytes)) in generate_pbdata_bytes() {
        group.throughput(Throughput::Bytes(lp_bytes as u64));
        group.bench_function(BenchmarkId::from_parameter(bench), |b| {
            b.iter(|| {
                let mut batch = MutableBatch::new();
                let database_batch = DatabaseBatch::decode(pbdata_bytes.clone()).unwrap();
                assert_eq!(database_batch.table_batches.len(), 1);

                mutable_batch_pb::decode::write_table_batch(
                    &mut batch,
                    &database_batch.table_batches[0],
                )
                .unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, write_pb);
criterion_main!(benches);
