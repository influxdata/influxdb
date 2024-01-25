use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use data_types::StatValues;
use mutable_batch::{
    column::{recompute_min_max, Column, ColumnData},
    writer::Writer,
    MutableBatch,
};

const N_VALUES: usize = 16_000; // Must be multiple of 8

fn generate_f64() -> Column {
    let mut mb = MutableBatch::default();

    let mut w = Writer::new(&mut mb, N_VALUES);
    let mask = std::iter::repeat(0b01010101)
        .take(N_VALUES / 8)
        .collect::<Vec<_>>();

    let values = (0..).map(|v| v as f64).take(N_VALUES / 2);

    w.write_f64("v", Some(mask.as_slice()), values)
        .expect("failed to generate test column");

    w.commit();

    mb.column("v").unwrap().clone()
}

fn generate_u64() -> Column {
    let mut mb = MutableBatch::default();

    let mut w = Writer::new(&mut mb, N_VALUES);
    let mask = std::iter::repeat(0b01010101)
        .take(N_VALUES / 8)
        .collect::<Vec<_>>();

    let values = (0..).map(|v| v as u64).take(N_VALUES / 2);

    w.write_u64("v", Some(mask.as_slice()), values)
        .expect("failed to generate test column");

    w.commit();

    mb.column("v").unwrap().clone()
}

fn generate_bool() -> Column {
    let mut mb = MutableBatch::default();

    let mut w = Writer::new(&mut mb, N_VALUES);
    let mask = std::iter::repeat(0b01010101)
        .take(N_VALUES / 8)
        .collect::<Vec<_>>();

    let values = (0..).map(|v| v & 1 == 0).take(N_VALUES / 2);

    w.write_bool("v", Some(mask.as_slice()), values)
        .expect("failed to generate test column");

    w.commit();

    mb.column("v").unwrap().clone()
}

fn generate_tag() -> Column {
    let mut mb = MutableBatch::default();

    let mut w = Writer::new(&mut mb, N_VALUES);
    let mask = std::iter::repeat(0b01010101)
        .take(N_VALUES / 8)
        .collect::<Vec<_>>();

    let values = (0..)
        .map(|v| (v % 100).to_string())
        .take(N_VALUES / 2)
        .collect::<Vec<_>>();

    w.write_tag(
        "v",
        Some(mask.as_slice()),
        values.iter().map(|v| v.as_str()),
    )
    .expect("failed to generate test column");

    w.commit();

    mb.column("v").unwrap().clone()
}

fn bench_rebuild(data: &mut Column) {
    recompute_min_max(data);
}

fn bench_stats(col: &Column) {
    match col.data() {
        ColumnData::F64(data, _) => {
            let mut s = StatValues::new(None, None, N_VALUES as _, None);
            for (i, v) in data.iter().enumerate() {
                if col.valid_mask().get(i) {
                    s.update(v)
                }
            }
            black_box(s);
        }
        ColumnData::I64(data, _) => {
            let mut s = StatValues::new(None, None, N_VALUES as _, None);
            for (i, v) in data.iter().enumerate() {
                if col.valid_mask().get(i) {
                    s.update(v)
                }
            }
            black_box(s);
        }
        ColumnData::U64(data, _) => {
            let mut s = StatValues::new(None, None, N_VALUES as _, None);
            for (i, v) in data.iter().enumerate() {
                if col.valid_mask().get(i) {
                    s.update(v)
                }
            }
            black_box(s);
        }
        ColumnData::Bool(data, _) => {
            let mut s = StatValues::new(None, None, N_VALUES as _, None);
            for (i, v) in data.iter().enumerate() {
                if col.valid_mask().get(i) {
                    s.update(&v)
                }
            }
            black_box(s);
        }
        ColumnData::String(data, _) => {
            let mut s = StatValues::new(None, None, N_VALUES as _, None);
            for (i, v) in data.iter().enumerate() {
                if col.valid_mask().get(i) {
                    s.update(v)
                }
            }
            black_box(s);
        }
        ColumnData::Tag(data, dict, _) => {
            let mut s = StatValues::new(None, None, N_VALUES as _, None);
            for (i, id) in data.iter().enumerate() {
                if col.valid_mask().get(i) {
                    s.update(dict.lookup_id(*id).unwrap())
                }
            }
            black_box(s);
        }
    }
}

fn run_bench(col: Column, c: &mut Criterion) {
    let mut group = c.benchmark_group(col.data().to_string());
    group.throughput(Throughput::Bytes(col.size() as u64));
    group.bench_function("StatValues", |b| {
        b.iter(|| {
            bench_stats(&col);
        });
    });
    group.bench_function("recompute_min_max", |b| {
        b.iter_batched(
            || col.clone(),
            |mut col| {
                bench_rebuild(&mut col);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

pub fn bench_statistics(c: &mut Criterion) {
    run_bench(generate_f64(), c);
    run_bench(generate_u64(), c);
    run_bench(generate_bool(), c);
    run_bench(generate_tag(), c);
}

criterion_group!(benches, bench_statistics);
criterion_main!(benches);
