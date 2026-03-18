#![expect(unused_crate_dependencies)]

use core::hint::black_box;

use criterion::*;
use generated_types::influxdata::pbdata::v1::TableBatch;
use mutable_batch::MutableBatch;
use mutable_batch_pb::{decode::write_table_batch, encode::encode_batch};

fn lp_to_table_batch(lp: &str) -> TableBatch {
    let mb = lp_to_mutable_batch(lp);
    encode_batch(1, &mb)
}

fn lp_to_mutable_batch(lp: &str) -> MutableBatch {
    if lp.is_empty() || lp.lines().all(|l| l.is_empty()) {
        return MutableBatch::default();
    }

    let batches = mutable_batch_lp::lines_to_batches(lp, 0).unwrap();
    assert_eq!(batches.len(), 1);

    batches.into_iter().next().unwrap().1
}

fn run(c: &mut Criterion, initial: &str, new: &[&str], id: &str) {
    let mb = lp_to_mutable_batch(initial);
    let tb = new
        .iter()
        .map(|lp| lp_to_table_batch(lp))
        .collect::<Vec<_>>();

    c.bench_function(id, |bencher| {
        bencher.iter_batched(
            || mb.clone(),
            |mut mb| {
                for tb in &*tb {
                    black_box(write_table_batch(&mut mb, tb)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn read_line_proto_and_split<const N: usize>(name: &'static str) -> [String; N] {
    let path = format!(
        "{}/../test_fixtures/lineproto/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let lineproto = std::fs::read_to_string(path).unwrap();

    let mut table = None;
    let lines = lineproto
        .lines()
        .filter(|l| match table {
            None => {
                table = Some(l.split_once(',').unwrap().0);
                true
            }
            Some(t) => l.starts_with(t),
        })
        .collect::<Vec<_>>();

    let quarter = lines.len() / N;
    std::array::from_fn(|i| lines[i * quarter..][..quarter].join("\n"))
}

fn bench_file(c: &mut Criterion, file: &'static str) {
    let [quarter_1, quarter_2, quarter_3, quarter_4] = read_line_proto_and_split(file);
    run(
        c,
        &quarter_1,
        &[&quarter_2, &quarter_3, &quarter_4],
        &format!("{file}_three_quarters"),
    );

    let first_half = quarter_1 + "\n" + &quarter_2;
    let second_half = quarter_3 + "\n" + &quarter_4;
    run(c, &first_half, &[&second_half], &format!("{file}_halved"));

    let full = first_half + "\n" + &second_half;
    run(c, "", &[&full], &format!("{file}_full"));
}

fn air_and_water(c: &mut Criterion) {
    bench_file(c, "air_and_water.lp");
}

fn influxql_logs(c: &mut Criterion) {
    bench_file(c, "influxql_logs.lp");
}

fn metrics(c: &mut Criterion) {
    bench_file(c, "metrics.lp");
}

fn prometheus(c: &mut Criterion) {
    bench_file(c, "prometheus.lp");
}

fn temperature(c: &mut Criterion) {
    bench_file(c, "temperature.lp");
}

criterion_group!(
    benches,
    air_and_water,
    influxql_logs,
    metrics,
    prometheus,
    temperature
);
criterion_main!(benches);
