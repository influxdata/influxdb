use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use read_buffer::{BinaryExpr, ChunkMetrics, Predicate, RBChunk};
use schema::builder::SchemaBuilder;
use std::sync::Arc;

const BASE_TIME: i64 = 1351700038292387000_i64;
const ONE_MS: i64 = 1_000_000;

fn satisfies_predicate(c: &mut Criterion) {
    let rb = generate_row_group(500_000);
    let chunk = RBChunk::new("table_a", rb, ChunkMetrics::new_unregistered());

    // no predicate
    benchmark_satisfies_predicate(
        c,
        "database_satisfies_predicate_all_tables",
        &chunk,
        Predicate::default(),
        true,
    );

    // predicate but meta-data rules out matches
    benchmark_satisfies_predicate(
        c,
        "database_satisfies_predicate_meta_pred_no_match",
        &chunk,
        Predicate::new(vec![BinaryExpr::from(("env", "=", "zoo"))]),
        false,
    );

    // predicate - single expression matches at least one row
    benchmark_satisfies_predicate(
        c,
        "database_satisfies_predicate_single_pred_match",
        &chunk,
        Predicate::new(vec![BinaryExpr::from(("env", "=", "prod"))]),
        true,
    );

    // predicate - at least one row matches all expressions
    benchmark_satisfies_predicate(
        c,
        "database_satisfies_predicate_multi_pred_match",
        &chunk,
        Predicate::new(vec![
            BinaryExpr::from(("env", "=", "prod")),
            BinaryExpr::from(("time", ">=", BASE_TIME)),
            BinaryExpr::from(("time", "<", BASE_TIME + (ONE_MS * 10000))),
        ]),
        true,
    );
}

fn benchmark_satisfies_predicate(
    c: &mut Criterion,
    bench_name: &str,
    chunk: &RBChunk,
    predicate: Predicate,
    satisfies: bool,
) {
    c.bench_function(bench_name, |b| {
        b.iter_batched(
            || predicate.clone(), // don't want to time predicate cloning
            |predicate: Predicate| {
                assert_eq!(chunk.satisfies_predicate(&predicate), satisfies);
            },
            BatchSize::SmallInput,
        );
    });
}

// generate a row group with three columns of varying cardinality.
fn generate_row_group(rows: usize) -> RecordBatch {
    let schema = SchemaBuilder::new()
        .non_null_tag("env")
        .non_null_tag("container_id")
        .timestamp()
        .build()
        .unwrap();

    let container_ids = (0..rows)
        .into_iter()
        .map(|i| format!("my_container_{:?}", i))
        .collect::<Vec<_>>();

    let data: Vec<ArrayRef> = vec![
        // sorted 2 cardinality column
        Arc::new(StringArray::from(
            (0..rows)
                .into_iter()
                .map(|i| if i < rows / 2 { "prod" } else { "dev" })
                .collect::<Vec<_>>(),
        )),
        // completely unique cardinality column
        Arc::new(StringArray::from(
            container_ids
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
        )),
        // ms increasing time column;
        Arc::new(Int64Array::from(
            (0..rows)
                .into_iter()
                .map(|i| BASE_TIME + (i as i64 * ONE_MS))
                .collect::<Vec<_>>(),
        )),
    ];

    RecordBatch::try_new(schema.into(), data).unwrap()
}

criterion_group!(benches, satisfies_predicate);
criterion_main!(benches);
