use std::{collections::BTreeSet, sync::Arc};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use internal_types::schema::builder::SchemaBuilder;
use read_buffer::{BinaryExpr, Chunk, Predicate};

const BASE_TIME: i64 = 1351700038292387000_i64;
const ONE_MS: i64 = 1_000_000;

fn table_names(c: &mut Criterion) {
    let rb = generate_row_group(500_000);
    let chunk = Chunk::new(0);
    chunk.upsert_table("table_a", rb);

    // no predicate - return all the tables
    benchmark_table_names(
        c,
        "database_table_names_all_tables",
        &chunk,
        Predicate::default(),
        1,
    );

    // predicate - meta data proves not present
    benchmark_table_names(
        c,
        "database_table_names_meta_pred_no_match",
        &chunk,
        Predicate::new(vec![BinaryExpr::from(("env", "=", "zoo"))]),
        0,
    );

    // predicate - single predicate match
    benchmark_table_names(
        c,
        "database_table_names_single_pred_match",
        &chunk,
        Predicate::new(vec![BinaryExpr::from(("env", "=", "prod"))]),
        1,
    );

    // predicate - multi predicate match
    benchmark_table_names(
        c,
        "database_table_names_multi_pred_match",
        &chunk,
        Predicate::new(vec![
            BinaryExpr::from(("env", "=", "prod")),
            BinaryExpr::from(("time", ">=", BASE_TIME)),
            BinaryExpr::from(("time", "<", BASE_TIME + (ONE_MS * 10000))),
        ]),
        1,
    );
}

fn benchmark_table_names(
    c: &mut Criterion,
    bench_name: &str,
    chunk: &Chunk,
    predicate: Predicate,
    expected_rows: usize,
) {
    c.bench_function(bench_name, |b| {
        b.iter_batched(
            || predicate.clone(), // don't want to time predicate cloning
            |predicate: Predicate| {
                let tables = chunk.table_names(&predicate, &BTreeSet::new());
                assert_eq!(tables.len(), expected_rows);
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

criterion_group!(benches, table_names);
criterion_main!(benches);
