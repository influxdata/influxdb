use criterion::{BenchmarkId, Criterion, Throughput};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;
use rand_distr::{Distribution, Normal};

use packers::{sorter, Packers};
use read_buffer::benchmarks::{Column, ColumnType, RowGroup};
use read_buffer::{AggregateType, Predicate};

const ONE_MS: i64 = 1_000_000;

pub fn read_group(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let row_group = generate_row_group(500_000, &mut rng);
    read_group_predicate_all_time(c, &row_group, &mut rng);
    read_group_pre_computed_groups(c, &row_group, &mut rng);
}

// These benchmarks track the performance of read_group using the general
// approach of building up a mapping of group keys. To avoid hitting the
// optimised no predicate implementation we apply a time predicate that covers
// the `RowGroup`.
fn read_group_predicate_all_time(c: &mut Criterion, row_group: &RowGroup, rng: &mut ThreadRng) {
    // This benchmark fixes the number of rows in the `RowGroup` (500K), and
    // varies the cardinality of the group keys.
    let time_pred = Predicate::with_time_range(&[], i64::MIN, i64::MAX);
    benchmark_read_group_vary_cardinality(
        c,
        "row_group_read_group_all_time_vary_cardinality",
        &row_group,
        &time_pred,
        // grouping columns and expected cardinality
        vec![
            (vec!["env"], 2),
            (vec!["env", "data_centre"], 20),
            (vec!["data_centre", "cluster"], 200),
            (vec!["cluster", "node_id"], 2000),
            (vec!["cluster", "node_id", "pod_id"], 20000),
        ]
        .as_slice(),
    );

    // This benchmark fixes the cardinality of the group keys and varies the
    // number of columns grouped to produce that group key cardinality.
    benchmark_read_group_vary_group_cols(
        c,
        "row_group_read_group_all_time_vary_columns",
        &row_group,
        &time_pred,
        // number of cols to group on and expected cardinality
        vec![
            (vec!["pod_id"], 20000),
            (vec!["node_id", "pod_id"], 20000),
            (vec!["cluster", "node_id", "pod_id"], 20000),
            (vec!["data_centre", "cluster", "node_id", "pod_id"], 20000),
        ]
        .as_slice(),
    );

    // This benchmark fixes the cardinality of the group keys and the number of
    // columns grouped on. It then varies the number of rows in the  `RowGroup`
    // to be processed.
    benchmark_read_group_vary_rows(
        c,
        "row_group_read_group_all_time_vary_rows",
        &[250_000, 500_000, 750_000, 1_000_000], //  `RowGroup` row sizes to vary
        &time_pred,
        (vec!["node_id", "pod_id"], 20000),
        rng,
    );
}

// These benchmarks track the performance of read_group when it is able to use
// the per-group bitsets provided by RLE-encoded columns. These code-path are
// hit due to the encoding of the grouping-columns and the lack of predicates on
// the query.
fn read_group_pre_computed_groups(c: &mut Criterion, row_group: &RowGroup, rng: &mut ThreadRng) {
    // This benchmark fixes the number of rows in the  `RowGroup` (500K), and
    // varies the cardinality of the group keys.
    benchmark_read_group_vary_cardinality(
        c,
        "row_group_read_group_pre_computed_groups_vary_cardinality",
        &row_group,
        &Predicate::default(),
        // grouping columns and expected cardinality
        vec![
            (vec!["env"], 2),
            (vec!["env", "data_centre"], 20),
            (vec!["data_centre", "cluster"], 200),
            (vec!["cluster", "node_id"], 2000),
        ]
        .as_slice(),
    );

    // This benchmark fixes the cardinality of the group keys and varies the
    // number of columns grouped to produce that group key cardinality.
    benchmark_read_group_vary_group_cols(
        c,
        "row_group_read_group_pre_computed_groups_vary_columns",
        &row_group,
        &Predicate::default(),
        // number of cols to group on and expected cardinality
        vec![
            (vec!["cluster"], 200),
            (vec!["data_centre", "cluster"], 200),
            (vec!["env", "data_centre", "cluster"], 200),
        ]
        .as_slice(),
    );

    // This benchmark fixes the cardinality of the group keys and the number of
    // columns grouped on. It then varies the number of rows in the `RowGroup`
    // to be processed.
    benchmark_read_group_vary_rows(
        c,
        "row_group_read_group_pre_computed_groups_vary_rows",
        &[250_000, 500_000, 750_000, 1_000_000], // `RowGroup` row sizes to vary
        &Predicate::default(),
        (vec!["data_centre", "cluster"], 200),
        rng,
    );
}

// This benchmarks the impact that the cardinality of group keys has on the
// performance of read_group.
fn benchmark_read_group_vary_cardinality(
    c: &mut Criterion,
    benchmark_group_name: &str,
    row_group: &RowGroup,
    predicate: &Predicate,
    cardinalities: &[(Vec<&str>, usize)],
) {
    let mut group = c.benchmark_group(benchmark_group_name);

    for (group_cols, expected_cardinality) in cardinalities {
        // benchmark measures the throughput of group creation.
        group.throughput(Throughput::Elements(*expected_cardinality as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "cardinality_{:?}_columns_{:?}_rows_{:?}",
                expected_cardinality,
                &group_cols.len(),
                500_000
            )),
            &expected_cardinality,
            |b, expected_cardinality| {
                b.iter(|| {
                    let result = row_group.read_aggregate(
                        predicate,
                        group_cols.as_slice(),
                        &[("duration", AggregateType::Count)],
                    );

                    // data_centre cardinality is split across env
                    assert_eq!(result.cardinality(), **expected_cardinality, "{}", &result);
                });
            },
        );
    }
    group.finish();
}

// This benchmarks the impact that the number of rows in a `RowGroup` has on the
// performance of read_group.
fn benchmark_read_group_vary_rows(
    c: &mut Criterion,
    benchmark_group_name: &str,
    row_sizes: &[usize],
    predicate: &Predicate,
    group_columns: (Vec<&str>, usize),
    rng: &mut ThreadRng,
) {
    let mut group = c.benchmark_group(benchmark_group_name);

    for num_rows in row_sizes {
        let row_group = generate_row_group(*num_rows, rng);

        // benchmark measures the throughput of group creation.
        group.throughput(Throughput::Elements(group_columns.1 as u64));

        group.bench_function(
            BenchmarkId::from_parameter(format!(
                "cardinality_{:?}_columns_{:?}_rows_{:?}",
                group_columns.1,
                group_columns.0.len(),
                num_rows
            )),
            |b| {
                b.iter(|| {
                    let result = row_group.read_aggregate(
                        predicate,
                        group_columns.0.as_slice(),
                        &[("duration", AggregateType::Count)],
                    );

                    // data_centre cardinality is split across env
                    assert_eq!(result.cardinality(), group_columns.1, "{}", &result);
                });
            },
        );
    }
    group.finish();
}

// This benchmarks the impact that the number of group columns has on the
// performance of read_group.
fn benchmark_read_group_vary_group_cols(
    c: &mut Criterion,
    benchmark_group_name: &str,
    row_group: &RowGroup,
    predicates: &Predicate,
    group_columns: &[(Vec<&str>, usize)],
) {
    let mut group = c.benchmark_group(benchmark_group_name);

    for (group_cols, expected_cardinality) in group_columns {
        let num_cols = group_cols.len();
        // benchmark measures the throughput of group creation.
        group.throughput(Throughput::Elements(*expected_cardinality as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "cardinality_{:?}_columns_{:?}_rows_{:?}",
                *expected_cardinality, num_cols, 500_000
            )),
            &group_cols,
            |b, group_cols| {
                b.iter(|| {
                    let result = row_group.read_aggregate(
                        predicates,
                        group_cols.as_slice(),
                        &[("duration", AggregateType::Count)],
                    );

                    assert_eq!(result.cardinality(), *expected_cardinality, "{}", &result);
                });
            },
        );
    }
    group.finish();
}

//
// This generates a `RowGroup` with a known schema, ~known column cardinalities
// and variable number of rows.
//
// The schema and cardinalities are in-line with a tracing data use-case.
fn generate_row_group(rows: usize, rng: &mut ThreadRng) -> RowGroup {
    let mut timestamp = 1351700038292387000_i64;
    let spans_per_trace = 10;

    let mut column_packers: Vec<Packers> = vec![
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // env (card 2)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // data_centre (card 20)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // cluster (card 200)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // user_id (card 200,000)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // request_id (card 2,000,000)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // node_id (card 2,000)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // pod_id (card 20,000)
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // trace_id (card "rows / 10")
        Packers::from(Vec::<Option<String>>::with_capacity(rows)), // span_id (card "rows")
        Packers::from(Vec::<Option<i64>>::with_capacity(rows)),    // duration
        Packers::from(Vec::<Option<i64>>::with_capacity(rows)),    // time
    ];

    let n = rows / spans_per_trace;
    for _ in 0..n {
        column_packers =
            generate_trace_for_row_group(spans_per_trace, timestamp, column_packers, rng);

        // next trace is ~10 seconds in the future
        timestamp += 10_000 * ONE_MS;
    }

    // sort the packers according to lowest to highest cardinality excluding
    // columns that are likely to be unique.
    //
    // - env, data_centre, cluster, node_id, pod_id, user_id, request_id, time
    sorter::sort(&mut column_packers, &[0, 1, 2, 5, 6, 3, 4, 10]).unwrap();

    // create columns
    let columns = vec![
        (
            "env".to_string(),
            ColumnType::Tag(Column::from(column_packers[0].str_packer().values())),
        ),
        (
            "data_centre".to_string(),
            ColumnType::Tag(Column::from(column_packers[1].str_packer().values())),
        ),
        (
            "cluster".to_string(),
            ColumnType::Tag(Column::from(column_packers[2].str_packer().values())),
        ),
        (
            "user_id".to_string(),
            ColumnType::Tag(Column::from(column_packers[3].str_packer().values())),
        ),
        (
            "request_id".to_string(),
            ColumnType::Tag(Column::from(column_packers[4].str_packer().values())),
        ),
        (
            "node_id".to_string(),
            ColumnType::Tag(Column::from(column_packers[5].str_packer().values())),
        ),
        (
            "pod_id".to_string(),
            ColumnType::Tag(Column::from(column_packers[6].str_packer().values())),
        ),
        (
            "trace_id".to_string(),
            ColumnType::Tag(Column::from(column_packers[7].str_packer().values())),
        ),
        (
            "span_id".to_string(),
            ColumnType::Tag(Column::from(column_packers[8].str_packer().values())),
        ),
        (
            "duration".to_string(),
            ColumnType::Field(Column::from(
                column_packers[9].i64_packer().some_values().as_slice(),
            )),
        ),
        (
            "time".to_string(),
            ColumnType::Time(Column::from(
                column_packers[10].i64_packer().some_values().as_slice(),
            )),
        ),
    ];

    RowGroup::new(rows as u32, columns)
}

fn generate_trace_for_row_group(
    spans_per_trace: usize,
    timestamp: i64,
    mut column_packers: Vec<Packers>,
    rng: &mut ThreadRng,
) -> Vec<Packers> {
    let env_idx = 0;
    let data_centre_idx = 1;
    let cluster_idx = 2;
    let user_id_idx = 3;
    let request_id_idx = 4;
    let node_id_idx = 5;
    let pod_id_idx = 6;
    let trace_id_idx = 7;
    let span_id_idx = 8;
    let duration_idx = 9;
    let time_idx = 10;

    let env_value = rng.gen_range(0_u8..2);
    let env = format!("env-{:?}", env_value); // cardinality of 2.

    let data_centre_value = rng.gen_range(0_u8..10);
    let data_centre = format!("data_centre-{:?}-{:?}", env_value, data_centre_value); // cardinality of 2 * 10  = 20

    let cluster_value = rng.gen_range(0_u8..10);
    let cluster = format!(
        "cluster-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value // cardinality of 2 * 10 * 10 = 200
    );

    // user id is dependent on the cluster
    let user_id_value = rng.gen_range(0_u32..1000);
    let user_id = format!(
        "uid-{:?}-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value,
        user_id_value // cardinality of 2 * 10 * 10 * 1000 = 200,000
    );

    let request_id_value = rng.gen_range(0_u32..10);
    let request_id = format!(
        "rid-{:?}-{:?}-{:?}-{:?}-{:?}",
        env_value,
        data_centre_value,
        cluster_value,
        user_id_value,
        request_id_value // cardinality of 2 * 10 * 10 * 1000 * 10 = 2,000,000
    );

    let trace_id = rng
        .sample_iter(&Alphanumeric)
        .map(char::from)
        .take(8)
        .collect::<String>();

    // the trace should move across hosts, which in this setup would be nodes
    // and pods.
    let normal = Normal::new(10.0, 5.0).unwrap();
    let node_id_prefix = format!("{}-{}-{}", env_value, data_centre_value, cluster_value,);
    for _ in 0..spans_per_trace {
        // these values are not the same for each span so need to be generated
        // separately.
        let node_id = rng.gen_range(0..10); // cardinality is 2 * 10 * 10 * 10 = 2,000

        column_packers[pod_id_idx].str_packer_mut().push(format!(
            "pod_id-{}-{}-{}",
            node_id_prefix,
            node_id,
            rng.gen_range(0..10) // cardinality is 2 * 10 * 10 * 10 * 10 = 20,000
        ));

        column_packers[node_id_idx]
            .str_packer_mut()
            .push(format!("node_id-{}-{}", node_id_prefix, node_id));

        // randomly generate a span_id
        column_packers[span_id_idx].str_packer_mut().push(
            rng.sample_iter(&Alphanumeric)
                .map(char::from)
                .take(8)
                .collect::<String>(),
        );

        // randomly generate some duration times in milliseconds.
        column_packers[duration_idx].i64_packer_mut().push(
            (normal.sample(rng) * ONE_MS as f64)
                .max(ONE_MS as f64) // minimum duration is 1ms
                .round() as i64,
        );
    }

    column_packers[env_idx]
        .str_packer_mut()
        .fill_with(env, spans_per_trace);
    column_packers[data_centre_idx]
        .str_packer_mut()
        .fill_with(data_centre, spans_per_trace);
    column_packers[cluster_idx]
        .str_packer_mut()
        .fill_with(cluster, spans_per_trace);
    column_packers[user_id_idx]
        .str_packer_mut()
        .fill_with(user_id, spans_per_trace);
    column_packers[request_id_idx]
        .str_packer_mut()
        .fill_with(request_id, spans_per_trace);
    column_packers[trace_id_idx]
        .str_packer_mut()
        .fill_with(trace_id, spans_per_trace);

    column_packers[time_idx]
        .i64_packer_mut()
        .fill_with(timestamp, spans_per_trace);

    column_packers
}

// criterion_group!(benches, read_group);
// criterion_main!(benches);
