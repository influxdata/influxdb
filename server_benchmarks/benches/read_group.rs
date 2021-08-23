use std::io::Read;

use criterion::{BenchmarkId, Criterion};
use datafusion::logical_plan::{col, lit};
// This is a struct that tells Criterion.rs to use the "futures" crate's
// current-thread executor
use flate2::read::GzDecoder;
use tokio::runtime::Runtime;

use query::exec::ExecutorType;
use query::predicate::PredicateBuilder;
use query::{exec::Executor, predicate::Predicate};
use query::{frontend::influxrpc::InfluxRpcPlanner, group_by::Aggregate};
use query_tests::scenarios::DbScenario;
use server::db::Db;

// Uses the `query_tests` module to generate some chunk scenarios, specifically
// the scenarios where there are:
//
// - a single open mutable buffer chunk;
// - a closed mutable buffer chunk and another open one;
// - an open mutable buffer chunk and a closed read buffer chunk;
// - two closed read buffer chunks.
//
// The chunks are all fed the *same* line protocol, so these benchmarks are
// useful for assessing the differences in performance between querying the
// chunks held in different execution engines.
//
// These benchmarks use a synthetically generated set of line protocol using
// `inch`. Each point is a new series containing 5 tag keys, which results in
// five tag columns in IOx. There is a single field column and a timestamp column.
//
//   - tag0, cardinality 2.
//   - tag1, cardinality 10.
//   - tag2, cardinality 10.
//   - tag3, cardinality 50.
//   - tag4, cardinality 1.
//
// In total there are 10K rows. The timespan of the points in the line
// protocol is around 1m of wall-clock time.
async fn setup_scenarios() -> Vec<DbScenario> {
    let raw = include_bytes!("../../tests/fixtures/lineproto/read_filter.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    let db = query_tests::scenarios::make_two_chunk_scenarios("2021-04-26T13", &lp, &lp).await;
    db
}

// Run all benchmarks for `read_group`.
pub fn benchmark_read_group(c: &mut Criterion) {
    let scenarios = Runtime::new().unwrap().block_on(setup_scenarios());
    execute_benchmark_group(c, scenarios.as_slice());
}

// Runs an async criterion benchmark against the provided scenarios and
// predicate.
fn execute_benchmark_group(c: &mut Criterion, scenarios: &[DbScenario]) {
    let planner = InfluxRpcPlanner::new();

    let predicates = vec![
        (PredicateBuilder::default().build(), "no_pred"),
        (
            PredicateBuilder::default()
                .add_expr(col("tag3").eq(lit("value49")))
                .build(),
            "with_pred_tag_3=value49",
        ),
    ];

    for scenario in scenarios {
        let DbScenario { scenario_name, db } = scenario;
        let mut group = c.benchmark_group(format!("read_group/{}", scenario_name));

        for (predicate, pred_name) in &predicates {
            // The number of expected frames, based on the expected number of
            // individual series keys, which for grouping is the same no matter
            // how many chunks we query over.
            let exp_data_frames = if predicate.is_empty() {
                10000 + 10 // 10 groups when grouping on `tag2`
            } else {
                200 + 10 // 10 groups when grouping on `tag2`
            };

            group.bench_with_input(
                BenchmarkId::from_parameter(pred_name),
                predicate,
                |b, predicate| {
                    let executor = db.executor();
                    b.to_async(Runtime::new().unwrap()).iter(|| {
                        build_and_execute_plan(
                            &planner,
                            executor.as_ref(),
                            db,
                            predicate.clone(),
                            Aggregate::Sum,
                            &["tag2"],
                            exp_data_frames,
                        )
                    });
                },
            );
        }

        group.finish();
    }
}

// Plans and runs a tag_values query.
async fn build_and_execute_plan(
    planner: &InfluxRpcPlanner,
    executor: &Executor,
    db: &Db,
    predicate: Predicate,
    agg: Aggregate,
    group: &[&str],
    exp_frames: usize,
) {
    let plan = planner
        .read_group(db, predicate, agg, group)
        .expect("built plan successfully");

    let results = executor
        .new_context(ExecutorType::Query)
        .to_series_set(plan)
        .await
        .expect("Running series set plan");

    assert_eq!(results.len(), exp_frames);
}
