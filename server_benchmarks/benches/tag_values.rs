use std::io::Read;

use criterion::{BenchmarkId, Criterion};
use datafusion::logical_plan::{col, lit};
// This is a struct that tells Criterion.rs to use the "futures" crate's
// current-thread executor
use flate2::read::GzDecoder;
use tokio::runtime::Runtime;

use query::frontend::influxrpc::InfluxRpcPlanner;
use query::predicate::PredicateBuilder;
use query::{exec::Executor, predicate::Predicate};
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
// useful for assessig the differences in performance between querying the
// chunks held in different execution engines.
//
// These benchmarks use a synthetically generated set of line protocol using
// `inch`. Each point is a new series containing three tag keys. Those tag keys
// are:
//
//   - tag0, cardinality 10.
//   - tag1, cardinality 100.
//   - tag2, cardinality 1,000.
//
// The timespan of the points in the line protocol is around 1m or wall-clock
// time.
async fn setup_scenarios() -> Vec<DbScenario> {
    let raw = include_bytes!("../../tests/fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    let db = query_tests::scenarios::make_two_chunk_scenarios("2021-04-12T17", &lp, &lp).await;
    db
}

// Run all benchmarks for `tag_values`.
pub fn benchmark_tag_values(c: &mut Criterion) {
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
                .add_expr(col("tag2").eq(lit("value321")))
                .build(),
            "with_pred",
        ),
    ];

    // these tags have different cardinalities: 10, 100, 1000.
    let tag_keys = &["tag0", "tag1", "tag2"];

    for scenario in scenarios {
        let DbScenario { scenario_name, db } = scenario;
        let mut group = c.benchmark_group(scenario_name);

        for (predicate, pred_name) in &predicates {
            for tag_key in tag_keys {
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{}/{}", tag_key, pred_name)),
                    tag_key,
                    |b, &tag_key| {
                        let executor = db.executor();
                        b.to_async(Runtime::new().unwrap()).iter(|| {
                            run_tag_values_query(
                                &planner,
                                executor.as_ref(),
                                db,
                                tag_key,
                                predicate.clone(),
                            )
                        });
                    },
                );
            }
        }

        group.finish();
    }
}

// Plans and runs a tag_values query.
async fn run_tag_values_query(
    planner: &InfluxRpcPlanner,
    executor: &Executor,
    db: &Db,
    tag_key: &str,
    predicate: Predicate,
) {
    let plan = planner
        .tag_values(db, &tag_key, predicate)
        .expect("built plan successfully");
    let names = executor.to_string_set(plan).await.expect(
        "converted plan to strings
                            successfully",
    );
    assert!(names.len() > 0);
}
