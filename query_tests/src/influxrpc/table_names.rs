//! Tests for the Influx gRPC queries
use query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
    predicate::{Predicate, PredicateBuilder, EMPTY_PREDICATE},
};

use crate::scenarios::*;

/// runs table_names(predicate) and compares it to the expected
/// output
macro_rules! run_table_names_test_case {
    ($DB_SETUP:expr, $PREDICATE:expr, $EXPECTED_NAMES:expr) => {
        test_helpers::maybe_start_logging();
        let predicate = $PREDICATE;
        for scenario in $DB_SETUP.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!("Predicate: '{:#?}'", predicate);
            let planner = InfluxRpcPlanner::new();

            let plan = planner
                .table_names(&db, predicate.clone())
                .expect("built plan successfully");
            let names = db
                .executor()
                .to_string_set(plan)
                .await
                .expect("converted plan to strings successfully");

            let expected_names = $EXPECTED_NAMES;
            assert_eq!(
                names,
                to_stringset(&expected_names),
                "Error in  scenario '{}'\n\nexpected:\n{:?}\nactual:\n{:?}",
                scenario_name,
                expected_names,
                names
            );
        }
    };
}

#[tokio::test]
async fn list_table_names_no_data_no_pred() {
    run_table_names_test_case!(NoData {}, EMPTY_PREDICATE, vec![]);
}

#[tokio::test]
async fn list_table_names_no_data_pred() {
    run_table_names_test_case!(TwoMeasurements {}, EMPTY_PREDICATE, vec!["cpu", "disk"]);
}

#[tokio::test]
async fn list_table_names_data_pred_0_201() {
    run_table_names_test_case!(TwoMeasurements {}, tsp(0, 201), vec!["cpu", "disk"]);
}

#[tokio::test]
async fn list_table_names_data_pred_0_200() {
    run_table_names_test_case!(TwoMeasurements {}, tsp(0, 200), vec!["cpu"]);
}

#[tokio::test]
async fn list_table_names_data_pred_50_101() {
    run_table_names_test_case!(TwoMeasurements {}, tsp(50, 101), vec!["cpu"]);
}

#[tokio::test]
async fn list_table_names_data_pred_250_300() {
    run_table_names_test_case!(TwoMeasurements {}, tsp(250, 300), vec![]);
}

// make a single timestamp predicate between r1 and r2
fn tsp(r1: i64, r2: i64) -> Predicate {
    PredicateBuilder::default().timestamp_range(r1, r2).build()
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
