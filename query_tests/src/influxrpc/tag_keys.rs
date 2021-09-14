use datafusion::logical_plan::{col, lit};
use predicate::predicate::PredicateBuilder;
use query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};

use crate::scenarios::*;

/// Creates and loads several database scenarios using the db_setup
/// function.
///
/// runs table_column_names(predicate) and compares it to the expected
/// output
macro_rules! run_tag_keys_test_case {
    ($DB_SETUP:expr, $PREDICATE:expr, $EXPECTED_NAMES:expr) => {
        test_helpers::maybe_start_logging();
        let predicate = $PREDICATE;
        let expected_names = $EXPECTED_NAMES;
        for scenario in $DB_SETUP.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!("Predicate: '{:#?}'", predicate);
            let planner = InfluxRpcPlanner::new();
            let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

            let plan = planner
                .tag_keys(db.as_ref(), predicate.clone())
                .expect("built plan successfully");
            let names = ctx
                .to_string_set(plan)
                .await
                .expect("converted plan to strings successfully");

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
async fn list_tag_columns_no_predicate() {
    let predicate = PredicateBuilder::default().build();
    let expected_tag_keys = vec!["borough", "city", "county", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_columns_timestamp() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(150, 201)
        .build();
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}
#[tokio::test]
async fn list_tag_columns_predicate() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let expected_tag_keys = vec!["city", "county", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_columns_timestamp_and_predicate() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(150, 201)
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_columns_measurement_name() {
    let predicate = PredicateBuilder::default().table("o2").build();
    let expected_tag_keys = vec!["borough", "city", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_timestamp() {
    let predicate = PredicateBuilder::default()
        .table("o2")
        .timestamp_range(150, 201)
        .build();
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_predicate() {
    let predicate = PredicateBuilder::default()
        .table("o2")
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let expected_tag_keys = vec!["borough", "city", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_predicate_and_timestamp() {
    let predicate = PredicateBuilder::default()
        .table("o2")
        .timestamp_range(1, 550)
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case!(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys);
}

#[tokio::test]
async fn list_tag_name_end_to_end() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(0, 10000)
        .add_expr(col("host").eq(lit("server01")))
        .build();
    let expected_tag_keys = vec!["host", "name", "region"];
    run_tag_keys_test_case!(EndToEndTest {}, predicate, expected_tag_keys);
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
