use datafusion::logical_plan::{col, lit};
use predicate::predicate::{Predicate, PredicateBuilder};
use query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};

use crate::scenarios::*;

/// runs tag_value(predicate) and compares it to the expected
/// output
async fn run_tag_values_test_case<D>(
    db_setup: D,
    tag_name: &str,
    predicate: Predicate,
    expected_tag_values: Vec<&str>,
) where
    D: DbSetup,
{
    test_helpers::maybe_start_logging();

    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;
        println!("Running scenario '{}'", scenario_name);
        println!("Predicate: '{:#?}'", predicate);
        let planner = InfluxRpcPlanner::new();
        let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

        let plan = planner
            .tag_values(db.as_ref(), tag_name, predicate.clone())
            .expect("built plan successfully");
        let names = ctx
            .to_string_set(plan)
            .await
            .expect("converted plan to strings successfully");

        assert_eq!(
            names,
            to_stringset(&expected_tag_values),
            "Error in  scenario '{}'\n\nexpected:\n{:?}\nactual:\n{:?}",
            scenario_name,
            expected_tag_values,
            names
        );
    }
}

#[tokio::test]
async fn list_tag_values_no_tag() {
    let predicate = PredicateBuilder::default().build();
    // If the tag is not present, expect no values back (not error)
    let tag_name = "tag_not_in_chunks";
    let expected_tag_keys = vec![];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_no_predicate_state_col() {
    let predicate = PredicateBuilder::default().build();
    let tag_name = "state";
    let expected_tag_keys = vec!["CA", "MA", "NY"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_no_predicate_city_col() {
    let tag_name = "city";
    let predicate = PredicateBuilder::default().build();
    let expected_tag_keys = vec!["Boston", "LA", "NYC"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_timestamp_pred_state_col() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default().timestamp_range(50, 201).build();
    let expected_tag_keys = vec!["CA", "MA"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_state_pred_state_col() {
    let tag_name = "city";
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let expected_tag_keys = vec!["Boston"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_timestamp_and_state_pred_state_col() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default()
        .timestamp_range(150, 301)
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let expected_tag_keys = vec!["MA"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_table_pred_state_col() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default().table("h2o").build();
    let expected_tag_keys = vec!["CA", "MA"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_table_pred_city_col() {
    let tag_name = "city";
    let predicate = PredicateBuilder::default().table("o2").build();
    let expected_tag_keys = vec!["Boston", "NYC"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_table_and_timestamp_and_table_pred_state_col() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default()
        .table("o2")
        .timestamp_range(50, 201)
        .build();
    let expected_tag_keys = vec!["MA"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_table_and_state_pred_state_col() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default()
        .table("o2")
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let expected_tag_keys = vec!["NY"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_table_and_timestamp_and_state_pred_state_col() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default()
        .table("o2")
        .timestamp_range(1, 550)
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let expected_tag_keys = vec!["NY"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_table_and_timestamp_and_state_pred_state_col_no_rows() {
    let tag_name = "state";
    let predicate = PredicateBuilder::default()
        .table("o2")
        .timestamp_range(1, 300) // filters out the NY row
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let expected_tag_keys = vec![];

    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_field_col() {
    let db_setup = TwoMeasurementsManyNulls {};
    let predicate = PredicateBuilder::default().build();

    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;
        println!("Running scenario '{}'", scenario_name);
        println!("Predicate: '{:#?}'", predicate);
        let planner = InfluxRpcPlanner::new();

        // Test: temp is a field, not a tag
        let tag_name = "temp";
        let plan_result = planner.tag_values(db.as_ref(), tag_name, predicate.clone());

        assert_eq!(
            plan_result.unwrap_err().to_string(),
            "gRPC planner error: column \'temp\' is not a tag, it is Some(Field(Float))"
        );
    }
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
