use datafusion::prelude::{col, lit};
use iox_query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::Predicate;

use crate::scenarios::*;

/// runs tag_value(predicate) and compares it to the expected
/// output
async fn run_tag_values_test_case<D>(
    db_setup: D,
    tag_name: &str,
    predicate: InfluxRpcPredicate,
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
        let ctx = db.new_query_context(None);
        let planner = InfluxRpcPlanner::new(ctx.child_ctx("planner"));

        let plan = planner
            .tag_values(db.as_query_namespace_arc(), tag_name, predicate.clone())
            .await
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
    let predicate = Predicate::default();
    let predicate = InfluxRpcPredicate::new(None, predicate);
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

// NGA todo: add delete tests when TwoMeasurementsManyNullsWithDelete available

#[tokio::test]
async fn list_tag_values_no_predicate_state_col() {
    let tag_name = "state";
    let expected_tag_keys = vec!["CA", "MA", "NY"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        InfluxRpcPredicate::default(),
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_no_predicate_city_col() {
    let tag_name = "city";
    let expected_tag_keys = vec!["Boston", "LA", "NYC"];
    run_tag_values_test_case(
        TwoMeasurementsManyNulls {},
        tag_name,
        InfluxRpcPredicate::default(),
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_timestamp_pred_state_col() {
    let tag_name = "state";
    let predicate = Predicate::default().with_range(50, 201);
    let predicate = InfluxRpcPredicate::new(None, predicate);
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
    let predicate = Predicate::default().with_expr(col("state").eq(lit("MA"))); // state=MA
    let predicate = InfluxRpcPredicate::new(None, predicate);
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
    let predicate = Predicate::default()
        .with_range(150, 301)
        .with_expr(col("state").eq(lit("MA"))); // state=MA
    let predicate = InfluxRpcPredicate::new(None, predicate);
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
    let predicate = InfluxRpcPredicate::new_table("h2o", Default::default());
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
    let predicate = InfluxRpcPredicate::new_table("o2", Default::default());
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
    let predicate = Predicate::default().with_range(50, 201);
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
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
    let predicate = Predicate::default().with_expr(col("state").eq(lit("NY"))); // state=NY
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
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
    let predicate = Predicate::default()
        .with_range(1, 550)
        .with_expr(col("state").eq(lit("NY"))); // state=NY
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
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
    let predicate = Predicate::default()
        .with_range(1, 300) // filters out the NY row
        .with_expr(col("state").eq(lit("NY"))); // state=NY
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
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
async fn list_tag_values_measurement_pred() {
    let tag_name = "state";
    let predicate = Predicate::default()
        .with_range(1, 600) // filters out the NY row
        .with_expr(col("_measurement").not_eq(lit("o2")));
    let predicate = InfluxRpcPredicate::new(None, predicate);
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
async fn list_tag_values_measurement_pred_and_or() {
    let tag_name = "city";
    let predicate = Predicate::default()
        .with_range(1, 600) // filters out the NY row
        // since there is an 'OR' in this predicate, can't answer
        // with metadata alone
        // _measurement = 'o2' OR temp > 70.0
        .with_expr(
            col("_measurement")
                .eq(lit("o2"))
                .or(col("_value").gt(lit(70.0))),
        );
    let predicate = InfluxRpcPredicate::new(None, predicate);
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
async fn list_tag_values_field_col_on_tag() {
    let db_setup = TwoMeasurementsManyNulls {};

    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;
        println!("Running scenario '{}'", scenario_name);
        let ctx = db.new_query_context(None);
        let planner = InfluxRpcPlanner::new(ctx.child_ctx("planner"));

        // Test: temp is a field, not a tag
        let tag_name = "temp";
        let plan_result = planner
            .tag_values(
                db.as_query_namespace_arc(),
                tag_name,
                InfluxRpcPredicate::default(),
            )
            .await;

        assert_eq!(
            plan_result.unwrap_err().to_string(),
            "gRPC planner error: column \'temp\' is not a tag, it is Some(Field(Float))"
        );
    }
}

#[tokio::test]
async fn list_tag_values_field_col_does_not_exist() {
    let tag_name = "state";
    let predicate = Predicate::default()
        .with_range(0, 1000) // get all rows
        // since this field doesn't exist this predicate should match no values
        .with_expr(col("_field").eq(lit("not_a_column")));
    let predicate = InfluxRpcPredicate::new(None, predicate);
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
async fn list_tag_values_field_col_does_exist() {
    let tag_name = "state";
    let predicate = Predicate::default()
        .with_range(0, 1000000) // get all rows
        // this field does exist, but only for rows MA(not CA)
        .with_expr(col("_field").eq(lit("moisture")));
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["MA"];

    run_tag_values_test_case(
        TwoMeasurementsManyFields {},
        tag_name,
        predicate,
        expected_tag_keys,
    )
    .await;
}

#[tokio::test]
async fn list_tag_values_with_periods() {
    let tag_name = "tag.one";
    let predicate = Predicate::default().with_range(0, 1700000001000000000);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["value", "value2"];

    run_tag_values_test_case(PeriodsInNames {}, tag_name, predicate, expected_tag_keys).await;
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
