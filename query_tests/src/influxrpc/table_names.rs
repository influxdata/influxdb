//! Tests for the Influx gRPC queries
use data_types::timestamp::{MAX_NANO_TIME, MIN_NANO_TIME};
use datafusion::logical_plan::{col, lit};
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::PredicateBuilder;
use query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};

use crate::scenarios::*;

/// runs table_names(predicate) and compares it to the expected
/// output
async fn run_table_names_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    expected_names: Vec<&str>,
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
        let planner = InfluxRpcPlanner::default();
        let ctx = db.new_query_context(None);

        let plan = planner
            .table_names(db.as_query_database(), predicate.clone())
            .await
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
}

#[tokio::test]
async fn list_table_names_no_data_pred() {
    run_table_names_test_case(
        TwoMeasurements {},
        InfluxRpcPredicate::default(),
        vec!["cpu", "disk"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_no_data_passes() {
    // no rows pass this predicate
    run_table_names_test_case(
        TwoMeasurementsManyFields {},
        tsp(10000000, 20000000),
        vec![],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_no_non_null_data_passes() {
    // only a single row with a null field passes this predicate (expect no table names)
    let predicate = PredicateBuilder::default()
        // only get last row of o2 (timestamp = 300)
        .timestamp_range(200, 400)
        // model predicate like _field='reading' which last row does not have
        .field_columns(vec!["reading"])
        .build();
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);

    run_table_names_test_case(TwoMeasurementsManyFields {}, predicate, vec![]).await;
}

#[tokio::test]
async fn list_table_names_no_non_null_general_data_passes() {
    // only a single row with a null field passes this predicate
    // (expect no table names) -- has a general purpose predicate to
    // force a generic plan
    let predicate = PredicateBuilder::default()
        // only get last row of o2 (timestamp = 300)
        .timestamp_range(200, 400)
        // model predicate like _field='reading' which last row does not have
        .field_columns(vec!["reading"])
        // (state = CA) OR (temp > 50)
        .add_expr(col("state").eq(lit("CA")).or(col("temp").gt(lit(50))))
        .build();
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);

    run_table_names_test_case(TwoMeasurementsManyFields {}, predicate, vec![]).await;
}

#[tokio::test]
async fn list_table_names_no_data_pred_with_delete() {
    run_table_names_test_case(
        TwoMeasurementsWithDelete {},
        InfluxRpcPredicate::default(),
        vec!["cpu", "disk"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_no_data_pred_with_delete_all() {
    run_table_names_test_case(
        TwoMeasurementsWithDeleteAll {},
        InfluxRpcPredicate::default(),
        vec!["disk"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_201() {
    run_table_names_test_case(TwoMeasurements {}, tsp(0, 201), vec!["cpu", "disk"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_201_with_delete() {
    run_table_names_test_case(
        TwoMeasurementsWithDelete {},
        tsp(0, 201),
        vec!["cpu", "disk"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_201_with_delete_all() {
    run_table_names_test_case(TwoMeasurementsWithDeleteAll {}, tsp(0, 201), vec!["disk"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_200() {
    run_table_names_test_case(TwoMeasurements {}, tsp(0, 200), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_200_with_delete() {
    run_table_names_test_case(TwoMeasurementsWithDelete {}, tsp(0, 200), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_200_with_delete_all() {
    run_table_names_test_case(TwoMeasurementsWithDeleteAll {}, tsp(0, 200), vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_50_101() {
    run_table_names_test_case(TwoMeasurements {}, tsp(50, 101), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_50_101_with_delete() {
    run_table_names_test_case(TwoMeasurementsWithDelete {}, tsp(50, 101), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_50_101_with_delete_all() {
    run_table_names_test_case(TwoMeasurementsWithDeleteAll {}, tsp(50, 101), vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_101_160() {
    run_table_names_test_case(TwoMeasurements {}, tsp(101, 160), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_101_160_with_delete() {
    run_table_names_test_case(TwoMeasurementsWithDelete {}, tsp(101, 160), vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_101_160_with_delete_all() {
    run_table_names_test_case(TwoMeasurementsWithDeleteAll {}, tsp(101, 160), vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_250_300() {
    run_table_names_test_case(TwoMeasurements {}, tsp(250, 300), vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_250_300_with_delete() {
    run_table_names_test_case(TwoMeasurementsWithDelete {}, tsp(250, 300), vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_250_300_with_delete_all() {
    run_table_names_test_case(TwoMeasurementsWithDeleteAll {}, tsp(250, 300), vec![]).await;
}

#[tokio::test]
async fn list_table_names_max_time() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        tsp(MIN_NANO_TIME, MAX_NANO_TIME),
        vec!["cpu"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_max_i64() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        // outside valid timestamp range
        tsp(i64::MIN, i64::MAX),
        vec!["cpu"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_time_less_one() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        tsp(MIN_NANO_TIME, MAX_NANO_TIME - 1),
        vec![],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_max_time_greater_one() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        // one more than max timestamp
        tsp(MIN_NANO_TIME + 1, MAX_NANO_TIME),
        vec![],
    )
    .await;
}

// Note when table names supports general purpose predicates, add a
// test here with a `_measurement` predicate
// https://github.com/influxdata/influxdb_iox/issues/762

// make a single timestamp predicate between r1 and r2
fn tsp(r1: i64, r2: i64) -> InfluxRpcPredicate {
    InfluxRpcPredicate::new(
        None,
        PredicateBuilder::default().timestamp_range(r1, r2).build(),
    )
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
