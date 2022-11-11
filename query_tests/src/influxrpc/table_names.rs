//! Tests for the Influx gRPC queries
use crate::scenarios::*;
use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
use datafusion::prelude::{col, lit};
use iox_query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};
use predicate::{rpc_predicate::InfluxRpcPredicate, Predicate};

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
        let ctx = db.new_query_context(None);
        let planner = InfluxRpcPlanner::new(ctx.child_ctx("planner"));

        let plan = planner
            .table_names(db.as_query_namespace_arc(), predicate.clone())
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
    let predicate = Predicate::default()
        // only get last row of o2 (timestamp = 300)
        .with_range(200, 400)
        // model predicate like _field='reading' which last row does not have
        .with_field_columns(vec!["reading"]);
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);

    run_table_names_test_case(TwoMeasurementsManyFields {}, predicate, vec![]).await;
}

#[tokio::test]
async fn list_table_names_no_non_null_general_data_passes() {
    // only a single row with a null field passes this predicate
    // (expect no table names) -- has a general purpose predicate to
    // force a generic plan
    let predicate = Predicate::default()
        // only get last row of o2 (timestamp = 300)
        .with_range(200, 400)
        // model predicate like _field='reading' which last row does not have
        .with_field_columns(vec!["reading"])
        // (state = CA) OR (temp > 50)
        .with_expr(col("state").eq(lit("CA")).or(col("temp").gt(lit(50))));
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);

    run_table_names_test_case(TwoMeasurementsManyFields {}, predicate, vec![]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_201() {
    run_table_names_test_case(TwoMeasurements {}, tsp(0, 201), vec!["cpu", "disk"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_0_200() {
    run_table_names_test_case(TwoMeasurements {}, tsp(0, 200), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_50_101() {
    run_table_names_test_case(TwoMeasurements {}, tsp(50, 101), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_101_160() {
    run_table_names_test_case(TwoMeasurements {}, tsp(101, 160), vec!["cpu"]).await;
}

#[tokio::test]
async fn list_table_names_data_pred_250_300() {
    run_table_names_test_case(TwoMeasurements {}, tsp(250, 300), vec![]).await;
}

#[tokio::test]
async fn list_table_names_max_time_included() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        tsp(MIN_NANO_TIME + 1, MAX_NANO_TIME + 1),
        vec!["cpu"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_max_time_excluded() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        // outside valid timestamp range
        tsp(MIN_NANO_TIME + 1, MAX_NANO_TIME),
        vec![],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_all_time() {
    run_table_names_test_case(
        MeasurementWithMaxTime {},
        tsp(MIN_NANO_TIME, MAX_NANO_TIME + 1),
        vec!["cpu"],
    )
    .await;
}

#[tokio::test]
async fn list_table_names_with_periods() {
    run_table_names_test_case(
        PeriodsInNames {},
        tsp(MIN_NANO_TIME, MAX_NANO_TIME + 1),
        vec!["measurement.one"],
    )
    .await;
}

// Note when table names supports general purpose predicates, add a
// test here with a `_measurement` predicate
// https://github.com/influxdata/influxdb_iox/issues/762

// make a single timestamp predicate between r1 and r2
fn tsp(r1: i64, r2: i64) -> InfluxRpcPredicate {
    InfluxRpcPredicate::new(None, Predicate::default().with_range(r1, r2))
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
