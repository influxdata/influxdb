use crate::scenarios::*;
use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
use datafusion::prelude::{col, lit};
use iox_query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};
use predicate::{rpc_predicate::InfluxRpcPredicate, Predicate};

use super::util::make_empty_tag_ref_expr;

/// Creates and loads several database scenarios using the db_setup function.
///
/// Runs table_column_names(predicate) and compares it to the expected output.
async fn run_tag_keys_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    expected_tag_keys: Vec<&str>,
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
            .tag_keys(db.as_query_namespace_arc(), predicate.clone())
            .await
            .expect("built plan successfully");
        let names = ctx
            .to_string_set(plan)
            .await
            .expect("converted plan to strings successfully");

        assert_eq!(
            names,
            to_stringset(&expected_tag_keys),
            "Error in  scenario '{}'\n\nexpected:\n{:?}\nactual:\n{:?}",
            scenario_name,
            expected_tag_keys,
            names
        );
    }
}

#[tokio::test]
async fn list_tag_columns_with_no_tags() {
    run_tag_keys_test_case(
        OneMeasurementNoTags {},
        InfluxRpcPredicate::default(),
        vec![],
    )
    .await;

    let predicate = Predicate::default().with_range(0, 1000);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    run_tag_keys_test_case(OneMeasurementNoTags {}, predicate, vec![]).await;
}

#[tokio::test]
async fn list_tag_columns_no_predicate() {
    let expected_tag_keys = vec!["borough", "city", "county", "state"];
    run_tag_keys_test_case(
        TwoMeasurementsManyNulls {},
        InfluxRpcPredicate::default(),
        expected_tag_keys,
    )
    .await;
}

// NGA todo: add delete tests when TwoMeasurementsManyNullsWithDelete available

#[tokio::test]
async fn list_tag_columns_timestamp() {
    let predicate = Predicate::default().with_range(150, 201);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_predicate() {
    let predicate = Predicate::default().with_expr(col("state").eq(lit("MA"))); // state=MA
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "county", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_predicate_negative_nonexistent_column2() {
    let predicate = Predicate::default()
        .with_expr(col("state").eq(lit("MA"))) // state=MA
        .with_expr(make_empty_tag_ref_expr("host").not_eq(lit("server01"))); // nonexistent column with !=; always true
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "county", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_pred() {
    // Select only the following line using a _measurement predicate
    //
    // "o2,state=NY,city=NYC temp=61.0 500",
    let predicate = Predicate::default()
        .with_range(450, 550)
        .with_expr(col("_measurement").eq(lit("o2"))); // _measurement=o2
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_timestamp_and_predicate() {
    let predicate = Predicate::default()
        .with_range(150, 201)
        .with_expr(col("state").eq(lit("MA"))); // state=MA
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name() {
    let predicate = InfluxRpcPredicate::new_table("o2", Default::default());
    let expected_tag_keys = vec!["borough", "city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_timestamp() {
    let predicate = Predicate::default().with_range(150, 201);
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_predicate() {
    let predicate = Predicate::default().with_expr(col("state").eq(lit("NY"))); // state=NY
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
    let expected_tag_keys = vec!["borough", "city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_predicate_and_timestamp() {
    let predicate = Predicate::default()
        .with_range(1, 550)
        .with_expr(col("state").eq(lit("NY"))); // state=NY
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_end_to_end() {
    let predicate = Predicate::default()
        .with_range(0, 10000)
        .with_expr(col("host").eq(lit("server01")));
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host", "name", "region"];
    run_tag_keys_test_case(EndToEndTest {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_time() {
    test_helpers::maybe_start_logging();
    let predicate = Predicate::default().with_range(MIN_NANO_TIME, i64::MAX);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host"];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_all_time() {
    test_helpers::maybe_start_logging();
    let predicate = Predicate::default().with_range(MIN_NANO_TIME, MAX_NANO_TIME + 1);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host"];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_time_excluded() {
    test_helpers::maybe_start_logging();
    let predicate = Predicate::default().with_range(MIN_NANO_TIME + 1, MAX_NANO_TIME); // exclusive end
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec![];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_time_included() {
    test_helpers::maybe_start_logging();
    // The predicate matters (since MIN_NANO_TIME would be filtered out) and so cannot be optimized away
    let predicate = Predicate::default().with_range(MIN_NANO_TIME + 1, MAX_NANO_TIME + 1);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host"];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_with_periods() {
    test_helpers::maybe_start_logging();
    let predicate = Predicate::default().with_range(0, 1700000001000000000);
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["tag.one", "tag.two"];
    run_tag_keys_test_case(PeriodsInNames {}, predicate, expected_tag_keys).await;
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
