//! Tests for the Influx gRPC queries
use std::sync::Arc;

#[cfg(test)]
use crate::scenarios::{
    DbScenario, DbSetup, TwoMeasurements, TwoMeasurementsManyFields, TwoMeasurementsWithDelete,
    TwoMeasurementsWithDeleteAll,
};
use crate::{
    db::AbstractDb,
    influxrpc::util::run_series_set_plan_maybe_error,
    scenarios::{
        MeasurementStatusCode, MeasurementsForDefect2845, MeasurementsSortableTags,
        MeasurementsSortableTagsWithDelete, TwoMeasurementsMultiSeries,
        TwoMeasurementsMultiSeriesWithDelete, TwoMeasurementsMultiSeriesWithDeleteAll,
    },
};
use datafusion::logical_plan::{col, lit, when};
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::PredicateBuilder;
use query::frontend::influxrpc::InfluxRpcPlanner;
use test_helpers::assert_contains;

/// runs read_filter(predicate) and compares it to the expected
/// output
async fn run_read_filter_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    expected_results: Vec<&str>,
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
        let string_results = run_read_filter(predicate.clone(), db)
            .await
            .expect("Unexpected error running read filter");

        assert_eq!(
            expected_results, string_results,
            "Error in  scenario '{}'\n\nexpected:\n{:#?}\n\nactual:\n{:#?}\n\n",
            scenario_name, expected_results, string_results
        );
    }
}

/// runs read_filter(predicate) and compares it to the expected
/// output
async fn run_read_filter(
    predicate: InfluxRpcPredicate,
    db: Arc<dyn AbstractDb>,
) -> Result<Vec<String>, String> {
    let planner = InfluxRpcPlanner::default();

    let plan = planner
        .read_filter(db.as_query_database(), predicate)
        .await
        .map_err(|e| e.to_string())?;

    let ctx = db.new_query_context(None);
    run_series_set_plan_maybe_error(&ctx, plan)
        .await
        .map_err(|e| e.to_string())
}

/// runs read_filter(predicate), expecting an error and compares to expected message
async fn run_read_filter_error_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    expected_error: &str,
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
        let result = run_read_filter(predicate.clone(), db)
            .await
            .expect_err("Unexpected success running error case");

        assert_contains!(result.to_string(), expected_error);
    }
}

#[tokio::test]
async fn test_read_filter_data_no_pred() {
    let expected_results = vec![
    "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [70.4, 72.4]",
    "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200, 350], values: [90.0, 90.0]",
    "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
    "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeries {},
        InfluxRpcPredicate::default(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_exclusive_predicate() {
    let predicate = PredicateBuilder::new()
        // should not return the 350 row as predicate is
        // range.start <= ts < range.end
        .timestamp_range(349, 350)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_inclusive_predicate() {
    let predicate = PredicateBuilder::new()
        // should return  350 row!
        .timestamp_range(350, 351)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [350], values: [90.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_exact_predicate() {
    let predicate = PredicateBuilder::new()
        // should return  250 rows!
        .timestamp_range(250, 251)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [72.4]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [250], values: [51.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_tag_predicate() {
    let predicate = PredicateBuilder::new()
        // region = region
        .add_expr(col("region").eq(col("region")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // expect both series to be returned
    let expected_results = vec![
        "Series tags={_measurement=cpu, region=west, _field=user}\n  FloatPoints timestamps: [100, 150], values: [23.2, 21.0]",
        "Series tags={_measurement=disk, region=east, _field=bytes}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_invalid_predicate() {
    let predicate = PredicateBuilder::new()
        // region > 5 (region is a tag(string) column, so this predicate is invalid)
        .add_expr(col("region").gt(lit(5i32)))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_error = "Error during planning: 'Dictionary(Int32, Utf8) > Int32' can't be evaluated because there isn't a common type to coerce the types to";

    run_read_filter_error_case(TwoMeasurements {}, predicate, expected_error).await;
}

#[tokio::test]
async fn test_read_filter_invalid_predicate_case() {
    let predicate = PredicateBuilder::new()
        // https://github.com/influxdata/influxdb_iox/issues/3635
        // model what happens when a field is treated like a tag
        // CASE WHEN system" IS NULL THEN '' ELSE system END = 5;
        .add_expr(
            when(col("system").is_null(), lit(""))
                .otherwise(col("system"))
                .unwrap()
                .eq(lit(5i32)),
        )
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_error = "gRPC planner got error creating predicates: Error during planning: 'Utf8 = Int32' can't be evaluated because there isn't a common type to coerce the types to";

    run_read_filter_error_case(TwoMeasurements {}, predicate, expected_error).await;
}

#[tokio::test]
async fn test_read_filter_unknown_column_in_predicate() {
    let predicate = PredicateBuilder::new()
        // mystery_region is not a real column, so this predicate is
        // invalid but IOx should be able to handle it (and produce no results)
        .add_expr(
            col("baz")
                .eq(lit(4i32))
                .or(col("bar").and(col("mystery_region").gt(lit(5i32)))),
        )
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_no_pred_with_delete() {
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100], values: [70.4]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [350], values: [90.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        InfluxRpcPredicate::default(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_no_pred_with_delete_all() {
    // nothing from h2o table because all rows were deleted
    let expected_results = vec![
    "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
    "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDeleteAll {},
        InfluxRpcPredicate::default(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_filter() {
    // filter out one row in h20
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").eq(lit("CA"))) // state=CA
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200], values: [90.0]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeries {},
        predicate,
        expected_results.clone(),
    )
    .await;

    // Same results via a != predicate.
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").not_eq(lit("MA"))) // state=CA
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_filter_with_delete() {
    // filter out one row in h20 but the leftover row was deleted to nothing will be returned
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").eq(lit("CA"))) // state=CA
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate,
        expected_results.clone(),
    )
    .await;

    // Same results via a != predicate.
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").not_eq(lit("MA"))) // state=CA
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate,
        expected_results,
    )
    .await;

    // Use different predicate to have data returned
    let predicate = PredicateBuilder::default()
        .timestamp_range(100, 300)
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .add_expr(col("_measurement").eq(lit("h2o")))
        .build();

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100], values: [70.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_filter_fields() {
    // filter out one row in h20
    let predicate = PredicateBuilder::default()
        .field_columns(vec!["other_temp"])
        .add_expr(col("state").eq(lit("CA"))) // state=CA
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Only expect other_temp in this location
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=CA, _field=other_temp}\n  FloatPoints timestamps: [350], values: [72.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

// NGA todo: add delete tests here after we have delete scenarios for 2 chunks for 1 table

#[tokio::test]
async fn test_read_filter_data_filter_measurement_pred() {
    // use an expr on table name to pick just the last row from o2
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 400)
        .add_expr(col("_measurement").eq(lit("o2")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Only expect other_temp in this location
    let expected_results = vec![
        "Series tags={_measurement=o2, state=CA, _field=temp}\n  FloatPoints timestamps: [300], values: [79.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_non_existent_column() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_non_existent_column_with_delete() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(TwoMeasurementsWithDelete {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=cpu, region=west, _field=user}\n  FloatPoints timestamps: [100, 150], values: [23.2, 21.0]",
        "Series tags={_measurement=disk, region=east, _field=bytes}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns_with_delete() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=cpu, region=west, _field=user}\n  FloatPoints timestamps: [100], values: [23.2]",
        "Series tags={_measurement=disk, region=east, _field=bytes}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurementsWithDelete {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns_with_delete_all() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Only table disk has no deleted data
    let expected_results = vec![
    "Series tags={_measurement=disk, region=east, _field=bytes}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurementsWithDeleteAll {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_good_and_non_existent_columns() {
    // predicate with both a column that does and does not appear
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA")))
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(
        TwoMeasurements {},
        predicate.clone(),
        expected_results.clone(),
    )
    .await;
    run_read_filter_test_case(
        TwoMeasurementsWithDelete {},
        predicate.clone(),
        expected_results.clone(),
    )
    .await;
    run_read_filter_test_case(TwoMeasurementsWithDeleteAll {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_match() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        // will match CA state
        .build_regex_match_expr("state", "C.*")
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200], values: [90.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_match_with_delete() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        // will match CA state
        .build_regex_match_expr("state", "C.*")
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // the selected row was soft deleted
    let expected_results = vec![];
    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate,
        expected_results,
    )
    .await;

    // Different predicate to have data returned
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 400)
        // will match CA state
        .build_regex_match_expr("state", "C.*")
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [350], values: [90.0]",
    ];
    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate.clone(),
        expected_results,
    )
    .await;

    // Try same predicate but on delete_all data
    let expected_results = vec![];
    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDeleteAll {},
        predicate,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_not_match() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        // will filter out any rows with a state that matches "CA"
        .build_regex_not_match_expr("state", "C.*")
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [72.4]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [250], values: [51.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_regex_escape() {
    let predicate = PredicateBuilder::default()
        // Came from InfluxQL like `SELECT value FROM db0.rp0.status_code WHERE url =~ /https\:\/\/influxdb\.com/`,
        .build_regex_match_expr("url", r#"https\://influxdb\.com"#)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // expect one series with influxdb.com
    let expected_results = vec![
        "Series tags={_measurement=status_code, url=https://influxdb.com, _field=value}\n  FloatPoints timestamps: [1527018816000000000], values: [418.0]",
    ];

    run_read_filter_test_case(MeasurementStatusCode {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_not_match_regex_escape() {
    let predicate = PredicateBuilder::default()
        // Came from InfluxQL like `SELECT value FROM db0.rp0.status_code WHERE url !~ /https\:\/\/influxdb\.com/`,
        .build_regex_not_match_expr("url", r#"https\://influxdb\.com"#)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // expect one series with influxdb.com
    let expected_results = vec![
        "Series tags={_measurement=status_code, url=http://www.example.com, _field=value}\n  FloatPoints timestamps: [1527018806000000000], values: [404.0]",
    ];

    run_read_filter_test_case(MeasurementStatusCode {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_unsupported_in_scan() {
    test_helpers::maybe_start_logging();

    // These predicates can't be pushed down into chunks, but they can
    // be evaluated by the general purpose DataFusion plan

    // (STATE = 'CA') OR (READING > 0)
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("CA")).or(col("reading").gt(lit(0))))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Note these results include data from both o2 and h2o
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200, 350], values: [90.0, 90.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_unsupported_in_scan_with_delete() {
    test_helpers::maybe_start_logging();

    // These predicates can't be pushed down into chunks, but they can
    // be evaluated by the general purpose DataFusion plan

    // (STATE = 'CA') OR (READING > 0)
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("CA")).or(col("reading").gt(lit(0))))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Note these results include data from both o2 and h2o
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [350], values: [90.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate.clone(),
        expected_results,
    )
    .await;

    // With delete all from h2o, no rows from h2p should be returned
    let expected_results = vec![
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=reading}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];
    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDeleteAll {},
        predicate,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_plan_order() {
    test_helpers::maybe_start_logging();
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=CA, _field=temp}\n  FloatPoints timestamps: [250], values: [70.3]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=other}\n  FloatPoints timestamps: [250], values: [5.0]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [70.5]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, zz_tag=A, _field=temp}\n  FloatPoints timestamps: [1000], values: [70.4]",
        "Series tags={_measurement=h2o, city=Kingston, state=MA, zz_tag=A, _field=temp}\n  FloatPoints timestamps: [800], values: [70.1]",
        "Series tags={_measurement=h2o, city=Kingston, state=MA, zz_tag=B, _field=temp}\n  FloatPoints timestamps: [100], values: [70.2]",
    ];

    run_read_filter_test_case(
        MeasurementsSortableTags {},
        InfluxRpcPredicate::default(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_plan_order_with_delete() {
    test_helpers::maybe_start_logging();
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=other}\n  FloatPoints timestamps: [250], values: [5.0]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [250], values: [70.5]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, zz_tag=A, _field=temp}\n  FloatPoints timestamps: [1000], values: [70.4]",
        "Series tags={_measurement=h2o, city=Kingston, state=MA, zz_tag=A, _field=temp}\n  FloatPoints timestamps: [800], values: [70.1]",
        "Series tags={_measurement=h2o, city=Kingston, state=MA, zz_tag=B, _field=temp}\n  FloatPoints timestamps: [100], values: [70.2]",
    ];

    run_read_filter_test_case(
        MeasurementsSortableTagsWithDelete {},
        InfluxRpcPredicate::default(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_filter_on_value() {
    test_helpers::maybe_start_logging();

    let predicate = PredicateBuilder::default()
        .add_expr(col("_value").eq(lit(1.77)))
        .add_expr(col("_field").eq(lit("load4")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=system, host=host.local, _field=load4}\n  FloatPoints timestamps: [1527018806000000000, 1527018826000000000], values: [1.77, 1.77]",
    ];

    run_read_filter_test_case(MeasurementsForDefect2845 {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_on_field() {
    test_helpers::maybe_start_logging();

    // Predicate should pick 'temp' field from h2o
    // (_field = 'temp')
    let p1 = col("_field").eq(lit("temp"));
    let predicate = PredicateBuilder::default().add_expr(p1).build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [50, 100000], values: [70.4, 70.4]",
        "Series tags={_measurement=o2, state=CA, _field=temp}\n  FloatPoints timestamps: [300], values: [79.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [50], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_on_field_single_measurement() {
    test_helpers::maybe_start_logging();

    // Predicate should pick 'temp' field from h2o
    // (_field = 'temp' AND _measurement = 'h2o')
    let p1 = col("_field")
        .eq(lit("temp"))
        .and(col("_measurement").eq(lit("h2o")));
    let predicate = PredicateBuilder::default().add_expr(p1).build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [50, 100000], values: [70.4, 70.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_on_field_multi_measurement() {
    test_helpers::maybe_start_logging();

    // Predicate should pick 'temp' field from h2o and 'other_temp' from o2
    //
    // (_field = 'other_temp' AND _measurement = 'h2o') OR (_field = 'temp' AND _measurement = 'o2')
    let p1 = col("_field")
        .eq(lit("other_temp"))
        .and(col("_measurement").eq(lit("h2o")));
    let p2 = col("_field")
        .eq(lit("temp"))
        .and(col("_measurement").eq(lit("o2")));
    let predicate = PredicateBuilder::default().add_expr(p1.or(p2)).build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // SHOULD NOT contain temp from h2o
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=CA, _field=other_temp}\n  FloatPoints timestamps: [350], values: [72.4]",
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=other_temp}\n  FloatPoints timestamps: [250], values: [70.4]",
        // This series should not be here (_field = temp)
        // WRONG ANSWER, tracked by: https://github.com/influxdata/influxdb_iox/issues/3428
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [50, 100000], values: [70.4, 70.4]",
        "Series tags={_measurement=o2, state=CA, _field=temp}\n  FloatPoints timestamps: [300], values: [79.0]",
        "Series tags={_measurement=o2, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [50], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}
