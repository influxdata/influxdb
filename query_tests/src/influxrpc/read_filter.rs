//! Tests for the Influx gRPC queries
use std::sync::Arc;

#[cfg(test)]
use crate::scenarios::{
    DbScenario, DbSetup, EndToEndTest, StringFieldWithNumericValue, TwoMeasurements,
    TwoMeasurementsManyFields,
};
use crate::{
    db::AbstractDb,
    influxrpc::util::run_series_set_plan_maybe_error,
    scenarios::{
        MeasurementStatusCode, MeasurementsForDefect2845, MeasurementsSortableTags, PeriodsInNames,
        TwoMeasurementsMultiSeries, TwoMeasurementsMultiTagValue,
    },
};
use datafusion::{
    prelude::{col, lit},
    scalar::ScalarValue,
};
use datafusion_util::AsExpr;
use iox_query::frontend::influxrpc::InfluxRpcPlanner;
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::Predicate;
use test_helpers::assert_contains;

use super::util::make_empty_tag_ref_expr;

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
    let ctx = db.new_query_context(None);
    let planner = InfluxRpcPlanner::new(ctx.child_ctx("planner"));

    let plan = planner
        .read_filter(db.as_query_namespace_arc(), predicate)
        .await
        .map_err(|e| e.to_string())?;

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
    "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [70.4, 72.4]",
    "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200, 350], values: [90.0, 90.0]",
    "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
    "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
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
    let predicate = Predicate::new()
        // should not return the 350 row as predicate is
        // range.start <= ts < range.end
        .with_range(349, 350);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_inclusive_predicate() {
    let predicate = Predicate::new()
        // should return  350 row!
        .with_range(350, 351);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [350], values: [90.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_exact_predicate() {
    let predicate = Predicate::new()
        // should return  250 rows!
        .with_range(250, 251);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [72.4]",
        "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [51.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_tag_predicate() {
    let predicate = Predicate::new()
        // region = region
        .with_expr(col("region").eq(col("region")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // expect both series to be returned
    let expected_results = vec![
        "Series tags={_field=user, _measurement=cpu, region=west}\n  FloatPoints timestamps: [100, 150], values: [23.2, 21.0]",
        "Series tags={_field=bytes, _measurement=disk, region=east}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_invalid_predicate() {
    let v = ScalarValue::Binary(Some(vec![]));
    let predicate = Predicate::new()
        // region > <binary> (region is a tag(string) column, so this predicate is invalid)
        .with_expr(col("region").gt(lit(v)));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_error = "Dictionary(Int32, Utf8) > Binary' can't be evaluated because there isn't a common type to coerce the types to";

    run_read_filter_error_case(TwoMeasurements {}, predicate, expected_error).await;
}

#[tokio::test]
async fn test_read_filter_invalid_predicate_case() {
    let v = ScalarValue::Binary(Some(vec![]));
    let predicate = Predicate::new()
        // https://github.com/influxdata/influxdb_iox/issues/3635
        // model what happens when a field is treated like a tag
        // CASE WHEN system" IS NULL THEN '' ELSE system END = binary;
        .with_expr(make_empty_tag_ref_expr("system").eq(lit(v)));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_error = "gRPC planner got error creating predicates: Error during planning: 'Utf8 = Binary' can't be evaluated because there isn't a common type to coerce the types to";

    run_read_filter_error_case(TwoMeasurements {}, predicate, expected_error).await;
}

#[tokio::test]
async fn test_read_filter_unknown_column_in_predicate() {
    let predicate = Predicate::new()
        // mystery_region and bar are not real columns, so this predicate is
        // invalid but IOx should be able to handle it (and produce no results)
        .with_expr(
            col("baz").eq(lit(4i32)).or(col("bar")
                .eq(lit("baz"))
                .and(col("mystery_region").gt(lit(5i32)))),
        );

    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_field_as_tag() {
    // Columns in the RPC predicate must be treated as tags:
    // https://github.com/influxdata/idpe/issues/16238
    let predicate = Predicate::new().with_expr(make_empty_tag_ref_expr("fld").eq(lit("200")));
    let predicate = InfluxRpcPredicate::new(None, predicate);
    // fld exists in the table, but only as a field, not a tag, so no data should be returned.
    let expected_results = vec![];
    run_read_filter_test_case(StringFieldWithNumericValue {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_field_as_tag_coerce_number() {
    // Same as above except for the predicate compares to an integer literal.
    let predicate = Predicate::new().with_expr(make_empty_tag_ref_expr("fld").eq(lit(200)));
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_results = vec![];
    run_read_filter_test_case(StringFieldWithNumericValue {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_filter() {
    // filter out one row in h20
    let predicate = Predicate::default()
        .with_range(200, 300)
        .with_expr(col("state").eq(lit("CA"))); // state=CA
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200], values: [90.0]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeries {},
        predicate,
        expected_results.clone(),
    )
    .await;

    // Same results via a != predicate.
    let predicate = Predicate::default()
        .with_range(200, 300)
        .with_expr(col("state").not_eq(lit("MA"))); // state=CA
    let predicate = InfluxRpcPredicate::new(None, predicate);

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_filter_fields() {
    // filter out one row in h20
    let predicate = Predicate::default()
        .with_field_columns(vec!["other_temp"])
        .with_expr(col("state").eq(lit("CA"))); // state=CA

    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Only expect other_temp in this location
    let expected_results = vec![
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [350], values: [72.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_filter_measurement_pred() {
    // use an expr on table name to pick just the last row from o2
    let predicate = Predicate::default()
        .with_range(200, 400)
        .with_expr(col("_measurement").eq(lit("o2")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Only expect other_temp in this location
    let expected_results = vec![
        "Series tags={_field=temp, _measurement=o2, state=CA}\n  FloatPoints timestamps: [300], values: [79.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_non_existent_column() {
    let predicate = Predicate::default().with_expr(col("tag_not_in_h20").eq(lit("foo")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns() {
    // predicate with no columns,
    let predicate = Predicate::default().with_expr(lit("foo").eq(lit("foo")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=user, _measurement=cpu, region=west}\n  FloatPoints timestamps: [100, 150], values: [23.2, 21.0]",
        "Series tags={_field=bytes, _measurement=disk, region=east}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_match() {
    let predicate = Predicate::default()
        .with_range(200, 300)
        // will match CA state
        .with_regex_match_expr("state", "C.*");
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200], values: [90.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_match_on_field() {
    let predicate = Predicate::default().with_regex_match_expr("_field", "temp");
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Should see results for temp and other_temp (but not reading)
    let expected_results = vec![
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [350], values: [72.4]",
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [70.4]",
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [50, 100000], values: [70.4, 70.4]",
        "Series tags={_field=temp, _measurement=o2, state=CA}\n  FloatPoints timestamps: [300], values: [79.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [50], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_not_match() {
    let predicate = Predicate::default()
        .with_range(200, 300)
        // will filter out any rows with a state that matches "CA"
        .with_regex_not_match_expr("state", "C.*");
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [72.4]",
        "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [51.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_regex_escape() {
    let predicate = Predicate::default()
        // Came from InfluxQL like `SELECT value FROM db0.rp0.status_code WHERE url =~ /https\:\/\/influxdb\.com/`,
        .with_regex_match_expr("url", r#"https\://influxdb\.com"#);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // expect one series with influxdb.com
    let expected_results = vec![
        "Series tags={_field=value, _measurement=status_code, url=https://influxdb.com}\n  FloatPoints timestamps: [1527018816000000000], values: [418.0]",
    ];

    run_read_filter_test_case(MeasurementStatusCode {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_not_match_regex_escape() {
    let predicate = Predicate::default()
        // Came from InfluxQL like `SELECT value FROM db0.rp0.status_code WHERE url !~ /https\:\/\/influxdb\.com/`,
        .with_regex_not_match_expr("url", r#"https\://influxdb\.com"#);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // expect one series with influxdb.com
    let expected_results = vec![
        "Series tags={_field=value, _measurement=status_code, url=http://www.example.com}\n  FloatPoints timestamps: [1527018806000000000], values: [404.0]",
    ];

    run_read_filter_test_case(MeasurementStatusCode {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_unsupported_in_scan() {
    test_helpers::maybe_start_logging();

    // These predicates can't be pushed down into chunks, but they can
    // be evaluated by the general purpose DataFusion plan

    // (STATE = 'CA') OR (CITY = 'Boston')
    let predicate = Predicate::default()
        .with_expr(col("state").eq(lit("CA")).or(col("city").eq(lit("Boston"))));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Note these results include data from both o2 and h2o
    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [100], values: [70.4]",
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200], values: [90.0]",
        "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100], values: [50.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100], values: [50.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiTagValue {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_plan_order() {
    test_helpers::maybe_start_logging();
    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [250], values: [70.3]",
        "Series tags={_field=other, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [5.0]",
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [70.5]",
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA, zz_tag=A}\n  FloatPoints timestamps: [1000], values: [70.4]",
        "Series tags={_field=temp, _measurement=h2o, city=Kingston, state=MA, zz_tag=A}\n  FloatPoints timestamps: [800], values: [70.1]",
        "Series tags={_field=temp, _measurement=h2o, city=Kingston, state=MA, zz_tag=B}\n  FloatPoints timestamps: [100], values: [70.2]",
    ];

    run_read_filter_test_case(
        MeasurementsSortableTags {},
        InfluxRpcPredicate::default(),
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_filter_on_value() {
    test_helpers::maybe_start_logging();

    let predicate = Predicate::default()
        .with_expr(col("_value").eq(lit(1.77)))
        .with_expr(col("_field").eq(lit("load4")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=load4, _measurement=system, host=host.local}\n  FloatPoints timestamps: [1527018806000000000, 1527018826000000000], values: [1.77, 1.77]",
    ];

    run_read_filter_test_case(MeasurementsForDefect2845 {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_on_field() {
    test_helpers::maybe_start_logging();

    // Predicate should pick 'temp' field from h2o
    // (_field = 'temp')
    let p1 = col("_field").eq(lit("temp"));
    let predicate = Predicate::default().with_expr(p1);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [50, 100000], values: [70.4, 70.4]",
        "Series tags={_field=temp, _measurement=o2, state=CA}\n  FloatPoints timestamps: [300], values: [79.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [50], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_on_not_field() {
    test_helpers::maybe_start_logging();

    // Predicate should pick up all fields other than 'temp' from h2o
    // (_field != 'temp')
    let p1 = col("_field").not_eq(lit("temp"));
    let predicate = Predicate::default().with_expr(p1);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [350], values: [72.4]",
        "Series tags={_field=moisture, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [100000], values: [43.0]",
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [70.4]",
        "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [50], values: [51.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_unsupported_field_predicate() {
    test_helpers::maybe_start_logging();

    // Can not evaluate the following predicate as it refers to both
    // _field and the values in the "tag" column, so we can't figure
    // out which columns ("_field" prediate) at planning time -- we
    // have to do it at runtime somehow
    //
    // https://github.com/influxdata/influxdb_iox/issues/5310

    // (_field != 'temp') OR (city = "Boston")
    let p1 = col("_field")
        .not_eq(lit("temp"))
        .or(col("city").eq(lit("Boston")));
    let predicate = Predicate::default().with_expr(p1);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_error = "Unsupported _field predicate";

    run_read_filter_error_case(TwoMeasurementsManyFields {}, predicate, expected_error).await;
}

#[tokio::test]
async fn test_read_filter_on_field_single_measurement() {
    test_helpers::maybe_start_logging();

    // Predicate should pick 'temp' field from h2o
    // (_field = 'temp' AND _measurement = 'h2o')
    let p1 = col("_field")
        .eq(lit("temp"))
        .and(col("_measurement").eq(lit("h2o")));
    let predicate = Predicate::default().with_expr(p1);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [50, 100000], values: [70.4, 70.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_multi_negation() {
    // reproducer for https://github.com/influxdata/influxdb_iox/issues/4800
    test_helpers::maybe_start_logging();

    let host = make_empty_tag_ref_expr("host");
    let p1 = host.clone().eq(lit("server01")).or(host.eq(lit("")));
    let predicate = Predicate::default().with_expr(p1);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_results = vec![
        "Series tags={_field=color, _measurement=attributes}\n  StringPoints timestamps: [8000], values: [\"blue\"]",
        "Series tags={_field=value, _measurement=cpu_load_short, host=server01}\n  FloatPoints timestamps: [1000], values: [27.99]",
        "Series tags={_field=value, _measurement=cpu_load_short, host=server01, region=us-east}\n  FloatPoints timestamps: [3000], values: [1234567.891011]",
        "Series tags={_field=value, _measurement=cpu_load_short, host=server01, region=us-west}\n  FloatPoints timestamps: [0, 4000], values: [0.64, 3e-6]",
        "Series tags={_field=active, _measurement=status}\n  BooleanPoints timestamps: [7000], values: [true]",
        "Series tags={_field=in, _measurement=swap, host=server01, name=disk0}\n  FloatPoints timestamps: [6000], values: [3.0]",
        "Series tags={_field=out, _measurement=swap, host=server01, name=disk0}\n  FloatPoints timestamps: [6000], values: [4.0]",
    ];

    run_read_filter_test_case(EndToEndTest {}, predicate, expected_results).await;
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
    let predicate = Predicate::default().with_expr(p1.or(p2));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // SHOULD NOT contain temp from h2o
    let expected_results = vec![
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [350], values: [72.4]",
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [70.4]",
        "Series tags={_field=temp, _measurement=o2, state=CA}\n  FloatPoints timestamps: [300], values: [79.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [50], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_with_periods() {
    test_helpers::maybe_start_logging();

    let predicate = Predicate::default().with_range(0, 1700000001000000000);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Should return both series
    let expected_results = vec![
        "Series tags={_field=field.one, _measurement=measurement.one, tag.one=value, tag.two=other}\n  FloatPoints timestamps: [1609459201000000001], values: [1.0]",
        "Series tags={_field=field.two, _measurement=measurement.one, tag.one=value, tag.two=other}\n  BooleanPoints timestamps: [1609459201000000001], values: [true]",
        "Series tags={_field=field.one, _measurement=measurement.one, tag.one=value2, tag.two=other2}\n  FloatPoints timestamps: [1609459201000000002], values: [1.0]",
        "Series tags={_field=field.two, _measurement=measurement.one, tag.one=value2, tag.two=other2}\n  BooleanPoints timestamps: [1609459201000000002], values: [false]",
    ];

    run_read_filter_test_case(PeriodsInNames {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_with_periods_predicates() {
    test_helpers::maybe_start_logging();

    let predicate = Predicate::default()
        .with_range(0, 1700000001000000000)
        // tag.one = "value"
        .with_expr("tag.one".as_expr().eq(lit("value")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // Should return both series
    let expected_results = vec![
        "Series tags={_field=field.one, _measurement=measurement.one, tag.one=value, tag.two=other}\n  FloatPoints timestamps: [1609459201000000001], values: [1.0]",
        "Series tags={_field=field.two, _measurement=measurement.one, tag.one=value, tag.two=other}\n  BooleanPoints timestamps: [1609459201000000001], values: [true]",
    ];

    run_read_filter_test_case(PeriodsInNames {}, predicate, expected_results).await;
}
