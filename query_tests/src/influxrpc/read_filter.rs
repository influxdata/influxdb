//! Tests for the Influx gRPC queries
use crate::influxrpc::util::run_series_set_plan;
#[cfg(test)]
use crate::scenarios::{
    util::all_scenarios_for_one_chunk, DbScenario, DbSetup, NoData, TwoMeasurements,
    TwoMeasurementsManyFields, TwoMeasurementsWithDelete, TwoMeasurementsWithDeleteAll,
};
use async_trait::async_trait;
use data_types::timestamp::TimestampRange;
use datafusion::logical_plan::{col, lit};
use predicate::{
    delete_predicate::DeletePredicate,
    predicate::{Predicate, PredicateBuilder, EMPTY_PREDICATE},
};
use query::frontend::influxrpc::InfluxRpcPlanner;

#[derive(Debug)]
pub struct TwoMeasurementsMultiSeries {}
#[async_trait]
impl DbSetup for TwoMeasurementsMultiSeries {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let mut lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100", // to row 2
            "h2o,state=MA,city=Boston temp=72.4 250", // to row 1
            "h2o,state=CA,city=LA temp=90.0 200",     // to row 0
            "h2o,state=CA,city=LA temp=90.0 350",     // to row 3
            "o2,state=MA,city=Boston temp=50.4,reading=50 100", // to row 5
            "o2,state=MA,city=Boston temp=53.4,reading=51 250", // to row 4
        ];

        // Swap around  data is not inserted in series order
        lp_lines.swap(0, 2);
        lp_lines.swap(4, 5);

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsMultiSeriesWithDelete {}
#[async_trait]
impl DbSetup for TwoMeasurementsMultiSeriesWithDelete {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let mut lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100", // to row 2
            "h2o,state=MA,city=Boston temp=72.4 250", // to row 1
            "h2o,state=CA,city=LA temp=90.0 200",     // to row 0
            "h2o,state=CA,city=LA temp=90.0 350",     // to row 3
            "o2,state=MA,city=Boston temp=50.4,reading=50 100", // to row 5
            "o2,state=MA,city=Boston temp=53.4,reading=51 250", // to row 4
        ];

        // Swap around  data is not inserted in series order
        lp_lines.swap(0, 2);
        lp_lines.swap(4, 5);

        // pred: delete from h20 where 120 <= time <= 250
        let delete_table_name = "h2o";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 120,
                end: 250,
            },
            exprs: vec![],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsMultiSeriesWithDeleteAll {}
#[async_trait]
impl DbSetup for TwoMeasurementsMultiSeriesWithDeleteAll {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let mut lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100", // to row 2
            "h2o,state=MA,city=Boston temp=72.4 250", // to row 1
            "h2o,state=CA,city=LA temp=90.0 200",     // to row 0
            "h2o,state=CA,city=LA temp=90.0 350",     // to row 3
            "o2,state=MA,city=Boston temp=50.4,reading=50 100", // to row 5
            "o2,state=MA,city=Boston temp=53.4,reading=51 250", // to row 4
        ];

        // Swap around  data is not inserted in series order
        lp_lines.swap(0, 2);
        lp_lines.swap(4, 5);

        // Delete all data form h2o
        // pred: delete from h20 where 100 <= time <= 360
        let delete_table_name = "h2o";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 100,
                end: 360,
            },
            exprs: vec![],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

/// runs read_filter(predicate) and compares it to the expected
/// output
async fn run_read_filter_test_case<D>(
    db_setup: D,
    predicate: Predicate,
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
        let planner = InfluxRpcPlanner::new();

        let plan = planner
            .read_filter(db.as_ref(), predicate.clone())
            .expect("built plan successfully");

        let ctx = db.executor().new_context(query::exec::ExecutorType::Query);
        let string_results = run_series_set_plan(&ctx, plan).await;

        assert_eq!(
            expected_results, string_results,
            "Error in  scenario '{}'\n\nexpected:\n{:#?}\n\nactual:\n{:#?}",
            scenario_name, expected_results, string_results
        );
    }
}

#[tokio::test]
async fn test_read_filter_no_data_no_pred() {
    let predicate = EMPTY_PREDICATE;
    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(NoData {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_no_pred() {
    let predicate = EMPTY_PREDICATE;
    let expected_results = vec![
    "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [70.4, 72.4]",
    "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200, 350], values: [90.0, 90.0]",
    "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
    "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_no_pred_with_delete() {
    let predicate = EMPTY_PREDICATE;
    let expected_results = vec![
    "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [100], values: [70.4]",
    "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [350], values: [90.0]",
    "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
    "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDelete {},
        predicate,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_filter_data_no_pred_with_delete_all() {
    let predicate = EMPTY_PREDICATE;
    // nothing from h2o table because all rows were deleted
    let expected_results = vec![
    "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
    "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(
        TwoMeasurementsMultiSeriesWithDeleteAll {},
        predicate,
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
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").not_eq(lit("MA"))) // state=CA
        .build();

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_filter_fields() {
    // filter out one row in h20
    let predicate = PredicateBuilder::default()
        .field_columns(vec!["other_temp"])
        .add_expr(col("state").eq(lit("CA"))) // state=CA
        .build();

    // Only expect other_temp in this location
    let expected_results = vec![
        "Series tags={_field=other_temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [350], values: [72.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_filter_measurement_pred() {
    // use an expr on table name to pick just the last row from o2
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 400)
        .add_expr(col("_measurement").eq(lit("o2")))
        .build();

    // Only expect other_temp in this location
    let expected_results = vec![
        "Series tags={_field=temp, _measurement=o2, state=CA}\n  FloatPoints timestamps: [300], values: [79.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsManyFields {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_non_existent_column() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_non_existent_column_with_delete() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case(TwoMeasurementsWithDelete {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();

    let expected_results = vec![
        "Series tags={_field=user, _measurement=cpu, region=west}\n  FloatPoints timestamps: [100, 150], values: [23.2, 21.0]",
        "Series tags={_field=bytes, _measurement=disk, region=east}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurements {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns_with_delete() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();

    let expected_results = vec![
        "Series tags={_field=user, _measurement=cpu, region=west}\n  FloatPoints timestamps: [100], values: [23.2]",
        "Series tags={_field=bytes, _measurement=disk, region=east}\n  IntegerPoints timestamps: [200], values: [99]",
    ];

    run_read_filter_test_case(TwoMeasurementsWithDelete {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns_with_delete_all() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();

    // Only table disk has no deleted data
    let expected_results = vec![
    "Series tags={_field=bytes, _measurement=disk, region=east}\n  IntegerPoints timestamps: [200], values: [99]",
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

    let expected_results = vec![
    "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200], values: [90.0]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_using_regex_not_match() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        // will filter out any rows with a state that matches "CA"
        .build_regex_not_match_expr("state", "C.*")
        .build();

    let expected_results = vec![
    "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [72.4]",
    "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [51.0]",
    "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[tokio::test]
async fn test_read_filter_data_pred_unsupported_in_scan() {
    test_helpers::maybe_start_logging();

    // These predicates can't be pushed down into chunks, but they can
    // be evaluated by the general purpose DataFusion plan
    // https://github.com/influxdata/influxdb_iox/issues/883
    // (STATE = 'CA') OR (READING > 0)
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("CA")).or(col("reading").gt(lit(0))))
        .build();

    // Note these results are incorrect (they do not include data from h2o where
    // state = CA)
    let expected_results = vec![
        "Series tags={_field=reading, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.0, 51.0]",
        "Series tags={_field=temp, _measurement=o2, city=Boston, state=MA}\n  FloatPoints timestamps: [100, 250], values: [50.4, 53.4]",
    ];

    run_read_filter_test_case(TwoMeasurementsMultiSeries {}, predicate, expected_results).await;
}

#[derive(Debug)]
pub struct MeasurementsSortableTags {}
#[async_trait]
impl DbSetup for MeasurementsSortableTags {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
            "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
            "h2o,state=CA,city=Boston temp=70.3 250",
            "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
            "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

#[tokio::test]
async fn test_read_filter_data_plan_order() {
    test_helpers::maybe_start_logging();
    let predicate = Predicate::default();
    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=CA}\n  FloatPoints timestamps: [250], values: [70.3]",
        "Series tags={_field=other, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [5.0]",
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [250], values: [70.5]",
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA, zz_tag=A}\n  FloatPoints timestamps: [1000], values: [70.4]",
        "Series tags={_field=temp, _measurement=h2o, city=Kingston, state=MA, zz_tag=A}\n  FloatPoints timestamps: [800], values: [70.1]",
        "Series tags={_field=temp, _measurement=h2o, city=Kingston, state=MA, zz_tag=B}\n  FloatPoints timestamps: [100], values: [70.2]",
    ];

    run_read_filter_test_case(MeasurementsSortableTags {}, predicate, expected_results).await;
}
