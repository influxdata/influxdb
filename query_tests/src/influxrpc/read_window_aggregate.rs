//! Tests for the Influx gRPC queries
use crate::{influxrpc::util::run_series_set_plan, scenarios::*};
use datafusion::prelude::*;
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::PredicateBuilder;
use query::{frontend::influxrpc::InfluxRpcPlanner, Aggregate, WindowDuration};

/// runs read_window_aggregate(predicate) and compares it to the expected
/// output
async fn run_read_window_aggregate_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    agg: Aggregate,
    every: WindowDuration,
    offset: WindowDuration,
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
        let planner = InfluxRpcPlanner::default();
        let ctx = db.new_query_context(None);

        let plan = planner
            .read_window_aggregate(
                db.as_query_database(),
                predicate.clone(),
                agg,
                every,
                offset,
            )
            .await
            .expect("built plan successfully");

        let string_results = run_series_set_plan(&ctx, plan).await;

        assert_eq!(
            expected_results, string_results,
            "Error in  scenario '{}'\n\nexpected:\n{:#?}\n\nactual:\n{:#?}\n",
            scenario_name, expected_results, string_results
        );
    }
}

#[tokio::test]
async fn test_read_window_aggregate_nanoseconds() {
    let predicate = PredicateBuilder::default()
        // city=Boston or city=LA
        .add_expr(col("city").eq(lit("Boston")).or(col("city").eq(lit("LA"))))
        .timestamp_range(100, 450)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Mean;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);

    // note the name of the field is "temp" even though it is the average
    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [200, 400, 600], values: [70.0, 71.5, 73.0]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200, 400, 600], values: [90.0, 91.5, 93.0]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForWindowAggregate {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_window_aggregate_nanoseconds_measurement_pred() {
    let predicate = PredicateBuilder::default()
        // city=Cambridge OR (_measurement != 'other' AND city = LA)
        .add_expr(
            col("city").eq(lit("Boston")).or(col("_measurement")
                .not_eq(lit("other"))
                .and(col("city").eq(lit("LA")))),
        )
        .timestamp_range(100, 450)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Mean;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  FloatPoints timestamps: [200, 400, 600], values: [70.0, 71.5, 73.0]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  FloatPoints timestamps: [200, 400, 600], values: [90.0, 91.5, 93.0]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForWindowAggregate {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_window_aggregate_nanoseconds_measurement_count() {
    // Expect that the type of `Count` is Integer
    let predicate = PredicateBuilder::default()
        .timestamp_range(100, 450)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Count;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);

    let expected_results = vec![
        "Series tags={_measurement=h2o, city=Boston, state=MA, _field=temp}\n  IntegerPoints timestamps: [200, 400, 600], values: [1, 2, 1]",
        "Series tags={_measurement=h2o, city=Cambridge, state=MA, _field=temp}\n  IntegerPoints timestamps: [200, 400, 600], values: [1, 2, 1]",
        "Series tags={_measurement=h2o, city=LA, state=CA, _field=temp}\n  IntegerPoints timestamps: [200, 400, 600], values: [1, 2, 1]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForWindowAggregate {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

// See https://github.com/influxdata/influxdb_iox/issues/2697
#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_min_defect_2697() {
    let predicate = PredicateBuilder::default()
        // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
        .timestamp_range(1609459201000000001, 1609459201000000031)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Min;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // Because the windowed aggregate is using a selector aggregate (one of MIN,
    // MAX, FIRST, LAST) we need to run a plan that brings along the timestamps
    // for the chosen aggregate in the window.
    let expected_results = vec![
        "Series tags={_measurement=mm, section=1a, _field=bar}\n  FloatPoints timestamps: [1609459201000000011], values: [5.0]",
        "Series tags={_measurement=mm, section=1a, _field=foo}\n  FloatPoints timestamps: [1609459201000000001, 1609459201000000024], values: [1.0, 11.24]",
        "Series tags={_measurement=mm, section=2b, _field=bar}\n  FloatPoints timestamps: [1609459201000000009, 1609459201000000015, 1609459201000000022], values: [4.0, 6.0, 1.2]",
        "Series tags={_measurement=mm, section=2b, _field=foo}\n  FloatPoints timestamps: [1609459201000000002], values: [2.0]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForDefect2697 {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_min_defect_2697_with_delete() {
    let predicate = PredicateBuilder::default()
        // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
        .timestamp_range(1609459201000000001, 1609459201000000031)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Min;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // one row deleted
    let expected_results = vec![
        "Series tags={_measurement=mm, section=1a, _field=bar}\n  FloatPoints timestamps: [1609459201000000011], values: [5.0]",
        "Series tags={_measurement=mm, section=1a, _field=foo}\n  FloatPoints timestamps: [1609459201000000001, 1609459201000000024], values: [1.0, 11.24]",
        "Series tags={_measurement=mm, section=2b, _field=bar}\n  FloatPoints timestamps: [1609459201000000009, 1609459201000000015], values: [4.0, 6.0]",
        "Series tags={_measurement=mm, section=2b, _field=foo}\n  FloatPoints timestamps: [1609459201000000002], values: [2.0]",
    ];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2697WithDelete {},
        predicate.clone(),
        agg,
        every,
        offset,
        expected_results,
    )
    .await;

    // all rows deleted
    let expected_results = vec![];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2697WithDeleteAll {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

// See https://github.com/influxdata/influxdb_iox/issues/2697
#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_sum_defect_2697() {
    let predicate = PredicateBuilder::default()
        // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
        .timestamp_range(1609459201000000001, 1609459201000000031)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Sum;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAD).
    // For each distinct series the window defines the `time` column
    let expected_results = vec![
        "Series tags={_measurement=mm, section=1a, _field=bar}\n  FloatPoints timestamps: [1609459201000000020], values: [5.0]",
        "Series tags={_measurement=mm, section=1a, _field=foo}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000030], values: [4.0, 11.24]",
        "Series tags={_measurement=mm, section=2b, _field=bar}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000020, 1609459201000000030], values: [4.0, 6.0, 1.2]",
        "Series tags={_measurement=mm, section=2b, _field=foo}\n  FloatPoints timestamps: [1609459201000000010], values: [2.0]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForDefect2697 {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

// See issue: https://github.com/influxdata/influxdb_iox/issues/2845
//
// Adds coverage to window_aggregate plan for filtering on _field.
#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_filter_on_field() {
    let predicate = PredicateBuilder::default()
        // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
        .timestamp_range(1609459201000000001, 1609459201000000031)
        .add_expr(col("_field").eq(lit("foo")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Sum;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAD).
    // For each distinct series the window defines the `time` column
    let expected_results = vec![
        "Series tags={_measurement=mm, section=1a, _field=foo}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000030], values: [4.0, 11.24]",
        "Series tags={_measurement=mm, section=2b, _field=foo}\n  FloatPoints timestamps: [1609459201000000010], values: [2.0]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForDefect2697 {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_sum_defect_2697_with_delete() {
    let predicate = PredicateBuilder::default()
        // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
        .timestamp_range(1609459201000000001, 1609459201000000031)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Sum;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // one row deleted

    // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAD).
    // For each distinct series the window defines the `time` column
    let expected_results = vec![
        "Series tags={_measurement=mm, section=1a, _field=bar}\n  FloatPoints timestamps: [1609459201000000020], values: [5.0]",
        "Series tags={_measurement=mm, section=1a, _field=foo}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000030], values: [4.0, 11.24]",
        "Series tags={_measurement=mm, section=2b, _field=bar}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000020], values: [4.0, 6.0]",
        "Series tags={_measurement=mm, section=2b, _field=foo}\n  FloatPoints timestamps: [1609459201000000010], values: [2.0]",
    ];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2697WithDelete {},
        predicate.clone(),
        agg,
        every,
        offset,
        expected_results,
    )
    .await;

    // all rows deleted
    let expected_results = vec![];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2697WithDeleteAll {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

#[tokio::test]
async fn test_read_window_aggregate_overflow() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(1609459201000000001, 1609459201000000024)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let agg = Aggregate::Max;
    // Note the giant window (every=9223372036854775807)
    let every = WindowDuration::from_nanoseconds(i64::MAX);
    let offset = WindowDuration::from_nanoseconds(0);

    let expected_results = vec![
        "Series tags={_measurement=mm, _field=bar}\n  FloatPoints timestamps: [1609459201000000015], values: [6.0]",
        "Series tags={_measurement=mm, _field=foo}\n  FloatPoints timestamps: [1609459201000000005], values: [3.0]",
    ];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2890 {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}
