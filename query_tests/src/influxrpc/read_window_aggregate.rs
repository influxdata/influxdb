//! Tests for the Influx gRPC queries
use crate::{
    influxrpc::util::run_series_set_plan,
    scenarios::{util::all_scenarios_for_one_chunk, *},
};

use data_types::timestamp::TimestampRange;
use server::{db::test_helpers::write_lp, utils::make_db};

use async_trait::async_trait;
use datafusion::prelude::*;
use predicate::{
    delete_predicate::DeletePredicate,
    predicate::{Predicate, PredicateBuilder},
};
use query::{
    frontend::influxrpc::InfluxRpcPlanner,
    group_by::{Aggregate, WindowDuration},
};

/// runs read_window_aggregate(predicate) and compares it to the expected
/// output
async fn run_read_window_aggregate_test_case<D>(
    db_setup: D,
    predicate: Predicate,
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
        let planner = InfluxRpcPlanner::new();
        let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

        let plan = planner
            .read_window_aggregate(
                db.as_ref(),
                predicate.clone(),
                agg,
                every.clone(),
                offset.clone(),
            )
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
async fn test_read_window_aggregate_no_data_no_pred() {
    let predicate = Predicate::default();
    let agg = Aggregate::Mean;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);
    let expected_results = vec![] as Vec<&str>;

    run_read_window_aggregate_test_case(NoData {}, predicate, agg, every, offset, expected_results)
        .await;
}

struct MeasurementForWindowAggregate {}
#[async_trait]
impl DbSetup for MeasurementForWindowAggregate {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.0 100",
            "h2o,state=MA,city=Boston temp=71.0 200",
            "h2o,state=MA,city=Boston temp=72.0 300",
            "h2o,state=MA,city=Boston temp=73.0 400",
            "h2o,state=MA,city=Boston temp=74.0 500",
            "h2o,state=MA,city=Cambridge temp=80.0 100",
            "h2o,state=MA,city=Cambridge temp=81.0 200",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge temp=82.0 300",
            "h2o,state=MA,city=Cambridge temp=83.0 400",
            "h2o,state=MA,city=Cambridge temp=84.0 500",
            "h2o,state=CA,city=LA temp=90.0 100",
            "h2o,state=CA,city=LA temp=91.0 200",
            "h2o,state=CA,city=LA temp=92.0 300",
            "h2o,state=CA,city=LA temp=93.0 400",
            "h2o,state=CA,city=LA temp=94.0 500",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// NGA todo: add delete DbSetup after all scenarios are done for 2 chunks

#[tokio::test]
async fn test_read_window_aggregate_nanoseconds() {
    let predicate = PredicateBuilder::default()
        // city=Boston or city=LA
        .add_expr(col("city").eq(lit("Boston")).or(col("city").eq(lit("LA"))))
        .timestamp_range(100, 450)
        .build();

    let agg = Aggregate::Mean;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);

    // note the name of the field is "temp" even though it is the average
    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [200, 400, 600], values: [70.0, 71.5, 73.0]",
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200, 400, 600], values: [90.0, 91.5, 93.0]",
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

    let agg = Aggregate::Mean;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [200, 400, 600], values: [70.0, 71.5, 73.0]",
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  FloatPoints timestamps: [200, 400, 600], values: [90.0, 91.5, 93.0]",
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

    let agg = Aggregate::Count;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);

    let expected_results = vec![
        "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  IntegerPoints timestamps: [200, 400, 600], values: [1, 2, 1]",
        "Series tags={_field=temp, _measurement=h2o, city=Cambridge, state=MA}\n  IntegerPoints timestamps: [200, 400, 600], values: [1, 2, 1]",
        "Series tags={_field=temp, _measurement=h2o, city=LA, state=CA}\n  IntegerPoints timestamps: [200, 400, 600], values: [1, 2, 1]",
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

struct MeasurementForWindowAggregateMonths {}
#[async_trait]
impl DbSetup for MeasurementForWindowAggregateMonths {
    async fn make(&self) -> Vec<DbScenario> {
        // Note the lines are written into 4 different partititions (as we are
        // partitioned by day, effectively)
        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.0 1583020800000000000", // 2020-03-01T00:00:00Z
            "h2o,state=MA,city=Boston temp=71.0 1583107920000000000", // 2020-03-02T00:12:00Z
            "h2o,state=MA,city=Boston temp=72.0 1585699200000000000", // 2020-04-01T00:00:00Z
            "h2o,state=MA,city=Boston temp=73.0 1585785600000000000", // 2020-04-02T00:00:00Z
        ];
        // partition keys are: ["2020-03-02T00", "2020-03-01T00", "2020-04-01T00",
        // "2020-04-02T00"]

        let db = make_db().await.db;
        let data = lp_lines.join("\n");
        write_lp(&db, &data).await;
        let scenario1 = DbScenario {
            scenario_name: "Data in 4 partitions, open chunks of mutable buffer".into(),
            db,
        };

        let db = make_db().await.db;
        let data = lp_lines.join("\n");
        write_lp(&db, &data).await;
        db.rollover_partition("h2o", "2020-03-01T00").await.unwrap();
        db.rollover_partition("h2o", "2020-03-02T00").await.unwrap();
        let scenario2 = DbScenario {
            scenario_name:
                "Data in 4 partitions, two open chunk and two closed chunks of mutable buffer"
                    .into(),
            db,
        };

        let db = make_db().await.db;
        let data = lp_lines.join("\n");
        write_lp(&db, &data).await;
        // roll over and load chunks into both RUB and OS
        rollover_and_load(&db, "2020-03-01T00", "h2o").await;
        rollover_and_load(&db, "2020-03-02T00", "h2o").await;
        rollover_and_load(&db, "2020-04-01T00", "h2o").await;
        rollover_and_load(&db, "2020-04-02T00", "h2o").await;
        let scenario3 = DbScenario {
            scenario_name: "Data in 4 partitions, 4 closed chunks in mutable buffer".into(),
            db,
        };

        // TODO: Add a scenario for OS only in #1342

        vec![scenario1, scenario2, scenario3]
    }
}

// NGA todo: add delete DbSetup

#[tokio::test]
async fn test_read_window_aggregate_months() {
    let predicate = PredicateBuilder::default().build();

    let agg = Aggregate::Mean;
    let every = WindowDuration::from_months(1, false);
    let offset = WindowDuration::from_months(0, false);

    // note the name of the field is "temp" even though it is the average
    let expected_results = vec![
    "Series tags={_field=temp, _measurement=h2o, city=Boston, state=MA}\n  FloatPoints timestamps: [1585699200000000000, 1588291200000000000], values: [70.5, 72.5]",
    ];

    run_read_window_aggregate_test_case(
        MeasurementForWindowAggregateMonths {},
        predicate,
        agg,
        every,
        offset,
        expected_results,
    )
    .await;
}

// Test data to validate fix for:
// https://github.com/influxdata/influxdb_iox/issues/2697
struct MeasurementForDefect2697 {}
#[async_trait]
impl DbSetup for MeasurementForDefect2697 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "mm,section=1a bar=5.0 1609459201000000011",
            "mm,section=1a bar=0.28 1609459201000000031",
            "mm,section=2b bar=4.0 1609459201000000009",
            "mm,section=2b bar=6.0 1609459201000000015",
            "mm,section=2b bar=1.2 1609459201000000022",
            "mm,section=1a foo=1.0 1609459201000000001",
            "mm,section=1a foo=3.0 1609459201000000005",
            "mm,section=1a foo=11.24 1609459201000000024",
            "mm,section=2b foo=2.0 1609459201000000002",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "mm", partition_key).await
    }
}

struct MeasurementForDefect2697WithDelete {}
#[async_trait]
impl DbSetup for MeasurementForDefect2697WithDelete {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "mm,section=1a bar=5.0 1609459201000000011",
            "mm,section=1a bar=0.28 1609459201000000031",
            "mm,section=2b bar=4.0 1609459201000000009",
            "mm,section=2b bar=6.0 1609459201000000015",
            "mm,section=2b bar=1.2 1609459201000000022",
            "mm,section=1a foo=1.0 1609459201000000001",
            "mm,section=1a foo=3.0 1609459201000000005",
            "mm,section=1a foo=11.24 1609459201000000024",
            "mm,section=2b foo=2.0 1609459201000000002",
        ];

        // pred: delete from mm where 1609459201000000022 <= time <= 1609459201000000022
        // 1 row of m0 with timestamp 1609459201000000022 (section=2b bar=1.2)
        let delete_table_name = "mm";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 1609459201000000022,
                end: 1609459201000000022,
            },
            exprs: vec![],
        };

        all_scenarios_for_one_chunk(vec![&pred], vec![], lp, delete_table_name, partition_key).await
    }
}

struct MeasurementForDefect2697WithDeleteAll {}
#[async_trait]
impl DbSetup for MeasurementForDefect2697WithDeleteAll {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "mm,section=1a bar=5.0 1609459201000000011",
            "mm,section=1a bar=0.28 1609459201000000031",
            "mm,section=2b bar=4.0 1609459201000000009",
            "mm,section=2b bar=6.0 1609459201000000015",
            "mm,section=2b bar=1.2 1609459201000000022",
            "mm,section=1a foo=1.0 1609459201000000001",
            "mm,section=1a foo=3.0 1609459201000000005",
            "mm,section=1a foo=11.24 1609459201000000024",
            "mm,section=2b foo=2.0 1609459201000000002",
        ];

        // pred: delete from mm where 1 <= time <= 1609459201000000031
        let delete_table_name = "mm";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 1,
                end: 1609459201000000031,
            },
            exprs: vec![],
        };

        all_scenarios_for_one_chunk(vec![&pred], vec![], lp, delete_table_name, partition_key).await
    }
}

// See https://github.com/influxdata/influxdb_iox/issues/2697
#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_min_defect_2697() {
    let predicate = PredicateBuilder::default()
        // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
        .timestamp_range(1609459201000000001, 1609459201000000031)
        .build();

    let agg = Aggregate::Min;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // Because the windowed aggregate is using a selector aggregate (one of MIN,
    // MAX, FIRST, LAST) we need to run a plan that brings along the timestamps
    // for the chosen aggregate in the window.
    let expected_results = vec![
        "Series tags={_field=bar, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000011], values: [5.0]",
        "Series tags={_field=foo, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000001, 1609459201000000024], values: [1.0, 11.24]",
        "Series tags={_field=bar, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000009, 1609459201000000015, 1609459201000000022], values: [4.0, 6.0, 1.2]",
        "Series tags={_field=foo, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000002], values: [2.0]",
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

    let agg = Aggregate::Min;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // one row deleted
    let expected_results = vec![
        "Series tags={_field=bar, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000011], values: [5.0]",
        "Series tags={_field=foo, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000001, 1609459201000000024], values: [1.0, 11.24]",
        "Series tags={_field=bar, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000009, 1609459201000000015], values: [4.0, 6.0]",
        "Series tags={_field=foo, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000002], values: [2.0]",
    ];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2697WithDelete {},
        predicate.clone(),
        agg,
        every.clone(),
        offset.clone(),
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

    let agg = Aggregate::Sum;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAD).
    // For each distinct series the window defines the `time` column
    let expected_results = vec![
        "Series tags={_field=bar, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000020], values: [5.0]",
        "Series tags={_field=foo, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000030], values: [4.0, 11.24]",
        "Series tags={_field=bar, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000020, 1609459201000000030], values: [4.0, 6.0, 1.2]",
        "Series tags={_field=foo, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000010], values: [2.0]",
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

    let agg = Aggregate::Sum;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAD).
    // For each distinct series the window defines the `time` column
    let expected_results = vec![
        "Series tags={_field=foo, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000030], values: [4.0, 11.24]",
        "Series tags={_field=foo, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000010], values: [2.0]",
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

    let agg = Aggregate::Sum;
    let every = WindowDuration::from_nanoseconds(10);
    let offset = WindowDuration::from_nanoseconds(0);

    // one row deleted

    // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAD).
    // For each distinct series the window defines the `time` column
    let expected_results = vec![
        "Series tags={_field=bar, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000020], values: [5.0]",
        "Series tags={_field=foo, _measurement=mm, section=1a}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000030], values: [4.0, 11.24]",
        "Series tags={_field=bar, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000010, 1609459201000000020], values: [4.0, 6.0]",
        "Series tags={_field=foo, _measurement=mm, section=2b}\n  FloatPoints timestamps: [1609459201000000010], values: [2.0]",
    ];
    run_read_window_aggregate_test_case(
        MeasurementForDefect2697WithDelete {},
        predicate.clone(),
        agg,
        every.clone(),
        offset.clone(),
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

// Test data to validate fix for:
// https://github.com/influxdata/influxdb_iox/issues/2890
struct MeasurementForDefect2890 {}
#[async_trait]
impl DbSetup for MeasurementForDefect2890 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "mm foo=2.0 1609459201000000001",
            "mm foo=2.0 1609459201000000002",
            "mm foo=3.0 1609459201000000005",
            "mm foo=11.24 1609459201000000024",
            "mm bar=4.0 1609459201000000009",
            "mm bar=5.0 1609459201000000011",
            "mm bar=6.0 1609459201000000015",
            "mm bar=1.2 1609459201000000022",
            "mm bar=2.8 1609459201000000031",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "mm", partition_key).await
    }
}

#[tokio::test]
async fn test_read_window_aggregate_overflow() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(1609459201000000001, 1609459201000000024)
        .build();

    let agg = Aggregate::Max;
    // Note the giant window (every=9223372036854775807)
    let every = WindowDuration::from_nanoseconds(i64::MAX);
    let offset = WindowDuration::from_nanoseconds(0);

    let expected_results = vec![
        "Series tags={_field=bar, _measurement=mm}\n  FloatPoints timestamps: [1609459201000000015], values: [6.0]",
        "Series tags={_field=foo, _measurement=mm}\n  FloatPoints timestamps: [1609459201000000005], values: [3.0]",
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
