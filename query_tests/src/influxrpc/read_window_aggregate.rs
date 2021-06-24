//! Tests for the Influx gRPC queries
use crate::scenarios::*;

use server::{db::test_helpers::write_lp, utils::make_db};

use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::prelude::*;
use query::{
    frontend::influxrpc::InfluxRpcPlanner,
    group_by::{Aggregate, WindowDuration},
    predicate::{Predicate, PredicateBuilder},
};

/// runs read_window_aggregate(predicate) and compares it to the expected
/// output
macro_rules! run_read_window_aggregate_test_case {
    ($DB_SETUP:expr, $PREDICATE:expr, $AGG:expr, $EVERY:expr, $OFFSET:expr, $EXPECTED_RESULTS:expr) => {
        test_helpers::maybe_start_logging();
        let predicate = $PREDICATE;
        let agg = $AGG;
        let every = $EVERY;
        let offset = $OFFSET;
        let expected_results = $EXPECTED_RESULTS;
        for scenario in $DB_SETUP.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!("Predicate: '{:#?}'", predicate);
            let planner = InfluxRpcPlanner::new();

            let plans = planner
                .read_window_aggregate(&db, predicate.clone(), agg, every.clone(), offset.clone())
                .expect("built plan successfully");

            let plans = plans.into_inner();

            let mut string_results = vec![];
            for plan in plans.into_iter() {
                let batches = db
                    .executor()
                    .run_logical_plan(plan.plan)
                    .await
                    .expect("ok running plan");

                string_results.extend(
                    pretty_format_batches(&batches)
                        .expect("formatting results")
                        .trim()
                        .split('\n')
                        .map(|s| s.to_string()),
                );
            }

            assert_eq!(
                expected_results, string_results,
                "Error in  scenario '{}'\n\nexpected:\n{:#?}\nactual:\n{:#?}",
                scenario_name, expected_results, string_results
            );
        }
    };
}

#[tokio::test]
async fn test_read_window_aggregate_no_data_no_pred() {
    let predicate = Predicate::default();
    let agg = Aggregate::Mean;
    let every = WindowDuration::from_nanoseconds(200);
    let offset = WindowDuration::from_nanoseconds(0);
    let expected_results = vec![] as Vec<&str>;

    run_read_window_aggregate_test_case!(
        NoData {},
        predicate,
        agg,
        every,
        offset,
        expected_results
    );
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
        "+--------+-------+-------------------------------+------+",
        "| city   | state | time                          | temp |",
        "+--------+-------+-------------------------------+------+",
        "| Boston | MA    | 1970-01-01 00:00:00.000000200 | 70   |",
        "| Boston | MA    | 1970-01-01 00:00:00.000000400 | 71.5 |",
        "| Boston | MA    | 1970-01-01 00:00:00.000000600 | 73   |",
        "| LA     | CA    | 1970-01-01 00:00:00.000000200 | 90   |",
        "| LA     | CA    | 1970-01-01 00:00:00.000000400 | 91.5 |",
        "| LA     | CA    | 1970-01-01 00:00:00.000000600 | 93   |",
        "+--------+-------+-------------------------------+------+",
    ];

    run_read_window_aggregate_test_case!(
        MeasurementForWindowAggregate {},
        predicate,
        agg,
        every,
        offset,
        expected_results
    );
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

#[tokio::test]
async fn test_read_window_aggregate_months() {
    let predicate = PredicateBuilder::default().build();

    let agg = Aggregate::Mean;
    let every = WindowDuration::from_months(1, false);
    let offset = WindowDuration::from_months(0, false);

    // note the name of the field is "temp" even though it is the average
    let expected_results = vec![
        "+--------+-------+---------------------+------+",
        "| city   | state | time                | temp |",
        "+--------+-------+---------------------+------+",
        "| Boston | MA    | 2020-04-01 00:00:00 | 70.5 |",
        "| Boston | MA    | 2020-05-01 00:00:00 | 72.5 |",
        "+--------+-------+---------------------+------+",
    ];

    run_read_window_aggregate_test_case!(
        MeasurementForWindowAggregateMonths {},
        predicate,
        agg,
        every,
        offset,
        expected_results
    );
}
