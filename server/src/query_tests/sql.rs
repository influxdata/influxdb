//! basic tests of SQL query planning DataFusion contains much more
//! extensive test coverage, this module is meant to show we have
//! wired all the pieces together (as well as ensure any particularly
//! important SQL does not regress)

use super::scenarios::*;
use arrow_deps::{
    arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
};
use query::{exec::Executor, frontend::sql::SQLQueryPlanner};

/// runs table_names(predicate) and compares it to the expected
/// output
macro_rules! run_sql_test_case {
    ($DB_SETUP:expr, $SQL:expr, $EXPECTED_LINES:expr) => {
        let sql = $SQL.to_string();
        for scenario in $DB_SETUP.make().await {
            let DBScenario { scenario_name, db } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!("SQL: '{:#?}'", sql);
            let planner = SQLQueryPlanner::new();
            let executor = Executor::new();

            let physical_plan = planner
                .query(&db, &sql, &executor)
                .await
                .expect("built plan successfully");

            let results: Vec<RecordBatch> = collect(physical_plan).await.expect("Running plan");

            assert_table_eq!($EXPECTED_LINES, &results);
        }
    };
}

#[tokio::test]
async fn sql_select_from_cpu() {
    let expected = vec![
        "+--------+------+------+",
        "| region | time | user |",
        "+--------+------+------+",
        "| west   | 100  | 23.2 |",
        "| west   | 150  | 21   |",
        "+--------+------+------+",
    ];
    run_sql_test_case!(TwoMeasurements {}, "SELECT * from cpu", &expected);
}

#[tokio::test]
async fn sql_select_from_cpu_with_projection() {
    // expect that to get a subset of the columns and in the order specified
    let expected = vec![
        "+------+--------+",
        "| user | region |",
        "+------+--------+",
        "| 23.2 | west   |",
        "| 21   | west   |",
        "+------+--------+",
    ];
    run_sql_test_case!(
        TwoMeasurements {},
        "SELECT user, region from cpu",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_cpu_pred() {
    let expected = vec![
        "+--------+------+------+",
        "| region | time | user |",
        "+--------+------+------+",
        "| west   | 150  | 21   |",
        "+--------+------+------+",
    ];
    run_sql_test_case!(
        TwoMeasurements {},
        "SELECT * from cpu where time > 125",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_cpu_with_projection_and_pred() {
    // expect that to get a subset of the columns and in the order specified
    let expected = vec![
        "+------+--------+",
        "| user | region |",
        "+------+--------+",
        "| 21   | west   |",
        "+------+--------+",
    ];
    run_sql_test_case!(
        TwoMeasurements {},
        "SELECT user, region from cpu where time > 125",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_cpu_group() {
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    run_sql_test_case!(
        TwoMeasurements {},
        "SELECT count(*) from cpu group by region",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_disk() {
    let expected = vec![
        "+-------+--------+------+",
        "| bytes | region | time |",
        "+-------+--------+------+",
        "| 99    | east   | 200  |",
        "+-------+--------+------+",
    ];
    run_sql_test_case!(TwoMeasurements {}, "SELECT * from disk", &expected);
}
