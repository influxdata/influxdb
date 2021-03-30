//! basic tests of SQL query planning DataFusion contains much more
//! extensive test coverage, this module is meant to show we have
//! wired all the pieces together (as well as ensure any particularly
//! important SQL does not regress)

use super::scenarios::*;
use arrow_deps::{
    arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
};
use query::{exec::Executor, frontend::sql::SQLQueryPlanner};
use std::sync::Arc;

/// runs table_names(predicate) and compares it to the expected
/// output
macro_rules! run_sql_test_case {
    ($DB_SETUP:expr, $SQL:expr, $EXPECTED_LINES:expr) => {
        test_helpers::maybe_start_logging();
        let sql = $SQL.to_string();
        for scenario in $DB_SETUP.make().await {
            let DBScenario {
                scenario_name, db, ..
            } = scenario;
            let db = Arc::new(db);

            println!("Running scenario '{}'", scenario_name);
            println!("SQL: '{:#?}'", sql);
            let planner = SQLQueryPlanner::default();
            let executor = Executor::new();

            let physical_plan = planner
                .query(db, &sql, &executor)
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

#[tokio::test]
async fn sql_select_with_schema_merge() {
    let expected = vec![
        "+------+--------+--------+------+------+",
        "| host | region | system | time | user |",
        "+------+--------+--------+------+------+",
        "|      | west   | 5      | 100  | 23.2 |",
        "|      | west   | 6      | 150  | 21   |",
        "| foo  | east   |        | 100  | 23.2 |",
        "| bar  | west   |        | 250  | 21   |",
        "+------+--------+--------+------+------+",
    ];
    run_sql_test_case!(MultiChunkSchemaMerge {}, "SELECT * from cpu", &expected);
}

#[tokio::test]
async fn sql_select_from_restaurant() {
    let expected = vec![
        "+---------+-------+",
        "| town    | count |",
        "+---------+-------+",
        "| andover | 40000 |",
        "| reading | 632   |",
        "+---------+-------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsUnsignedType {},
        "SELECT town, count from restaurant",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_school() {
    let expected = vec![
        "+---------+-------+",
        "| town    | count |",
        "+---------+-------+",
        "| reading | 17    |",
        "| andover | 25    |",
        "+---------+-------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsUnsignedType {},
        "SELECT town, count from school",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_information_schema_tables() {
    // validate we have access to information schema for listing table
    // names
    let expected = vec![
        "+---------------+--------------------+------------+------------+",
        "| table_catalog | table_schema       | table_name | table_type |",
        "+---------------+--------------------+------------+------------+",
        "| public        | iox                | h2o        | BASE TABLE |",
        "| public        | iox                | o2         | BASE TABLE |",
        "| public        | information_schema | tables     | VIEW       |",
        "+---------------+--------------------+------------+------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFields {},
        "SELECT * from information_schema.tables",
        &expected
    );
}

#[tokio::test]
async fn sql_union_all() {
    // validate name resolution works for UNION ALL queries
    let expected = vec![
        "+--------+",
        "| name   |",
        "+--------+",
        "| MA     |",
        "| MA     |",
        "| CA     |",
        "| MA     |",
        "| Boston |",
        "| Boston |",
        "| Boston |",
        "| Boston |",
        "+--------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFields {},
        "select state as name from h2o UNION ALL select city as name from h2o",
        &expected
    );
}

#[tokio::test]
async fn sql_select_with_schema_merge_subset() {
    let expected = vec![
        "+------+--------+--------+",
        "| host | region | system |",
        "+------+--------+--------+",
        "|      | west   | 5      |",
        "|      | west   | 6      |",
        "| foo  | east   |        |",
        "| bar  | west   |        |",
        "+------+--------+--------+",
    ];
    run_sql_test_case!(
        MultiChunkSchemaMerge {},
        "SELECT host, region, system from cpu",
        &expected
    );
}
