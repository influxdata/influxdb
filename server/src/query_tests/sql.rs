//! basic tests of SQL query planning DataFusion contains much more
//! extensive test coverage, this module is meant to show we have
//! wired all the pieces together (as well as ensure any particularly
//! important SQL does not regress)

#![allow(unused_imports, dead_code, unused_macros)]

use super::scenarios::*;
use arrow_deps::{arrow::record_batch::RecordBatch, assert_batches_sorted_eq};
use query::frontend::sql::SqlQueryPlanner;
use std::sync::Arc;

/// runs table_names(predicate) and compares it to the expected
/// output
macro_rules! run_sql_test_case {
    ($DB_SETUP:expr, $SQL:expr, $EXPECTED_LINES:expr) => {
        test_helpers::maybe_start_logging();
        let sql = $SQL.to_string();
        for scenario in $DB_SETUP.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            let db = Arc::new(db);

            println!("Running scenario '{}'", scenario_name);
            println!("SQL: '{:#?}'", sql);
            let planner = SqlQueryPlanner::default();
            let executor = db.executor();

            let physical_plan = planner
                .query(db, &sql, executor.as_ref())
                .expect("built plan successfully");

            let results: Vec<RecordBatch> =
                executor.collect(physical_plan).await.expect("Running plan");

            assert_batches_sorted_eq!($EXPECTED_LINES, &results);
        }
    };
}

#[tokio::test]
async fn sql_select_from_cpu() {
    let expected = vec![
        "+--------+-------------------------------+------+",
        "| region | time                          | user |",
        "+--------+-------------------------------+------+",
        "| west   | 1970-01-01 00:00:00.000000100 | 23.2 |",
        "| west   | 1970-01-01 00:00:00.000000150 | 21   |",
        "+--------+-------------------------------+------+",
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
        "+--------+-------------------------------+------+",
        "| region | time                          | user |",
        "+--------+-------------------------------+------+",
        "| west   | 1970-01-01 00:00:00.000000150 | 21   |",
        "+--------+-------------------------------+------+",
    ];
    run_sql_test_case!(
        TwoMeasurements {},
        "SELECT * from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00')",
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
        "SELECT user, region from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00')",
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
        "+-------+--------+-------------------------------+",
        "| bytes | region | time                          |",
        "+-------+--------+-------------------------------+",
        "| 99    | east   | 1970-01-01 00:00:00.000000200 |",
        "+-------+--------+-------------------------------+",
    ];
    run_sql_test_case!(TwoMeasurements {}, "SELECT * from disk", &expected);
}

#[tokio::test]
async fn sql_select_with_schema_merge() {
    let expected = vec![
        "+------+--------+--------+-------------------------------+------+",
        "| host | region | system | time                          | user |",
        "+------+--------+--------+-------------------------------+------+",
        "|      | west   | 5      | 1970-01-01 00:00:00.000000100 | 23.2 |",
        "|      | west   | 6      | 1970-01-01 00:00:00.000000150 | 21   |",
        "| foo  | east   |        | 1970-01-01 00:00:00.000000100 | 23.2 |",
        "| bar  | west   |        | 1970-01-01 00:00:00.000000250 | 21   |",
        "+------+--------+--------+-------------------------------+------+",
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
        "| public        | system             | chunks     | BASE TABLE |",
        "| public        | system             | columns    | BASE TABLE |",
        "| public        | information_schema | tables     | VIEW       |",
        "| public        | information_schema | columns    | VIEW       |",
        "+---------------+--------------------+------------+------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFields {},
        "SELECT * from information_schema.tables",
        &expected
    );
    run_sql_test_case!(TwoMeasurementsManyFields {}, "SHOW TABLES", &expected);
}

#[tokio::test]
async fn sql_select_from_information_schema_columns() {
    // validate we have access to information schema for listing columns
    // names
    let expected = vec![
    "+---------------+--------------+------------+---------------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
    "| table_catalog | table_schema | table_name | column_name         | ordinal_position | column_default | is_nullable | data_type                   | character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale | datetime_precision | interval_type |",
    "+---------------+--------------+------------+---------------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
    "| public        | iox          | h2o        | city                | 0                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | iox          | h2o        | moisture            | 1                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
    "| public        | iox          | h2o        | other_temp          | 2                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
    "| public        | iox          | h2o        | state               | 3                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | iox          | h2o        | temp                | 4                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
    "| public        | iox          | h2o        | time                | 5                |                | NO          | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
    "| public        | iox          | o2         | city                | 0                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | iox          | o2         | reading             | 1                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
    "| public        | iox          | o2         | state               | 2                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | iox          | o2         | temp                | 3                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
    "| public        | iox          | o2         | time                | 4                |                | NO          | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
    "| public        | system       | chunks     | estimated_bytes     | 3                |                | YES         | UInt64                      |                          |                        |                   |                         |               |                    |               |",
    "| public        | system       | chunks     | id                  | 0                |                | NO          | UInt32                      |                          |                        | 32                | 2                       |               |                    |               |",
    "| public        | system       | chunks     | partition_key       | 1                |                | NO          | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | system       | chunks     | storage             | 2                |                | NO          | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | system       | chunks     | time_closing        | 6                |                | YES         | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
    "| public        | system       | chunks     | time_of_first_write | 4                |                | YES         | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
    "| public        | system       | chunks     | time_of_last_write  | 5                |                | YES         | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
    "| public        | system       | columns    | column_name         | 2                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | system       | columns    | count               | 3                |                | YES         | UInt64                      |                          |                        |                   |                         |               |                    |               |",
    "| public        | system       | columns    | partition_key       | 0                |                | NO          | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "| public        | system       | columns    | table_name          | 1                |                | YES         | Utf8                        |                          | 2147483647             |                   |                         |               |                    |               |",
    "+---------------+--------------+------------+---------------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFields {},
        "SELECT * from information_schema.columns",
        &expected
    );
}

#[tokio::test]
async fn sql_show_columns() {
    // validate we have access to SHOW SCHEMA for listing columns
    // names
    let expected = vec![
    "+---------------+--------------+------------+-------------+-----------------------------+-------------+",
    "| table_catalog | table_schema | table_name | column_name | data_type                   | is_nullable |",
    "+---------------+--------------+------------+-------------+-----------------------------+-------------+",
    "| public        | iox          | h2o        | city        | Utf8                        | YES         |",
    "| public        | iox          | h2o        | moisture    | Float64                     | YES         |",
    "| public        | iox          | h2o        | other_temp  | Float64                     | YES         |",
    "| public        | iox          | h2o        | state       | Utf8                        | YES         |",
    "| public        | iox          | h2o        | temp        | Float64                     | YES         |",
    "| public        | iox          | h2o        | time        | Timestamp(Nanosecond, None) | NO          |",
    "+---------------+--------------+------------+-------------+-----------------------------+-------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFields {},
        "SHOW COLUMNS FROM h2o",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_system_tables() {
    // system tables reflect the state of chunks, so don't run them
    // with different chunk configurations.

    //  ensures the tables / plumbing are hooked up (so no need to
    //  test timestamps, etc)

    let expected = vec![
        "+----+---------------+-------------------+-----------------+",
        "| id | partition_key | storage           | estimated_bytes |",
        "+----+---------------+-------------------+-----------------+",
        "| 0  | 1970-01-01T00 | OpenMutableBuffer | 501             |",
        "+----+---------------+-------------------+-----------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFieldsOneChunk {},
        "SELECT id, partition_key, storage, estimated_bytes from system.chunks",
        &expected
    );

    let expected = vec![
        "+---------------+------------+-------------+-------+",
        "| partition_key | table_name | column_name | count |",
        "+---------------+------------+-------------+-------+",
        "| 1970-01-01T00 | h2o        | city        | 3     |",
        "| 1970-01-01T00 | h2o        | other_temp  | 2     |",
        "| 1970-01-01T00 | h2o        | state       | 3     |",
        "| 1970-01-01T00 | h2o        | temp        | 1     |",
        "| 1970-01-01T00 | h2o        | time        | 3     |",
        "| 1970-01-01T00 | o2         | city        | 1     |",
        "| 1970-01-01T00 | o2         | state       | 2     |",
        "| 1970-01-01T00 | o2         | temp        | 2     |",
        "| 1970-01-01T00 | o2         | time        | 2     |",
        "| 1970-01-01T00 | o2         | reading     | 1     |",
        "+---------------+------------+-------------+-------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFieldsOneChunk {},
        "SELECT * from system.columns",
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
