//! basic tests of SQL query planning DataFusion contains much more
//! extensive test coverage, this module is meant to show we have
//! wired all the pieces together (as well as ensure any particularly
//! important SQL does not regress)

#![allow(unused_imports, dead_code, unused_macros)]

use super::scenarios::*;
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
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
        "+---------------+--------------------+---------------+------------+",
        "| table_catalog | table_schema       | table_name    | table_type |",
        "+---------------+--------------------+---------------+------------+",
        "| public        | information_schema | columns       | VIEW       |",
        "| public        | information_schema | tables        | VIEW       |",
        "| public        | iox                | h2o           | BASE TABLE |",
        "| public        | iox                | o2            | BASE TABLE |",
        "| public        | system             | chunk_columns | BASE TABLE |",
        "| public        | system             | chunks        | BASE TABLE |",
        "| public        | system             | columns       | BASE TABLE |",
        "| public        | system             | operations    | BASE TABLE |",
        "+---------------+--------------------+---------------+------------+",
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
        "+---------------+--------------+------------+-------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
        "| table_catalog | table_schema | table_name | column_name | ordinal_position | column_default | is_nullable | data_type                   | character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale | datetime_precision | interval_type |",
        "+---------------+--------------+------------+-------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
        "| public        | iox          | h2o        | city        | 0                |                | YES         | Dictionary(Int32, Utf8)     |                          |                        |                   |                         |               |                    |               |",
        "| public        | iox          | h2o        | moisture    | 1                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
        "| public        | iox          | h2o        | other_temp  | 2                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
        "| public        | iox          | h2o        | state       | 3                |                | YES         | Dictionary(Int32, Utf8)     |                          |                        |                   |                         |               |                    |               |",
        "| public        | iox          | h2o        | temp        | 4                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
        "| public        | iox          | h2o        | time        | 5                |                | NO          | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
        "| public        | iox          | o2         | city        | 0                |                | YES         | Dictionary(Int32, Utf8)     |                          |                        |                   |                         |               |                    |               |",
        "| public        | iox          | o2         | reading     | 1                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
        "| public        | iox          | o2         | state       | 2                |                | YES         | Dictionary(Int32, Utf8)     |                          |                        |                   |                         |               |                    |               |",
        "| public        | iox          | o2         | temp        | 3                |                | YES         | Float64                     |                          |                        | 24                | 2                       |               |                    |               |",
        "| public        | iox          | o2         | time        | 4                |                | NO          | Timestamp(Nanosecond, None) |                          |                        |                   |                         |               |                    |               |",
        "+---------------+--------------+------------+-------------+------------------+----------------+-------------+-----------------------------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFields {},
        "SELECT * from information_schema.columns where table_name = 'h2o' OR table_name = 'o2'",
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
        "| public        | iox          | h2o        | city        | Dictionary(Int32, Utf8)     | YES         |",
        "| public        | iox          | h2o        | moisture    | Float64                     | YES         |",
        "| public        | iox          | h2o        | other_temp  | Float64                     | YES         |",
        "| public        | iox          | h2o        | state       | Dictionary(Int32, Utf8)     | YES         |",
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
async fn sql_select_from_system_chunks() {
    // system tables reflect the state of chunks, so don't run them
    // with different chunk configurations.

    //  ensures the tables / plumbing are hooked up (so no need to
    //  test timestamps, etc)

    let expected = vec![
        "+----+---------------+------------+-------------------+-----------------+-----------+",
        "| id | partition_key | table_name | storage           | estimated_bytes | row_count |",
        "+----+---------------+------------+-------------------+-----------------+-----------+",
        "| 0  | 1970-01-01T00 | h2o        | OpenMutableBuffer | 257             | 3         |",
        "| 0  | 1970-01-01T00 | o2         | OpenMutableBuffer | 221             | 2         |",
        "+----+---------------+------------+-------------------+-----------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFieldsOneChunk {},
        "SELECT id, partition_key, table_name, storage, estimated_bytes, row_count from system.chunks",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_system_columns() {
    // system tables reflect the state of chunks, so don't run them
    // with different chunk configurations.

    //  ensures the tables / plumbing are hooked up (so no need to
    //  test timestamps, etc)

    let expected = vec![
        "+---------------+------------+-------------+-------------+---------------+",
        "| partition_key | table_name | column_name | column_type | influxdb_type |",
        "+---------------+------------+-------------+-------------+---------------+",
        "| 1970-01-01T00 | h2o        | city        | String      | Tag           |",
        "| 1970-01-01T00 | h2o        | other_temp  | F64         | Field         |",
        "| 1970-01-01T00 | h2o        | state       | String      | Tag           |",
        "| 1970-01-01T00 | h2o        | temp        | F64         | Field         |",
        "| 1970-01-01T00 | h2o        | time        | I64         | Timestamp     |",
        "| 1970-01-01T00 | o2         | city        | String      | Tag           |",
        "| 1970-01-01T00 | o2         | reading     | F64         | Field         |",
        "| 1970-01-01T00 | o2         | state       | String      | Tag           |",
        "| 1970-01-01T00 | o2         | temp        | F64         | Field         |",
        "| 1970-01-01T00 | o2         | time        | I64         | Timestamp     |",
        "+---------------+------------+-------------+-------------+---------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFieldsOneChunk {},
        "SELECT * from system.columns",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_system_chunk_columns() {
    // system tables reflect the state of chunks, so don't run them
    // with different chunk configurations.

    let expected = vec![
        "+---------------+----------+------------+--------------+-------------------+-------+-----------+-----------+-----------------+",
        "| partition_key | chunk_id | table_name | column_name  | storage           | count | min_value | max_value | estimated_bytes |",
        "+---------------+----------+------------+--------------+-------------------+-------+-----------+-----------+-----------------+",
        "| 1970-01-01T00 | 0        | h2o        | city         | ReadBuffer        | 2     | Boston    | time      | 585             |",
        "| 1970-01-01T00 | 0        | h2o        | other_temp   | ReadBuffer        | 2     | 70.4      | 70.4      | 369             |",
        "| 1970-01-01T00 | 0        | h2o        | state        | ReadBuffer        | 2     | Boston    | time      | 585             |",
        "| 1970-01-01T00 | 0        | h2o        | temp         | ReadBuffer        | 2     | 70.4      | 70.4      | 369             |",
        "| 1970-01-01T00 | 0        | h2o        | time         | ReadBuffer        | 2     | 50        | 250       | 51              |",
        "| 1970-01-01T00 | 0        | o2         | __dictionary | OpenMutableBuffer |       |           |           | 112             |",
        "| 1970-01-01T00 | 0        | o2         | city         | OpenMutableBuffer | 1     | Boston    | Boston    | 17              |",
        "| 1970-01-01T00 | 0        | o2         | reading      | OpenMutableBuffer | 1     | 51        | 51        | 25              |",
        "| 1970-01-01T00 | 0        | o2         | state        | OpenMutableBuffer | 2     | CA        | MA        | 17              |",
        "| 1970-01-01T00 | 0        | o2         | temp         | OpenMutableBuffer | 2     | 53.4      | 79        | 25              |",
        "| 1970-01-01T00 | 0        | o2         | time         | OpenMutableBuffer | 2     | 50        | 300       | 25              |",
        "| 1970-01-01T00 | 1        | h2o        | __dictionary | OpenMutableBuffer |       |           |           | 94              |",
        "| 1970-01-01T00 | 1        | h2o        | city         | OpenMutableBuffer | 1     | Boston    | Boston    | 13              |",
        "| 1970-01-01T00 | 1        | h2o        | other_temp   | OpenMutableBuffer | 1     | 72.4      | 72.4      | 17              |",
        "| 1970-01-01T00 | 1        | h2o        | state        | OpenMutableBuffer | 1     | CA        | CA        | 13              |",
        "| 1970-01-01T00 | 1        | h2o        | time         | OpenMutableBuffer | 1     | 350       | 350       | 17              |",
        "+---------------+----------+------------+--------------+-------------------+-------+-----------+-----------+-----------------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsManyFieldsTwoChunks {},
        "SELECT * from system.chunk_columns",
        &expected
    );
}

#[tokio::test]
async fn sql_select_from_system_operations() {
    test_helpers::maybe_start_logging();
    let expected = vec![
        "+----+----------+---------------+----------------+---------------+----------+---------------------------------+",
        "| id | status   | took_cpu_time | took_wall_time | partition_key | chunk_id | description                     |",
        "+----+----------+---------------+----------------+---------------+----------+---------------------------------+",
        "| 0  | Complete | true          | true           | 1970-01-01T00 | 0        | Loading chunk to ReadBuffer     |",
        "| 1  | Complete | true          | true           | 1970-01-01T00 | 0        | Writing chunk to Object Storage |",
        "+----+----------+---------------+----------------+---------------+----------+---------------------------------+",
    ];

    // Check that the cpu time used reported is greater than zero as it isn't
    // repeatable
    run_sql_test_case!(
        TwoMeasurementsManyFieldsLifecycle {},
        "SELECT id, status, CAST(cpu_time_used AS BIGINT) > 0 as took_cpu_time, CAST(wall_time_used AS BIGINT) > 0 as took_wall_time, partition_key, chunk_id, description from system.operations",
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

#[tokio::test]
async fn sql_predicate_pushdown() {
    // Test 1: Select everything
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 189   | 7      | 1970-01-01 00:00:00.000000110 | bedford   |",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 40000 | 5      | 1970-01-01 00:00:00.000000100 | andover   |",
        "| 471   | 6      | 1970-01-01 00:00:00.000000110 | tewsbury  |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant",
        &expected
    );

    // TODO: Make push-down predicates shown in explain verbose. Ticket #1538
    // Check the plan
    // run_sql_test_case!(
    //     TwoMeasurementsPredicatePushDown {},
    //     "EXPLAIN VERBOSE SELECT * from restaurant",
    //     &expected
    // );

    // Test 2: One push-down expression: count > 200
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 40000 | 5      | 1970-01-01 00:00:00.000000100 | andover   |",
        "| 471   | 6      | 1970-01-01 00:00:00.000000110 | tewsbury  |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200",
        &expected
    );

    // Check the plan
    // run_sql_test_case!(
    //     TwoMeasurementsPredicatePushDown {},
    //     "EXPLAIN VERBOSE SELECT * from restaurant where count > 200",
    //     &expected
    // );

    // Test 3: Two push-down expression: count > 200 and town != 'tewsbury'
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 40000 | 5      | 1970-01-01 00:00:00.000000100 | andover   |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200 and town != 'tewsbury'",
        &expected
    );

    // Check the plan
    // run_sql_test_case!(
    //     TwoMeasurementsPredicatePushDown {},
    //     "EXPLAIN VERBOSE SELECT * from restaurant where count > 200 and town != 'tewsbury'",
    //     &expected
    // );

    // Test 4: Still two push-down expression: count > 200 and town != 'tewsbury'
    // even though the results are different
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 40000 | 5      | 1970-01-01 00:00:00.000000100 | andover   |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence')",
        &expected
    );

    // Test 5: three push-down expression: count > 200 and town != 'tewsbury' and count < 40000
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence') and count < 40000",
        &expected
    );

    // Test 6: two push-down expression: count > 200 and count < 40000
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 471   | 6      | 1970-01-01 00:00:00.000000110 | tewsbury  |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200  and count < 40000",
        &expected
    );

    // Test 7: two push-down expression on float: system > 4.0 and system < 7.0
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+-----------+",
        "| count | system | time                          | town      |",
        "+-------+--------+-------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01 00:00:00.000000100 | lexington |",
        "| 40000 | 5      | 1970-01-01 00:00:00.000000100 | andover   |",
        "| 471   | 6      | 1970-01-01 00:00:00.000000110 | tewsbury  |",
        "| 632   | 5      | 1970-01-01 00:00:00.000000120 | reading   |",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading   |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence  |",
        "+-------+--------+-------------------------------+-----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 4.0 and system < 7.0",
        &expected
    );

    // Test 8: two push-down expression on float: system > 5.0 and system < 7.0
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+----------+",
        "| count | system | time                          | town     |",
        "+-------+--------+-------------------------------+----------+",
        "| 471   | 6      | 1970-01-01 00:00:00.000000110 | tewsbury |",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading  |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence |",
        "+-------+--------+-------------------------------+----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and system < 7.0",
        &expected
    );

    // Test 9: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+----------+",
        "| count | system | time                          | town     |",
        "+-------+--------+-------------------------------+----------+",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading  |",
        "| 872   | 6      | 1970-01-01 00:00:00.000000110 | lawrence |",
        "+-------+--------+-------------------------------+----------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and town != 'tewsbury' and 7.0 > system",
        &expected
    );

    // Test 10: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
    // even though there are more expressions,(count = 632 or town = 'reading'), in the filter
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+---------+",
        "| count | system | time                          | town    |",
        "+-------+--------+-------------------------------+---------+",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading |",
        "+-------+--------+-------------------------------+---------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and (count = 632 or town = 'reading')",
        &expected
    );

    // Test 11: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
    // After DF ticket, https://github.com/apache/arrow-datafusion/issues/383 is done,
    // there will be more pushed-down predicate time > to_timestamp('1970-01-01T00:00:00.000000120+00:00')
    //
    // Check correctness
    let expected = vec!["++", "++"];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where 5.0 < system and town != 'tewsbury' and system < 7.0 and (count = 632 or town = 'reading') and time > to_timestamp('1970-01-01T00:00:00.000000130+00:00')",
        &expected
    );

    // TODO: Hit stackoverflow in DF. Ticket https://github.com/apache/arrow-datafusion/issues/419
    // // Test 12: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0 and town = 'reading'
    // //
    // // Check correctness
    // let expected = vec![
    //     "+-------+--------+-------------------------------+---------+",
    //     "| count | system | time                          | town    |",
    //     "+-------+--------+-------------------------------+---------+",
    //     "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading |",
    //     "+-------+--------+-------------------------------+---------+",
    // ];
    // run_sql_test_case!(
    //     TwoMeasurementsPredicatePushDown {},
    //     "SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and town = 'reading'",
    //     &expected
    // );

    // Test 13: three push-down expression: system > 5.0 and system < 7.0 and town = 'reading'
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+-------------------------------+---------+",
        "| count | system | time                          | town    |",
        "+-------+--------+-------------------------------+---------+",
        "| 632   | 6      | 1970-01-01 00:00:00.000000130 | reading |",
        "+-------+--------+-------------------------------+---------+",
    ];
    run_sql_test_case!(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and system < 7.0 and town = 'reading'",
        &expected
    );
}
