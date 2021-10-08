//! basic tests of SQL query planning DataFusion contains much more
//! extensive test coverage, this module is meant to show we have
//! wired all the pieces together (as well as ensure any particularly
//! important SQL does not regress)

use crate::scenarios;

use super::scenarios::*;
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use query::{exec::ExecutionContextProvider, frontend::sql::SqlQueryPlanner};

/// Runs table_names(predicate) and compares it to the expected
/// output.
async fn run_sql_test_case<D>(db_setup: D, sql: &str, expected_lines: &[&str])
where
    D: DbSetup,
{
    test_helpers::maybe_start_logging();

    let sql = sql.to_string();
    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;

        println!("Running scenario '{}'", scenario_name);
        println!("SQL: '{:#?}'", sql);
        let planner = SqlQueryPlanner::default();
        let ctx = db.new_query_context(None);

        let physical_plan = planner
            .query(&sql, &ctx)
            .await
            .expect("built plan successfully");

        let results: Vec<RecordBatch> = ctx.collect(physical_plan).await.expect("Running plan");
        assert_batches_sorted_eq!(expected_lines, &results);
    }
}

#[tokio::test]
async fn sql_select_from_cpu() {
    let expected = vec![
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    run_sql_test_case(TwoMeasurements {}, "SELECT * from cpu", &expected).await;
}

// BUG: https://github.com/influxdata/influxdb_iox/issues/2776
#[ignore]
#[tokio::test]
async fn sql_select_from_cpu_min_utf8() {
    let expected = vec![
        "+----------------+",
        "| MIN(cpu.region |",
        "+----------------+",
        "| west           |",
        "+----------------+",
    ];
    run_sql_test_case(TwoMeasurements {}, "SELECT min(region) from cpu", &expected).await;
}

#[tokio::test]
async fn sql_select_from_cpu_2021() {
    let expected = vec![
        "+--------+----------------------+------+",
        "| region | time                 | user |",
        "+--------+----------------------+------+",
        "| west   | 2021-07-20T19:28:50Z | 23.2 |",
        "| west   | 2021-07-20T19:30:30Z | 21   |",
        "+--------+----------------------+------+",
    ];
    run_sql_test_case(
        OneMeasurementRealisticTimes {},
        "SELECT * from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_cpu_with_timestamp_predicate_explicit_utc() {
    let expected = vec![
        "+--------+----------------------+------+",
        "| region | time                 | user |",
        "+--------+----------------------+------+",
        "| west   | 2021-07-20T19:30:30Z | 21   |",
        "+--------+----------------------+------+",
    ];

    run_sql_test_case(
        OneMeasurementRealisticTimes {},
        "SELECT * FROM cpu WHERE time  > to_timestamp('2021-07-20 19:28:50+00:00')",
        &expected,
    )
    .await;

    // Use RCF3339 format
    run_sql_test_case(
        OneMeasurementRealisticTimes {},
        "SELECT * FROM cpu WHERE time  > to_timestamp('2021-07-20T19:28:50Z')",
        &expected,
    )
    .await;

    // use cast workaround
    run_sql_test_case(
        OneMeasurementRealisticTimes {},
        "SELECT * FROM cpu WHERE \
         CAST(time AS BIGINT) > CAST(to_timestamp('2021-07-20T19:28:50Z') AS BIGINT)",
        &expected,
    )
    .await;
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
    run_sql_test_case(
        TwoMeasurements {},
        "SELECT user, region from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_cpu_pred() {
    let expected = vec![
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    run_sql_test_case(
        TwoMeasurements {},
        "SELECT * from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00')",
        &expected,
    )
    .await;
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
    run_sql_test_case(
        TwoMeasurements {},
        "SELECT user, region from cpu where time > to_timestamp('1970-01-01T00:00:00.000000120+00:00')",
        &expected
    ).await;
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
    run_sql_test_case(
        TwoMeasurements {},
        "SELECT count(*) from cpu group by region",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_disk() {
    let expected = vec![
        "+-------+--------+--------------------------------+",
        "| bytes | region | time                           |",
        "+-------+--------+--------------------------------+",
        "| 99    | east   | 1970-01-01T00:00:00.000000200Z |",
        "+-------+--------+--------------------------------+",
    ];
    run_sql_test_case(TwoMeasurements {}, "SELECT * from disk", &expected).await;
}

#[tokio::test]
async fn sql_select_with_schema_merge() {
    let expected = vec![
        "+------+--------+--------+--------------------------------+------+",
        "| host | region | system | time                           | user |",
        "+------+--------+--------+--------------------------------+------+",
        "|      | west   | 5      | 1970-01-01T00:00:00.000000100Z | 23.2 |",
        "|      | west   | 6      | 1970-01-01T00:00:00.000000150Z | 21   |",
        "| bar  | west   |        | 1970-01-01T00:00:00.000000250Z | 21   |",
        "| foo  | east   |        | 1970-01-01T00:00:00.000000100Z | 23.2 |",
        "+------+--------+--------+--------------------------------+------+",
    ];
    run_sql_test_case(MultiChunkSchemaMerge {}, "SELECT * from cpu", &expected).await;
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
    run_sql_test_case(
        TwoMeasurementsUnsignedType {},
        "SELECT town, count from restaurant",
        &expected,
    )
    .await;
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
    run_sql_test_case(
        TwoMeasurementsUnsignedType {},
        "SELECT town, count from school",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_information_schema_tables() {
    // validate we have access to information schema for listing table
    // names
    let expected = vec![
        "+---------------+--------------------+---------------------+------------+",
        "| table_catalog | table_schema       | table_name          | table_type |",
        "+---------------+--------------------+---------------------+------------+",
        "| public        | information_schema | columns             | VIEW       |",
        "| public        | information_schema | tables              | VIEW       |",
        "| public        | iox                | h2o                 | BASE TABLE |",
        "| public        | iox                | o2                  | BASE TABLE |",
        "| public        | system             | chunk_columns       | BASE TABLE |",
        "| public        | system             | chunks              | BASE TABLE |",
        "| public        | system             | columns             | BASE TABLE |",
        "| public        | system             | operations          | BASE TABLE |",
        "| public        | system             | persistence_windows | BASE TABLE |",
        "+---------------+--------------------+---------------------+------------+",
    ];
    run_sql_test_case(
        TwoMeasurementsManyFields {},
        "SELECT * from information_schema.tables",
        &expected,
    )
    .await;
    run_sql_test_case(TwoMeasurementsManyFields {}, "SHOW TABLES", &expected).await;
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
    run_sql_test_case(
        TwoMeasurementsManyFields {},
        "SELECT * from information_schema.columns where table_name = 'h2o' OR table_name = 'o2'",
        &expected,
    )
    .await;
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
    run_sql_test_case(
        TwoMeasurementsManyFields {},
        "SHOW COLUMNS FROM h2o",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_system_chunks() {
    // system tables reflect the state of chunks, so don't run them
    // with different chunk configurations.

    //  ensures the tables / plumbing are hooked up (so no need to
    //  test timestamps, etc)

    let expected = vec![
        "+----+---------------+------------+-------------------+--------------+-----------+",
        "| id | partition_key | table_name | storage           | memory_bytes | row_count |",
        "+----+---------------+------------+-------------------+--------------+-----------+",
        "| 0  | 1970-01-01T00 | h2o        | OpenMutableBuffer | 1639         | 3         |",
        "| 0  | 1970-01-01T00 | o2         | OpenMutableBuffer | 1635         | 2         |",
        "+----+---------------+------------+-------------------+--------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsManyFieldsOneChunk {},
        "SELECT id, partition_key, table_name, storage, memory_bytes, row_count from system.chunks",
        &expected,
    )
    .await;
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
    run_sql_test_case(
        TwoMeasurementsManyFieldsOneChunk {},
        "SELECT * from system.columns",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_system_chunk_columns() {
    // system tables reflect the state of chunks, so don't run them
    // with different chunk configurations.

    let expected = vec![
        "+---------------+------------+-------------+-------------------+-----------+------------+-----------+-----------+--------------+",
        "| partition_key | table_name | column_name | storage           | row_count | null_count | min_value | max_value | memory_bytes |",
        "+---------------+------------+-------------+-------------------+-----------+------------+-----------+-----------+--------------+",
        "| 1970-01-01T00 | h2o        | city        | OpenMutableBuffer | 1         | 0          | Boston    | Boston    | 309          |",
        "| 1970-01-01T00 | h2o        | city        | ReadBuffer        | 2         | 0          | Boston    | Boston    | 359          |",
        "| 1970-01-01T00 | h2o        | other_temp  | OpenMutableBuffer | 1         | 0          | 72.4      | 72.4      | 297          |",
        "| 1970-01-01T00 | h2o        | other_temp  | ReadBuffer        | 2         | 1          | 70.4      | 70.4      | 471          |",
        "| 1970-01-01T00 | h2o        | state       | OpenMutableBuffer | 1         | 0          | CA        | CA        | 309          |",
        "| 1970-01-01T00 | h2o        | state       | ReadBuffer        | 2         | 0          | MA        | MA        | 347          |",
        "| 1970-01-01T00 | h2o        | temp        | ReadBuffer        | 2         | 1          | 70.4      | 70.4      | 471          |",
        "| 1970-01-01T00 | h2o        | time        | OpenMutableBuffer | 1         | 0          | 350       | 350       | 297          |",
        "| 1970-01-01T00 | h2o        | time        | ReadBuffer        | 2         | 0          | 50        | 250       | 110          |",
        "| 1970-01-01T00 | o2         | city        | OpenMutableBuffer | 2         | 1          | Boston    | Boston    | 309          |",
        "| 1970-01-01T00 | o2         | reading     | OpenMutableBuffer | 2         | 1          | 51        | 51        | 297          |",
        "| 1970-01-01T00 | o2         | state       | OpenMutableBuffer | 2         | 0          | CA        | MA        | 313          |",
        "| 1970-01-01T00 | o2         | temp        | OpenMutableBuffer | 2         | 0          | 53.4      | 79        | 297          |",
        "| 1970-01-01T00 | o2         | time        | OpenMutableBuffer | 2         | 0          | 50        | 300       | 297          |",
        "+---------------+------------+-------------+-------------------+-----------+------------+-----------+-----------+--------------+",
    ];
    run_sql_test_case(
        TwoMeasurementsManyFieldsTwoChunks {},
        "SELECT partition_key, table_name, column_name, storage, row_count, null_count, min_value, max_value, memory_bytes from system.chunk_columns",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_from_system_operations() {
    test_helpers::maybe_start_logging();
    let expected = vec![
        "+----+---------+------------+---------------+----------------+------------+---------------+-------------------------------------+",
        "| id | status  | start_time | took_cpu_time | took_wall_time | table_name | partition_key | description                         |",
        "+----+---------+------------+---------------+----------------+------------+---------------+-------------------------------------+",
        "| 0  | Success | true       | true          | true           | h2o        | 1970-01-01T00 | Compacting chunks to ReadBuffer     |",
        "| 1  | Success | true       | true          | true           | h2o        | 1970-01-01T00 | Persisting chunks to object storage |",
        "| 2  | Success | true       | true          | true           | h2o        | 1970-01-01T00 | Writing chunk to Object Storage     |",
        "+----+---------+------------+---------------+----------------+------------+---------------+-------------------------------------+",
    ];

    // Check that the cpu time used reported is greater than zero as it isn't
    // repeatable
    run_sql_test_case(
        TwoMeasurementsManyFieldsLifecycle {},
        "SELECT id, status, CAST(start_time as BIGINT) > 0 as start_time, CAST(cpu_time_used AS BIGINT) > 0 as took_cpu_time, CAST(wall_time_used AS BIGINT) > 0 as took_wall_time, table_name, partition_key, description from system.operations",
        &expected
    ).await;
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
    run_sql_test_case(
        TwoMeasurementsManyFields {},
        "select state as name from h2o UNION ALL select city as name from h2o",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_distinct_aggregates() {
    // validate distinct aggregates work against dictionary columns
    // which have nulls in them
    let expected = vec![
        "+-------------------------+",
        "| COUNT(DISTINCT o2.city) |",
        "+-------------------------+",
        "| 2                       |",
        "+-------------------------+",
    ];
    run_sql_test_case(
        TwoMeasurementsManyNulls {},
        "select count(distinct city) from o2",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_aggregate_on_tags() {
    // validate aggregates work on dictionary columns
    // which have nulls in them
    let expected = vec![
        "+-----------------+--------+",
        "| COUNT(UInt8(1)) | city   |",
        "+-----------------+--------+",
        "| 1               | Boston |",
        "| 2               |        |",
        "| 2               | NYC    |",
        "+-----------------+--------+",
    ];
    run_sql_test_case(
        TwoMeasurementsManyNulls {},
        "select count(*), city from o2 group by city",
        &expected,
    )
    .await;
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
    run_sql_test_case(
        MultiChunkSchemaMerge {},
        "SELECT host, region, system from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_1() {
    // Test 1: Select everything
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 189   | 7      | 1970-01-01T00:00:00.000000110Z | bedford   |",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 40000 | 5      | 1970-01-01T00:00:00.000000100Z | andover   |",
        "| 471   | 6      | 1970-01-01T00:00:00.000000110Z | tewsbury  |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_2() {
    // Test 2: One push-down expression: count > 200
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 40000 | 5      | 1970-01-01T00:00:00.000000100Z | andover   |",
        "| 471   | 6      | 1970-01-01T00:00:00.000000110Z | tewsbury  |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_3() {
    // Test 3: Two push-down expression: count > 200 and town != 'tewsbury'
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 40000 | 5      | 1970-01-01T00:00:00.000000100Z | andover   |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200 and town != 'tewsbury'",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_4() {
    // Test 4: Still two push-down expression: count > 200 and town != 'tewsbury'
    // even though the results are different
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 40000 | 5      | 1970-01-01T00:00:00.000000100Z | andover   |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence')",
        &expected
    ).await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_5() {
    // Test 5: three push-down expression: count > 200 and town != 'tewsbury' and count < 40000
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence') and count < 40000",
        &expected
    ).await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_6() {
    // Test 6: two push-down expression: count > 200 and count < 40000
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 471   | 6      | 1970-01-01T00:00:00.000000110Z | tewsbury  |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 200  and count < 40000",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_7() {
    // Test 7: two push-down expression on float: system > 4.0 and system < 7.0
    let expected = vec![
        "+-------+--------+--------------------------------+-----------+",
        "| count | system | time                           | town      |",
        "+-------+--------+--------------------------------+-----------+",
        "| 372   | 5      | 1970-01-01T00:00:00.000000100Z | lexington |",
        "| 40000 | 5      | 1970-01-01T00:00:00.000000100Z | andover   |",
        "| 471   | 6      | 1970-01-01T00:00:00.000000110Z | tewsbury  |",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading   |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading   |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence  |",
        "+-------+--------+--------------------------------+-----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 4.0 and system < 7.0",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_8() {
    // Test 8: two push-down expression on float: system > 5.0 and system < 7.0
    let expected = vec![
        "+-------+--------+--------------------------------+----------+",
        "| count | system | time                           | town     |",
        "+-------+--------+--------------------------------+----------+",
        "| 471   | 6      | 1970-01-01T00:00:00.000000110Z | tewsbury |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading  |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence |",
        "+-------+--------+--------------------------------+----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and system < 7.0",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_9() {
    // Test 9: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
    let expected = vec![
        "+-------+--------+--------------------------------+----------+",
        "| count | system | time                           | town     |",
        "+-------+--------+--------------------------------+----------+",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading  |",
        "| 872   | 6      | 1970-01-01T00:00:00.000000110Z | lawrence |",
        "+-------+--------+--------------------------------+----------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and town != 'tewsbury' and 7.0 > system",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_10() {
    // Test 10: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
    // even though there are more expressions,(count = 632 or town = 'reading'), in the filter
    let expected = vec![
        "+-------+--------+--------------------------------+---------+",
        "| count | system | time                           | town    |",
        "+-------+--------+--------------------------------+---------+",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading |",
        "+-------+--------+--------------------------------+---------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and (count = 632 or town = 'reading')",
        &expected
    ).await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_11() {
    // Test 11: four push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0 and
    // time > to_timestamp('1970-01-01T00:00:00.000000120+00:00') (rewritten to time GT int(130))
    //
    let expected = vec!["++", "++"];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where 5.0 < system and town != 'tewsbury' and system < 7.0 and (count = 632 or town = 'reading') and time > to_timestamp('1970-01-01T00:00:00.000000130+00:00')",
        &expected
    ).await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_12() {
    // Test 12: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0 and town = 'reading'
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+--------------------------------+---------+",
        "| count | system | time                           | town    |",
        "+-------+--------+--------------------------------+---------+",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading |",
        "+-------+--------+--------------------------------+---------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and town = 'reading'",
        &expected
    ).await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_13() {
    // Test 13: three push-down expression: system > 5.0 and system < 7.0 and town = 'reading'
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+--------------------------------+---------+",
        "| count | system | time                           | town    |",
        "+-------+--------+--------------------------------+---------+",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading |",
        "+-------+--------+--------------------------------+---------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where system > 5.0 and system < 7.0 and town = 'reading'",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_predicate_pushdown_correctness_14() {
    // Test 14: on push-down expression with a literal type different from the
    // column type.
    //
    // Check correctness
    let expected = vec![
        "+-------+--------+--------------------------------+---------+",
        "| count | system | time                           | town    |",
        "+-------+--------+--------------------------------+---------+",
        "| 632   | 5      | 1970-01-01T00:00:00.000000120Z | reading |",
        "| 632   | 6      | 1970-01-01T00:00:00.000000130Z | reading |",
        "+-------+--------+--------------------------------+---------+",
    ];
    run_sql_test_case(
        TwoMeasurementsPredicatePushDown {},
        "SELECT * from restaurant where count > 500.76 and count < 640.0",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_deduplicate_1() {
    // This current expected is wrong because deduplicate is not available yet
    let sql =
        "select time, state, city, min_temp, max_temp, area from h2o order by time, state, city";
    let expected = vec![
        "+--------------------------------+-------+---------+----------+----------+------+",
        "| time                           | state | city    | min_temp | max_temp | area |",
        "+--------------------------------+-------+---------+----------+----------+------+",
        "| 1970-01-01T00:00:00.000000050Z | MA    | Boston  | 70.4     |          |      |",
        "| 1970-01-01T00:00:00.000000150Z | MA    | Bedford | 71.59    | 78.75    | 742  |",
        "| 1970-01-01T00:00:00.000000250Z | MA    | Andover |          | 69.2     |      |",
        "| 1970-01-01T00:00:00.000000250Z | MA    | Boston  | 65.4     | 75.4     |      |",
        "| 1970-01-01T00:00:00.000000250Z | MA    | Reading | 53.4     |          |      |",
        "| 1970-01-01T00:00:00.000000300Z | CA    | SF      | 79       | 87.2     | 500  |",
        "| 1970-01-01T00:00:00.000000300Z | CA    | SJ      | 78.5     | 88       |      |",
        "| 1970-01-01T00:00:00.000000350Z | CA    | SJ      | 75.5     | 84.08    |      |",
        "| 1970-01-01T00:00:00.000000400Z | MA    | Bedford | 65.22    | 80.75    | 750  |",
        "| 1970-01-01T00:00:00.000000400Z | MA    | Boston  | 65.4     | 82.67    |      |",
        "| 1970-01-01T00:00:00.000000450Z | CA    | SJ      | 77       | 90.7     |      |",
        "| 1970-01-01T00:00:00.000000500Z | CA    | SJ      | 69.5     | 88.2     |      |",
        "| 1970-01-01T00:00:00.000000600Z | MA    | Bedford |          | 88.75    | 742  |",
        "| 1970-01-01T00:00:00.000000600Z | MA    | Boston  | 67.4     |          |      |",
        "| 1970-01-01T00:00:00.000000600Z | MA    | Reading | 60.4     |          |      |",
        "| 1970-01-01T00:00:00.000000650Z | CA    | SF      | 68.4     | 85.7     | 500  |",
        "| 1970-01-01T00:00:00.000000650Z | CA    | SJ      | 69.5     | 89.2     |      |",
        "| 1970-01-01T00:00:00.000000700Z | CA    | SJ      | 75.5     | 84.08    |      |",
        "+--------------------------------+-------+---------+----------+----------+------+",
    ];
    run_sql_test_case(OneMeasurementFourChunksWithDuplicates {}, sql, &expected).await;
}

#[tokio::test]
async fn sql_select_non_keys() {
    let expected = vec![
        "+------+", "| temp |", "+------+", "|      |", "|      |", "| 53.4 |", "| 70.4 |",
        "+------+",
    ];
    run_sql_test_case(
        OneMeasurementTwoChunksDifferentTagSet {},
        "SELECT temp from h2o",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_all_different_tags_chunks() {
    let expected = vec![
        "+--------+------------+---------+-------+------+--------------------------------+",
        "| city   | other_temp | reading | state | temp | time                           |",
        "+--------+------------+---------+-------+------+--------------------------------+",
        "|        |            |         | MA    | 70.4 | 1970-01-01T00:00:00.000000050Z |",
        "|        | 70.4       |         | MA    |      | 1970-01-01T00:00:00.000000250Z |",
        "| Boston |            | 51      |       | 53.4 | 1970-01-01T00:00:00.000000050Z |",
        "| Boston | 72.4       |         |       |      | 1970-01-01T00:00:00.000000350Z |",
        "+--------+------------+---------+-------+------+--------------------------------+",
    ];
    run_sql_test_case(
        OneMeasurementTwoChunksDifferentTagSet {},
        "SELECT * from h2o",
        &expected,
    )
    .await;
}

// ----------------------------------------------
// Delete tests
#[tokio::test]
async fn sql_select_with_delete_from_one_expr_delete_all() {
    // select *
    let expected = vec!["++", "++"];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunkDeleteAll {},
        "SELECT * from cpu",
        &expected,
    )
    .await;

    // select a specific column
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunkDeleteAll {},
        "SELECT time from cpu",
        &expected,
    )
    .await;

    // Count
    let expected = vec![
        "+-----------------+----------------+-----------------+",
        "| COUNT(UInt8(1)) | COUNT(cpu.bar) | COUNT(cpu.time) |",
        "+-----------------+----------------+-----------------+",
        "| 0               | 0              | 0               |",
        "+-----------------+----------------+-----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunkDeleteAll {},
        "SELECT count(*), count(bar), count(time) from cpu",
        &expected,
    )
    .await;

    // Min & Max
    let expected = vec![
        "+--------------+--------------+---------------+---------------+",
        "| MIN(cpu.bar) | MAX(cpu.bar) | MIN(cpu.time) | MAX(cpu.time) |",
        "+--------------+--------------+---------------+---------------+",
        "|              |              |               |               |",
        "+--------------+--------------+---------------+---------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunkDeleteAll {},
        "SELECT min(bar), max(bar), min(time), max(time) from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_delete_from_one_expr() {
    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 2   | 1970-01-01T00:00:00.000000020Z |",
        "+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT * from cpu",
        &expected,
    )
    .await;

    // select all explicit columns
    let expected = vec![
        "+--------------------------------+-----+",
        "| time                           | bar |",
        "+--------------------------------+-----+",
        "| 1970-01-01T00:00:00.000000020Z | 2   |",
        "+--------------------------------+-----+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT time, bar from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_delete_from_one_expr_min_max() {
    // Min & Max of bar only
    let expected = vec![
        "+--------------+--------------+",
        "| MIN(cpu.bar) | MAX(cpu.bar) |",
        "+--------------+--------------+",
        "| 2            | 2            |",
        "+--------------+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT min(bar), max(bar) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_one_expr_time() {
    // one column, time,  and no cover all 2 columns, time and bar, of the delete predicate
    let expected = vec![
        "+--------------------------------+",
        "| time                           |",
        "+--------------------------------+",
        "| 1970-01-01T00:00:00.000000020Z |",
        "+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT time from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_one_expr_max_time() {
    // max on one column and no cover all 2 columns, time and bar, of the delete predicate
    let expected = vec![
        "+--------------------------------+",
        "| MAX(cpu.time)                  |",
        "+--------------------------------+",
        "| 1970-01-01T00:00:00.000000020Z |",
        "+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT max(time)  from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_one_expr_count_max_time() {
    // Count and max on one column and no cover all 2 columns, time and bar, of the delete predicate
    let expected = vec![
        "+-----------------+--------------------------------+",
        "| COUNT(cpu.time) | MAX(cpu.time)                  |",
        "+-----------------+--------------------------------+",
        "| 1               | 1970-01-01T00:00:00.000000020Z |",
        "+-----------------+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT count(time), max(time)  from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_one_expr_count_time() {
    // Count and max on one column and no cover all 2 columns, time and bar, of the delete predicate
    let expected = vec![
        "+-----------------+",
        "| COUNT(cpu.time) |",
        "+-----------------+",
        "| 1               |",
        "+-----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT count(time)  from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_delete_from_one_expr_with_select_predicate() {
    // Select predicate does not eliminate extra rows
    let expected = vec![
        "+-----+--------------------------------+",
        "| bar | time                           |",
        "+-----+--------------------------------+",
        "| 2   | 1970-01-01T00:00:00.000000020Z |",
        "+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT * from cpu where bar = 2.0",
        &expected,
    )
    .await;

    // Select predicate eliminates rows
    let expected = vec!["++", "++"];

    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT * from cpu where bar != 2.0",
        &expected,
    )
    .await;

    // Count, min and max
    let expected = vec![
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
        "| COUNT(cpu.time) | COUNT(UInt8(1)) | COUNT(cpu.bar) | MIN(cpu.bar) | MAX(cpu.bar) | MIN(cpu.time)                  | MAX(cpu.time)                  |",
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
        "| 1               | 1               | 1              | 2            | 2            | 1970-01-01T00:00:00.000000020Z | 1970-01-01T00:00:00.000000020Z |",
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteSimpleExprOneChunk {},
        "SELECT count(time), count(*), count(bar), min(bar), max(bar), min(time), max(time)  from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs() {
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "| 2   | you | 1970-01-01T00:00:00.000000020Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT * from cpu",
        &expected,
    )
    .await;

    // select one column, delete predicates on all 3 columns of the table
    let expected = vec![
        "+-----+", "| bar |", "+-----+", "| 1   |", "| 2   |", "+-----+",
    ];

    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT bar from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

// tests without delete
#[tokio::test]
async fn sql_select_without_delete_agg() {
    // Count, min and max on many columns but not `foo` that is included in delete predicate
    let expected = vec![
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
        "| COUNT(cpu.time) | COUNT(UInt8(1)) | COUNT(cpu.bar) | MIN(cpu.bar) | MAX(cpu.bar) | MIN(cpu.time)                  | MAX(cpu.time)                  |",
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
        "| 4               | 4               | 4              | 1            | 2            | 1970-01-01T00:00:00.000000010Z | 1970-01-01T00:00:00.000000040Z |",
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "SELECT count(time), count(*), count(bar), min(bar), max(bar), min(time), max(time)  from cpu",
        &expected,
    )
    .await;
}

// BUG: https://github.com/influxdata/influxdb_iox/issues/2776
#[ignore]
#[tokio::test]
async fn sql_select_without_delete_max_foo() {
    let expected = vec![
        "+--------------+",
        "| MAX(cpu.foo) |",
        "+--------------+",
        "| you           |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "SELECT max(foo) from cpu",
        &expected,
    )
    .await;
}

// BUG: as as above
#[ignore]
#[tokio::test]
async fn sql_select_without_delete_min_foo() {
    let expected = vec![
        "+--------------+",
        "| MIN(cpu.foo)  |",
        "+--------------+",
        "| me            |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "SELECT min(foo) from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_agg() {
    // Count, min and max on many columns but not `foo` that is included in delete predicate
    let expected = vec![
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
        "| COUNT(cpu.time) | COUNT(UInt8(1)) | COUNT(cpu.bar) | MIN(cpu.bar) | MAX(cpu.bar) | MIN(cpu.time)                  | MAX(cpu.time)                  |",
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
        "| 2               | 2               | 2              | 1            | 2            | 1970-01-01T00:00:00.000000020Z | 1970-01-01T00:00:00.000000040Z |",
        "+-----------------+-----------------+----------------+--------------+--------------+--------------------------------+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT count(time), count(*), count(bar), min(bar), max(bar), min(time), max(time)  from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_count_time() {
    // Count, min and max on many columns but not `foo` that is included in delete predicate
    let expected = vec![
        "+-----------------+",
        "| COUNT(cpu.time) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT count(time)  from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_count_foo() {
    let expected = vec![
        "+----------------+",
        "| COUNT(cpu.foo) |",
        "+----------------+",
        "| 2              |",
        "+----------------+",
    ];

    // OneDeleteMultiExprsOneChunk's delete predicates include columns foo, bar and time
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT count(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_count_bar() {
    let expected = vec![
        "+----------------+",
        "| COUNT(cpu.bar) |",
        "+----------------+",
        "| 2              |",
        "+----------------+",
    ];

    // OneDeleteMultiExprsOneChunk's delete predicates include columns foo, bar and time
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT count(bar) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_count_star() {
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT count(*) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_min_bar() {
    let expected = vec![
        "+--------------+",
        "| MIN(cpu.bar) |",
        "+--------------+",
        "| 1            |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT min(bar) from cpu",
        &expected,
    )
    .await;
}

// BUG: https://github.com/influxdata/influxdb_iox/issues/2776
#[ignore]
#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_min_foo() {
    let expected = vec![
        "+--------------+",
        "| MIN(cpu.foo) |",
        "+--------------+",
        "| me           |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT min(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_foo() {
    let expected = vec![
        "+-----+", "| foo |", "+-----+", "| me  |", "| you |", "+-----+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT foo from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_min_time() {
    let expected = vec![
        "+--------------------------------+",
        "| MIN(cpu.time)                  |",
        "+--------------------------------+",
        "| 1970-01-01T00:00:00.000000020Z |",
        "+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT min(time) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_max_bar() {
    let expected = vec![
        "+--------------+",
        "| MAX(cpu.bar) |",
        "+--------------+",
        "| 2            |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT max(bar) from cpu",
        &expected,
    )
    .await;
}

// BUG: https://github.com/influxdata/influxdb_iox/issues/2776
#[ignore]
#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_max_foo() {
    let expected = vec![
        "+----------------+",
        "| COUNT(cpu.foo) |",
        "+----------------+",
        "| 2              |",
        "+----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT max(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_max_time() {
    let expected = vec![
        "+--------------------------------+",
        "| MAX(cpu.time)                  |",
        "+--------------------------------+",
        "| 1970-01-01T00:00:00.000000040Z |",
        "+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT max(time) from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_delete_from_multi_exprs_with_select_predicate() {
    // not eliminate extra row
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "| 2   | you | 1970-01-01T00:00:00.000000020Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT * from cpu where bar >= 1.0",
        &expected,
    )
    .await;

    // eliminate something
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 2   | you | 1970-01-01T00:00:00.000000020Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT * from cpu where foo = 'you'",
        &expected,
    )
    .await;

    // eliminate all
    let expected = vec!["++", "++"];
    run_sql_test_case(
        scenarios::delete::OneDeleteMultiExprsOneChunk {},
        "SELECT * from cpu where foo = 'you' and bar > 2.0",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_two_deletes_from_multi_exprs() {
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::TwoDeletesMultiExprsOneChunk {},
        "SELECT * from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_two_deletes_from_multi_exprs_with_select_predicate() {
    // no extra eliminated
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::TwoDeletesMultiExprsOneChunk {},
        "SELECT * from cpu where cast(time as bigint) > 30",
        &expected,
    )
    .await;

    // all eliminated
    let expected = vec!["++", "++"];

    run_sql_test_case(
        scenarios::delete::TwoDeletesMultiExprsOneChunk {},
        "SELECT * from cpu where cast(time as bigint) > 40",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks() {
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "| 1   | me  | 1970-01-01T00:00:00.000000042Z |",
        "| 1   | me  | 1970-01-01T00:00:00.000000062Z |",
        "| 3   | you | 1970-01-01T00:00:00.000000070Z |",
        "| 4   | me  | 1970-01-01T00:00:00.000000050Z |",
        "| 5   | me  | 1970-01-01T00:00:00.000000060Z |",
        "| 7   | me  | 1970-01-01T00:00:00.000000080Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT * from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_count_star() {
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 7               |",
        "+-----------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(*) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_count_time() {
    let expected = vec![
        "+-----------------+",
        "| COUNT(cpu.time) |",
        "+-----------------+",
        "| 7               |",
        "+-----------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(time) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_count_foo() {
    let expected = vec![
        "+----------------+",
        "| COUNT(cpu.foo) |",
        "+----------------+",
        "| 7              |",
        "+----------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_count_bar() {
    let expected = vec![
        "+----------------+",
        "| COUNT(cpu.bar) |",
        "+----------------+",
        "| 7              |",
        "+----------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(bar) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_min_bar() {
    let expected = vec![
        "+--------------+",
        "| MIN(cpu.bar) |",
        "+--------------+",
        "| 1            |",
        "+--------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT min(bar) from cpu",
        &expected,
    )
    .await;
}

// BUG: https://github.com/influxdata/influxdb_iox/issues/2776
#[ignore]
#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_max_foo() {
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 7   | me  | 1970-01-01T00:00:00.000000080Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT max(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_min_max_time() {
    let expected = vec![
        "+--------------------------------+--------------------------------+",
        "| MIN(cpu.time)                  | MAX(cpu.time)                  |",
        "+--------------------------------+--------------------------------+",
        "| 1970-01-01T00:00:00.000000040Z | 1970-01-01T00:00:00.000000080Z |",
        "+--------------------------------+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT min(time), max(time) from cpu",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate() {
    test_helpers::maybe_start_logging();

    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 3   | you | 1970-01-01T00:00:00.000000070Z |",
        "| 4   | me  | 1970-01-01T00:00:00.000000050Z |",
        "| 5   | me  | 1970-01-01T00:00:00.000000060Z |",
        "| 7   | me  | 1970-01-01T00:00:00.000000080Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT * from cpu where bar != 1.0",
        &expected,
    )
    .await;

    //--------------------

    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 4   | me  | 1970-01-01T00:00:00.000000050Z |",
        "| 5   | me  | 1970-01-01T00:00:00.000000060Z |",
        "| 7   | me  | 1970-01-01T00:00:00.000000080Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT * from cpu where foo = 'me' and bar > 2.0",
        &expected,
    )
    .await;

    //--------------------

    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "| 1   | me  | 1970-01-01T00:00:00.000000042Z |",
        "| 1   | me  | 1970-01-01T00:00:00.000000062Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT * from cpu where bar = 1",
        &expected,
    )
    .await;

    //--------------------
    // Multi select expressions and eliminate something
    let expected = vec![
        "+-----+-----+--------------------------------+",
        "| bar | foo | time                           |",
        "+-----+-----+--------------------------------+",
        "| 1   | me  | 1970-01-01T00:00:00.000000040Z |",
        "| 1   | me  | 1970-01-01T00:00:00.000000042Z |",
        "| 1   | me  | 1970-01-01T00:00:00.000000062Z |",
        "| 4   | me  | 1970-01-01T00:00:00.000000050Z |",
        "| 5   | me  | 1970-01-01T00:00:00.000000060Z |",
        "| 7   | me  | 1970-01-01T00:00:00.000000080Z |",
        "+-----+-----+--------------------------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT * from cpu where foo = 'me' and (bar > 2 or bar = 1.0)",
        &expected,
    )
    .await;

    //--------------------
    // Multi select expressions and eliminate everything
    let expected = vec!["++", "++"];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT * from cpu where foo = 'you' and (bar > 3.0 or bar = 1)",
        &expected,
    )
    .await;
}

// --------------------------------------------------------

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate_min_bar() {
    let expected = vec![
        "+--------------+",
        "| MIN(cpu.bar) |",
        "+--------------+",
        "| 1            |",
        "+--------------+",
    ];

    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT min(bar) from cpu where foo = 'me' and (bar > 2 or bar = 1.0)",
        &expected,
    )
    .await;
}

// BUG: https://github.com/influxdata/influxdb_iox/issues/2776
#[ignore]
#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate_max_foo() {
    let expected = vec![
        "+--------------+",
        "| MAX(cpu.foo) |",
        "+--------------+",
        "| me           |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT max(foo) from cpu where foo = 'me' and (bar > 2 or bar = 1.0)",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate_min_time() {
    let expected = vec![
        "+--------------------------------+",
        "| MIN(cpu.time)                  |",
        "+--------------------------------+",
        "| 1970-01-01T00:00:00.000000040Z |",
        "+--------------------------------+",
    ];
    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT min(time) from cpu where foo = 'me' and (bar > 2 or bar = 1.0)",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate_count_bar() {
    let expected = vec![
        "+----------------+",
        "| COUNT(cpu.bar) |",
        "+----------------+",
        "| 6              |",
        "+----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(bar) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate_count_time() {
    let expected = vec![
        "+-----------------+",
        "| COUNT(cpu.time) |",
        "+-----------------+",
        "| 6               |",
        "+-----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(time) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_with_three_deletes_from_three_chunks_with_select_predicate_count_star() {
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 6               |",
        "+-----------------+",
    ];
    run_sql_test_case(
        scenarios::delete::ThreeDeleteThreeChunks {},
        "SELECT count(*) from cpu where foo = 'me' and (bar > 2 or bar = 1.0);",
        &expected,
    )
    .await;
}
