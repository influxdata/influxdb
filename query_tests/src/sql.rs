//! basic tests of SQL query planning DataFusion contains much more
//! extensive test coverage, this module is meant to show we have
//! wired all the pieces together (as well as ensure any particularly
//! important SQL does not regress)

use crate::scenarios;

use super::scenarios::*;
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use datafusion::error::DataFusionError;
use iox_query::frontend::sql::SqlQueryPlanner;
use test_helpers::assert_contains;

/// Runs the query in `sql` and compares it to the expected output.
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

/// Runs the query in `sql` which is expected to error, and ensures
/// the output contains the expected message.
async fn run_sql_error_test_case<D>(db_setup: D, sql: &str, expected_error: &str)
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

        let result: Result<(), DataFusionError> = async {
            let physical_plan = planner.query(&sql, &ctx).await?;

            ctx.collect(physical_plan).await?;
            Ok(())
        }
        .await;

        let err = result.expect_err("Expected failure to plan");
        assert_contains!(err.to_string(), expected_error);
    }
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
async fn sql_select_with_schema_merge_nonexistent_column() {
    let expected_error = "Schema error: No field named 'foo'";
    run_sql_error_test_case(
        MultiChunkSchemaMerge {},
        "SELECT * from cpu where foo = 8",
        expected_error,
    )
    .await;
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
async fn sql_deduplicate_1() {
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

#[tokio::test]
async fn sql_select_without_delete_max_foo() {
    let expected = vec![
        "+--------------+",
        "| MAX(cpu.foo) |",
        "+--------------+",
        "| you          |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "SELECT max(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_select_without_delete_min_foo() {
    let expected = vec![
        "+--------------+",
        "| MIN(cpu.foo) |",
        "+--------------+",
        "| me           |",
        "+--------------+",
    ];
    run_sql_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "SELECT min(foo) from cpu",
        &expected,
    )
    .await;
}

#[tokio::test]
async fn sql_create_external_table() {
    let expected_error = "Unsupported logical plan: CreateExternalTable";
    // Datafusion supports CREATE EXTERNAL TABLE, but IOx should not (as that would be a security hole)
    run_sql_error_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "CREATE EXTERNAL TABLE foo(ts TIMESTAMP) STORED AS CSV LOCATION '/tmp/foo.csv';",
        expected_error,
    )
    .await;
}

#[tokio::test]
async fn sql_create_schema() {
    let expected_error = "Unsupported logical plan: CreateCatalogSchema";
    // Datafusion supports CREATE SCHEMA, but IOx should not (as that would be a security hole)
    run_sql_error_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "CREATE SCHEMA foo;",
        expected_error,
    )
    .await;
}

#[tokio::test]
async fn bad_selector_num_args() {
    let expected_error = "selector_last requires exactly 2 arguments, got 1";
    run_sql_error_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "select selector_last(time)['bar'] from cpu;",
        expected_error,
    )
    .await;
}

#[tokio::test]
async fn bad_selector_arg_types() {
    let expected_error = "selector_last second argument must be a timestamp, but got Float64";
    run_sql_error_test_case(
        scenarios::delete::NoDeleteOneChunk {},
        "select selector_last(time, bar)['value'] from cpu;",
        expected_error,
    )
    .await;
}

// --------------------------------------------------------
