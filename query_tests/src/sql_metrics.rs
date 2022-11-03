use std::sync::Arc;

use crate::scenarios::{DbScenario, DbSetup, OneMeasurementFourChunksWithDuplicatesParquetOnly};
use arrow::record_batch::RecordBatch;
use arrow_util::assert_batches_sorted_eq;
use datafusion::physical_plan::{
    display::DisplayableExecutionPlan,
    metrics::{MetricValue, MetricsSet},
};
use iox_query::{frontend::sql::SqlQueryPlanner, provider::parquet_metrics};

#[tokio::test]
async fn sql_predicate_pushdown() {
    test_helpers::maybe_start_logging();

    // parquet pushdown is only relevant for parquet
    let db_setup = OneMeasurementFourChunksWithDuplicatesParquetOnly {};

    // This predicate should result in rows being pruned, and we verify this with metrics
    let sql = "SELECT * from h2o where state = 'MA'".to_string();

    let expected = vec![
        "+------+---------+----------+----------+-------+--------------------------------+",
        "| area | city    | max_temp | min_temp | state | time                           |",
        "+------+---------+----------+----------+-------+--------------------------------+",
        "|      | Andover | 69.2     |          | MA    | 1970-01-01T00:00:00.000000250Z |",
        "|      | Boston  |          | 67.4     | MA    | 1970-01-01T00:00:00.000000600Z |",
        "|      | Boston  |          | 70.4     | MA    | 1970-01-01T00:00:00.000000050Z |",
        "|      | Boston  | 75.4     | 65.4     | MA    | 1970-01-01T00:00:00.000000250Z |",
        "|      | Boston  | 82.67    | 65.4     | MA    | 1970-01-01T00:00:00.000000400Z |",
        "|      | Reading |          | 53.4     | MA    | 1970-01-01T00:00:00.000000250Z |",
        "|      | Reading |          | 60.4     | MA    | 1970-01-01T00:00:00.000000600Z |",
        "| 742  | Bedford | 78.75    | 71.59    | MA    | 1970-01-01T00:00:00.000000150Z |",
        "| 742  | Bedford | 88.75    |          | MA    | 1970-01-01T00:00:00.000000600Z |",
        "| 750  | Bedford | 80.75    | 65.22    | MA    | 1970-01-01T00:00:00.000000400Z |",
        "+------+---------+----------+----------+-------+--------------------------------+",
    ];

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

        let results: Vec<RecordBatch> = ctx
            .collect(Arc::clone(&physical_plan))
            .await
            .expect("Running plan");
        assert_batches_sorted_eq!(expected, &results);

        println!(
            "Physical plan:\n\n{}",
            DisplayableExecutionPlan::new(physical_plan.as_ref()).indent()
        );

        // verify that pushdown was enabled and that it filtered rows
        let metrics = parquet_metrics(physical_plan);
        assert_eq!(
            metric_value_sum(&metrics, "pushdown_rows_filtered"),
            8,
            "Unexpected number of rows filtered in:\n\n{:#?}",
            metrics
        );
    }
}

/// returns the sum of all the metrics with the specified name
/// the returned set.
///
/// Count: returns value
///
/// Panics if no such metric.
fn metric_value_sum(metrics: &[MetricsSet], metric_name: &str) -> usize {
    metrics.iter().map(|m| metric_value(m, metric_name)).sum()
}

fn metric_value(metrics: &MetricsSet, metric_name: &str) -> usize {
    let sum = metrics
        .sum(|m| matches!(m.value(), MetricValue::Count { name, .. } if name == metric_name));

    match sum {
        Some(MetricValue::Count { count, .. }) => count.value(),
        _ => {
            panic!(
                "Expected metric not found. Looking for '{}' in\n\n{:#?}",
                metric_name, metrics
            );
        }
    }
}
