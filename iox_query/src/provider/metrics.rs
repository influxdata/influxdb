use std::sync::Arc;

use datafusion::physical_plan::{file_format::ParquetExec, metrics::MetricsSet, ExecutionPlan};

use super::IOxReadFilterNode;

/// Recursively retrieve metrics from all ParquetExec's in `plan`
pub fn parquet_metrics(plan: Arc<dyn ExecutionPlan>) -> Vec<MetricsSet> {
    let mut output = vec![];
    parquet_metrics_impl(plan, &mut output);
    output
}

fn parquet_metrics_impl(plan: Arc<dyn ExecutionPlan>, output: &mut Vec<MetricsSet>) {
    // Temporarily need to special case `IoxReadFilter` as it
    // may create  `ParquetExec` during execution.
    //
    // This can be removed when
    // <https://github.com/influxdata/influxdb_iox/issues/5897> is
    // completed
    if let Some(iox_read_node) = plan.as_any().downcast_ref::<IOxReadFilterNode>() {
        if let Some(metrics) = iox_read_node.metrics() {
            output.push(metrics)
        }
    }

    if let Some(parquet) = plan.as_any().downcast_ref::<ParquetExec>() {
        if let Some(metrics) = parquet.metrics() {
            output.push(metrics)
        }
    }

    for child in plan.children() {
        parquet_metrics_impl(child, output)
    }
}
