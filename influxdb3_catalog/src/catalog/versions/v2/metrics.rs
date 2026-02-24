use std::sync::Arc;

use metric::{Attributes, Metric, Registry, U64Counter};

use super::log::{
    CatalogBatch, ClearRetentionPeriodLog, DatabaseCatalogOp, DeleteOp, GenerationOp,
    NodeCatalogOp, SetRetentionPeriodLog, TokenCatalogOp,
};
use crate::channel::versions::v2::CatalogUpdateReceiver;

pub(super) const CATALOG_OPERATION_RETRIES_METRIC_NAME: &str =
    "influxdb3_catalog_operation_retries";
const CATALOG_OPERATION_RETRIES_METRIC_DESCRIPTION: &str =
    "catalog updates that had to be retried because the catalog was updated elsewhere";

pub(super) const CATALOG_OPERATIONS_METRIC_NAME: &str = "influxdb3_catalog_operations";
const CATALOG_OPERATIONS_METRIC_DESCRIPTION: &str =
    "counter of different catalog operations by their operation type";

#[derive(Debug)]
pub(super) struct CatalogMetrics {
    pub(super) catalog_operation_retries: U64Counter,
    catalog_operations: OperationMetrics,
}

impl CatalogMetrics {
    pub(super) fn new(metric_registry: &Arc<Registry>) -> Self {
        let retries: Metric<U64Counter> = metric_registry.register_metric(
            CATALOG_OPERATION_RETRIES_METRIC_NAME,
            CATALOG_OPERATION_RETRIES_METRIC_DESCRIPTION,
        );
        let catalog_operation_retries = retries.recorder([]);
        Self {
            catalog_operation_retries,
            catalog_operations: OperationMetrics::new(metric_registry),
        }
    }

    pub(super) fn operation_observer(self: &Arc<Self>, mut recv: CatalogUpdateReceiver) {
        let metrics = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(update) = recv.recv().await {
                for batch in update.batches() {
                    match batch {
                        CatalogBatch::Node(node_batch) => {
                            for op in &node_batch.ops {
                                metrics.catalog_operations.record(op);
                            }
                        }
                        CatalogBatch::Database(database_batch) => {
                            for op in &database_batch.ops {
                                metrics.catalog_operations.record(op);
                            }
                        }
                        CatalogBatch::Token(token_batch) => {
                            for op in &token_batch.ops {
                                metrics.catalog_operations.record(op)
                            }
                        }
                        CatalogBatch::Delete(delete_batch) => {
                            for op in &delete_batch.ops {
                                metrics.catalog_operations.record(op);
                            }
                        }
                        CatalogBatch::Generation(generation_batch) => {
                            for op in &generation_batch.ops {
                                metrics.catalog_operations.record(op);
                            }
                        }
                    }
                }
            }
        });
    }
}

#[derive(Debug)]
struct OperationMetrics {
    operations: Metric<U64Counter>,
}

impl OperationMetrics {
    fn new(registry: &Arc<Registry>) -> Self {
        let operations: Metric<U64Counter> = registry.register_metric(
            CATALOG_OPERATIONS_METRIC_NAME,
            CATALOG_OPERATIONS_METRIC_DESCRIPTION,
        );
        Self { operations }
    }

    pub(super) fn record<O: AsMetricStr>(&self, op: &O) {
        let attributes = Attributes::from(&[("type", op.as_metric_str())]);
        self.operations.recorder(attributes).inc(1);
    }
}

pub(super) trait AsMetricStr {
    fn as_metric_str(&self) -> &'static str;
}

impl AsMetricStr for NodeCatalogOp {
    fn as_metric_str(&self) -> &'static str {
        match self {
            NodeCatalogOp::RegisterNode(_) => "register_node",
            NodeCatalogOp::StopNode(_) => "stop_node",
        }
    }
}

impl AsMetricStr for TokenCatalogOp {
    fn as_metric_str(&self) -> &'static str {
        match self {
            TokenCatalogOp::CreateAdminToken(_) => "create_admin_token",
            TokenCatalogOp::RegenerateAdminToken(_) => "regenerate_admin_token",
            TokenCatalogOp::DeleteToken(_) => "delete_token",
        }
    }
}

impl AsMetricStr for DatabaseCatalogOp {
    fn as_metric_str(&self) -> &'static str {
        match self {
            DatabaseCatalogOp::CreateDatabase(_) => "create_database",
            DatabaseCatalogOp::SoftDeleteDatabase(_) => "soft_delete_database",
            DatabaseCatalogOp::CreateTable(_) => "create_table",
            DatabaseCatalogOp::SoftDeleteTable(_) => "soft_delete_table",
            DatabaseCatalogOp::AddColumns(_) => "add_columns",
            DatabaseCatalogOp::CreateDistinctCache(_) => "create_distinct_cache",
            DatabaseCatalogOp::DeleteDistinctCache(_) => "delete_distinct_cache",
            DatabaseCatalogOp::CreateLastCache(_) => "create_last_cache",
            DatabaseCatalogOp::DeleteLastCache(_) => "delete_last_cache",
            DatabaseCatalogOp::CreateTrigger(_) => "create_trigger",
            DatabaseCatalogOp::DeleteTrigger(_) => "delete_trigger",
            DatabaseCatalogOp::EnableTrigger(_) => "enable_trigger",
            DatabaseCatalogOp::DisableTrigger(_) => "disable_trigger",
            DatabaseCatalogOp::SetRetentionPeriod(SetRetentionPeriodLog { .. }) => {
                "set_retention_period_db"
            }
            DatabaseCatalogOp::ClearRetentionPeriod(ClearRetentionPeriodLog { .. }) => {
                "clear_retention_period_db"
            }
        }
    }
}

impl AsMetricStr for DeleteOp {
    fn as_metric_str(&self) -> &'static str {
        match self {
            DeleteOp::DeleteDatabase(_) => "delete_database",
            DeleteOp::DeleteTable(_, _) => "delete_table",
        }
    }
}

impl AsMetricStr for GenerationOp {
    fn as_metric_str(&self) -> &'static str {
        match self {
            GenerationOp::SetGenerationDuration(_) => "set_generation_duration",
        }
    }
}

#[cfg(test)]
mod tests;
