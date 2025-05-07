use std::sync::Arc;

use metric::{Attributes, Metric, Registry, U64Counter};

use crate::{
    channel::CatalogUpdateReceiver,
    log::{
        CatalogBatch, DatabaseCatalogOp, NodeCatalogOp, TokenCatalogOp,
        versions::v3::{ClearRetentionPeriodLog, SetRetentionPeriodLog},
    },
};

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
            DatabaseCatalogOp::AddFields(_) => "add_fields",
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iox_time::{MockProvider, Time};
    use metric::{Attributes, Metric, Registry, U64Counter};
    use object_store::memory::InMemory;

    use crate::{
        catalog::{Catalog, Prompt, metrics::CATALOG_OPERATIONS_METRIC_NAME},
        log::{FieldDataType, NodeMode},
    };

    use super::CATALOG_OPERATION_RETRIES_METRIC_NAME;

    #[tokio::test]
    async fn test_catalog_retry_metrics() {
        let os = Arc::new(InMemory::new());
        let tp = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        // create two catalogs, both pointing at the same object store, but with their own metric
        // registry/instrument/observer

        // first:
        let m1 = Arc::new(Registry::new());
        let c1 = Catalog::new(
            "node",
            Arc::clone(&os) as _,
            Arc::clone(&tp) as _,
            Arc::clone(&m1),
        )
        .await
        .unwrap();
        let i1 = m1
            .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATION_RETRIES_METRIC_NAME)
            .unwrap();
        let o1 = i1.get_observer(&Attributes::from([])).unwrap();

        // second:
        let m2 = Arc::new(Registry::new());
        let c2 = Catalog::new(
            "node",
            Arc::clone(&os) as _,
            Arc::clone(&tp) as _,
            Arc::clone(&m2),
        )
        .await
        .unwrap();
        let i2 = m2
            .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATION_RETRIES_METRIC_NAME)
            .unwrap();
        let o2 = i2.get_observer(&Attributes::from([])).unwrap();

        // create db on 1, should not require any retries:
        assert_eq!(0, o1.fetch());
        c1.create_database("foo").await.unwrap();
        assert_eq!(0, o1.fetch());

        // create a different db on 2, this will require a retry since the catalog was updated
        // previously on object store, and will succeed...
        assert_eq!(0, o2.fetch());
        c2.create_database("bar").await.unwrap();
        assert_eq!(1, o2.fetch());

        // create the "bar" db on 1 now, this will require a retry since the catalog was updated on
        // object store by 2, but will fail...
        assert_eq!(0, o1.fetch());
        c1.create_database("bar").await.unwrap_err();
        assert_eq!(1, o1.fetch());
    }

    #[test_log::test(tokio::test)]
    async fn test_catalog_operation_counter() {
        let metrics = Arc::new(Registry::new());
        let os = Arc::new(InMemory::new());
        let tp = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Catalog::new(
            "node",
            Arc::clone(&os) as _,
            Arc::clone(&tp) as _,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();
        check_metric_empty(&metrics, "create_database");
        catalog.create_database("foo").await.unwrap();
        check_metric(&metrics, "create_database", 1);
        catalog.create_database("bar").await.unwrap();
        check_metric(&metrics, "create_database", 2);

        let mut txn = catalog.begin("baz").unwrap();
        txn.table_or_create("employees").unwrap();
        txn.column_or_create("employees", "name", FieldDataType::String)
            .unwrap();
        txn.column_or_create("employees", "job_title", FieldDataType::String)
            .unwrap();
        txn.column_or_create("employees", "hire_date", FieldDataType::String)
            .unwrap();
        check_metric_empty(&metrics, "create_table");
        check_metric_empty(&metrics, "add_fields");
        let Prompt::Success(_) = catalog.commit(txn).await.unwrap() else {
            panic!("transaction should commit");
        };
        check_metric(&metrics, "create_database", 3);
        check_metric(&metrics, "create_table", 1);
        check_metric(&metrics, "add_fields", 3);

        check_metric_empty(&metrics, "register_node");
        catalog
            .register_node("node2", 4, vec![NodeMode::Core])
            .await
            .unwrap();
        check_metric(&metrics, "register_node", 1);

        check_metric_empty(&metrics, "create_admin_token");
        catalog.create_admin_token(false).await.unwrap();
        check_metric(&metrics, "create_admin_token", 1);
    }

    fn check_metric_empty(registry: &Arc<Registry>, operation_type: &'static str) {
        let instrument = registry
            .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATIONS_METRIC_NAME)
            .unwrap();
        assert!(
            instrument
                .get_observer(&Attributes::from(&[("type", operation_type)]))
                .is_none(),
            "the metric was present for the operation '{operation_type}'"
        );
    }

    fn check_metric(registry: &Arc<Registry>, operation_type: &'static str, expected: u64) {
        let instrument = registry
            .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATIONS_METRIC_NAME)
            .unwrap();
        let observer = instrument
            .get_observer(&Attributes::from(&[("type", operation_type)]))
            .unwrap();
        assert_eq!(expected, observer.fetch(), "metric check failed");
    }
}
