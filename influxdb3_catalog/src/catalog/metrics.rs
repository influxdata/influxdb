use std::sync::Arc;

use metric::{Metric, Registry, U64Counter};

pub(super) const CATALOG_OPERATION_RETRIES_METRIC_NAME: &str =
    "influxdb3_catalog_operation_retries";
const CATALOG_OPERATION_RETRIES_METRIC_DESCRIPTION: &str =
    "catalog updates that had to be retried because the catalog was updated elsewhere";

#[derive(Debug)]
pub(super) struct CatalogMetrics {
    pub(super) catalog_operation_retries: U64Counter,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iox_time::{MockProvider, Time};
    use metric::{Attributes, Metric, Registry, U64Counter};
    use object_store::memory::InMemory;

    use crate::catalog::Catalog;

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
}
