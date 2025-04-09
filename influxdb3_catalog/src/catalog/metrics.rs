use std::{sync::Arc, time::Instant};

use hashbrown::HashMap;
use metric::{Attributes, DurationHistogram, Metric, Registry};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

pub(super) const CATALOG_OPERATIONS_DURATION: &str = "influxdb3_catalog_operations_duration";

#[derive(Debug)]
pub(super) struct CatalogMetrics {
    metric_registry: Arc<Registry>,
    /// Mutex'd map of the operation kind, e.g., "create_database", to the metrics for that operation
    metrics: Mutex<HashMap<&'static str, Metrics>>,
}

impl CatalogMetrics {
    pub(super) fn new(metric_registry: Arc<Registry>) -> Self {
        Self {
            metric_registry,
            metrics: Default::default(),
        }
    }

    pub(super) fn recorder(self: &Arc<Self>, operation: &'static str) -> MetricsRecorder {
        MetricsRecorder {
            metrics: Arc::clone(self),
            start_instant: Instant::now(),
            status: Default::default(),
            operation,
        }
    }

    fn metrics(&self, operation: &'static str) -> MappedMutexGuard<'_, Metrics> {
        MutexGuard::map(self.metrics.lock(), |metrics| {
            let (_, m) = metrics
                .raw_entry_mut()
                .from_key(&operation)
                .or_insert_with(|| {
                    let attributes = Attributes::from(&[("type", operation)]);
                    (operation, Metrics::new(&self.metric_registry, attributes))
                });
            m
        })
    }
}

#[derive(Debug)]
struct Metrics {
    operations: OperationMetrics,
}

impl Metrics {
    fn new(registry: &Registry, attributes: impl Into<Attributes>) -> Self {
        let duration: Metric<DurationHistogram> = registry.register_metric(
            CATALOG_OPERATIONS_DURATION,
            "time taken to perform catalog operations",
        );
        let operations = OperationMetrics::new(&duration, attributes.into());
        Self { operations }
    }
}

#[derive(Debug)]
struct OperationMetrics {
    success: DurationHistogram,
    bad_request: DurationHistogram,
    persist_failure: DurationHistogram,
    broadcast_failure: DurationHistogram,
}

impl OperationMetrics {
    fn new(metric: &Metric<DurationHistogram>, mut attributes: Attributes) -> Self {
        attributes.insert("status", "success");
        let success = metric.recorder(attributes.clone());

        attributes.insert("status", "persist_failure");
        let persist_failure = metric.recorder(attributes.clone());

        attributes.insert("status", "bad_request");
        let bad_request = metric.recorder(attributes.clone());

        attributes.insert("status", "broadcast_failure");
        let broadcast_failure = metric.recorder(attributes.clone());

        Self {
            success,
            bad_request,
            persist_failure,
            broadcast_failure,
        }
    }
}

#[derive(Debug, Default)]
pub(super) enum OperationStatus {
    #[default]
    Success,
    BadRequest,
    PersistFailure,
    BroadcastFailure,
}

#[derive(Debug)]
pub(super) struct MetricsRecorder {
    metrics: Arc<CatalogMetrics>,
    start_instant: Instant,
    status: Mutex<OperationStatus>,
    operation: &'static str,
}

impl MetricsRecorder {
    pub(super) fn set_status(&self, status: OperationStatus) {
        *self.status.lock() = status;
    }
}

impl Drop for MetricsRecorder {
    fn drop(&mut self) {
        let metrics = self.metrics.metrics(self.operation);
        let duration = self.start_instant.elapsed();
        match *self.status.lock() {
            OperationStatus::Success => metrics.operations.success.record(duration),
            OperationStatus::BadRequest => metrics.operations.bad_request.record(duration),
            OperationStatus::PersistFailure => metrics.operations.persist_failure.record(duration),
            OperationStatus::BroadcastFailure => {
                metrics.operations.broadcast_failure.record(duration)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iox_time::{MockProvider, Time};
    use metric::{Attributes, DurationHistogram, Metric, Registry};
    use object_store::memory::InMemory;

    use crate::{catalog::Catalog, log::FieldDataType};

    use super::CATALOG_OPERATIONS_DURATION;

    #[tokio::test]
    async fn test_catalog_metrics() {
        let metric_registry = Arc::new(Registry::new());
        let catalog = Catalog::new(
            "nog",
            Arc::new(InMemory::new()),
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::clone(&metric_registry),
        )
        .await
        .unwrap();

        let durations_observer = metric_registry
            .get_instrument::<Metric<DurationHistogram>>(CATALOG_OPERATIONS_DURATION)
            .unwrap();

        assert_eq!(
            // 1 because of the _internal db:
            1,
            durations_observer
                .get_observer(&Attributes::from(&[
                    ("type", "create_database"),
                    ("status", "success")
                ]))
                .unwrap()
                .fetch()
                .sample_count()
        );

        catalog.create_database("foo").await.unwrap();

        assert_eq!(
            2,
            durations_observer
                .get_observer(&Attributes::from(&[
                    ("type", "create_database"),
                    ("status", "success")
                ]))
                .unwrap()
                .fetch()
                .sample_count()
        );

        assert!(
            durations_observer
                .get_observer(&Attributes::from(&[
                    ("type", "create_table"),
                    ("status", "success")
                ]))
                .is_none()
        );

        catalog
            .create_table("foo", "bar", &["t"], &[("f", FieldDataType::String)])
            .await
            .unwrap();

        assert_eq!(
            1,
            durations_observer
                .get_observer(&Attributes::from(&[
                    ("type", "create_table"),
                    ("status", "success")
                ]))
                .unwrap()
                .fetch()
                .sample_count()
        );

        // create a database that already exists, should fail:
        assert_eq!(
            0,
            durations_observer
                .get_observer(&Attributes::from(&[
                    ("type", "create_database"),
                    ("status", "bad_request")
                ]))
                .unwrap()
                .fetch()
                .sample_count()
        );
        catalog.create_database("foo").await.unwrap_err();
        assert_eq!(
            1,
            durations_observer
                .get_observer(&Attributes::from(&[
                    ("type", "create_database"),
                    ("status", "bad_request")
                ]))
                .unwrap()
                .fetch()
                .sample_count()
        );
    }
}
