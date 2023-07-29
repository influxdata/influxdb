use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;
use metric::{Registry, U64Counter};

use crate::error::{DynError, ErrorKind, ErrorKindExt};

use super::CompactionJobDoneSink;

const METRIC_NAME_PARTITION_COMPLETE_COUNT: &str = "iox_compactor_partition_complete_count";

#[derive(Debug)]
pub struct MetricsCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    ok_counter: U64Counter,
    error_counter: HashMap<ErrorKind, U64Counter>,
    inner: T,
}

impl<T> MetricsCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let metric = registry.register_metric::<U64Counter>(
            METRIC_NAME_PARTITION_COMPLETE_COUNT,
            "Number of completed partitions",
        );
        let ok_counter = metric.recorder(&[("result", "ok")]);
        let error_counter = ErrorKind::variants()
            .iter()
            .map(|kind| {
                (
                    *kind,
                    metric.recorder(&[("result", "error"), ("kind", kind.name())]),
                )
            })
            .collect();

        Self {
            ok_counter,
            error_counter,
            inner,
        }
    }
}

impl<T> Display for MetricsCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[async_trait]
impl<T> CompactionJobDoneSink for MetricsCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError> {
        match &res {
            Ok(()) => {
                self.ok_counter.inc(1);
            }
            Err(e) => {
                // classify and track counts of compactor ErrorKind
                let kind = e.classify();
                self.error_counter
                    .get(&kind)
                    .expect("all kinds constructed")
                    .inc(1);
            }
        }
        self.inner.record(job, res).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use data_types::PartitionId;
    use metric::{assert_counter, Attributes};
    use object_store::Error as ObjectStoreError;

    use super::{super::mock::MockCompactionJobDoneSink, *};

    #[test]
    fn test_display() {
        let registry = Registry::new();
        let sink =
            MetricsCompactionJobDoneSinkWrapper::new(MockCompactionJobDoneSink::new(), &registry);
        assert_eq!(sink.to_string(), "metrics(mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let registry = Registry::new();
        let inner = Arc::new(MockCompactionJobDoneSink::new());
        let sink = MetricsCompactionJobDoneSinkWrapper::new(Arc::clone(&inner), &registry);

        assert_ok_counter(&registry, 0);
        assert_error_counter(&registry, "unknown", 0);
        assert_error_counter(&registry, "object_store", 0);

        let cj_1 = CompactionJob::new(PartitionId::new(1));
        let cj_2 = CompactionJob::new(PartitionId::new(2));
        let cj_3 = CompactionJob::new(PartitionId::new(3));

        sink.record(cj_1.clone(), Err("msg 1".into()))
            .await
            .expect("record failed");
        sink.record(cj_2.clone(), Err("msg 2".into()))
            .await
            .expect("record failed");
        sink.record(
            cj_1.clone(),
            Err(Box::new(ObjectStoreError::NotImplemented)),
        )
        .await
        .expect("record failed");
        sink.record(cj_3.clone(), Ok(()))
            .await
            .expect("record failed");

        assert_ok_counter(&registry, 1);
        assert_error_counter(&registry, "unknown", 2);
        assert_error_counter(&registry, "object_store", 1);

        assert_eq!(
            inner.results(),
            HashMap::from([
                (cj_1, Err(String::from("Operation not yet implemented.")),),
                (cj_2, Err(String::from("msg 2"))),
                (cj_3, Ok(())),
            ]),
        );
    }

    fn assert_ok_counter(registry: &Registry, value: u64) {
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_PARTITION_COMPLETE_COUNT,
            labels = Attributes::from(&[("result", "ok")]),
            value = value,
        );
    }

    fn assert_error_counter(registry: &Registry, kind: &'static str, value: u64) {
        assert_counter!(
            registry,
            U64Counter,
            METRIC_NAME_PARTITION_COMPLETE_COUNT,
            labels = Attributes::from(&[("result", "error"), ("kind", kind)]),
            value = value,
        );
    }
}
