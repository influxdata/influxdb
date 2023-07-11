use std::{collections::HashSet, fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler::{
    CompactionJob, CompactionJobStatus, CompactionJobStatusResult, CompactionJobStatusVariant,
    ErrorKind as SchedulerErrorKind, PartitionDoneSink, Scheduler,
};
use data_types::PartitionId;

use crate::error::{DynError, ErrorKind, ErrorKindExt};

#[derive(Debug)]
pub struct ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    kind: HashSet<ErrorKind>,
    inner: T,
    scheduler: Arc<dyn Scheduler>,
}

impl<T> ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    pub fn new(inner: T, kind: HashSet<ErrorKind>, scheduler: Arc<dyn Scheduler>) -> Self {
        Self {
            kind,
            inner,
            scheduler,
        }
    }
}

impl<T> Display for ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut kinds = self.kind.iter().copied().collect::<Vec<_>>();
        kinds.sort();
        write!(f, "kind({:?}, {})", kinds, self.inner)
    }
}

#[async_trait]
impl<T> PartitionDoneSink for ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) {
        match res {
            Ok(()) => self.inner.record(partition, Ok(())).await,
            Err(e) if self.kind.contains(&e.classify()) => {
                let scheduler_error = match SchedulerErrorKind::from(e.classify()) {
                    SchedulerErrorKind::OutOfMemory => SchedulerErrorKind::OutOfMemory,
                    SchedulerErrorKind::ObjectStore => SchedulerErrorKind::ObjectStore,
                    SchedulerErrorKind::Timeout => SchedulerErrorKind::Timeout,
                    SchedulerErrorKind::Unknown(_) => SchedulerErrorKind::Unknown(e.to_string()),
                };

                match self
                    .scheduler
                    .job_status(CompactionJobStatus {
                        job: CompactionJob::new(partition),
                        status: CompactionJobStatusVariant::Error(scheduler_error),
                    })
                    .await
                {
                    Ok(CompactionJobStatusResult::Ack) => {}
                    _ => panic!("unexpected result from scheduler"),
                }
                self.inner.record(partition, Err(e)).await;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use compactor_scheduler::{create_test_scheduler, MockPartitionDoneSink};
    use datafusion::error::DataFusionError;
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};
    use object_store::Error as ObjectStoreError;

    use super::*;

    #[test]
    fn test_display() {
        let sink = ErrorKindPartitionDoneSinkWrapper::new(
            MockPartitionDoneSink::new(),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
            create_test_scheduler(
                TestCatalog::new().catalog(),
                Arc::new(MockProvider::new(Time::MIN)),
                None,
            ),
        );
        assert_eq!(sink.to_string(), "kind([ObjectStore, OutOfMemory], mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let inner = Arc::new(MockPartitionDoneSink::new());
        let sink = ErrorKindPartitionDoneSinkWrapper::new(
            Arc::clone(&inner),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
            create_test_scheduler(
                TestCatalog::new().catalog(),
                Arc::new(MockProvider::new(Time::MIN)),
                None,
            ),
        );

        sink.record(
            PartitionId::new(1),
            Err(Box::new(ObjectStoreError::NotImplemented)),
        )
        .await;
        sink.record(
            PartitionId::new(2),
            Err(Box::new(DataFusionError::ResourcesExhausted(String::from(
                "foo",
            )))),
        )
        .await;
        sink.record(PartitionId::new(3), Err("foo".into())).await;
        sink.record(PartitionId::new(4), Ok(())).await;

        assert_eq!(
            inner.results(),
            HashMap::from([
                (
                    PartitionId::new(1),
                    Err(String::from("Operation not yet implemented.")),
                ),
                (
                    PartitionId::new(2),
                    Err(String::from("Resources exhausted: foo")),
                ),
                (PartitionId::new(4), Ok(()),),
            ]),
        );
    }
}
