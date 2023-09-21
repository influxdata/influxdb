use std::{collections::HashSet, fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler::{
    CompactionJob, CompactionJobStatus, CompactionJobStatusResponse, CompactionJobStatusVariant,
    ErrorKind as SchedulerErrorKind, Scheduler,
};

use crate::error::{DynError, ErrorKind, ErrorKindExt};

use super::CompactionJobDoneSink;

#[derive(Debug)]
pub struct ErrorKindCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    kind: HashSet<ErrorKind>,
    inner: T,
    scheduler: Arc<dyn Scheduler>,
}

impl<T> ErrorKindCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    pub fn new(inner: T, kind: HashSet<ErrorKind>, scheduler: Arc<dyn Scheduler>) -> Self {
        Self {
            kind,
            inner,
            scheduler,
        }
    }
}

impl<T> Display for ErrorKindCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut kinds = self.kind.iter().copied().collect::<Vec<_>>();
        kinds.sort();
        write!(f, "kind({:?}, {})", kinds, self.inner)
    }
}

#[async_trait]
impl<T> CompactionJobDoneSink for ErrorKindCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError> {
        match res {
            Ok(()) => self.inner.record(job, Ok(())).await,
            Err(e) if self.kind.contains(&e.classify()) => {
                let scheduler_error = match SchedulerErrorKind::from(e.classify()) {
                    SchedulerErrorKind::OutOfMemory => SchedulerErrorKind::OutOfMemory,
                    SchedulerErrorKind::ObjectStore => SchedulerErrorKind::ObjectStore,
                    SchedulerErrorKind::Timeout => SchedulerErrorKind::Timeout,
                    SchedulerErrorKind::Unknown(_) => SchedulerErrorKind::Unknown(e.to_string()),
                };

                match self
                    .scheduler
                    .update_job_status(CompactionJobStatus {
                        job: job.clone(),
                        status: CompactionJobStatusVariant::Error(scheduler_error),
                    })
                    .await?
                {
                    CompactionJobStatusResponse::Ack => {}
                    CompactionJobStatusResponse::CreatedParquetFiles(_) => {
                        unreachable!("scheduler should not created parquet files")
                    }
                }

                self.inner.record(job, Err(e)).await
            }
            Err(e) => {
                // contract of this abstraction,
                // where we do not pass to `self.inner` if not in `self.kind`
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use compactor_scheduler::create_test_scheduler;
    use data_types::PartitionId;
    use datafusion::error::DataFusionError;
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};
    use object_store::Error as ObjectStoreError;

    use super::{super::mock::MockCompactionJobDoneSink, *};

    #[test]
    fn test_display() {
        let sink = ErrorKindCompactionJobDoneSinkWrapper::new(
            MockCompactionJobDoneSink::new(),
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
        let inner = Arc::new(MockCompactionJobDoneSink::new());
        let sink = ErrorKindCompactionJobDoneSinkWrapper::new(
            Arc::clone(&inner),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
            create_test_scheduler(
                TestCatalog::new().catalog(),
                Arc::new(MockProvider::new(Time::MIN)),
                None,
            ),
        );

        let cj_1 = CompactionJob::new(PartitionId::new(1));
        let cj_2 = CompactionJob::new(PartitionId::new(2));
        let cj_3 = CompactionJob::new(PartitionId::new(3));
        let cj_4 = CompactionJob::new(PartitionId::new(4));

        sink.record(
            cj_1.clone(),
            Err(Box::new(ObjectStoreError::NotImplemented)),
        )
        .await
        .expect("record failed");
        sink.record(
            cj_2.clone(),
            Err(Box::new(DataFusionError::ResourcesExhausted(String::from(
                "foo",
            )))),
        )
        .await
        .expect("record failed");
        sink.record(cj_3, Err("foo".into())).await.unwrap_err();
        sink.record(cj_4.clone(), Ok(()))
            .await
            .expect("record failed");

        assert_eq!(
            inner.results(),
            HashMap::from([
                (cj_1, Err(String::from("Operation not yet implemented.")),),
                (cj_2, Err(String::from("Resources exhausted: foo")),),
                (cj_4, Ok(()),),
            ]),
        );
    }
}
