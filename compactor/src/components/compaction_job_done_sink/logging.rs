use std::fmt::Display;

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;
use observability_deps::tracing::{error, info};

use crate::error::{DynError, ErrorKindExt};

use super::CompactionJobDoneSink;

#[derive(Debug)]
pub struct LoggingCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    inner: T,
}

impl<T> LoggingCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> CompactionJobDoneSink for LoggingCompactionJobDoneSinkWrapper<T>
where
    T: CompactionJobDoneSink,
{
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError> {
        match &res {
            Ok(()) => {
                info!(
                    partition_id = job.partition_id.get(),
                    job_uuid = job.uuid().to_string(),
                    "Finished compaction job",
                );
            }
            Err(e) => {
                // log compactor errors, classified by compactor ErrorKind
                error!(
                    %e,
                    kind=e.classify().name(),
                    partition_id = job.partition_id.get(),
                    job_uuid = job.uuid().to_string(),
                    "Error while compacting partition",
                );
            }
        }
        self.inner.record(job, res).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use data_types::PartitionId;
    use object_store::Error as ObjectStoreError;
    use test_helpers::tracing::TracingCapture;

    use super::{super::mock::MockCompactionJobDoneSink, *};

    #[test]
    fn test_display() {
        let sink = LoggingCompactionJobDoneSinkWrapper::new(MockCompactionJobDoneSink::new());
        assert_eq!(sink.to_string(), "logging(mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let inner = Arc::new(MockCompactionJobDoneSink::new());
        let sink = LoggingCompactionJobDoneSinkWrapper::new(Arc::clone(&inner));

        let capture = TracingCapture::new();

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

        assert_eq!(
            capture.to_string(),
            format!("level = ERROR; message = Error while compacting partition; e = msg 1; kind = \"unknown\"; partition_id = 1; job_uuid = {:?}; \n\
level = ERROR; message = Error while compacting partition; e = msg 2; kind = \"unknown\"; partition_id = 2; job_uuid = {:?}; \n\
level = ERROR; message = Error while compacting partition; e = Operation not yet implemented.; kind = \"object_store\"; partition_id = 1; job_uuid = {:?}; \n\
level = INFO; message = Finished compaction job; partition_id = 3; job_uuid = {:?}; ", cj_1.uuid().to_string(), cj_2.uuid().to_string(), cj_1.uuid().to_string(), cj_3.uuid().to_string()),
        );

        assert_eq!(
            inner.results(),
            HashMap::from([
                (cj_1, Err(String::from("Operation not yet implemented.")),),
                (cj_2, Err(String::from("msg 2"))),
                (cj_3, Ok(())),
            ]),
        );
    }
}
