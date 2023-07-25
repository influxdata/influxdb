use std::{fmt::Display, sync::Arc};

use compactor_scheduler::CompactionJob;
use futures::{stream::BoxStream, StreamExt};

use super::{super::compaction_jobs_source::CompactionJobsSource, CompactionJobStream};

#[derive(Debug)]
pub struct OnceCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    source: Arc<T>,
}

impl<T> OnceCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    pub fn new(source: T) -> Self {
        Self {
            source: Arc::new(source),
        }
    }
}

impl<T> Display for OnceCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "once({})", self.source)
    }
}

impl<T> CompactionJobStream for OnceCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    fn stream(&self) -> BoxStream<'_, CompactionJob> {
        let source = Arc::clone(&self.source);
        futures::stream::once(async move { futures::stream::iter(source.fetch().await) })
            .flatten()
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;

    use super::{super::super::compaction_jobs_source::mock::MockCompactionJobsSource, *};

    #[test]
    fn test_display() {
        let stream = OnceCompactionJobStream::new(MockCompactionJobsSource::new(vec![]));
        assert_eq!(stream.to_string(), "once(mock)");
    }

    #[tokio::test]
    async fn test_stream() {
        let ids = vec![
            CompactionJob::new(PartitionId::new(1)),
            CompactionJob::new(PartitionId::new(3)),
            CompactionJob::new(PartitionId::new(2)),
        ];
        let stream = OnceCompactionJobStream::new(MockCompactionJobsSource::new(ids.clone()));

        // stream is stateless
        for _ in 0..2 {
            assert_eq!(stream.stream().collect::<Vec<_>>().await, ids,);
        }
    }
}
