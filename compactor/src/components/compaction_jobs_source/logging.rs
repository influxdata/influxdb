use std::fmt::Display;

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;
use observability_deps::tracing::{info, warn};

use super::CompactionJobsSource;

#[derive(Debug)]
pub struct LoggingCompactionJobsWrapper<T>
where
    T: CompactionJobsSource,
{
    inner: T,
}

impl<T> LoggingCompactionJobsWrapper<T>
where
    T: CompactionJobsSource,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingCompactionJobsWrapper<T>
where
    T: CompactionJobsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> CompactionJobsSource for LoggingCompactionJobsWrapper<T>
where
    T: CompactionJobsSource,
{
    async fn fetch(&self) -> Vec<CompactionJob> {
        let jobs = self.inner.fetch().await;
        info!(n_jobs = jobs.len(), "Fetch jobs",);
        if jobs.is_empty() {
            warn!("No compaction job found");
        }
        jobs
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;
    use test_helpers::tracing::TracingCapture;

    use super::{super::mock::MockCompactionJobsSource, *};

    #[test]
    fn test_display() {
        let source = LoggingCompactionJobsWrapper::new(MockCompactionJobsSource::new(vec![]));
        assert_eq!(source.to_string(), "logging(mock)",);
    }

    #[tokio::test]
    async fn test_fetch_empty() {
        let source = LoggingCompactionJobsWrapper::new(MockCompactionJobsSource::new(vec![]));
        let capture = TracingCapture::new();
        assert_eq!(source.fetch().await, vec![],);
        // logs normal log message (so it's easy search for every single call) but also an extra warning
        assert_eq!(
            capture.to_string(),
            "level = INFO; message = Fetch jobs; n_jobs = 0; \
            \nlevel = WARN; message = No compaction job found; ",
        );
    }

    #[tokio::test]
    async fn test_fetch_some() {
        let cj_1 = CompactionJob::new(PartitionId::new(5));
        let cj_2 = CompactionJob::new(PartitionId::new(1));
        let cj_3 = CompactionJob::new(PartitionId::new(12));
        let jobs = vec![cj_1, cj_2, cj_3];

        let source = LoggingCompactionJobsWrapper::new(MockCompactionJobsSource::new(jobs.clone()));
        let capture = TracingCapture::new();
        assert_eq!(source.fetch().await, jobs,);
        // just the ordinary log message, no warning
        assert_eq!(
            capture.to_string(),
            "level = INFO; message = Fetch jobs; n_jobs = 3; ",
        );
    }
}
