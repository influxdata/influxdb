use async_trait::async_trait;
use compactor_scheduler::CompactionJob;
use parking_lot::Mutex;

use super::CompactionJobsSource;

/// A mock structure for providing [compaction jobs](CompactionJob).
#[derive(Debug)]
pub struct MockCompactionJobsSource {
    compaction_jobs: Mutex<Vec<CompactionJob>>,
}

impl MockCompactionJobsSource {
    #[allow(dead_code)]
    /// Create a new MockCompactionJobsSource.
    pub fn new(jobs: Vec<CompactionJob>) -> Self {
        Self {
            compaction_jobs: Mutex::new(jobs),
        }
    }

    /// Set CompactionJobs for MockCompactionJobsSource.
    #[allow(dead_code)] // not used anywhere
    pub fn set(&self, jobs: Vec<CompactionJob>) {
        *self.compaction_jobs.lock() = jobs;
    }
}

impl std::fmt::Display for MockCompactionJobsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl CompactionJobsSource for MockCompactionJobsSource {
    async fn fetch(&self) -> Vec<CompactionJob> {
        self.compaction_jobs.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockCompactionJobsSource::new(vec![]).to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_fetch() {
        let source = MockCompactionJobsSource::new(vec![]);
        assert_eq!(source.fetch().await, vec![],);

        let cj_1 = CompactionJob::new(PartitionId::new(5));
        let cj_2 = CompactionJob::new(PartitionId::new(1));
        let cj_3 = CompactionJob::new(PartitionId::new(12));
        let parts = vec![cj_1, cj_2, cj_3];
        source.set(parts.clone());
        assert_eq!(source.fetch().await, parts,);
    }
}
