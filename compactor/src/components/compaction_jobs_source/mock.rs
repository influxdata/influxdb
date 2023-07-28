use async_trait::async_trait;
use compactor_scheduler::CompactionJob;
use parking_lot::Mutex;

use super::CompactionJobsSource;

/// A mock structure for providing [compaction jobs](CompactionJob).
#[derive(Debug)]
pub struct MockCompactionJobsSource {
    partitions: Mutex<Vec<CompactionJob>>,
}

impl MockCompactionJobsSource {
    #[allow(dead_code)]
    /// Create a new MockCompactionJobsSource.
    pub fn new(partitions: Vec<CompactionJob>) -> Self {
        Self {
            partitions: Mutex::new(partitions),
        }
    }

    /// Set CompactionJobs for MockCompactionJobsSource.
    #[allow(dead_code)] // not used anywhere
    pub fn set(&self, partitions: Vec<CompactionJob>) {
        *self.partitions.lock() = partitions;
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
        self.partitions.lock().clone()
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

        let p_1 = CompactionJob::new(PartitionId::new(5));
        let p_2 = CompactionJob::new(PartitionId::new(1));
        let p_3 = CompactionJob::new(PartitionId::new(12));
        let parts = vec![p_1, p_2, p_3];
        source.set(parts.clone());
        assert_eq!(source.fetch().await, parts,);
    }
}
