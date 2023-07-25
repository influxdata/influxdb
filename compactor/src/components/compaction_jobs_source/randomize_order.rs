use std::fmt::Display;

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

use super::CompactionJobsSource;

#[derive(Debug)]
pub struct RandomizeOrderCompactionJobsSourcesWrapper<T>
where
    T: CompactionJobsSource,
{
    inner: T,
    seed: u64,
}

impl<T> RandomizeOrderCompactionJobsSourcesWrapper<T>
where
    T: CompactionJobsSource,
{
    pub fn new(inner: T, seed: u64) -> Self {
        Self { inner, seed }
    }
}

impl<T> Display for RandomizeOrderCompactionJobsSourcesWrapper<T>
where
    T: CompactionJobsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "randomize_order({})", self.inner)
    }
}

#[async_trait]
impl<T> CompactionJobsSource for RandomizeOrderCompactionJobsSourcesWrapper<T>
where
    T: CompactionJobsSource,
{
    async fn fetch(&self) -> Vec<CompactionJob> {
        let mut partitions = self.inner.fetch().await;
        let mut rng = StdRng::seed_from_u64(self.seed);
        partitions.shuffle(&mut rng);
        partitions
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;

    use super::{super::mock::MockCompactionJobsSource, *};

    #[test]
    fn test_display() {
        let source = RandomizeOrderCompactionJobsSourcesWrapper::new(
            MockCompactionJobsSource::new(vec![]),
            123,
        );
        assert_eq!(source.to_string(), "randomize_order(mock)",);
    }

    #[tokio::test]
    async fn test_fetch_empty() {
        let source = RandomizeOrderCompactionJobsSourcesWrapper::new(
            MockCompactionJobsSource::new(vec![]),
            123,
        );
        assert_eq!(source.fetch().await, vec![],);
    }

    #[tokio::test]
    async fn test_fetch_some() {
        let p_1 = CompactionJob::new(PartitionId::new(5));
        let p_2 = CompactionJob::new(PartitionId::new(1));
        let p_3 = CompactionJob::new(PartitionId::new(12));
        let partitions = vec![p_1.clone(), p_2.clone(), p_3.clone()];

        // shuffles
        let source = RandomizeOrderCompactionJobsSourcesWrapper::new(
            MockCompactionJobsSource::new(partitions.clone()),
            123,
        );
        assert_eq!(
            source.fetch().await,
            vec![p_3.clone(), p_2.clone(), p_1.clone(),],
        );

        // is deterministic in same source
        for _ in 0..100 {
            assert_eq!(
                source.fetch().await,
                vec![p_3.clone(), p_2.clone(), p_1.clone(),],
            );
        }

        // is deterministic with new source
        for _ in 0..100 {
            let source = RandomizeOrderCompactionJobsSourcesWrapper::new(
                MockCompactionJobsSource::new(partitions.clone()),
                123,
            );
            assert_eq!(
                source.fetch().await,
                vec![p_3.clone(), p_2.clone(), p_1.clone(),],
            );
        }

        // different seed => different output
        let source = RandomizeOrderCompactionJobsSourcesWrapper::new(
            MockCompactionJobsSource::new(partitions.clone()),
            1234,
        );
        assert_eq!(source.fetch().await, vec![p_2, p_3, p_1,],);
    }
}
