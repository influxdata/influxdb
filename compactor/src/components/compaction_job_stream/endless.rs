use std::{collections::VecDeque, fmt::Display, sync::Arc};

use compactor_scheduler::CompactionJob;
use futures::{stream::BoxStream, StreamExt};

use super::super::{
    compaction_jobs_source::CompactionJobsSource, partition_files_source::rate_limit::RateLimit,
};
use super::CompactionJobStream;

#[derive(Debug)]
pub struct EndlessCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    source: Arc<T>,
    limiter: RateLimit,
}

impl<T> EndlessCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    pub fn new(source: T) -> Self {
        Self {
            source: Arc::new(source),
            limiter: RateLimit::new(1, 1), // Initial rate is irrelevant, it will be updated before first use.
        }
    }
}

impl<T> Display for EndlessCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "endless({})", self.source)
    }
}

impl<T> CompactionJobStream for EndlessCompactionJobStream<T>
where
    T: CompactionJobsSource,
{
    fn stream(&self) -> BoxStream<'_, CompactionJob> {
        let source = Arc::clone(&self.source);

        // Note: we use a VecDeque as a buffer so we can preserve the order and cheaply remove the first element without
        // relocating the entire buffer content.
        futures::stream::unfold(VecDeque::new(), move |mut buffer| {
            let source = Arc::clone(&source);
            async move {
                loop {
                    while let Some(d) = self.limiter.can_proceed() {
                        // Throttling because either we don't need to go this fast, or we're at risk of hitting the catalog
                        // to hard, or both.
                        tokio::time::sleep(d).await;
                    }

                    if let Some(p_id) = buffer.pop_front() {
                        return Some((p_id, buffer));
                    }

                    // fetch new data
                    buffer = VecDeque::from(source.fetch().await);

                    // update rate limiter so we can complete the batch in 5m, which is plenty fast.
                    // allow a burst of 25, so after a period of inactivity, up to 25 can go quickly.
                    let mut rate = buffer.len() / (5 * 60);

                    if rate < 10 {
                        // The purpose of this rate limiter is to keep us from hitting the catalog too hard.  There is no need to
                        // slow it down to less than 10/s
                        rate = 10;
                    }
                    self.limiter.update_rps(rate, 25);
                }
            }
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;

    use super::{super::super::compaction_jobs_source::mock::MockCompactionJobsSource, *};

    #[test]
    fn test_display() {
        let stream = EndlessCompactionJobStream::new(MockCompactionJobsSource::new(vec![]));
        assert_eq!(stream.to_string(), "endless(mock)");
    }

    #[tokio::test]
    async fn test_stream() {
        let ids = vec![
            CompactionJob::new(PartitionId::new(1)),
            CompactionJob::new(PartitionId::new(3)),
            CompactionJob::new(PartitionId::new(2)),
        ];
        let stream = EndlessCompactionJobStream::new(MockCompactionJobsSource::new(ids.clone()));

        // stream is stateless
        for _ in 0..2 {
            // we need to limit the stream at one point because it is endless
            assert_eq!(
                stream.stream().take(5).collect::<Vec<_>>().await,
                [&ids[..], &ids[..2]].concat(),
            );
        }
    }
}
