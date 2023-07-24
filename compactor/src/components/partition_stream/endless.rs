use std::{collections::VecDeque, fmt::Display, sync::Arc};

use compactor_scheduler::PartitionsSource;
use data_types::PartitionId;
use futures::{stream::BoxStream, StreamExt};

use super::super::partition_files_source::rate_limit::RateLimit;
use super::PartitionStream;

#[derive(Debug)]
pub struct EndlessPartititionStream<T>
where
    T: PartitionsSource,
{
    source: Arc<T>,
    limiter: RateLimit,
}

impl<T> EndlessPartititionStream<T>
where
    T: PartitionsSource,
{
    pub fn new(source: T) -> Self {
        Self {
            source: Arc::new(source),
            limiter: RateLimit::new(1, 1), // Initial rate is irrelevant, it will be updated before first use.
        }
    }
}

impl<T> Display for EndlessPartititionStream<T>
where
    T: PartitionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "endless({})", self.source)
    }
}

impl<T> PartitionStream for EndlessPartititionStream<T>
where
    T: PartitionsSource,
{
    fn stream(&self) -> BoxStream<'_, PartitionId> {
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
                    if rate < 1 {
                        rate = 1;
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
    use compactor_scheduler::MockPartitionsSource;

    use super::*;

    #[test]
    fn test_display() {
        let stream = EndlessPartititionStream::new(MockPartitionsSource::new(vec![]));
        assert_eq!(stream.to_string(), "endless(mock)");
    }

    #[tokio::test]
    async fn test_stream() {
        let ids = vec![
            PartitionId::new(1),
            PartitionId::new(3),
            PartitionId::new(2),
        ];
        let stream = EndlessPartititionStream::new(MockPartitionsSource::new(ids.clone()));

        // stream is stateless
        for _ in 0..2 {
            // we need to limit the stream at one point because it is endless
            assert_eq!(
                stream.stream().take(5).collect::<Vec<_>>().await,
                vec![
                    PartitionId::new(1),
                    PartitionId::new(3),
                    PartitionId::new(2),
                    PartitionId::new(1),
                    PartitionId::new(3)
                ],
            );
        }
    }
}
