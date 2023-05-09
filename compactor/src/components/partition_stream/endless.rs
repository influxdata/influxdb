use std::{collections::VecDeque, fmt::Display, sync::Arc};

use data_types::PartitionId;
use futures::{stream::BoxStream, StreamExt};

use crate::components::partitions_source::PartitionsSource;

use super::PartitionStream;

#[derive(Debug)]
pub struct EndlessPartititionStream<T>
where
    T: PartitionsSource,
{
    source: Arc<T>,
}

impl<T> EndlessPartititionStream<T>
where
    T: PartitionsSource,
{
    pub fn new(source: T) -> Self {
        Self {
            source: Arc::new(source),
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
                    if let Some(p_id) = buffer.pop_front() {
                        return Some((p_id, buffer));
                    }

                    // fetch new data
                    buffer = VecDeque::from(source.fetch().await);
                }
            }
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::components::partitions_source::mock::MockPartitionsSource;

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
