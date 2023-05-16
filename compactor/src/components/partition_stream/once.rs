use std::{fmt::Display, sync::Arc};

use data_types::PartitionId;
use futures::{stream::BoxStream, StreamExt};

use crate::components::partitions_source::PartitionsSource;

use super::PartitionStream;

#[derive(Debug)]
pub struct OncePartititionStream<T>
where
    T: PartitionsSource,
{
    source: Arc<T>,
}

impl<T> OncePartititionStream<T>
where
    T: PartitionsSource,
{
    pub fn new(source: T) -> Self {
        Self {
            source: Arc::new(source),
        }
    }
}

impl<T> Display for OncePartititionStream<T>
where
    T: PartitionsSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "once({})", self.source)
    }
}

impl<T> PartitionStream for OncePartititionStream<T>
where
    T: PartitionsSource,
{
    fn stream(&self) -> BoxStream<'_, PartitionId> {
        let source = Arc::clone(&self.source);
        futures::stream::once(async move { futures::stream::iter(source.fetch().await) })
            .flatten()
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::components::partitions_source::mock::MockPartitionsSource;

    use super::*;

    #[test]
    fn test_display() {
        let stream = OncePartititionStream::new(MockPartitionsSource::new(vec![]));
        assert_eq!(stream.to_string(), "once(mock)");
    }

    #[tokio::test]
    async fn test_stream() {
        let ids = vec![
            PartitionId::new(1),
            PartitionId::new(3),
            PartitionId::new(2),
        ];
        let stream = OncePartititionStream::new(MockPartitionsSource::new(ids.clone()));

        // stream is stateless
        for _ in 0..2 {
            assert_eq!(stream.stream().collect::<Vec<_>>().await, ids,);
        }
    }
}
