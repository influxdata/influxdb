use std::{collections::HashSet, fmt::Display};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::error::{ErrorKind, ErrorKindExt};

use super::PartitionErrorSink;

#[derive(Debug)]
pub struct KindPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    kind: HashSet<ErrorKind>,
    inner: T,
}

impl<T> KindPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    pub fn new(inner: T, kind: HashSet<ErrorKind>) -> Self {
        Self { kind, inner }
    }
}

impl<T> Display for KindPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut kinds = self.kind.iter().copied().collect::<Vec<_>>();
        kinds.sort();
        write!(f, "kind({:?}, {})", kinds, self.inner)
    }
}

#[async_trait]
impl<T> PartitionErrorSink for KindPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    async fn record(&self, partition: PartitionId, e: Box<dyn std::error::Error + Send + Sync>) {
        let kind = e.classify();
        if self.kind.contains(&kind) {
            self.inner.record(partition, e).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::components::partition_error_sink::mock::MockPartitionErrorSink;

    use datafusion::error::DataFusionError;
    use object_store::Error as ObjectStoreError;

    use super::*;

    #[test]
    fn test_display() {
        let sink = KindPartitionErrorSinkWrapper::new(
            MockPartitionErrorSink::new(),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
        );
        assert_eq!(sink.to_string(), "kind([ObjectStore, OutOfMemory], mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let inner = Arc::new(MockPartitionErrorSink::new());
        let sink = KindPartitionErrorSinkWrapper::new(
            Arc::clone(&inner),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
        );

        sink.record(
            PartitionId::new(1),
            Box::new(ObjectStoreError::NotImplemented),
        )
        .await;
        sink.record(
            PartitionId::new(2),
            Box::new(DataFusionError::ResourcesExhausted(String::from("foo"))),
        )
        .await;
        sink.record(PartitionId::new(3), "foo".into()).await;

        assert_eq!(
            inner.errors(),
            HashMap::from([
                (
                    PartitionId::new(1),
                    String::from("Operation not yet implemented.")
                ),
                (
                    PartitionId::new(2),
                    String::from("Resources exhausted: foo")
                ),
            ]),
        );
    }
}
