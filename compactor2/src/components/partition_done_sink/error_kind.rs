use std::{collections::HashSet, fmt::Display};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::error::{ErrorKind, ErrorKindExt};

use super::PartitionDoneSink;

#[derive(Debug)]
pub struct ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    kind: HashSet<ErrorKind>,
    inner: T,
}

impl<T> ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    pub fn new(inner: T, kind: HashSet<ErrorKind>) -> Self {
        Self { kind, inner }
    }
}

impl<T> Display for ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut kinds = self.kind.iter().copied().collect::<Vec<_>>();
        kinds.sort();
        write!(f, "kind({:?}, {})", kinds, self.inner)
    }
}

#[async_trait]
impl<T> PartitionDoneSink for ErrorKindPartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) {
        match res {
            Ok(()) => self.inner.record(partition, Ok(())).await,
            Err(e) if self.kind.contains(&e.classify()) => {
                self.inner.record(partition, Err(e)).await;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::components::partition_done_sink::mock::MockPartitionDoneSink;

    use datafusion::error::DataFusionError;
    use object_store::Error as ObjectStoreError;

    use super::*;

    #[test]
    fn test_display() {
        let sink = ErrorKindPartitionDoneSinkWrapper::new(
            MockPartitionDoneSink::new(),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
        );
        assert_eq!(sink.to_string(), "kind([ObjectStore, OutOfMemory], mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let inner = Arc::new(MockPartitionDoneSink::new());
        let sink = ErrorKindPartitionDoneSinkWrapper::new(
            Arc::clone(&inner),
            HashSet::from([ErrorKind::ObjectStore, ErrorKind::OutOfMemory]),
        );

        sink.record(
            PartitionId::new(1),
            Err(Box::new(ObjectStoreError::NotImplemented)),
        )
        .await;
        sink.record(
            PartitionId::new(2),
            Err(Box::new(DataFusionError::ResourcesExhausted(String::from(
                "foo",
            )))),
        )
        .await;
        sink.record(PartitionId::new(3), Err("foo".into())).await;
        sink.record(PartitionId::new(4), Ok(())).await;

        assert_eq!(
            inner.errors(),
            HashMap::from([
                (
                    PartitionId::new(1),
                    Err(String::from("Operation not yet implemented.")),
                ),
                (
                    PartitionId::new(2),
                    Err(String::from("Resources exhausted: foo")),
                ),
                (PartitionId::new(4), Ok(()),),
            ]),
        );
    }
}
