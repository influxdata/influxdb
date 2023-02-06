use std::fmt::Display;

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use observability_deps::tracing::info;

use super::Commit;

#[derive(Debug)]
pub struct LoggingCommitWrapper<T>
where
    T: Commit,
{
    inner: T,
}

impl<T> LoggingCommitWrapper<T>
where
    T: Commit,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingCommitWrapper<T>
where
    T: Commit,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> Commit for LoggingCommitWrapper<T>
where
    T: Commit,
{
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Vec<ParquetFileId> {
        // Perform commit first and report status AFTERWARDS.
        let created = self
            .inner
            .commit(partition_id, delete, upgrade, create, target_level)
            .await;

        // Log numbers BEFORE IDs because the list may be so long that we hit the line-length limit. In this case we at
        // least have the important information. Note that the message always is printed first, so we'll never loose
        // that one.
        info!(
            target_level=?target_level,
            partition_id=partition_id.get(),
            n_delete=delete.len(),
            n_upgrade=upgrade.len(),
            n_create=created.len(),
            delete=?delete.iter().map(|f| f.id.get()).collect::<Vec<_>>(),
            upgrade=?upgrade.iter().map(|f| f.id.get()).collect::<Vec<_>>(),
            create=?created.iter().map(|id| id.get()).collect::<Vec<_>>(),
            "committed parquet file change",
        );

        created
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_helpers::tracing::TracingCapture;

    use crate::{
        components::commit::mock::{CommitHistoryEntry, MockCommit},
        test_util::ParquetFileBuilder,
    };

    use super::*;

    #[test]
    fn test_display() {
        let commit = LoggingCommitWrapper::new(MockCommit::new());
        assert_eq!(commit.to_string(), "logging(mock)");
    }

    #[tokio::test]
    async fn test_commit() {
        let inner = Arc::new(MockCommit::new());
        let commit = LoggingCommitWrapper::new(Arc::clone(&inner));

        let existing_1 = ParquetFileBuilder::new(1).build();
        let existing_2 = ParquetFileBuilder::new(2).build();
        let existing_3 = ParquetFileBuilder::new(3).build();

        let created_1 = ParquetFileBuilder::new(1000).with_partition(1).build();
        let created_2 = ParquetFileBuilder::new(1001).with_partition(1).build();

        let capture = TracingCapture::new();

        let ids = commit
            .commit(
                PartitionId::new(1),
                &[existing_1.clone()],
                &[],
                &[created_1.clone().into(), created_2.clone().into()],
                CompactionLevel::Final,
            )
            .await;
        assert_eq!(
            ids,
            vec![ParquetFileId::new(1000), ParquetFileId::new(1001)]
        );

        let ids = commit
            .commit(
                PartitionId::new(2),
                &[existing_2.clone(), existing_3.clone()],
                &[existing_1.clone()],
                &[],
                CompactionLevel::Final,
            )
            .await;
        assert_eq!(ids, vec![]);

        assert_eq!(
            capture.to_string(),
            "level = INFO; message = committed parquet file change; target_level = Final; partition_id = 1; n_delete = 1; n_upgrade = 0; n_create = 2; delete = [1]; upgrade = []; create = [1000, 1001]; \n\
level = INFO; message = committed parquet file change; target_level = Final; partition_id = 2; n_delete = 2; n_upgrade = 1; n_create = 0; delete = [2, 3]; upgrade = [1]; create = []; "
        );

        assert_eq!(
            inner.history(),
            vec![
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![existing_1.clone()],
                    upgrade: vec![],
                    created: vec![created_1, created_2],
                    target_level: CompactionLevel::Final,
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(2),
                    delete: vec![existing_2, existing_3],
                    upgrade: vec![existing_1],
                    created: vec![],
                    target_level: CompactionLevel::Final,
                },
            ]
        );
    }
}
