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
            files_delete=delete.len(),
            files_upgrade=upgrade.len(),
            files_create=created.len(),
            bytes_delete=delete.iter().map(|f| f.file_size_bytes).sum::<i64>(),
            bytes_upgrade=upgrade.iter().map(|f| f.file_size_bytes).sum::<i64>(),
            bytes_create=create.iter().map(|f| f.file_size_bytes).sum::<i64>(),
            rows_delete=delete.iter().map(|f| f.row_count).sum::<i64>(),
            rows_upgrade=upgrade.iter().map(|f| f.row_count).sum::<i64>(),
            rows_create=create.iter().map(|f| f.row_count).sum::<i64>(),
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

    use super::*;
    use crate::components::commit::mock::{CommitHistoryEntry, MockCommit};
    use iox_tests::ParquetFileBuilder;

    #[test]
    fn test_display() {
        let commit = LoggingCommitWrapper::new(MockCommit::new());
        assert_eq!(commit.to_string(), "logging(mock)");
    }

    #[tokio::test]
    async fn test_commit() {
        let inner = Arc::new(MockCommit::new());
        let commit = LoggingCommitWrapper::new(Arc::clone(&inner));

        let existing_1 = ParquetFileBuilder::new(1)
            .with_file_size_bytes(10_001)
            .with_row_count(101)
            .build();
        let existing_2 = ParquetFileBuilder::new(2)
            .with_file_size_bytes(10_002)
            .with_row_count(102)
            .build();
        let existing_3 = ParquetFileBuilder::new(3)
            .with_file_size_bytes(10_005)
            .with_row_count(105)
            .build();

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
            "level = INFO; message = committed parquet file change; target_level = Final; partition_id = 1; files_delete = 1; files_upgrade = 0; files_create = 2; bytes_delete = 10001; bytes_upgrade = 0; bytes_create = 2; rows_delete = 101; rows_upgrade = 0; rows_create = 2; delete = [1]; upgrade = []; create = [1000, 1001]; \n\
level = INFO; message = committed parquet file change; target_level = Final; partition_id = 2; files_delete = 2; files_upgrade = 1; files_create = 0; bytes_delete = 20007; bytes_upgrade = 10001; bytes_create = 0; rows_delete = 207; rows_upgrade = 101; rows_create = 0; delete = [2, 3]; upgrade = [1]; create = []; "
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
