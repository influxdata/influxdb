use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex,
    },
};

use async_trait::async_trait;
use data_types::{ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};

use super::Commit;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CommitHistoryEntry {
    pub partition_id: PartitionId,
    pub delete: Vec<ParquetFileId>,
    pub created: Vec<ParquetFile>,
}

#[derive(Debug)]
pub struct MockCommit {
    history: Mutex<Vec<CommitHistoryEntry>>,
    id_counter: AtomicI64,
}

impl MockCommit {
    #[allow(dead_code)] // not used anywhere
    pub fn new() -> Self {
        Self {
            history: Default::default(),
            id_counter: AtomicI64::new(1000),
        }
    }

    #[allow(dead_code)] // not used anywhere
    pub fn history(&self) -> Vec<CommitHistoryEntry> {
        self.history.lock().expect("not poisoned").clone()
    }
}

impl Display for MockCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl Commit for MockCommit {
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFileId],
        create: &[ParquetFileParams],
    ) -> Vec<ParquetFileId> {
        let (created, ids): (Vec<_>, Vec<_>) = create
            .iter()
            .map(|params| {
                let id = ParquetFileId::new(self.id_counter.fetch_add(1, Ordering::SeqCst));
                let created = ParquetFile::from_params(params.clone(), id);
                (created, id)
            })
            .unzip();

        self.history
            .lock()
            .expect("not poisoned")
            .push(CommitHistoryEntry {
                partition_id,
                delete: delete.to_vec(),
                created,
            });

        ids
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::ParquetFileBuilder;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockCommit::new().to_string(), "mock");
    }

    #[tokio::test]
    async fn test_commit() {
        let commit = MockCommit::new();

        let created_1_1 = ParquetFileBuilder::new(1000).with_partition(1).build();
        let created_1_2 = ParquetFileBuilder::new(1001).with_partition(1).build();
        let created_1_3 = ParquetFileBuilder::new(1003).with_partition(1).build();
        let created_2_1 = ParquetFileBuilder::new(1002).with_partition(2).build();

        let ids = commit
            .commit(
                PartitionId::new(1),
                &[ParquetFileId::new(1), ParquetFileId::new(2)],
                &[created_1_1.clone().into(), created_1_2.clone().into()],
            )
            .await;
        assert_eq!(
            ids,
            vec![ParquetFileId::new(1000), ParquetFileId::new(1001)]
        );

        let ids = commit
            .commit(
                PartitionId::new(2),
                &[ParquetFileId::new(3)],
                &[created_2_1.clone().into()],
            )
            .await;
        assert_eq!(ids, vec![ParquetFileId::new(1002)]);

        let ids = commit
            .commit(
                PartitionId::new(1),
                &[
                    ParquetFileId::new(5),
                    ParquetFileId::new(6),
                    ParquetFileId::new(7),
                ],
                &[created_1_3.clone().into()],
            )
            .await;
        assert_eq!(ids, vec![ParquetFileId::new(1003)]);

        // simulate fill implosion of the file (this may happen w/ delete predicates)
        let ids = commit
            .commit(PartitionId::new(1), &[ParquetFileId::new(8)], &[])
            .await;
        assert_eq!(ids, vec![]);

        assert_eq!(
            commit.history(),
            vec![
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![ParquetFileId::new(1), ParquetFileId::new(2)],
                    created: vec![created_1_1, created_1_2],
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(2),
                    delete: vec![ParquetFileId::new(3)],
                    created: vec![created_2_1],
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![
                        ParquetFileId::new(5),
                        ParquetFileId::new(6),
                        ParquetFileId::new(7)
                    ],
                    created: vec![created_1_3],
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![ParquetFileId::new(8)],
                    created: vec![],
                },
            ]
        )
    }
}
