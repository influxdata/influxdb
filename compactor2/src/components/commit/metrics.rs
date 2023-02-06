use std::fmt::Display;

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use metric::{Registry, U64Counter};

use super::Commit;

#[derive(Debug)]
pub struct MetricsCommitWrapper<T>
where
    T: Commit,
{
    create_counter: U64Counter,
    upgrade_counter: U64Counter,
    delete_counter: U64Counter,
    commit_counter: U64Counter,
    inner: T,
}

impl<T> MetricsCommitWrapper<T>
where
    T: Commit,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        let create_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_file_create_count",
                "Number of files created by the compactor",
            )
            .recorder(&[]);
        let upgrade_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_file_upgrade_count",
                "Number of files upgraded by the compactor",
            )
            .recorder(&[]);
        let delete_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_file_delete_count",
                "Number of files deleted by the compactor",
            )
            .recorder(&[]);
        let commit_counter = registry
            .register_metric::<U64Counter>(
                "iox_compactor_file_commit_count",
                "Number of changes committed by the compactor. This is equivalent to the number of performend compaction tasks.",
            )
            .recorder(&[]);
        Self {
            create_counter,
            upgrade_counter,
            delete_counter,
            commit_counter,
            inner,
        }
    }
}

impl<T> Display for MetricsCommitWrapper<T>
where
    T: Commit,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics({})", self.inner)
    }
}

#[async_trait]
impl<T> Commit for MetricsCommitWrapper<T>
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
        let ids = self
            .inner
            .commit(partition_id, delete, upgrade, create, target_level)
            .await;

        self.create_counter.inc(ids.len() as u64);
        self.upgrade_counter.inc(upgrade.len() as u64);
        self.delete_counter.inc(delete.len() as u64);
        self.commit_counter.inc(1);

        ids
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metric::{Attributes, Metric};

    use crate::{
        components::commit::mock::{CommitHistoryEntry, MockCommit},
        test_util::ParquetFileBuilder,
    };

    use super::*;

    #[test]
    fn test_display() {
        let registry = Registry::new();
        let commit = MetricsCommitWrapper::new(MockCommit::new(), &registry);
        assert_eq!(commit.to_string(), "metrics(mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let registry = Registry::new();
        let inner = Arc::new(MockCommit::new());
        let commit = MetricsCommitWrapper::new(Arc::clone(&inner), &registry);

        let existing_1 = ParquetFileBuilder::new(1).build();
        let existing_2 = ParquetFileBuilder::new(2).build();
        let existing_3 = ParquetFileBuilder::new(3).build();
        let existing_4 = ParquetFileBuilder::new(4).build();

        let created = ParquetFileBuilder::new(1000).with_partition(1).build();

        assert_eq!(create_counter(&registry), 0);
        assert_eq!(upgrade_counter(&registry), 0);
        assert_eq!(delete_counter(&registry), 0);
        assert_eq!(commit_counter(&registry), 0);

        let ids = commit
            .commit(
                PartitionId::new(1),
                &[existing_1.clone()],
                &[existing_2.clone()],
                &[created.clone().into()],
                CompactionLevel::FileNonOverlapped,
            )
            .await;
        assert_eq!(ids, vec![ParquetFileId::new(1000)]);

        let ids = commit
            .commit(
                PartitionId::new(2),
                &[existing_2.clone(), existing_3.clone()],
                &[existing_4.clone()],
                &[],
                CompactionLevel::Final,
            )
            .await;
        assert_eq!(ids, vec![]);

        assert_eq!(create_counter(&registry), 1);
        assert_eq!(upgrade_counter(&registry), 2);
        assert_eq!(delete_counter(&registry), 3);
        assert_eq!(commit_counter(&registry), 2);

        assert_eq!(
            inner.history(),
            vec![
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![existing_1],
                    upgrade: vec![existing_2.clone()],
                    created: vec![created],
                    target_level: CompactionLevel::FileNonOverlapped,
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(2),
                    delete: vec![existing_2, existing_3],
                    upgrade: vec![existing_4],
                    created: vec![],
                    target_level: CompactionLevel::Final,
                },
            ]
        );
    }

    fn create_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_file_create_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }

    fn upgrade_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_file_upgrade_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }

    fn delete_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_file_delete_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }

    fn commit_counter(registry: &Registry) -> u64 {
        registry
            .get_instrument::<Metric<U64Counter>>("iox_compactor_file_commit_count")
            .expect("instrument not found")
            .get_observer(&Attributes::from([]))
            .expect("observer not found")
            .fetch()
    }
}
