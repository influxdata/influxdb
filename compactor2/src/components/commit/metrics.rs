use std::fmt::Display;

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use metric::{Registry, U64Histogram, U64HistogramOptions};

use super::Commit;

#[derive(Debug, Clone, Copy)]
enum HistogramType {
    Bytes,
    Files,
    Rows,
}

impl From<HistogramType> for U64HistogramOptions {
    fn from(value: HistogramType) -> Self {
        match value {
            HistogramType::Bytes => Self::new([
                1_000,
                10_000,
                100_000,
                1_000_000,
                10_000_000,
                100_000_000,
                1_000_000_000,
                10_000_000_000,
                u64::MAX,
            ]),
            HistogramType::Files => Self::new([1, 10, 100, 1_000, 10_000, u64::MAX]),
            HistogramType::Rows => Self::new([
                100,
                1_000,
                10_000,
                100_000,
                1_000_000,
                10_000_000,
                100_000_000,
                u64::MAX,
            ]),
        }
    }
}

#[derive(Debug)]
struct Histogram {
    create: U64Histogram,
    delete: U64Histogram,
    upgrade: U64Histogram,
}

impl Histogram {
    fn new(
        registry: &Registry,
        name: &'static str,
        description: &'static str,
        t: HistogramType,
    ) -> Self {
        let metric =
            registry
                .register_metric_with_options::<U64Histogram, _>(name, description, || t.into());
        let create = metric.recorder(&[("op", "create")]);
        let delete = metric.recorder(&[("op", "delete")]);
        let upgrade = metric.recorder(&[("op", "upgrade")]);
        Self {
            create,
            delete,
            upgrade,
        }
    }
}

#[derive(Debug)]
pub struct MetricsCommitWrapper<T>
where
    T: Commit,
{
    file_bytes: Histogram,
    file_rows: Histogram,
    job_files: Histogram,
    job_bytes: Histogram,
    job_rows: Histogram,
    inner: T,
}

const METRIC_NAME_FILE_BYTES: &str = "iox_compactor_commit_file_bytes";
const METRIC_NAME_FILE_ROWS: &str = "iox_compactor_commit_file_rows";
const METRIC_NAME_JOB_FILES: &str = "iox_compactor_commit_job_files";
const METRIC_NAME_JOB_BYTES: &str = "iox_compactor_commit_job_bytes";
const METRIC_NAME_JOB_ROWS: &str = "iox_compactor_commit_job_rows";

impl<T> MetricsCommitWrapper<T>
where
    T: Commit,
{
    pub fn new(inner: T, registry: &Registry) -> Self {
        Self {
            file_bytes: Histogram::new(
                registry,
                METRIC_NAME_FILE_BYTES,
                "Number of bytes committed by the compactor, per file",
                HistogramType::Bytes,
            ),
            file_rows: Histogram::new(
                registry,
                METRIC_NAME_FILE_ROWS,
                "Number of rows committed by the compactor, per file",
                HistogramType::Rows,
            ),
            job_files: Histogram::new(
                registry,
                METRIC_NAME_JOB_FILES,
                "Number of files committed by the compactor, per job",
                HistogramType::Files,
            ),
            job_bytes: Histogram::new(
                registry,
                METRIC_NAME_JOB_BYTES,
                "Number of bytes committed by the compactor, per job",
                HistogramType::Bytes,
            ),
            job_rows: Histogram::new(
                registry,
                METRIC_NAME_JOB_ROWS,
                "Number of rows committed by the compactor, per job",
                HistogramType::Rows,
            ),
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

        // per file metrics
        for f in create {
            self.file_bytes.create.record(f.file_size_bytes as u64);
            self.file_rows.create.record(f.row_count as u64);
        }
        for f in delete {
            self.file_bytes.delete.record(f.file_size_bytes as u64);
            self.file_rows.delete.record(f.row_count as u64);
        }
        for f in upgrade {
            self.file_bytes.upgrade.record(f.file_size_bytes as u64);
            self.file_rows.upgrade.record(f.row_count as u64);
        }

        // per-partition metrics
        self.job_files.create.record(create.len() as u64);
        self.job_files.delete.record(delete.len() as u64);
        self.job_files.upgrade.record(upgrade.len() as u64);
        self.job_bytes
            .create
            .record(create.iter().map(|f| f.file_size_bytes as u64).sum::<u64>());
        self.job_bytes
            .delete
            .record(delete.iter().map(|f| f.file_size_bytes as u64).sum::<u64>());
        self.job_bytes.upgrade.record(
            upgrade
                .iter()
                .map(|f| f.file_size_bytes as u64)
                .sum::<u64>(),
        );
        self.job_rows
            .create
            .record(create.iter().map(|f| f.row_count as u64).sum::<u64>());
        self.job_rows
            .delete
            .record(delete.iter().map(|f| f.row_count as u64).sum::<u64>());
        self.job_rows
            .upgrade
            .record(upgrade.iter().map(|f| f.row_count as u64).sum::<u64>());

        ids
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metric::{Attributes, HistogramObservation, Metric};

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

        let existing_1 = ParquetFileBuilder::new(1)
            .with_file_size_bytes(10_001)
            .with_row_count(1_001)
            .build();
        let existing_2 = ParquetFileBuilder::new(2)
            .with_file_size_bytes(10_002)
            .with_row_count(1_002)
            .build();
        let existing_3 = ParquetFileBuilder::new(3)
            .with_file_size_bytes(10_004)
            .with_row_count(1_004)
            .build();
        let existing_4 = ParquetFileBuilder::new(4)
            .with_file_size_bytes(10_008)
            .with_row_count(1_008)
            .build();

        let created = ParquetFileBuilder::new(1000)
            .with_file_size_bytes(10_016)
            .with_row_count(1_016)
            .with_partition(1)
            .build();

        for metric_name in [
            METRIC_NAME_FILE_BYTES,
            METRIC_NAME_FILE_ROWS,
            METRIC_NAME_JOB_BYTES,
            METRIC_NAME_JOB_FILES,
            METRIC_NAME_JOB_ROWS,
        ] {
            for op in ["create", "delete", "upgrade"] {
                assert_eq!(hist_count(&registry, metric_name, op), 0);
                assert_eq!(hist_total(&registry, metric_name, op), 0);
            }
        }

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

        assert_eq!(hist_count(&registry, METRIC_NAME_FILE_BYTES, "create"), 1);
        assert_eq!(hist_count(&registry, METRIC_NAME_FILE_BYTES, "upgrade"), 2);
        assert_eq!(hist_count(&registry, METRIC_NAME_FILE_BYTES, "delete"), 3);
        assert_eq!(
            hist_total(&registry, METRIC_NAME_FILE_BYTES, "create"),
            10_016
        );
        assert_eq!(
            hist_total(&registry, METRIC_NAME_FILE_BYTES, "upgrade"),
            20_010
        );
        assert_eq!(
            hist_total(&registry, METRIC_NAME_FILE_BYTES, "delete"),
            30_007
        );

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

    fn hist(
        registry: &Registry,
        metric_name: &'static str,
        op: &'static str,
    ) -> HistogramObservation<u64> {
        registry
            .get_instrument::<Metric<U64Histogram>>(metric_name)
            .expect("instrument not found")
            .get_observer(&Attributes::from(&[("op", op)]))
            .expect("observer not found")
            .fetch()
    }

    fn hist_count(registry: &Registry, metric_name: &'static str, op: &'static str) -> u64 {
        hist(registry, metric_name, op).sample_count()
    }

    fn hist_total(registry: &Registry, metric_name: &'static str, op: &'static str) -> u64 {
        hist(registry, metric_name, op).total
    }
}
