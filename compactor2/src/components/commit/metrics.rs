use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use itertools::Itertools;
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
    /// Files created, by the level they were created at.
    create: HashMap<CompactionLevel, U64Histogram>,

    /// Files deleted, by the level they had at this point in time.
    delete: HashMap<CompactionLevel, U64Histogram>,

    /// Files upgraded, by level they had before the upgrade and the target compaction level.
    upgrade: HashMap<(CompactionLevel, CompactionLevel), U64Histogram>,
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
        let create = CompactionLevel::all()
            .iter()
            .map(|level| {
                (
                    *level,
                    metric.recorder(&[("op", "create"), ("level", level.name())]),
                )
            })
            .collect();
        let delete = CompactionLevel::all()
            .iter()
            .map(|level| {
                (
                    *level,
                    metric.recorder(&[("op", "delete"), ("level", level.name())]),
                )
            })
            .collect();
        let upgrade = CompactionLevel::all()
            .iter()
            .cartesian_product(CompactionLevel::all())
            .map(|(from, to)| {
                (
                    (*from, *to),
                    metric.recorder(&[("op", "upgrade"), ("from", from.name()), ("to", to.name())]),
                )
            })
            .collect();
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
            self.file_bytes
                .create
                .get(&f.compaction_level)
                .expect("all compaction levels covered")
                .record(f.file_size_bytes as u64);
            self.file_rows
                .create
                .get(&f.compaction_level)
                .expect("all compaction levels covered")
                .record(f.row_count as u64);
        }
        for f in delete {
            self.file_bytes
                .delete
                .get(&f.compaction_level)
                .expect("all compaction levels covered")
                .record(f.file_size_bytes as u64);
            self.file_rows
                .delete
                .get(&f.compaction_level)
                .expect("all compaction levels covered")
                .record(f.row_count as u64);
        }
        for f in upgrade {
            self.file_bytes
                .upgrade
                .get(&(f.compaction_level, target_level))
                .expect("all compaction levels covered")
                .record(f.file_size_bytes as u64);
            self.file_rows
                .upgrade
                .get(&(f.compaction_level, target_level))
                .expect("all compaction levels covered")
                .record(f.row_count as u64);
        }

        // per-partition metrics
        for file_level in CompactionLevel::all() {
            let create = create
                .iter()
                .filter(|f| f.compaction_level == *file_level)
                .collect::<Vec<_>>();
            let delete = delete
                .iter()
                .filter(|f| f.compaction_level == *file_level)
                .collect::<Vec<_>>();
            let upgrade = upgrade
                .iter()
                .filter(|f| f.compaction_level == *file_level)
                .collect::<Vec<_>>();

            self.job_files
                .create
                .get(file_level)
                .expect("all compaction levels covered")
                .record(create.len() as u64);
            self.job_bytes
                .create
                .get(file_level)
                .expect("all compaction levels covered")
                .record(create.iter().map(|f| f.file_size_bytes as u64).sum::<u64>());
            self.job_rows
                .create
                .get(file_level)
                .expect("all compaction levels covered")
                .record(create.iter().map(|f| f.row_count as u64).sum::<u64>());

            self.job_files
                .delete
                .get(file_level)
                .expect("all compaction levels covered")
                .record(delete.len() as u64);
            self.job_bytes
                .delete
                .get(file_level)
                .expect("all compaction levels covered")
                .record(delete.iter().map(|f| f.file_size_bytes as u64).sum::<u64>());
            self.job_rows
                .delete
                .get(file_level)
                .expect("all compaction levels covered")
                .record(delete.iter().map(|f| f.row_count as u64).sum::<u64>());

            self.job_files
                .upgrade
                .get(&(*file_level, target_level))
                .expect("all compaction levels covered")
                .record(upgrade.len() as u64);
            self.job_bytes
                .upgrade
                .get(&(*file_level, target_level))
                .expect("all compaction levels covered")
                .record(
                    upgrade
                        .iter()
                        .map(|f| f.file_size_bytes as u64)
                        .sum::<u64>(),
                );
            self.job_rows
                .upgrade
                .get(&(*file_level, target_level))
                .expect("all compaction levels covered")
                .record(upgrade.iter().map(|f| f.row_count as u64).sum::<u64>());
        }

        ids
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metric::{Attributes, HistogramObservation, Metric};

    use crate::components::commit::mock::{CommitHistoryEntry, MockCommit};
    use iox_tests::ParquetFileBuilder;

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
        let existing_2a = ParquetFileBuilder::new(2)
            .with_file_size_bytes(10_002)
            .with_row_count(1_002)
            .with_compaction_level(CompactionLevel::Initial)
            .build();
        let existing_2b = ParquetFileBuilder::from(existing_2a.clone())
            .with_compaction_level(CompactionLevel::FileNonOverlapped)
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
            .with_compaction_level(CompactionLevel::Initial)
            .build();

        for metric_name in [
            METRIC_NAME_FILE_BYTES,
            METRIC_NAME_FILE_ROWS,
            METRIC_NAME_JOB_BYTES,
            METRIC_NAME_JOB_FILES,
            METRIC_NAME_JOB_ROWS,
        ] {
            for file_level in CompactionLevel::all() {
                for op in ["create", "delete"] {
                    assert_eq!(
                        hist_count(
                            &registry,
                            metric_name,
                            [("op", op), ("level", file_level.name())]
                        ),
                        0
                    );
                    assert_eq!(
                        hist_total(
                            &registry,
                            metric_name,
                            [("op", op), ("level", file_level.name())]
                        ),
                        0
                    );
                }

                for target_level in CompactionLevel::all() {
                    assert_eq!(
                        hist_count(
                            &registry,
                            metric_name,
                            [
                                ("op", "upgrade"),
                                ("from", file_level.name()),
                                ("to", target_level.name())
                            ]
                        ),
                        0
                    );
                    assert_eq!(
                        hist_total(
                            &registry,
                            metric_name,
                            [
                                ("op", "upgrade"),
                                ("from", file_level.name()),
                                ("to", target_level.name())
                            ]
                        ),
                        0
                    );
                }
            }
        }

        let ids = commit
            .commit(
                PartitionId::new(1),
                &[existing_1.clone()],
                &[existing_2a.clone()],
                &[created.clone().into()],
                CompactionLevel::FileNonOverlapped,
            )
            .await;
        assert_eq!(ids, vec![ParquetFileId::new(1000)]);

        let ids = commit
            .commit(
                PartitionId::new(2),
                &[existing_2b.clone(), existing_3.clone()],
                &[existing_4.clone()],
                &[],
                CompactionLevel::Final,
            )
            .await;
        assert_eq!(ids, vec![]);

        assert_eq!(
            hist_count(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "create"), ("level", "L0")]
            ),
            1
        );
        assert_eq!(
            hist_count(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "upgrade"), ("from", "L0"), ("to", "L1")]
            ),
            1
        );
        assert_eq!(
            hist_count(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "upgrade"), ("from", "L1"), ("to", "L2")]
            ),
            1
        );
        assert_eq!(
            hist_count(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "delete"), ("level", "L1")]
            ),
            3
        );
        assert_eq!(
            hist_total(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "create"), ("level", "L0")]
            ),
            10_016
        );
        assert_eq!(
            hist_total(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "upgrade"), ("from", "L0"), ("to", "L1")]
            ),
            10_002
        );
        assert_eq!(
            hist_total(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "upgrade"), ("from", "L1"), ("to", "L2")]
            ),
            10_008
        );
        assert_eq!(
            hist_total(
                &registry,
                METRIC_NAME_FILE_BYTES,
                [("op", "delete"), ("level", "L1")]
            ),
            30_007
        );

        assert_eq!(
            inner.history(),
            vec![
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![existing_1],
                    upgrade: vec![existing_2a.clone()],
                    created: vec![created],
                    target_level: CompactionLevel::FileNonOverlapped,
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(2),
                    delete: vec![existing_2b, existing_3],
                    upgrade: vec![existing_4],
                    created: vec![],
                    target_level: CompactionLevel::Final,
                },
            ]
        );
    }

    fn hist<const N: usize>(
        registry: &Registry,
        metric_name: &'static str,
        attributes: [(&'static str, &'static str); N],
    ) -> HistogramObservation<u64> {
        registry
            .get_instrument::<Metric<U64Histogram>>(metric_name)
            .expect("instrument not found")
            .get_observer(&Attributes::from(&attributes))
            .expect("observer not found")
            .fetch()
    }

    fn hist_count<const N: usize>(
        registry: &Registry,
        metric_name: &'static str,
        attributes: [(&'static str, &'static str); N],
    ) -> u64 {
        hist(registry, metric_name, attributes).sample_count()
    }

    fn hist_total<const N: usize>(
        registry: &Registry,
        metric_name: &'static str,
        attributes: [(&'static str, &'static str); N],
    ) -> u64 {
        hist(registry, metric_name, attributes).total
    }
}
