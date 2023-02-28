//! Handles recording commit information to the test run log

use async_trait::async_trait;
use compactor2::{Commit, CommitWrapper};
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};

use crate::display::ParquetFileInfo;

/// Records catalog operations to a shared run_log during tests
#[derive(Debug)]
struct CommitRecorder {
    /// The inner commit that does the work
    inner: Arc<(dyn Commit)>,

    /// a log of what happened during this test
    run_log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Commit for CommitRecorder {
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Vec<ParquetFileId> {
        // lock scope
        {
            let mut run_log = self.run_log.lock().unwrap();

            run_log.push(format!("Committing partition {partition_id}:"));

            if !delete.is_empty() {
                run_log.push(format!(
                    "  Soft Deleting {} files: {}",
                    delete.len(),
                    id_list(delete)
                ));
            }

            if !upgrade.is_empty() {
                run_log.push(format!(
                    "  Upgrading {} files level to {}: {}",
                    upgrade.len(),
                    target_level,
                    id_list(upgrade)
                ));
            }

            if !create.is_empty() {
                run_log.push(format!(
                    "  Creating {} files at level {}",
                    create.len(),
                    target_level
                ));
            }
        }
        self.inner
            .commit(partition_id, delete, upgrade, create, target_level)
            .await
    }
}

fn id_list(files: &[ParquetFile]) -> String {
    //  sort by id to get a more consistent display
    let mut files: Vec<_> = files.iter().collect();
    files.sort_by_key(|f| f.id);

    let files: Vec<_> = files.iter().map(|f| f.display_id()).collect();
    files.join(", ")
}

impl Display for CommitRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitRecorder")
    }
}

/// Wraps another commit reference in a CommitRecorder
#[derive(Debug)]
pub(crate) struct CommitRecorderBuilder {
    /// a shared log of what happened during a simulated run
    run_log: Arc<Mutex<Vec<String>>>,
}

impl CommitRecorderBuilder {
    pub fn new(run_log: Arc<Mutex<Vec<String>>>) -> Self {
        Self { run_log }
    }
}

impl CommitWrapper for CommitRecorderBuilder {
    fn wrap(&self, inner: Arc<(dyn Commit)>) -> Arc<(dyn Commit + 'static)> {
        let run_log = Arc::clone(&self.run_log);
        Arc::new(CommitRecorder { inner, run_log })
    }
}
