use std::{collections::BTreeSet, sync::Arc};

use futures::{StreamExt, stream::FuturesOrdered};
use hashbrown::{HashMap, HashSet};
use object_store::ObjectStore;
use object_store_utils::RetryableObjectStore;
use observability_deps::tracing::{debug, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Semaphore;

use influxdb3_id::{DbId, ParquetFileId, TableId, TableIndexId};
use influxdb3_wal::SnapshotSequenceNumber;

use crate::{
    ParquetFile, PersistedSnapshot, PersistedSnapshotVersion,
    paths::{SnapshotInfoFilePath, TableIndexPath, TableIndexSnapshotPath},
};

#[derive(Debug, Error)]
pub enum TableIndexError {
    #[error("Failed to load table index from object store")]
    LoadIndex(#[source] object_store::Error),

    #[error("Failed to deserialize table index")]
    SerializeIndex(#[source] serde_json::Error),

    #[error("Failed to deserialize table index")]
    DeserializeIndex(#[source] serde_json::Error),

    #[error("Failed to list table index snapshots from object store")]
    ListSnapshots(#[source] object_store::Error),

    #[error("Failed to list table indices from object store")]
    ListIndices(#[source] object_store::Error),

    #[error("Failed to load table index snapshot from object store")]
    LoadSnapshot(#[source] object_store::Error),

    #[error("Failed to deserialize table index snapshot")]
    DeserializeSnapshot(#[source] serde_json::Error),

    #[error("Failed to persist table index to object store")]
    PersistIndex(#[source] object_store::Error),

    #[error("Failed to delete table index snapshot from object store")]
    DeleteSnapshot(#[source] object_store::Error),

    #[error("Cannot merge table indices with mismatched identifiers: {expected} != {actual}")]
    MergeMismatch {
        expected: TableIndexId,
        actual: TableIndexId,
    },

    #[error("Failed to parse table index path from object store path")]
    TableIndexPath(#[source] crate::paths::PathError),

    #[error("Failed to parse table index snapshot path from object store path")]
    TableIndexSnapshotPath(#[source] crate::paths::PathError),

    #[error("Failed to update table index: join task failed")]
    UpdateTaskFailed(#[source] tokio::task::JoinError),

    #[error("Object meta is missing filename")]
    MissingFilename,

    #[error("Failed to parse snapshot sequence number from filename")]
    InvalidSnapshotSequenceNumber,

    #[error("Table snapshot persistence task failed")]
    TableSnapshotPersistenceTaskFailed(#[source] tokio::task::JoinError),

    #[error("Unexpected error")]
    Unexpected(#[source] anyhow::Error),

    #[error("Object store operation failed")]
    ObjectStore(#[source] object_store::Error),

    #[error("JSON serialization/deserialization failed")]
    Json(#[source] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, TableIndexError>;

/// An partial, incremental index into a given database/table. Should not be used to make decisions
/// regarding gen1 file retention -- instead, all snapshots should be aggregated into a full
/// CoreTableIndex.
///
/// TableIndexSnapshots are expected to be created during the normal snapshotting process alongside
/// the full process-wide database + table index.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TableIndexSnapshot {
    #[serde(flatten)]
    pub id: TableIndexId,
    pub snapshot_sequence_number: SnapshotSequenceNumber,

    pub files: HashSet<ParquetFile>,
    pub removed_files: HashSet<ParquetFileId>,

    #[serde(flatten)]
    pub metadata: IndexMetadata,
}

impl From<PersistedSnapshotVersion> for Vec<TableIndexSnapshot> {
    fn from(psv: PersistedSnapshotVersion) -> Vec<TableIndexSnapshot> {
        match psv {
            PersistedSnapshotVersion::V1(ps) => ps.into(),
        }
    }
}

impl From<PersistedSnapshot> for Vec<TableIndexSnapshot> {
    fn from(ps: PersistedSnapshot) -> Vec<TableIndexSnapshot> {
        let node_id = ps.node_id.clone();
        let snapshot_sequence_number = ps.snapshot_sequence_number;

        let mut table_index_snapshots: HashMap<(DbId, TableId), TableIndexSnapshot> =
            HashMap::new();

        let new_tis_fn = |(db_id, table_id): &(DbId, TableId)| -> TableIndexSnapshot {
            TableIndexSnapshot {
                id: TableIndexId::new(node_id.clone(), *db_id, *table_id),
                snapshot_sequence_number,
                files: Default::default(),
                removed_files: Default::default(),
                metadata: IndexMetadata::empty(),
            }
        };

        for (db_id, db_tables) in &ps.removed_files {
            for (table_id, files) in &db_tables.tables {
                let tis = table_index_snapshots
                    .entry((*db_id, *table_id))
                    .or_insert_with_key(new_tis_fn);
                for file in files {
                    tis.metadata.parquet_size_bytes -= file.size_bytes as i64;
                    tis.metadata.row_count -= file.row_count as i64;
                    tis.removed_files.insert(file.id);
                }
            }
        }
        for (db_id, db_tables) in &ps.databases {
            for (table_id, files) in &db_tables.tables {
                let tis = table_index_snapshots
                    .entry((*db_id, *table_id))
                    .or_insert_with_key(new_tis_fn);
                for file in files {
                    // Only update metadata if this is a new file (not a duplicate)
                    if tis.files.insert(file.clone()) {
                        tis.metadata.merge_from_parquet_file(file);
                    }
                }
            }
        }

        table_index_snapshots.into_iter().map(|t| t.1).collect()
    }
}

impl TableIndexSnapshot {
    pub async fn persist(&self, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        let path = TableIndexSnapshotPath::new(
            self.id.node_id(),
            self.id.db_id().get(),
            self.id.table_id().get(),
            self.snapshot_sequence_number,
        );

        let json = serde_json::to_vec_pretty(self).map_err(TableIndexError::Json)?;

        object_store
            .put_with_default_retries(
                path.as_ref(),
                json.into(),
                format!(
                    "Persisting table index snapshot for table {} at sequence {}",
                    self.id.table_id(),
                    self.snapshot_sequence_number
                ),
            )
            .await
            .map_err(TableIndexError::ObjectStore)?;
        Ok(())
    }

    /// Load a persisted snapshot from object store and split it into table index snapshots
    pub async fn load_split_persist(
        object_store: Arc<dyn ObjectStore>,
        location: SnapshotInfoFilePath,
    ) -> Result<()> {
        debug!(?location, "loading persisted snapshot from object store");

        let bytes = object_store
            .get_with_default_retries(
                location.as_ref(),
                format!("Loading persisted snapshot from {}", location.as_ref()),
            )
            .await
            .map_err(TableIndexError::ObjectStore)?
            .bytes()
            .await
            .map_err(TableIndexError::ObjectStore)?;

        let snapshot: PersistedSnapshotVersion =
            serde_json::from_slice(&bytes).map_err(TableIndexError::Json)?;

        let table_snapshots: Vec<TableIndexSnapshot> = snapshot.into();

        for table_snapshot in table_snapshots {
            table_snapshot.persist(Arc::clone(&object_store)).await?;
        }

        Ok(())
    }
}

/// Trait for accessing table index data.
#[async_trait::async_trait]
pub trait TableIndex: Send + Sync {
    /// Get the full table identifier
    async fn id(&self) -> TableIndexId;

    /// Get the latest snapshot sequence number
    async fn latest_snapshot_sequence_number(&self) -> SnapshotSequenceNumber;

    /// Get the index metadata
    async fn metadata(&self) -> IndexMetadata;

    /// Get an iterator over parquet files in this index.
    async fn parquet_files(
        &self,
    ) -> Box<dyn Iterator<Item = Arc<ParquetFile>> + Send + Sync + 'static>;
}

/// A full index into the gen 1 parquet files for a given table on a given ingester.
///
/// The latest snapshot sequence number can be compared with next value of the global static
/// initializer to determine how far out of date it is. It may be necessary have the most
/// up-to-date values for some usages but not all; if it is necessary, then roll up all outstanding
/// snapshots before executing.
///
/// The advantage of using a CoreTableIndex in this way is that it significantly reduces the memory
/// cost of performing table-specific operations such as applying retention periods or hard
/// deletion to gen1 files.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct CoreTableIndex {
    #[serde(flatten)]
    pub id: TableIndexId,

    pub files: BTreeSet<Arc<ParquetFile>>,

    pub latest_snapshot_sequence_number: SnapshotSequenceNumber,

    #[serde(flatten)]
    pub metadata: IndexMetadata,
}

impl CoreTableIndex {
    pub(crate) fn merge(&mut self, other: Self) -> Result<()> {
        // Validate that both indices refer to the same node/db/table
        if self.id != other.id {
            return Err(TableIndexError::MergeMismatch {
                expected: self.id.clone(),
                actual: other.id.clone(),
            });
        }

        self.metadata.merge(&other.metadata);
        self.files.extend(other.files);
        self.latest_snapshot_sequence_number = self
            .latest_snapshot_sequence_number
            .max(other.latest_snapshot_sequence_number);

        Ok(())
    }

    pub(crate) async fn persist(&self, object_store: Arc<dyn ObjectStore>) -> Result<()> {
        let path = TableIndexPath::new(
            self.id.node_id(),
            self.id.db_id().get(),
            self.id.table_id().get(),
        );

        let json = serde_json::to_vec_pretty(self).map_err(TableIndexError::SerializeIndex)?;

        object_store
            .put_with_default_retries(
                path.as_ref(),
                json.into(),
                format!(
                    "Persisting core table index for table {}",
                    self.id.table_id()
                ),
            )
            .await
            .map_err(TableIndexError::PersistIndex)?;
        Ok(())
    }

    pub(crate) async fn from_object_store(
        object_store: Arc<dyn ObjectStore>,
        path: &TableIndexPath,
    ) -> Result<Self> {
        let mut found = false;

        // Try to load existing CoreTableIndex from object store
        let mut table_index = match object_store
            .get_with_default_retries(
                path.as_ref(),
                format!("Loading table index for {}", path.full_table_id()),
            )
            .await
        {
            Ok(result) => {
                found = true;
                let index_bytes = result.bytes().await.map_err(TableIndexError::LoadIndex)?;
                serde_json::from_slice(&index_bytes).map_err(TableIndexError::DeserializeIndex)?
            }
            Err(object_store::Error::NotFound { .. }) => {
                // Create empty CoreTableIndex if not found
                Self {
                    id: path.full_table_id(),
                    files: BTreeSet::new(),
                    latest_snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                    metadata: IndexMetadata::empty(),
                }
            }
            Err(e) => return Err(TableIndexError::LoadIndex(e)),
        };

        // Update from any newer snapshots with default concurrency
        table_index
            .update_from_object_store(Arc::clone(&object_store), 10)
            .await?;

        // If the index didn't exist before, persist it now
        if !found {
            let json = serde_json::to_vec_pretty(&table_index)
                .map_err(TableIndexError::DeserializeIndex)?;

            object_store
                .put_with_default_retries(
                    path.as_ref(),
                    json.into(),
                    format!("Initial persist of table index for {}", table_index.id),
                )
                .await
                .map_err(TableIndexError::PersistIndex)?;
        }

        Ok(table_index)
    }

    pub(crate) async fn update_from_object_store(
        &mut self,
        object_store: Arc<dyn ObjectStore>,
        concurrency: usize,
    ) -> Result<()> {
        let prefix = TableIndexSnapshotPath::prefix(
            self.id.node_id(),
            self.id.db_id().get(),
            self.id.table_id().get(),
        );

        // List all snapshots for this table
        let objects: Vec<object_store::ObjectMeta> = object_store
            .list_with_default_retries(
                Some(&prefix),
                None,
                format!(
                    "Listing table index snapshots for table {}",
                    self.id.table_id()
                ),
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(TableIndexError::ListSnapshots)?;

        // Filter and collect snapshots newer than our current sequence number
        let mut newer_snapshots = Vec::new();
        for meta in objects {
            // Extract sequence number from filename
            let snapshot_seq =
                TableIndexSnapshotPath::parse_sequence_number(meta.location.to_string().as_str())
                    .expect("snapshot filenames should have valid sequence numbers");
            if snapshot_seq > self.latest_snapshot_sequence_number {
                newer_snapshots.push((snapshot_seq, meta));
            }
        }

        if newer_snapshots.is_empty() {
            return Ok(());
        }

        // Sort by sequence number to ensure we apply in order
        newer_snapshots.sort_by_key(|(seq, _)| *seq);

        // Load snapshots concurrently with limited concurrency
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let object_store_clone = Arc::clone(&object_store);

        let mut futures = FuturesOrdered::new();
        for (seq, meta) in &newer_snapshots {
            let sem = Arc::clone(&semaphore);
            let store = Arc::clone(&object_store_clone);
            let location = meta.location.clone();
            let sequence = *seq;

            futures.push_back(async move {
                let _permit = sem.acquire().await.unwrap();

                let result = match store
                    .get_with_default_retries(
                        &location,
                        format!("Loading table index snapshot at sequence {}", sequence),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(object_store::Error::NotFound { path, source }) => {
                        // Snapshot may have been deleted by another concurrent operation (e.g.,
                        // another task that already merged and cleaned up this snapshot). While
                        // this isn't typically expected, we play it safe here to avoid spurious
                        // table index cache loading failures.
                        warn!(
                            %path,
                            %sequence,
                            "Snapshot not found, likely already merged by concurrent operation"
                        );
                        let _ = source; // acknowledge the source error
                        return Ok(None);
                    }
                    Err(e) => return Err(TableIndexError::LoadSnapshot(e)),
                };

                let snapshot_bytes = result
                    .bytes()
                    .await
                    .map_err(TableIndexError::LoadSnapshot)?;
                let snapshot: TableIndexSnapshot = serde_json::from_slice(&snapshot_bytes)
                    .map_err(TableIndexError::DeserializeSnapshot)?;

                Ok::<Option<(SnapshotSequenceNumber, TableIndexSnapshot)>, TableIndexError>(Some((
                    sequence, snapshot,
                )))
            });
        }

        // Collect loaded snapshots in order, skipping any that were already deleted
        let mut loaded_snapshots = Vec::new();
        while let Some(result) = futures.next().await {
            if let Some((seq, snapshot)) = result? {
                loaded_snapshots.push(snapshot);
                self.latest_snapshot_sequence_number = seq;
            }
        }

        // Merge snapshots into current index
        if !loaded_snapshots.is_empty() {
            let (new_index, removed) = CoreTableIndex::from_snapshots(&self.id, loaded_snapshots)?;
            self.merge(new_index)?;
            self.prune_removed(&removed);
        }

        // Persist updated index
        let path = TableIndexPath::new(
            self.id.node_id(),
            self.id.db_id().get(),
            self.id.table_id().get(),
        );
        let json = serde_json::to_vec_pretty(self).map_err(TableIndexError::DeserializeIndex)?;

        object_store
            .put_with_default_retries(
                path.as_ref(),
                json.into(),
                format!(
                    "Persisting updated table index for table {}",
                    self.id.table_id()
                ),
            )
            .await
            .map_err(TableIndexError::PersistIndex)?;

        // Delete merged snapshots concurrently
        let delete_futures: Vec<_> = newer_snapshots
            .into_iter()
            .map(|(_, meta)| {
                let store = Arc::clone(&object_store);
                let location = meta.location;
                async move {
                    store
                        .delete_with_default_retries(
                            &location,
                            format!("Deleting merged table index snapshot at {}", location),
                        )
                        .await
                        .map_err(TableIndexError::DeleteSnapshot)
                }
            })
            .collect();

        // Wait for all deletions to complete
        let results = futures::future::join_all(delete_futures).await;
        for result in results {
            result?;
        }

        Ok(())
    }

    pub(crate) fn prune_removed(&mut self, removed: &HashSet<ParquetFileId>) {
        let mut new_metadata = IndexMetadata::empty();
        self.files.retain(|item| {
            if removed.contains(&item.id) {
                false
            } else {
                new_metadata.merge_from_parquet_file(item.as_ref());
                true
            }
        });
        self.metadata = new_metadata;
    }

    pub(crate) fn from_snapshots(
        id: &TableIndexId,
        snapshots: Vec<TableIndexSnapshot>,
    ) -> Result<(Self, HashSet<ParquetFileId>)> {
        let mut removed: HashSet<ParquetFileId> = HashSet::new();
        let mut latest_snapshot_seq = SnapshotSequenceNumber::new(0);

        for snapshot in &snapshots {
            // Validate that both indices refer to the same node/db/table
            if id != &snapshot.id {
                return Err(TableIndexError::MergeMismatch {
                    expected: id.clone(),
                    actual: snapshot.id.clone(),
                });
            }
            removed.extend(&snapshot.removed_files);
            latest_snapshot_seq = latest_snapshot_seq.max(snapshot.snapshot_sequence_number);
        }

        let mut metadata = IndexMetadata::empty();
        let files = snapshots
            .into_iter()
            .flat_map(|s| s.files.into_iter().filter(|f| !removed.contains(&f.id)))
            .map(|file| {
                metadata.merge_from_parquet_file(&file);
                Arc::new(file)
            })
            .collect();

        let new_index = Self {
            id: id.clone(),
            files,
            latest_snapshot_sequence_number: latest_snapshot_seq,
            metadata,
        };
        Ok((new_index, removed))
    }
}

#[async_trait::async_trait]
impl TableIndex for CoreTableIndex {
    async fn id(&self) -> TableIndexId {
        self.id.clone()
    }

    async fn latest_snapshot_sequence_number(&self) -> SnapshotSequenceNumber {
        self.latest_snapshot_sequence_number
    }

    async fn metadata(&self) -> IndexMetadata {
        self.metadata
    }

    async fn parquet_files(
        &self,
    ) -> Box<dyn Iterator<Item = Arc<ParquetFile>> + Send + Sync + 'static> {
        Box::new(self.files.clone().into_iter())
    }
}

/// Metadata used for either a [`CoreTableIndex`] or a [`TableIndexSnapshot`].
///
/// The primary way we expect to construct this is from a reference to a [`ParquetFile`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexMetadata {
    /// The size of the snapshot parquet files in bytes.
    pub parquet_size_bytes: i64,
    /// The number of rows across all parquet files in the snapshot.
    pub row_count: i64,
    /// The min time from all parquet files in the snapshot.
    pub min_time: i64,
    /// The max time from all parquet files in the snapshot.
    pub max_time: i64,
}

impl IndexMetadata {
    pub(crate) fn empty() -> Self {
        IndexMetadata {
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: 0,
        }
    }

    pub(crate) fn merge_from_parquet_file(&mut self, pf: &ParquetFile) {
        *self = Self {
            parquet_size_bytes: self.parquet_size_bytes + pf.size_bytes as i64,
            row_count: self.row_count + pf.row_count as i64,
            min_time: self.min_time.min(pf.min_time),
            max_time: self.max_time.max(pf.max_time),
        };
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        *self = Self {
            parquet_size_bytes: self.parquet_size_bytes + other.parquet_size_bytes,
            row_count: self.row_count + other.row_count,
            min_time: self.min_time.min(other.min_time),
            max_time: self.max_time.max(other.max_time),
        };
    }
}

#[cfg(test)]
mod test_persisted_snapshot_conversion;
#[cfg(test)]
pub(crate) mod test_table_index_operations;
