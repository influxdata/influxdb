use std::{collections::BTreeSet, sync::Arc};

use futures::{StreamExt, stream::FuturesOrdered};
use hashbrown::{HashMap, HashSet};
use object_store::ObjectStore;
use object_store_utils::RetryableObjectStore;
use observability_deps::tracing::debug;
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

                let result = store
                    .get_with_default_retries(
                        &location,
                        format!("Loading table index snapshot at sequence {}", sequence),
                    )
                    .await
                    .map_err(TableIndexError::LoadSnapshot)?;

                let snapshot_bytes = result
                    .bytes()
                    .await
                    .map_err(TableIndexError::LoadSnapshot)?;
                let snapshot: TableIndexSnapshot = serde_json::from_slice(&snapshot_bytes)
                    .map_err(TableIndexError::DeserializeSnapshot)?;

                Ok::<(SnapshotSequenceNumber, TableIndexSnapshot), TableIndexError>((
                    sequence, snapshot,
                ))
            });
        }

        // Collect loaded snapshots in order
        let mut loaded_snapshots = Vec::new();
        while let Some(result) = futures.next().await {
            let (seq, snapshot) = result?;
            loaded_snapshots.push(snapshot);
            self.latest_snapshot_sequence_number = seq;
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
mod test_persisted_snapshot_conversion {
    use super::*;

    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use rstest::rstest;

    use crate::{DatabaseTables, ParquetFile, PersistedSnapshot};

    #[derive(Default)]
    struct CreateTestParquetFileArgs {
        /// Optional file ID; if None, a new ID will be generated
        file_id: Option<ParquetFileId>,
        /// The path for the parquet file
        path: Option<String>,
        /// Size in bytes (default: 100)
        size_bytes: Option<u64>,
        /// Number of rows (default: 10,000)
        row_count: Option<u64>,
        /// Time offset in minutes for calculating timestamps
        time_offset_minutes: i64,
    }

    // Helper function to create a test ParquetFile
    fn create_test_parquet_file(args: CreateTestParquetFileArgs) -> ParquetFile {
        static MINUTE_NS: i64 = 10_i64.pow(9) * 60 * 10;
        let time_offset = args.time_offset_minutes;
        ParquetFile {
            id: args.file_id.unwrap_or_default(),
            path: args.path.unwrap_or_else(|| "whatever".to_string()),
            size_bytes: args.size_bytes.unwrap_or(100),
            row_count: args.row_count.unwrap_or(10_000),
            chunk_time: time_offset * MINUTE_NS,
            min_time: time_offset * MINUTE_NS,
            max_time: (time_offset + 1) * MINUTE_NS,
        }
    }

    struct CreateTestDatabaseTablesArgs {
        /// Set of (database_id, table_id) tuples to create tables for
        table_ids: HashSet<(DbId, TableId)>,
        /// Number of parquet files to create per table
        files_per_table: usize,
        /// Base time offset in minutes for file timestamps
        base_time_offset_minutes: i64,
    }

    // Helper function to create a SerdeVecMap with database tables
    fn create_database_tables(
        args: CreateTestDatabaseTablesArgs,
    ) -> SerdeVecMap<DbId, DatabaseTables> {
        let mut databases: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
        for (db_id, table_id) in &args.table_ids {
            let db_tables = databases.entry(*db_id).or_default();
            let parquet_files = db_tables.tables.entry(*table_id).or_default();
            for _ in 0..args.files_per_table {
                parquet_files.push(create_test_parquet_file(CreateTestParquetFileArgs {
                    time_offset_minutes: args.base_time_offset_minutes,
                    ..Default::default()
                }));
            }
        }
        databases
    }

    struct CreateTestPersistedSnapshotArgs {
        /// Snapshot sequence number
        sequence_number: u64,
        /// Node ID (default: "test_host")
        node_id: Option<String>,
        /// Database tables with files to add in this snapshot
        added_tables: Option<SerdeVecMap<DbId, DatabaseTables>>,
        /// Database tables with files to remove in this snapshot
        removed_tables: Option<SerdeVecMap<DbId, DatabaseTables>>,
    }

    impl Default for CreateTestPersistedSnapshotArgs {
        fn default() -> Self {
            Self {
                sequence_number: 1,
                node_id: None,
                added_tables: None,
                removed_tables: None,
            }
        }
    }

    // Helper function to create a PersistedSnapshot
    fn create_persisted_snapshot(args: CreateTestPersistedSnapshotArgs) -> PersistedSnapshot {
        let databases = args.added_tables.unwrap_or_default();
        let removed_files = args.removed_tables.unwrap_or_default();

        // Calculate min/max times from files
        let mut min_time = i64::MAX;
        let mut max_time = 0;

        for (_, tables) in &databases {
            for (_, files) in &tables.tables {
                for file in files {
                    min_time = min_time.min(file.min_time);
                    max_time = max_time.max(file.max_time);
                }
            }
        }

        if min_time == i64::MAX {
            min_time = 0;
        }

        PersistedSnapshot {
            node_id: args.node_id.unwrap_or_else(|| "test_host".to_string()),
            next_file_id: ParquetFileId::from(args.sequence_number),
            snapshot_sequence_number: SnapshotSequenceNumber::new(args.sequence_number),
            wal_file_sequence_number: WalFileSequenceNumber::new(args.sequence_number),
            catalog_sequence_number: CatalogSequenceNumber::new(args.sequence_number),
            databases,
            removed_files,
            min_time,
            max_time,
            row_count: 0,
            parquet_size_bytes: 0,
        }
    }

    #[derive(Debug)]
    struct ExpectedTableSnapshot {
        db_id: DbId,
        table_id: TableId,
        expected_file_count: usize,
        expected_removed_file_count: usize,
        expected_metadata: ExpectedMetadata,
    }

    #[derive(Debug)]
    struct ExpectedMetadata {
        parquet_size_bytes: Option<i64>,
        row_count: Option<i64>,
        min_time_check: TimeCheck,
        max_time_check: TimeCheck,
    }

    #[derive(Debug)]
    enum TimeCheck {
        #[allow(dead_code)]
        Exact(i64),
        MatchSnapshot,
        Max, // i64::MAX
        Zero,
    }

    struct TestCase {
        name: &'static str,
        snapshot: PersistedSnapshot,
        expected_tables: Vec<ExpectedTableSnapshot>,
        additional_validations: Option<fn(&PersistedSnapshot, &[TableIndexSnapshot])>,
    }

    impl std::fmt::Display for TestCase {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestCase")
                .field("name", &self.name)
                .finish()
        }
    }

    #[rstest]
    #[case::simple_single_table_conversion(TestCase {
      name: "simple_single_table_conversion",
      snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
          sequence_number: 1,
          added_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (0.into(), 0.into()),
                  (0.into(), 1.into()),
                  (1.into(), 0.into()),
                  (1.into(), 1.into()),
              ]),
              files_per_table: 3,
              base_time_offset_minutes: 1,
          })),
          ..Default::default()
      }),
      expected_tables: vec![
          ExpectedTableSnapshot {
              db_id: 0.into(),
              table_id: 0.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 0.into(),
              table_id: 1.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 1.into(),
              table_id: 0.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 1.into(),
              table_id: 1.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
      ],
      additional_validations: None,
    })]
    #[case::multiple_databases_tables_conversion(TestCase {
      name: "multiple_databases_tables_conversion",
      snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
          sequence_number: 1,
          added_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (0.into(), 0.into()),
                  (0.into(), 1.into()),
                  (1.into(), 0.into()),
                  (1.into(), 1.into()),
              ]),
              files_per_table: 3,
              base_time_offset_minutes: 1,
          })),
          ..Default::default()
      }),
      expected_tables: vec![
          ExpectedTableSnapshot {
              db_id: 0.into(),
              table_id: 0.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 0.into(),
              table_id: 1.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 1.into(),
              table_id: 0.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 1.into(),
              table_id: 1.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
      ],
      additional_validations: Some(|_snapshot, table_snapshots| {
          // Verify no cross-contamination between tables
          let mut seen_files = HashSet::new();
          for ts in table_snapshots {
              for file in &ts.files {
                  assert!(
                      seen_files.insert(file.id),
                      "File {:?} appears in multiple tables",
                      file.id
                  );
              }
          }
      }),
    })]
    #[case::removed_files_handling(TestCase {
      name: "removed_files_handling",
      snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
          sequence_number: 1,
          added_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (3.into(), 3.into()),
                  (3.into(), 4.into()),
                  (4.into(), 3.into()),
                  (4.into(), 4.into()),
              ]),
              files_per_table: 3,
              base_time_offset_minutes: 1,
          })),
          removed_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (0.into(), 0.into()),
                  (0.into(), 1.into()),
                  (1.into(), 0.into()),
                  (1.into(), 1.into()),
              ]),
              files_per_table: 3,
              base_time_offset_minutes: 1,
          })),
          ..Default::default()
      }),
      expected_tables: vec![
          // Tables with added files
          ExpectedTableSnapshot {
              db_id: 3.into(),
              table_id: 3.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 3.into(),
              table_id: 4.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 4.into(),
              table_id: 3.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          ExpectedTableSnapshot {
              db_id: 4.into(),
              table_id: 4.into(),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          // Tables with removed files only
          ExpectedTableSnapshot {
              db_id: 0.into(),
              table_id: 0.into(),
              expected_file_count: 0,
              expected_removed_file_count: 3,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-300),
                  row_count: Some(-30_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
          ExpectedTableSnapshot {
              db_id: 0.into(),
              table_id: 1.into(),
              expected_file_count: 0,
              expected_removed_file_count: 3,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-300),
                  row_count: Some(-30_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
          ExpectedTableSnapshot {
              db_id: 1.into(),
              table_id: 0.into(),
              expected_file_count: 0,
              expected_removed_file_count: 3,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-300),
                  row_count: Some(-30_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
          ExpectedTableSnapshot {
              db_id: 1.into(),
              table_id: 1.into(),
              expected_file_count: 0,
              expected_removed_file_count: 3,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-300),
                  row_count: Some(-30_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
      ],
      additional_validations: None,
    })]
    #[case::empty_snapshot_conversion(TestCase {
      name: "empty_snapshot_conversion",
      snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
          sequence_number: 1,
          ..Default::default()
      }),
      expected_tables: vec![],
      additional_validations: None,
    })]
    #[case::table_with_only_removed_files(TestCase {
      name: "table_with_only_removed_files",
      snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
          sequence_number: 1,
          removed_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (DbId::from(1), TableId::from(1)),
                  (DbId::from(1), TableId::from(2)),
              ]),
              files_per_table: 2,
              base_time_offset_minutes: 1,
          })),
          ..Default::default()
      }),
      expected_tables: vec![
          ExpectedTableSnapshot {
              db_id: DbId::from(1),
              table_id: TableId::from(1),
              expected_file_count: 0,
              expected_removed_file_count: 2,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-200),
                  row_count: Some(-20_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
          ExpectedTableSnapshot {
              db_id: DbId::from(1),
              table_id: TableId::from(2),
              expected_file_count: 0,
              expected_removed_file_count: 2,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-200),
                  row_count: Some(-20_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
      ],
      additional_validations: None,
    })]
    #[case::overlapping_added_and_removed_files(TestCase {
      name: "overlapping_added_and_removed_files",
      snapshot: {
          // Create both added and removed files - note that they share (1,1) as an overlapping table
          let added = create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (DbId::from(1), TableId::from(1)), // overlapping
                  (DbId::from(2), TableId::from(1)), // added only
              ]),
              files_per_table: 3,
              base_time_offset_minutes: 1,
          });
          let removed = create_database_tables(CreateTestDatabaseTablesArgs {
              table_ids: HashSet::from_iter(vec![
                  (DbId::from(1), TableId::from(1)), // overlapping
                  (DbId::from(1), TableId::from(2)), // removed only
              ]),
              files_per_table: 2,
              base_time_offset_minutes: 1,
          });
          create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
              sequence_number: 1,
              added_tables: Some(added),
              removed_tables: Some(removed),
              ..Default::default()
          })
      },
      expected_tables: vec![
          // Overlapping table
          ExpectedTableSnapshot {
              db_id: DbId::from(1),
              table_id: TableId::from(1),
              expected_file_count: 3,
              expected_removed_file_count: 2,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(100), // 300 (added) - 200 (removed) = 100
                  row_count: Some(10_000), // 30000 (added) - 20000 (removed) = 10000
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          // Added only table
          ExpectedTableSnapshot {
              db_id: DbId::from(2),
              table_id: TableId::from(1),
              expected_file_count: 3,
              expected_removed_file_count: 0,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(300),
                  row_count: Some(30_000),
                  min_time_check: TimeCheck::MatchSnapshot,
                  max_time_check: TimeCheck::MatchSnapshot,
              },
          },
          // Removed only table
          ExpectedTableSnapshot {
              db_id: DbId::from(1),
              table_id: TableId::from(2),
              expected_file_count: 0,
              expected_removed_file_count: 2,
              expected_metadata: ExpectedMetadata {
                  parquet_size_bytes: Some(-200),
                  row_count: Some(-20_000),
                  min_time_check: TimeCheck::Max,
                  max_time_check: TimeCheck::Zero,
              },
          },
      ],
      additional_validations: Some(|_snapshot, table_snapshots| {
          // Verify all file IDs are unique within each table
          for ts in table_snapshots {
              let file_count = ts.files.len();
              let unique_ids: HashSet<_> = ts.files.iter().map(|f| f.id).collect();
              assert_eq!(
                  unique_ids.len(),
                  file_count,
                  "All file IDs should be unique within table"
              );
          }
      }),
    })]
    fn validate(#[case] tc: TestCase) {
        println!("running {tc}");
        let snapshot = &tc.snapshot;

        // Handle empty snapshot case
        if tc.expected_tables.is_empty() {
            assert!(
                snapshot.databases.is_empty(),
                "Expected empty persisted snapshot files",
            );
            return;
        }

        // Convert to table index snapshots
        let table_snapshots: Vec<TableIndexSnapshot> = snapshot.clone().into();

        assert_eq!(
            table_snapshots.len(),
            tc.expected_tables.len(),
            "Expected {} table snapshots, got {}",
            tc.expected_tables.len(),
            table_snapshots.len()
        );

        // Verify each expected table
        for expected in &tc.expected_tables {
            println!("verifying {expected:?}");
            let table_snapshot = table_snapshots
                .iter()
                .find(|ts| ts.id.db_id() == expected.db_id && ts.id.table_id() == expected.table_id)
                .expect("Expected to find table");

            // Extract the ParquetFiles from the original PersistedSnapshot for this table
            let original_files = snapshot
                .databases
                .get(&expected.db_id)
                .and_then(|db| db.tables.get(&expected.table_id))
                .map(|files| files.iter().cloned().collect::<HashSet<_>>())
                .unwrap_or_default();

            // Verify that the files in the TableIndexSnapshot match exactly
            assert_eq!(
                table_snapshot.files, original_files,
                "Files don't match exactly for table {:?}/{:?}",
                expected.db_id, expected.table_id
            );

            // Extract removed file IDs from the original PersistedSnapshot
            let original_removed_ids = snapshot
                .removed_files
                .get(&expected.db_id)
                .and_then(|db| db.tables.get(&expected.table_id))
                .map(|files| files.iter().map(|f| f.id).collect::<HashSet<_>>())
                .unwrap_or_default();

            // Verify that the removed file IDs match exactly
            assert_eq!(
                table_snapshot.removed_files, original_removed_ids,
                "Removed file IDs don't match exactly for table {:?}/{:?}",
                expected.db_id, expected.table_id
            );

            // Verify common fields
            assert_eq!(
                table_snapshot.id.node_id(),
                snapshot.node_id,
                "node_id mismatch",
            );
            assert_eq!(
                table_snapshot.snapshot_sequence_number, snapshot.snapshot_sequence_number,
                "snapshot_sequence_number mismatch",
            );

            // Verify file counts
            assert_eq!(
                table_snapshot.files.len(),
                expected.expected_file_count,
                "File count mismatch for table {:?}/{:?}",
                expected.db_id,
                expected.table_id
            );
            assert_eq!(
                table_snapshot.removed_files.len(),
                expected.expected_removed_file_count,
                "Removed file count mismatch for table {:?}/{:?}",
                expected.db_id,
                expected.table_id
            );

            // Verify metadata
            if let Some(expected_bytes) = expected.expected_metadata.parquet_size_bytes {
                assert_eq!(
                    table_snapshot.metadata.parquet_size_bytes, expected_bytes,
                    "parquet_size_bytes mismatch for table {:?}/{:?}",
                    expected.db_id, expected.table_id
                );
            }

            if let Some(expected_rows) = expected.expected_metadata.row_count {
                assert_eq!(
                    table_snapshot.metadata.row_count, expected_rows,
                    "row_count mismatch for table {:?}/{:?}",
                    expected.db_id, expected.table_id
                );
            }

            // Verify times
            match expected.expected_metadata.min_time_check {
                TimeCheck::Exact(time) => {
                    assert_eq!(table_snapshot.metadata.min_time, time, "min_time mismatch",)
                }
                TimeCheck::MatchSnapshot => assert!(
                    table_snapshot.metadata.min_time >= snapshot.min_time,
                    "min_time should be >= snapshot min_time",
                ),
                TimeCheck::Max => assert_eq!(
                    table_snapshot.metadata.min_time,
                    i64::MAX,
                    "min_time should be i64::MAX",
                ),
                TimeCheck::Zero => {
                    assert_eq!(table_snapshot.metadata.min_time, 0, "min_time should be 0",)
                }
            }

            match expected.expected_metadata.max_time_check {
                TimeCheck::Exact(time) => {
                    assert_eq!(table_snapshot.metadata.max_time, time, "max_time mismatch",)
                }
                TimeCheck::MatchSnapshot => assert!(
                    table_snapshot.metadata.max_time <= snapshot.max_time,
                    "max_time should be <= snapshot max_time",
                ),
                TimeCheck::Max => assert_eq!(
                    table_snapshot.metadata.max_time,
                    i64::MAX,
                    "max_time should be i64::MAX",
                ),
                TimeCheck::Zero => {
                    assert_eq!(table_snapshot.metadata.max_time, 0, "max_time should be 0",)
                }
            }
        }

        // Run additional validations if provided
        if let Some(validation_fn) = tc.additional_validations {
            validation_fn(snapshot, &table_snapshots);
        }
    }
}

#[cfg(test)]
pub(crate) mod test_table_index_operations {
    use super::*;

    use std::collections::BTreeSet;

    use influxdb3_id::{DbId, ParquetFileId, TableId};
    use influxdb3_wal::SnapshotSequenceNumber;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use rstest::rstest;

    use crate::{
        ParquetFile,
        paths::{TableIndexPath, TableIndexSnapshotPath},
    };

    #[derive(Debug, Clone)]
    pub(crate) enum SnapshotVariant {
        /// Snapshot with only added files
        AddedFilesOnly { file_count: usize },
        /// Snapshot with both added and removed files
        Mixed {
            added_count: usize,
            removed_ids: Vec<ParquetFileId>,
        },
        /// Empty snapshot
        Empty,
    }

    pub(crate) struct TableIndexSnapshotBuilder {
        id: TableIndexId,
        sequence: SnapshotSequenceNumber,
        variant: SnapshotVariant,
        metadata_override: Option<IndexMetadata>,
        file_id_start: u64,
        // Store specific file IDs for controlled testing
        specific_file_ids: Option<Vec<ParquetFileId>>,
    }

    impl TableIndexSnapshotBuilder {
        pub(crate) fn new(id: TableIndexId) -> Self {
            Self {
                id,
                sequence: SnapshotSequenceNumber::new(1),
                variant: SnapshotVariant::Empty,
                metadata_override: None,
                file_id_start: 1,
                specific_file_ids: None,
            }
        }

        pub(crate) fn with_sequence(mut self, seq: SnapshotSequenceNumber) -> Self {
            self.sequence = seq;
            self
        }

        pub(crate) fn with_variant(mut self, variant: SnapshotVariant) -> Self {
            self.variant = variant;
            self
        }

        pub(crate) fn with_file_id_start(mut self, start: u64) -> Self {
            self.file_id_start = start;
            self
        }

        pub(crate) fn with_specific_file_ids(mut self, ids: Vec<ParquetFileId>) -> Self {
            self.specific_file_ids = Some(ids);
            self
        }

        pub(crate) fn build(self) -> TableIndexSnapshot {
            let mut files = HashSet::new();
            let mut removed_files = HashSet::new();
            let mut metadata = self.metadata_override.unwrap_or_else(IndexMetadata::empty);

            match self.variant {
                SnapshotVariant::AddedFilesOnly { file_count } => {
                    for i in 0..file_count {
                        let file_id = if let Some(ref ids) = self.specific_file_ids {
                            ids.get(i).cloned().unwrap_or_else(ParquetFileId::new)
                        } else {
                            ParquetFileId::new()
                        };
                        let file = ParquetFile {
                            id: file_id,
                            path: format!("test_file_{i}"),
                            size_bytes: 100 * (i as u64 + 1),
                            row_count: 1000 * (i as u64 + 1),
                            chunk_time: 0,
                            min_time: 1000 + i as i64,
                            max_time: 2000 + i as i64,
                        };

                        // Update metadata if not overridden
                        if self.metadata_override.is_none() {
                            metadata.parquet_size_bytes += (100 * (i + 1)) as i64;
                            metadata.row_count += (1000 * (i + 1)) as i64;
                            metadata.min_time = metadata.min_time.min(1000 + i as i64);
                            metadata.max_time = metadata.max_time.max(2000 + i as i64);
                        }

                        files.insert(file);
                    }
                }
                SnapshotVariant::Mixed {
                    added_count,
                    removed_ids,
                } => {
                    // Add files
                    for i in 0..added_count {
                        let file_id = if let Some(ref ids) = self.specific_file_ids {
                            ids.get(i).cloned().unwrap_or_else(ParquetFileId::new)
                        } else {
                            ParquetFileId::new()
                        };
                        let file = ParquetFile {
                            id: file_id,
                            path: format!("test_file_{i}"),
                            size_bytes: 100 * (i as u64 + 1),
                            row_count: 1000 * (i as u64 + 1),
                            chunk_time: 0,
                            min_time: 1000 + i as i64,
                            max_time: 2000 + i as i64,
                        };

                        if self.metadata_override.is_none() {
                            metadata.parquet_size_bytes += (100 * (i + 1)) as i64;
                            metadata.row_count += (1000 * (i + 1)) as i64;
                            metadata.min_time = metadata.min_time.min(1000 + i as i64);
                            metadata.max_time = metadata.max_time.max(2000 + i as i64);
                        }

                        files.insert(file);
                    }
                    // Add removed files
                    removed_files.extend(removed_ids);
                }
                SnapshotVariant::Empty => {
                    // No files, empty metadata
                }
            }

            TableIndexSnapshot {
                id: self.id.clone(),
                snapshot_sequence_number: self.sequence,
                files,
                removed_files,
                metadata,
            }
        }
    }

    // Common test constants
    const NODE_1: &str = "node-1";
    const NODE_2: &str = "node-2";

    fn db_1() -> DbId {
        DbId::new(1)
    }

    fn table_index(
        node_id: &str,
        db_id: DbId,
        table_id: TableId,
        files: BTreeSet<ParquetFile>,
        latest_snapshot_sequence_number: SnapshotSequenceNumber,
        metadata: IndexMetadata,
    ) -> CoreTableIndex {
        CoreTableIndex {
            id: TableIndexId::new(node_id, db_id, table_id),
            files: files.into_iter().map(Arc::new).collect(),
            latest_snapshot_sequence_number,
            metadata,
        }
    }
    fn db_2() -> DbId {
        DbId::new(2)
    }
    fn table_1() -> TableId {
        TableId::new(1)
    }
    fn table_2() -> TableId {
        TableId::new(2)
    }

    struct LoadFromObjectStoreTestCase {
        node_id: String,
        db_id: DbId,
        table_id: TableId,
        // Initial index state (if any) to put in object store
        initial_index: Option<CoreTableIndex>,
        // Snapshots to put in object store
        snapshots: Vec<TableIndexSnapshot>,
        // Expected final CoreTableIndex state
        expected: CoreTableIndex,
    }

    #[rstest]
    #[case::no_snapshots_or_index(LoadFromObjectStoreTestCase {
        node_id: NODE_1.to_string(),
        db_id: db_1(),
        table_id: table_1(),
        initial_index: None,
        snapshots: vec![],
        expected: table_index(NODE_1, db_1(), table_1(), BTreeSet::new(), SnapshotSequenceNumber::new(0), IndexMetadata::empty()),
    })]
    #[case::snapshots_only(LoadFromObjectStoreTestCase {
        node_id: NODE_1.to_string(),
        db_id: db_1(),
        table_id: table_1(),
        initial_index: None,
        snapshots: vec![
            TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
                .with_sequence(SnapshotSequenceNumber::new(10))
                .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 2 })
                .with_specific_file_ids(vec![ParquetFileId::from(100), ParquetFileId::from(101)])
                .build(),
            TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
                .with_sequence(SnapshotSequenceNumber::new(20))
                .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
                .with_specific_file_ids(vec![ParquetFileId::from(102)])
                .with_file_id_start(2)
                .build(),
        ],
        expected: table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                // Files from first snapshot (seq 10) - 2 files
                files.insert(ParquetFile {
                    id: ParquetFileId::from(100),
                    path: "test_file_0".to_string(),
                    size_bytes: 100,
                    row_count: 1000,
                    chunk_time: 0,
                    min_time: 1000,
                    max_time: 2000,
                });
                files.insert(ParquetFile {
                    id: ParquetFileId::from(101),
                    path: "test_file_1".to_string(),
                    size_bytes: 200,
                    row_count: 2000,
                    chunk_time: 0,
                    min_time: 1001,
                    max_time: 2001,
                });
                // File from second snapshot (seq 20) - 1 file
                files.insert(ParquetFile {
                    id: ParquetFileId::from(102),
                    path: "test_file_0".to_string(),
                    size_bytes: 100,
                    row_count: 1000,
                    chunk_time: 0,
                    min_time: 1000,
                    max_time: 2000,
                });
                files
            }, SnapshotSequenceNumber::new(20), IndexMetadata {
                parquet_size_bytes: 400,
                row_count: 4000,
                min_time: 1000,
                max_time: 2001,
            }),
    })]
    #[case::index_only(LoadFromObjectStoreTestCase {
        node_id: NODE_1.to_string(),
        db_id: db_1(),
        table_id: table_1(),
        initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(200),
                    path: "existing_file".to_string(),
                    size_bytes: 500,
                    row_count: 5000,
                    chunk_time: 0,
                    min_time: 500,
                    max_time: 1500,
                });
                files
            }, SnapshotSequenceNumber::new(5), IndexMetadata {
                parquet_size_bytes: 500,
                row_count: 5000,
                min_time: 500,
                max_time: 1500,
            })),
        snapshots: vec![],
        expected: table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(200),
                    path: "existing_file".to_string(),
                    size_bytes: 500,
                    row_count: 5000,
                    chunk_time: 0,
                    min_time: 500,
                    max_time: 1500,
                });
                files
            }, SnapshotSequenceNumber::new(5), IndexMetadata {
                parquet_size_bytes: 500,
                row_count: 5000,
                min_time: 500,
                max_time: 1500,
            }),
    })]
    #[case::index_and_snapshots(LoadFromObjectStoreTestCase {
        node_id: NODE_1.to_string(),
        db_id: db_1(),
        table_id: table_1(),
        initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(200),
                    path: "existing_file".to_string(),
                    size_bytes: 500,
                    row_count: 5000,
                    chunk_time: 0,
                    min_time: 500,
                    max_time: 1500,
                });
                files
            }, SnapshotSequenceNumber::new(5), IndexMetadata {
                parquet_size_bytes: 500,
                row_count: 5000,
                min_time: 500,
                max_time: 1500,
            })),
        snapshots: vec![
            TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
                .with_sequence(SnapshotSequenceNumber::new(15))
                .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
                .with_specific_file_ids(vec![ParquetFileId::from(201)])
                .build(),
        ],
        expected: table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(200),
                    path: "existing_file".to_string(),
                    size_bytes: 500,
                    row_count: 5000,
                    chunk_time: 0,
                    min_time: 500,
                    max_time: 1500,
                });
                files.insert(ParquetFile {
                    id: ParquetFileId::from(201),
                    path: "test_file_0".to_string(),
                    size_bytes: 100,
                    row_count: 1000,
                    chunk_time: 0,
                    min_time: 1000,
                    max_time: 2000,
                });
                files
            }, SnapshotSequenceNumber::new(15), IndexMetadata {
                parquet_size_bytes: 600,
                row_count: 6000,
                min_time: 500,
                max_time: 2000,
            }),
    })]
    #[case::mix_of_snapshots_with_older_sequence_number(LoadFromObjectStoreTestCase {
        node_id: NODE_1.to_string(),
        db_id: db_1(),
        table_id: table_1(),
        initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(200),
                    path: "existing_file".to_string(),
                    size_bytes: 500,
                    row_count: 5000,
                    chunk_time: 0,
                    min_time: 500,
                    max_time: 1500,
                });
                files
            }, SnapshotSequenceNumber::new(20), IndexMetadata {
                parquet_size_bytes: 500,
                row_count: 5000,
                min_time: 500,
                max_time: 1500,
            })),
        snapshots: vec![
            // Older snapshot - should be ignored
            TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
                .with_sequence(SnapshotSequenceNumber::new(10))
                .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
                .with_specific_file_ids(vec![ParquetFileId::from(300)])
                .build(),
            // Newer snapshot - should be applied
            TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
                .with_sequence(SnapshotSequenceNumber::new(30))
                .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
                .with_specific_file_ids(vec![ParquetFileId::from(301)])
                .with_file_id_start(1)
                .build(),
        ],
        expected: table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(200),
                    path: "existing_file".to_string(),
                    size_bytes: 500,
                    row_count: 5000,
                    chunk_time: 0,
                    min_time: 500,
                    max_time: 1500,
                });
                files.insert(ParquetFile {
                    id: ParquetFileId::from(301),
                    path: "test_file_0".to_string(),
                    size_bytes: 100,
                    row_count: 1000,
                    chunk_time: 0,
                    min_time: 1000,
                    max_time: 2000,
                });
                files
            }, SnapshotSequenceNumber::new(30), IndexMetadata {
                parquet_size_bytes: 600,
                row_count: 6000,
                min_time: 500,
                max_time: 2000,
            }),
    })]
    #[case::snapshot_removes_file_from_initial_index(LoadFromObjectStoreTestCase {
        node_id: NODE_1.to_string(),
        db_id: db_1(),
        table_id: table_1(),
        initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(100),
                    path: "file_to_keep".to_string(),
                    size_bytes: 1000,
                    row_count: 10000,
                    chunk_time: 0,
                    min_time: 100,
                    max_time: 200,
                });
                files.insert(ParquetFile {
                    id: ParquetFileId::from(101),
                    path: "file_to_remove".to_string(),
                    size_bytes: 2000,
                    row_count: 20000,
                    chunk_time: 0,
                    min_time: 200,
                    max_time: 300,
                });
                files
            }, SnapshotSequenceNumber::new(10), IndexMetadata {
                parquet_size_bytes: 3000,
                row_count: 30000,
                min_time: 100,
                max_time: 300,
            })),
        snapshots: vec![
            // Snapshot that removes file 101 and adds file 102
            TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
                .with_sequence(SnapshotSequenceNumber::new(20))
                .with_variant(SnapshotVariant::Mixed {
                    added_count: 1,
                    removed_ids: vec![ParquetFileId::from(101)],
                })
                .with_specific_file_ids(vec![ParquetFileId::from(102)])
                .build(),
        ],
        expected: table_index(NODE_1, db_1(), table_1(), {
                let mut files = BTreeSet::new();
                // File 100 remains
                files.insert(ParquetFile {
                    id: ParquetFileId::from(100),
                    path: "file_to_keep".to_string(),
                    size_bytes: 1000,
                    row_count: 10000,
                    chunk_time: 0,
                    min_time: 100,
                    max_time: 200,
                });
                // File 101 was removed
                // File 102 was added
                files.insert(ParquetFile {
                    id: ParquetFileId::from(102),
                    path: "test_file_0".to_string(),
                    size_bytes: 100,
                    row_count: 1000,
                    chunk_time: 0,
                    min_time: 1000,
                    max_time: 2000,
                });
                files
            }, SnapshotSequenceNumber::new(20), IndexMetadata {
                parquet_size_bytes: 1100,  // 1000 + 100
                row_count: 11000,           // 10000 + 1000
                min_time: 100,
                max_time: 2000,
            }),
    })]
    #[tokio::test]
    async fn load_from_object_store(#[case] tc: LoadFromObjectStoreTestCase) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Set up initial index if provided
        let path = TableIndexPath::new(&tc.node_id, tc.db_id.get(), tc.table_id.get());
        if let Some(initial) = tc.initial_index {
            let json = serde_json::to_vec_pretty(&initial).unwrap();
            object_store.put(path.as_ref(), json.into()).await.unwrap();
        }

        // Set up snapshots
        for snapshot in tc.snapshots {
            let path = TableIndexSnapshotPath::new(
                &tc.node_id,
                tc.db_id.get(),
                tc.table_id.get(),
                snapshot.snapshot_sequence_number,
            );
            let json = serde_json::to_vec_pretty(&snapshot).unwrap();
            object_store.put(path.as_ref(), json.into()).await.unwrap();
        }

        // Load from object store
        let loaded = CoreTableIndex::from_object_store(Arc::clone(&object_store), &path)
            .await
            .expect("loading from object store should succeed");

        assert_eq!(loaded, tc.expected);
    }

    #[tokio::test]
    async fn load_from_object_store_malformed_index() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_id = NODE_1;
        let db_id = db_1();
        let table_id = table_1();

        // Create a malformed index
        let path = TableIndexPath::new(node_id, db_id.get(), table_id.get());
        let malformed_json = b"{ this is not valid json }";
        object_store
            .put(path.as_ref(), malformed_json.to_vec().into())
            .await
            .unwrap();

        // Try to load from object store and expect an error
        let result = CoreTableIndex::from_object_store(Arc::clone(&object_store), &path).await;

        assert!(result.is_err(), "Expected an error but got success");
        let err = result.unwrap_err();
        assert!(
            matches!(err, TableIndexError::DeserializeIndex(_)),
            "Error type mismatch. Got: {err:?}",
        );
    }

    #[tokio::test]
    async fn load_from_object_store_malformed_snapshot() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let node_id = NODE_1;
        let db_id = db_1();
        let table_id = table_1();

        // Create a valid (empty) index first
        let index = table_index(
            node_id,
            db_id,
            table_id,
            BTreeSet::new(),
            SnapshotSequenceNumber::new(0),
            IndexMetadata::empty(),
        );
        let path = TableIndexPath::new(node_id, db_id.get(), table_id.get());
        let json = serde_json::to_vec_pretty(&index).unwrap();
        object_store.put(path.as_ref(), json.into()).await.unwrap();

        // Add a malformed snapshot
        let snapshot_path = TableIndexSnapshotPath::new(
            node_id,
            db_id.get(),
            table_id.get(),
            SnapshotSequenceNumber::new(10),
        );
        let malformed_json = b"{ this is not valid json }";
        object_store
            .put(snapshot_path.as_ref(), malformed_json.to_vec().into())
            .await
            .unwrap();

        // Try to load from object store and expect an error
        let result = CoreTableIndex::from_object_store(Arc::clone(&object_store), &path).await;

        assert!(result.is_err(), "Expected an error but got success");
        let err = result.unwrap_err();
        assert!(
            matches!(err, TableIndexError::DeserializeSnapshot(_)),
            "Error type mismatch. Got: {err:?}",
        );
    }

    #[rstest]
    #[case::matching_identifiers(NODE_1, db_1(), table_1(), NODE_1, db_1(), table_1(), true)]
    #[case::different_node(NODE_1, db_1(), table_1(), NODE_2, db_1(), table_1(), false)]
    #[case::different_db(NODE_1, db_1(), table_1(), NODE_1, db_2(), table_1(), false)]
    #[case::different_table(NODE_1, db_1(), table_1(), NODE_1, db_1(), table_2(), false)]
    fn test_merge_validation(
        #[case] node1: &str,
        #[case] db1: DbId,
        #[case] table1: TableId,
        #[case] node2: &str,
        #[case] db2: DbId,
        #[case] table2: TableId,
        #[case] should_succeed: bool,
    ) {
        let mut index1 = table_index(
            node1,
            db1,
            table1,
            {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(1),
                    path: "file1".to_string(),
                    size_bytes: 100,
                    row_count: 1000,
                    chunk_time: 0,
                    min_time: 100,
                    max_time: 200,
                });
                files
            },
            SnapshotSequenceNumber::new(5),
            IndexMetadata {
                parquet_size_bytes: 100,
                row_count: 1000,
                min_time: 100,
                max_time: 200,
            },
        );

        let index2 = table_index(
            node2,
            db2,
            table2,
            {
                let mut files = BTreeSet::new();
                files.insert(ParquetFile {
                    id: ParquetFileId::from(2),
                    path: "file2".to_string(),
                    size_bytes: 200,
                    row_count: 2000,
                    chunk_time: 0,
                    min_time: 150,
                    max_time: 250,
                });
                files
            },
            SnapshotSequenceNumber::new(10),
            IndexMetadata {
                parquet_size_bytes: 200,
                row_count: 2000,
                min_time: 150,
                max_time: 250,
            },
        );

        let result = index1.merge(index2);

        if should_succeed {
            assert!(
                result.is_ok(),
                "Merge should succeed for matching identifiers"
            );
            assert_eq!(index1.files.len(), 2, "Should have both files after merge");
            assert_eq!(
                index1.latest_snapshot_sequence_number,
                SnapshotSequenceNumber::new(10)
            );
            assert_eq!(index1.metadata.parquet_size_bytes, 300);
            assert_eq!(index1.metadata.row_count, 3000);
            assert_eq!(index1.metadata.min_time, 100);
            assert_eq!(index1.metadata.max_time, 250);
        } else {
            assert!(
                result.is_err(),
                "Merge should fail for mismatched identifiers"
            );
            match result.unwrap_err() {
                TableIndexError::MergeMismatch { expected, actual } => {
                    assert_eq!(expected.node_id(), node1);
                    assert_eq!(actual.node_id(), node2);
                    assert_eq!(expected.db_id(), db1);
                    assert_eq!(actual.db_id(), db2);
                    assert_eq!(expected.table_id(), table1);
                    assert_eq!(actual.table_id(), table2);
                }
                err => panic!("Expected MergeMismatch error, got: {err:?}"),
            }
        }
    }
}
