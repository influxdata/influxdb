//! This package contains definitions for writing data into InfluxDB3. The logical data model is the standard
//! InfluxDB model of Database > Table > Row. As the data arrives into the server it is written into the wal
//! and buffered in memory in a queryable format. Periodically the WAL is snapshot, which converts the in memory
//! data into parquet files that are persisted to object storage. A snapshot file is written that contains the
//! metadata of the parquet files that were written in that snapshot.

pub(crate) mod async_collections;
pub mod chunk;
pub mod deleter;
pub mod paths;
pub mod persister;
pub mod retention_period_handler;
pub mod table_index;
pub mod table_index_cache;
pub mod write_buffer;

use anyhow::Context;
use async_trait::async_trait;
use data_types::TimestampMinMax;
use datafusion::{
    catalog::Session,
    common::{Column, DFSchema},
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::interval_arithmetic::Interval,
    physical_expr::{AnalysisContext, ExprBoundaries, analyze, create_physical_expr},
    prelude::Expr,
    scalar::ScalarValue,
};
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, CatalogSequenceNumber, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_types::DatabaseName;
pub use influxdb3_types::write::Precision;
use influxdb3_wal::{SnapshotSequenceNumber, Wal, WalFileSequenceNumber};
use iox_query::QueryChunk;
use iox_time::Time;
use observability_deps::tracing::debug;
use schema::TIME_COLUMN_NAME;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("object store path error: {0}")]
    ObjStorePath(#[from] object_store::path::Error),

    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] write_buffer::Error),

    #[error("persister error: {0}")]
    Persister(#[from] persister::PersisterError),

    #[error("queries not supported in compactor only mode")]
    CompactorOnly,

    #[error("unexpected: {0:?}")]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait WriteBuffer: Bufferer + ChunkContainer + DistinctCacheManager + LastCacheManager {}

/// The buffer is for buffering data in memory and in the wal before it is persisted as parquet files in storage.
#[async_trait]
pub trait Bufferer: Debug + Send + Sync + 'static {
    /// Validates the line protocol, writes it into the WAL if configured, writes it into the in memory buffer
    /// and returns the result with any lines that had errors and summary statistics.
    async fn write_lp(
        &self,
        database: DatabaseName,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest>;

    /// Returns the database schema provider
    fn catalog(&self) -> Arc<Catalog>;

    /// Returns the WAL this bufferer is using
    fn wal(&self) -> Arc<dyn Wal>;

    /// Returns the parquet files for a given database and table
    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.parquet_files_filtered(db_id, table_id, &ChunkFilter::default())
    }

    /// Returns the parquet files for a given database and table that satisfy the given filter
    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile>;

    /// A channel to watch for when new persisted snapshots are created
    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>>;
}

/// ChunkContainer is used by the query engine to get chunks for a given table. Chunks will generally be in the
/// `Bufferer` for those in memory from buffered writes or the `Persister` for parquet files that have been persisted.
pub trait ChunkContainer: Debug + Send + Sync + 'static {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

/// [`DistinctCacheManager`] is used to manage interaction with a [`DistinctCacheProvider`].
#[async_trait::async_trait]
pub trait DistinctCacheManager: Debug + Send + Sync + 'static {
    /// Get a reference to the distinct value cache provider
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider>;
}

/// [`LastCacheManager`] is used to manage interaction with a last-n-value cache provider.
#[async_trait::async_trait]
pub trait LastCacheManager: Debug + Send + Sync + 'static {
    /// Get a reference to the last cache provider
    fn last_cache_provider(&self) -> Arc<LastCacheProvider>;
}

pub use influxdb3_types::write::WriteLineError;

/// A write that has been validated against the catalog schema, written to the WAL (if configured), and buffered in
/// memory. This is the summary information for the write along with any errors that were encountered.
#[derive(Debug)]
pub struct BufferedWriteRequest {
    pub db_name: DatabaseName,
    pub invalid_lines: Vec<WriteLineError>,
    pub line_count: usize,
    pub field_count: usize,
    pub index_count: usize,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "version")]
pub enum PersistedSnapshotVersion {
    #[serde(rename = "1")]
    V1(PersistedSnapshot),
}

impl PersistedSnapshotVersion {
    #[cfg(test)]
    fn v1_ref(&self) -> &PersistedSnapshot {
        match self {
            Self::V1(ps) => ps,
        }
    }
}

/// The collection of Parquet files that were persisted in a snapshot
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PersistedSnapshot {
    /// The node identifier that persisted this snapshot
    // TODO: deprecate this alias
    #[serde(alias = "writer_id")]
    pub node_id: String,
    /// The next file id to be used with `ParquetFile`s when the snapshot is loaded
    pub next_file_id: ParquetFileId,
    /// The snapshot sequence number associated with this snapshot
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    /// The wal file sequence number that triggered this snapshot
    pub wal_file_sequence_number: WalFileSequenceNumber,
    /// The catalog sequence number associated with this snapshot
    pub catalog_sequence_number: CatalogSequenceNumber,
    /// The size of the snapshot parquet files in bytes.
    pub parquet_size_bytes: u64,
    /// The number of rows across all parquet files in the snapshot.
    pub row_count: u64,
    /// The min time from all parquet files in the snapshot.
    pub min_time: i64,
    /// The max time from all parquet files in the snapshot.
    pub max_time: i64,
    /// The collection of databases that had tables persisted in this snapshot. The tables will then have their
    /// name and the parquet file.
    pub databases: SerdeVecMap<DbId, DatabaseTables>,
    /// The collection of databases that had files removed in this snapshot.
    /// The tables will then have their name and the parquet file that was removed.
    #[serde(default)]
    pub removed_files: SerdeVecMap<DbId, DatabaseTables>,
    /// The timestamp (ms since epoch) when this snapshot was persisted to object storage.
    /// Populated from ObjectMeta.last_modified during loading. Used for checkpoint grouping.
    /// Not serialized - this is transient metadata populated at load time.
    #[serde(skip)]
    pub persisted_at: Option<i64>,
}

impl PersistedSnapshot {
    pub fn new(
        node_id: String,
        snapshot_sequence_number: SnapshotSequenceNumber,
        wal_file_sequence_number: WalFileSequenceNumber,
        catalog_sequence_number: CatalogSequenceNumber,
    ) -> Self {
        Self {
            node_id,
            next_file_id: ParquetFileId::next_id(),
            snapshot_sequence_number,
            wal_file_sequence_number,
            catalog_sequence_number,
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: i64::MIN,
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            persisted_at: None,
        }
    }

    fn add_parquet_file(
        &mut self,
        database_id: DbId,
        table_id: TableId,
        parquet_file: ParquetFile,
    ) {
        // Update the next_file_id field, as we likely have a new file
        self.next_file_id = ParquetFileId::next_id();
        self.parquet_size_bytes += parquet_file.size_bytes;
        self.row_count += parquet_file.row_count;
        self.min_time = self.min_time.min(parquet_file.min_time);
        self.max_time = self.max_time.max(parquet_file.max_time);

        self.databases
            .entry(database_id)
            .or_default()
            .tables
            .entry(table_id)
            .or_default()
            .push(parquet_file);
    }

    pub fn db_table_and_file_count(&self) -> (u64, u64, u64) {
        let mut db_count = 0;
        let mut table_count = 0;
        let mut file_count = 0;
        for (_, db_tables) in &self.databases {
            db_count += 1;
            table_count += db_tables.tables.len() as u64;
            file_count += db_tables.tables.values().fold(0, |mut acc, files| {
                acc += files.len() as u64;
                acc
            });
        }
        (db_count, table_count, file_count)
    }

    pub fn overall_db_table_file_counts(host_snapshots: &[PersistedSnapshot]) -> (u64, u64, u64) {
        host_snapshots.iter().fold((0, 0, 0), |mut acc, item| {
            let (db_count, table_count, file_count) = item.db_table_and_file_count();
            acc.0 += db_count;
            acc.1 += table_count;
            acc.2 += file_count;
            acc
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct DatabaseTables {
    pub tables: SerdeVecMap<TableId, Vec<ParquetFile>>,
}

/// The summary data for a persisted parquet file in a snapshot.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ParquetFile {
    pub id: ParquetFileId,
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
    /// chunk time nanos
    pub chunk_time: i64,
    /// min time nanos; aka the time of the oldest record in the file
    pub min_time: i64,
    /// max time nanos; aka the time of the newest record in the file
    pub max_time: i64,
}

impl std::hash::Hash for ParquetFile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.path.hash(state);
    }
}

impl std::cmp::Ord for ParquetFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl std::cmp::PartialOrd for ParquetFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ParquetFile {
    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.min_time,
            max: self.max_time,
        }
    }
}

impl AsRef<ParquetFile> for ParquetFile {
    fn as_ref(&self) -> &ParquetFile {
        self
    }
}

#[cfg(test)]
impl ParquetFile {
    pub(crate) fn create_for_test(path: impl Into<String>) -> Self {
        Self {
            id: ParquetFileId::new(),
            path: path.into(),
            size_bytes: 1024,
            row_count: 1,
            chunk_time: 0,
            min_time: 0,
            max_time: 1,
        }
    }
}

/// A year-month value in YYYY-MM format (e.g., "2025-01").
///
/// Used for organizing snapshot checkpoints by month. Stores year and month
/// as integers for efficient comparison and sorting, while serializing to
/// the standard "YYYY-MM" string format for backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct YearMonth {
    year: u16,
    month: u8,
}

/// Error type for invalid YearMonth values.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum YearMonthError {
    #[error("invalid month {0}, expected 1-12")]
    InvalidMonth(u8),
    #[error("invalid year-month format: {0}, expected YYYY-MM")]
    InvalidFormat(String),
}

impl YearMonth {
    /// Create a YearMonth from year and month without validation.
    ///
    /// # Safety
    /// Use only when the values are known to be valid (e.g., from chrono).
    /// In debug builds, panics if month is not in 1-12.
    pub fn new_unchecked(year: u16, month: u8) -> Self {
        debug_assert!((1..=12).contains(&month), "month must be 1-12");
        Self { year, month }
    }

    /// Get the year component.
    pub fn year(&self) -> u16 {
        self.year
    }

    /// Get the month component (1-12).
    pub fn month(&self) -> u8 {
        self.month
    }
}

impl std::fmt::Display for YearMonth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

impl std::str::FromStr for YearMonth {
    type Err = YearMonthError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: "YYYY-MM" (exactly 7 chars)
        if s.len() != 7 || s.as_bytes().get(4) != Some(&b'-') {
            return Err(YearMonthError::InvalidFormat(s.to_string()));
        }
        let year = s[0..4]
            .parse::<u16>()
            .map_err(|_| YearMonthError::InvalidFormat(s.to_string()))?;
        let month = s[5..7]
            .parse::<u8>()
            .map_err(|_| YearMonthError::InvalidFormat(s.to_string()))?;
        if !(1..=12).contains(&month) {
            return Err(YearMonthError::InvalidMonth(month));
        }
        Ok(Self::new_unchecked(year, month))
    }
}

impl serde::Serialize for YearMonth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for YearMonth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A versioned container for snapshot checkpoint persistence.
/// Used for serialization/deserialization with version tagging.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "version")]
pub enum PersistedSnapshotCheckpointVersion {
    #[serde(rename = "1")]
    V1(PersistedSnapshotCheckpoint),
}

impl PersistedSnapshotCheckpointVersion {
    /// Get the last snapshot sequence number from the checkpoint
    pub fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber {
        match self {
            Self::V1(checkpoint) => checkpoint.last_snapshot_sequence_number,
        }
    }

    /// Get the year-month from the checkpoint
    pub fn year_month(&self) -> YearMonth {
        match self {
            Self::V1(checkpoint) => checkpoint.year_month,
        }
    }
}

/// A checkpoint that aggregates snapshot data for a specific month.
/// Used to speed up server startup by reducing the number of snapshot files to load.
///
/// Checkpoints consolidate parquet file metadata from multiple snapshots within a month,
/// allowing the server to load a single checkpoint per month instead of many individual
/// snapshot files. Each checkpoint tracks:
/// - All parquet files added during the month
/// - Files marked for removal that reference previous months (pending_removed_files)
/// - The latest snapshot sequence number that was merged into this checkpoint
///
/// Checkpoints do not enable us to delete old persisted snapshots and they are not used in
/// resolving retention periods for gen1 files. TableIndexCaches are used for retention period
/// handling.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PersistedSnapshotCheckpoint {
    /// The node identifier that created this checkpoint
    pub node_id: String,
    /// The year-month this checkpoint covers (e.g., "2025-01")
    pub year_month: YearMonth,
    /// The latest snapshot sequence number merged into this checkpoint
    pub last_snapshot_sequence_number: SnapshotSequenceNumber,
    /// The next file ID to be used with `ParquetFile`s when the checkpoint is loaded.
    /// None for newly created checkpoints until update_from_snapshot() is called.
    #[serde(default)]
    pub next_file_id: Option<ParquetFileId>,
    /// The WAL file sequence number from the latest merged snapshot
    pub wal_file_sequence_number: WalFileSequenceNumber,
    /// The catalog sequence number from the latest merged snapshot
    pub catalog_sequence_number: CatalogSequenceNumber,
    /// The size of all parquet files in bytes
    pub parquet_size_bytes: u64,
    /// The number of rows across all parquet files
    pub row_count: u64,
    /// The min time from all parquet files
    pub min_time: i64,
    /// The max time from all parquet files
    pub max_time: i64,
    /// The aggregated collection of databases/tables/files.
    /// This is the cumulative state after applying all snapshots for the month.
    pub databases: SerdeVecMap<DbId, DatabaseTables>,
    /// Files marked for removal that reference files from previous months.
    /// These are retained because they may reference files not in this checkpoint's databases.
    #[serde(default)]
    pub pending_removed_files: SerdeVecMap<DbId, DatabaseTables>,
}

impl PersistedSnapshotCheckpoint {
    /// Create a new empty checkpoint for the given node and month
    pub fn new(node_id: String, year_month: YearMonth) -> Self {
        Self {
            node_id,
            year_month,
            last_snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            next_file_id: None,
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::new(0),
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: i64::MIN,
            databases: SerdeVecMap::new(),
            pending_removed_files: SerdeVecMap::new(),
        }
    }

    /// Add a file to the checkpoint and update metrics.
    pub fn add_file(&mut self, db_id: DbId, table_id: TableId, file: ParquetFile) {
        self.parquet_size_bytes += file.size_bytes;
        self.row_count += file.row_count;
        self.min_time = self.min_time.min(file.min_time);
        self.max_time = self.max_time.max(file.max_time);

        self.databases
            .entry(db_id)
            .or_default()
            .tables
            .entry(table_id)
            .or_default()
            .push(file);
    }

    /// Remove a file from the checkpoint and adjust metrics.
    /// Returns true if the file was found and removed.
    pub fn remove_file(&mut self, db_id: DbId, table_id: TableId, file_id: ParquetFileId) -> bool {
        let Some(db_tables) = self.databases.get_mut(&db_id) else {
            return false;
        };
        let Some(table_files) = db_tables.tables.get_mut(&table_id) else {
            return false;
        };
        let Some(pos) = table_files.iter().position(|f| f.id == file_id) else {
            return false;
        };

        let removed = table_files.remove(pos);
        self.parquet_size_bytes = self.parquet_size_bytes.saturating_sub(removed.size_bytes);
        self.row_count = self.row_count.saturating_sub(removed.row_count);

        // Note: min_time/max_time can't be recalculated from the perspective of a single file
        // removal without iterating over all remaining files; that iteration is expected to be
        // handled by the calling context using `recalculate_time_range` once all removals have
        // been handled
        true
    }

    /// Add a file to the pending_removed_files collection.
    pub fn add_pending_removed(&mut self, db_id: DbId, table_id: TableId, file: ParquetFile) {
        self.pending_removed_files
            .entry(db_id)
            .or_default()
            .tables
            .entry(table_id)
            .or_default()
            .push(file);
    }

    /// Update sequence numbers from a snapshot.
    pub fn update_from_snapshot(&mut self, snapshot: &PersistedSnapshot) {
        self.last_snapshot_sequence_number = snapshot.snapshot_sequence_number;
        self.next_file_id = Some(snapshot.next_file_id);
        self.wal_file_sequence_number = snapshot.wal_file_sequence_number;
        self.catalog_sequence_number = snapshot.catalog_sequence_number;
    }

    /// Recalculate min_time and max_time by scanning all files.
    pub fn recalculate_time_range(&mut self) {
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;

        for (_, db_tables) in &self.databases {
            for (_, files) in &db_tables.tables {
                for file in files {
                    min_time = min_time.min(file.min_time);
                    max_time = max_time.max(file.max_time);
                }
            }
        }

        self.min_time = min_time;
        self.max_time = max_time;
    }

    /// Merge another checkpoint into this one.
    ///
    /// The `other` checkpoint should be chronologically later than `self`.
    /// This method:
    /// 1. Adds all files from `other.databases` to `self`
    /// 2. Applies `other.pending_removed_files` to remove files from `self`
    /// 3. Updates sequence numbers from `other`
    pub fn merge(&mut self, other: PersistedSnapshotCheckpoint) {
        // Add all files from the other checkpoint
        for (db_id, db_tables) in other.databases {
            for (table_id, files) in db_tables.tables {
                for file in files {
                    self.add_file(db_id, table_id, file);
                }
            }
        }

        // Apply pending_removed_files from the other checkpoint
        // These reference files from previous months (i.e., files in self)
        let mut any_removed = false;
        for (db_id, db_tables) in other.pending_removed_files {
            let Some(self_db_tables) = self.databases.get_mut(&db_id) else {
                continue;
            };

            for (table_id, files_to_remove) in db_tables.tables {
                let Some(self_table_files) = self_db_tables.tables.get_mut(&table_id) else {
                    continue;
                };

                for file in files_to_remove {
                    if let Some(idx) = self_table_files.iter().position(|f| f.id == file.id) {
                        let removed = self_table_files.remove(idx);
                        self.parquet_size_bytes =
                            self.parquet_size_bytes.saturating_sub(removed.size_bytes);
                        self.row_count = self.row_count.saturating_sub(removed.row_count);
                        any_removed = true;
                    }
                }
            }
        }

        if any_removed {
            self.recalculate_time_range();
        }

        // Update sequence numbers and metadata from the later checkpoint
        self.year_month = other.year_month;
        self.last_snapshot_sequence_number = other.last_snapshot_sequence_number;
        self.next_file_id = other.next_file_id;
        self.wal_file_sequence_number = other.wal_file_sequence_number;
        self.catalog_sequence_number = other.catalog_sequence_number;
    }
}

/// A derived set of filters that are used to prune data in the buffer when serving queries
#[derive(Debug, Default)]
pub struct ChunkFilter<'a> {
    pub time_lower_bound_ns: Option<i64>,
    pub time_upper_bound_ns: Option<i64>,
    filters: &'a [Expr],
}

impl<'a> ChunkFilter<'a> {
    /// Create a new `ChunkFilter` given a [`TableDefinition`] and set of filter
    /// [`Expr`]s from a logical query plan.
    ///
    /// This method analyzes the incoming `exprs` to determine if there are any
    /// filters on the `time` column and attempts to derive the boundaries on
    /// `time` from the query.
    pub fn new(table_def: &Arc<TableDefinition>, exprs: &'a [Expr]) -> Result<Self> {
        debug!(input = ?exprs, "creating chunk filter");
        let mut time_interval: Option<Interval> = None;
        let arrow_schema = table_def.schema.as_arrow();
        let time_col_index = arrow_schema
            .fields()
            .iter()
            .position(|f| f.name() == TIME_COLUMN_NAME)
            .context("table should have a time column")?;

        // DF schema and execution properties used for handling physical expressions:
        let df_schema = DFSchema::try_from(Arc::clone(&arrow_schema))
            .context("table schema was not able to convert to datafusion schema")?;
        let props = ExecutionProps::new();

        for expr in exprs.iter().filter(|e| {
            // NOTE: filter out most expression types, as they are not relevant to
            // time bound analysis:
            matches!(e, Expr::BinaryExpr(_) | Expr::Not(_) | Expr::Between(_))
            // Check if the expression refers to the `time` column:
                && e.column_refs()
                    .contains(&Column::new_unqualified(TIME_COLUMN_NAME))
        }) {
            let Ok(physical_expr) = create_physical_expr(expr, &df_schema, &props) else {
                continue;
            };
            // Determine time bounds, if provided:
            let boundaries = ExprBoundaries::try_new_unbounded(&arrow_schema)
                .context("unable to create unbounded expr boundaries on incoming expression")?;
            // DataFusion does not support all possible expressions. If the expression can't be analyzed
            // skip the analysis rather than erroring the query
            // see https://github.com/influxdata/influxdb/issues/26163
            let Ok(mut analysis) = analyze(
                &physical_expr,
                AnalysisContext::new(boundaries),
                &arrow_schema,
            )
            .inspect_err(|error| {
                debug!(
                    ?error,
                    logical_expr = ?expr,
                    "unable to analyze provided filters for a boundary on the time column"
                );
            }) else {
                continue;
            };

            // Set the boundaries on the time column using the evaluated
            // interval, if it exists.
            // If an interval was already derived from a previous expression, we
            // take their intersection, or produce an error if:
            // - the derived intervals are not compatible (different types)
            // - the derived intervals do not intersect, this should be a user
            //   error, i.e., a poorly formed query or querying outside of a
            //   retention policy for the table
            if let Some(ExprBoundaries {
                interval: Some(new_interval),
                ..
            }) = (time_col_index < analysis.boundaries.len())
                .then_some(analysis.boundaries.remove(time_col_index))
            {
                if let Some(existing) = time_interval.take() {
                    let intersection = existing.intersect(new_interval).context(
                                "failed to derive a time interval from provided filters",
                            )?.context("provided filters on time column did not produce a valid set of boundaries")?;
                    time_interval = Some(intersection);
                } else {
                    time_interval = Some(new_interval);
                }
            }
        }

        // Determine the lower and upper bound from the derived interval on time:
        // TODO: we may open this up more to other scalar types, e.g., other timestamp types
        //       depending on how users define time bounds.
        let (time_lower_bound_ns, time_upper_bound_ns) = if let Some(i) = time_interval {
            let low = if let ScalarValue::TimestampNanosecond(Some(l), _) = i.lower() {
                Some(*l)
            } else {
                None
            };
            let high = if let ScalarValue::TimestampNanosecond(Some(h), _) = i.upper() {
                Some(*h)
            } else {
                None
            };

            (low, high)
        } else {
            (None, None)
        };

        Ok(Self {
            time_lower_bound_ns,
            time_upper_bound_ns,
            filters: exprs,
        })
    }

    /// Test a `min` and `max` time against this filter to check if the range they define overlaps
    /// with the range defined by the bounds in this filter.
    pub fn test_time_stamp_min_max(&self, min_time_ns: i64, max_time_ns: i64) -> bool {
        match (self.time_lower_bound_ns, self.time_upper_bound_ns) {
            (None, None) => true,
            (None, Some(u)) => min_time_ns <= u,
            (Some(l), None) => max_time_ns >= l,
            (Some(l), Some(u)) => min_time_ns <= u && max_time_ns >= l,
        }
    }

    pub fn original_filters(&self) -> &[Expr] {
        self.filters
    }
}

pub mod test_helpers {
    use crate::ChunkFilter;
    use crate::WriteBuffer;
    use crate::write_buffer::validator::WriteValidator;
    use arrow::array::RecordBatch;
    use datafusion::prelude::Expr;
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
    use influxdb3_types::DatabaseName;
    use influxdb3_wal::{Gen1Duration, WriteBatch};
    use iox_query::exec::IOxSessionContext;
    use iox_time::Time;
    use std::sync::Arc;

    /// Helper trait for getting [`RecordBatch`]es from a [`WriteBuffer`] implementation in tests
    #[async_trait::async_trait]
    pub trait WriteBufferTester {
        /// Get record batches for the given database and table, using the provided filter `Expr`s
        async fn get_record_batches_filtered_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            filters: &[Expr],
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch>;

        /// Get record batches for the given database and table
        async fn get_record_batches_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch>;
    }

    #[async_trait::async_trait]
    impl<T> WriteBufferTester for T
    where
        T: WriteBuffer,
    {
        async fn get_record_batches_filtered_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            filters: &[Expr],
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch> {
            let db_schema = self
                .catalog()
                .db_schema(database_name)
                .expect("database should exist");
            let table_def = db_schema
                .table_definition(table_name)
                .expect("table should exist");
            let filter =
                ChunkFilter::new(&table_def, filters).expect("filter expressions should be valid");
            let chunks = self
                .get_table_chunks(db_schema, table_def, &filter, None, &ctx.inner().state())
                .expect("should get query chunks");
            let mut batches = vec![];
            for chunk in chunks {
                batches.extend(
                    chunk
                        .data()
                        .read_to_batches(chunk.schema(), ctx.inner())
                        .await,
                );
            }
            batches
        }

        async fn get_record_batches_unchecked(
            &self,
            database_name: &str,
            table_name: &str,
            ctx: &IOxSessionContext,
        ) -> Vec<RecordBatch> {
            self.get_record_batches_filtered_unchecked(database_name, table_name, &[], ctx)
                .await
        }
    }

    pub async fn do_write(wb: &dyn WriteBuffer, db: &str, lp: &str, time: Time) {
        let db_name = DatabaseName::new(db.to_owned()).expect("valid database name");
        wb.write_lp(
            db_name,
            lp,
            time,
            false,
            influxdb3_types::write::Precision::Auto,
            false,
        )
        .await
        .expect("valid write operation");
    }

    #[derive(Debug)]
    pub struct TestWriter {
        catalog: Arc<Catalog>,
    }

    impl TestWriter {
        pub const DB_NAME: &str = "test_db";

        pub fn new_with_catalog(catalog: Arc<Catalog>) -> Self {
            Self { catalog }
        }

        pub async fn write_lp_to_write_batch(
            &self,
            lp: impl AsRef<str>,
            time_ns: i64,
        ) -> WriteBatch {
            let validated = WriteValidator::initialize(
                Self::DB_NAME.try_into().unwrap(),
                Arc::clone(&self.catalog),
            )
            .expect("initialize write validator")
            .v1_parse_lines_and_catalog_updates(
                lp.as_ref(),
                false,
                Time::from_timestamp_nanos(time_ns),
                crate::Precision::Nanosecond,
            )
            .expect("parse and validate v1 line protocol")
            .commit_catalog_changes()
            .await
            .unwrap()
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_1m());
            validated.into()
        }

        pub fn catalog(&self) -> Arc<Catalog> {
            Arc::clone(&self.catalog)
        }

        pub fn db_schema(&self) -> Arc<DatabaseSchema> {
            self.catalog
                .db_schema(Self::DB_NAME)
                .expect("db schema should be initialized")
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
