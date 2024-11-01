//! This crate provides a Write Ahead Log (WAL) for InfluxDB 3.0. The WAL is used to buffer writes
//! in memory and persist them as individual files in an object store. The WAL is used to make
//! writes durable until they can be written in larger batches as Parquet files and other snapshot and
//! index files in object storage.

pub mod object_store;
pub mod serialize;
mod snapshot_tracker;

use crate::snapshot_tracker::SnapshotInfo;
use async_trait::async_trait;
use data_types::Timestamp;
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use influxdb_line_protocol::v3::SeriesValue;
use influxdb_line_protocol::FieldValue;
use iox_time::Time;
use observability_deps::tracing::error;
use schema::{InfluxColumnType, InfluxFieldType};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{any::Any, num::ParseIntError};
use thiserror::Error;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

#[derive(Debug, Error)]
pub enum Error {
    #[error("wal buffer full with {0} ops")]
    BufferFull(usize),

    #[error("error writing wal file: {0}")]
    WriteError(String),

    #[error("deserialize error: {0}")]
    Serialize(#[from] crate::serialize::Error),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] ::object_store::Error),

    #[error("wal is shutdown and not accepting writes")]
    Shutdown,

    #[error("invalid gen1 duration {0}. Must be one of 1m, 5m, 10m")]
    InvalidGen1Duration(String),

    #[error("last cache size must be from 1 to 10")]
    InvalidLastCacheSize,

    #[error("invalid WAL file path")]
    InvalidWalFilePath,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Wal: Debug + Send + Sync + 'static {
    /// Buffer into a single larger operation in memory. Returns before the operation is persisted.
    async fn buffer_op_unconfirmed(&self, op: WalOp) -> Result<(), Error>;

    /// Writes the ops into the buffer and waits until the WAL file is persisted. When this returns
    /// the operations are durable in the configured object store and the file notifier has been
    /// called, which puts it into the queryable memory buffer.
    async fn write_ops(&self, ops: Vec<WalOp>) -> Result<(), Error>;

    /// Flushes all buffered writes to a single WAL file and calls the file notifier with the contents.
    /// If it is time for a snapshot, it will tell the notifier to start the snapshot and return
    /// a receiver that will be signalled when the snapshot is complete along with the semaphore
    /// permit to release when done. The caller is responsible for cleaning up the wal.
    async fn flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotInfo,
        OwnedSemaphorePermit,
    )>;

    /// Removes any snapshot wal files
    async fn cleanup_snapshot(
        &self,
        snapshot_details: SnapshotInfo,
        snapshot_permit: OwnedSemaphorePermit,
    );

    /// Returns the last persisted wal file sequence number
    async fn last_wal_sequence_number(&self) -> WalFileSequenceNumber;

    /// Returns the last persisted wal file sequence number
    async fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber;

    /// Stop all writes to the WAL and flush the buffer to a WAL file.
    async fn shutdown(&self);
}

/// When the WAL persists a file with buffered ops, the contents are sent to this
/// notifier so that the data can be loaded into the in memory buffer and caches.
#[async_trait]
pub trait WalFileNotifier: Debug + Send + Sync + 'static {
    /// Notify the handler that a new WAL file has been persisted with the given contents.
    fn notify(&self, write: WalContents);

    /// Notify the handler that a new WAL file has been persisted with the given contents and tell
    /// it to snapshot the data. The returned receiver will be signalled when the snapshot is complete.
    async fn notify_and_snapshot(
        &self,
        write: WalContents,
        snapshot_details: SnapshotDetails,
    ) -> oneshot::Receiver<SnapshotDetails>;

    fn as_any(&self) -> &dyn Any;
}

/// The configuration for the WAL
#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    /// The duration of time of chunks to be persisted as Parquet files
    pub gen1_duration: Gen1Duration,
    /// The maximum number of writes that can be buffered before we must flush to a wal file
    pub max_write_buffer_size: usize,
    /// The interval at which to flush the buffer to a wal file
    pub flush_interval: Duration,
    /// The number of wal files to snapshot at a time
    pub snapshot_size: usize,
}

impl WalConfig {
    pub fn test_config() -> Self {
        Self {
            gen1_duration: Gen1Duration::new_5m(),
            max_write_buffer_size: 1000,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 100,
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            gen1_duration: Default::default(),
            max_write_buffer_size: 100_000,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 600,
        }
    }
}

/// The duration of data timestamps, grouped into files persisted into object storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gen1Duration(Duration);

impl Gen1Duration {
    pub fn duration_seconds(&self) -> i64 {
        self.0.as_secs() as i64
    }

    /// Returns the time of the chunk the given timestamp belongs to based on the duration
    pub fn chunk_time_for_timestamp(&self, t: Timestamp) -> i64 {
        t.get() - (t.get() % self.0.as_nanos() as i64)
    }

    /// Given a time, returns the start time of the gen1 chunk that contains the time.
    pub fn start_time(&self, timestamp_seconds: i64) -> Time {
        let duration_seconds = self.duration_seconds();
        let rounded_seconds = (timestamp_seconds / duration_seconds) * duration_seconds;
        Time::from_timestamp(rounded_seconds, 0).unwrap()
    }

    pub fn as_duration(&self) -> Duration {
        self.0
    }

    pub fn as_nanos(&self) -> i64 {
        self.0.as_nanos() as i64
    }

    pub fn new_1m() -> Self {
        Self(Duration::from_secs(60))
    }

    pub fn new_5m() -> Self {
        Self(Duration::from_secs(300))
    }
}

impl FromStr for Gen1Duration {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "1m" => Ok(Self(Duration::from_secs(60))),
            "5m" => Ok(Self(Duration::from_secs(300))),
            "10m" => Ok(Self(Duration::from_secs(600))),
            _ => Err(Error::InvalidGen1Duration(s.to_string())),
        }
    }
}

impl Default for Gen1Duration {
    fn default() -> Self {
        Self(Duration::from_secs(600))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WalOp {
    Write(WriteBatch),
    Catalog(CatalogBatch),
}

impl WalOp {
    pub fn as_write(&self) -> Option<&WriteBatch> {
        match self {
            WalOp::Write(w) => Some(w),
            WalOp::Catalog(_) => None,
        }
    }

    pub fn as_catalog(&self) -> Option<&CatalogBatch> {
        match self {
            WalOp::Write(_) => None,
            WalOp::Catalog(c) => Some(c),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogBatch {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub time_ns: i64,
    pub ops: Vec<CatalogOp>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogOp {
    CreateDatabase(DatabaseDefinition),
    CreateTable(TableDefinition),
    AddFields(FieldAdditions),
    CreateLastCache(LastCacheDefinition),
    DeleteLastCache(LastCacheDelete),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseDefinition {
    pub database_id: DbId,
    pub database_name: Arc<str>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_definitions: Vec<FieldDefinition>,
    pub key: Option<Vec<ColumnId>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldAdditions {
    pub database_name: Arc<str>,
    pub database_id: DbId,
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub field_definitions: Vec<FieldDefinition>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: Arc<str>,
    pub id: ColumnId,
    pub data_type: FieldDataType,
}

impl FieldDefinition {
    pub fn new(
        id: ColumnId,
        name: impl Into<Arc<str>>,
        data_type: impl Into<FieldDataType>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
    Timestamp,
    Tag,
    Key,
}

// FieldDataType from an InfluxColumnType
impl From<&InfluxColumnType> for FieldDataType {
    fn from(influx_column_type: &InfluxColumnType) -> Self {
        match influx_column_type {
            InfluxColumnType::Tag => FieldDataType::Tag,
            InfluxColumnType::Timestamp => FieldDataType::Timestamp,
            InfluxColumnType::Field(InfluxFieldType::String) => FieldDataType::String,
            InfluxColumnType::Field(InfluxFieldType::Integer) => FieldDataType::Integer,
            InfluxColumnType::Field(InfluxFieldType::UInteger) => FieldDataType::UInteger,
            InfluxColumnType::Field(InfluxFieldType::Float) => FieldDataType::Float,
            InfluxColumnType::Field(InfluxFieldType::Boolean) => FieldDataType::Boolean,
        }
    }
}

impl From<FieldDataType> for InfluxColumnType {
    fn from(field_data_type: FieldDataType) -> Self {
        match field_data_type {
            FieldDataType::Tag => InfluxColumnType::Tag,
            FieldDataType::Timestamp => InfluxColumnType::Timestamp,
            FieldDataType::String => InfluxColumnType::Field(InfluxFieldType::String),
            FieldDataType::Integer => InfluxColumnType::Field(InfluxFieldType::Integer),
            FieldDataType::UInteger => InfluxColumnType::Field(InfluxFieldType::UInteger),
            FieldDataType::Float => InfluxColumnType::Field(InfluxFieldType::Float),
            FieldDataType::Boolean => InfluxColumnType::Field(InfluxFieldType::Boolean),
            FieldDataType::Key => panic!("Key is not a valid InfluxDB column type"),
        }
    }
}

/// Defines a last cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct LastCacheDefinition {
    /// The table id the cache is associated with
    pub table_id: TableId,
    /// The table name the cache is associated with
    pub table: Arc<str>,
    /// Given name of the cache
    pub name: Arc<str>,
    /// Columns intended to be used as predicates in the cache
    pub key_columns: Vec<ColumnId>,
    /// Columns that store values in the cache
    pub value_columns: LastCacheValueColumnsDef,
    /// The number of last values to hold in the cache
    pub count: LastCacheSize,
    /// The time-to-live (TTL) in seconds for entries in the cache
    pub ttl: u64,
}

impl LastCacheDefinition {
    /// Create a new [`LastCacheDefinition`] with explicit value columns
    ///
    /// This is intended for tests and expects that the column id for the time
    /// column is included in the value columns argument.
    pub fn new_with_explicit_value_columns(
        table_id: TableId,
        table: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        key_columns: Vec<ColumnId>,
        value_columns: Vec<ColumnId>,
        count: usize,
        ttl: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            table_id,
            table: table.into(),
            name: name.into(),
            key_columns,
            value_columns: LastCacheValueColumnsDef::Explicit {
                columns: value_columns,
            },
            count: count.try_into()?,
            ttl,
        })
    }

    /// Create a new [`LastCacheDefinition`] with explicit value columns
    ///
    /// This is intended for tests.
    pub fn new_all_non_key_value_columns(
        table_id: TableId,
        table: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        key_columns: Vec<ColumnId>,
        count: usize,
        ttl: u64,
    ) -> Result<Self, Error> {
        Ok(Self {
            table_id,
            table: table.into(),
            name: name.into(),
            key_columns,
            value_columns: LastCacheValueColumnsDef::AllNonKeyColumns,
            count: count.try_into()?,
            ttl,
        })
    }
}

/// A last cache will either store values for an explicit set of columns, or will accept all
/// non-key columns
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LastCacheValueColumnsDef {
    /// Explicit list of column names
    Explicit { columns: Vec<ColumnId> },
    /// Stores all non-key columns
    AllNonKeyColumns,
}

/// The maximum allowed size for a last cache
pub const LAST_CACHE_MAX_SIZE: usize = 10;

/// The size of the last cache
///
/// Must be between 1 and [`LAST_CACHE_MAX_SIZE`]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub struct LastCacheSize(usize);

impl LastCacheSize {
    pub fn new(size: usize) -> Result<Self, Error> {
        if size == 0 || size > LAST_CACHE_MAX_SIZE {
            Err(Error::InvalidLastCacheSize)
        } else {
            Ok(Self(size))
        }
    }
}

impl TryFrom<usize> for LastCacheSize {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LastCacheSize> for usize {
    fn from(value: LastCacheSize) -> Self {
        value.0
    }
}

impl From<LastCacheSize> for u64 {
    fn from(value: LastCacheSize) -> Self {
        value
            .0
            .try_into()
            .expect("usize fits into a 64 bit unsigned integer")
    }
}

impl PartialEq<usize> for LastCacheSize {
    fn eq(&self, other: &usize) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<LastCacheSize> for usize {
    fn eq(&self, other: &LastCacheSize) -> bool {
        self.eq(&other.0)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LastCacheDelete {
    pub table_name: Arc<str>,
    pub table_id: TableId,
    pub name: Arc<str>,
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_chunks: SerdeVecMap<TableId, TableChunks>,
    pub min_time_ns: i64,
    pub max_time_ns: i64,
}

impl WriteBatch {
    pub fn new(
        database_id: DbId,
        database_name: Arc<str>,
        table_chunks: IndexMap<TableId, TableChunks>,
    ) -> Self {
        // find the min and max times across the table chunks
        let (min_time_ns, max_time_ns) = table_chunks.values().fold(
            (i64::MAX, i64::MIN),
            |(min_time_ns, max_time_ns), table_chunks| {
                (
                    min_time_ns.min(table_chunks.min_time),
                    max_time_ns.max(table_chunks.max_time),
                )
            },
        );

        Self {
            database_id,
            database_name,
            table_chunks: table_chunks.into(),
            min_time_ns,
            max_time_ns,
        }
    }

    pub fn add_write_batch(
        &mut self,
        new_table_chunks: SerdeVecMap<TableId, TableChunks>,
        min_time_ns: i64,
        max_time_ns: i64,
    ) {
        self.min_time_ns = self.min_time_ns.min(min_time_ns);
        self.max_time_ns = self.max_time_ns.max(max_time_ns);

        for (table_name, new_chunks) in new_table_chunks {
            let chunks = self.table_chunks.entry(table_name).or_default();
            for (chunk_time, new_chunk) in new_chunks.chunk_time_to_chunk {
                let chunk = chunks.chunk_time_to_chunk.entry(chunk_time).or_default();
                for r in &new_chunk.rows {
                    chunks.min_time = chunks.min_time.min(r.time);
                    chunks.max_time = chunks.max_time.max(r.time);
                }
                chunk.rows.extend(new_chunk.rows);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableChunks {
    pub min_time: i64,
    pub max_time: i64,
    pub chunk_time_to_chunk: HashMap<i64, TableChunk>,
}

impl Default for TableChunks {
    fn default() -> Self {
        Self {
            min_time: i64::MAX,
            max_time: i64::MIN,
            chunk_time_to_chunk: Default::default(),
        }
    }
}

impl TableChunks {
    pub fn push_row(&mut self, chunk_time: i64, row: Row) {
        self.min_time = self.min_time.min(row.time);
        self.max_time = self.max_time.max(row.time);
        let chunk = self.chunk_time_to_chunk.entry(chunk_time).or_default();
        chunk.rows.push(row);
    }

    pub fn row_count(&self) -> usize {
        self.chunk_time_to_chunk
            .values()
            .map(|c| c.rows.len())
            .sum()
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableChunk {
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub id: ColumnId,
    pub value: FieldData,
}

impl Field {
    pub fn new(id: ColumnId, value: impl Into<FieldData>) -> Self {
        Self {
            id,
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub time: i64,
    pub fields: Vec<Field>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FieldData {
    Timestamp(i64),
    Key(String),
    Tag(String),
    String(String),
    Integer(i64),
    UInteger(u64),
    Float(f64),
    Boolean(bool),
}

impl PartialEq for FieldData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldData::Timestamp(a), FieldData::Timestamp(b)) => a == b,
            (FieldData::Tag(a), FieldData::Tag(b)) => a == b,
            (FieldData::Key(a), FieldData::Key(b)) => a == b,
            (FieldData::String(a), FieldData::String(b)) => a == b,
            (FieldData::Integer(a), FieldData::Integer(b)) => a == b,
            (FieldData::UInteger(a), FieldData::UInteger(b)) => a == b,
            (FieldData::Float(a), FieldData::Float(b)) => a == b,
            (FieldData::Boolean(a), FieldData::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldData {}

impl<'a> From<&SeriesValue<'a>> for FieldData {
    fn from(sk: &SeriesValue<'a>) -> Self {
        match sk {
            SeriesValue::String(s) => Self::Key(s.to_string()),
        }
    }
}

impl<'a> From<FieldValue<'a>> for FieldData {
    fn from(value: FieldValue<'a>) -> Self {
        match value {
            FieldValue::I64(v) => Self::Integer(v),
            FieldValue::U64(v) => Self::UInteger(v),
            FieldValue::F64(v) => Self::Float(v),
            FieldValue::String(v) => Self::String(v.to_string()),
            FieldValue::Boolean(v) => Self::Boolean(v),
        }
    }
}

impl<'a> From<&FieldValue<'a>> for FieldData {
    fn from(value: &FieldValue<'a>) -> Self {
        match value {
            FieldValue::I64(v) => Self::Integer(*v),
            FieldValue::U64(v) => Self::UInteger(*v),
            FieldValue::F64(v) => Self::Float(*v),
            FieldValue::String(v) => Self::String(v.to_string()),
            FieldValue::Boolean(v) => Self::Boolean(*v),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalContents {
    /// The min timestamp from any writes in the WAL file
    pub min_timestamp_ns: i64,
    /// The max timestamp from any writes in the WAL file
    pub max_timestamp_ns: i64,
    pub wal_file_number: WalFileSequenceNumber,
    pub ops: Vec<WalOp>,
    /// If present, the buffer should be snapshot after the contents of this file are loaded.
    pub snapshot: Option<SnapshotDetails>,
}

impl WalContents {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.snapshot.is_none()
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct WalFileSequenceNumber(u64);

impl WalFileSequenceNumber {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for WalFileSequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WalFileSequenceNumber {
    type Err = ParseIntError;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        s.parse::<u64>().map(Self)
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SnapshotSequenceNumber(u64);

impl SnapshotSequenceNumber {
    pub fn new(number: u64) -> Self {
        Self(number)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SnapshotSequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Details about a snapshot of the WAL
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDetails {
    /// The sequence number for this snapshot
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    /// All chunks with data before this time can be snapshot and persisted
    pub end_time_marker: i64,
    /// All wal files with a sequence number <= to this can be deleted once snapshotting is complete
    pub last_wal_sequence_number: WalFileSequenceNumber,
}

pub fn background_wal_flush<W: Wal>(
    wal: Arc<W>,
    flush_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let cleanup_after_snapshot = wal.flush_buffer().await;

            // handle snapshot cleanup outside of the flush loop
            if let Some((snapshot_complete, snapshot_info, snapshot_permit)) =
                cleanup_after_snapshot
            {
                let snapshot_wal = Arc::clone(&wal);
                tokio::spawn(async move {
                    let snapshot_details = snapshot_complete.await.expect("snapshot failed");
                    assert!(snapshot_info.snapshot_details == snapshot_details);

                    snapshot_wal
                        .cleanup_snapshot(snapshot_info, snapshot_permit)
                        .await;
                });
            }
        }
    })
}
