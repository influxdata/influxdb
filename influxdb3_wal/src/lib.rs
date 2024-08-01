//! This crate provides a Write Ahead Log (WAL) for InfluxDB 3.0. The WAL is used to buffer writes
//! in memory and persist them as individual files in an object store. The WAL is used to make
//! writes durable until they can be written in larger batches as Parquet files and other snapshot and
//! index files in object storage.

pub mod object_store;
mod serialize;
mod snapshot_tracker;

use crate::snapshot_tracker::SnapshotInfo;
use async_trait::async_trait;
use hashbrown::HashMap;
use influxdb_line_protocol::v3::SeriesValue;
use influxdb_line_protocol::FieldValue;
use observability_deps::tracing::error;
use schema::{InfluxColumnType, InfluxFieldType};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
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
    async fn last_sequence_number(&self) -> WalFileSequenceNumber;

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
    pub level_0_duration: Duration,
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
            level_0_duration: Duration::from_secs(600),
            max_write_buffer_size: 1000,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 100,
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            level_0_duration: Duration::from_secs(600),
            max_write_buffer_size: 100_000,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 600,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WalOp {
    Write(WriteBatch),
    Catalog(CatalogBatch),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogBatch {
    pub database_name: Arc<str>,
    pub ops: Vec<CatalogOp>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogOp {
    CreateDatabase(DatabaseDefinition),
    CreateTable(TableDefinition),
    AddFields(FieldAdditions),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseDefinition {
    pub database_name: Arc<str>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub database_name: Arc<str>,
    pub table_name: Arc<str>,
    pub field_definitions: Vec<FieldDefinition>,
    pub key: Option<Vec<String>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldAdditions {
    pub database_name: Arc<str>,
    pub table_name: Arc<str>,
    pub field_definitions: Vec<FieldDefinition>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: Arc<str>,
    pub data_type: FieldDataType,
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub database_name: Arc<str>,
    pub table_chunks: HashMap<Arc<str>, TableChunks>,
    pub min_time_ns: i64,
    pub max_time_ns: i64,
}

impl WriteBatch {
    pub fn new(database_name: Arc<str>, table_chunks: HashMap<Arc<str>, TableChunks>) -> Self {
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
            database_name,
            table_chunks,
            min_time_ns,
            max_time_ns,
        }
    }

    pub fn add_write_batch(
        &mut self,
        new_table_chunks: HashMap<Arc<str>, TableChunks>,
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
    pub name: Arc<str>,
    pub value: FieldData,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WalFileSequenceNumber(u64);

impl WalFileSequenceNumber {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl Default for WalFileSequenceNumber {
    fn default() -> Self {
        Self(1)
    }
}

/// Details about a snapshot of the WAL
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDetails {
    /// All chunks with data before this time can be snapshot and persisted
    pub end_time_marker: i64,
    /// All wal files with a sequence number <= to this can be deleted once snapshotting is complete
    pub last_sequence_number: WalFileSequenceNumber,
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
