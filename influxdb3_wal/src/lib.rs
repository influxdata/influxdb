//! This crate provides a Write Ahead Log (WAL) for InfluxDB 3 Core. The WAL is used to buffer writes
//! in memory and persist them as individual files in an object store. The WAL is used to make
//! writes durable until they can be written in larger batches as Parquet files and other snapshot and
//! index files in object storage.

pub mod create;
pub mod object_store;
pub mod serialize;
mod snapshot_tracker;

use async_trait::async_trait;
use data_types::Timestamp;
use hashbrown::HashMap;
use indexmap::IndexMap;
use influxdb_line_protocol::FieldValue;
use influxdb_line_protocol::v3::SeriesValue;
use influxdb3_id::{ColumnId, DbId, SerdeVecMap, TableId};
use iox_time::Time;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{any::Any, num::ParseIntError};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

#[derive(Debug, Error)]
pub enum Error {
    #[error("wal buffer full with {0} ops")]
    BufferFull(usize),

    #[error("error writing wal file: {0}")]
    WriteError(String),

    #[error("deserialize error: {0}")]
    Serialize(#[from] crate::serialize::Error),

    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] ::object_store::Error),

    #[error("wal is shutdown and not accepting writes")]
    Shutdown,

    #[error("invalid gen1 duration {0}. Must be one of 1m, 5m, 10m")]
    InvalidGen1Duration(String),

    #[error("invalid WAL file path")]
    InvalidWalFilePath,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Wal: Debug + Send + Sync + 'static {
    /// Buffer writes ops into the buffer, but returns before the operation is persisted to the WAL.
    async fn write_ops_unconfirmed(&self, op: Vec<WalOp>) -> Result<(), Error>;

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
        SnapshotDetails,
        OwnedSemaphorePermit,
    )>;

    /// This is similar to flush buffer but it allows for snapshot to be done immediately rather
    /// than waiting for wal periods to stack up in [`snapshot_tracker::SnapshotTracker`].
    /// Because forcing flush buffer can happen with no ops in wal buffer this call will add
    /// a [`WalOp::Noop`] into the wal buffer if it is empty and then carry on with snapshotting.
    async fn force_flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        OwnedSemaphorePermit,
    )>;

    /// Removes any snapshot wal files
    async fn cleanup_snapshot(
        &self,
        snapshot_details: SnapshotDetails,
        snapshot_permit: OwnedSemaphorePermit,
    );

    /// Returns the last persisted wal file sequence number
    async fn last_wal_sequence_number(&self) -> WalFileSequenceNumber;

    /// Returns the last persisted wal file sequence number
    async fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber;

    /// Stop all writes to the WAL and flush the buffer to a WAL file.
    async fn shutdown(&self);

    /// Adds a new file notifier listener to the WAL (for use by the processing engine). The WAL
    /// will send new file notifications to the listener, but ignore any snapshot receiver.
    /// Only the notifier passed in the WAL constructor should be used for snapshots (i.e. the
    /// `QueryableBuffer`).
    fn add_file_notifier(&self, notifier: Arc<dyn WalFileNotifier>);
}

/// When the WAL persists a file with buffered ops, the contents are sent to this
/// notifier so that the data can be loaded into the in memory buffer and caches.
#[async_trait]
pub trait WalFileNotifier: Debug + Send + Sync + 'static {
    /// Notify the handler that a new WAL file has been persisted with the given contents.
    async fn notify(&self, write: Arc<WalContents>);

    /// Notify the handler that a new WAL file has been persisted with the given contents and tell
    /// it to snapshot the data. The returned receiver will be signalled when the snapshot is complete.
    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct NoopDetails {
    timestamp_ns: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum WalOp {
    Write(WriteBatch),
    Noop(NoopDetails),
}

impl PartialOrd for WalOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WalOp {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // For two Write ops, consider them equal
            (WalOp::Write(_), WalOp::Write(_)) => Ordering::Equal,
            // all noops should stay where they are no need to reorder them
            // the noop at the moment at least should appear only in cases
            // when there are no other ops in wal buffer.
            (_, WalOp::Noop(_)) => Ordering::Equal,
            (WalOp::Noop(_), _) => Ordering::Equal,
        }
    }
}

impl WalOp {
    pub fn as_write(&self) -> Option<&WriteBatch> {
        match self {
            WalOp::Write(w) => Some(w),
            WalOp::Noop(_) => None,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub catalog_sequence: u64,
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_chunks: SerdeVecMap<TableId, TableChunks>,
    pub min_time_ns: i64,
    pub max_time_ns: i64,
}

impl WriteBatch {
    pub fn new(
        catalog_sequence: u64,
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
            catalog_sequence,
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
    /// The time at which this WAL file is being persisted
    pub persist_timestamp_ms: i64,
    /// The min timestamp from any writes in the WAL file
    pub min_timestamp_ns: i64,
    /// The max timestamp from any writes in the WAL file
    pub max_timestamp_ns: i64,
    /// A number that increments for every generated WAL file
    pub wal_file_number: WalFileSequenceNumber,
    /// The operations contained in the WAL file
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
    /// All wal files with a sequence number >= to this can be deleted once snapshotting is complete
    pub first_wal_sequence_number: WalFileSequenceNumber,
    /// All wal files with a sequence number <= to this can be deleted once snapshotting is complete
    pub last_wal_sequence_number: WalFileSequenceNumber,
    // both forced and 3 times snapshot size should set this flag
    pub forced: bool,
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
                    assert_eq!(snapshot_info, snapshot_details);

                    snapshot_wal
                        .cleanup_snapshot(snapshot_info, snapshot_permit)
                        .await;
                });
            }
        }
    })
}
