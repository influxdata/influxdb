//! This package contains definitions for writing data into InfluxDB3. The logical data model is the standard
//! InfluxDB model of Database > Table > Row. As the data arrives into the server it is written into the open
//! segment in the buffer. If the buffer has a WAL configured, the data is written into the WAL before being
//! buffered in memory.
//!
//! When the segment reaches a certain size, or a certain amount of time has passed, it will be closed and marked
//! to be persisted. A new open segment will be created and new writes will be written to that segment.

pub mod catalog;
pub mod paths;
pub mod persister;
pub mod wal;
pub mod write_buffer;

use crate::catalog::Catalog;
use crate::paths::ParquetFilePath;
use async_trait::async_trait;
use bytes::Bytes;
use data_types::NamespaceName;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::Expr;
use iox_query::QueryChunk;
use parquet::format::FileMetaData;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("database not found {db_name}")]
    DatabaseNotFound { db_name: String },

    #[error("object store path error: {0}")]
    ObjStorePath(#[from] object_store::path::Error),

    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] write_buffer::Error),

    #[error("persister error: {0}")]
    Persister(#[from] persister::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait WriteBuffer: Bufferer + ChunkContainer {}

/// The buffer is for buffering data in memory before it is persisted to object storage. The buffer is queryable and
/// aims to use as little memory as possible, converting data into in-memory Parquet data periodically as it arrives
/// and is queried out. The buffer is repsonsible for keeping a consistent view of the catalog and  is also responsible
/// for writing data into the WAL if it is configured.
///
/// Data in the buffer is organized into monotonically increasing segments, each associated with a single WAL file, if
/// the WAL is configured. As a segment is closed, it is persisted and then freed from memory.
#[async_trait]
pub trait Bufferer: Debug + Send + Sync + 'static {
    /// Validates the line protocol, writes it into the WAL if configured, writes it into the in memory buffer
    /// and returns the result with any lines that had errors and summary statistics. This writes into the currently
    /// open segment or it will open one. The open segment id and the memory usage of the currently open segment are
    /// returned.
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        default_time: i64,
        accept_partial: bool,
        precision: Precision,
    ) -> write_buffer::Result<BufferedWriteRequest>;

    /// Closes the open segment and returns it so that it can be persisted or thrown away. A new segment will be opened
    /// with the catalog rolling over.
    async fn close_open_segment(&self) -> Result<Arc<dyn BufferSegment>>;

    /// Once a process opens segments with the Persister, they'll know the last segment that was persisted.
    /// This can be used with a `Bufferer` to pass into this method, which will look for any WAL segments with an
    /// ID greater than the one passed in.
    async fn load_segments_after(
        &self,
        segment_id: SegmentId,
        catalog: Catalog,
    ) -> Result<Vec<Arc<dyn BufferSegment>>>;

    /// Returns the configured WAL, if there is one.
    fn wal(&self) -> Option<Arc<impl Wal>>;

    /// Returns the catalog
    fn catalog(&self) -> Arc<catalog::Catalog>;
}

/// A segment in the buffer that corresponds to a single WAL segment file. It contains a catalog with any updates
/// that have been made to it since the segment was opened. It can convert all buffered data to parquet data that
/// can be persisted.
#[async_trait]
pub trait BufferSegment: Debug + Send + Sync + 'static {
    fn id(&self) -> SegmentId;

    fn catalog(&self) -> Arc<catalog::Catalog>;

    /// If the catalog has been updated in this buffer segment, it is written using the passed in persister. Then it
    /// writes all data in the buffered segment to parquet files using the passed persister. Finally, it writes
    /// the segment file with all parquet file summaries to object storage using the passed persister.
    async fn persist(&self, persister: Arc<dyn Persister<Error = persister::Error>>) -> Result<()>;
}

/// ChunkContainer is used by the query engine to get chunks for a given table. Chunks will generally be in the
/// `Bufferer` for those in memory from buffered writes or the `Persister` for parquet files that have been persisted
/// from previously buffered segments.
pub trait ChunkContainer: Debug + Send + Sync + 'static {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}

/// The segment identifier, which will be monotonically increasing.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SegmentId(u32);
pub type SegmentIdBytes = [u8; 4];

impl SegmentId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_bytes(&self) -> SegmentIdBytes {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: SegmentIdBytes) -> Self {
        Self(u32::from_be_bytes(bytes))
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SequenceNumber(u32);

impl SequenceNumber {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[async_trait]
pub trait Persister: Debug + Send + Sync + 'static {
    type Error;

    /// Loads the most recently persisted catalog from object storage.
    async fn load_catalog(&self) -> Result<Option<PersistedCatalog>, Self::Error>;

    /// Loads the most recently persisted N segment parquet file lists from object storage.
    async fn load_segments(
        &self,
        most_recent_n: usize,
    ) -> Result<Vec<PersistedSegment>, Self::Error>;

    // Loads a Parquet file from ObjectStore
    async fn load_parquet_file(&self, path: ParquetFilePath) -> Result<Bytes, Self::Error>;

    /// Persists the catalog with the given segment ID. If this is the highest segment ID, it will
    /// be the catalog that is returned the next time `load_catalog` is called.
    async fn persist_catalog(
        &self,
        segment_id: SegmentId,
        catalog: catalog::Catalog,
    ) -> Result<(), Self::Error>;

    /// Writes a single file to object storage that contains the information for the parquet files persisted
    /// for this segment.
    async fn persist_segment(&self, persisted_segment: PersistedSegment)
        -> Result<(), Self::Error>;

    // Writes a SendableRecorgBatchStream to the Parquet format and persists it
    // to Object Store at the given path. Returns the number of bytes written and the file metadata.
    async fn persist_parquet_file(
        &self,
        path: ParquetFilePath,
        record_batch: SendableRecordBatchStream,
    ) -> Result<(u64, FileMetaData), Self::Error>;

    /// Returns the configured `ObjectStore` that data is loaded from and persisted to.
    fn object_store(&self) -> Arc<dyn object_store::ObjectStore>;

    fn as_any(&self) -> &dyn Any;
}

pub trait Wal: Debug + Send + Sync + 'static {
    /// Opens a writer to a segment, either creating a new file or appending to an existing file.
    fn open_segment_writer(&self, segment_id: SegmentId) -> wal::Result<Box<dyn WalSegmentWriter>>;

    /// Opens a reader to a segment file.
    fn open_segment_reader(&self, segment_id: SegmentId) -> wal::Result<Box<dyn WalSegmentReader>>;

    /// Checks the WAL directory for any segment files and returns them.
    fn segment_files(&self) -> wal::Result<Vec<SegmentFile>>;

    /// Deletes the WAL segment file from disk.
    fn delete_wal_segment(&self, segment_id: SegmentId) -> wal::Result<()>;
}

#[derive(Debug)]
pub struct SegmentFile {
    /// The path to the segment file
    pub path: PathBuf,
    /// The segment id parsed from the file name
    pub segment_id: SegmentId,
}

pub trait WalSegmentWriter: Debug + Send + Sync + 'static {
    fn id(&self) -> SegmentId;

    fn bytes_written(&self) -> u64;

    fn write_batch(&mut self, ops: Vec<WalOp>) -> wal::Result<SequenceNumber>;

    fn last_sequence_number(&self) -> SequenceNumber;
}

pub trait WalSegmentReader: Debug + Send + Sync + 'static {
    fn id(&self) -> SegmentId;

    fn next_batch(&mut self) -> wal::Result<Option<WalOpBatch>>;
}

/// Individual WalOps get batched into the WAL asynchronously. The batch is then written to the segment file.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WalOpBatch {
    pub sequence_number: SequenceNumber,
    pub ops: Vec<WalOp>,
}

/// A WalOp can be write of line protocol, the creation of a database, or other kinds of state that eventually
/// lands in object storage. Things in the WAL are buffered until they are persisted to object storage. The write
/// is called an `LpWrite` because it is a write of line protocol and we intend to have a new write protocol for
/// 3.0 that supports a different kind of schema.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum WalOp {
    LpWrite(LpWriteOp),
}

/// A write of 1 or more lines of line protocol to a single database. The default time is set by the server at the
/// time the write comes in.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct LpWriteOp {
    pub db_name: String,
    pub lp: String,
    pub default_time: i64,
}

/// A single write request can have many lines in it. A writer can request to accept all lines that are valid, while
/// returning an error for any invalid lines. This is the error information for a single invalid line.
#[derive(Debug, Serialize)]
pub struct WriteLineError {
    pub original_line: String,
    pub line_number: usize,
    pub error_message: String,
}

/// A write that has been validated against the catalog schema, written to the WAL (if configured), and buffered in
/// memory. This is the summary information for the write along with any errors that were encountered.
#[derive(Debug)]
pub struct BufferedWriteRequest {
    pub db_name: NamespaceName<'static>,
    pub invalid_lines: Vec<WriteLineError>,
    pub line_count: usize,
    pub field_count: usize,
    pub tag_count: usize,
    pub total_buffer_memory_used: usize,
    pub segment_id: SegmentId,
    pub sequence_number: SequenceNumber,
}

/// A persisted Catalog that contains the database, table, and column schemas.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PersistedCatalog {
    /// The segment_id that this catalog was persisted with.
    pub segment_id: SegmentId,
    /// The catalog that was persisted.
    pub catalog: catalog::InnerCatalog,
}

/// The collection of Parquet files that were persisted for a segment.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PersistedSegment {
    /// The segment_id that these parquet files were persisted with.
    pub segment_id: SegmentId,
    /// The size of the segment WAL in bytes.
    pub segment_wal_size_bytes: u64,
    /// The size of the segment parquet files in bytes.
    pub segment_parquet_size_bytes: u64,
    /// The number of rows across all parquet files in the segment.
    pub segment_row_count: u64,
    /// The min time from all parquet files in the segment.
    pub segment_min_time: i64,
    /// The max time from all parquet files in the segment.
    pub segment_max_time: i64,
    /// The collection of databases that had tables persisted in this segment. The tables will then have their
    /// name and the parquet files.
    pub databases: HashMap<String, DatabaseTables>,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct DatabaseTables {
    pub tables: HashMap<String, TableParquetFiles>,
}

/// A collection of parquet files persisted in a segment for a specific table.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TableParquetFiles {
    /// The table name.
    pub table_name: String,
    /// The parquet files persisted in this segment for this table.
    pub parquet_files: Vec<ParquetFile>,
    /// The sort key used for all parquet files in this segment.
    pub sort_key: Vec<String>,
}

/// The summary data for a persisted parquet file in a segment.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ParquetFile {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,
    pub min_time: i64,
    pub max_time: i64,
}

/// The summary data for a persisted parquet file in a segment.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Auto,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Auto
    }
}

/// Guess precision based off of a given timestamp.
// Note that this will fail in June 2128, but that's not our problem
pub(crate) fn guess_precision(timestamp: i64) -> Precision {
    const NANO_SECS_PER_SEC: i64 = 1_000_000_000;
    // Get the absolute value of the timestamp so we can work with negative
    // numbers
    let val = timestamp.abs() / NANO_SECS_PER_SEC;

    if val < 5 {
        // If the time sent to us is in seconds then this will be a number less than
        // 5 so for example if the time in seconds is 1_708_976_567 then it will be
        // 1 (due to integer truncation) and be less than 5
        Precision::Second
    } else if val < 5_000 {
        // If however the value is milliseconds and not seconds than the same number
        // for time but now in milliseconds 1_708_976_567_000 when divided will now
        // be 1708 which is bigger than the previous if statement but less than this
        // one and so we return milliseconds
        Precision::Millisecond
    } else if val < 5_000_000 {
        // If we do the same thing here by going up another order of magnitude then
        // 1_708_976_567_000_000 when divided will be 1708976 which is large enough
        // for this if statement
        Precision::Microsecond
    } else {
        // Anything else we can assume is large enough of a number that it must
        // be nanoseconds
        Precision::Nanosecond
    }
}

#[cfg(test)]
mod test_helpers {
    use crate::catalog::{Catalog, DatabaseSchema};
    use crate::write_buffer::buffer_segment::WriteBatch;
    use crate::write_buffer::{parse_validate_and_update_schema, Partitioner, TableBatch};
    use crate::Precision;
    use data_types::NamespaceName;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub(crate) fn lp_to_write_batch(
        catalog: &Catalog,
        db_name: &'static str,
        lp: &str,
    ) -> WriteBatch {
        let mut write_batch = WriteBatch::default();
        let (seq, db) = catalog.db_or_create(db_name).unwrap();
        let partitioner = Partitioner::new_per_day_partitioner();
        let result = parse_validate_and_update_schema(
            lp,
            &db,
            &partitioner,
            0,
            false,
            Precision::Nanosecond,
        )
        .unwrap();
        if let Some(db) = result.schema {
            catalog.replace_database(seq, Arc::new(db)).unwrap();
        }
        let db_name = NamespaceName::new(db_name).unwrap();
        write_batch.add_db_write(db_name, result.table_batches);
        write_batch
    }

    pub(crate) fn lp_to_table_batches(lp: &str, default_time: i64) -> HashMap<String, TableBatch> {
        let db = Arc::new(DatabaseSchema::new("foo"));
        let partitioner = Partitioner::new_per_day_partitioner();
        let result = parse_validate_and_update_schema(
            lp,
            &db,
            &partitioner,
            default_time,
            false,
            Precision::Nanosecond,
        )
        .unwrap();

        result.table_batches
    }
}
