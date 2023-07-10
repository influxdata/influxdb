//! # WAL
//!
//! This crate provides a local-disk WAL for the IOx ingestion pipeline.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader},
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use data_types::{sequence_number_set::SequenceNumberSet, NamespaceId, TableId};
use generated_types::{
    google::{FieldViolation, OptionalField},
    influxdata::iox::wal::v1::{
        sequenced_wal_op::Op as WalOp, SequencedWalOp as ProtoSequencedWalOp,
    },
};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_pb::decode::decode_database_batch;
use observability_deps::tracing::info;
use parking_lot::Mutex;
use snafu::prelude::*;
use tokio::{sync::watch, task::JoinHandle};
use writer_thread::WriterIoThreadHandle;

use crate::blocking::{
    ClosedSegmentFileReader as RawClosedSegmentFileReader, OpenSegmentFileWriter,
};

pub mod blocking;
mod writer_thread;

const WAL_FLUSH_INTERVAL: Duration = Duration::from_millis(10);

// TODO: Should have more variants / error types to avoid reusing these
#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    SegmentFileIdentifierMismatch {},

    UnableToReadFileMetadata {
        source: io::Error,
    },

    UnableToCreateWalDir {
        source: io::Error,
        path: PathBuf,
    },

    UnableToWriteSequenceNumber {
        source: io::Error,
    },

    UnableToWriteChecksum {
        source: io::Error,
    },

    UnableToWriteLength {
        source: io::Error,
    },

    UnableToWriteData {
        source: io::Error,
    },

    UnableToSync {
        source: io::Error,
    },

    UnableToReadDirectoryContents {
        source: io::Error,
        path: PathBuf,
    },

    UnableToOpenFile {
        source: blocking::ReaderError,
        path: PathBuf,
    },

    UnableToSendRequestToReaderTask,

    UnableToReceiveResponseFromSenderTask {
        source: tokio::sync::oneshot::error::RecvError,
    },

    UnableToReadFileHeader {
        source: blocking::ReaderError,
    },

    UnableToReadEntries {
        source: blocking::ReaderError,
    },

    UnableToReadNextOps {
        source: blocking::ReaderError,
    },

    InvalidId {
        filename: String,
        source: std::num::ParseIntError,
    },

    SegmentNotFound {
        id: SegmentId,
    },

    DeleteClosedSegment {
        source: std::io::Error,
        path: PathBuf,
    },

    OpenSegmentDirectory {
        source: std::io::Error,
        path: PathBuf,
    },

    UnableToWrite {
        source: blocking::WriterError,
    },

    UnableToCreateSegmentFile {
        source: blocking::WriterError,
    },
}

/// Errors that occur when decoding internal types from a WAL file.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DecodeError {
    UnableToCreateMutableBatch {
        source: mutable_batch_pb::decode::Error,
    },

    FailedToReadWal {
        source: Error,
    },
}

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Segments are identified by a u64 that indicates file ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(u64);

pub type SegmentIdBytes = [u8; 8];

#[allow(missing_docs)]
impl SegmentId {
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn as_bytes(&self) -> SegmentIdBytes {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: SegmentIdBytes) -> Self {
        let v = u64::from_be_bytes(bytes);
        Self::new(v)
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) fn build_segment_path(dir: impl Into<PathBuf>, id: SegmentId) -> PathBuf {
    let mut path = dir.into();
    path.push(id.to_string());
    path.set_extension(SEGMENT_FILE_EXTENSION);
    path
}

/// The first bytes written into a segment file to identify it and its version.
// TODO: What's the expected way of upgrading -- what happens when we need version 31?
type FileTypeIdentifier = [u8; 8];
const FILE_TYPE_IDENTIFIER: &FileTypeIdentifier = b"INFLUXV3";
/// File extension for segment files.
const SEGMENT_FILE_EXTENSION: &str = "dat";

/// The main type representing one WAL for one ingester instance.
///
/// # Constraints
///
/// Creating multiple separate instances of this type using the same root path as the storage
/// location is not supported. Each instance needs to be the only logical owner of the files within
/// its root directory.
///
/// Similarly, editing or deleting files within a `Wal`'s root directory via some other mechanism
/// is not supported.
pub struct Wal {
    root: PathBuf,
    segments: Arc<Mutex<Segments>>,
    next_id_source: Arc<AtomicU64>,
    buffer: Mutex<WalBuffer>,

    /// The handle to the [`Wal::flush_buffer_background_task()`] task.
    flusher_task: Mutex<Option<JoinHandle<()>>>,
}

impl Wal {
    /// Creates a `Wal` instance that manages files in the specified root directory.
    /// # Constraints
    ///
    /// Creating multiple separate instances of this type using the same root path as the storage
    /// location is not supported. Each instance needs to be the only logical owner of the files
    /// within its root directory.
    ///
    /// Similarly, editing or deleting files within a `Wal`'s root directory via some other
    /// mechanism is not supported.
    pub async fn new(root: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let root = root.into();
        info!(wal_dir=?root, "Initalizing Write Ahead Log (WAL)");
        tokio::fs::create_dir_all(&root)
            .await
            .context(UnableToCreateWalDirSnafu { path: &root })?;

        // ensure the directory creation is actually fsync'd so that when we create files there
        // we don't lose them (see: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf)
        File::open(&root)
            .expect("should be able to open just-created directory")
            .sync_all()
            .expect("fsync failure");

        let mut dir = tokio::fs::read_dir(&root)
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: &root })?;

        // Closed segments must be ordered by ID, which is the order they were written in and the
        // order they should be replayed in.
        let mut closed_segments = BTreeMap::new();

        while let Some(child) = dir
            .next_entry()
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: &root })?
        {
            let metadata = child
                .metadata()
                .await
                .context(UnableToReadFileMetadataSnafu)?;
            if metadata.is_file() {
                let child_path = child.path();
                let filename = child_path
                    .file_stem()
                    .expect("WAL files created by IOx should have a file stem");
                let filename = filename
                    .to_str()
                    .expect("WAL files created by IOx should be named with valid UTF-8");
                let id = SegmentId::new(filename.parse().context(InvalidIdSnafu { filename })?);
                let segment = ClosedSegment {
                    id,
                    path: child.path(),
                    size: metadata.len(),
                };
                closed_segments.insert(id, segment);
            }
        }

        let next_id = closed_segments
            .keys()
            .last()
            .copied()
            .map(|id| id.get() + 1)
            .unwrap_or(0);
        let next_id_source = Arc::new(AtomicU64::new(next_id));
        let open_segment =
            OpenSegmentFileWriter::new_in_directory(&root, Arc::clone(&next_id_source))
                .context(UnableToCreateSegmentFileSnafu)?;

        let buffer = WalBuffer::new(None);

        let wal = Self {
            root,
            segments: Arc::new(Mutex::new(Segments {
                closed_segments,
                open_segment,
                open_segment_ids: SequenceNumberSet::default(),
            })),
            next_id_source,
            buffer: Mutex::new(buffer),
            flusher_task: Default::default(),
        };

        let wal = Arc::new(wal);
        let flush_wal = Arc::clone(&wal);

        // Retain the handle to the flusher task so it can be stopped later.
        *wal.flusher_task.lock() = Some(tokio::task::spawn(async move {
            flush_wal.flush_buffer_background_task().await
        }));

        Ok(wal)
    }

    /// Get the closed segments from the WAL
    pub fn closed_segments(&self) -> Vec<ClosedSegment> {
        let s = self.segments.lock();
        s.closed_segments.values().cloned().collect()
    }

    /// Open a reader to a closed segment
    pub fn reader_for_segment(&self, id: SegmentId) -> Result<ClosedSegmentFileReader> {
        let path = build_segment_path(&self.root, id);
        ClosedSegmentFileReader::from_path(path)
    }

    /// Writes one [`SequencedWalOp`] to the buffer and returns a watch channel
    /// for when the buffer is flushed and fsync'd to disk.
    pub fn write_op(&self, op: SequencedWalOp) -> watch::Receiver<Option<WriteResult>> {
        let mut b = self.buffer.lock();
        b.ops.push(op);
        b.flush_notification.clone()
    }

    /// Closes the currently open segment and opens a new one, returning the
    /// closed segment details, including the [`SequenceNumberSet`] containing
    /// the sequence numbers of the writes within the closed segment.
    pub fn rotate(&self) -> Result<(ClosedSegment, SequenceNumberSet)> {
        let new_open_segment =
            OpenSegmentFileWriter::new_in_directory(&self.root, Arc::clone(&self.next_id_source))
                .context(UnableToCreateSegmentFileSnafu)?;

        let mut segments = self.segments.lock();

        let closed = std::mem::replace(&mut segments.open_segment, new_open_segment);
        let seqnum_set = std::mem::take(&mut segments.open_segment_ids);
        let closed = closed.close().expect("should convert to closed segment");

        let previous_value = segments.closed_segments.insert(closed.id(), closed.clone());
        assert!(
            previous_value.is_none(),
            "should always add new closed segment entries, not replace"
        );

        Ok((closed, seqnum_set))
    }

    async fn flush_buffer_background_task(&self) {
        // Start a separate I/O thread to handle the serialisation, compression,
        // and actual file I/O.
        //
        // This prevents the file I/O from blocking an async runtime thread,
        // which in turn would starve it of the ability to service other tasks.
        //
        // When this handle is dropped, the I/O thread is gracefully stopped.
        let io_thread = WriterIoThreadHandle::new(Arc::clone(&self.segments));

        let mut interval = tokio::time::interval(WAL_FLUSH_INTERVAL);

        // Pre-allocate the WAL buffer outside of the exclusive lock, and track
        // the buffer utilisation to optimise pre-allocation.
        let mut size_hint = None;
        let mut new_buf = WalBuffer::new(size_hint);

        loop {
            interval.tick().await;

            // Rust's move properties ensure we never accidentally reuse a
            // buffer, but make it clear the buffer is always fresh before use.
            assert!(new_buf.ops.is_empty());

            // only write to the disk if there are writes buffered
            let filled_buffer = {
                let mut b = self.buffer.lock();
                if b.ops.is_empty() {
                    // Don't replace the buffer, as the current buffer is empty.
                    //
                    // This doesn't throw away the pre-allocated buffer - that
                    // can be used later!
                    continue;
                }

                // Update the size hint to match the size of the last batch.
                size_hint = Some(b.ops.len());

                // Move the pre-allocated buffer to replace the old.
                std::mem::replace(&mut *b, new_buf)
            };

            // Allocate the next buffer, using the previous buffer size as a
            // pre-allocation hint.
            new_buf = WalBuffer::new(size_hint);

            io_thread.enqueue_batch(filled_buffer).await;
        }
    }

    /// Deletes the specified segment from disk.
    pub async fn delete(&self, id: SegmentId) -> Result<()> {
        let closed = self
            .segments
            .lock()
            .closed_segments
            .remove(&id)
            .context(SegmentNotFoundSnafu { id })?;
        std::fs::remove_file(&closed.path).context(DeleteClosedSegmentSnafu { path: closed.path })
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Stop the background flusher task, if any.
        if let Some(t) = self.flusher_task.lock().take() {
            t.abort()
        }
    }
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("root", &self.root)
            .field("next_id_source", &self.next_id_source)
            .finish()
    }
}

#[derive(Debug)]
struct Segments {
    closed_segments: BTreeMap<SegmentId, ClosedSegment>,
    open_segment: OpenSegmentFileWriter,
    open_segment_ids: SequenceNumberSet,
}

#[derive(Debug)]
struct WalBuffer {
    ops: Vec<SequencedWalOp>,
    notify_flush: tokio::sync::watch::Sender<Option<WriteResult>>,
    flush_notification: tokio::sync::watch::Receiver<Option<WriteResult>>,
}

impl WalBuffer {
    fn new(size_hint: Option<usize>) -> Self {
        let (tx, rx) = tokio::sync::watch::channel::<Option<WriteResult>>(None);

        Self {
            ops: Vec::with_capacity(size_hint.unwrap_or(20)),
            notify_flush: tx,
            flush_notification: rx,
        }
    }
}

/// A wal operation with a sequence number
#[derive(Debug, PartialEq, Clone)]
pub struct SequencedWalOp {
    /// This mapping assigns a sequence number to table ID modified by this
    /// write.
    pub table_write_sequence_numbers: std::collections::HashMap<TableId, u64>,
    /// The underlying WAL operation which this wrapper sequences.
    pub op: WalOp,
}

impl TryFrom<ProtoSequencedWalOp> for SequencedWalOp {
    type Error = FieldViolation;

    fn try_from(proto: ProtoSequencedWalOp) -> Result<Self, Self::Error> {
        let ProtoSequencedWalOp {
            table_write_sequence_numbers,
            op,
        } = proto;

        Ok(Self {
            table_write_sequence_numbers: table_write_sequence_numbers
                .into_iter()
                .map(|(table_id, sequence_number)| (TableId::new(table_id), sequence_number))
                .collect(),
            op: op.unwrap_field("op")?,
        })
    }
}

impl From<SequencedWalOp> for ProtoSequencedWalOp {
    fn from(seq_op: SequencedWalOp) -> Self {
        let SequencedWalOp {
            table_write_sequence_numbers,
            op,
        } = seq_op;

        Self {
            table_write_sequence_numbers: table_write_sequence_numbers
                .into_iter()
                .map(|(table_id, sequence_number)| (table_id.get(), sequence_number))
                .collect(),
            op: Some(op),
        }
    }
}

/// Raw, uncompressed and unstructured data for a Segment entry with a checksum.
#[derive(Debug, Eq, PartialEq)]
pub struct SegmentEntry {
    /// The uncompressed data
    pub data: Vec<u8>,
}

/// Result from a WAL flush and fsync. This needs to be cloneable which is why it doesn't
/// use a regular error type. Also, receivers of this can't really do anything with the
/// error result other than determining that there was an error and that they should do an
/// orderly shutdown.
#[derive(Debug, Clone)]
pub enum WriteResult {
    Ok(WriteSummary),
    Err(String),
}

/// Summary information after a write
#[derive(Debug, Copy, Clone)]
pub struct WriteSummary {
    /// Total size of the segment in bytes
    pub total_bytes: usize,
    /// Number of bytes written to segment in this write
    pub bytes_written: usize,
    /// Which segment file this entry was written to
    pub segment_id: SegmentId,
}

/// Reader for a closed segment file
pub struct ClosedSegmentFileReader {
    id: SegmentId,
    file: RawClosedSegmentFileReader<BufReader<File>>,
}

impl Iterator for ClosedSegmentFileReader {
    type Item = Result<Vec<SequencedWalOp>>;

    /// Read the next batch of sequenced WAL operations from the file
    fn next(&mut self) -> Option<Self::Item> {
        self.file
            .next_batch()
            .context(UnableToReadNextOpsSnafu)
            .transpose()
    }
}

impl ClosedSegmentFileReader {
    /// Return the segment file id
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Open the segment file and read its header, ensuring it is a segment file and reading its id.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut file =
            RawClosedSegmentFileReader::from_path(path).context(UnableToOpenFileSnafu { path })?;

        let (file_type, id) = file.read_header().context(UnableToReadFileHeaderSnafu)?;

        ensure!(
            &file_type == FILE_TYPE_IDENTIFIER,
            SegmentFileIdentifierMismatchSnafu,
        );

        let id = SegmentId::from_bytes(id);

        Ok(Self { id, file })
    }
}

impl std::fmt::Debug for ClosedSegmentFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosedSegmentFileReader")
            .field("id", &self.id)
            .finish()
    }
}

/// An in-memory representation of a WAL write operation entry.
#[derive(Debug)]
pub struct WriteOpEntry {
    pub namespace: NamespaceId,
    pub table_batches: HashMap<TableId, MutableBatch>,
}

/// A decoder that reads from a closed segment file and parses write
/// operations from their on-disk format to an internal format.
#[derive(Debug)]
pub struct WriteOpEntryDecoder {
    reader: ClosedSegmentFileReader,
}

impl From<ClosedSegmentFileReader> for WriteOpEntryDecoder {
    /// Creates a decoder which will use the closed segment file of `reader` to
    /// decode write ops from their on-disk format.
    fn from(reader: ClosedSegmentFileReader) -> Self {
        Self { reader }
    }
}

impl Iterator for WriteOpEntryDecoder {
    type Item = Result<Vec<WriteOpEntry>, DecodeError>;

    /// Reads a collection of write op entries in the next WAL entry batch from the
    /// underlying closed segment. A returned Ok(None) indicates that there are no
    /// more entries to be decoded from the underlying segment. A zero-length vector
    /// may be returned if there are no writes in a WAL entry batch, but does not
    /// indicate the decoder is consumed.
    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.reader
                .next()?
                .context(FailedToReadWalSnafu)
                .map(|batch| {
                    batch
                        .into_iter()
                        .filter_map(|sequenced_op| match sequenced_op.op {
                            WalOp::Write(w) => Some(w),
                            WalOp::Delete(..) => None,
                            WalOp::Persist(..) => None,
                        })
                        .map(|w| {
                            Ok(WriteOpEntry {
                                namespace: NamespaceId::new(w.database_id),
                                table_batches: decode_database_batch(&w)
                                    .context(UnableToCreateMutableBatchSnafu)?
                                    .into_iter()
                                    .map(|(id, mb)| (TableId::new(id), mb))
                                    .collect(),
                            })
                        })
                        .collect::<Self::Item>()
                })
                .unwrap_or_else(Err),
        )
    }
}

/// Metadata for a WAL segment that is no longer accepting writes, but can be read for replay
/// purposes.
#[derive(Debug, Clone)]
pub struct ClosedSegment {
    id: SegmentId,
    path: PathBuf,
    size: u64,
}

impl ClosedSegment {
    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs::{read_dir, OpenOptions};
    use std::io::Write;

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, SequenceNumber, TableId};
    use dml::DmlWrite;
    use generated_types::influxdata::{
        iox::{delete::v1::DeletePayload, wal::v1::PersistOp},
        pbdata::v1::DatabaseBatch,
    };
    use mutable_batch_lp::lines_to_batches;

    use super::*;

    const TEST_NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

    #[tokio::test]
    async fn wal_write_and_read_ops() {
        let dir = test_helpers::tmp_dir().unwrap();
        let wal = Wal::new(&dir.path()).await.unwrap();

        let w1 = test_data("m1,t=foo v=1i 1");
        // Use multiple tables for a write to test per-partition sequencing is preserved
        let w2 = test_data("m1,t=foo v=2i 2\nm2,u=bar v=1i 1");

        let op1 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 0)].into_iter().collect(),
            op: WalOp::Write(w1),
        };
        let op2 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 1), (TableId::new(1), 2)]
                .into_iter()
                .collect(),
            op: WalOp::Write(w2),
        };
        let op3 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 3)].into_iter().collect(),
            op: WalOp::Delete(test_delete()),
        };
        let op4 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 3)].into_iter().collect(),
            op: WalOp::Persist(test_persist()),
        };

        wal.write_op(op1.clone());
        wal.write_op(op2.clone());
        wal.write_op(op3.clone());
        wal.write_op(op4.clone()).changed().await.unwrap();

        let (closed, ids) = wal.rotate().unwrap();

        let ops: Vec<SequencedWalOp> = wal
            .reader_for_segment(closed.id)
            .expect("should be able to open reader for closed WAL segment")
            .flat_map(|batch| batch.expect("failed to read WAL op batch"))
            .collect();
        assert_eq!(vec![op1, op2, op3, op4], ops);

        // Assert the set has recorded the op IDs.
        //
        // Note that one op has a duplicate sequence number above!
        assert_eq!(ids.len(), 4);

        // Assert the sequence number set contains the specified ops.
        let ids = ids.iter().collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                SequenceNumber::new(0),
                SequenceNumber::new(1),
                SequenceNumber::new(2),
                SequenceNumber::new(3),
            ]
        );

        // Assert the partitioned sequence numbers contain the correct values
        assert_eq!(
            ops.into_iter()
                .map(|op| op.table_write_sequence_numbers)
                .collect::<Vec<std::collections::HashMap<TableId, u64>>>(),
            [
                [(TableId::new(0), 0)].into_iter().collect(),
                [(TableId::new(0), 1), (TableId::new(1), 2)]
                    .into_iter()
                    .collect(),
                [(TableId::new(0), 3)].into_iter().collect(),
                [(TableId::new(0), 3)].into_iter().collect(),
            ]
            .into_iter()
            .collect::<Vec<std::collections::HashMap<TableId, u64>>>(),
        );
    }

    // open wal with files that aren't segments (should log and skip)

    // read segment works even if last entry is truncated

    #[tokio::test]
    async fn rotate_without_writes() {
        let dir = test_helpers::tmp_dir().unwrap();

        let wal = Wal::new(dir.path()).await.unwrap();

        // Just-created WALs have no closed segments.
        let closed = wal.closed_segments();
        assert!(
            closed.is_empty(),
            "Expected empty closed segments; got {closed:?}"
        );

        // No writes, but rotating is totally fine
        let (closed_segment_details, ids) = wal.rotate().unwrap();
        assert_eq!(closed_segment_details.size(), 16);
        assert!(ids.is_empty());

        // There's one closed segment
        let closed = wal.closed_segments();
        let closed_segment_ids: Vec<_> = closed.iter().map(|c| c.id()).collect();
        assert_eq!(closed_segment_ids, &[closed_segment_details.id()]);

        // There aren't any entries in the closed segment because nothing was written
        let mut reader = wal.reader_for_segment(closed_segment_details.id()).unwrap();
        assert!(reader.next().is_none());

        // Can delete an empty segment, leaving no closed segments again
        wal.delete(closed_segment_details.id()).await.unwrap();
        let closed = wal.closed_segments();
        assert!(
            closed.is_empty(),
            "Expected empty closed segments; got {closed:?}"
        );
    }

    #[tokio::test]
    async fn decode_write_op_entries() {
        let dir = test_helpers::tmp_dir().unwrap();
        let wal = Wal::new(dir.path()).await.unwrap();

        let w1 = test_data("m1,t=foo v=1i 1");
        let w2 = test_data("m1,t=foo v=2i 2\nm2,u=foo w=2i 2");
        let w3 = test_data("m1,t=foo v=3i 3");

        let op1 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 0)].into_iter().collect(),
            op: WalOp::Write(w1.to_owned()),
        };
        let op2 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 1), (TableId::new(1), 2)]
                .into_iter()
                .collect(),
            op: WalOp::Write(w2.to_owned()),
        };
        let op3 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 3)].into_iter().collect(),
            op: WalOp::Delete(test_delete()),
        };
        let op4 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 3)].into_iter().collect(),
            op: WalOp::Persist(test_persist()),
        };
        // A third write entry coming after a delete and persist entry must still be yielded
        let op5 = SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 4)].into_iter().collect(),
            op: WalOp::Write(w3.to_owned()),
        };

        wal.write_op(op1);
        wal.write_op(op2);
        wal.write_op(op3).changed().await.unwrap();
        wal.write_op(op4);
        wal.write_op(op5).changed().await.unwrap();

        let (closed, _) = wal.rotate().unwrap();

        let decoder = WriteOpEntryDecoder::from(
            wal.reader_for_segment(closed.id)
                .expect("failed to open reader for closed WAL segment"),
        );

        let wal_entries = decoder
            .into_iter()
            .map(|r| r.expect("unexpected bad entry"))
            .collect::<Vec<_>>();
        // The decoder should find 2 entries, with a total of 3 write ops
        assert_eq!(wal_entries.len(), 2);
        let write_op_entries = wal_entries.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(write_op_entries.len(), 3);
        assert_matches!(write_op_entries.get(0), Some(got_op1) => {
            assert_op_shape(got_op1, &w1);
        });
        assert_matches!(write_op_entries.get(1), Some(got_op2) => {
            assert_op_shape(got_op2, &w2);
        });
        assert_matches!(write_op_entries.get(2), Some(got_op3) => {
            assert_op_shape(got_op3, &w3);
        });
    }

    #[tokio::test]
    async fn decode_write_op_entry_from_corrupted_wal() {
        let dir = test_helpers::tmp_dir().unwrap();
        let wal = Wal::new(dir.path()).await.unwrap();

        // Log a write operation to test recovery from a tail-corrupted WAL.
        let good_write = test_data("m3,a=baz b=4i 1");
        wal.write_op(SequencedWalOp {
            table_write_sequence_numbers: vec![(TableId::new(0), 0)].into_iter().collect(),
            op: WalOp::Write(good_write.to_owned()),
        })
        .changed()
        .await
        .unwrap();

        // Append some garbage to the tail end of the WAL, then rotate it
        {
            let mut reader = read_dir(dir.path()).unwrap();
            let closed_path = reader
                .next()
                .expect("no segment file found in WAL dir")
                .unwrap()
                .path();
            assert_matches!(reader.next(), None);

            OpenOptions::new()
                .append(true)
                .open(closed_path)
                .expect("unable to open closed WAL segment for writing")
                .write_all(b"ceci ne pas une banane")
                .unwrap();
        }
        let (closed, _) = wal.rotate().expect("failed to rotate WAL");

        let mut decoder = WriteOpEntryDecoder::from(
            wal.reader_for_segment(closed.id())
                .expect("failed to open reader for closed segment"),
        );
        // Recover the single entry in front of the garbage and assert the expected
        // error is thrown
        assert_matches!(decoder.next(), Some(Ok(batch)) => {
            assert_eq!(batch.len(), 1);
            assert_op_shape(batch.get(0).unwrap(), &good_write);
        });
        assert_matches!(
            decoder.next(),
            Some(Err(DecodeError::FailedToReadWal { .. }))
        );
    }

    fn assert_op_shape(left: &WriteOpEntry, right: &DatabaseBatch) {
        assert_eq!(left.namespace, NamespaceId::new(right.database_id));
        assert_eq!(left.table_batches.len(), right.table_batches.len());
        for right_tb in &right.table_batches {
            let right_key = TableId::new(right_tb.table_id);
            let left_mb = left
                .table_batches
                .get(&right_key)
                .unwrap_or_else(|| panic!("left value missing table batch for key {right_key}"));
            assert_eq!(
                left_mb.column_names(),
                right_tb
                    .columns
                    .iter()
                    .map(|c| c.column_name.as_str())
                    .collect::<BTreeSet<_>>()
            )
        }
    }

    fn test_data(lp: &str) -> DatabaseBatch {
        let batches = lines_to_batches(lp, 0).unwrap();
        let batches = batches
            .into_iter()
            .enumerate()
            .map(|(i, (_table_name, batch))| (TableId::new(i as _), batch))
            .collect();

        let write = DmlWrite::new(
            TEST_NAMESPACE_ID,
            batches,
            "bananas".into(),
            Default::default(),
        );

        mutable_batch_pb::encode::encode_write(42, &write)
    }

    fn test_delete() -> DeletePayload {
        DeletePayload {
            database_id: TEST_NAMESPACE_ID.get(),
            predicate: None,
            table_name: "bananas".into(),
        }
    }

    fn test_persist() -> PersistOp {
        PersistOp {
            namespace_id: TEST_NAMESPACE_ID.get(),
            parquet_file_uuid: "b4N4N4Z".into(),
            partition_id: 43,
            table_id: 44,
        }
    }
}
