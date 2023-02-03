#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! # WAL
//!
//! This crate provides a local-disk WAL for the IOx ingestion pipeline.

use crate::blocking::{
    ClosedSegmentFileReader as RawClosedSegmentFileReader, OpenSegmentFileWriter,
};
use generated_types::{
    google::{FieldViolation, OptionalField},
    influxdata::iox::wal::v1::{
        sequenced_wal_op::Op as WalOp, SequencedWalOp as ProtoSequencedWalOp,
        WalOpBatch as ProtoWalOpBatch,
    },
};
use observability_deps::tracing::info;
use parking_lot::Mutex;
use prost::Message;
use snafu::prelude::*;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader},
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::sync::watch;

pub mod blocking;

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

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Segments are identified by a u64 that indicates file ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(u64);

pub type SegmentIdBytes = [u8; 8];

#[allow(missing_docs)]
impl SegmentId {
    pub fn new(v: u64) -> Self {
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
    segments: Mutex<Segments>,
    next_id_source: Arc<AtomicU64>,
    buffer: Mutex<WalBuffer>,
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

        let buffer = WalBuffer::new();

        let wal = Self {
            root,
            segments: Mutex::new(Segments {
                closed_segments,
                open_segment,
            }),
            next_id_source,
            buffer: Mutex::new(buffer),
        };

        let wal = Arc::new(wal);
        let flush_wal = Arc::clone(&wal);

        tokio::task::spawn(async move { flush_wal.flush_buffer_background_task().await });

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

    /// Writes one [`SequencedWalOp`] to the buffer and returns a watch channel for when the buffer
    /// is flushed and fsync'd to disk.
    pub fn write_op(&self, op: SequencedWalOp) -> watch::Receiver<Option<WriteResult>> {
        let mut b = self.buffer.lock();
        b.ops.push(op);
        b.flush_notification.clone()
    }

    /// Closes the currently open segment and opens a new one, returning the closed segment details.
    pub fn rotate(&self) -> Result<ClosedSegment> {
        let new_open_segment =
            OpenSegmentFileWriter::new_in_directory(&self.root, Arc::clone(&self.next_id_source))
                .context(UnableToCreateSegmentFileSnafu)?;

        let mut segments = self.segments.lock();

        let closed = std::mem::replace(&mut segments.open_segment, new_open_segment);
        let closed = closed.close().expect("should convert to closed segmet");

        let previous_value = segments.closed_segments.insert(closed.id(), closed.clone());
        assert!(
            previous_value.is_none(),
            "Should always add new closed segment entries, not replace"
        );

        Ok(closed)
    }

    async fn flush_buffer_background_task(&self) {
        let mut interval = tokio::time::interval(WAL_FLUSH_INTERVAL);

        loop {
            interval.tick().await;

            // only write to the disk if there are writes buffered
            let filled_buffer = {
                let mut b = self.buffer.lock();
                if b.ops.is_empty() {
                    continue;
                }

                std::mem::replace(&mut *b, WalBuffer::new())
            };

            // do the encoding while we're not holding any locks
            let ops: Vec<_> = filled_buffer
                .ops
                .into_iter()
                .map(ProtoSequencedWalOp::from)
                .collect();
            let batch = ProtoWalOpBatch { ops };
            let encoded = batch.encode_to_vec();

            // now write the data and let everyone know how things went
            let res = {
                let mut segments = self.segments.lock();
                match segments.open_segment.write(&encoded) {
                    Ok(summary) => WriteResult::Ok(summary),
                    Err(e) => WriteResult::Err(e.to_string()),
                }
            };

            // Do not panic if no thread is waiting for the flush notification -
            // this may be the case if all writes disconnected before the WAL
            // was flushed.
            let _ = filled_buffer.notify_flush.send(Some(res));
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

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("root", &self.root)
            .field("next_id_source", &self.next_id_source)
            .finish()
    }
}

struct Segments {
    closed_segments: BTreeMap<SegmentId, ClosedSegment>,
    open_segment: OpenSegmentFileWriter,
}

struct WalBuffer {
    ops: Vec<SequencedWalOp>,
    notify_flush: tokio::sync::watch::Sender<Option<WriteResult>>,
    flush_notification: tokio::sync::watch::Receiver<Option<WriteResult>>,
}

impl WalBuffer {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel::<Option<WriteResult>>(None);

        Self {
            ops: vec![],
            notify_flush: tx,
            flush_notification: rx,
        }
    }
}

/// A wal operation with a sequence number
#[derive(Debug, PartialEq, Clone)]
pub struct SequencedWalOp {
    pub sequence_number: u64,
    pub op: WalOp,
}

impl TryFrom<ProtoSequencedWalOp> for SequencedWalOp {
    type Error = FieldViolation;

    fn try_from(proto: ProtoSequencedWalOp) -> Result<Self, Self::Error> {
        let ProtoSequencedWalOp {
            sequence_number,
            op,
        } = proto;

        Ok(Self {
            sequence_number,
            op: op.unwrap_field("op")?,
        })
    }
}

impl From<SequencedWalOp> for ProtoSequencedWalOp {
    fn from(seq_op: SequencedWalOp) -> Self {
        let SequencedWalOp {
            sequence_number,
            op,
        } = seq_op;

        Self {
            sequence_number,
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

impl ClosedSegmentFileReader {
    /// Get the next batch of sequenced wal ops from the file
    pub fn next_batch(&mut self) -> Result<Option<Vec<SequencedWalOp>>> {
        self.file.next_batch().context(UnableToReadNextOpsSnafu)
    }

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
    use super::*;
    use data_types::{NamespaceId, TableId};
    use dml::DmlWrite;
    use generated_types::influxdata::{
        iox::{delete::v1::DeletePayload, wal::v1::PersistOp},
        pbdata::v1::DatabaseBatch,
    };
    use mutable_batch_lp::lines_to_batches;

    #[tokio::test]
    async fn wal_write_and_read_ops() {
        let dir = test_helpers::tmp_dir().unwrap();
        let wal = Wal::new(&dir.path()).await.unwrap();

        let w1 = test_data("m1,t=foo v=1i 1");
        let w2 = test_data("m1,t=foo v=2i 2");

        let op1 = SequencedWalOp {
            sequence_number: 0,
            op: WalOp::Write(w1),
        };
        let op2 = SequencedWalOp {
            sequence_number: 1,
            op: WalOp::Write(w2),
        };
        let op3 = SequencedWalOp {
            sequence_number: 2,
            op: WalOp::Delete(test_delete()),
        };
        let op4 = SequencedWalOp {
            sequence_number: 2,
            op: WalOp::Persist(test_persist()),
        };

        wal.write_op(op1.clone());
        wal.write_op(op2.clone());
        wal.write_op(op3.clone());
        wal.write_op(op4.clone()).changed().await.unwrap();

        let closed = wal.rotate().unwrap();

        let mut reader = wal.reader_for_segment(closed.id).unwrap();

        let mut ops = vec![];
        while let Ok(Some(mut batch)) = reader.next_batch() {
            ops.append(&mut batch);
        }
        assert_eq!(vec![op1, op2, op3, op4], ops);
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
        let closed_segment_details = wal.rotate().unwrap();
        assert_eq!(closed_segment_details.size(), 16);

        // There's one closed segment
        let closed = wal.closed_segments();
        let closed_segment_ids: Vec<_> = closed.iter().map(|c| c.id()).collect();
        assert_eq!(closed_segment_ids, &[closed_segment_details.id()]);

        // There aren't any entries in the closed segment because nothing was written
        let mut reader = wal.reader_for_segment(closed_segment_details.id()).unwrap();
        assert!(reader.next_batch().unwrap().is_none());

        // Can delete an empty segment, leaving no closed segments again
        wal.delete(closed_segment_details.id()).await.unwrap();
        let closed = wal.closed_segments();
        assert!(
            closed.is_empty(),
            "Expected empty closed segments; got {closed:?}"
        );
    }

    fn test_data(lp: &str) -> DatabaseBatch {
        let batches = lines_to_batches(lp, 0).unwrap();
        let batches = batches
            .into_iter()
            .enumerate()
            .map(|(i, (_table_name, batch))| (TableId::new(i as _), batch))
            .collect();

        let write = DmlWrite::new(
            NamespaceId::new(42),
            batches,
            "bananas".into(),
            Default::default(),
        );

        mutable_batch_pb::encode::encode_write(42, &write)
    }

    fn test_delete() -> DeletePayload {
        DeletePayload {
            database_id: 42,
            predicate: None,
            table_name: "bananas".into(),
        }
    }

    fn test_persist() -> PersistOp {
        PersistOp {
            namespace_id: 42,
            parquet_file_uuid: "b4N4N4Z".into(),
            partition_id: 43,
            table_id: 44,
        }
    }
}
