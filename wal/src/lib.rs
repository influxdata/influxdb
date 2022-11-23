#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
// TEMP until everything is fleshed out
#![allow(dead_code)]

//! # WAL
//!
//! This crate provides a local-disk WAL for the IOx ingestion pipeline.

use generated_types::influxdata::{iox::delete::v1::DeletePayload, pbdata::v1::DatabaseBatch};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::{collections::HashMap, io, path::PathBuf};
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

mod blocking;

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

    InvalidUuid {
        filename: String,
        source: uuid::Error,
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
}

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `SequenceNumber` is metadata provided by users of the WAL for their tracking purposes of data
/// getting written into a segment. No properties of `SequenceNumber` are verified in the WAL.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
#[serde(transparent)]
pub struct SequenceNumberNg(u64);

#[allow(missing_docs)]
impl SequenceNumberNg {
    pub fn new(v: u64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> u64 {
        self.0
    }
}

/// Segments are identified by a type 4 UUID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(Uuid);

#[allow(missing_docs)]
impl SegmentId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn get(&self) -> Uuid {
        self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<Uuid> for SegmentId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for SegmentId {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: find a better name
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
#[derive(Debug)]
pub struct Wal {
    root: PathBuf,
    closed_segments: RwLock<HashMap<SegmentId, ClosedSegment>>,
    open_segment: OpenSegmentFile,
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
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(&root)
            .await
            .context(UnableToCreateWalDirSnafu { path: &root })?;

        let mut dir = tokio::fs::read_dir(&root)
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: &root })?;

        let mut closed_segments = HashMap::new();

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
                let filename = child_path.file_name().unwrap();
                let filename = filename.to_str().unwrap();
                let id = Uuid::parse_str(filename)
                    .context(InvalidUuidSnafu { filename })?
                    .into();
                let segment = ClosedSegment {
                    id,
                    path: child.path(),
                    size: metadata.len(),
                };
                closed_segments.insert(id, segment);
            }
        }

        let open_segment = OpenSegmentFile::new_in_directory(&root).await?;

        Ok(Self {
            root,
            closed_segments: RwLock::new(closed_segments),
            open_segment,
        })
    }

    /// Returns a handle to the WAL that enables commiting entries to the currently active segment.
    pub async fn write_handle(&self) -> WalWriter {
        self.open_segment.write_handle()
    }

    /// Returns a handle to the WAL that enables listing and reading entries from closed segments.
    pub fn read_handle(&self) -> WalReader<'_> {
        WalReader(self)
    }

    /// Returns a handle to the WAL that enables rotating the open segment and deleting closed
    /// segments.
    pub async fn rotation_handle(&self) -> WalRotator<'_> {
        WalRotator(self)
    }
}

/// Handle to the one currently open segment for users of the WAL to send [`SequencedWalOp`]s to.
#[derive(Debug)]
pub struct WalWriter(mpsc::Sender<OpenSegmentFileWriterRequest>);

impl WalWriter {
    async fn write(&self, data: &[u8]) -> Result<WriteSummary> {
        OpenSegmentFile::one_command(&self.0, OpenSegmentFileWriterRequest::Write, data.to_vec())
            .await
    }

    /// Writes one [`SequencedWalOp`] to disk and returns when it is durable.
    pub async fn write_op(&self, op: &SequencedWalOp) -> Result<WriteSummary> {
        // todo: bincode instead of serde_json
        let encoded = serde_json::to_vec(op).unwrap();
        self.write(&encoded).await
    }
}

/// Handle to list the closed segments and read [`SequencedWalOp`]s from them for replay.
#[derive(Debug)]
pub struct WalReader<'a>(&'a Wal);

impl<'a> WalReader<'a> {
    /// Gets a list of the closed segments
    pub async fn closed_segments(&self) -> Vec<ClosedSegment> {
        self.0
            .closed_segments
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    /// Opens a reader for a given segment from the WAL
    pub async fn reader_for_segment(&self, id: SegmentId) -> Result<ClosedSegmentFileReader> {
        let path = build_segment_path(&self.0.root, id);
        ClosedSegmentFileReader::from_path(path).await
    }
}

/// Handle to rotate open segments to closed and delete closed segments.
#[derive(Debug)]
pub struct WalRotator<'a>(&'a Wal);

impl<'a> WalRotator<'a> {
    /// Closes the currently open segment and opens a new one, returning the closed segment details.
    pub async fn rotate(&self) -> Result<ClosedSegment> {
        let closed = OpenSegmentFile::one_command(
            &self.0.open_segment.tx.clone(),
            OpenSegmentFileWriterRequest::Rotate,
            (),
        )
        .await?;
        self.0
            .closed_segments
            .write()
            .await
            .insert(closed.id, closed.clone());
        Ok(closed)
    }

    /// Deletes the specified segment from disk.
    pub async fn delete(&self, id: SegmentId) -> Result<()> {
        let closed = self
            .0
            .closed_segments
            .write()
            .await
            .remove(&id)
            .context(SegmentNotFoundSnafu { id })?;
        std::fs::remove_file(&closed.path).context(DeleteClosedSegmentSnafu { path: closed.path })
    }
}

/// Operation recorded in the WAL
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum WalOp {
    Write(DatabaseBatch),
    Delete(DeletePayload),
    Persist(PersistOp),
}

/// WAL operation with a sequence number, which is used to inform read buffers when to evict data
/// from the buffer
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SequencedWalOp {
    pub sequence_number: SequenceNumberNg,
    pub op: WalOp,
}

/// Serializable and deserializable persist information that can be saved to the WAL. This is
/// used during replay to evict data from memory.
#[derive(Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct PersistOp {
    // todo: use data_types for these
    namespace_id: i64,
    table_id: i64,
    partition_id: i64,
    parquet_file_uuid: String,
}

/// Raw, uncompressed and unstructured data for a Segment entry with a checksum.
#[derive(Debug, Eq, PartialEq)]
pub struct SegmentEntry {
    /// The CRC checksum of the uncompressed data
    checksum: u32,
    /// The uncompressed data
    pub data: Vec<u8>,
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
    /// Checksum for the compressed data written to segment
    checksum: u32,
}

#[derive(Debug)]
enum OpenSegmentFileWriterRequest {
    Write(oneshot::Sender<WriteSummary>, Vec<u8>), // todo Bytes
    Rotate(oneshot::Sender<ClosedSegment>, ()),
}

/// An open segment in a WAL.
#[derive(Debug)]
struct OpenSegmentFile {
    tx: mpsc::Sender<OpenSegmentFileWriterRequest>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl OpenSegmentFile {
    async fn new_in_directory(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        let dir_for_closure = dir.clone();
        let (tx, rx) = mpsc::channel(10);
        let task = tokio::task::spawn_blocking(move || Self::task_main(rx, dir_for_closure));
        std::fs::File::open(&dir)
            .context(OpenSegmentDirectorySnafu { path: dir })?
            .sync_all()
            .expect("fsync failure");
        Ok(Self { tx, task })
    }

    fn task_main(
        mut rx: tokio::sync::mpsc::Receiver<OpenSegmentFileWriterRequest>,
        dir: PathBuf,
    ) -> Result<()> {
        let new_writ = || Ok(blocking::OpenSegmentFileWriter::new_in_directory(&dir).unwrap());
        let mut open_write = new_writ()?;

        while let Some(req) = rx.blocking_recv() {
            use OpenSegmentFileWriterRequest::*;

            match req {
                Write(tx, data) => {
                    let x = open_write.write(&data).unwrap();
                    tx.send(x).unwrap();
                }

                Rotate(tx, ()) => {
                    let old = std::mem::replace(&mut open_write, new_writ()?);
                    let res = old.close().unwrap();
                    tx.send(res).unwrap();
                }
            }
        }

        Ok(())
    }

    async fn one_command<Req, Resp, Args>(
        tx: &mpsc::Sender<OpenSegmentFileWriterRequest>,
        req: Req,
        args: Args,
    ) -> Result<Resp>
    where
        Req: FnOnce(oneshot::Sender<Resp>, Args) -> OpenSegmentFileWriterRequest,
    {
        let (req_tx, req_rx) = oneshot::channel();

        tx.send(req(req_tx, args)).await.unwrap();
        Ok(req_rx.await.unwrap())
    }

    fn write_handle(&self) -> WalWriter {
        WalWriter(self.tx.clone())
    }

    async fn rotate(&self) -> Result<ClosedSegment> {
        Self::one_command(&self.tx, OpenSegmentFileWriterRequest::Rotate, ()).await
    }
}

#[derive(Debug)]
enum ClosedSegmentFileReaderRequest {
    ReadHeader(oneshot::Sender<blocking::ReaderResult<(FileTypeIdentifier, uuid::Bytes)>>),

    Entries(oneshot::Sender<blocking::ReaderResult<Vec<SegmentEntry>>>),

    NextOps(oneshot::Sender<blocking::ReaderResult<Option<SequencedWalOp>>>),
}

/// Enables reading a particular closed segment's entries.
#[derive(Debug)]
pub struct ClosedSegmentFileReader {
    id: SegmentId,
    tx: mpsc::Sender<ClosedSegmentFileReaderRequest>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl ClosedSegmentFileReader {
    async fn from_path(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        let (tx, rx) = mpsc::channel::<ClosedSegmentFileReaderRequest>(10);
        let task = tokio::task::spawn_blocking(|| Self::task_main(rx, path));

        let (file_type, id) = Self::one_command(&tx, ClosedSegmentFileReaderRequest::ReadHeader)
            .await?
            .context(UnableToReadFileHeaderSnafu)?;

        ensure!(
            &file_type == FILE_TYPE_IDENTIFIER,
            SegmentFileIdentifierMismatchSnafu,
        );

        let id = Uuid::from_bytes(id);
        let id = SegmentId::from(id);

        Ok(Self { id, tx, task })
    }

    fn task_main(
        mut rx: mpsc::Receiver<ClosedSegmentFileReaderRequest>,
        path: PathBuf,
    ) -> Result<()> {
        let mut reader = blocking::ClosedSegmentFileReader::from_path(&path)
            .context(UnableToOpenFileSnafu { path })?;

        while let Some(req) = rx.blocking_recv() {
            use ClosedSegmentFileReaderRequest::*;

            // We don't care if we can't respond to the request.
            match req {
                ReadHeader(tx) => {
                    tx.send(reader.read_header()).ok();
                }

                Entries(tx) => {
                    tx.send(reader.entries()).ok();
                }

                NextOps(tx) => {
                    tx.send(reader.next_ops()).ok();
                }
            };
        }

        Ok(())
    }

    async fn one_command<Req, Resp>(
        tx: &mpsc::Sender<ClosedSegmentFileReaderRequest>,
        req: Req,
    ) -> Result<Resp>
    where
        Req: FnOnce(oneshot::Sender<Resp>) -> ClosedSegmentFileReaderRequest,
    {
        let (req_tx, req_rx) = oneshot::channel();
        tx.send(req(req_tx))
            .await
            .ok()
            .context(UnableToSendRequestToReaderTaskSnafu)?;
        req_rx
            .await
            .context(UnableToReceiveResponseFromSenderTaskSnafu)
    }

    // TODO: Should this return a stream instead of a big vector?
    async fn entries(&mut self) -> Result<Vec<SegmentEntry>> {
        Self::one_command(&self.tx, ClosedSegmentFileReaderRequest::Entries)
            .await?
            .context(UnableToReadEntriesSnafu)
    }

    /// Return the next [`SequencedWalOp`] from this reader, if any.
    pub async fn next_ops(&mut self) -> Result<Option<SequencedWalOp>> {
        Self::one_command(&self.tx, ClosedSegmentFileReaderRequest::NextOps)
            .await?
            .context(UnableToReadNextOpsSnafu)
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
    use mutable_batch_lp::lines_to_batches;

    #[tokio::test]
    async fn segment_file_write_and_read_entries() {
        let dir = test_helpers::tmp_dir().unwrap();
        let sf = OpenSegmentFile::new_in_directory(dir.path()).await.unwrap();
        let writer = sf.write_handle();

        let data = b"whatevs";
        let write_summary = writer.write(data).await.unwrap();

        let data2 = b"another";
        let summary2 = writer.write(data2).await.unwrap();

        let closed = sf.rotate().await.unwrap();

        let mut reader = ClosedSegmentFileReader::from_path(&closed.path)
            .await
            .unwrap();
        let entries = reader.entries().await.unwrap();
        assert_eq!(
            &entries,
            &[
                SegmentEntry {
                    checksum: write_summary.checksum,
                    data: data.to_vec(),
                },
                SegmentEntry {
                    checksum: summary2.checksum,
                    data: data2.to_vec()
                },
            ]
        );
    }

    #[tokio::test]
    async fn segment_file_write_and_read_ops() {
        let dir = test_helpers::tmp_dir().unwrap();
        let segment = OpenSegmentFile::new_in_directory(dir.path()).await.unwrap();
        let writer = segment.write_handle();

        let w1 = test_data("m1,t=foo v=1i 1");
        let w2 = test_data("m1,t=foo v=2i 2");

        let op1 = SequencedWalOp {
            sequence_number: SequenceNumberNg::new(0),
            op: WalOp::Write(w1),
        };
        let op2 = SequencedWalOp {
            sequence_number: SequenceNumberNg::new(1),
            op: WalOp::Write(w2),
        };

        writer.write_op(&op1).await.unwrap();
        writer.write_op(&op2).await.unwrap();

        let closed = segment.rotate().await.unwrap();

        let mut reader = ClosedSegmentFileReader::from_path(&closed.path)
            .await
            .unwrap();
        let read_op1 = reader.next_ops().await.unwrap().unwrap();
        assert_eq!(op1, read_op1);

        let read_op2 = reader.next_ops().await.unwrap().unwrap();
        assert_eq!(op2, read_op2);
    }

    // test delete and persist ops

    // open wal and write and read ops from segment

    // rotates wal to new segment

    // open wal and get closed segments and read them

    // delete segment

    // open wal with files that aren't segments (should log and skip)

    // read segment works even if last entry is truncated

    // writes get batched

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
}
