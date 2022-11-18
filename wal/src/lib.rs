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

use async_trait::async_trait;
use crc32fast::Hasher;
use generated_types::influxdata::{iox::delete::v1::DeletePayload, pbdata::v1::DatabaseBatch};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt, Snafu};
use std::{
    convert::TryFrom,
    io, mem, num,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    SegmentCreate {
        source: io::Error,
    },

    SegmentWrite {
        source: io::Error,
    },

    SegmentFileIdentifierMismatch {},

    UnableToReadFileMetadata {
        source: io::Error,
    },

    UnableToReadSequenceNumber {
        source: io::Error,
    },

    UnableToReadChecksum {
        source: io::Error,
    },

    UnableToReadLength {
        source: io::Error,
    },

    UnableToReadData {
        source: io::Error,
    },

    LengthMismatch {
        expected: usize,
        actual: usize,
    },

    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },

    ChunkSizeTooLarge {
        source: num::TryFromIntError,
        actual: usize,
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

    UnableToCompressData {
        source: snap::Error,
    },

    UnableToDecompressData {
        source: snap::Error,
    },

    UnableToSync {
        source: io::Error,
    },

    UnableToOpenFile {
        source: io::Error,
        path: PathBuf,
    },

    UnableToCreateFile {
        source: io::Error,
        path: PathBuf,
    },

    UnableToCopyFileContents {
        source: io::Error,
        src: PathBuf,
        dst: PathBuf,
    },

    UnableToReadDirectoryContents {
        source: io::Error,
        path: PathBuf,
    },

    UnableToReadSegmentId {
        source: io::Error,
    },

    UnableToReadCreated {
        source: io::Error,
    },
}

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

// todo: change to newtypes
/// SequenceNumber is a u64 monotonically-increasing number provided by users of the WAL for
/// their tracking purposes of data getting written into a segment.
// who enforces monotonicity? what happens if WAL receives a lower sequence number?
pub type SequenceNumber = u64;

/// Segments are identified by a type 4 UUID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

/// The first bytes written into a segment file to identify it and its version.
const FILE_TYPE_IDENTIFIER: &[u8] = b"INFLUXV3";
/// File extension for segment files.
const SEGMENT_FILE_EXTENSION: &str = "dat";

#[derive(Debug)]
pub struct Wal {
    root: PathBuf,
    closed_segments: Vec<ClosedSegment>,
    open_segment: Arc<SegmentFile>,
}

impl Wal {
    pub async fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        tokio::fs::create_dir_all(root)
            .await
            .context(UnableToCreateWalDirSnafu { path: root })?;

        let mut dir = tokio::fs::read_dir(root)
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: root })?;

        let mut closed_segments = Vec::new();

        while let Some(child) = dir
            .next_entry()
            .await
            .context(UnableToReadDirectoryContentsSnafu { path: root })?
        {
            let metadata = child
                .metadata()
                .await
                .context(UnableToReadFileMetadataSnafu)?;
            if metadata.is_file() {
                let segment = ClosedSegment {
                    path: child.path(),
                    size: metadata.len(),
                    created_at: metadata.created().context(UnableToReadFileMetadataSnafu)?,
                };
                closed_segments.push(segment);
            }
        }

        let open_segment = Arc::new(SegmentFile::new_writer(root).await?);

        Ok(Self {
            root: root.to_owned(),
            closed_segments,
            open_segment,
        })
    }
}

/// Methods for working with segments (a WAL)
#[async_trait]
pub trait SegmentWal {
    /// Closes the currently open segment and opens a new one, returning the closed segment details
    /// and a handle to the newly opened segment
    async fn rotate(&self) -> Result<(ClosedSegment, Arc<dyn Segment>)>;

    /// Gets a list of the closed segments
    fn closed_segments(&self) -> &[ClosedSegment];

    /// Opens a reader for a given segment from the WAL
    async fn reader_for_segment(&self, id: SegmentId) -> Result<Box<dyn OpReader>>;

    /// Returns a handle to the open segment
    async fn open_segment(&self) -> Arc<dyn Segment>;

    /// Deletes the segment from storage
    async fn delete_segment(&self, id: SegmentId) -> Result<()>;
}

#[async_trait]
impl SegmentWal for Wal {
    async fn rotate(&self) -> Result<(ClosedSegment, Arc<dyn Segment>)> {
        todo!();
    }

    fn closed_segments(&self) -> &[ClosedSegment] {
        &self.closed_segments
    }

    async fn reader_for_segment(&self, _id: SegmentId) -> Result<Box<dyn OpReader>> {
        todo!()
    }

    async fn open_segment(&self) -> Arc<dyn Segment> {
        Arc::clone(&self.open_segment) as _
    }

    async fn delete_segment(&self, _id: SegmentId) -> Result<()> {
        todo!();
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
    pub sequence_number: SequenceNumber,
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

/// Methods for reading ops from a segment in a WAL
#[async_trait]
pub trait OpReader {
    // get the next collection of ops. Since ops are batched into Segments, they come
    // back as a collection. Each `SegmentEntry` will encode a `Vec<WalOps>`.
    // todo: change to Result<Vec<WalOp>>, or a stream of `Vec<WalOps>`?
    async fn next(&mut self) -> Result<Option<Vec<WalOp>>>;
}

/// Methods for a `Segment`
#[async_trait]
pub trait Segment {
    /// Get the id of the segment
    fn id(&self) -> SegmentId;

    /// Persist an operation into the segment. The `Segment` trait implementer is meant to be an
    /// accumulator that will batch ops together, and `write_op` calls will return when the
    /// collection has been persisted to the segment file.
    async fn write_op(&self, op: &SequencedWalOp) -> Result<WriteSummary>;

    /// Return a reader for the ops in the segment
    async fn reader(&self) -> Result<Box<dyn OpReader>>;
}

/// Data for a Segment entry
#[derive(Debug, Eq, PartialEq)]
pub struct SegmentEntry {
    /// The CRC checksum of the uncompressed data
    pub checksum: u32,
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
    /// Checksum for the compressed data written to segment
    pub checksum: u32,
}

/// A Segment in a WAL. One segment is stored in one file.
#[derive(Debug)]
struct SegmentFile {
    id: SegmentId,
    path: PathBuf,
    state: RwLock<SegmentFileInner>, // todo: is this lock needed?
}

#[derive(Debug)]
struct SegmentFileInner {
    f: tokio::fs::File,
    bytes_written: usize,
}

impl SegmentFile {
    async fn new_writer(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path = path.into();
        let id = SegmentId::new();
        path.push(id.to_string());
        path.set_extension(SEGMENT_FILE_EXTENSION);

        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .await
            .context(SegmentCreateSnafu)?;
        f.write_all(FILE_TYPE_IDENTIFIER)
            .await
            .context(SegmentWriteSnafu)?;
        let bytes_written = FILE_TYPE_IDENTIFIER.len();
        let id_bytes = id.as_bytes();
        f.write_all(id_bytes).await.context(SegmentWriteSnafu)?;
        let id_bytes_written = id_bytes.len();

        f.sync_all().await.context(SegmentWriteSnafu)?;

        Ok(Self {
            id,
            path,
            state: RwLock::new(SegmentFileInner {
                f,
                bytes_written: bytes_written + id_bytes_written,
            }),
        })
    }

    async fn write(&self, data: &[u8]) -> Result<WriteSummary> {
        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = data.len();
        u32::try_from(uncompressed_len).context(ChunkSizeTooLargeSnafu {
            actual: uncompressed_len,
        })?;

        let mut encoder = snap::raw::Encoder::new();
        let compressed_data = encoder
            .compress_vec(data)
            .context(UnableToCompressDataSnafu)?;
        let actual_compressed_len = compressed_data.len();
        let actual_compressed_len =
            u32::try_from(actual_compressed_len).context(ChunkSizeTooLargeSnafu {
                actual: actual_compressed_len,
            })?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data);
        let checksum = hasher.finalize();

        let mut state = self.state.write().await;

        state
            .f
            .write_u32(checksum)
            .await
            .context(SegmentWriteSnafu)?;
        state
            .f
            .write_u32(actual_compressed_len)
            .await
            .context(SegmentWriteSnafu)?;
        state
            .f
            .write_all(&compressed_data)
            .await
            .context(SegmentWriteSnafu)?;

        let bytes_written = mem::size_of::<u32>() + mem::size_of::<u32>() + compressed_data.len();
        state.bytes_written += bytes_written;

        Ok(WriteSummary {
            checksum,
            total_bytes: state.bytes_written,
            bytes_written,
        })
    }

    async fn write_ops(&self, ops: &[SequencedWalOp]) -> Result<WriteSummary> {
        // todo: bincode instead of serde_json
        let encoded = serde_json::to_vec(&ops).unwrap();
        self.write(&encoded).await
    }

    // async fn write_op(&self, op: &SequencedWalOp) -> Result<WriteSummary> {
    //
    // }

    async fn close(self) -> Result<ClosedSegment> {
        let state = self.state.write().await;
        let metadata = state
            .f
            .metadata()
            .await
            .context(UnableToReadFileMetadataSnafu)?;
        Ok(ClosedSegment {
            path: self.path,
            size: metadata.len(),
            created_at: metadata.created().context(UnableToReadCreatedSnafu)?,
        })
    }
}

#[async_trait]
impl Segment for SegmentFile {
    fn id(&self) -> SegmentId {
        self.id
    }

    async fn write_op(&self, _op: &SequencedWalOp) -> Result<WriteSummary> {
        // create a oneshot channel
        // call inner write op with the op and the transmitter of the channel
        // await the receiving end of the channel
        todo!()
    }

    async fn reader(&self) -> Result<Box<dyn OpReader>> {
        todo!()
    }
}

struct SegmentFileReader {
    // todo: pin necessary?
    f: Pin<Box<dyn AsyncRead>>,
    id: SegmentId,
}

impl SegmentFileReader {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let f = tokio::fs::File::open(path)
            .await
            .context(UnableToOpenFileSnafu { path })?;

        let mut f: Pin<Box<dyn AsyncRead>> = Box::pin(f);

        is_segment_stream(&mut f).await?;
        let id = read_id(&mut f).await?;

        Ok(Self { f, id })
    }

    pub async fn next(&mut self) -> Result<Option<SegmentEntry>> {
        read_entry(&mut self.f).await
    }

    async fn entries(&mut self) -> Result<Vec<SegmentEntry>> {
        let mut entries = vec![];
        while let Some(entry) = self.next().await? {
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn next_ops(&mut self) -> Result<Option<Vec<SequencedWalOp>>> {
        if let Some(entry) = read_entry(&mut self.f).await? {
            let decoded: Vec<_> = serde_json::from_slice(&entry.data).unwrap();

            return Ok(Some(decoded));
        }

        Ok(None)
    }
}

async fn is_segment_stream(f: &mut Pin<Box<dyn AsyncRead>>) -> Result<()> {
    let mut header = vec![0u8; FILE_TYPE_IDENTIFIER.len()];
    f.read_exact(&mut header)
        .await
        .context(UnableToReadFileMetadataSnafu)?;

    ensure!(
        header == FILE_TYPE_IDENTIFIER,
        SegmentFileIdentifierMismatchSnafu {}
    );

    Ok(())
}

const UUID_BYTES_LEN: usize = 16;

async fn read_id(f: &mut Pin<Box<dyn AsyncRead>>) -> Result<SegmentId> {
    let mut id_header = vec![0u8; UUID_BYTES_LEN];
    f.read_exact(&mut id_header)
        .await
        .context(UnableToReadSegmentIdSnafu)?;

    let uuid = Uuid::from_slice(&id_header).expect("uuid bytes length should always be 16");
    Ok(SegmentId::from(uuid))
}

async fn read_entry(f: &mut Pin<Box<dyn AsyncRead>>) -> Result<Option<SegmentEntry>> {
    let checksum = match f.read_u32().await {
        Ok(sn) => sn,
        Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::UnableToReadChecksum { source: e }),
    };

    let expected_len = f.read_u32().await.context(UnableToReadLengthSnafu)?;
    let expected_len_us =
        usize::try_from(expected_len).expect("Only designed to run on 32-bit systems or higher");
    let mut compressed_data = Vec::with_capacity(expected_len_us);
    let actual_compressed_len = f
        .take(u64::from(expected_len))
        .read_to_end(&mut compressed_data)
        .await
        .context(UnableToReadDataSnafu)?;

    ensure!(
        expected_len_us == actual_compressed_len,
        LengthMismatchSnafu {
            expected: expected_len_us,
            actual: actual_compressed_len
        }
    );

    let mut hasher = Hasher::new();
    hasher.update(&compressed_data);
    let actual_checksum = hasher.finalize();

    ensure!(
        checksum == actual_checksum,
        ChecksumMismatchSnafu {
            expected: checksum,
            actual: actual_checksum
        }
    );

    let mut decoder = snap::raw::Decoder::new();
    let data = decoder
        .decompress_vec(&compressed_data)
        .context(UnableToDecompressDataSnafu)?;

    Ok(Some(SegmentEntry { checksum, data }))
}

#[derive(Debug, Clone)]
pub struct ClosedSegment {
    path: PathBuf,
    size: u64,
    created_at: SystemTime,
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
        let sf = SegmentFile::new_writer(dir.path()).await.unwrap();

        let data = b"whatevs";
        let write_summary = sf.write(data).await.unwrap();

        let mut reader = SegmentFileReader::new(&sf.path).await.unwrap();
        let entries = reader.entries().await.unwrap();
        assert_eq!(
            &entries,
            &[SegmentEntry {
                checksum: write_summary.checksum,
                data: data.to_vec(),
            }]
        );

        let data2 = b"another";
        let summary2 = sf.write(data2).await.unwrap();

        let mut reader = SegmentFileReader::new(&sf.path).await.unwrap();
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
        let segment = SegmentFile::new_writer(dir.path()).await.unwrap();

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

        let ops = vec![op1, op2];
        segment.write_ops(&ops).await.unwrap();

        let mut reader = SegmentFileReader::new(&segment.path).await.unwrap();
        let read_ops = reader.next_ops().await.unwrap().unwrap();
        assert_eq!(ops, read_ops);
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
        let ids = batches
            .keys()
            .enumerate()
            .map(|(i, name)| (name.clone(), TableId::new(i as _)))
            .collect();

        let write = DmlWrite::new(
            "test_db",
            NamespaceId::new(42),
            batches,
            ids,
            "bananas".into(),
            Default::default(),
        );

        mutable_batch_pb::encode::encode_write("db", 42, &write)
    }
}
