//! This is the implementation of the `Wal` that the buffer uses to make buffered data durable
//! on disk.

use crate::{
    SegmentFile, SegmentId, SegmentIdBytes, SequenceNumber, Wal, WalOp, WalOpBatch,
    WalSegmentReader, WalSegmentWriter,
};
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use datafusion::parquet::file::reader::Length;
use observability_deps::tracing::{info, warn};
use snap::read::FrameDecoder;
use std::fmt::Debug;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, Cursor, Read, Write},
    mem,
    path::PathBuf,
};
use thiserror::Error;

/// The first bytes written into a segment file to identify it and its version.
type FileTypeIdentifier = [u8; 8];
const FILE_TYPE_IDENTIFIER: &[u8] = b"idb3.001";

/// File extension for segment files
const SEGMENT_FILE_EXTENSION: &str = "wal";

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {source}")]
    Io {
        #[from]
        source: io::Error,
    },

    #[error("error converting wal batch to json: {source}")]
    Json {
        #[from]
        source: serde_json::Error,
    },

    #[error("converting u64 to u32: {source}")]
    TryFromU64 {
        #[from]
        source: std::num::TryFromIntError,
    },
    #[error("invalid segment file {segment_id:?} at {path:?}: {reason}")]
    InvalidSegmentFile {
        segment_id: SegmentId,
        path: PathBuf,
        reason: String,
    },

    #[error("unable to read crc for segment {segment_id:?}")]
    UnableToReadCrc { segment_id: SegmentId },

    #[error("unable to read length for segment data {segment_id:?}")]
    UnableToReadLength { segment_id: SegmentId },

    #[error("length mismatch for segment {segment_id:?}: expected {expected}, got {actual}")]
    LengthMismatch {
        segment_id: SegmentId,
        expected: u32,
        actual: u32,
    },

    #[error("checksum mismatch for segment {segment_id:?}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        segment_id: SegmentId,
        expected: u32,
        actual: u32,
    },

    #[error("invalid segment file name {0:?}")]
    InvalidSegmentFileName(String),

    #[error("unable to parse segment id from file: {source}")]
    UnableToParseSegmentId {
        file_name: String,
        source: std::num::ParseIntError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WalImpl {
    root: PathBuf,
}

impl WalImpl {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let root = path.into();
        info!(wal_dir=?root, "Ensuring WAL directory exists");
        std::fs::create_dir_all(&root)?;

        // ensure the directory creation is actually fsync'd so that when we create files there
        // we don't lose them (see: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf)
        File::open(&root)
            .expect("should be able to open just-created directory")
            .sync_all()
            .expect("fsync failure");

        Ok(Self { root })
    }

    fn open_segment_writer(&self, segment_id: SegmentId) -> Result<impl WalSegmentWriter> {
        let writer = WalSegmentWriterImpl::new_or_open(self.root.clone(), segment_id)?;
        Ok(writer)
    }

    fn open_segment_reader(&self, segment_id: SegmentId) -> Result<impl WalSegmentReader> {
        let reader = WalSegmentReaderImpl::new(self.root.clone(), segment_id)?;
        Ok(reader)
    }

    fn segment_files(&self) -> Result<Vec<SegmentFile>> {
        let dir = std::fs::read_dir(&self.root)?;

        let mut segment_files = Vec::new();

        for child in dir.into_iter() {
            let child = child?;
            let meta = child.metadata()?;
            if meta.is_file() {
                let path = child.path();
                let file_name = path
                    .file_stem()
                    .expect("WAL segment files created by InfluxDB3 should have a file stem");
                let file_name = file_name.to_str().expect("WAL segment files created by InfluxDB3 should have a file stem that is valid UTF-8");

                if let Ok(segment_id) = segment_id_from_file_name(file_name) {
                    segment_files.push(SegmentFile { segment_id, path });
                } else {
                    warn!(
                        file_name,
                        "WAL segment file has an invalid file stem, ignoring"
                    );
                }
            }
        }

        segment_files.sort_by_key(|f| f.segment_id);

        Ok(segment_files)
    }

    fn delete_wal_segment(&self, segment_id: SegmentId) -> Result<()> {
        let path = build_segment_path(self.root.clone(), segment_id);
        std::fs::remove_file(path)?;
        Ok(())
    }
}

impl Wal for WalImpl {
    fn open_segment_writer(&self, segment_id: SegmentId) -> Result<impl WalSegmentWriter> {
        self.open_segment_writer(segment_id)
    }

    fn open_segment_reader(&self, segment_id: SegmentId) -> Result<impl WalSegmentReader> {
        self.open_segment_reader(segment_id)
    }

    fn segment_files(&self) -> Result<Vec<SegmentFile>> {
        self.segment_files()
    }

    fn delete_wal_segment(&self, _segment_id: SegmentId) -> Result<()> {
        self.delete_wal_segment(_segment_id)
    }
}

#[derive(Debug)]
pub struct WalSegmentWriterImpl {
    segment_id: SegmentId,
    f: File,
    bytes_written: usize,
    sequence_number: SequenceNumber,

    buffer: Vec<u8>,
}

impl WalSegmentWriterImpl {
    pub fn new_or_open(root: PathBuf, segment_id: SegmentId) -> Result<Self> {
        let path = build_segment_path(root, segment_id);

        // if there's already a file there, validate its header and pull the sequence number from the last entry
        if path.exists() {
            if let Some(file_info) =
                WalSegmentReaderImpl::read_segment_file_info_if_exists(path.clone(), segment_id)?
            {
                let f = OpenOptions::new().write(true).append(true).open(&path)?;

                return Ok(Self {
                    segment_id,
                    f,
                    bytes_written: file_info
                        .bytes_written
                        .try_into()
                        .expect("file length must fit in usize"),
                    sequence_number: file_info.last_sequence_number,
                    buffer: Vec::with_capacity(8 * 1204), // 8kiB initial size
                });
            } else {
                return Err(Error::InvalidSegmentFile {
                    segment_id,
                    path,
                    reason: "file exists but is invalid".to_string(),
                });
            }
        }

        // it's a new file, initialize it with the header and get ready to start writing
        let mut f = OpenOptions::new().write(true).create(true).open(&path)?;

        f.write_all(FILE_TYPE_IDENTIFIER)?;
        let file_type_bytes_written = FILE_TYPE_IDENTIFIER.len();

        let id_bytes = segment_id.as_bytes();
        f.write_all(&id_bytes)?;
        let id_bytes_written = id_bytes.len();

        f.sync_all().expect("fsync failure");

        let bytes_written = file_type_bytes_written + id_bytes_written;

        Ok(Self {
            segment_id,
            f,
            bytes_written,
            sequence_number: SequenceNumber::new(0),
            buffer: Vec::with_capacity(8 * 1204), // 8kiB initial size
        })
    }

    fn write_batch(&mut self, ops: Vec<WalOp>) -> Result<SequenceNumber> {
        // Ensure the write buffer is always empty before using it.
        self.buffer.clear();

        self.sequence_number = self.sequence_number.next();

        let batch = WalOpBatch {
            sequence_number: self.sequence_number,
            ops,
        };

        let data = serde_json::to_vec(&batch)?;

        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = data.len();
        u32::try_from(uncompressed_len)?;

        // The chunk header is two u32 values, so write a dummy u64 value and
        // come back to fill them in later.
        self.buffer
            .write_u64::<BigEndian>(0)
            .expect("cannot fail to write to buffer");

        // Compress the payload into the reused buffer, recording the crc hash
        // as it is wrote.
        let mut encoder = snap::write::FrameEncoder::new(HasherWrapper::new(&mut self.buffer));
        encoder.write_all(&data)?;
        let (checksum, buf) = encoder
            .into_inner()
            .expect("cannot fail to flush to a Vec")
            .finalize();

        // Adjust the compressed length to take into account the u64 padding
        // above.
        let compressed_len = buf.len() - mem::size_of::<u64>();
        let compressed_len = u32::try_from(compressed_len)?;

        // Go back and write the chunk header values
        let mut buf = Cursor::new(buf);
        buf.set_position(0);

        buf.write_u32::<BigEndian>(checksum)?;
        buf.write_u32::<BigEndian>(compressed_len)?;

        // Write the entire buffer to the file
        let buf = buf.into_inner();
        let bytes_written = buf.len();
        self.f.write_all(buf)?;

        // fsync the fd
        self.f.sync_all().expect("fsync failure");

        self.bytes_written += bytes_written;

        Ok(self.sequence_number)
    }
}

#[async_trait]
impl WalSegmentWriter for WalSegmentWriterImpl {
    fn id(&self) -> SegmentId {
        self.segment_id
    }

    fn write_batch(&mut self, ops: Vec<WalOp>) -> Result<SequenceNumber> {
        self.write_batch(ops)
    }
}

#[derive(Debug)]
pub struct WalSegmentReaderImpl {
    f: BufReader<File>,
    segment_id: SegmentId,
}

impl WalSegmentReaderImpl {
    pub fn new(root: impl Into<PathBuf>, segment_id: SegmentId) -> Result<Self> {
        let path = build_segment_path(root, segment_id);
        let f = BufReader::new(File::open(path.clone())?);

        let mut reader = Self { f, segment_id };

        let (file_type, id) = reader.read_header()?;

        if file_type != FILE_TYPE_IDENTIFIER {
            return Err(Error::InvalidSegmentFile {
                segment_id,
                path,
                reason: format!(
                    "expected file type identifier {:?}, got {:?}",
                    FILE_TYPE_IDENTIFIER, file_type
                ),
            });
        }

        if id != segment_id.as_bytes() {
            return Err(Error::InvalidSegmentFile {
                segment_id,
                path,
                reason: format!(
                    "expected segment id {:?} in file, got {:?}",
                    segment_id.as_bytes(),
                    id
                ),
            });
        }

        Ok(reader)
    }

    fn read_segment_file_info_if_exists(
        path: PathBuf,
        segment_id: SegmentId,
    ) -> Result<Option<ExistingSegmentFileInfo>> {
        let f = match File::open(path.clone()) {
            Ok(f) => f,
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let bytes_written = f.len().try_into()?;

        let mut reader = Self {
            f: BufReader::new(f),
            segment_id,
        };

        let (file_type, _id) = reader.read_header()?;

        if file_type != FILE_TYPE_IDENTIFIER {
            return Err(Error::InvalidSegmentFile {
                segment_id,
                path,
                reason: format!(
                    "expected file type identifier {:?}, got {:?}",
                    FILE_TYPE_IDENTIFIER, file_type
                ),
            });
        }

        let mut last_block = None;

        while let Some(block) = reader.next_segment_block()? {
            last_block = Some(block);
        }

        if let Some(block) = last_block {
            let batch: WalOpBatch = serde_json::from_slice(&block)?;

            Ok(Some(ExistingSegmentFileInfo {
                last_sequence_number: batch.sequence_number,
                bytes_written,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn next_batch(&mut self) -> Result<Option<WalOpBatch>> {
        if let Some(data) = self.next_segment_block()? {
            let batch: WalOpBatch = serde_json::from_slice(&data)?;

            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn next_segment_block(&mut self) -> Result<Option<Vec<u8>>> {
        let expected_checksum = match self.f.read_u32::<BigEndian>() {
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            other => other?,
        };

        let expected_len: u32 = self.f.read_u32::<BigEndian>()?;

        let compressed_read = self.f.by_ref().take(expected_len.into());
        let hashing_read = CrcReader::new(compressed_read);
        let mut decompressing_read = FrameDecoder::new(hashing_read);

        let mut data = Vec::with_capacity(100);
        decompressing_read.read_to_end(&mut data)?;

        let (actual_compressed_len, actual_checksum) = decompressing_read.into_inner().checksum();
        let actual_compressed_len = u32::try_from(actual_compressed_len)
            .expect("segment blocks are only designed to support chunks up to u32::max bytes long");

        if expected_len != actual_compressed_len {
            return Err(Error::LengthMismatch {
                segment_id: self.segment_id,
                expected: expected_len,
                actual: actual_compressed_len,
            });
        }

        if expected_checksum != actual_checksum {
            return Err(Error::ChecksumMismatch {
                segment_id: self.segment_id,
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        Ok(Some(data))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut data = [0u8; N];
        self.f.read_exact(&mut data)?;
        Ok(data)
    }

    fn read_header(&mut self) -> Result<(FileTypeIdentifier, SegmentIdBytes)> {
        Ok((self.read_array()?, self.read_array()?))
    }
}

struct ExistingSegmentFileInfo {
    last_sequence_number: SequenceNumber,
    bytes_written: u32,
}

impl WalSegmentReader for WalSegmentReaderImpl {
    fn id(&self) -> SegmentId {
        self.segment_id
    }

    fn next_batch(&mut self) -> Result<Option<WalOpBatch>> {
        self.next_batch()
    }
}

struct CrcReader<R> {
    inner: R,
    hasher: Hasher,
    bytes_seen: u64,
}

impl<R> CrcReader<R> {
    fn new(inner: R) -> Self {
        let hasher = Hasher::default();
        Self {
            inner,
            hasher,
            bytes_seen: 0,
        }
    }

    fn checksum(self) -> (u64, u32) {
        (self.bytes_seen, self.hasher.finalize())
    }
}

impl<R> Read for CrcReader<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.inner.read(buf)?;
        let len_u64 = u64::try_from(len).expect("Only designed to run on 32-bit systems or higher");

        self.bytes_seen += len_u64;
        self.hasher.update(&buf[..len]);
        Ok(len)
    }
}

/// A [`HasherWrapper`] acts as a [`Write`] decorator, recording the crc
/// checksum of the data wrote to the inner [`Write`] implementation.
struct HasherWrapper<W> {
    inner: W,
    hasher: Hasher,
}

impl<W> HasherWrapper<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Hasher::default(),
        }
    }

    fn finalize(self) -> (u32, W) {
        (self.hasher.finalize(), self.inner)
    }
}

impl<W> Write for HasherWrapper<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn build_segment_path(dir: impl Into<PathBuf>, id: SegmentId) -> PathBuf {
    let mut path = dir.into();
    path.push(format!("{:010}", id.0));
    path.set_extension(SEGMENT_FILE_EXTENSION);
    path
}

fn segment_id_from_file_name(name: &str) -> Result<SegmentId> {
    let id = name
        .parse::<u32>()
        .map_err(|source| Error::UnableToParseSegmentId {
            file_name: name.to_string(),
            source,
        })?;
    Ok(SegmentId::new(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LpWriteOp;

    #[test]
    fn segment_writer_reader() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();

        let mut writer = WalSegmentWriterImpl::new_or_open(dir.clone(), SegmentId::new(0)).unwrap();
        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "foo".to_string(),
            lp: "cpu host=a val=10i 10".to_string(),
            default_time: 1,
        });
        writer.write_batch(vec![wal_op.clone()]).unwrap();

        let mut reader = WalSegmentReaderImpl::new(dir, SegmentId::new(0)).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();

        assert_eq!(batch.ops, vec![wal_op]);
        assert_eq!(batch.sequence_number, SequenceNumber::new(1));
    }

    #[test]
    fn segment_writer_can_open_previously_existing_segment() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "foo".to_string(),
            lp: "cpu host=a val=10i 10".to_string(),
            default_time: 1,
        });

        // open the file, write and close it
        {
            let mut writer =
                WalSegmentWriterImpl::new_or_open(dir.clone(), SegmentId::new(0)).unwrap();
            writer.write_batch(vec![wal_op.clone()]).unwrap();
        }

        // open it again, send a new write in and close it
        {
            let mut writer =
                WalSegmentWriterImpl::new_or_open(dir.clone(), SegmentId::new(0)).unwrap();
            writer.write_batch(vec![wal_op.clone()]).unwrap();
        }

        // ensure that we have two WalOpBatch in the file with the correct sequence numbers
        let mut reader = WalSegmentReaderImpl::new(dir, SegmentId::new(0)).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();

        assert_eq!(batch.ops, vec![wal_op.clone()]);
        assert_eq!(batch.sequence_number, SequenceNumber::new(1));

        let batch = reader.next_batch().unwrap().unwrap();

        assert_eq!(batch.ops, vec![wal_op]);
        assert_eq!(batch.sequence_number, SequenceNumber::new(2));
    }

    #[test]
    fn wal_can_open_write_and_read_segments() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "foo".to_string(),
            lp: "cpu host=a val=10i 10".to_string(),
            default_time: 1,
        });

        let wal = WalImpl::new(dir.clone()).unwrap();
        let mut writer = wal.open_segment_writer(SegmentId::new(0)).unwrap();
        writer.write_batch(vec![wal_op.clone()]).unwrap();

        let mut writer2 = wal.open_segment_writer(SegmentId::new(1)).unwrap();
        writer2.write_batch(vec![wal_op.clone()]).unwrap();

        let segments = wal.segment_files().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].segment_id, SegmentId::new(0));
        assert_eq!(segments[1].segment_id, SegmentId::new(1));

        let mut reader = wal.open_segment_reader(SegmentId::new(0)).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.ops, vec![wal_op.clone()]);
        assert_eq!(batch.sequence_number, SequenceNumber::new(1));
    }
}
