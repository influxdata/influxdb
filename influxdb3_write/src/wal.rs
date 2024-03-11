//! This is the implementation of the `Wal` that the buffer uses to make buffered data durable
//! on disk.

use crate::paths::SegmentWalFilePath;
use crate::{
    SegmentFile, SegmentId, SegmentRange, SequenceNumber, Wal, WalOp, WalOpBatch, WalSegmentReader,
    WalSegmentWriter,
};
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use datafusion::parquet::file::reader::Length;
use iox_time::Time;
use observability_deps::tracing::{info, warn};
use serde::{Deserialize, Serialize};
use snap::read::FrameDecoder;
use std::any::Any;
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
    #[error("invalid segment file {path:?}: {reason}")]
    InvalidSegmentFile { path: PathBuf, reason: String },

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

    #[error("file exists: {0}")]
    FileExists(PathBuf),

    #[error("file doens't exist: {0}")]
    FileDoesntExist(PathBuf),

    #[error("segment start time not open: {0}")]
    SegmentStartTimeNotOpen(Time),

    #[error("open segment limit reached: {0}")]
    OpenSegmentLimitReached(usize),
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

    fn open_segment_reader(&self, segment_id: SegmentId) -> Result<Box<dyn WalSegmentReader>> {
        let reader = WalSegmentReaderImpl::new(self.root.clone(), segment_id)?;
        Ok(Box::new(reader))
    }

    fn segment_files(&self) -> Result<Vec<SegmentFile>> {
        let dir = std::fs::read_dir(&self.root)?;

        let mut segment_files = Vec::new();

        for child in dir.into_iter() {
            let child = child?;
            let meta = child.metadata()?;
            if meta.is_file() {
                let path = child.path();

                if let Some(file_name) = path.file_stem() {
                    match file_name.to_str() {
                        Some(file_name) => {
                            if let Ok(segment_id) = segment_id_from_file_name(file_name) {
                                segment_files.push(SegmentFile { segment_id, path });
                            } else {
                                warn!(
                                    file_name=?file_name,
                                    "File in wal dir has an invalid file stem that doesn't parse to an integer segment id, ignoring"
                                );
                            }
                        }
                        None => {
                            warn!(
                                file_name=?file_name,
                                "File in wal dir has an invalid file stem that isn't valid UTF-8, ignoring"
                            );
                        }
                    }
                } else {
                    warn!(
                        path=?path,
                        "File in wal dir doesn't have a file stem, ignoring"
                    );
                }
            }
        }

        segment_files.sort_by_key(|f| f.segment_id);

        Ok(segment_files)
    }

    fn delete_wal_segment(&self, segment_id: SegmentId) -> Result<()> {
        let path = SegmentWalFilePath::new(self.root.clone(), segment_id);
        std::fs::remove_file(path)?;
        Ok(())
    }
}

impl Wal for WalImpl {
    fn new_segment_writer(
        &self,
        segment_id: SegmentId,
        range: SegmentRange,
    ) -> Result<Box<dyn WalSegmentWriter>> {
        let writer = WalSegmentWriterImpl::new(self.root.clone(), segment_id, range)?;
        Ok(Box::new(writer))
    }

    fn open_segment_writer(&self, segment_id: SegmentId) -> Result<Box<dyn WalSegmentWriter>> {
        let writer = WalSegmentWriterImpl::open(self.root.clone(), segment_id)?;
        Ok(Box::new(writer))
    }

    fn open_segment_reader(&self, segment_id: SegmentId) -> Result<Box<dyn WalSegmentReader>> {
        self.open_segment_reader(segment_id)
    }

    fn segment_files(&self) -> Result<Vec<SegmentFile>> {
        self.segment_files()
    }

    fn delete_wal_segment(&self, _segment_id: SegmentId) -> Result<()> {
        self.delete_wal_segment(_segment_id)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SegmentHeader {
    pub id: SegmentId,
    pub range: SegmentRange,
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
    pub fn new(root: PathBuf, segment_id: SegmentId, range: SegmentRange) -> Result<Self> {
        let path = SegmentWalFilePath::new(root, segment_id);

        // if there's already a file there, error out
        if path.exists() {
            return Err(Error::FileExists(path.to_path_buf()));
        }

        // it's a new file, initialize it with the header and get ready to start writing
        let mut f = OpenOptions::new().write(true).create(true).open(&path)?;

        f.write_all(FILE_TYPE_IDENTIFIER)?;
        let file_type_bytes_written = FILE_TYPE_IDENTIFIER.len();

        let header = SegmentHeader {
            id: segment_id,
            range,
        };

        let header_bytes = serde_json::to_vec(&header)?;
        f.write_u16::<BigEndian>(
            header_bytes
                .len()
                .try_into()
                .expect("header byes longer than u16"),
        )?;
        f.write_all(&header_bytes)?;

        f.sync_all().expect("fsync failure");

        let bytes_written = file_type_bytes_written + header_bytes.len();

        Ok(Self {
            segment_id,
            f,
            bytes_written,
            sequence_number: SequenceNumber::new(0),
            buffer: Vec::with_capacity(8 * 1204), // 8kiB initial size
        })
    }
    pub fn open(root: PathBuf, segment_id: SegmentId) -> Result<Self> {
        let path = SegmentWalFilePath::new(root, segment_id);

        if let Some(file_info) =
            WalSegmentReaderImpl::read_segment_file_info_if_exists(path.clone())?
        {
            let f = OpenOptions::new().write(true).append(true).open(&path)?;

            Ok(Self {
                segment_id,
                f,
                bytes_written: file_info
                    .bytes_written
                    .try_into()
                    .expect("file length must fit in usize"),
                sequence_number: file_info.last_sequence_number,
                buffer: Vec::with_capacity(8 * 1204), // 8kiB initial size
            })
        } else {
            Err(Error::FileDoesntExist(path.to_path_buf()))
        }
    }

    fn write_batch(&mut self, ops: Vec<WalOp>) -> Result<()> {
        // Ensure the write buffer is always empty before using it.
        self.buffer.clear();

        let sequence_number = self.sequence_number.next();

        let batch = WalOpBatch {
            sequence_number,
            ops,
        };

        let data = serde_json::to_vec(&batch)?;

        let bytes_written = self.write_bytes(data)?;

        self.bytes_written += bytes_written;
        self.sequence_number = sequence_number;

        Ok(())
    }

    fn write_bytes(&mut self, data: Vec<u8>) -> Result<usize> {
        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = data.len();
        u32::try_from(uncompressed_len)?;

        // The chunk header is two u32 values, so write a dummy u64 value and
        // come back to fill them in later.
        self.buffer
            .write_u64::<BigEndian>(0)
            .expect("cannot fail to write to buffer");

        // Compress the payload into the reused buffer, recording the crc hash
        // as it is written.
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

        Ok(bytes_written)
    }
}

#[async_trait]
impl WalSegmentWriter for WalSegmentWriterImpl {
    fn id(&self) -> SegmentId {
        self.segment_id
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written as u64
    }

    fn write_batch(&mut self, ops: Vec<WalOp>) -> Result<()> {
        self.write_batch(ops)
    }

    fn last_sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }
}

#[derive(Debug)]
pub struct WalSegmentWriterNoopImpl {
    segment_id: SegmentId,
    sequence_number: SequenceNumber,
    wal_ops_written: usize,
}

impl WalSegmentWriterNoopImpl {
    pub fn new(segment_id: SegmentId) -> Self {
        Self {
            segment_id,
            sequence_number: SequenceNumber::new(0),
            wal_ops_written: 0,
        }
    }
}

impl WalSegmentWriter for WalSegmentWriterNoopImpl {
    fn id(&self) -> SegmentId {
        self.segment_id
    }

    fn bytes_written(&self) -> u64 {
        self.wal_ops_written as u64
    }

    fn write_batch(&mut self, ops: Vec<WalOp>) -> Result<()> {
        let sequence_number = self.sequence_number.next();
        self.sequence_number = sequence_number;
        self.wal_ops_written += ops.len();
        Ok(())
    }

    fn last_sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }
}

#[derive(Debug)]
pub struct WalSegmentReaderImpl {
    f: BufReader<File>,
    path: SegmentWalFilePath,
    segment_header: SegmentHeader,
}

impl WalSegmentReaderImpl {
    pub fn new(root: impl Into<PathBuf>, segment_id: SegmentId) -> Result<Self> {
        let path = SegmentWalFilePath::new(root, segment_id);
        let mut f = BufReader::new(File::open(path.clone())?);

        let segment_header = read_header(&path, &mut f)?;

        if segment_id != segment_header.id {
            return Err(Error::InvalidSegmentFile {
                path: path.to_path_buf(),
                reason: format!(
                    "expected segment id {:?} in file, got {:?}",
                    segment_id, segment_header.id,
                ),
            });
        }

        let reader = Self {
            f,
            path,
            segment_header,
        };

        Ok(reader)
    }

    fn read_segment_file_info_if_exists(
        path: SegmentWalFilePath,
    ) -> Result<Option<ExistingSegmentFileInfo>> {
        let f = match File::open(path.clone()) {
            Ok(f) => f,
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let bytes_written = f.len().try_into()?;

        let mut f = BufReader::new(f);
        let segment_header = read_header(&path, &mut f)?;

        let mut reader = Self {
            f,
            path,
            segment_header,
        };

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
            Ok(Some(ExistingSegmentFileInfo {
                last_sequence_number: SequenceNumber::new(0),
                bytes_written,
            }))
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
                segment_id: self.segment_header.id,
                expected: expected_len,
                actual: actual_compressed_len,
            });
        }

        if expected_checksum != actual_checksum {
            return Err(Error::ChecksumMismatch {
                segment_id: self.segment_header.id,
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        Ok(Some(data))
    }
}

fn read_header(path: &SegmentWalFilePath, f: &mut BufReader<File>) -> Result<SegmentHeader> {
    let file_type: FileTypeIdentifier = read_array(f)?;

    if file_type != FILE_TYPE_IDENTIFIER {
        return Err(Error::InvalidSegmentFile {
            path: path.to_path_buf(),
            reason: format!(
                "expected file type identifier {:?}, got {:?}",
                FILE_TYPE_IDENTIFIER, file_type
            ),
        });
    }

    let len = f.read_u16::<BigEndian>()?;
    let mut data = vec![0u8; len.into()];
    f.read_exact(&mut data)?;
    let header: SegmentHeader = serde_json::from_slice(&data)?;

    Ok(header)
}

fn read_array<const N: usize>(f: &mut BufReader<File>) -> Result<[u8; N]> {
    let mut data = [0u8; N];
    f.read_exact(&mut data)?;
    Ok(data)
}

struct ExistingSegmentFileInfo {
    last_sequence_number: SequenceNumber,
    bytes_written: u32,
}

impl WalSegmentReader for WalSegmentReaderImpl {
    fn next_batch(&mut self) -> Result<Option<WalOpBatch>> {
        self.next_batch()
    }

    fn header(&self) -> &SegmentHeader {
        &self.segment_header
    }

    fn path(&self) -> &SegmentWalFilePath {
        &self.path
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

        let mut writer =
            WalSegmentWriterImpl::new(dir.clone(), SegmentId::new(0), SegmentRange::test_range())
                .unwrap();
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
            let mut writer = WalSegmentWriterImpl::new(
                dir.clone(),
                SegmentId::new(0),
                SegmentRange::test_range(),
            )
            .unwrap();
            writer.write_batch(vec![wal_op.clone()]).unwrap();
        }

        // open it again, send a new write in and close it
        {
            let mut writer = WalSegmentWriterImpl::open(dir.clone(), SegmentId::new(0)).unwrap();
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
        let mut writer = wal
            .new_segment_writer(SegmentId::new(0), SegmentRange::test_range())
            .unwrap();
        writer.write_batch(vec![wal_op.clone()]).unwrap();

        let mut writer2 = wal
            .new_segment_writer(SegmentId::new(1), SegmentRange::test_range())
            .unwrap();
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
