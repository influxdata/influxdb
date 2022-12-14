use crate::{FileTypeIdentifier, SegmentEntry, SegmentIdBytes, SequencedWalOp};
use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher;
use generated_types::influxdata::iox::wal::v1::WalOpBatch as ProtoWalOpBatch;
use prost::Message;
use snafu::prelude::*;
use snap::read::FrameDecoder;
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct ClosedSegmentFileReader<R>(R);

impl ClosedSegmentFileReader<BufReader<File>> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let f = File::open(path).context(UnableToOpenFileSnafu { path })?;
        let f = BufReader::new(f);
        Ok(Self::new(f))
    }
}

impl<R> ClosedSegmentFileReader<R>
where
    R: Read,
{
    pub fn new(f: R) -> Self {
        Self(f)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut data = [0u8; N];
        self.0
            .read_exact(&mut data)
            .context(UnableToReadArraySnafu { length: N })?;
        Ok(data)
    }

    pub fn read_header(&mut self) -> Result<(FileTypeIdentifier, SegmentIdBytes)> {
        Ok((self.read_array()?, self.read_array()?))
    }

    fn one_entry(&mut self) -> Result<Option<SegmentEntry>> {
        let expected_checksum = match self.0.read_u32::<BigEndian>() {
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            other => other.context(UnableToReadChecksumSnafu)?,
        };

        let expected_len = self
            .0
            .read_u32::<BigEndian>()
            .context(UnableToReadLengthSnafu)?
            .into();

        let compressed_read = self.0.by_ref().take(expected_len);
        let hashing_read = CrcReader::new(compressed_read);
        let mut decompressing_read = FrameDecoder::new(hashing_read);

        let mut data = Vec::with_capacity(100);
        decompressing_read
            .read_to_end(&mut data)
            .context(UnableToReadDataSnafu)?;

        let (actual_compressed_len, actual_checksum) = decompressing_read.get_mut().checksum();

        ensure!(
            expected_len == actual_compressed_len,
            LengthMismatchSnafu {
                expected: expected_len,
                actual: actual_compressed_len
            }
        );

        ensure!(
            expected_checksum == actual_checksum,
            ChecksumMismatchSnafu {
                expected: expected_checksum,
                actual: actual_checksum
            }
        );

        Ok(Some(SegmentEntry {
            checksum: expected_checksum,
            data,
        }))
    }

    pub fn next_batch(&mut self) -> Result<Option<Vec<SequencedWalOp>>> {
        if let Some(entry) = self.one_entry()? {
            let decoded =
                ProtoWalOpBatch::decode(&*entry.data).context(UnableToDeserializeDataSnafu)?;

            let mut ops = Vec::with_capacity(decoded.ops.len());
            for op in decoded.ops {
                ops.push(op.try_into().context(InvalidMessageSnafu)?);
            }

            return Ok(Some(ops));
        }

        Ok(None)
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

    fn checksum(&mut self) -> (u64, u32) {
        // FIXME: If rust-snappy added an `into_inner`, we should
        // take `self` by value
        (
            std::mem::take(&mut self.bytes_seen),
            std::mem::take(&mut self.hasher).finalize(),
        )
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

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToOpenFile {
        source: io::Error,
        path: PathBuf,
    },

    UnableToReadArray {
        source: io::Error,
        length: usize,
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
        expected: u64,
        actual: u64,
    },

    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },

    UnableToDecompressData {
        source: snap::Error,
    },

    UnableToDeserializeData {
        source: prost::DecodeError,
    },

    InvalidMessage {
        source: generated_types::google::FieldViolation,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SegmentId, FILE_TYPE_IDENTIFIER};
    use byteorder::WriteBytesExt;
    use std::io::Write;
    use test_helpers::assert_error;

    #[test]
    fn successful_read_no_entries() {
        let segment_file = FakeSegmentFile::new();

        let data = segment_file.data();
        let mut reader = ClosedSegmentFileReader::new(data.as_slice());

        let (file_type_id, uuid) = reader.read_header().unwrap();
        assert_eq!(&file_type_id, FILE_TYPE_IDENTIFIER);
        assert_eq!(uuid, segment_file.id.as_bytes());

        let entry = reader.one_entry().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn successful_read_with_entries() {
        let mut segment_file = FakeSegmentFile::new();
        let entry_input_1 = FakeSegmentEntry::new(b"hello");
        segment_file.add_entry(entry_input_1.clone());

        let entry_input_2 = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(entry_input_2.clone());

        let data = segment_file.data();
        let mut reader = ClosedSegmentFileReader::new(data.as_slice());

        let (file_type_id, uuid) = reader.read_header().unwrap();
        assert_eq!(&file_type_id, FILE_TYPE_IDENTIFIER);
        assert_eq!(uuid, segment_file.id.as_bytes());

        let entry_output_1 = reader.one_entry().unwrap().unwrap();
        let expected_1 = SegmentEntry::from(&entry_input_1);
        assert_eq!(entry_output_1.checksum, expected_1.checksum);
        assert_eq!(entry_output_1.data, expected_1.data);

        let entry_output_2 = reader.one_entry().unwrap().unwrap();
        let expected_2 = SegmentEntry::from(&entry_input_2);
        assert_eq!(entry_output_2.checksum, expected_2.checksum);
        assert_eq!(entry_output_2.data, expected_2.data);

        let entry = reader.one_entry().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn unsuccessful_read_too_short_len() {
        let mut segment_file = FakeSegmentFile::new();
        let bad_entry_input = FakeSegmentEntry::new(b"hello");
        let good_length = bad_entry_input.compressed_len();
        let bad_entry_input = bad_entry_input.with_compressed_len(good_length - 1);
        segment_file.add_entry(bad_entry_input);

        let good_entry_input = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(good_entry_input);

        let data = segment_file.data();
        let mut reader = ClosedSegmentFileReader::new(data.as_slice());

        let (file_type_id, uuid) = reader.read_header().unwrap();
        assert_eq!(&file_type_id, FILE_TYPE_IDENTIFIER);
        assert_eq!(uuid, segment_file.id.as_bytes());

        let read_fail = reader.one_entry();
        assert_error!(read_fail, Error::UnableToReadData { .. });
        // Trying to continue reading will fail as well, see:
        // <https://github.com/influxdata/influxdb_iox/issues/6222>
        assert_error!(reader.one_entry(), Error::UnableToReadData { .. });
    }

    #[test]
    fn unsuccessful_read_too_long_len() {
        let mut segment_file = FakeSegmentFile::new();
        let bad_entry_input = FakeSegmentEntry::new(b"hello");
        let good_length = bad_entry_input.compressed_len();
        let bad_entry_input = bad_entry_input.with_compressed_len(good_length + 1);
        segment_file.add_entry(bad_entry_input);

        let good_entry_input = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(good_entry_input);

        let data = segment_file.data();
        let mut reader = ClosedSegmentFileReader::new(data.as_slice());

        let (file_type_id, uuid) = reader.read_header().unwrap();
        assert_eq!(&file_type_id, FILE_TYPE_IDENTIFIER);
        assert_eq!(uuid, segment_file.id.as_bytes());

        let read_fail = reader.one_entry();
        assert_error!(read_fail, Error::UnableToReadData { .. });
        // Trying to continue reading will fail as well, see:
        // <https://github.com/influxdata/influxdb_iox/issues/6222>
        assert_error!(reader.one_entry(), Error::UnableToReadData { .. });
    }

    #[test]
    fn unsuccessful_read_checksum_mismatch() {
        let mut segment_file = FakeSegmentFile::new();
        let bad_entry_input = FakeSegmentEntry::new(b"hello");
        let good_checksum = bad_entry_input.checksum();
        let bad_entry_input = bad_entry_input.with_checksum(good_checksum + 1);
        segment_file.add_entry(bad_entry_input);

        let good_entry_input = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(good_entry_input.clone());

        let data = segment_file.data();
        let mut reader = ClosedSegmentFileReader::new(data.as_slice());

        let (file_type_id, uuid) = reader.read_header().unwrap();
        assert_eq!(&file_type_id, FILE_TYPE_IDENTIFIER);
        assert_eq!(uuid, segment_file.id.as_bytes());

        let read_fail = reader.one_entry();
        assert_error!(read_fail, Error::ChecksumMismatch { .. });

        // A bad checksum won't corrupt further entries
        let entry_output_2 = reader.one_entry().unwrap().unwrap();
        let expected_2 = SegmentEntry::from(&good_entry_input);
        assert_eq!(entry_output_2.checksum, expected_2.checksum);
        assert_eq!(entry_output_2.data, expected_2.data);

        let entry = reader.one_entry().unwrap();
        assert!(entry.is_none());
    }

    #[derive(Debug)]
    struct FakeSegmentFile {
        id: SegmentId,
        entries: Vec<FakeSegmentEntry>,
    }

    impl FakeSegmentFile {
        fn new() -> Self {
            Self {
                id: SegmentId::new(0),
                entries: Default::default(),
            }
        }

        fn add_entry(&mut self, entry: FakeSegmentEntry) {
            self.entries.push(entry);
        }

        fn data(&self) -> Vec<u8> {
            let mut f = Vec::new();

            f.write_all(FILE_TYPE_IDENTIFIER).unwrap();

            let id_bytes = self.id.as_bytes();
            f.write_all(&id_bytes).unwrap();

            for entry in &self.entries {
                f.write_u32::<BigEndian>(entry.checksum()).unwrap();
                f.write_u32::<BigEndian>(entry.compressed_len()).unwrap();
                f.write_all(&entry.compressed_data()).unwrap();
            }

            f
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeSegmentEntry {
        checksum: Option<u32>,
        compressed_len: Option<u32>,
        uncompressed_data: Vec<u8>,
    }

    impl FakeSegmentEntry {
        fn new(data: &[u8]) -> Self {
            Self {
                checksum: None,
                compressed_len: None,
                uncompressed_data: data.to_vec(),
            }
        }

        fn with_compressed_len(self, compressed_len: u32) -> Self {
            Self {
                compressed_len: Some(compressed_len),
                ..self
            }
        }

        fn with_checksum(self, checksum: u32) -> Self {
            Self {
                checksum: Some(checksum),
                ..self
            }
        }

        fn checksum(&self) -> u32 {
            self.checksum.unwrap_or_else(|| {
                let mut hasher = Hasher::new();
                hasher.update(&self.compressed_data());
                hasher.finalize()
            })
        }

        fn compressed_data(&self) -> Vec<u8> {
            let mut encoder = snap::write::FrameEncoder::new(Vec::new());
            encoder.write_all(&self.uncompressed_data).unwrap();
            encoder.into_inner().expect("cannot fail to flush to a Vec")
        }

        fn compressed_len(&self) -> u32 {
            self.compressed_len
                .unwrap_or_else(|| self.compressed_data().len() as u32)
        }
    }

    impl From<&FakeSegmentEntry> for SegmentEntry {
        fn from(fake: &FakeSegmentEntry) -> Self {
            Self {
                checksum: fake.checksum(),
                data: fake.uncompressed_data.clone(),
            }
        }
    }
}
