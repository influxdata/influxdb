use crate::{FileTypeIdentifier, SegmentEntry, SequencedWalOp};
use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher;
use snafu::prelude::*;
use snap::read::FrameDecoder;
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

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

    pub fn read_header(&mut self) -> Result<(FileTypeIdentifier, uuid::Bytes)> {
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

    pub fn entries(&mut self) -> Result<Vec<SegmentEntry>> {
        self.collect()
    }

    pub fn next_ops(&mut self) -> Result<Option<Vec<SequencedWalOp>>> {
        if let Some(entry) = self.one_entry()? {
            let decoded =
                serde_json::from_slice(&entry.data).context(UnableToDeserializeDataSnafu)?;

            return Ok(Some(decoded));
        }

        Ok(None)
    }
}

impl<R> Iterator for ClosedSegmentFileReader<R>
where
    R: io::Read,
{
    type Item = Result<SegmentEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.one_entry().transpose()
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
    UnableToOpenFile { source: io::Error, path: PathBuf },

    UnableToReadArray { source: io::Error, length: usize },

    UnableToReadChecksum { source: io::Error },

    UnableToReadLength { source: io::Error },

    UnableToReadData { source: io::Error },

    LengthMismatch { expected: u64, actual: u64 },

    ChecksumMismatch { expected: u32, actual: u32 },

    UnableToDecompressData { source: snap::Error },

    UnableToDeserializeData { source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
