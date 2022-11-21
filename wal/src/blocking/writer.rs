use crate::{ClosedSegment, SegmentId, WriteSummary, FILE_TYPE_IDENTIFIER};
use byteorder::{BigEndian, WriteBytesExt};
use crc32fast::Hasher;
use snafu::prelude::*;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    mem, num,
    path::PathBuf,
};

pub struct OpenSegmentFileWriter {
    id: SegmentId,
    path: PathBuf,
    f: File,
    bytes_written: usize,
}

impl OpenSegmentFileWriter {
    pub fn new_in_directory(dir: impl Into<PathBuf>) -> Result<Self> {
        let id = SegmentId::new();
        let path = crate::fnamex(dir, id);

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .context(SegmentCreateSnafu)?;

        f.write_all(FILE_TYPE_IDENTIFIER)
            .context(SegmentWriteFileTypeSnafu)?;
        let file_type_bytes_written = FILE_TYPE_IDENTIFIER.len();

        let id_bytes = id.as_bytes();
        f.write_all(id_bytes).context(SegmentWriteIdSnafu)?;
        let id_bytes_written = id_bytes.len();

        f.sync_all().context(SegmentSyncHeaderSnafu)?;

        let bytes_written = file_type_bytes_written + id_bytes_written;

        Ok(Self {
            id,
            path,
            f,
            bytes_written,
        })
    }

    pub fn write(&mut self, data: &[u8]) -> Result<WriteSummary> {
        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = data.len();
        u32::try_from(uncompressed_len).context(ChunkSizeTooLargeSnafu {
            actual: uncompressed_len,
        })?;

        // TODO: Can this code avoid keeping all intermediate data in memory?
        // TODO: The snappy frame format has a built-in CRC32; should we just use that?
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(data).context(UnableToCompressDataSnafu)?;
        let compressed_data = encoder.into_inner().expect("cannot fail to flush to a Vec");
        let actual_compressed_len = compressed_data.len();
        let actual_compressed_len =
            u32::try_from(actual_compressed_len).context(ChunkSizeTooLargeSnafu {
                actual: actual_compressed_len,
            })?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data);
        let checksum = hasher.finalize();

        self.f
            .write_u32::<BigEndian>(checksum)
            .context(SegmentWriteChecksumSnafu)?;
        self.f
            .write_u32::<BigEndian>(actual_compressed_len)
            .context(SegmentWriteLengthSnafu)?;
        self.f
            .write_all(&compressed_data)
            .context(SegmentWriteDataSnafu)?;

        // TODO: should there be a `sync_all` here?

        let bytes_written = mem::size_of_val(&checksum)
            + mem::size_of_val(&actual_compressed_len)
            + compressed_data.len();
        self.bytes_written += bytes_written;

        Ok(WriteSummary {
            checksum,
            total_bytes: self.bytes_written,
            bytes_written,
        })
    }

    pub fn close(self) -> Result<ClosedSegment> {
        let Self {
            id,
            path,
            f,
            bytes_written: _,
        } = self;
        let metadata = f.metadata().context(UnableToReadFileMetadataSnafu)?;
        Ok(ClosedSegment {
            id,
            path,
            size: metadata.len(),
            created_at: metadata.created().context(UnableToReadCreatedSnafu)?,
        })
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    SegmentCreate {
        source: io::Error,
    },

    SegmentWriteFileType {
        source: io::Error,
    },

    SegmentWriteId {
        source: io::Error,
    },

    SegmentSyncHeader {
        source: io::Error,
    },

    SegmentWriteChecksum {
        source: io::Error,
    },

    SegmentWriteLength {
        source: io::Error,
    },

    SegmentWriteData {
        source: io::Error,
    },

    ChunkSizeTooLarge {
        source: num::TryFromIntError,
        actual: usize,
    },

    UnableToCompressData {
        source: io::Error,
    },

    UnableToReadFileMetadata {
        source: io::Error,
    },

    UnableToReadCreated {
        source: io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
