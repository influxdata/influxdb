use crate::{ClosedSegment, SegmentId, WriteSummary, FILE_TYPE_IDENTIFIER};
use byteorder::{BigEndian, WriteBytesExt};
use crc32fast::Hasher;
use snafu::prelude::*;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    mem, num,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Struct for writing data to a segment file in a wal
#[derive(Debug)]
pub struct OpenSegmentFileWriter {
    id: SegmentId,
    path: PathBuf,
    f: File,
    bytes_written: usize,
}

impl OpenSegmentFileWriter {
    pub fn new_in_directory(
        dir: impl Into<PathBuf>,
        next_id_source: Arc<AtomicU64>,
    ) -> Result<Self> {
        let id = SegmentId::new(next_id_source.fetch_add(1, Ordering::Relaxed));
        let path = crate::build_segment_path(dir, id);

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .context(SegmentCreateSnafu)?;

        f.write_all(FILE_TYPE_IDENTIFIER)
            .context(SegmentWriteFileTypeSnafu)?;
        let file_type_bytes_written = FILE_TYPE_IDENTIFIER.len();

        let id_bytes = id.as_bytes();
        f.write_all(&id_bytes).context(SegmentWriteIdSnafu)?;
        let id_bytes_written = id_bytes.len();

        f.sync_all().expect("fsync failure");

        let bytes_written = file_type_bytes_written + id_bytes_written;

        Ok(Self {
            id,
            path,
            f,
            bytes_written,
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
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

        self.f.sync_all().expect("fsync failure");

        let bytes_written = mem::size_of_val(&checksum)
            + mem::size_of_val(&actual_compressed_len)
            + compressed_data.len();
        self.bytes_written += bytes_written;

        Ok(WriteSummary {
            total_bytes: self.bytes_written,
            bytes_written,
            segment_id: self.id,
            checksum,
        })
    }

    pub fn close(self) -> Result<ClosedSegment> {
        let Self {
            id,
            path,
            bytes_written,
            ..
        } = self;
        Ok(ClosedSegment {
            id,
            path,
            size: bytes_written
                .try_into()
                .expect("bytes_written did not fit in size type"),
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
