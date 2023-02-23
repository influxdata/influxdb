use crate::{ClosedSegment, SegmentId, WriteSummary, FILE_TYPE_IDENTIFIER};
use byteorder::{BigEndian, WriteBytesExt};
use crc32fast::Hasher;
use snafu::prelude::*;
use std::{
    fs::{File, OpenOptions},
    io::{self, Cursor, Write},
    mem, num,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Defines the desired maximum size of the re-used write
/// [`OpenSegmentFileWriter`] buffer.
///
/// The buffer is free to exceed this soft limit as necessary, but will always
/// be shrunk back down to at most this size eventually.
///
/// Setting this too low causes needless reallocations for each write that
/// exceeds it. Setting it too high wastes memory. Configure it to a tolerable
/// amount of memory overhead for the lifetime of the writer.
const SOFT_MAX_BUFFER_LEN: usize = 1024 * 128; // 128kiB

/// Struct for writing data to a segment file in a wal
#[derive(Debug)]
pub struct OpenSegmentFileWriter {
    id: SegmentId,
    path: PathBuf,
    f: File,
    bytes_written: usize,

    buffer: Vec<u8>,
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
            buffer: Vec::with_capacity(8 * 1204), // 8kiB initial size
        })
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn write(&mut self, data: &[u8]) -> Result<WriteSummary> {
        // Ensure the write buffer is always empty before using it.
        self.buffer.clear();
        // And shrink the buffer below the maximum permitted size should the odd
        // large batch grow it. This is a NOP if the size is less than
        // SOFT_MAX_BUFFER_LEN already.
        self.buffer.shrink_to(SOFT_MAX_BUFFER_LEN);

        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = data.len();
        u32::try_from(uncompressed_len).context(ChunkSizeTooLargeSnafu {
            actual: uncompressed_len,
        })?;

        // The chunk header is two u32 values, so write a dummy u64 value and
        // come back to fill them in later.
        self.buffer
            .write_u64::<BigEndian>(0)
            .expect("cannot fail to write to buffer");

        // Compress the payload into the reused buffer, recording the crc hash
        // as it is wrote.
        let mut encoder = snap::write::FrameEncoder::new(HasherWrapper::new(&mut self.buffer));
        encoder.write_all(data).context(UnableToCompressDataSnafu)?;
        let (checksum, buf) = encoder
            .into_inner()
            .expect("cannot fail to flush to a Vec")
            .finalize();

        // Adjust the compressed length to take into account the u64 padding
        // above.
        let compressed_len = buf.len() - mem::size_of::<u64>();
        let compressed_len = u32::try_from(compressed_len).context(ChunkSizeTooLargeSnafu {
            actual: compressed_len,
        })?;

        // Go back and write the chunk header values
        let mut buf = Cursor::new(buf);
        buf.set_position(0);

        buf.write_u32::<BigEndian>(checksum)
            .context(SegmentWriteChecksumSnafu)?;
        buf.write_u32::<BigEndian>(compressed_len)
            .context(SegmentWriteLengthSnafu)?;

        // Write the entire buffer to the file
        let buf = buf.into_inner();
        let bytes_written = buf.len();
        self.f.write_all(buf).context(SegmentWriteDataSnafu)?;

        // fsync the fd
        self.f.sync_all().expect("fsync failure");

        self.bytes_written += bytes_written;

        Ok(WriteSummary {
            total_bytes: self.bytes_written,
            bytes_written,
            segment_id: self.id,
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
