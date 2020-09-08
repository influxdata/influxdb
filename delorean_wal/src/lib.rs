#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

//! # delorean_wal
//!
//! This crate provides a WAL tailored for delorean `Partition`s to optionally use.
//!
//! Work remaining:
//!
//! - More testing for correctness; the existing tests mostly demonstrate possible usages.
//! - Error handling

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use once_cell::sync::Lazy;
use regex::Regex;
use snafu::{ensure, ResultExt, Snafu};
use std::{
    convert::TryFrom,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Seek, SeekFrom, Write},
    iter, mem, num,
    path::{Path, PathBuf},
};

/// Opaque public `Error` type
#[derive(Debug, Snafu)]
pub struct Error(InternalError);

/// SequenceNumber is a u64 monotonically increasing number for each WAL entry
pub type SequenceNumber = u64;

#[derive(Debug, Snafu)]
enum InternalError {
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
}

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Build a Wal rooted at a directory.
///
/// May take more configuration options in the future.
#[derive(Debug, Clone)]
pub struct WalBuilder {
    root: PathBuf,
    file_rollover_size: u64,
}

impl WalBuilder {
    /// The default size to create new WAL files at. Currently 10MiB.
    ///
    /// See [WalBuilder::file_rollover_size]
    pub const DEFAULT_FILE_ROLLOVER_SIZE_BYTES: u64 = 10 * 1024 * 1024;

    /// Create a new WAL rooted at the provided directory on disk.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        // TODO: Error if `root` is not a directory?
        let root = root.into();
        Self {
            root,
            file_rollover_size: Self::DEFAULT_FILE_ROLLOVER_SIZE_BYTES,
        }
    }

    /// Set the size (in bytes) of each WAL file that should prompt a file rollover when it is
    /// exceeded.
    ///
    /// File rollover happens per sync batch. If the file is underneath this file size limit at the
    /// start of a sync operation, the entire sync batch will be written to that file even if
    /// some of the entries in the batch cause the file to exceed the file size limit.
    ///
    /// See [WalBuilder::DEFAULT_FILE_ROLLOVER_SIZE_BYTES]
    pub fn file_rollover_size(mut self, file_rollover_size: u64) -> Self {
        self.file_rollover_size = file_rollover_size;
        self
    }

    /// Consume the builder and create a `Wal`.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    pub fn wal(self) -> Result<Wal> {
        let rollover_size = self.file_rollover_size;
        Wal::new(self.file_locator(), rollover_size)
    }

    /// Consume the builder to get an iterator of all entries in this
    /// WAL that have been persisted to disk.
    ///
    /// Sequence numbers on the entries will be in increasing order, but if files have been
    /// modified or deleted since getting this iterator, there may be gaps in the sequence.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    pub fn entries(self) -> Result<impl Iterator<Item = Result<Entry>>> {
        Loader::load(self.file_locator())
    }

    fn file_locator(self) -> FileLocator {
        FileLocator {
            root: self.root,
            file_rollover_size: self.file_rollover_size,
        }
    }
}

/// The main WAL type to interact with.
///
/// For use in single-threaded synchronous contexts. For multi-threading or
/// asynchronous, you should wrap the WAL in the appropriate patterns.
///
/// # Example
///
/// This demonstrates using the WAL with the Tokio asynchronous runtime.
///
/// ```
/// # fn example(root_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
/// use delorean_wal::{WalBuilder, WritePayload};
///
/// // This wal should be either protected with a mutex or moved into a single
/// // worker thread that receives writes from channels.
/// let mut wal = WalBuilder::new(root_path).wal()?;
///
/// // Now create a payload and append it
/// let payload = WritePayload::new(Vec::from("some data"))?;
///
/// // append will create a new WAL entry with its own sequence number, which is returned
/// let sequence_number = wal.append(payload)?;
///
/// // after appends, call sync_all to fsync the underlying WAL file
/// wal.sync_all()?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Wal {
    files: FileLocator,
    sequence_number: u64,
    total_size: u64,
    active_file: Option<File>,
    file_rollover_size: u64,
}

impl Wal {
    fn new(files: FileLocator, file_rollover_size: u64) -> Result<Self> {
        let last_sequence_number = Loader::last_sequence_number(&files)?;
        let sequence_number = last_sequence_number.map_or(0, |last| last + 1);

        let total_size = files.total_size();

        Ok(Self {
            files,
            sequence_number,
            total_size,
            file_rollover_size,
            active_file: None,
        })
    }

    /// A path to a file for storing arbitrary metadata about this WAL, guaranteed not to collide
    /// with the data files.
    pub fn metadata_path(&self) -> PathBuf {
        self.files.root.join("metadata")
    }

    /// Appends a WritePayload to the active segment file in the WAL and returns its
    /// assigned sequence number.
    ///
    /// To ensure the data is written to disk, `sync_all` should be called after a
    /// single or batch of append operations.
    pub fn append(&mut self, payload: WritePayload) -> Result<SequenceNumber> {
        let sequence_number = self.sequence_number;

        let mut f = match self.active_file.take() {
            Some(f) => f,
            None => self.files.open_file_for_append(sequence_number)?,
        };

        let h = Header {
            sequence_number,
            checksum: payload.checksum,
            len: payload.len,
        };

        h.write(&mut f)?;
        f.write_all(&payload.data).context(UnableToWriteData)?;

        self.total_size += Header::LEN + payload.len as u64;
        self.active_file = Some(f);
        self.sequence_number += 1;

        Ok(sequence_number)
    }

    /// Total size, in bytes, of all the data in all the files in the WAL. If files are deleted
    /// from disk without deleting them through the WAL, the size won't reflect that deletion
    /// until the WAL is recreated.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// Deletes files up to, but not including, the file that contains the entry number specified
    pub fn delete_up_to_entry(&self, entry_number: u64) -> Result<()> {
        let mut iter = self.files.existing_filenames()?.peekable();
        let hypothetical_filename = self
            .files
            .filename_starting_at_sequence_number(entry_number);

        while let Some(inner_path) = iter.next() {
            if iter.peek().map_or(false, |p| p < &hypothetical_filename) {
                // Intentionally ignore failures. Should we collect them for reporting instead?
                let _ = fs::remove_file(inner_path);
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Flush all pending bytes in the active segment file to disk and closes it if it is over
    /// the file rollover size.
    pub fn sync_all(&mut self) -> Result<()> {
        let f = self.active_file.take();

        if let Some(f) = f {
            f.sync_all().context(UnableToSync)?;

            let meta = f.metadata().context(UnableToReadFileMetadata)?;
            if meta.len() < self.file_rollover_size {
                self.active_file = Some(f);
            }
        }

        Ok(())
    }
}

// Manages files within the WAL directory
#[derive(Debug)]
struct FileLocator {
    root: PathBuf,
    file_rollover_size: u64,
}

impl FileLocator {
    const PREFIX: &'static str = "wal_";
    const EXTENSION: &'static str = "db";

    fn open_files_for_read(&self) -> Result<impl Iterator<Item = Result<Option<File>>> + '_> {
        Ok(self
            .existing_filenames()?
            .map(move |path| self.open_file_for_read(&path)))
    }

    fn total_size(&self) -> u64 {
        self.existing_filenames()
            .map(|files| {
                files
                    .map(|file| {
                        fs::metadata(file)
                            .map(|metadata| metadata.len())
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    fn open_file_for_read(&self, path: &Path) -> Result<Option<File>> {
        let r = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path);

        match r {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            r => r
                .map(Some)
                .context(UnableToOpenFile { path })
                .map_err(Into::into),
        }
    }

    fn open_file_for_append(&self, starting_sequence_number: u64) -> Result<File> {
        // Is there an existing file?
        let file_name = self
            .active_filename()?
            .filter(|existing| {
                // If there is an existing file, check its size.
                fs::metadata(&existing)
                    // Use the existing file if its size is under the file size limit.
                    .map(|metadata| metadata.len() < self.file_rollover_size)
                    .unwrap_or(false)
            })
            // If there is no file or the file is over the file size limit, start a new file.
            .unwrap_or_else(|| self.filename_starting_at_sequence_number(starting_sequence_number));

        Ok(OpenOptions::new()
            .read(false)
            .append(true)
            .create(true)
            .open(&file_name)
            .context(UnableToOpenFile { path: file_name })?)
    }

    fn active_filename(&self) -> Result<Option<PathBuf>> {
        Ok(self.existing_filenames()?.last())
    }

    fn existing_filenames(&self) -> Result<impl Iterator<Item = PathBuf>> {
        static FILENAME_PATTERN: Lazy<Regex> = Lazy::new(|| {
            let pattern = format!(r"^{}[0-9a-f]{{16}}$", FileLocator::PREFIX);
            Regex::new(&pattern).expect("Hardcoded regex should be valid")
        });

        let mut wal_paths: Vec<_> = fs::read_dir(&self.root)
            .context(UnableToReadDirectoryContents { path: &self.root })?
            .flatten() // Discard errors
            .map(|e| e.path())
            .filter(|path| path.extension() == Some(OsStr::new(Self::EXTENSION)))
            .filter(|path| {
                path.file_stem().map_or(false, |file_stem| {
                    let file_stem = file_stem.to_string_lossy();
                    FILENAME_PATTERN.is_match(&file_stem)
                })
            })
            .collect();

        wal_paths.sort();

        Ok(wal_paths.into_iter())
    }

    fn filename_starting_at_sequence_number(&self, starting_sequence_number: u64) -> PathBuf {
        let file_stem = format!("{}{:016x}", Self::PREFIX, starting_sequence_number);
        let mut filename = self.root.join(file_stem);
        filename.set_extension(Self::EXTENSION);
        filename
    }
}

/// Produces an iterator over the on-disk entries in the WAL.
///
/// # Asynchronous considerations
///
/// This type performs blocking IO and care should be taken when using
/// it in an asynchronous context.
#[derive(Debug)]
struct Loader;

impl Loader {
    fn last_sequence_number(files: &FileLocator) -> Result<Option<u64>> {
        let last = Self::headers(files)?.last().transpose()?;
        Ok(last.map(|h| h.sequence_number))
    }

    fn headers(files: &FileLocator) -> Result<impl Iterator<Item = Result<Header>>> {
        let r = files
            .open_files_for_read()?
            .flat_map(|result_option_file| result_option_file.transpose())
            .map(|result_file| result_file.and_then(Self::headers_from_one_file));

        itertools::process_results(r, |iterator_of_iterators_of_result_headers| {
            iterator_of_iterators_of_result_headers
                .flatten()
                .collect::<Vec<_>>()
                .into_iter()
        })
    }

    fn headers_from_one_file(mut file: File) -> Result<impl Iterator<Item = Result<Header>>> {
        let metadata = file.metadata().context(UnableToReadFileMetadata)?;
        let mut length_remaining = metadata.len();

        Ok(Box::new(iter::from_fn(move || {
            if length_remaining == 0 {
                return None;
            }

            match Header::read(&mut file) {
                Ok(header) => {
                    let data_len = i64::from(header.len);
                    file.seek(SeekFrom::Current(data_len)).unwrap();

                    length_remaining -= Header::LEN + u64::from(header.len);

                    Some(Ok(header))
                }
                Err(e) => Some(Err(e)),
            }
        })))
    }

    fn load(files: FileLocator) -> Result<impl Iterator<Item = Result<Entry>>> {
        let r = files
            .open_files_for_read()?
            .flat_map(|result_option_file| result_option_file.transpose())
            .map(|result_file| result_file.and_then(Self::load_from_one_file));

        itertools::process_results(r, |iterator_of_iterators_of_result_entries| {
            iterator_of_iterators_of_result_entries
                .flatten()
                .collect::<Vec<_>>()
                .into_iter()
        })
    }

    fn load_from_one_file(mut file: File) -> Result<impl Iterator<Item = Result<Entry>>> {
        let metadata = file.metadata().context(UnableToReadFileMetadata)?;
        let mut length_remaining = metadata.len();

        Ok(Box::new(iter::from_fn(move || {
            if length_remaining == 0 {
                return None;
            }

            match Self::load_one(&mut file) {
                Ok((entry, bytes_read)) => {
                    length_remaining -= bytes_read;

                    Some(Ok(entry))
                }
                Err(e) => Some(Err(e)),
            }
        })))
    }

    fn load_one(file: &mut File) -> Result<(Entry, u64)> {
        let header = Header::read(&mut *file)?;

        let expected_len_us =
            usize::try_from(header.len).expect("Only designed to run on 32-bit systems or higher");

        let mut data = Vec::with_capacity(expected_len_us);
        let actual_len = file
            .take(u64::from(header.len))
            .read_to_end(&mut data)
            .context(UnableToReadData)?;

        ensure!(
            expected_len_us == actual_len,
            LengthMismatch {
                expected: expected_len_us,
                actual: actual_len
            }
        );

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let actual_checksum = hasher.finalize();

        ensure!(
            header.checksum == actual_checksum,
            ChecksumMismatch {
                expected: header.checksum,
                actual: actual_checksum
            }
        );

        let entry = Entry {
            sequence_number: header.sequence_number,
            data,
        };

        let bytes_read = Header::LEN + u64::from(header.len);

        Ok((entry, bytes_read))
    }
}

#[derive(Debug)]
struct Header {
    sequence_number: u64,
    checksum: u32,
    len: u32,
}

impl Header {
    const LEN: u64 = (mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>()) as u64;

    fn read(mut r: impl Read) -> Result<Self> {
        let sequence_number = r
            .read_u64::<LittleEndian>()
            .context(UnableToReadSequenceNumber)?;
        let checksum = r.read_u32::<LittleEndian>().context(UnableToReadChecksum)?;
        let len = r.read_u32::<LittleEndian>().context(UnableToReadLength)?;

        Ok(Self {
            sequence_number,
            checksum,
            len,
        })
    }

    fn write(&self, mut w: impl Write) -> Result<()> {
        w.write_u64::<LittleEndian>(self.sequence_number)
            .context(UnableToWriteSequenceNumber)?;
        w.write_u32::<LittleEndian>(self.checksum)
            .context(UnableToWriteChecksum)?;
        w.write_u32::<LittleEndian>(self.len)
            .context(UnableToWriteLength)?;
        Ok(())
    }
}

/// One batch of data read from the WAL.
///
/// This corresponds to one call to `Wal::append`.
#[derive(Debug)]
pub struct Entry {
    sequence_number: u64,
    data: Vec<u8>,
}

impl Entry {
    /// Gets the unique, increasing sequence number associated with this data
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Gets a reference to the entry's data
    pub fn as_data(&self) -> &[u8] {
        &self.data
    }

    /// Gets the entry's data
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

/// A single write to append to the WAL file
#[derive(Debug)]
pub struct WritePayload {
    checksum: u32,
    data: Vec<u8>,
    len: u32,
}

impl WritePayload {
    /// Initializes a write payload and computes its CRC from the passed in vec.
    pub fn new(data: Vec<u8>) -> Result<Self> {
        // Only designed to support chunks up to `u32::max` bytes long.
        let len = data.len();
        let len = u32::try_from(len).context(ChunkSizeTooLarge { actual: len })?;

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        Ok(Self {
            checksum,
            data,
            len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn sequence_numbers_are_persisted() -> Result {
        let dir = delorean_test_helpers::tmp_dir()?;
        let builder = WalBuilder::new(dir.as_ref());
        let mut wal;

        // Create one in-memory WAL and sync it
        {
            wal = builder.clone().wal()?;

            let data = Vec::from("somedata");
            let data = WritePayload::new(data)?;
            let seq = wal.append(data)?;
            assert_eq!(0, seq);
            wal.sync_all()?;
        }

        // Pretend the process restarts
        {
            wal = builder.wal()?;

            assert_eq!(1, wal.sequence_number);
        }

        Ok(())
    }

    #[test]
    fn sequence_numbers_increase_by_number_of_pending_entries() -> Result {
        let dir = delorean_test_helpers::tmp_dir()?;
        let builder = WalBuilder::new(dir.as_ref());
        let mut wal = builder.wal()?;

        // Write 1 entry then sync
        let data = Vec::from("some");
        let data = WritePayload::new(data)?;
        let seq = wal.append(data)?;
        wal.sync_all()?;
        assert_eq!(0, seq);

        // Sequence number should increase by 1
        assert_eq!(1, wal.sequence_number);

        // Write 2 entries then sync
        let data = Vec::from("other");
        let data = WritePayload::new(data)?;
        let seq = wal.append(data)?;
        assert_eq!(1, seq);

        let data = Vec::from("again");
        let data = WritePayload::new(data)?;
        let seq = wal.append(data)?;
        assert_eq!(2, seq);
        wal.sync_all()?;

        // Sequence number should increase by 2
        assert_eq!(3, wal.sequence_number);

        Ok(())
    }
}
