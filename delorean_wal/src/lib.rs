#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, missing_copy_implementations)]
#![warn(missing_docs)]

//! # delorean_wal
//!
//! This crate provides a WAL tailored for delorean `Partition`s to optionally use.
//!
//! Work remaining:
//!
//! - Entry sequence number
//! - File rollover
//! - Metadata file
//! - Atomic remove of entries
//! - Tracking of the total size of all segments in the log
//! - More testing for correctness; the existing tests mostly demonstrate possible usages.
//! - Error handling

use arc_swap::ArcSwap;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Bytes, BytesMut};
use crc32fast::Hasher;
use snafu::{ensure, ResultExt, Snafu};
use std::{
    convert::TryFrom,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    iter, num,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
};

/// Opaque public `Error` type
#[derive(Debug, Snafu, Clone)]
pub struct Error(InternalError);

#[derive(Debug, Snafu, Clone)]
enum InternalError {
    UnableToReadChecksum {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToReadLength {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToReadData {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
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

    UnableToWriteChecksum {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToWriteLength {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToWriteData {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToSync {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToOpenFile {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        path: PathBuf,
    },

    UnableToCreateFile {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        path: PathBuf,
    },

    UnableToCopyFileContents {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        src: PathBuf,
        dst: PathBuf,
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
}

impl WalBuilder {
    /// Create a new WAL rooted at the provided directory on disk.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        // TODO: Error if `root` is not a directory?
        let root = root.into();
        Self { root }
    }

    /// Consume the builder and create a `Wal`.
    ///
    /// # Generics
    ///
    /// - `N` is a type that will be returned when this batch is synchronized to
    ///   disk (or attempted to). It should be cheaply clonable.
    pub fn wal<N: Clone>(self) -> Wal<N> {
        Wal::new(self.file_locator())
    }

    /// Consume the builder to get an iterator of all entries in this
    /// WAL that have been persisted to disk.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    pub fn entries(self) -> Result<impl Iterator<Item = Result<Entry>>> {
        Loader::load(self.file_locator())
    }

    fn file_locator(self) -> FileLocator {
        FileLocator { root: self.root }
    }
}

/// The main WAL type to interact with.
///
/// Can be used in single-threaded, multi-threaded, and asynchronous contexts.
///
/// # Example
///
/// This demonstrates using the WAL with the Tokio asynchronous runtime.
///
/// ```
/// # fn example(root_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
/// use delorean_wal::WalBuilder;
/// use futures::{SinkExt, StreamExt, channel::mpsc};
/// use std::{io::Write, time::Duration};
/// use tokio::{task, time};
///
/// // This value should be cloned and used across multiple tasks, such as per
/// // HTTP request.
/// let wal = WalBuilder::new(root_path).wal::<mpsc::Sender<delorean_wal::Result<_>>>();
///
/// // We will mimic a HTTP request as a Tokio task.
/// // There can be many of these.
/// let wal_for_http_request = wal.clone();
/// tokio::spawn(async move {
///     let mut batch = wal_for_http_request.append();
///     batch.write_all(b"Pretend this came from the HTTP request").unwrap();
///
///     // The task that periodically syncs will notify us through this channel
///     // once our bytes are safely on disk.
///     let (batch_synced_tx, mut batch_synced_rx) = mpsc::channel(1);
///     batch.finalize(batch_synced_tx);
///
///     // Once this future resolves, we know the data has been written to disk
///     batch_synced_rx.next().await.unwrap().unwrap();
/// });
///
/// let wal_for_syncing = wal;
/// tokio::spawn(async move {
///     loop {
///         tokio::time::delay_for(Duration::from_millis(100)).await;
///
///         // Syncing the WAL performs **blocking IO** and we need to notify
///         // the asynchronous runtime about that.
///         let (tasks_to_notify, outcome) = task::block_in_place(|| wal_for_syncing.sync());
///
///         for mut waiting_http_task in tasks_to_notify {
///             waiting_http_task.send(outcome.clone()).await.unwrap();
///         }
///     }
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Wal<N>
where
    N: Clone,
{
    files: FileLocator,
    pending: Arc<ArcSwap<Vec<Pending<N>>>>,
}

impl<N> Wal<N>
where
    N: Clone,
{
    fn new(files: FileLocator) -> Self {
        Self {
            files,
            pending: Default::default(),
        }
    }

    /// Start appending data to this WAL.
    ///
    /// It is required to call `finalize` on the `Append` instance when the
    /// appending is completed, or the written data will not be synced and will
    /// be lost.
    ///
    /// # Asynchronous considerations
    ///
    /// This method may be called in an asynchronous context with no special
    /// handling.
    pub fn append(&self) -> Append<'_, N> {
        Append::new(self)
    }

    // TODO: Maybe a third struct?
    /// Flush all pending bytes to disk.
    ///
    /// Returns clients that can be notified that their data has been persisted.
    /// Intended to be called periodically to batch disk operations.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    pub fn sync(&self) -> (Vec<N>, Result<()>) {
        // Atomically get all pending operations and start a new list.
        let pending = self.pending.swap(Default::default());
        let pending = &**pending;

        let to_notify: Vec<_> = pending.iter().map(|p| N::clone(&p.notify)).collect();

        let outcome = (move || {
            // Get the file to write to
            let mut file = self.files.open_file_for_append()?;

            // TODO: Would a user ever wish to resume or retry some failed IO?
            for pending in pending {
                let Pending {
                    checksum,
                    data,
                    len,
                    ..
                } = pending;

                // TODO: also write a sequence number
                file.write_u32::<LittleEndian>(*checksum)
                    .context(UnableToWriteChecksum)?;
                file.write_u32::<LittleEndian>(*len)
                    .context(UnableToWriteLength)?;

                file.write_all(&data).context(UnableToWriteData)?;
            }

            file.sync_all_and_rename().context(UnableToSync)?;

            Ok(())
        })();

        (to_notify, outcome)
    }

    /// # Asynchronous considerations
    ///
    /// This method may be called in an asynchronous context with no special
    /// handling.
    fn append_pending(&self, pending: Pending<N>) {
        self.pending.rcu(|p| {
            let mut p = Vec::<Pending<_>>::clone(p);
            p.push(Pending::clone(&pending));
            p
        });
    }
}

// Manages files within the WAL directory
#[derive(Debug, Clone)]
struct FileLocator {
    root: PathBuf,
}

impl FileLocator {
    const EXTENSION: &'static str = "db";
    const TEMP_EXTENSION: &'static str = "tmpdb";

    fn open_file_for_read(&self) -> Result<File> {
        // Currently only uses one file named `wal.db`, TODO: file rollover
        let mut path = self.root.join("wal");
        path.set_extension(Self::EXTENSION);

        OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path)
            .context(UnableToOpenFile { path })
            .map_err(Into::into)
    }

    fn open_file_for_append(&self) -> Result<SideFile> {
        // TODO: create directories?

        // File where data will end up eventually
        let mut final_destination = self.root.join("wal");
        final_destination.set_extension(Self::EXTENSION);

        // Temporary file for the purposes of doing an atomic-ish write
        let mut temporary_destination = final_destination.clone();
        temporary_destination.set_extension(Self::TEMP_EXTENSION);

        SideFile::new(final_destination, temporary_destination)
    }
}

/// Provides atomic file writing capabilities.
///
/// It is *required* to call `SideFile::sync_all_and_rename` when done with the
/// file to ensure that the appending happens.
///
/// # Discussion
///
/// Operating systems / filesystems don't provide APIs for atomic writes. The
/// workaround is to copy the existing file to a temporary location, perform
/// non-atomic writes, then rename the temporary file over the original.
///
/// File creation and renaming are atomic, so consumers of the non-temporary
/// files will only ever see the data there or not.
///
/// Without this mechanism, a consumer could see a file in a state where we've
/// written some but not all of the data and the file would look corrupted or
/// otherwise be invalid.
#[derive(Debug)]
struct SideFile {
    final_destination: PathBuf,
    temporary_destination: PathBuf,
    temporary_file: File,
}

impl SideFile {
    fn new(final_destination: PathBuf, temporary_destination: PathBuf) -> Result<Self> {
        // Creating the temporary file will fail if there's already an existing
        // temp file with this name. This should never happen as long as only
        // one task is performing syncing.
        let mut temporary_file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(&temporary_destination)
            .context(UnableToCreateFile {
                path: &temporary_destination,
            })?;

        // Don't worry about errors opening the original file to read from; it
        // might not exist if no data has been written to the WAL.
        if let Ok(mut src) = File::open(&final_destination) {
            // Copy the contents from the existing file to the temp file.
            io::copy(&mut src, &mut temporary_file).context(UnableToCopyFileContents {
                src: &final_destination,
                dst: &temporary_destination,
            })?;
        }

        Ok(SideFile {
            final_destination,
            temporary_destination,
            temporary_file,
        })
    }

    fn sync_all_and_rename(self) -> io::Result<()> {
        self.temporary_file.sync_all()?;
        fs::rename(self.temporary_destination, self.final_destination)
    }
}

impl Deref for SideFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.temporary_file
    }
}

impl DerefMut for SideFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.temporary_file
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
    fn load(files: FileLocator) -> Result<impl Iterator<Item = Result<Entry>>> {
        let mut file = files.open_file_for_read()?;
        Ok(iter::from_fn(move || Self::load_one(&mut file).transpose()))
    }

    fn load_one(file: &mut File) -> Result<Option<Entry>> {
        let expected_checksum = match file.read_u32::<LittleEndian>() {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            e => e.context(UnableToReadChecksum)?,
        };
        let expected_len = file
            .read_u32::<LittleEndian>()
            .context(UnableToReadLength)?;

        let expected_len_us = usize::try_from(expected_len)
            .expect("Only designed to run on 32-bit systems or higher");

        let mut data = Vec::with_capacity(expected_len_us);
        let actual_len = file
            .take(u64::from(expected_len))
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
            expected_checksum == actual_checksum,
            ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum
            }
        );

        Ok(Some(Entry { data }))
    }
}

/// One batch of data read from the WAL.
///
/// This corresponds to one call to `Wal::append`.
#[derive(Debug)]
pub struct Entry {
    data: Vec<u8>,
}

impl Entry {
    /// Gets a reference to the entry's data
    pub fn as_data(&self) -> &[u8] {
        &self.data
    }

    /// Gets the entry's data
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

/// One batch of data waiting to be appended to the current WAL file.
///
/// # Generics
///
/// - `N` is a type that will be returned when this batch is synchronized to
///   disk (or attempted to). It should be cheaply clonable.
#[derive(Debug, Clone)]
struct Pending<N>
where
    N: Clone,
{
    checksum: u32,
    data: Bytes,
    len: u32,
    notify: N,
}

/// An in-memory batch of writes to treat atomically.
///
/// Use the `io::Write` interface and then call `finalize` to signify a batch's
/// end.
///
/// # Generics
///
/// - `N` is a type that will be returned when this batch is synchronized to
///   disk (or attempted to). It should be cheaply clonable.
#[derive(Debug)]
pub struct Append<'a, N>
where
    N: Clone,
{
    wal: &'a Wal<N>,
    buffer: BytesMut,
}

impl<'a, N> Append<'a, N>
where
    N: Clone,
{
    fn new(wal: &'a Wal<N>) -> Self {
        Self {
            wal,
            buffer: Default::default(),
        }
    }

    /// Signal that writing of the batch has completed.
    ///
    /// # Asynchronous considerations
    ///
    /// This function may be called in an asynchronous context with no special
    /// handling.
    pub fn finalize(self, notify: N) -> Result<()> {
        let data = self.buffer.freeze();

        // Only designed to support chunks up to `u32::max` bytes long.
        let len = data.len();
        let len = u32::try_from(len).context(ChunkSizeTooLarge { actual: len })?;

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        let pending = Pending {
            checksum,
            data,
            len,
            notify,
        };

        self.wal.append_pending(pending);
        Ok(())
    }
}

impl<'a, N> Write for Append<'a, N>
where
    N: Clone,
{
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn side_file_basic() -> Result {
        let dir = delorean_test_helpers::tmp_dir()?;
        let existing_path = dir.as_ref().join("existing");
        let temp_path = dir.as_ref().join("temp");

        let mut side_file = SideFile::new(existing_path.clone(), temp_path)?;

        side_file.write_all(b"hello")?;
        side_file.write_all(b"world")?;

        side_file.sync_all_and_rename()?;

        let s = fs::read_to_string(existing_path)?;

        assert_eq!("helloworld", s);

        Ok(())
    }
}
