//! Write buffer that uses files to encode messages.
//!
//! This implementation can be used by multiple readers and writers at the same time. It is ideal for local end2end
//! testing. However it might not perform extremely well when dealing with large messages and (currently) does not
//! implement any message pruning.
//!
//! # Format
//! Given a root path, the database name and the number of sequencers, the directory structure looks like this:
//!
//! ```text
//! <root>/<db_name>/
//!                 /active                              | Location of current state
//!                 : |
//!                 : |
//!                 : | symlink
//!                 : |
//!                 : +--------+
//!                 :          |
//!                 :          |
//!                 :          |
//!                 /version/  V
//!                         /<uuid>/
//!                         :      /0/
//!                         :      : /committed/0        \
//!                         :      : :         /1        | Message files
//!                         :      : :         /2        | (finished)
//!                         :      : :          ...      /
//!                         :      : :
//!                         :      : :
//!                         :      : /temp/<uuid>        \
//!                         :      :      /<uuid>        | Message files
//!                         :      :      /<uuid>        | (to be committed)
//!                         :      :       ...           /
//!                         :      :
//!                         :      :
//!                         :      /1/...                \
//!                         :      /2/...                | More sequencers
//!                         :      ...                   /
//!                         :
//!                         :
//!                         /<uuid>/                     \
//!                         /<uuid>/                     | Incomplete initialization attempts
//!                         ...                          /
//! ```
//!
//! Every message file then uses an HTTP-inspired format:
//!
//! ```text
//! <header_1>: <value_1>
//! <header_2>: <value_2>
//! ...
//! <header_n>: <value_n>
//!
//! <payload>
//! ```
//!
//! The payload is binary data. The headers contain metadata about it (like timestamp, format, tracing information).
//!
//!
//! # Implementation Notes
//! Some notes about file system functionality that shaped this implementation
//!
//! ## Atomic File Creation
//! It is quite easy to create a file and ensure that it did not exist beforehand using [`open(2)`] together with
//! `O_CREAT` and `O_EXCL`. However writing actual content to that file requires time and a reader could already see an
//! incomplete version of that. A workaround is to use a scratchpad file at a temporary location, write the entire
//! desired content to it and then move the file to the target location. This assumes that the target location and the
//! file content are independent, e.g. that the file itself does not contain the `sequence_number`. Now we need to find
//! a way to make this move operation reliable though.
//!
//! Files can be renamed using [`rename(2)`]. There is the `RENAME_NOREPLACE` flag that prevents that we silently
//! overwrite the target file. This however is only implemented for a handful of filesystems (notable NOT [NFS]). So to
//! use [`rename(2)`] we would need some additional locking.
//!
//! Then there is [`link(2)`] which creates a new link to an existing file. It explicitly states that the target is
//! NEVER overwritten. According to <https://unix.stackexchange.com/a/125946> this should even work properly on [NFS].
//! We then need to use [`unlink(2)`] to clean the scratchpad file.
//!
//! ## Atomic Directory Creation
//! To setup a new sequencer config we need to create the directory structure in an atomic way. Hardlinks don't work for
//! directories, but [`symlink(2)`] does and -- like [`link(2)`] -- does not overwrite existing targets.
//!
//! ## File Locking
//! Instead of atomic operations we could also use file locking. Under Linux there are a few ways this can be archived:
//!
//! - **[`fcntl(2)`] via `F_SETLK`, `F_SETLKW`, `F_GETLK`:** <br />
//!   Works on [NFS], but is process-bound (aka if you have multiple writers within the same process, only one can
//!   acquire the lock).
//! - **[`fcntl(2)`] via `F_OFD_SETLK`, `F_OFD_SETLKW`, `F_OFD_GETLK`:** <br />
//!   Works on [NFS] and is file-descriptor-bound.
//! - **[`flock(2)`]:** <br />
//!   Works on [NFS] but is technically emulated via [`fcntl(2)`] so the latter should probably be preferred.
//!
//! The biggest issue with file locking is what happens when an operation fails while a lock is being held. Either the
//! resulting state is obviously unfinished (e.g. due to some checksum or size mismatch, due to some missing marker) or
//! we would need to implement some form of lock poisoning. Since this can get quite tricky, I have decided that atomic
//! file and directory operations are easier to reason about.
//!
//! ## Message Metadata
//! We are NOT using any file-based metadata (like `mtime` or extended attributes) because they are often broken.
//!
//!
//! [`fcntl(2)`]: https://www.man7.org/linux/man-pages/man2/fcntl.2.html
//! [`flock(2)`]: https://www.man7.org/linux/man-pages/man2/flock.2.html
//! [`link(2)`]: https://man7.org/linux/man-pages/man2/link.2.html
//! [NFS]: https://en.wikipedia.org/wiki/Network_File_System
//! [`open(2)`]: https://man7.org/linux/man-pages/man2/open.2.html
//! [`rename(2)`]: https://man7.org/linux/man-pages/man2/rename.2.html
//! [`symlink(2)`]: https://man7.org/linux/man-pages/man2/symlink.2.html
//! [`unlink(2)`]: https://man7.org/linux/man-pages/man2/unlink.2.html
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::codec::{ContentType, IoxHeaders};
use async_trait::async_trait;
use data_types::{sequence::Sequence, write_buffer::WriteBufferCreationConfig};
use dml::{DmlMeta, DmlOperation, DmlWrite};
use futures::{channel::mpsc::Receiver, FutureExt, SinkExt, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use time::{Time, TimeProvider};
use tokio::task::JoinHandle;
use trace::TraceCollector;
use uuid::Uuid;

use crate::core::{
    FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
    WriteBufferWriting, WriteStream,
};

/// Header used to declare the creation time of the message.
pub const HEADER_TIME: &str = "last-modified";

/// File-based write buffer writer.
#[derive(Debug)]
pub struct FileBufferProducer {
    db_name: String,
    dirs: BTreeMap<u32, PathBuf>,
    time_provider: Arc<dyn TimeProvider>,
}

impl FileBufferProducer {
    /// Create new writer.
    pub async fn new(
        root: &Path,
        database_name: &str,
        creation_config: Option<&WriteBufferCreationConfig>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self, WriteBufferError> {
        let root = root.join(database_name);
        let dirs = maybe_auto_create_directories(&root, creation_config).await?;
        Ok(Self {
            db_name: database_name.to_string(),
            dirs,
            time_provider,
        })
    }
}

#[async_trait]
impl WriteBufferWriting for FileBufferProducer {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        self.dirs.keys().cloned().collect()
    }

    async fn store_write(
        &self,
        sequencer_id: u32,
        write: &DmlWrite,
    ) -> Result<DmlMeta, WriteBufferError> {
        let sequencer_path = self
            .dirs
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown sequencer: {}", sequencer_id).into()
            })?;

        // measure time

        let now = write
            .meta()
            .producer_ts()
            .unwrap_or_else(|| self.time_provider.now());

        // assemble message
        let mut message: Vec<u8> = format!("{}: {}\n", HEADER_TIME, now.to_rfc3339()).into_bytes();
        let iox_headers =
            IoxHeaders::new(ContentType::Protobuf, write.meta().span_context().cloned());

        for (name, value) in iox_headers.headers() {
            message.extend(format!("{}: {}\n", name, value).into_bytes())
        }

        message.extend(b"\n");

        crate::codec::encode_write(&self.db_name, write, &mut message)?;

        // write data to scratchpad file in temp directory
        let temp_file = sequencer_path.join("temp").join(Uuid::new_v4().to_string());
        tokio::fs::write(&temp_file, &message).await?;

        // scan existing files to figure out new sequence number
        let committed = sequencer_path.join("committed");
        let existing_files = scan_dir::<u64>(&committed, FileType::File).await?;
        let mut sequence_number = if let Some(max) = existing_files.keys().max() {
            max.checked_add(1).ok_or_else::<WriteBufferError, _>(|| {
                "Overflow during sequence number calculation"
                    .to_string()
                    .into()
            })?
        } else {
            0
        };

        // try to link scratchpad file to "current" dir
        loop {
            let committed_file = committed.join(sequence_number.to_string());
            if tokio::fs::hard_link(&temp_file, &committed_file)
                .await
                .is_ok()
            {
                break;
            }
            sequence_number = sequence_number
                .checked_add(1)
                .ok_or_else::<WriteBufferError, _>(|| {
                    "Overflow during sequence number calculation"
                        .to_string()
                        .into()
                })?;
        }

        // unlink scratchpad file (and ignore error)
        tokio::fs::remove_file(&temp_file).await.ok();

        Ok(DmlMeta::sequenced(
            Sequence::new(sequencer_id, sequence_number),
            now,
            write.meta().span_context().cloned(),
            message.len(),
        ))
    }

    fn type_name(&self) -> &'static str {
        "file"
    }
}

/// File-based write buffer reader.
#[derive(Debug)]
pub struct FileBufferConsumer {
    dirs: BTreeMap<u32, (PathBuf, Arc<AtomicU64>)>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl FileBufferConsumer {
    /// Create new reader.
    pub async fn new(
        root: &Path,
        database_name: &str,
        creation_config: Option<&WriteBufferCreationConfig>,
        // `trace_collector` has to be a reference due to https://github.com/rust-lang/rust/issues/63033
        trace_collector: Option<&Arc<dyn TraceCollector>>,
    ) -> Result<Self, WriteBufferError> {
        let root = root.join(database_name);
        let dirs = maybe_auto_create_directories(&root, creation_config)
            .await?
            .into_iter()
            .map(|(sequencer_id, path)| (sequencer_id, (path, Arc::new(AtomicU64::new(0)))))
            .collect();
        Ok(Self {
            dirs,
            trace_collector: trace_collector.map(|x| Arc::clone(x)),
        })
    }
}

#[async_trait]
impl WriteBufferReading for FileBufferConsumer {
    fn streams(&mut self) -> BTreeMap<u32, WriteStream<'_>> {
        let mut streams = BTreeMap::default();

        for (sequencer_id, (sequencer_path, next_sequence_number)) in &self.dirs {
            let committed = sequencer_path.join("committed");

            let stream = ConsumerStream::new(
                *sequencer_id,
                committed.clone(),
                Arc::clone(next_sequence_number),
                self.trace_collector.clone(),
            )
            .boxed();

            let fetch_high_watermark = move || {
                let committed = committed.clone();

                let fut = async move { watermark(&committed).await };
                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.insert(
                *sequencer_id,
                WriteStream {
                    stream,
                    fetch_high_watermark,
                },
            );
        }

        streams
    }

    async fn seek(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
    ) -> Result<(), WriteBufferError> {
        let path_and_next_sequence_number = self
            .dirs
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown sequencer: {}", sequencer_id).into()
            })?;
        path_and_next_sequence_number
            .1
            .store(sequence_number, Ordering::SeqCst);

        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "file"
    }
}

#[pin_project(PinnedDrop)]
struct ConsumerStream {
    join_handle: JoinHandle<()>,
    #[pin]
    rx: Receiver<Result<DmlOperation, WriteBufferError>>,
}

impl ConsumerStream {
    fn new(
        sequencer_id: u32,
        path: PathBuf,
        next_sequence_number: Arc<AtomicU64>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Self {
        let (mut tx, rx) = futures::channel::mpsc::channel(1);

        let join_handle = tokio::spawn(async move {
            loop {
                let sequence_number = next_sequence_number.load(Ordering::SeqCst);

                // read file
                let file_path = path.join(sequence_number.to_string());
                let msg = match tokio::fs::read(&file_path).await {
                    Ok(data) => {
                        // decode file
                        let sequence = Sequence {
                            id: sequencer_id,
                            number: sequence_number,
                        };
                        match Self::decode_file(data, sequence, trace_collector.clone()) {
                            Ok(write) => {
                                match next_sequence_number.compare_exchange(
                                    sequence_number,
                                    sequence_number + 1,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                ) {
                                    Ok(_) => {
                                        // can send to output
                                        Ok(write)
                                    }
                                    Err(_) => {
                                        // interleaving change, retry
                                        continue;
                                    }
                                }
                            }
                            Err(e) => Err(e),
                        }
                    }
                    Err(error) => {
                        match error.kind() {
                            std::io::ErrorKind::NotFound => {
                                // figure out watermark and see if there's a gap in the stream
                                if let Ok(watermark) = watermark(&path).await {
                                    // watermark is "last sequence number + 1", so substract 1 before comparing
                                    if watermark.saturating_sub(1) > sequence_number {
                                        // update position
                                        // failures are OK here since we'll re-read this value next round
                                        next_sequence_number
                                            .compare_exchange(
                                                sequence_number,
                                                sequence_number + 1,
                                                Ordering::SeqCst,
                                                Ordering::SeqCst,
                                            )
                                            .ok();
                                        continue;
                                    }
                                };

                                // no gap detected, just wait a bit for new data
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                continue;
                            }
                            _ => {
                                // cannot read file => communicate to user
                                Err(Box::new(error) as WriteBufferError)
                            }
                        }
                    }
                };

                if tx.send(msg).await.is_err() {
                    // Receiver is gone
                    return;
                }
            }
        });

        Self { join_handle, rx }
    }

    fn decode_file(
        mut data: Vec<u8>,
        sequence: Sequence,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<DmlOperation, WriteBufferError> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        match httparse::parse_headers(&data, &mut headers)? {
            httparse::Status::Complete((offset, headers)) => {
                let iox_headers = IoxHeaders::from_headers(
                    headers.iter().map(|header| (header.name, header.value)),
                    trace_collector.as_ref(),
                )?;

                // parse timestamp
                let mut timestamp = None;
                for header in headers {
                    if header.name.eq_ignore_ascii_case(HEADER_TIME) {
                        if let Ok(value) = String::from_utf8(header.value.to_vec()) {
                            if let Ok(time) = Time::from_rfc3339(&value) {
                                timestamp = Some(time);
                            }
                        }
                    }
                }
                let timestamp = if let Some(timestamp) = timestamp {
                    timestamp
                } else {
                    return Err("Timestamp missing".to_string().into());
                };

                // parse entry
                let entry_data = data.split_off(offset);

                crate::codec::decode(&entry_data, iox_headers, sequence, timestamp)
            }
            httparse::Status::Partial => Err("Too many headers".to_string().into()),
        }
    }
}

#[pinned_drop]
impl PinnedDrop for ConsumerStream {
    fn drop(self: Pin<&mut Self>) {
        self.join_handle.abort();
    }
}

impl Stream for ConsumerStream {
    type Item = Result<DmlOperation, WriteBufferError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.rx.poll_next(cx)
    }
}

async fn maybe_auto_create_directories(
    root: &Path,
    creation_config: Option<&WriteBufferCreationConfig>,
) -> Result<BTreeMap<u32, PathBuf>, WriteBufferError> {
    loop {
        // figure out if a active version exists
        let active = root.join("active");
        if tokio::fs::metadata(&active).await.is_ok() {
            // Scan for directories
            let directories = scan_dir(&active, FileType::Dir).await?;

            if directories.is_empty() {
                return Err("Active configuration has zero sequencers."
                    .to_string()
                    .into());
            }
            return Ok(directories);
        }

        // no active config exists
        if let Some(creation_config) = creation_config {
            // create version directory
            let version = root.join("version").join(Uuid::new_v4().to_string());
            tokio::fs::create_dir_all(&version).await?;

            let mut directories = BTreeMap::new();
            for sequencer_id in 0..creation_config.n_sequencers.get() {
                let sequencer_path_in_version = version.join(sequencer_id.to_string());
                tokio::fs::create_dir(&sequencer_path_in_version).await?;

                let committed = sequencer_path_in_version.join("committed");
                tokio::fs::create_dir(&committed).await?;

                let temp = sequencer_path_in_version.join("temp");
                tokio::fs::create_dir(&temp).await?;

                let sequencer_path_in_active = active.join(sequencer_id.to_string());
                directories.insert(sequencer_id, sequencer_path_in_active);
            }

            // symlink active->version
            if tokio::fs::symlink(&version, active).await.is_ok() {
                // linking worked
                return Ok(directories);
            } else {
                // linking did not work, assuming a concurrent initialization process. Remove version and and try again.
                tokio::fs::remove_dir_all(&version).await?;
                continue;
            }
        } else {
            return Err("no sequencers initialized".to_string().into());
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FileType {
    Dir,
    File,
}

async fn scan_dir<T>(
    dir: &Path,
    file_type: FileType,
) -> Result<BTreeMap<T, PathBuf>, WriteBufferError>
where
    T: FromStr + Ord + Send,
{
    let mut results = BTreeMap::new();

    let mut read_dir = tokio::fs::read_dir(dir).await?;
    while let Some(dir_entry) = read_dir.next_entry().await? {
        let path = dir_entry.path();

        let ftype = dir_entry.file_type().await?;
        match file_type {
            FileType::Dir => {
                if !ftype.is_dir() {
                    return Err(format!("'{}' is not a directory", path.display()).into());
                }
            }
            FileType::File => {
                if !ftype.is_file() {
                    return Err(format!("'{}' is not a file", path.display()).into());
                }
            }
        }

        if let Some(sequencer_id) = path
            .file_name()
            .map(|p| p.to_str())
            .flatten()
            .map(|p| p.parse::<T>().ok())
            .flatten()
        {
            results.insert(sequencer_id, path);
        } else {
            return Err(format!("Cannot parse '{}'", path.display()).into());
        }
    }

    Ok(results)
}

async fn watermark(path: &Path) -> Result<u64, WriteBufferError> {
    let files = scan_dir::<u64>(path, FileType::File).await?;
    let watermark = files.keys().max().map(|n| n + 1).unwrap_or(0);
    Ok(watermark)
}

pub mod test_utils {
    use std::path::Path;

    /// Remove specific entry from write buffer.
    pub async fn remove_entry(
        write_buffer_path: &Path,
        database_name: &str,
        sequencer_id: u32,
        sequence_number: u64,
    ) {
        tokio::fs::remove_file(
            write_buffer_path
                .join(database_name)
                .join("active")
                .join(sequencer_id.to_string())
                .join("committed")
                .join(sequence_number.to_string()),
        )
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use dml::test_util::assert_write_op_eq;
    use tempfile::TempDir;
    use trace::RingBufferTraceCollector;

    use crate::core::test_utils::{perform_generic_tests, write, TestAdapter, TestContext};

    use super::test_utils::remove_entry;
    use super::*;

    struct FileTestAdapter {
        tempdir: TempDir,
    }

    impl FileTestAdapter {
        fn new() -> Self {
            Self {
                tempdir: TempDir::new().unwrap(),
            }
        }
    }

    #[async_trait]
    impl TestAdapter for FileTestAdapter {
        type Context = FileTestContext;

        async fn new_context_with_time(
            &self,
            n_sequencers: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            FileTestContext {
                path: self.tempdir.path().to_path_buf(),
                database_name: format!("test_db_{}", Uuid::new_v4()),
                n_sequencers,
                time_provider,
            }
        }
    }

    struct FileTestContext {
        path: PathBuf,
        database_name: String,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
    }

    impl FileTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_sequencers: self.n_sequencers,
                ..Default::default()
            })
        }
    }

    #[async_trait]
    impl TestContext for FileTestContext {
        type Writing = FileBufferProducer;
        type Reading = FileBufferConsumer;

        async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
            FileBufferProducer::new(
                &self.path,
                &self.database_name,
                self.creation_config(creation_config).as_ref(),
                Arc::clone(&self.time_provider),
            )
            .await
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            let trace_collector: Arc<dyn TraceCollector> =
                Arc::new(RingBufferTraceCollector::new(5));

            FileBufferConsumer::new(
                &self.path,
                &self.database_name,
                self.creation_config(creation_config).as_ref(),
                Some(&trace_collector),
            )
            .await
        }
    }

    #[tokio::test]
    async fn test_generic() {
        perform_generic_tests(FileTestAdapter::new()).await;
    }

    #[tokio::test]
    async fn test_ignores_missing_files_multi() {
        let adapter = FileTestAdapter::new();
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let sequencer_id = writer.sequencer_ids().into_iter().next().unwrap();
        let entry_1 = "upc,region=east user=1 100";
        let entry_2 = "upc,region=east user=2 200";
        let entry_3 = "upc,region=east user=3 300";
        let entry_4 = "upc,region=east user=4 400";

        let w1 = write(&writer, entry_1, sequencer_id, None).await;
        let w2 = write(&writer, entry_2, sequencer_id, None).await;
        let w3 = write(&writer, entry_3, sequencer_id, None).await;
        let w4 = write(&writer, entry_4, sequencer_id, None).await;

        remove_entry(
            &ctx.path,
            &ctx.database_name,
            sequencer_id,
            w2.meta().sequence().unwrap().number,
        )
        .await;
        remove_entry(
            &ctx.path,
            &ctx.database_name,
            sequencer_id,
            w3.meta().sequence().unwrap().number,
        )
        .await;

        let mut reader = ctx.reading(true).await.unwrap();
        let mut stream = reader.streams().remove(&sequencer_id).unwrap();

        assert_write_op_eq(&stream.stream.next().await.unwrap().unwrap(), &w1);
        assert_write_op_eq(&stream.stream.next().await.unwrap().unwrap(), &w4);
    }

    #[tokio::test]
    async fn test_ignores_missing_files_single() {
        let adapter = FileTestAdapter::new();
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let sequencer_id = writer.sequencer_ids().into_iter().next().unwrap();
        let entry_1 = "upc,region=east user=1 100";
        let entry_2 = "upc,region=east user=2 200";

        let w1 = write(&writer, entry_1, sequencer_id, None).await;
        let w2 = write(&writer, entry_2, sequencer_id, None).await;

        remove_entry(
            &ctx.path,
            &ctx.database_name,
            sequencer_id,
            w1.meta().sequence().unwrap().number,
        )
        .await;

        let mut reader = ctx.reading(true).await.unwrap();
        let mut stream = reader.streams().remove(&sequencer_id).unwrap();

        assert_write_op_eq(&stream.stream.next().await.unwrap().unwrap(), &w2);
    }
}
