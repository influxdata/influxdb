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
//!                 /cur                       | Location of current state
//!                 : |
//!                 : |
//!                 : | symlink
//!                 : |
//!                 : +----+
//!                 :      |
//!                 :      |
//!                 :      |
//!                 /pad/  V
//!                     /<uuid>/
//!                     :      /0/
//!                     :      : /cur/0         \
//!                     :      : :   /1         | Message files
//!                     :      : :   /2         | (finished)
//!                     :      : :    ...       /
//!                     :      : :
//!                     :      : :
//!                     :      : /pad/<uuid>    \
//!                     :      :     /<uuid>    | Message files
//!                     :      :     /<uuid>    | (to be committed)
//!                     :      :      ...       /
//!                     :      :
//!                     :      :
//!                     :      /1/...           \
//!                     :      /2/...           | More sequencers
//!                     :      ...              /
//!                     :
//!                     :
//!                     /<uuid>/                \
//!                     /<uuid>/                | Incomplete initialization attempts
//!                     ...                     /
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
    convert::TryFrom,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use data_types::{database_rules::WriteBufferCreationConfig, sequence::Sequence};
use entry::{Entry, SequencedEntry};
use futures::{channel::mpsc::Receiver, FutureExt, SinkExt, Stream, StreamExt};
use http::{header::HeaderName, HeaderMap, HeaderValue};
use pin_project::{pin_project, pinned_drop};
use time::{Time, TimeProvider};
use tokio::task::JoinHandle;
use trace::{ctx::SpanContext, TraceCollector};
use trace_http::ctx::{format_jaeger_trace_context, TraceHeaderParser};
use uuid::Uuid;

use crate::core::{
    EntryStream, FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
    WriteBufferWriting,
};

/// Header used to declare the content type of the message.
pub const HEADER_CONTENT_TYPE: &str = "content-type";

/// Header used to declare the creation time of the message.
pub const HEADER_TIME: &str = "last-modified";

/// Header used to declare the trace context (optional).
pub const HEADER_TRACE_CONTEXT: &str = "uber-trace-id";

/// Current flatbuffer-based content type.
///
/// This is a value for [`HEADER_CONTENT_TYPE`].
///
/// Inspired by:
/// - <https://stackoverflow.com/a/56502135>
/// - <https://stackoverflow.com/a/48051331>
pub const CONTENT_TYPE_FLATBUFFER: &str =
    r#"application/x-flatbuffers; schema="influxdata.iox.write.v1.Entry""#;

/// File-based write buffer writer.
#[derive(Debug)]
pub struct FileBufferProducer {
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

    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
        span_context: Option<&SpanContext>,
    ) -> Result<(Sequence, Time), WriteBufferError> {
        let sequencer_path = self
            .dirs
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown sequencer: {}", sequencer_id).into()
            })?;

        // measure time
        let now = self.time_provider.now();

        // assemble message
        let mut message: Vec<u8> = format!(
            "{}: {}\n{}: {}\n",
            HEADER_CONTENT_TYPE,
            CONTENT_TYPE_FLATBUFFER,
            HEADER_TIME,
            now.to_rfc3339(),
        )
        .into_bytes();
        if let Some(span_context) = span_context {
            message.extend(
                format!(
                    "{}: {}\n",
                    HEADER_TRACE_CONTEXT,
                    format_jaeger_trace_context(span_context),
                )
                .into_bytes(),
            )
        }
        message.extend(b"\n");
        message.extend(entry.data());

        // write data to scratchpad file
        let pad_file = sequencer_path.join("pad").join(Uuid::new_v4().to_string());
        tokio::fs::write(&pad_file, &message).await?;

        // scan existing files to figure out new sequence number
        let cur = sequencer_path.join("cur");
        let existing_files = scan_dir::<u64>(&cur, FileType::File).await?;
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
            let cur_file = cur.join(sequence_number.to_string());
            if tokio::fs::hard_link(&pad_file, &cur_file).await.is_ok() {
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
        tokio::fs::remove_file(&pad_file).await.ok();

        Ok((Sequence::new(sequencer_id, sequence_number), now))
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
    fn streams(&mut self) -> BTreeMap<u32, EntryStream<'_>> {
        let mut streams = BTreeMap::default();

        for (sequencer_id, (sequencer_path, next_sequence_number)) in &self.dirs {
            let cur = sequencer_path.join("cur");

            let stream = ConsumerStream::new(
                *sequencer_id,
                cur.clone(),
                Arc::clone(next_sequence_number),
                self.trace_collector.clone(),
            )
            .boxed();

            let fetch_high_watermark = move || {
                let cur = cur.clone();

                let fut = async move {
                    let files = scan_dir::<u64>(&cur, FileType::File).await?;
                    let watermark = files.keys().max().map(|n| n + 1).unwrap_or(0);

                    Ok(watermark)
                };
                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.insert(
                *sequencer_id,
                EntryStream {
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
    rx: Receiver<Result<SequencedEntry, WriteBufferError>>,
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
                let data = match tokio::fs::read(&file_path).await {
                    Ok(data) => data,
                    Err(_) => {
                        // just wait a bit
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };

                // decode file
                let sequence = Sequence {
                    id: sequencer_id,
                    number: sequence_number,
                };
                let msg = match Self::decode_file(data, sequence, trace_collector.clone()) {
                    Ok(sequence) => {
                        match next_sequence_number.compare_exchange(
                            sequence_number,
                            sequence_number + 1,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {
                                // can send to output
                                Ok(sequence)
                            }
                            Err(_) => {
                                // interleaving change, retry
                                continue;
                            }
                        }
                    }
                    e => e,
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
    ) -> Result<SequencedEntry, WriteBufferError> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        match httparse::parse_headers(&data, &mut headers)? {
            httparse::Status::Complete((offset, headers)) => {
                // parse content type
                let mut content_type = None;
                for header in headers {
                    if header.name.eq_ignore_ascii_case(HEADER_CONTENT_TYPE) {
                        if let Ok(s) = String::from_utf8(header.value.to_vec()) {
                            content_type = Some(s);
                        }
                    }
                }
                if let Some(content_type) = content_type {
                    if content_type != CONTENT_TYPE_FLATBUFFER {
                        return Err(format!("Unknown content type: {}", content_type).into());
                    }
                } else {
                    return Err("Content type missing".to_string().into());
                }

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

                // parse span context
                let mut span_context = None;
                if let Some(trace_collector) = trace_collector {
                    let mut header_map = HeaderMap::with_capacity(headers.len());
                    for header in headers {
                        if let (Ok(header_name), Ok(header_value)) = (
                            HeaderName::from_str(header.name),
                            HeaderValue::from_bytes(header.value),
                        ) {
                            header_map.insert(header_name, header_value);
                        }
                    }
                    let parser = TraceHeaderParser::new()
                        .with_jaeger_trace_context_header_name(HEADER_TRACE_CONTEXT);
                    span_context = parser.parse(&trace_collector, &header_map).ok().flatten();
                }

                // parse entry
                let entry_data = data.split_off(offset);
                let entry = Entry::try_from(entry_data)?;

                Ok(SequencedEntry::new_from_sequence_and_span_context(
                    sequence,
                    timestamp,
                    entry,
                    span_context,
                ))
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
    type Item = Result<SequencedEntry, WriteBufferError>;

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
        // figure out if a current version exists
        let cur = root.join("cur");
        if tokio::fs::metadata(&cur).await.is_ok() {
            // Scan for directories
            let directories = scan_dir(&cur, FileType::Dir).await?;

            if directories.is_empty() {
                return Err("Current configuration has zero sequencers."
                    .to_string()
                    .into());
            }
            return Ok(directories);
        }

        // no current config exists
        if let Some(creation_config) = creation_config {
            // create scratchpad directory
            let pad = root.join("pad").join(Uuid::new_v4().to_string());
            tokio::fs::create_dir_all(&pad).await?;

            let mut directories = BTreeMap::new();
            for sequencer_id in 0..creation_config.n_sequencers.get() {
                let sequencer_path_in_pad = pad.join(sequencer_id.to_string());
                tokio::fs::create_dir(&sequencer_path_in_pad).await?;

                let sequencer_path_in_pad_cur = sequencer_path_in_pad.join("cur");
                tokio::fs::create_dir(&sequencer_path_in_pad_cur).await?;

                let sequencer_path_in_pad_pad = sequencer_path_in_pad.join("pad");
                tokio::fs::create_dir(&sequencer_path_in_pad_pad).await?;

                let sequencer_path_in_cur = cur.join(sequencer_id.to_string());
                directories.insert(sequencer_id, sequencer_path_in_cur);
            }

            // symlink cur->pad
            if tokio::fs::symlink(&pad, cur).await.is_ok() {
                // linking worked
                return Ok(directories);
            } else {
                // linking did not work, assuming a concurrent initialization process. Remove scratchpad and and try again
                tokio::fs::remove_dir_all(&pad).await?;
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tempfile::TempDir;
    use trace::RingBufferTraceCollector;

    use crate::core::test_utils::{perform_generic_tests, TestAdapter, TestContext};

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
}
