//! This module is responsible for writing the given data to the specified
//! object store and reading it back.

use crate::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    serialize::{self, CodecError},
    ParquetFilePath,
};
use arrow::{
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::{AdapterStream, AutoAbortJoinHandle};
use futures::{Stream, TryStreamExt};
use object_store::{DynObjectStore, GetResult};
use observability_deps::tracing::*;
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::reader::SerializedFileReader,
};
use pin_project::{pin_project, pinned_drop};
use predicate::Predicate;
use rayon::{ThreadPool, ThreadPoolBuilder};
use schema::selection::Selection;
use std::{
    future::Future,
    io::SeekFrom,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

/// Errors returned during a Parquet "put" operation, covering [`RecordBatch`]
/// pull from the provided stream, encoding, and finally uploading the bytes to
/// the object store.
#[derive(Debug, Error)]
pub enum UploadError {
    /// A codec failure during serialisation.
    #[error(transparent)]
    Serialise(#[from] CodecError),

    /// An error during Parquet metadata conversion when attempting to
    /// instantiate a valid [`IoxParquetMetaData`] instance.
    #[error("failed to construct IOx parquet metadata: {0}")]
    Metadata(crate::metadata::Error),

    /// Uploading the Parquet file to object store failed.
    #[error("failed to upload to object storage: {0}")]
    Upload(#[from] object_store::Error),
}

/// Errors during Parquet file download & scan.
#[derive(Debug, Error)]
pub enum ReadError {
    /// Failed to create the temporary Parquet file on disk to which the
    /// downloaded parquet bytes will be spilled.
    #[error("failed to create temporary file: {0}")]
    TempFile(std::io::Error),

    /// Error writing the bytes fetched from object store to the temporary
    /// parquet file on disk.
    #[error("i/o error writing downloaded parquet: {0}")]
    IO(#[from] std::io::Error),

    /// An error fetching Parquet file bytes from object store.
    #[error("failed to read data from object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// An error reading the downloaded Parquet file.
    #[error("invalid parquet file: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Cannot poll arrow blocking wrapper
    #[error("cannot poll arrow blocking wrapper: {0}")]
    Poll(#[from] tokio::sync::oneshot::error::RecvError),
}

/// The [`ParquetStorage`] type encapsulates [`RecordBatch`] persistence to an
/// underlying [`ObjectStore`].
///
/// [`RecordBatch`] instances are serialized to Parquet files, with IOx specific
/// metadata ([`IoxParquetMetaData`]) attached.
///
/// Code that interacts with Parquet files in object storage should utilise this
/// type that encapsulates the storage & retrieval implementation.
///
/// [`ObjectStore`]: object_store::ObjectStore
#[derive(Debug, Clone)]
pub struct ParquetStorage {
    object_store: Arc<DynObjectStore>,
    parquet_io_threadpool: Arc<ThreadPool>,
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        let parquet_io_threadpool = Arc::new(
            ThreadPoolBuilder::new()
                .num_threads(8)
                .thread_name(|i| format!("parquet IO {i}"))
                .build()
                .expect("cannot build parquet IO threadpool"),
        );
        Self {
            object_store,
            parquet_io_threadpool,
        }
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// # Retries
    ///
    /// This method retries forever in the presence of object store errors. All
    /// other errors are returned as they occur.
    pub async fn upload<S>(
        &self,
        batches: S,
        meta: &IoxMetadata,
    ) -> Result<(IoxParquetMetaData, usize), UploadError>
    where
        S: Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    {
        // Stream the record batches into a parquet file.
        //
        // It would be nice to stream the encoded parquet to disk for this and
        // eliminate the buffering in memory, but the lack of a streaming object
        // store put negates any benefit of spilling to disk.
        //
        // This is not a huge concern, as the resulting parquet files are
        // currently smallish on average.
        let (data, parquet_file_meta) = serialize::to_parquet_bytes(batches, meta).await?;
        // TODO: remove this if after verifying the panic is thrown
        // correctly inside the serialize::to_parquet_bytes above
        if parquet_file_meta.row_groups.is_empty() {
            debug!(
                ?meta.partition_id, ?parquet_file_meta,
                "Created parquet_file_meta has no row groups which will introduce panic later when its statistics is read");
        }

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        debug!(
            ?meta.partition_id,
            ?parquet_meta,
            "IoxParquetMetaData coverted from Row Group Metadata (aka FileMetaData)"
        );

        // Derive the correct object store path from the metadata.
        let path = ParquetFilePath::from(meta).object_store_path();

        let file_size = data.len();
        let data = Bytes::from(data);

        // Retry uploading the file endlessly.
        //
        // This is abort-able by the user by dropping the upload() future.
        //
        // Cloning `data` is a ref count inc, rather than a data copy.
        while let Err(e) = self.object_store.put(&path, data.clone()).await {
            error!(error=%e, ?meta, "failed to upload parquet file to object storage");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok((parquet_meta, file_size))
    }

    /// Pull the Parquet-encoded [`RecordBatch`] at the file path derived from
    /// the provided [`ParquetFilePath`].
    ///
    /// The `selection` projection is pushed down to the Parquet deserializer.
    ///
    /// This impl fetches the associated Parquet file bytes from object storage,
    /// temporarily persisting them to a local temp file to feed to the arrow
    /// reader.
    ///
    /// No caching is performed by `read_filter()`, and each call to
    /// `read_filter()` will re-download the parquet file unless the underlying
    /// object store impl caches the fetched bytes.
    pub fn read_filter(
        &self,
        _predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        meta: &IoxMetadata,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        let path = ParquetFilePath::from(meta).object_store_path();
        trace!(path=?path, "fetching parquet data for filtered read");

        // Indices of columns in the schema needed to read
        let projection: Vec<usize> = column_indices(selection, Arc::clone(&schema));

        // Compute final (output) schema after selection
        let schema = Arc::new(Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        ));

        let (tx, rx) = tokio::sync::mpsc::channel(2);

        // Run async dance here to make sure any error returned
        // `download_and_scan_parquet` is sent back to the reader and
        // not silently ignored
        let object_store = Arc::clone(&self.object_store);
        let thread_pool = Arc::clone(&self.parquet_io_threadpool);
        let handle = tokio::task::spawn(async move {
            let download_result =
                download_and_scan_parquet(projection, path, object_store, tx.clone(), thread_pool)
                    .await;

            // If there was an error returned from download_and_scan_parquet send it back to the receiver.
            if let Err(e) = download_result {
                warn!(error=%e, "Parquet download & scan failed");
                let e = ArrowError::ExternalError(Box::new(e));
                if let Err(e) = tx.send(ArrowResult::Err(e)).await {
                    // if no one is listening, there is no one else to hear our screams
                    debug!(%e, "Error sending result of download function. Receiver is closed.");
                }
            }
        });

        // returned stream simply reads off the rx channel
        Ok(AdapterStream::adapt(
            schema,
            rx,
            Some(Arc::new(AutoAbortJoinHandle::new(handle))),
        ))
    }
}

/// Return indices of the schema's fields of the selection columns
fn column_indices(selection: Selection<'_>, schema: SchemaRef) -> Vec<usize> {
    let fields = schema.fields().iter();

    match selection {
        Selection::Some(cols) => fields
            .enumerate()
            .filter_map(|(p, x)| {
                if cols.contains(&x.name().as_str()) {
                    Some(p)
                } else {
                    None
                }
            })
            .collect(),
        Selection::All => fields.enumerate().map(|(p, _)| p).collect(),
    }
}

/// Downloads the specified parquet file to a local temporary file
/// and push the [`RecordBatch`] contents over `tx`, projecting the specified
/// column indexes.
///
/// This call MAY download a parquet file from object storage, temporarily
/// spilling it to disk while it is processed.
async fn download_and_scan_parquet(
    projection: Vec<usize>,
    path: object_store::path::Path,
    object_store: Arc<DynObjectStore>,
    tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
    thread_pool: Arc<ThreadPool>,
) -> Result<(), ReadError> {
    let read_stream = object_store.get(&path).await?;

    let file = match read_stream {
        GetResult::File(f, _) => {
            trace!(?path, "Using file directly");
            f.into_std().await
        }
        GetResult::Stream(mut read_stream) => {
            // read parquet file to local file
            let mut file = File::from_std(tempfile::tempfile().map_err(ReadError::TempFile)?);

            trace!(?path, ?file, "Beginning to read parquet to temp file");

            while let Some(bytes) = read_stream.try_next().await? {
                trace!(len = bytes.len(), "read bytes from object store");
                file.write_all(&bytes).await?;
            }

            file.seek(SeekFrom::Start(0)).await?;

            let file = file.into_std().await;
            trace!(?path, "Completed read parquet to tempfile");
            file
        }
    };

    ParquetBlockingReader::new(file, tx, projection, thread_pool)?.await?;
    debug!(?path, "Completed parquet download & scan");

    Ok(())
}

/// Helper to ensure that arrows file->RecordBatch parquet IO is done in a dedicated IO thread.
///
/// This deliberately does NOT use tokio's `spawn_blocking` because this might steal too many threads from our query executor.
#[pin_project(PinnedDrop)]
struct ParquetBlockingReader {
    cancelled: Arc<AtomicBool>,

    #[pin]
    finished: tokio::sync::oneshot::Receiver<Result<(), ReadError>>,
}

impl ParquetBlockingReader {
    fn new(
        file: std::fs::File,
        tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
        projection: Vec<usize>,
        thread_pool: Arc<ThreadPool>,
    ) -> Result<Self, ReadError> {
        let (tx_finished, finished) = tokio::sync::oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_captured = Arc::clone(&cancelled);

        thread_pool.spawn(move || {
            // early cancelleation check since this task may have been within the queue for a while
            if cancelled_captured.load(Ordering::SeqCst) {
                return;
            }

            let res = Self::inner(file, tx, projection, cancelled_captured);
            tx_finished.send(res).ok();
        });

        Ok(Self {
            finished,
            cancelled,
        })
    }

    fn inner(
        file: std::fs::File,
        tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
        projection: Vec<usize>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<(), ReadError> {
        // Size of each batch
        let batch_size = 1024; // Todo: make a constant or policy for this

        let file_reader = SerializedFileReader::new(file)?;
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
        let record_batch_reader =
            arrow_reader.get_record_reader_by_columns(projection, batch_size)?;

        if cancelled.load(Ordering::SeqCst) {
            return Ok(());
        }

        for batch in record_batch_reader {
            if tx.blocking_send(batch).is_err() {
                debug!("Receiver hung up - exiting");
                break;
            }

            if cancelled.load(Ordering::SeqCst) {
                break;
            }
        }

        Ok(())
    }
}

#[pinned_drop]
impl PinnedDrop for ParquetBlockingReader {
    fn drop(self: std::pin::Pin<&mut Self>) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
}

impl Future for ParquetBlockingReader {
    type Output = Result<(), ReadError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.finished.poll(cx)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, StringBuilder};
    use data_types::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
    use iox_time::Time;

    #[tokio::test]
    async fn test_parquet_round_trip() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = IoxMetadata {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            sequencer_id: SequencerId::new(2),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_id: PartitionId::new(4),
            partition_key: "potato".into(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(11),
            compaction_level: 1,
            sort_key: None,
        };
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        let stream = futures::stream::iter([Ok(batch.clone())]);

        // Serialize & upload the record batches.
        let (file_meta, _file_size) = store
            .upload(stream, &meta)
            .await
            .expect("should serialize and store sucessfully");

        // Extract the various bits of metadata.
        let file_meta = file_meta.decode().expect("should decode parquet metadata");
        let got_iox_meta = file_meta
            .read_iox_metadata_new()
            .expect("should read IOx metadata from parquet meta");

        // Ensure the metadata in the file decodes to the same IOx metadata we
        // provided when uploading.
        assert_eq!(got_iox_meta, meta);

        // Fetch the record batches and compare them to the input batches.
        let rx = store
            .read_filter(&Predicate::default(), Selection::All, schema, &meta)
            .expect("should read record batches from object store");

        // Drain the retrieved record batch stream
        let mut got = datafusion::physical_plan::common::collect(rx)
            .await
            .expect("failed to drain record stream");

        // And compare to the original input
        assert_eq!(got.len(), 1);
        assert_eq!(got.pop().unwrap(), batch);
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let mut builder = StringBuilder::new(strs.len());
        for s in strs {
            builder.append_value(s).expect("appending string");
        }
        Arc::new(builder.finish())
    }
}
