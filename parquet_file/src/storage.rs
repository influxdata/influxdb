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
use datafusion::{parquet::arrow::ProjectionMask, physical_plan::SendableRecordBatchStream};
use datafusion_util::{AdapterStream, AutoAbortJoinHandle};
use futures::{Stream, TryStreamExt};
use object_store::{DynObjectStore, GetResult};
use observability_deps::tracing::*;
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::reader::SerializedFileReader,
};
use predicate::Predicate;
use schema::selection::Selection;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::io::AsyncReadExt;

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

    /// Schema mismatch
    #[error("Schema mismatch (expected VS actual parquet file) for file: {path}")]
    SchemaMismatch {
        /// Path of the affected parquet file.
        path: object_store::path::Path,
    },
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
    /// Underlying object store.
    object_store: Arc<DynObjectStore>,
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self { object_store }
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
        path: &ParquetFilePath,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        let path = path.object_store_path();
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
        let schema_captured = Arc::clone(&schema);
        let handle = tokio::task::spawn(async move {
            let download_result = download_and_scan_parquet(
                projection,
                schema_captured,
                path,
                object_store,
                tx.clone(),
            )
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

    /// Read all data from the parquet file.
    pub fn read_all(
        &self,
        schema: SchemaRef,
        path: &ParquetFilePath,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        self.read_filter(&Predicate::default(), Selection::All, schema, path)
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
    expected_schema: SchemaRef,
    path: object_store::path::Path,
    object_store: Arc<DynObjectStore>,
    tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
) -> Result<(), ReadError> {
    let read_stream = object_store.get(&path).await?;

    let data = match read_stream {
        GetResult::File(mut f, _) => {
            trace!(?path, "Using file directly");
            let l = f.metadata().await?.len();
            let mut buf = Vec::with_capacity(l as usize);
            f.read_to_end(&mut buf).await?;
            buf
        }
        GetResult::Stream(read_stream) => {
            let chunks: Vec<_> = read_stream.try_collect().await?;

            let mut buf = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum::<usize>());
            for c in chunks {
                buf.extend(c);
            }

            buf
        }
    };

    // Size of each batch
    let batch_size = 1024; // Todo: make a constant or policy for this
    let file_reader = SerializedFileReader::new(Bytes::from(data))?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    // Check schema but ignore the metadata
    let schema = arrow_reader
        .get_schema()?
        .with_metadata(expected_schema.metadata().clone());
    if expected_schema.as_ref() != &schema {
        return Err(ReadError::SchemaMismatch { path });
    }

    let mask = ProjectionMask::roots(arrow_reader.parquet_schema(), projection);
    let record_batch_reader = arrow_reader.get_record_reader_by_columns(mask, batch_size)?;

    for batch in record_batch_reader {
        if tx.send(batch).await.is_err() {
            debug!("Receiver hung up - exiting");
            break;
        }
    }

    debug!(?path, "Completed parquet download & scan");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
    use data_types::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
    use datafusion::common::DataFusionError;
    use iox_time::Time;

    #[tokio::test]
    async fn test_parquet_round_trip() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
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
        let path: ParquetFilePath = (&meta).into();
        let rx = store
            .read_filter(&Predicate::default(), Selection::All, schema, &path)
            .expect("should read record batches from object store");

        // Drain the retrieved record batch stream
        let mut got = datafusion::physical_plan::common::collect(rx)
            .await
            .expect("failed to drain record stream");

        // And compare to the original input
        assert_eq!(got.len(), 1);
        assert_eq!(got.pop().unwrap(), batch);
    }

    #[tokio::test]
    async fn test_schema_check_fail() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_int_array(&[1]))]).unwrap();
        let schema = batch.schema();
        let stream = futures::stream::iter([Ok(other_batch)]);

        // Serialize & upload the record batches.
        store
            .upload(stream, &meta)
            .await
            .expect("should serialize and store sucessfully");

        // Fetch the record batches and compare them to the input batches.
        let path: ParquetFilePath = (&meta).into();
        let rx = store
            .read_filter(&Predicate::default(), Selection::All, schema, &path)
            .expect("should read record batches from object store");

        // Drain the retrieved record batch stream
        let err = datafusion::physical_plan::common::collect(rx)
            .await
            .unwrap_err();

        // And compare to the original input
        if let DataFusionError::ArrowError(ArrowError::ExternalError(err)) = err {
            assert_eq!(
                err.to_string(),
                "Schema mismatch (expected VS actual parquet file) for file: 1/3/2/4/00000000-0000-0000-0000-000000000000.parquet",
            );
        } else {
            panic!("Wrong error type: {err}");
        }
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_mem() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        let stream = futures::stream::iter([Ok(batch)]);

        // Serialize & upload the record batches.
        store
            .upload(stream, &meta)
            .await
            .expect("should serialize and store sucessfully");

        // add metadata to reference schema
        let schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
        );

        // Fetch the record batches and compare them to the input batches.
        let path: ParquetFilePath = (&meta).into();
        let rx = store
            .read_filter(&Predicate::default(), Selection::All, schema, &path)
            .expect("should read record batches from object store");

        // Drain the retrieved record batch stream
        datafusion::physical_plan::common::collect(rx)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_file() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        // add metadata to stored batch
        let batch = RecordBatch::try_new(
            Arc::new(
                schema
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
            ),
            batch.columns().to_vec(),
        )
        .unwrap();
        let stream = futures::stream::iter([Ok(batch)]);

        // Serialize & upload the record batches.
        store
            .upload(stream, &meta)
            .await
            .expect("should serialize and store sucessfully");

        // Fetch the record batches and compare them to the input batches.
        let path: ParquetFilePath = (&meta).into();
        let rx = store
            .read_filter(&Predicate::default(), Selection::All, schema, &path)
            .expect("should read record batches from object store");

        // Drain the retrieved record batch stream
        datafusion::physical_plan::common::collect(rx)
            .await
            .unwrap();
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let mut builder = StringBuilder::new(strs.len());
        for s in strs {
            builder.append_value(s).expect("appending string");
        }
        Arc::new(builder.finish())
    }

    fn to_int_array(vals: &[i64]) -> ArrayRef {
        let mut builder = Int64Builder::new(vals.len());
        for x in vals {
            builder.append_value(*x).expect("appending string");
        }
        Arc::new(builder.finish())
    }

    fn meta() -> IoxMetadata {
        IoxMetadata {
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
        }
    }
}
