use crate::metadata::{IoxMetadata, METADATA_KEY};
/// This module responsible to write given data to specify object store and
/// read them back
use arrow::{
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::AdapterStream;
use futures::{stream, StreamExt};
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use object_store::GetResult;
use observability_deps::tracing::*;
use parking_lot::Mutex;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use parquet::{
    self,
    arrow::ArrowWriter,
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties, writer::TryClone},
};
use predicate::Predicate;
use schema::selection::Selection;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    io::{Cursor, Seek, SeekFrom, Write},
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading stream while creating snapshot: {}", source))]
    ReadingStream { source: ArrowError },

    #[snafu(display("Error writing Parquet to memory: {}", source))]
    WritingParquetToMemory {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error closing Parquet Writer: {}", source))]
    ClosingParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },

    #[snafu(display("Error converting to vec[u8]: Nothing else should have a reference here"))]
    WritingToMemWriter {},

    #[snafu(display("Error opening temp file: {}", source))]
    OpenTempFile { source: std::io::Error },

    #[snafu(display("Error writing to temp file: {}", source))]
    WriteTempFile { source: std::io::Error },

    #[snafu(display("Error reading data from object store: {}", source))]
    ReadingObjectStore { source: object_store::Error },

    #[snafu(display("Cannot extract Parquet metadata from byte array: {}", source))]
    ExtractingMetadataFailure { source: crate::metadata::Error },

    #[snafu(display("Cannot encode metadata: {}", source))]
    MetadataEncodeFailure { source: prost::EncodeError },

    #[snafu(display("Error reading parquet: {}", source))]
    ParquetReader {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("No data to convert to parquet"))]
    NoData {},
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Storage {
    iox_object_store: Arc<IoxObjectStore>,

    // If `Some`, restricts the size of the row groups created in the parquet file
    max_row_group_size: Option<usize>,
}

impl Storage {
    pub fn new(iox_object_store: Arc<IoxObjectStore>) -> Self {
        Self {
            iox_object_store,
            max_row_group_size: None,
        }
    }

    /// Specify the maximum sized row group to make
    pub fn set_max_row_group_size(&mut self, max_row_group_size: usize) {
        self.max_row_group_size = Some(max_row_group_size);
    }

    fn writer_props(&self, metadata_bytes: &[u8]) -> WriterProperties {
        let builder = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue {
                key: METADATA_KEY.to_string(),
                value: Some(base64::encode(&metadata_bytes)),
            }]))
            .set_compression(Compression::ZSTD);

        let builder = if let Some(max_row_group_size) = self.max_row_group_size.as_ref() {
            builder.set_max_row_group_size(*max_row_group_size)
        } else {
            builder
        };

        builder.build()
    }

    /// Convert the given metadata and RecordBatches to parquet file bytes. Used by `ingester`.
    pub async fn parquet_bytes(
        &self,
        record_batches: Vec<RecordBatch>,
        schema: SchemaRef,
        metadata: &IoxMetadata,
    ) -> Result<Vec<u8>> {
        let metadata_bytes = metadata.to_protobuf().context(MetadataEncodeFailureSnafu)?;

        let mut stream = Box::pin(stream::iter(record_batches.into_iter().map(Ok)));

        let props = self.writer_props(&metadata_bytes);

        let mem_writer = MemWriter::default();
        {
            let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, Some(props))
                .context(OpeningParquetWriterSnafu)?;
            let mut no_stream_data = true;
            while let Some(batch) = stream.next().await {
                no_stream_data = false;
                let batch = batch.context(ReadingStreamSnafu)?;
                writer.write(&batch).context(WritingParquetToMemorySnafu)?;
            }
            if no_stream_data {
                return Ok(vec![]);
            }
            writer.close().context(ClosingParquetWriterSnafu)?;
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        mem_writer.into_inner().context(WritingToMemWriterSnafu)
    }

    /// Put the given vector of bytes to the specified location
    pub async fn to_object_store(&self, data: Vec<u8>, path: &ParquetFilePath) -> Result<()> {
        let data = Bytes::from(data);

        self.iox_object_store
            .put_parquet_file(path, data)
            .await
            .context(WritingToObjectStoreSnafu)
    }

    /// Return indices of the schema's fields of the selection columns
    pub fn column_indices(selection: Selection<'_>, schema: SchemaRef) -> Vec<usize> {
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
    /// and uses the `[ParquetExec`]
    ///
    /// The resulting record batches from Parquet are sent back to `tx`
    fn download_and_scan_parquet(
        projection: Vec<usize>,
        path: ParquetFilePath,
        store: Arc<IoxObjectStore>,
        tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
    ) -> Result<()> {
        // Size of each batch
        let batch_size = 1024; // Todo: make a constant or policy for this

        let read_stream = futures::executor::block_on(store.get_parquet_file(&path))
            .context(ReadingObjectStoreSnafu)?;

        let file = match read_stream {
            GetResult::File(f, _) => {
                debug!(?path, "Using file directly");
                futures::executor::block_on(f.into_std())
            }
            GetResult::Stream(read_stream) => {
                // read parquet file to local file
                let mut file = tempfile::tempfile().context(OpenTempFileSnafu)?;

                debug!(?path, ?file, "Beginning to read parquet to temp file");

                for bytes in futures::executor::block_on_stream(read_stream) {
                    let bytes = bytes.context(ReadingObjectStoreSnafu)?;
                    debug!(len = bytes.len(), "read bytes from object store");
                    file.write_all(&bytes).context(WriteTempFileSnafu)?;
                }

                file.rewind().context(WriteTempFileSnafu)?;

                debug!(?path, "Completed read parquet to tempfile");
                file
            }
        };

        let file_reader = SerializedFileReader::new(file).context(ParquetReaderSnafu)?;
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
        let record_batch_reader = arrow_reader
            .get_record_reader_by_columns(projection, batch_size)
            .context(ParquetReaderSnafu)?;

        for batch in record_batch_reader {
            if tx.blocking_send(batch).is_err() {
                debug!(?path, "Receiver hung up - exiting");
                break;
            }
        }

        debug!(?path, "Completed parquet download & scan");

        Ok(())
    }

    pub fn read_filter(
        _predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        path: ParquetFilePath,
        store: Arc<IoxObjectStore>,
    ) -> Result<SendableRecordBatchStream> {
        // Indices of columns in the schema needed to read
        let projection: Vec<usize> = Self::column_indices(selection, Arc::clone(&schema));

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
        tokio::task::spawn_blocking(move || {
            let download_result =
                Self::download_and_scan_parquet(projection, path, store, tx.clone());

            // If there was an error returned from download_and_scan_parquet send it back to the receiver.
            if let Err(e) = download_result {
                warn!(error=%e, "Parquet download & scan failed");
                let e = ArrowError::ExternalError(Box::new(e));
                if let Err(e) = tx.blocking_send(ArrowResult::Err(e)) {
                    // if no one is listening, there is no one else to hear our screams
                    debug!(%e, "Error sending result of download function. Receiver is closed.");
                }
            }
        });

        // returned stream simply reads off the rx channel
        Ok(AdapterStream::adapt(schema, rx))
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemWriter {
    mem: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MemWriter {
    /// Returns the inner buffer as long as there are no other references to the
    /// Arc.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.mem)
            .ok()
            .map(|mutex| mutex.into_inner().into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: Arc::clone(&self.mem),
        })
    }
}
