/// This module responsible to write given data to specify object store and
/// read them back
use arrow::{
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use data_types::chunk_metadata::ChunkAddr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::AdapterStream;
use futures::{stream, Stream, StreamExt};
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
    marker::Unpin,
    sync::Arc,
};

use crate::metadata::{IoxMetadata, IoxMetadataOld, IoxParquetMetaData, METADATA_KEY};

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

    /// Write the given stream of data of a specified table of
    /// a specified partitioned chunk to a parquet file of this storage
    ///
    /// returns the path to which the chunk was written, the size of
    /// the bytes, and the parquet metadata
    ///
    /// Nothing will be persisted if the input stream returns nothing as
    /// a result of hard deletes and deduplications
    pub async fn write_to_object_store(
        &self,
        chunk_addr: ChunkAddr,
        stream: SendableRecordBatchStream,
        metadata: IoxMetadataOld,
    ) -> Result<Option<(ParquetFilePath, usize, IoxParquetMetaData)>> {
        // Create full path location of this file in object store
        let path = ParquetFilePath::new_old_gen(&chunk_addr);

        let schema = stream.schema();
        let data = self
            .parquet_stream_to_bytes(stream, schema, metadata)
            .await?;
        // no data
        if data.is_empty() {
            return Ok(None);
        }

        let file_size_bytes = data.len();
        let data = Arc::new(data);
        let md = IoxParquetMetaData::from_file_bytes(Arc::clone(&data))
            .context(ExtractingMetadataFailureSnafu)?
            .context(NoDataSnafu)?;
        let data = Arc::try_unwrap(data).expect("dangling reference to data");
        self.to_object_store(data, &path).await?;

        Ok(Some((path, file_size_bytes, md)))
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

    /// Convert the given stream of RecordBatches to bytes. This should be deleted when switching
    /// over to use `ingester` only.
    async fn parquet_stream_to_bytes(
        &self,
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
        metadata: IoxMetadataOld,
    ) -> Result<Vec<u8>> {
        let metadata_bytes = metadata.to_protobuf().context(MetadataEncodeFailureSnafu)?;

        self.record_batches_to_parquet_bytes(stream, schema, &metadata_bytes)
            .await
    }

    /// Convert the given metadata and RecordBatches to parquet file bytes. Used by `ingester`.
    pub async fn parquet_bytes(
        &self,
        record_batches: Vec<RecordBatch>,
        schema: SchemaRef,
        metadata: &IoxMetadata,
    ) -> Result<Vec<u8>> {
        let metadata_bytes = metadata.to_protobuf().context(MetadataEncodeFailureSnafu)?;

        let stream = Box::pin(stream::iter(record_batches.into_iter().map(Ok)));

        self.record_batches_to_parquet_bytes(stream, schema, &metadata_bytes)
            .await
    }

    /// Share code between `parquet_stream_to_bytes` and `parquet_bytes`. When
    /// `parquet_stream_to_bytes` is deleted, this code can be moved into `parquet_bytes` and
    /// made simpler by using a plain `Iter` rather than a `Stream`.
    async fn record_batches_to_parquet_bytes(
        &self,
        mut stream: impl Stream<Item = ArrowResult<RecordBatch>> + Send + Unpin,
        schema: SchemaRef,
        metadata_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        let props = self.writer_props(metadata_bytes);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::generator::ChunkGenerator;
    use crate::test_utils::{
        create_partition_and_database_checkpoint, load_parquet_from_store, make_iox_object_store,
        make_record_batch, read_data_from_parquet_data, TestSize,
    };
    use arrow::array::{ArrayRef, StringArray};
    use arrow_util::assert_batches_eq;
    use data_types::chunk_metadata::{ChunkId, ChunkOrder};
    use datafusion_util::{stream_from_batch, MemoryStream};
    use parquet::schema::types::ColumnPath;
    use time::Time;

    #[tokio::test]
    async fn test_parquet_contains_key_value_metadata() {
        let table_name = Arc::from("table1");
        let partition_key = Arc::from("part1");
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
        );
        let metadata = IoxMetadataOld {
            creation_timestamp: Time::from_timestamp_nanos(3453),
            table_name,
            partition_key,
            chunk_id: ChunkId::new_test(1337),
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Time::from_timestamp_nanos(456),
            time_of_last_write: Time::from_timestamp_nanos(43069346),
            chunk_order: ChunkOrder::new(5).unwrap(),
            sort_key: None,
        };

        // create parquet file
        let (record_batches, schema, _column_summaries, _num_rows) =
            make_record_batch("foo", TestSize::Minimal);
        let stream: SendableRecordBatchStream = Box::pin(MemoryStream::new_with_schema(
            record_batches,
            Arc::clone(schema.inner()),
        ));
        let bytes = Storage::new(make_iox_object_store().await)
            .parquet_stream_to_bytes(stream, Arc::clone(schema.inner()), metadata.clone())
            .await
            .unwrap();

        // extract metadata
        let md = IoxParquetMetaData::from_file_bytes(Arc::new(bytes))
            .unwrap()
            .unwrap();
        let metadata_roundtrip = md.decode().unwrap().read_iox_metadata_old().unwrap();

        // compare with input
        assert_eq!(metadata_roundtrip, metadata);
    }

    #[tokio::test]
    async fn test_roundtrip() {
        test_helpers::maybe_start_logging();
        // validates that the async plumbing is setup to read parquet files from object store

        // prepare input
        let array = StringArray::from(vec!["foo", "bar", "baz"]);
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_test_column",
            Arc::new(array) as ArrayRef,
        )])
        .unwrap();

        let expected = vec![
            "+------------------------+",
            "| my_awesome_test_column |",
            "+------------------------+",
            "| foo                    |",
            "| bar                    |",
            "| baz                    |",
            "+------------------------+",
        ];

        let input_batches = vec![batch.clone()];
        assert_batches_eq!(&expected, &input_batches);

        // create Storage
        let table_name = Arc::from("my_table");
        let partition_key = Arc::from("my_partition");
        let chunk_id = ChunkId::new_test(33);
        let iox_object_store = make_iox_object_store().await;
        let db_name = Arc::from("db1");
        let storage = Storage::new(Arc::clone(&iox_object_store));

        // write the data in
        let schema = batch.schema();
        let input_stream = stream_from_batch(batch);
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
        );
        let metadata = IoxMetadataOld {
            creation_timestamp: Time::from_timestamp_nanos(43069346),
            table_name: Arc::clone(&table_name),
            partition_key: Arc::clone(&partition_key),
            chunk_id,
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Time::from_timestamp_nanos(234),
            time_of_last_write: Time::from_timestamp_nanos(4784),
            chunk_order: ChunkOrder::new(5).unwrap(),
            sort_key: None,
        };

        let (path, _file_size_bytes, _metadata) = storage
            .write_to_object_store(
                ChunkAddr {
                    db_name: Arc::clone(&db_name),
                    table_name,
                    partition_key,
                    chunk_id,
                },
                input_stream,
                metadata,
            )
            .await
            .expect("successfully wrote to object store")
            .unwrap();

        let iox_object_store = Arc::clone(&storage.iox_object_store);
        let read_stream = Storage::read_filter(
            &Predicate::default(),
            Selection::All,
            schema,
            path,
            iox_object_store,
        )
        .expect("successfully called read_filter");

        let read_batches = datafusion::physical_plan::common::collect(read_stream)
            .await
            .expect("collecting results");

        assert_batches_eq!(&expected, &read_batches);
    }

    #[tokio::test]
    async fn test_props_have_compression() {
        let storage = Storage::new(make_iox_object_store().await);

        // should be writing with compression
        let props = storage.writer_props(&[]);

        // arbitrary column name to get default values
        let col_path: ColumnPath = "default".into();
        assert_eq!(props.compression(&col_path), Compression::ZSTD);
    }

    #[tokio::test]
    async fn test_write_read() {
        ////////////////////
        // Store the data as a chunk and write it to in the object store
        // This tests Storage::write_to_object_store
        let mut generator = ChunkGenerator::new().await;
        let (chunk, _) = generator.generate().await.unwrap();
        let key_value_metadata = chunk.schema().as_arrow().metadata().clone();

        ////////////////////
        // Now let read it back
        //
        let parquet_data = Arc::new(
            load_parquet_from_store(&chunk, Arc::clone(generator.store()))
                .await
                .unwrap(),
        );
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(Arc::clone(&parquet_data))
            .unwrap()
            .unwrap();
        let decoded = parquet_metadata.decode().unwrap();
        //
        // 1. Check metadata at file level: Everything is correct
        let schema_actual = decoded.read_schema().unwrap();
        assert_eq!(chunk.schema(), schema_actual);
        assert_eq!(
            key_value_metadata.clone(),
            schema_actual.as_arrow().metadata().clone()
        );

        // 2. Check statistics
        let table_summary_actual = decoded.read_statistics(&schema_actual).unwrap();
        assert_eq!(table_summary_actual, chunk.table_summary().columns);

        // 3. Check data
        // Note that the read_data_from_parquet_data function fixes the row-group/batches' level metadata bug in arrow
        let actual_record_batches =
            read_data_from_parquet_data(chunk.schema().as_arrow(), parquet_data);
        let mut actual_num_rows = 0;
        for batch in actual_record_batches.clone() {
            actual_num_rows += batch.num_rows();

            // Check if record batch has meta data
            let batch_key_value_metadata = batch.schema().metadata().clone();
            assert_eq!(key_value_metadata, batch_key_value_metadata);
        }

        // Now verify return results. This assert_batches_eq still works correctly without the metadata
        // We might modify it to make it include checking metadata or add a new comparison checking macro that prints out the metadata too
        let expected = vec![
            "+----------------+---------------+-------------------+------------------+-------------------------+------------------------+----------------------------+---------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+-------------------+--------------------+------------------------+-----------------------+-------------------------+------------------------+-----------------------+--------------------------+-------------------------+-----------------------------+",
            "| foo_tag_normal | foo_tag_empty | foo_tag_null_some | foo_tag_null_all | foo_field_string_normal | foo_field_string_empty | foo_field_string_null_some | foo_field_string_null_all | foo_field_i64_normal | foo_field_i64_range  | foo_field_i64_null_some | foo_field_i64_null_all | foo_field_u64_normal | foo_field_u64_range  | foo_field_u64_null_some | foo_field_u64_null_all | foo_field_f64_normal | foo_field_f64_inf | foo_field_f64_zero | foo_field_f64_nan_some | foo_field_f64_nan_all | foo_field_f64_null_some | foo_field_f64_null_all | foo_field_bool_normal | foo_field_bool_null_some | foo_field_bool_null_all | time                        |",
            "+----------------+---------------+-------------------+------------------+-------------------------+------------------------+----------------------------+---------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+-------------------+--------------------+------------------------+-----------------------+-------------------------+------------------------+-----------------------+--------------------------+-------------------------+-----------------------------+",
            "| foo            |               |                   |                  | foo                     |                        |                            |                           | -1                   | -9223372036854775808 |                         |                        | 1                    | 0                    |                         |                        | 10.1                 | 0                 | 0                  | NaN                    | NaN                   |                         |                        | true                  |                          |                         | 1970-01-01T00:00:00.000001Z |",
            "| bar            |               | bar               |                  | bar                     |                        | bar                        |                           | 2                    | 9223372036854775807  | 2                       |                        | 2                    | 18446744073709551615 | 2                       |                        | 20.1                 | inf               | -0                 | 2                      | NaN                   | 20.1                    |                        | false                 | false                    |                         | 1970-01-01T00:00:00.000002Z |",
            "| baz            |               | baz               |                  | baz                     |                        | baz                        |                           | 3                    | -9223372036854775808 | 3                       |                        | 3                    | 0                    | 3                       |                        | 30.1                 | -inf              | 0                  | 1                      | NaN                   | 30.1                    |                        | true                  | true                     |                         | 1970-01-01T00:00:00.000003Z |",
            "| foo            |               |                   |                  | foo                     |                        |                            |                           | 4                    | 9223372036854775807  |                         |                        | 4                    | 18446744073709551615 |                         |                        | 40.1                 | 1                 | -0                 | NaN                    | NaN                   |                         |                        | false                 |                          |                         | 1970-01-01T00:00:00.000004Z |",
            "+----------------+---------------+-------------------+------------------+-------------------------+------------------------+----------------------------+---------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+----------------------+-------------------------+------------------------+----------------------+-------------------+--------------------+------------------------+-----------------------+-------------------------+------------------------+-----------------------+--------------------------+-------------------------+-----------------------------+",
        ];
        assert_eq!(chunk.rows(), actual_num_rows);
        assert_batches_eq!(expected, &actual_record_batches);
    }
}
