/// This module responsible to write given data to specify object store and
/// read them back
use arrow::{
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use data_types::chunk_metadata::ChunkAddr;
use datafusion::{
    logical_plan::Expr,
    physical_plan::{parquet::ParquetExec, ExecutionPlan, Partitioning, SendableRecordBatchStream},
};
use futures::StreamExt;
use internal_types::selection::Selection;
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use observability_deps::tracing::debug;
use parking_lot::Mutex;
use parquet::{
    self,
    arrow::ArrowWriter,
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties, writer::TryClone},
};
use predicate::predicate::Predicate;
use query::exec::stream::AdapterStream;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    io::{Cursor, Seek, SeekFrom, Write},
    sync::Arc,
};
use uuid::Uuid;

use crate::metadata::{IoxMetadata, IoxParquetMetaData, METADATA_KEY};

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

    #[snafu(display("Non local file not supported"))]
    NonLocalFile {},

    #[snafu(display("Error opening file: {}", source))]
    OpenFile { source: std::io::Error },

    #[snafu(display("Error opening temp file: {}", source))]
    OpenTempFile { source: std::io::Error },

    #[snafu(display("Error writing to temp file: {}", source))]
    WriteTempFile { source: std::io::Error },

    #[snafu(display("Internal error: can not get temp file as str: {}", path))]
    TempFilePathAsStr { path: String },

    #[snafu(display("Error creating parquet reader: {}", source))]
    CreatingParquetReader {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Internal error: unexpected partitioning in parquet reader: {:?}",
        partitioning
    ))]
    UnexpectedPartitioning { partitioning: Partitioning },

    #[snafu(display("Error creating pruning predicate: {}", source))]
    CreatingPredicate {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Error reading from parquet stream: {}", source))]
    ReadingParquet {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Error at serialized file reader: {}", source))]
    SerializedFileReaderError {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error at parquet arrow reader: {}", source))]
    ParquetArrowReaderError {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading data from parquet file: {}", source))]
    ReadingFile { source: ArrowError },

    #[snafu(display("Error reading data from object store: {}", source))]
    ReadingObjectStore { source: object_store::Error },

    #[snafu(display("Error sending results: {}", source))]
    SendResult {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Cannot extract Parquet metadata from byte array: {}", source))]
    ExtractingMetadataFailure { source: crate::metadata::Error },

    #[snafu(display("Cannot encode metadata: {}", source))]
    MetadataEncodeFailure { source: prost::EncodeError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Storage {
    iox_object_store: Arc<IoxObjectStore>,
    fixed_uuid: Option<Uuid>,
}

impl Storage {
    pub fn new(iox_object_store: Arc<IoxObjectStore>) -> Self {
        Self {
            iox_object_store,
            fixed_uuid: None,
        }
    }

    /// Create new instance for testing w/ a fixed UUID.
    pub fn new_for_testing(iox_object_store: Arc<IoxObjectStore>, uuid: Uuid) -> Self {
        Self {
            iox_object_store,
            fixed_uuid: Some(uuid),
        }
    }

    /// Write the given stream of data of a specified table of
    /// a specified partitioned chunk to a parquet file of this storage
    ///
    /// returns the path to which the chunk was written, the size of
    /// the bytes, and the parquet metadata
    pub async fn write_to_object_store(
        &self,
        chunk_addr: ChunkAddr,
        stream: SendableRecordBatchStream,
        metadata: IoxMetadata,
    ) -> Result<(ParquetFilePath, usize, IoxParquetMetaData)> {
        // Create full path location of this file in object store
        let path = match self.fixed_uuid {
            Some(uuid) => ParquetFilePath::new_for_testing(&chunk_addr, uuid),
            None => ParquetFilePath::new(&chunk_addr),
        };

        let schema = stream.schema();
        let data = Self::parquet_stream_to_bytes(stream, schema, metadata).await?;
        // TODO: make this work w/o cloning the byte vector (https://github.com/influxdata/influxdb_iox/issues/1504)
        let file_size_bytes = data.len();
        let md =
            IoxParquetMetaData::from_file_bytes(data.clone()).context(ExtractingMetadataFailure)?;
        self.to_object_store(data, &path).await?;

        Ok((path, file_size_bytes, md))
    }

    fn writer_props(metadata_bytes: &[u8]) -> WriterProperties {
        WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue {
                key: METADATA_KEY.to_string(),
                value: Some(base64::encode(&metadata_bytes)),
            }]))
            .set_compression(Compression::ZSTD)
            .build()
    }

    /// Convert the given stream of RecordBatches to bytes
    async fn parquet_stream_to_bytes(
        mut stream: SendableRecordBatchStream,
        schema: SchemaRef,
        metadata: IoxMetadata,
    ) -> Result<Vec<u8>> {
        let metadata_bytes = metadata.to_protobuf().context(MetadataEncodeFailure)?;

        let props = Self::writer_props(&metadata_bytes);

        let mem_writer = MemWriter::default();
        {
            let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, Some(props))
                .context(OpeningParquetWriter)?;
            while let Some(batch) = stream.next().await {
                let batch = batch.context(ReadingStream)?;
                writer.write(&batch).context(WritingParquetToMemory)?;
            }
            writer.close().context(ClosingParquetWriter)?;
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        mem_writer.into_inner().context(WritingToMemWriter)
    }

    /// Put the given vector of bytes to the specified location
    pub async fn to_object_store(&self, data: Vec<u8>, path: &ParquetFilePath) -> Result<()> {
        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.iox_object_store
            .put_parquet_file(
                path,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)
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
    /// and uses the `[ParquetExec`] from DataFusion to read that
    /// parquet file (including predicate and projection pushdown).
    ///
    /// The resulting record batches from Parquet are sent back to `tx`
    async fn download_and_scan_parquet(
        predicate: Option<Expr>,
        projection: Vec<usize>,
        path: ParquetFilePath,
        store: Arc<IoxObjectStore>,
        tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
    ) -> Result<()> {
        // Size of each batch
        let batch_size = 1024; // Todo: make a constant or policy for this
        let max_concurrency = 1; // Todo: make a constant or policy for this

        // Limit of total rows to read
        let limit: Option<usize> = None; // Todo: this should be a parameter of the function

        // todo(paul): Here is where I'd get the cache from object store. If it has
        //  one, I'd do the `fs_path_or_cache`. Otherwise, do the temp file like below.

        // read parquet file to local file
        let mut temp_file = tempfile::Builder::new()
            .prefix("iox-parquet-cache")
            .suffix(".parquet")
            .tempfile()
            .context(OpenTempFile)?;

        debug!(?path, ?temp_file, "Beginning to read parquet to temp file");
        let mut read_stream = store
            .get_parquet_file(&path)
            .await
            .context(ReadingObjectStore)?;

        while let Some(bytes) = read_stream.next().await {
            let bytes = bytes.context(ReadingObjectStore)?;
            debug!(len = bytes.len(), "read bytes from object store");
            temp_file.write_all(&bytes).context(WriteTempFile)?;
        }

        // now, create the appropriate parquet exec from datafusion and make it
        let temp_path = temp_file.into_temp_path();
        debug!(?temp_path, "Completed read parquet to tempfile");

        let temp_path = temp_path.to_str().with_context(|| TempFilePathAsStr {
            path: temp_path.to_string_lossy(),
        })?;

        // TODO: renenable when bug in parquet statistics generation
        // is fixed: https://github.com/apache/arrow-rs/issues/641
        // https://github.com/influxdata/influxdb_iox/issues/2163
        if predicate.is_some() {
            debug!(?predicate, "Skipping predicate pushdown due to XXX");
        }
        let predicate = None;

        let parquet_exec = ParquetExec::try_from_path(
            temp_path,
            Some(projection),
            predicate,
            batch_size,
            max_concurrency,
            limit,
        )
        .context(CreatingParquetReader)?;

        // We are assuming there is only a single stream in the
        // call to execute(0) below
        let partitioning = parquet_exec.output_partitioning();
        ensure!(
            matches!(partitioning, Partitioning::UnknownPartitioning(1)),
            UnexpectedPartitioning { partitioning }
        );

        let mut parquet_stream = parquet_exec.execute(0).await.context(ReadingParquet)?;

        while let Some(batch) = parquet_stream.next().await {
            if let Err(e) = tx.send(batch).await {
                debug!(%e, "Stopping parquet exec early, receiver hung up");
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn read_filter(
        predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        path: ParquetFilePath,
        store: Arc<IoxObjectStore>,
    ) -> Result<SendableRecordBatchStream> {
        // fire up a async task that will fetch the parquet file
        // locally, start it executing and send results

        // Indices of columns in the schema needed to read
        let projection: Vec<usize> = Self::column_indices(selection, Arc::clone(&schema));

        // Compute final (output) schema after selection
        let schema = Arc::new(Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        ));

        // pushdown predicate, if any
        let predicate = predicate.filter_expr();

        let (tx, rx) = tokio::sync::mpsc::channel(2);

        // Run async dance here to make sure any error returned
        // `download_and_scan_parquet` is sent back to the reader and
        // not silently ignored
        tokio::task::spawn(async move {
            let download_result =
                Self::download_and_scan_parquet(predicate, projection, path, store, tx.clone())
                    .await;

            // If there was an error returned from download_and_scan_parquet send it back to the receiver.
            if let Err(e) = download_result {
                let e = ArrowError::ExternalError(Box::new(e));
                if let Err(e) = tx.send(ArrowResult::Err(e)).await {
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
    use crate::test_utils::{
        chunk_addr, create_partition_and_database_checkpoint, load_parquet_from_store,
        make_chunk_given_record_batch, make_iox_object_store, make_record_batch,
        read_data_from_parquet_data, TestSize,
    };
    use arrow::array::{ArrayRef, StringArray};
    use arrow_util::assert_batches_eq;
    use chrono::Utc;
    use data_types::{chunk_metadata::ChunkOrder, partition_metadata::TableSummary};
    use datafusion::physical_plan::common::SizedRecordBatchStream;
    use datafusion_util::MemoryStream;
    use parquet::schema::types::ColumnPath;

    #[tokio::test]
    async fn test_parquet_contains_key_value_metadata() {
        let table_name = Arc::from("table1");
        let partition_key = Arc::from("part1");
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
        );
        let metadata = IoxMetadata {
            creation_timestamp: Utc::now(),
            table_name,
            partition_key,
            chunk_id: 1337,
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Utc::now(),
            time_of_last_write: Utc::now(),
            chunk_order: ChunkOrder::new(5),
        };

        // create parquet file
        let (_record_batches, schema, _column_summaries, _num_rows) =
            make_record_batch("foo", TestSize::Full);
        let stream: SendableRecordBatchStream = Box::pin(MemoryStream::new_with_schema(
            vec![],
            Arc::clone(schema.inner()),
        ));
        let bytes =
            Storage::parquet_stream_to_bytes(stream, Arc::clone(schema.inner()), metadata.clone())
                .await
                .unwrap();

        // extract metadata
        let md = IoxParquetMetaData::from_file_bytes(bytes).unwrap();
        let metadata_roundtrip = md.decode().unwrap().read_iox_metadata().unwrap();

        // compare with input
        assert_eq!(metadata_roundtrip, metadata);
    }

    #[tokio::test]
    async fn test_roundtrip() {
        test_helpers::maybe_start_logging();
        // validates that the async plubing is setup to read parquet files from object store

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
        let chunk_id = 33;
        let iox_object_store = make_iox_object_store().await;
        let storage = Storage::new(Arc::clone(&iox_object_store));

        // write the data in
        let schema = batch.schema();
        let input_stream = Box::pin(SizedRecordBatchStream::new(
            batch.schema(),
            vec![Arc::new(batch)],
        ));
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
        );
        let metadata = IoxMetadata {
            creation_timestamp: Utc::now(),
            table_name: Arc::clone(&table_name),
            partition_key: Arc::clone(&partition_key),
            chunk_id,
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Utc::now(),
            time_of_last_write: Utc::now(),
            chunk_order: ChunkOrder::new(5),
        };

        let (path, _file_size_bytes, _metadata) = storage
            .write_to_object_store(
                ChunkAddr {
                    db_name: iox_object_store.database_name().into(),
                    table_name,
                    partition_key,
                    chunk_id,
                },
                input_stream,
                metadata,
            )
            .await
            .expect("successfully wrote to object store");

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

    #[test]
    fn test_props_have_compression() {
        // should be writing with compression
        let props = Storage::writer_props(&[]);

        // arbitrary column name to get default values
        let col_path: ColumnPath = "default".into();
        assert_eq!(props.compression(&col_path), Compression::ZSTD);
    }

    #[tokio::test]
    async fn test_write_read() {
        ////////////////////
        // Create test data which is also the expected data
        let addr = chunk_addr(1);
        let table = Arc::clone(&addr.table_name);
        let (record_batches, schema, column_summaries, num_rows) =
            make_record_batch("foo", TestSize::Full);
        let mut table_summary = TableSummary::new(table.to_string());
        table_summary.columns = column_summaries.clone();
        let record_batch = record_batches[0].clone(); // Get the first one to compare key-value meta data that would be the same for all batches
        let key_value_metadata = record_batch.schema().metadata().clone();

        ////////////////////
        // Make an OS in memory
        let store = make_iox_object_store().await;

        ////////////////////
        // Store the data as a chunk and write it to in the object store
        // This test Storage::write_to_object_store
        let chunk = make_chunk_given_record_batch(
            Arc::clone(&store),
            record_batches.clone(),
            schema.clone(),
            addr,
            column_summaries.clone(),
            TestSize::Full,
        )
        .await;

        ////////////////////
        // Now let read it back
        //
        let parquet_data = load_parquet_from_store(&chunk, Arc::clone(&store))
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data.clone()).unwrap();
        let decoded = parquet_metadata.decode().unwrap();
        //
        // 1. Check metadata at file level: Everything is correct
        let schema_actual = decoded.read_schema().unwrap();
        assert_eq!(Arc::new(schema.clone()), schema_actual);
        assert_eq!(
            key_value_metadata.clone(),
            schema_actual.as_arrow().metadata().clone()
        );

        // 2. Check statistics
        let table_summary_actual = decoded.read_statistics(&schema_actual).unwrap();
        assert_eq!(table_summary_actual, table_summary.columns);

        // 3. Check data
        // Note that the read_data_from_parquet_data function fixes the row-group/batches' level metadata bug in arrow
        let actual_record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);
        let mut actual_num_rows = 0;
        for batch in actual_record_batches.clone() {
            actual_num_rows += batch.num_rows();

            // Check if record batch has meta data
            let batch_key_value_metadata = batch.schema().metadata().clone();
            assert_eq!(
                schema.as_arrow().metadata().clone(),
                batch_key_value_metadata
            );
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
        assert_eq!(num_rows, actual_num_rows);
        assert_batches_eq!(expected.clone(), &record_batches);
        assert_batches_eq!(expected, &actual_record_batches);
    }
}
