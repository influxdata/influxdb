/// This module responsible to write given data to specify object store and
/// read them back
use arrow::{
    datatypes::{Schema as ArrowSchema, SchemaRef},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{
    common::SizedRecordBatchStream, parquet::RowGroupPredicateBuilder, RecordBatchStream,
    SendableRecordBatchStream,
};
use internal_types::{schema::Schema, selection::Selection};
use object_store::{
    path::{parsed::DirsAndFileName, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use parquet::{
    self,
    arrow::{
        arrow_reader::ParquetFileArrowReader, parquet_to_arrow_schema, ArrowReader, ArrowWriter,
    },
    file::{
        metadata::ParquetMetaData,
        reader::FileReader,
        serialized_reader::{SerializedFileReader, SliceableCursor},
        writer::TryClone,
    },
};
use query::predicate::Predicate;

use bytes::Bytes;
use data_types::server_id::ServerId;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    convert::TryInto,
    io::{Cursor, Seek, SeekFrom, Write},
    sync::Arc,
    task::{Context, Poll},
};
use tokio_stream::wrappers::ReceiverStream;

use crate::metadata::read_parquet_metadata_from_file;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading stream while creating snapshot: {}", source))]
    ReadingStream { source: arrow::error::ArrowError },

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

    #[snafu(display("Error at serialized file reader: {}", source))]
    SerializedFileReaderError {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error at parquet arrow reader: {}", source))]
    ParquetArrowReaderError {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading data from parquet file: {}", source))]
    ReadingFile { source: arrow::error::ArrowError },

    #[snafu(display("Error reading data from object store: {}", source))]
    ReadingObjectStore { source: object_store::Error },

    #[snafu(display("Error sending results: {}", source))]
    SendResult {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Cannot read arrow schema from parquet: {}", source))]
    ArrowFromParquetFailure {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot read IOx schema from arrow: {}", source))]
    IoxFromArrowFailure {
        source: internal_types::schema::Error,
    },

    #[snafu(display("Cannot extract Parquet metadata from byte array: {}", source))]
    ExtractingMetadataFailure { source: crate::metadata::Error },

    #[snafu(display("Cannot parse location: {:?}", path))]
    LocationParsingFailure { path: DirsAndFileName },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ParquetStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    db_name: String,
}

impl Storage {
    pub fn new(object_store: Arc<ObjectStore>, server_id: ServerId, db_name: String) -> Self {
        Self {
            object_store,
            server_id,
            db_name,
        }
    }

    /// Return full path including filename in the object store to save a chunk
    /// table file.
    ///
    /// See [`parse_location`](Self::parse_location) for parsing.
    pub fn location(
        &self,
        partition_key: String,
        chunk_id: u32,
        table_name: String,
    ) -> object_store::path::Path {
        // Full path of the file in object store
        //    <writer id>/<database>/data/<partition key>/<chunk id>/<table
        // name>.parquet

        let mut path = self.object_store.new_path();
        path.push_dir(self.server_id.to_string());
        path.push_dir(self.db_name.clone());
        path.push_dir("data");
        path.push_dir(partition_key);
        path.push_dir(chunk_id.to_string());
        let file_name = format!("{}.parquet", table_name);
        path.set_file_name(file_name);

        path
    }

    /// Parse locations and return partition key, chunk ID and table name.
    ///
    /// See [`location`](Self::location) for path generation.
    pub fn parse_location(
        &self,
        path: impl Into<DirsAndFileName>,
    ) -> Result<(String, u32, String)> {
        let path: DirsAndFileName = path.into();

        let dirs: Vec<_> = path.directories.iter().map(|part| part.encoded()).collect();
        match (dirs.as_slice(), &path.file_name) {
            ([server_id, db_name, "data", partition_key, chunk_id], Some(filename))
                if (server_id == &self.server_id.to_string()) && (db_name == &self.db_name) =>
            {
                let chunk_id: u32 = match chunk_id.parse() {
                    Ok(x) => x,
                    Err(_) => return Err(Error::LocationParsingFailure { path }),
                };

                let parts: Vec<_> = filename.encoded().split('.').collect();
                let table_name = match parts[..] {
                    [name, "parquet"] => name,
                    _ => return Err(Error::LocationParsingFailure { path }),
                };

                Ok((partition_key.to_string(), chunk_id, table_name.to_string()))
            }
            _ => Err(Error::LocationParsingFailure { path }),
        }
    }

    /// Write the given stream of data of a specified table of
    // a specified partitioned chunk to a parquet file of this storage
    pub async fn write_to_object_store(
        &self,
        partition_key: String,
        chunk_id: u32,
        table_name: String,
        stream: SendableRecordBatchStream,
    ) -> Result<(Path, ParquetMetaData)> {
        // Create full path location of this file in object store
        let path = self.location(partition_key, chunk_id, table_name);

        let schema = stream.schema();
        let data = Self::parquet_stream_to_bytes(stream, schema).await?;
        // TODO: make this work w/o cloning the byte vector (https://github.com/influxdata/influxdb_iox/issues/1504)
        let md =
            read_parquet_metadata_from_file(data.clone()).context(ExtractingMetadataFailure)?;
        self.to_object_store(data, &path).await?;

        Ok((path.clone(), md))
    }

    /// Convert the given stream of RecordBatches to bytes

    pub async fn parquet_stream_to_bytes(
        mut stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Result<Vec<u8>> {
        let mem_writer = MemWriter::default();
        {
            let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, None)
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
    pub async fn to_object_store(
        &self,
        data: Vec<u8>,
        file_name: &object_store::path::Path,
    ) -> Result<()> {
        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.object_store
            .put(
                &file_name,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)
    }

    /// Make a datafusion predicate builder for the given predicate and schema
    pub fn predicate_builder(
        predicate: &Predicate,
        schema: ArrowSchema,
    ) -> Option<RowGroupPredicateBuilder> {
        if predicate.exprs.is_empty() {
            None
        } else {
            // Convert to datafusion's predicate
            let predicate = predicate.filter_expr()?;
            Some(RowGroupPredicateBuilder::try_new(&predicate, schema).ok()?)
        }
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

    pub fn read_filter(
        predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        path: Path,
        store: Arc<ObjectStore>,
    ) -> Result<SendableRecordBatchStream> {
        // Indices of columns in the schema needed to read
        let projection: Vec<usize> = Self::column_indices(selection, Arc::clone(&schema));

        // Filter needed predicates
        let builder_schema = ArrowSchema::new(schema.fields().clone());
        let predicate_builder = Self::predicate_builder(predicate, builder_schema);

        // Size of each batch
        let batch_size = 1024; // Todo: make a constant or policy for this

        // Limit of total rows to read
        let limit: Option<usize> = None; // Todo: this should be a parameter of the function

        let mut batches: Vec<Arc<RecordBatch>> = vec![];
        if let Err(e) = Self::read_file(
            path,
            Arc::clone(&store),
            projection.as_slice(),
            predicate_builder.as_ref(),
            batch_size,
            &mut batches,
            limit,
        ) {
            return Err(e);
        }

        // Schema of all record batches must be the same, Get the first one
        // to build record batch stream
        let batch_schema = if batches.is_empty() {
            schema
        } else {
            batches[0].schema()
        };

        Ok(Box::pin(SizedRecordBatchStream::new(batch_schema, batches)))
    }

    // Read the given path of the parquet file and return record batches satisfied
    // the given predicate_builder
    fn read_file(
        path: Path,
        store: Arc<ObjectStore>,
        projection: &[usize],
        predicate_builder: Option<&RowGroupPredicateBuilder>,
        batch_size: usize,
        batches: &mut Vec<Arc<RecordBatch>>,
        limit: Option<usize>,
    ) -> Result<()> {
        let parquet_data = futures::executor::block_on(async move {
            Self::load_parquet_data_from_object_store(path, store).await
        });

        let mut total_rows = 0;

        let cursor = SliceableCursor::new(parquet_data?);
        let mut reader = SerializedFileReader::new(cursor).context(SerializedFileReaderError)?;

        // TODO: remove these line after https://github.com/apache/arrow-rs/issues/252 is done
        // Get file level metadata to set it to the record batch's metadata below
        let metadata = reader.metadata();
        let schema = read_schema_from_parquet_metadata(metadata)?;

        if let Some(predicate_builder) = predicate_builder {
            let row_group_predicate =
                predicate_builder.build_row_group_predicate(metadata.row_groups());
            reader.filter_row_groups(&row_group_predicate); //filter out
                                                            // row group based
                                                            // on the predicate
        }

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader) as Arc<dyn FileReader>);

        let mut batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.to_owned(), batch_size)
            .context(ParquetArrowReaderError)?;

        loop {
            match batch_reader.next() {
                Some(Ok(batch)) => {
                    total_rows += batch.num_rows();

                    // TODO: remove these lines when arow-rs' ticket https://github.com/apache/arrow-rs/issues/252 is done
                    // Since arrow's parquet reading does not return the row group level's metadata, the
                    // work around here is to get it from the file level which is the same
                    let columns = batch.columns().to_vec();
                    let fields = batch.schema().fields().clone();
                    let arrow_column_schema = ArrowSchema::new_with_metadata(
                        fields,
                        schema.as_arrow().metadata().clone(),
                    );
                    let new_batch = RecordBatch::try_new(Arc::new(arrow_column_schema), columns)
                        .context(ReadingFile)?;

                    batches.push(Arc::new(new_batch));
                    if limit.map(|l| total_rows >= l).unwrap_or(false) {
                        break;
                    }
                }
                None => {
                    break;
                }
                Some(Err(e)) => {
                    return Err(e).context(ReadingFile);
                }
            }
        }

        Ok(())
    }

    pub async fn load_parquet_data_from_object_store(
        path: Path,
        store: Arc<ObjectStore>,
    ) -> Result<Vec<u8>> {
        store
            .get(&path)
            .await
            .context(ReadingObjectStore)?
            .map_ok(|bytes| bytes.to_vec())
            .try_concat()
            .await
            .context(ReadingObjectStore)
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

/// Read IOx schema from parquet metadata.
pub fn read_schema_from_parquet_metadata(parquet_md: &ParquetMetaData) -> Result<Schema> {
    let file_metadata = parquet_md.file_metadata();

    let arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )
    .context(ArrowFromParquetFailure {})?;

    let arrow_schema_ref = Arc::new(arrow_schema);

    let schema: Schema = arrow_schema_ref
        .try_into()
        .context(IoxFromArrowFailure {})?;
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::utils::make_object_store;
    use object_store::parsed_path;

    #[test]
    fn test_location_to_from_path() {
        let server_id = ServerId::new(NonZeroU32::new(1).unwrap());
        let store = Storage::new(make_object_store(), server_id, "my_db".to_string());

        // happy roundtrip
        let path = store.location("p1".to_string(), 42, "my_table".to_string());
        assert_eq!(path.display(), "1/my_db/data/p1/42/my_table.parquet");
        assert_eq!(
            store.parse_location(path).unwrap(),
            ("p1".to_string(), 42, "my_table".to_string())
        );

        // error cases
        assert!(store.parse_location(parsed_path!()).is_err());
        assert!(store
            .parse_location(parsed_path!(["too", "short"], "my_table.parquet"))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(
                ["this", "is", "way", "way", "too", "long"],
                "my_table.parquet"
            ))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(
                ["1", "my_db", "data", "p1", "not_a_number"],
                "my_table.parquet"
            ))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(
                ["1", "my_db", "not_data", "p1", "42"],
                "my_table.parquet"
            ))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(
                ["1", "other_db", "data", "p1", "42"],
                "my_table.parquet"
            ))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(
                ["2", "my_db", "data", "p1", "42"],
                "my_table.parquet"
            ))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(["1", "my_db", "data", "p1", "42"], "my_table"))
            .is_err());
        assert!(store
            .parse_location(parsed_path!(
                ["1", "my_db", "data", "p1", "42"],
                "my_table.parquet.tmp"
            ))
            .is_err());
    }
}
