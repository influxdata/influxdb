//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub(crate) mod buffer_segment;
mod flusher;
mod loader;
mod segment_state;
mod table_buffer;

use crate::cache::ParquetCache;
use crate::catalog::{Catalog, DatabaseSchema, TableDefinition, TIME_COLUMN_NAME};
use crate::chunk::ParquetChunk;
use crate::persister::PersisterImpl;
use crate::write_buffer::flusher::WriteBufferFlusher;
use crate::write_buffer::loader::load_starting_state;
use crate::write_buffer::segment_state::{run_buffer_segment_persist_and_cleanup, SegmentState};
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, LpWriteOp, Persister, Precision,
    SegmentDuration, SequenceNumber, Wal, WalOp, WriteBuffer, WriteLineError,
};
use async_trait::async_trait;
use data_types::{
    column_type_from_field, ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError,
};
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::path::Path as ObjPath;
use object_store::ObjectMeta;
use observability_deps::tracing::{debug, error};
use parking_lot::{Mutex, RwLock};
use parquet_file::storage::ParquetExecInput;
use sha2::Digest;
use sha2::Sha256;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::i64;
use std::sync::{Arc, OnceLock};
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parsing for line protocol failed")]
    ParseError(WriteLineError),

    #[error("column type mismatch for column {name}: existing: {existing:?}, new: {new:?}")]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[error("catalog update erorr {0}")]
    CatalogUpdateError(#[from] crate::catalog::Error),

    #[error("error from wal: {0}")]
    WalError(#[from] crate::wal::Error),

    #[error("error from buffer segment: {0}")]
    BufferSegmentError(String),

    #[error("error from persister: {0}")]
    PersisterError(#[from] crate::persister::Error),

    #[error("corrupt load state: {0}")]
    CorruptLoadState(String),

    #[error("database name error: {0}")]
    DatabaseNameError(#[from] NamespaceNameError),

    #[error("walop in file {0} contained data for more than one segment, which is invalid")]
    WalOpForMultipleSegments(String),

    #[error("error from table buffer: {0}")]
    TableBufferError(#[from] table_buffer::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl<W, T> {
    catalog: Arc<Catalog>,
    persister: Arc<PersisterImpl>,
    parquet_cache: Arc<ParquetCache>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    wal: Option<Arc<W>>,
    write_buffer_flusher: WriteBufferFlusher,
    segment_duration: SegmentDuration,
    #[allow(dead_code)]
    time_provider: Arc<T>,
    #[allow(dead_code)]
    segment_persist_handle: Mutex<tokio::task::JoinHandle<()>>,
    #[allow(dead_code)]
    shutdown_segment_persist_tx: watch::Sender<()>,
}

impl<W: Wal, T: TimeProvider> WriteBufferImpl<W, T> {
    pub async fn new(
        persister: Arc<PersisterImpl>,
        wal: Option<Arc<W>>,
        time_provider: Arc<T>,
        segment_duration: SegmentDuration,
        executor: Arc<iox_query::exec::Executor>,
    ) -> Result<Self> {
        let now = time_provider.now();
        let loaded_state = load_starting_state(
            Arc::clone(&persister) as _,
            wal.clone(),
            now,
            segment_duration,
        )
        .await?;

        let segment_state = Arc::new(RwLock::new(SegmentState::new(
            segment_duration,
            loaded_state.last_segment_id,
            Arc::clone(&loaded_state.catalog),
            Arc::clone(&time_provider),
            loaded_state.open_segments,
            loaded_state.persisting_buffer_segments,
            loaded_state.persisted_segments,
            wal.clone(),
        )));

        let write_buffer_flusher = WriteBufferFlusher::new(Arc::clone(&segment_state));

        let segment_state_persister = Arc::clone(&segment_state);
        let time_provider_persister = Arc::clone(&time_provider);
        let wal_perister = wal.clone();
        let cloned_persister = Arc::clone(&persister);

        let (shutdown_segment_persist_tx, shutdown_rx) = watch::channel(());
        let segment_persist_handle = tokio::task::spawn(async move {
            run_buffer_segment_persist_and_cleanup(
                cloned_persister,
                segment_state_persister,
                shutdown_rx,
                time_provider_persister,
                wal_perister,
                executor,
            )
            .await;
        });

        Ok(Self {
            catalog: loaded_state.catalog,
            segment_state,
            parquet_cache: Arc::new(ParquetCache::new(&persister.mem_pool)),
            persister,
            wal,
            write_buffer_flusher,
            time_provider,
            segment_duration,
            segment_persist_handle: Mutex::new(segment_persist_handle),
            shutdown_segment_persist_tx,
        })
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);

        let result = parse_validate_and_update_catalog(
            db_name.clone(),
            lp,
            &self.catalog,
            ingest_time,
            self.segment_duration,
            accept_partial,
            precision,
        )?;

        self.write_buffer_flusher
            .write_to_open_segment(result.valid_segmented_data)
            .await?;

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: result.errors,
            line_count: result.line_count,
            field_count: result.field_count,
            tag_count: result.tag_count,
        })
    }

    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let db_schema = self
            .catalog
            .db_schema(database_name)
            .ok_or_else(|| DataFusionError::Execution(format!("db {} not found", database_name)))?;

        let table_schema = {
            let table = db_schema.tables.get(table_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "table {} not found in db {}",
                    table_name, database_name
                ))
            })?;

            table.schema.clone()
        };

        let segment_state = self.segment_state.read();
        let mut chunks =
            segment_state.get_table_chunks(db_schema, table_name, filters, projection, ctx)?;
        let parquet_files = segment_state.get_parquet_files(database_name, table_name);

        let mut chunk_order = chunks.len() as i64;
        let object_store_url = self.persister.object_store_url();

        for parquet_file in parquet_files {
            // TODO: update persisted segments to serialize their key to use here
            let partition_key = data_types::PartitionKey::from(parquet_file.path.clone());
            let partition_id = data_types::partition::TransitionPartitionId::new(
                data_types::TableId::new(0),
                &partition_key,
            );

            let chunk_stats = create_chunk_statistics(
                Some(parquet_file.row_count as usize),
                &table_schema,
                Some(parquet_file.timestamp_min_max()),
                None,
            );

            let location = ObjPath::from(parquet_file.path.clone());

            let parquet_exec = ParquetExecInput {
                object_store_url: object_store_url.clone(),
                object_meta: ObjectMeta {
                    location,
                    last_modified: Default::default(),
                    size: parquet_file.size_bytes as usize,
                    e_tag: None,
                    version: None,
                },
                object_store: self.persister.object_store(),
            };

            let parquet_chunk = ParquetChunk {
                schema: table_schema.clone(),
                stats: Arc::new(chunk_stats),
                partition_id,
                sort_key: None,
                id: ChunkId::new(),
                chunk_order: ChunkOrder::new(chunk_order),
                parquet_exec,
            };

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        // Get any cached files and add them to the query
        // This is mostly the same as above, but we change the object store to
        // point to the in memory cache
        for parquet_file in self
            .parquet_cache
            .get_parquet_files(database_name, table_name)
        {
            let partition_key = data_types::PartitionKey::from(parquet_file.path.clone());
            let partition_id = data_types::partition::TransitionPartitionId::new(
                data_types::TableId::new(0),
                &partition_key,
            );

            let chunk_stats = create_chunk_statistics(
                Some(parquet_file.row_count as usize),
                &table_schema,
                Some(parquet_file.timestamp_min_max()),
                None,
            );

            let location = ObjPath::from(parquet_file.path.clone());

            let parquet_exec = ParquetExecInput {
                object_store_url: object_store_url.clone(),
                object_meta: ObjectMeta {
                    location,
                    last_modified: Default::default(),
                    size: parquet_file.size_bytes as usize,
                    e_tag: None,
                    version: None,
                },
                object_store: Arc::clone(&self.parquet_cache.object_store()),
            };

            let parquet_chunk = ParquetChunk {
                schema: table_schema.clone(),
                stats: Arc::new(chunk_stats),
                partition_id,
                sort_key: None,
                id: ChunkId::new(),
                chunk_order: ChunkOrder::new(chunk_order),
                parquet_exec,
            };

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
    }

    pub async fn cache_parquet(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        records: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        Ok(self
            .parquet_cache
            .persist_parquet_file(db_name, table_name, min_time, max_time, records, None)
            .await?)
    }

    pub async fn update_parquet(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        path: ObjPath,
        records: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        Ok(self
            .parquet_cache
            .persist_parquet_file(db_name, table_name, min_time, max_time, records, Some(path))
            .await?)
    }

    pub async fn remove_parquet(&self, path: ObjPath) -> Result<(), Error> {
        Ok(self.parquet_cache.remove_parquet_file(path).await?)
    }

    pub async fn purge_cache(&self) -> Result<(), Error> {
        Ok(self.parquet_cache.purge_cache().await?)
    }

    #[cfg(test)]
    fn get_table_record_batches(
        &self,
        datbase_name: &str,
        table_name: &str,
    ) -> Vec<arrow::record_batch::RecordBatch> {
        let db_schema = self.catalog.db_schema(datbase_name).unwrap();
        let table = db_schema.tables.get(table_name).unwrap();
        let schema = table.schema.clone();

        let segment_state = self.segment_state.read();
        segment_state.open_segments_table_record_batches(datbase_name, table_name, &schema)
    }
}

#[async_trait]
impl<W: Wal, T: TimeProvider> Bufferer for WriteBufferImpl<W, T> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    fn wal(&self) -> Option<Arc<impl Wal>> {
        self.wal.clone()
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog()
    }

    fn persister(&self) -> Arc<impl Persister> {
        Arc::clone(&self.persister)
    }
}

impl<W: Wal, T: TimeProvider> ChunkContainer for WriteBufferImpl<W, T> {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.get_table_chunks(database_name, table_name, filters, projection, ctx)
    }
}

impl<W: Wal, T: TimeProvider> WriteBuffer for WriteBufferImpl<W, T> {}

/// Returns a validated result and the sequence number of the catalog before any updates were
/// applied.
pub(crate) fn parse_validate_and_update_catalog(
    db_name: NamespaceName<'static>,
    lp: &str,
    catalog: &Catalog,
    ingest_time: Time,
    segment_duration: SegmentDuration,
    accept_partial: bool,
    precision: Precision,
) -> Result<ValidationResult> {
    let (sequence, db) = catalog.db_or_create(db_name.as_str())?;
    let mut result = parse_validate_and_update_schema(
        lp,
        &db,
        db_name,
        ingest_time,
        segment_duration,
        accept_partial,
        precision,
        sequence,
    )?;

    if let Some(schema) = result.schema.take() {
        debug!("replacing schema for {:?}", schema);

        catalog.replace_database(sequence, Arc::new(schema))?;
    }

    Ok(result)
}

/// Takes &str of line protocol, parses lines, validates the schema, and inserts new columns
/// if present. Assigns the default time to any lines that do not include a time
#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_validate_and_update_schema(
    lp: &str,
    schema: &DatabaseSchema,
    db_name: NamespaceName<'static>,
    ingest_time: Time,
    segment_duration: SegmentDuration,
    accept_partial: bool,
    precision: Precision,
    starting_catalog_sequence_number: SequenceNumber,
) -> Result<ValidationResult> {
    let mut errors = vec![];
    let mut lp_lines = lp.lines();

    let mut valid_parsed_and_raw_lines: Vec<(ParsedLine, &str)> = vec![];

    for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
        let line = match maybe_line {
            Ok(line) => line,
            Err(e) => {
                if !accept_partial {
                    return Err(Error::ParseError(WriteLineError {
                        // This unwrap is fine because we're moving line by line
                        // alongside the output from parse_lines
                        original_line: lp_lines.next().unwrap().to_string(),
                        line_number: line_idx + 1,
                        error_message: e.to_string(),
                    }));
                } else {
                    errors.push(WriteLineError {
                        original_line: lp_lines.next().unwrap().to_string(),
                        // This unwrap is fine because we're moving line by line
                        // alongside the output from parse_lines
                        line_number: line_idx + 1,
                        error_message: e.to_string(),
                    });
                }
                continue;
            }
        };
        // This unwrap is fine because we're moving line by line
        // alongside the output from parse_lines
        valid_parsed_and_raw_lines.push((line, lp_lines.next().unwrap()));
    }

    validate_or_insert_schema_and_partitions(
        valid_parsed_and_raw_lines,
        schema,
        db_name,
        ingest_time,
        segment_duration,
        precision,
        starting_catalog_sequence_number,
    )
    .map(move |mut result| {
        result.errors = errors;
        result
    })
}

/// Takes parsed lines, validates their schema. If new tables or columns are defined, they
/// are passed back as a new DatabaseSchema as part of the ValidationResult. Lines are split
/// into partitions and the validation result contains the data that can then be serialized
/// into the WAL.
pub(crate) fn validate_or_insert_schema_and_partitions(
    lines: Vec<(ParsedLine<'_>, &str)>,
    schema: &DatabaseSchema,
    db_name: NamespaceName<'static>,
    ingest_time: Time,
    segment_duration: SegmentDuration,
    precision: Precision,
    starting_catalog_sequence_number: SequenceNumber,
) -> Result<ValidationResult> {
    // The (potentially updated) DatabaseSchema to return to the caller.
    let mut schema = Cow::Borrowed(schema);

    // The parsed and validated table_batches
    let mut segment_table_batches: HashMap<Time, TableBatchMap> = HashMap::new();

    let line_count = lines.len();
    let mut field_count = 0;
    let mut tag_count = 0;

    for (line, raw_line) in lines.into_iter() {
        field_count += line.field_set.len();
        tag_count += line.series.tag_set.as_ref().map(|t| t.len()).unwrap_or(0);

        validate_and_convert_parsed_line(
            line,
            raw_line,
            &mut segment_table_batches,
            &mut schema,
            ingest_time,
            segment_duration,
            precision,
        )?;
    }

    let schema = match schema {
        Cow::Owned(s) => Some(s),
        Cow::Borrowed(_) => None,
    };

    let valid_segmented_data = segment_table_batches
        .into_iter()
        .map(|(segment_start, table_batches)| ValidSegmentedData {
            database_name: db_name.clone(),
            segment_start,
            table_batches: table_batches.table_batches,
            wal_op: WalOp::LpWrite(LpWriteOp {
                db_name: db_name.to_string(),
                lp: table_batches.lines.join("\n"),
                default_time: ingest_time.timestamp_nanos(),
                precision,
            }),
            starting_catalog_sequence_number,
        })
        .collect();

    Ok(ValidationResult {
        schema,
        line_count,
        field_count,
        tag_count,
        errors: vec![],
        valid_segmented_data,
    })
}

/// Check if the table exists in the schema and update the schema if it does not
// Because the entry API requires &mut it is not used to avoid a premature
// clone of the Cow.
fn validate_and_update_schema(line: &ParsedLine<'_>, schema: &mut Cow<'_, DatabaseSchema>) {
    let table_name = line.series.measurement.as_str();
    match schema.tables.get(table_name) {
        Some(t) => {
            // Collect new column definitions
            let mut new_cols = Vec::with_capacity(line.column_count() + 1);
            if let Some(tagset) = &line.series.tag_set {
                for (tag_key, _) in tagset {
                    if !t.column_exists(tag_key.as_str()) {
                        new_cols.push((tag_key.to_string(), ColumnType::Tag as i16));
                    }
                }
            }
            for (field_name, value) in &line.field_set {
                if !t.column_exists(field_name.as_str()) {
                    new_cols.push((field_name.to_string(), column_type_from_field(value) as i16));
                }
            }

            if !new_cols.is_empty() {
                let t = schema.to_mut().tables.get_mut(table_name).unwrap();
                t.add_columns(new_cols);
            }
        }
        None => {
            let mut columns = BTreeMap::new();
            if let Some(tag_set) = &line.series.tag_set {
                for (tag_key, _) in tag_set {
                    columns.insert(tag_key.to_string(), ColumnType::Tag as i16);
                }
            }
            for (field_name, value) in &line.field_set {
                columns.insert(field_name.to_string(), column_type_from_field(value) as i16);
            }

            columns.insert(TIME_COLUMN_NAME.to_string(), ColumnType::Time as i16);

            let table = TableDefinition::new(table_name, columns);

            assert!(schema
                .to_mut()
                .tables
                .insert(table_name.to_string(), table)
                .is_none());
        }
    };
}

fn validate_and_convert_parsed_line<'a>(
    line: ParsedLine<'_>,
    raw_line: &'a str,
    segment_table_batches: &mut HashMap<Time, TableBatchMap<'a>>,
    schema: &mut Cow<'_, DatabaseSchema>,
    ingest_time: Time,
    segment_duration: SegmentDuration,
    precision: Precision,
) -> Result<()> {
    validate_and_update_schema(&line, schema);

    // now that we've ensured all columns exist in the schema, construct the actual row and values
    // while validating the column types match.
    let mut values = Vec::with_capacity(line.column_count() + 1);

    // validate tags, collecting any new ones that must be inserted, or adding the values
    if let Some(tag_set) = line.series.tag_set {
        for (tag_key, value) in tag_set {
            let value = Field {
                name: tag_key.to_string(),
                value: FieldData::Tag(value.to_string()),
            };
            values.push(value);
        }
    }

    // validate fields, collecting any new ones that must be inserted, or adding values
    for (field_name, value) in line.field_set {
        let field_data = match value {
            FieldValue::I64(v) => FieldData::Integer(v),
            FieldValue::F64(v) => FieldData::Float(v),
            FieldValue::U64(v) => FieldData::UInteger(v),
            FieldValue::Boolean(v) => FieldData::Boolean(v),
            FieldValue::String(v) => FieldData::String(v.to_string()),
        };
        let value = Field {
            name: field_name.to_string(),
            value: field_data,
        };
        values.push(value);
    }

    // set the time value
    let time_value_nanos = line
        .timestamp
        .map(|ts| {
            let multiplier = match precision {
                Precision::Auto => match crate::guess_precision(ts) {
                    Precision::Second => 1_000_000_000,
                    Precision::Millisecond => 1_000_000,
                    Precision::Microsecond => 1_000,
                    Precision::Nanosecond => 1,

                    Precision::Auto => unreachable!(),
                },
                Precision::Second => 1_000_000_000,
                Precision::Millisecond => 1_000_000,
                Precision::Microsecond => 1_000,
                Precision::Nanosecond => 1,
            };

            ts * multiplier
        })
        .unwrap_or(ingest_time.timestamp_nanos());

    let segment_start = segment_duration.start_time(time_value_nanos / 1_000_000_000);

    values.push(Field {
        name: TIME_COLUMN_NAME.to_string(),
        value: FieldData::Timestamp(time_value_nanos),
    });

    let table_batch_map = segment_table_batches.entry(segment_start).or_default();

    let table_batch = table_batch_map
        .table_batches
        .entry(line.series.measurement.to_string())
        .or_default();
    table_batch.rows.push(Row {
        time: time_value_nanos,
        fields: values,
    });

    table_batch_map.lines.push(raw_line);

    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct TableBatch {
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) rows: Vec<Row>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Row {
    pub(crate) time: i64,
    pub(crate) fields: Vec<Field>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) value: FieldData,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldData {
    Timestamp(i64),
    Tag(String),
    String(String),
    Integer(i64),
    UInteger(u64),
    Float(f64),
    Boolean(bool),
}

impl PartialEq for FieldData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldData::Timestamp(a), FieldData::Timestamp(b)) => a == b,
            (FieldData::Tag(a), FieldData::Tag(b)) => a == b,
            (FieldData::String(a), FieldData::String(b)) => a == b,
            (FieldData::Integer(a), FieldData::Integer(b)) => a == b,
            (FieldData::UInteger(a), FieldData::UInteger(b)) => a == b,
            (FieldData::Float(a), FieldData::Float(b)) => a == b,
            (FieldData::Boolean(a), FieldData::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldData {}

/// Result of the validation. If the NamespaceSchema or PartitionMap were updated, they will be
/// in the result.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ValidationResult {
    /// If the namespace schema is updated with new tables or columns it will be here, which
    /// can be used to update the cache.
    pub(crate) schema: Option<DatabaseSchema>,
    /// Number of lines passed in
    pub(crate) line_count: usize,
    /// Number of fields passed in
    pub(crate) field_count: usize,
    /// Number of tags passed in
    pub(crate) tag_count: usize,
    /// Any errors that occurred while parsing the lines
    pub(crate) errors: Vec<crate::WriteLineError>,
    /// Only valid lines from what was passed in to validate, segmented based on the
    /// timestamps of the data.
    pub(crate) valid_segmented_data: Vec<ValidSegmentedData>,
}

#[derive(Debug)]
pub(crate) struct ValidSegmentedData {
    pub(crate) database_name: NamespaceName<'static>,
    pub(crate) segment_start: Time,
    pub(crate) table_batches: HashMap<String, TableBatch>,
    pub(crate) wal_op: WalOp,
    /// The sequence number of the catalog before any updates were applied based on this write.
    pub(crate) starting_catalog_sequence_number: SequenceNumber,
}

#[derive(Debug, Default)]
pub(crate) struct TableBatchMap<'a> {
    pub(crate) lines: Vec<&'a str>,
    pub(crate) table_batches: HashMap<String, TableBatch>,
}

/// The 32 byte SHA256 digest of the full tag set for a line of measurement data
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct SeriesId([u8; 32]);

impl std::fmt::Display for SeriesId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

fn default_series_sha() -> &'static [u8; 32] {
    static DEFAULT_SERIES_ID_SHA: OnceLock<[u8; 32]> = OnceLock::new();
    // the unwrap is safe here because the Sha256 digest will always be 32 bytes:
    DEFAULT_SERIES_ID_SHA.get_or_init(|| Sha256::digest("")[..].try_into().unwrap())
}

impl Default for SeriesId {
    fn default() -> Self {
        Self(default_series_sha().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persister::PersisterImpl;
    use crate::wal::WalImpl;
    use crate::{SegmentId, SequenceNumber, WalOpBatch};
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use datafusion_util::config::register_iox_object_store;
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    #[test]
    fn parse_lp_into_buffer() {
        let db = Arc::new(DatabaseSchema::new("foo"));
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        let result = parse_validate_and_update_schema(
            lp,
            &db,
            db_name,
            Time::from_timestamp_nanos(0),
            SegmentDuration::new_5m(),
            false,
            Precision::Nanosecond,
            SequenceNumber::new(0),
        )
        .unwrap();

        let db = result.schema.unwrap();

        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.tables.get("cpu").unwrap().columns().len(), 3);
        assert_eq!(db.tables.get("foo").unwrap().columns().len(), 2);
    }

    #[tokio::test]
    async fn buffers_and_persists_to_wal() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = WalImpl::new(dir.clone()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            Some(Arc::new(wal)),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
        )
        .await
        .unwrap();

        let summary = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        assert_eq!(summary.line_count, 1);
        assert_eq!(summary.field_count, 1);
        assert_eq!(summary.tag_count, 0);

        // ensure the data is in the buffer
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &actual);

        // ensure the data is in the wal
        let wal = WalImpl::new(dir).unwrap();
        let mut reader = wal.open_segment_reader(SegmentId::new(1)).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();
        let expected_batch = WalOpBatch {
            sequence_number: SequenceNumber::new(1),
            ops: vec![WalOp::LpWrite(LpWriteOp {
                db_name: "foo".to_string(),
                lp: "cpu bar=1 10".to_string(),
                default_time: 123,
                precision: Precision::Nanosecond,
            })],
        };
        assert_eq!(batch, expected_batch);

        // ensure we load state from the persister
        let write_buffer = WriteBufferImpl::new(
            persister,
            Some(Arc::new(wal)),
            time_provider,
            segment_duration,
            crate::test_help::make_exec(),
        )
        .await
        .unwrap();
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_buffered_persisted_and_persisting_data() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Some(Arc::new(WalImpl::new(dir.clone()).unwrap()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal.clone(),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
        )
        .await
        .unwrap();
        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // advance the time and wait for it to persist
        time_provider.set(Time::from_timestamp(800, 0).unwrap());
        loop {
            let segment_state = write_buffer.segment_state.read();
            if !segment_state.persisted_segments().is_empty() {
                break;
            }
        }

        // nothing should be open at this point
        assert!(write_buffer
            .segment_state
            .read()
            .open_segment_times()
            .is_empty());

        // verify we get the persisted data
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // now write some into the next segment we're in and verify we get both buffer and persisted
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2",
                Time::from_timestamp(900, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2.0 | 1970-01-01T00:15:00Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now reload the buffer and verify that we get persisted and the buffer again
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal,
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
        )
        .await
        .unwrap();
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now add to the buffer and verify that we still only get two chunks
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3",
                Time::from_timestamp(950, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2.0 | 1970-01-01T00:15:00Z           |",
            "| 3.0 | 1970-01-01T00:15:50Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sets_starting_catalog_number_on_new_segment() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Some(Arc::new(WalImpl::new(dir.clone()).unwrap()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal.clone(),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
        )
        .await
        .unwrap();
        let starting_catalog_sequence_number = write_buffer.catalog().sequence_number();

        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        // write data into the buffer that will go into a new segment
        let new_segment_time = Time::from_timestamp(360, 0).unwrap();
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1",
                new_segment_time,
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:06:00Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // get the segment for the new_segment_time and validate that it has the correct starting catalog sequence number
        let state = write_buffer.segment_state.read();
        let segment_start_time = SegmentDuration::new_5m().start_time(new_segment_time.timestamp());
        let segment = state.segment_for_time(segment_start_time).unwrap();
        assert_eq!(
            segment.starting_catalog_sequence_number(),
            starting_catalog_sequence_number
        );
    }

    async fn get_table_batches(
        write_buffer: &WriteBufferImpl<WalImpl, MockProvider>,
        database_name: &str,
        table_name: &str,
        ctx: &IOxSessionContext,
    ) -> Vec<RecordBatch> {
        let chunks = write_buffer
            .get_table_chunks(database_name, table_name, &[], None, &ctx.inner().state())
            .unwrap();
        let mut batches = vec![];
        for chunk in chunks {
            let chunk = chunk
                .data()
                .read_to_batches(chunk.schema(), ctx.inner())
                .await;
            batches.extend(chunk);
        }
        batches
    }
}
