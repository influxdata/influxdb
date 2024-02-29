//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub(crate) mod buffer_segment;
mod flusher;
mod loader;

use crate::catalog::{Catalog, DatabaseSchema, TableDefinition, TIME_COLUMN_NAME};
use crate::write_buffer::buffer_segment::{ClosedBufferSegment, OpenBufferSegment, TableBuffer};
use crate::write_buffer::flusher::WriteBufferFlusher;
use crate::write_buffer::loader::load_starting_state;
use crate::{
    persister, BufferSegment, BufferedWriteRequest, Bufferer, ChunkContainer, LpWriteOp, Persister,
    Precision, SegmentId, Wal, WalOp, WriteBuffer, WriteLineError,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use data_types::{
    column_type_from_field, ChunkId, ChunkOrder, ColumnType, NamespaceName, PartitionKey, TableId,
    TransitionPartitionId,
};
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::{QueryChunk, QueryChunkData};
use observability_deps::tracing::{debug, info};
use parking_lot::RwLock;
use schema::sort::SortKey;
use schema::Schema;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl<W> {
    catalog: Arc<Catalog>,
    segment_state: Arc<RwLock<SegmentState>>,
    #[allow(dead_code)]
    wal: Option<Arc<W>>,
    write_buffer_flusher: WriteBufferFlusher,
}

#[derive(Debug)]
struct SegmentState {
    open_segment: OpenBufferSegment,
    #[allow(dead_code)]
    persisting_segments: Vec<ClosedBufferSegment>,
}

impl SegmentState {
    pub fn new(open_segment: OpenBufferSegment) -> Self {
        Self {
            open_segment,
            persisting_segments: vec![],
        }
    }
}

impl<W: Wal> WriteBufferImpl<W> {
    pub async fn new(
        persister: Arc<dyn Persister<Error = persister::Error>>,
        wal: Option<Arc<W>>,
    ) -> Result<Self> {
        let loaded_state = load_starting_state(persister, wal.clone()).await?;
        let segment_state = Arc::new(RwLock::new(SegmentState::new(loaded_state.open_segment)));

        let write_buffer_flusher = WriteBufferFlusher::new(Arc::clone(&segment_state));

        Ok(Self {
            catalog: loaded_state.catalog,
            segment_state,
            wal,
            write_buffer_flusher,
        })
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        default_time: i64,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);

        let result = parse_validate_and_update_catalog(
            db_name.as_str(),
            lp,
            &self.catalog,
            default_time,
            accept_partial,
            precision,
        )?;

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: result.lp_valid,
            default_time,
        });

        let write_summary = self
            .write_buffer_flusher
            .write_to_open_segment(db_name.clone(), result.table_batches, wal_op)
            .await?;

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: result.errors,
            line_count: result.line_count,
            field_count: result.field_count,
            tag_count: result.tag_count,
            total_buffer_memory_used: write_summary.buffer_size,
            segment_id: write_summary.segment_id,
            sequence_number: write_summary.sequence_number,
        })
    }

    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        _filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let db_schema = self
            .catalog
            .db_schema(database_name)
            .ok_or_else(|| DataFusionError::Execution(format!("db {} not found", database_name)))?;
        let table = db_schema
            .tables
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;
        let schema = table.schema.clone();

        let table_buffer = self
            .clone_table_buffer(database_name, table_name)
            .unwrap_or_default();

        let mut chunks = Vec::with_capacity(table_buffer.partition_buffers.len());

        for (partition_key, partition_buffer) in table_buffer.partition_buffers {
            let partition_key: PartitionKey = partition_key.into();
            let batch = partition_buffer.rows_to_record_batch(&schema, table.columns());
            let batch_stats = create_chunk_statistics(
                Some(partition_buffer.row_count()),
                &schema,
                Some(partition_buffer.timestamp_min_max()),
                None,
            );

            let chunk = BufferChunk {
                batches: vec![batch],
                schema: schema.clone(),
                stats: Arc::new(batch_stats),
                partition_id: TransitionPartitionId::new(TableId::new(0), &partition_key),
                sort_key: None,
                id: ChunkId::new(),
                chunk_order: ChunkOrder::new(0),
            };

            chunks.push(Arc::new(chunk) as _);
        }

        Ok(chunks)
    }

    fn clone_table_buffer(&self, database_name: &str, table_name: &str) -> Option<TableBuffer> {
        let state = self.segment_state.read();
        state.open_segment.table_buffer(database_name, table_name)
    }

    #[cfg(test)]
    fn get_table_record_batches(&self, datbase_name: &str, table_name: &str) -> Vec<RecordBatch> {
        let db_schema = self.catalog.db_schema(datbase_name).unwrap();
        let table = db_schema.tables.get(table_name).unwrap();
        let schema = table.schema.clone();

        let table_buffer = self.clone_table_buffer(datbase_name, table_name).unwrap();

        let mut batches = Vec::with_capacity(table_buffer.partition_buffers.len());

        for (_, partition_buffer) in table_buffer.partition_buffers {
            let batch = partition_buffer.rows_to_record_batch(&schema, table.columns());
            batches.push(batch);
        }

        batches
    }
}

#[async_trait]
impl<W: Wal> Bufferer for WriteBufferImpl<W> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        default_time: i64,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(database, lp, default_time, accept_partial, precision)
            .await
    }

    async fn close_open_segment(&self) -> crate::Result<Arc<dyn BufferSegment>> {
        todo!()
    }

    async fn load_segments_after(
        &self,
        _segment_id: SegmentId,
        _catalog: Catalog,
    ) -> crate::Result<Vec<Arc<dyn BufferSegment>>> {
        todo!()
    }

    fn wal(&self) -> Option<Arc<impl Wal>> {
        self.wal.clone()
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog()
    }
}

impl<W: Wal> ChunkContainer for WriteBufferImpl<W> {
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

impl<W: Wal> WriteBuffer for WriteBufferImpl<W> {}

#[derive(Debug)]
pub struct BufferChunk {
    batches: Vec<RecordBatch>,
    schema: Schema,
    stats: Arc<Statistics>,
    partition_id: data_types::partition::TransitionPartitionId,
    sort_key: Option<SortKey>,
    id: data_types::ChunkId,
    chunk_order: data_types::ChunkOrder,
}

impl QueryChunk for BufferChunk {
    fn stats(&self) -> Arc<Statistics> {
        info!("BufferChunk stats {}", self.id);
        Arc::clone(&self.stats)
    }

    fn schema(&self) -> &Schema {
        info!("BufferChunk schema {}", self.id);
        &self.schema
    }

    fn partition_id(&self) -> &data_types::partition::TransitionPartitionId {
        info!("BufferChunk partition_id {}", self.id);
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        info!("BufferChunk sort_key {}", self.id);
        self.sort_key.as_ref()
    }

    fn id(&self) -> data_types::ChunkId {
        info!("BufferChunk id {}", self.id);
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn data(&self) -> QueryChunkData {
        info!("BufferChunk data {}", self.id);
        QueryChunkData::in_mem(self.batches.clone(), Arc::clone(self.schema.inner()))
    }

    fn chunk_type(&self) -> &str {
        "BufferChunk"
    }

    fn order(&self) -> data_types::ChunkOrder {
        info!("BufferChunk order {}", self.id);
        self.chunk_order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

const YEAR_MONTH_DAY_TIME_FORMAT: &str = "%Y-%m-%d";

pub(crate) fn parse_validate_and_update_catalog(
    db_name: &str,
    lp: &str,
    catalog: &Catalog,
    default_time: i64,
    accept_partial: bool,
    precision: Precision,
) -> Result<ValidationResult> {
    let (sequence, db) = catalog.db_or_create(db_name);
    let mut result = parse_validate_and_update_schema(
        lp,
        &db,
        &Partitioner::new_per_day_partitioner(),
        default_time,
        accept_partial,
        precision,
    )?;

    if let Some(schema) = result.schema.take() {
        debug!("replacing schema for {:?}", schema);

        catalog.replace_database(sequence, Arc::new(schema))?;
    }

    Ok(result)
}

/// Takes &str of line protocol, parses lines, validates the schema, and inserts new columns
/// and partitions if present. Assigns the default time to any lines that do not include a time
pub(crate) fn parse_validate_and_update_schema(
    lp: &str,
    schema: &DatabaseSchema,
    partitioner: &Partitioner,
    default_time: i64,
    accept_partial: bool,
    precision: Precision,
) -> Result<ValidationResult> {
    let mut lines = vec![];
    let mut errors = vec![];
    let mut valid_lines = vec![];
    let mut lp_lines = lp.lines();

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
        valid_lines.push(lp_lines.next().unwrap());
        lines.push(line);
    }

    validate_or_insert_schema_and_partitions(lines, schema, partitioner, default_time, precision)
        .map(move |mut result| {
            result.lp_valid = valid_lines.join("\n");
            result.errors = errors;
            result
        })
}

/// Takes parsed lines, validates their schema. If new tables or columns are defined, they
/// are passed back as a new DatabaseSchema as part of the ValidationResult. Lines are split
/// into partitions and the validation result contains the data that can then be serialized
/// into the WAL.
pub(crate) fn validate_or_insert_schema_and_partitions(
    lines: Vec<ParsedLine<'_>>,
    schema: &DatabaseSchema,
    partitioner: &Partitioner,
    default_time: i64,
    precision: Precision,
) -> Result<ValidationResult> {
    // The (potentially updated) DatabaseSchema to return to the caller.
    let mut schema = Cow::Borrowed(schema);

    // The parsed and validated table_batches
    let mut table_batches: HashMap<String, TableBatch> = HashMap::new();

    let line_count = lines.len();
    let mut field_count = 0;
    let mut tag_count = 0;

    for line in lines.into_iter() {
        field_count += line.field_set.len();
        tag_count += line.series.tag_set.as_ref().map(|t| t.len()).unwrap_or(0);

        validate_and_convert_parsed_line(
            line,
            &mut table_batches,
            &mut schema,
            partitioner,
            default_time,
            precision,
        )?;
    }

    let schema = match schema {
        Cow::Owned(s) => Some(s),
        Cow::Borrowed(_) => None,
    };

    Ok(ValidationResult {
        schema,
        table_batches,
        line_count,
        field_count,
        tag_count,
        errors: vec![],
        lp_valid: String::new(),
    })
}

// &mut Cow is used to avoid a copy, so allow it
#[allow(clippy::ptr_arg)]
fn validate_and_convert_parsed_line(
    line: ParsedLine<'_>,
    table_batches: &mut HashMap<String, TableBatch>,
    schema: &mut Cow<'_, DatabaseSchema>,
    partitioner: &Partitioner,
    default_time: i64,
    precision: Precision,
) -> Result<()> {
    let table_name = line.series.measurement.as_str();

    // Check if the table exists in the schema.
    //
    // Because the entry API requires &mut it is not used to avoid a premature
    // clone of the Cow.
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

    let partition_key = partitioner.partition_key_for_line(&line, default_time);

    // now that we've ensured all columns exist in the schema, construct the actual row and values
    // while validating the column types match.
    let mut values = Vec::with_capacity(line.column_count() + 1);

    // validate tags, collecting any new ones that must be inserted, or adding the values
    if let Some(tagset) = line.series.tag_set {
        for (tag_key, value) in tagset {
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
    let time_value = line
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
        .unwrap_or(default_time);
    values.push(Field {
        name: TIME_COLUMN_NAME.to_string(),
        value: FieldData::Timestamp(time_value),
    });

    let table_batch = table_batches.entry(table_name.to_string()).or_default();
    let partition_batch = table_batch
        .partition_batches
        .entry(partition_key)
        .or_default();

    // insert the row into the partition batch
    partition_batch.rows.push(Row {
        time: time_value,
        fields: values,
    });

    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct TableBatch {
    #[allow(dead_code)]
    pub(crate) name: String,
    // map of partition key to partition batch
    pub(crate) partition_batches: HashMap<String, PartitionBatch>,
}

#[derive(Debug, Default)]
pub(crate) struct PartitionBatch {
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
    /// Map of table name to TableBatch
    pub(crate) table_batches: HashMap<String, TableBatch>,
    /// Number of lines passed in
    pub(crate) line_count: usize,
    /// Number of fields passed in
    pub(crate) field_count: usize,
    /// Number of tags passed in
    pub(crate) tag_count: usize,
    /// Any errors that ocurred while parsing the lines
    pub(crate) errors: Vec<crate::WriteLineError>,
    /// Only valid lines from what was passed in to validate
    pub(crate) lp_valid: String,
}

/// Generates the partition key for a given line or row
#[derive(Debug)]
pub struct Partitioner {
    time_format: String,
}

impl Partitioner {
    /// Create a new time based partitioner using the time format
    pub fn new_time_partitioner(time_format: impl Into<String>) -> Self {
        Self {
            time_format: time_format.into(),
        }
    }

    /// Create a new time based partitioner that partitions by day
    pub fn new_per_day_partitioner() -> Self {
        Self::new_time_partitioner(YEAR_MONTH_DAY_TIME_FORMAT)
    }

    /// Given a parsed line and a default time, generate the string partition key
    pub fn partition_key_for_line(&self, line: &ParsedLine<'_>, default_time: i64) -> String {
        let timestamp = line.timestamp.unwrap_or(default_time);
        format!(
            "{}",
            Utc.timestamp_nanos(timestamp).format(&self.time_format)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persister::PersisterImpl;
    use crate::wal::WalImpl;
    use crate::{SequenceNumber, WalOpBatch};
    use arrow_util::assert_batches_eq;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    #[test]
    fn parse_lp_into_buffer() {
        let db = Arc::new(DatabaseSchema::new("foo"));
        let partitioner = Partitioner::new_per_day_partitioner();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        let result = parse_validate_and_update_schema(
            lp,
            &db,
            &partitioner,
            0,
            false,
            Precision::Nanosecond,
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
        let persister: Arc<dyn Persister<Error = persister::Error>> =
            Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let write_buffer = WriteBufferImpl::new(Arc::clone(&persister), Some(Arc::new(wal)))
            .await
            .unwrap();

        let summary = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                123,
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        assert_eq!(summary.line_count, 1);
        assert_eq!(summary.field_count, 1);
        assert_eq!(summary.tag_count, 0);
        assert_eq!(summary.total_buffer_memory_used, 1);
        assert_eq!(summary.segment_id, SegmentId::new(1));
        assert_eq!(summary.sequence_number, SequenceNumber::new(1));

        // ensure the data is in the buffer
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        let expected = vec![
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
            })],
        };
        assert_eq!(batch, expected_batch);

        // ensure we load state from the persister
        let write_buffer = WriteBufferImpl::new(persister, Some(Arc::new(wal)))
            .await
            .unwrap();
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        assert_batches_eq!(&expected, &actual);
    }
}
