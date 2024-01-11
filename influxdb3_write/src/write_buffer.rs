//! Implementation of an in-memory buffer for writes

use crate::catalog::{Catalog, DatabaseSchema, TableDefinition};
use crate::{
    BufferSegment, BufferedWriteRequest, Bufferer, ChunkContainer, SegmentId, Wal, WriteBuffer,
};
use arrow::array::ArrayRef;
use arrow::{
    array::{
        BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, StringDictionaryBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
    datatypes::Int32Type,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use data_types::{
    column_type_from_field, ChunkId, ChunkOrder, ColumnType, NamespaceName, PartitionKey, TableId,
    TimestampMinMax, TransitionPartitionId,
};
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use iox_catalog::TIME_COLUMN;
use iox_query::chunk_statistics::{create_chunk_statistics, ColumnRange};
use iox_query::{QueryChunk, QueryChunkData};
use observability_deps::tracing::{debug, info};
use parking_lot::RwLock;
use schema::sort::SortKey;
use schema::Schema;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error parsing line {line_number}: {message}")]
    ParseError { line_number: usize, message: String },

    #[error("column type mismatch for column {name}: existing: {existing:?}, new: {new:?}")]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },
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
    buffered_data: RwLock<HashMap<String, DatabaseBuffer>>,
    #[allow(dead_code)]
    wal: Option<Arc<W>>,
}

impl<W: Wal> WriteBufferImpl<W> {
    pub fn new(catalog: Arc<Catalog>, wal: Option<Arc<W>>) -> Self {
        Self {
            catalog,
            buffered_data: RwLock::new(HashMap::new()),
            wal,
        }
    }

    // TODO: write into segments and wal
    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        default_time: i64,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);
        let (sequence, db) = self.catalog.db_or_create(db_name.as_str());
        let result = parse_validate_and_update_schema(
            lp,
            &db,
            &Partitioner::new_per_day_partitioner(),
            default_time,
        )?;

        if let Some(schema) = result.schema {
            debug!("replacing schema for {:?}", schema);
            self.catalog
                .replace_database(sequence, Arc::new(schema))
                .unwrap();
        }

        let mut buffered_data = self.buffered_data.write();
        let db_buffer = buffered_data
            .entry(db_name.as_str().to_string())
            .or_default();
        for (table_name, table_batch) in result.table_batches {
            let table_buffer = db_buffer.table_buffers.entry(table_name).or_default();
            for (partition_key, partition_batch) in table_batch.partition_batches {
                let partition_buffer = table_buffer
                    .partition_buffers
                    .entry(partition_key)
                    .or_default();
                partition_buffer.rows.extend(partition_batch.rows);
            }
        }

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: vec![],
            line_count: result.line_count,
            field_count: result.field_count,
            tag_count: result.tag_count,
            total_buffer_memory_used: 0,
            segment_id: SegmentId(0),
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
        let db_schema = self.catalog.db_schema(database_name).unwrap();
        let table = db_schema.tables.get(table_name).unwrap();
        let schema = table.schema.as_ref().cloned().unwrap();

        let table_buffer = self.clone_table_buffer(database_name, table_name).unwrap();

        let mut chunks = Vec::with_capacity(table_buffer.partition_buffers.len());

        for (partition_key, partition_buffer) in table_buffer.partition_buffers {
            let partition_key: PartitionKey = partition_key.into();
            let batch = partition_buffer.rows_to_record_batch(&schema, table.columns());
            let column_ranges = Arc::new(partition_buffer.column_ranges);
            let batch_stats = create_chunk_statistics(
                partition_buffer.rows.len() as u64,
                &schema,
                Some(TimestampMinMax {
                    min: partition_buffer.timestamp_min,
                    max: partition_buffer.timestamp_max,
                }),
                &column_ranges,
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
        let binding = self.buffered_data.read();
        let table_buffer = binding.get(database_name)?.table_buffers.get(table_name)?;
        Some(table_buffer.clone())
    }
}

#[async_trait]
impl<W: Wal> Bufferer<W> for WriteBufferImpl<W> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        default_time: i64,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(database, lp, default_time).await
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

    fn wal(&self) -> Option<Arc<W>> {
        self.wal.clone()
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

impl<W: Wal> WriteBuffer<W> for WriteBufferImpl<W> {}

#[derive(Debug, Default)]
struct DatabaseBuffer {
    table_buffers: HashMap<String, TableBuffer>,
}

#[derive(Debug, Default, Clone)]
struct TableBuffer {
    partition_buffers: HashMap<String, PartitionBuffer>,
}

#[derive(Debug, Default, Clone)]
struct PartitionBuffer {
    rows: Vec<Row>,
    column_ranges: HashMap<Arc<str>, ColumnRange>,
    timestamp_min: i64,
    timestamp_max: i64,
}

impl PartitionBuffer {
    fn rows_to_record_batch(
        &self,
        schema: &Schema,
        column_types: &BTreeMap<String, ColumnType>,
    ) -> RecordBatch {
        let row_count = self.rows.len();
        let mut columns = BTreeMap::new();
        for (name, column_type) in column_types {
            match column_type {
                ColumnType::Bool => columns.insert(
                    name,
                    Builder::Bool(BooleanBuilder::with_capacity(row_count)),
                ),
                ColumnType::F64 => {
                    columns.insert(name, Builder::F64(Float64Builder::with_capacity(row_count)))
                }
                ColumnType::I64 => {
                    columns.insert(name, Builder::I64(Int64Builder::with_capacity(row_count)))
                }
                ColumnType::U64 => {
                    columns.insert(name, Builder::U64(UInt64Builder::with_capacity(row_count)))
                }
                ColumnType::String => columns.insert(name, Builder::String(StringBuilder::new())),
                ColumnType::Tag => {
                    columns.insert(name, Builder::Tag(StringDictionaryBuilder::new()))
                }
                ColumnType::Time => columns.insert(
                    name,
                    Builder::Time(TimestampNanosecondBuilder::with_capacity(row_count)),
                ),
            };
        }

        for r in &self.rows {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in &r.fields {
                let builder = columns.get_mut(&f.name).unwrap();
                match (&f.value, builder) {
                    (FieldData::Timestamp(v), Builder::Time(b)) => b.append_value(*v),
                    (FieldData::Tag(v), Builder::Tag(b)) => {
                        b.append(v).unwrap();
                    }
                    (FieldData::String(v), Builder::String(b)) => b.append_value(v),
                    (FieldData::Integer(v), Builder::I64(b)) => b.append_value(*v),
                    (FieldData::UInteger(v), Builder::U64(b)) => b.append_value(*v),
                    (FieldData::Float(v), Builder::F64(b)) => b.append_value(*v),
                    (FieldData::Boolean(v), Builder::Bool(b)) => b.append_value(*v),
                    _ => panic!("unexpected field type"),
                }
                value_added.insert(&f.name);
            }

            for (name, builder) in &mut columns {
                if !value_added.contains(name) {
                    match builder {
                        Builder::Bool(b) => b.append_null(),
                        Builder::F64(b) => b.append_null(),
                        Builder::I64(b) => b.append_null(),
                        Builder::U64(b) => b.append_null(),
                        Builder::String(b) => b.append_null(),
                        Builder::Tag(b) => b.append_null(),
                        Builder::Time(b) => b.append_null(),
                    }
                }
            }
        }

        // ensure the order of the columns matches their order in the Arrow schema definition
        let mut cols = Vec::with_capacity(columns.len());
        let schema = schema.as_arrow();
        for f in &schema.fields {
            cols.push(columns.remove(f.name()).unwrap().into_arrow());
        }

        RecordBatch::try_new(schema, cols).unwrap()
    }
}

enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

impl Builder {
    fn into_arrow(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::U64(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => Arc::new(b.finish()),
            Self::Tag(mut b) => Arc::new(b.finish()),
            Self::Time(mut b) => Arc::new(b.finish()),
        }
    }
}

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

/// Takes &str of line protocol, parses lines, validates the schema, and inserts new columns
/// and partitions if present. Assigns the default time to any lines that do not include a time
pub(crate) fn parse_validate_and_update_schema(
    lp: &str,
    schema: &DatabaseSchema,
    partitioner: &Partitioner,
    default_time: i64,
) -> Result<ValidationResult> {
    let mut lines = vec![];
    for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
        let line = maybe_line.map_err(|e| Error::ParseError {
            line_number: line_idx + 1,
            message: e.to_string(),
        })?;
        lines.push(line);
    }

    validate_or_insert_schema_and_partitions(lines, schema, partitioner, default_time)
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
                        new_cols.push((tag_key.to_string(), ColumnType::Tag));
                    }
                }
            }
            for (field_name, value) in &line.field_set {
                if !t.column_exists(field_name.as_str()) {
                    new_cols.push((field_name.to_string(), column_type_from_field(value)));
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
                    columns.insert(tag_key.to_string(), ColumnType::Tag);
                }
            }
            for (field_name, value) in &line.field_set {
                columns.insert(field_name.to_string(), column_type_from_field(value));
            }

            columns.insert(TIME_COLUMN.to_string(), ColumnType::Time);

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
    let time_value = line.timestamp.unwrap_or(default_time);
    values.push(Field {
        name: TIME_COLUMN.to_string(),
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct TableBatch {
    #[allow(dead_code)]
    pub(crate) name: String,
    // map of partition key to partition batch
    pub(crate) partition_batches: HashMap<String, PartitionBatch>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct PartitionBatch {
    pub(crate) rows: Vec<Row>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Row {
    pub(crate) time: i64,
    pub(crate) fields: Vec<Field>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) value: FieldData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum FieldData {
    Timestamp(i64),
    Tag(String),
    String(String),
    Integer(i64),
    UInteger(u64),
    Float(f64),
    Boolean(bool),
}

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
    use std::sync::Arc;

    #[test]
    fn parse_lp_into_buffer() {
        let db = Arc::new(DatabaseSchema::new("foo"));
        let partitioner = Partitioner::new_per_day_partitioner();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        let result = parse_validate_and_update_schema(lp, &db, &partitioner, 0).unwrap();

        println!("result: {:#?}", result);
        let db = result.schema.unwrap();

        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.tables.get("cpu").unwrap().columns().len(), 3);
        assert_eq!(db.tables.get("foo").unwrap().columns().len(), 2);
    }
}
