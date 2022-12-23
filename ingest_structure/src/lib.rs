//! This crate specifies the ingest structure for IOx. This is able to take line protocol and
//! convert it into the Proto definition data that is used for the router to write to ingesters,
//! which can then wrap that proto into a message that gets written into the WAL and a message
//! that gets sent to ingester read replicas.
//!
//! This also contains methods to convert that proto data into Arrow `RecordBatch` given the
//! table schema of the data.
use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
        StringDictionaryBuilder, TimestampNanosecondBuilder, UInt64Builder,
    },
    datatypes::Int32Type,
    record_batch::RecordBatch,
};
use chrono::{TimeZone, Utc};
use data_types::{
    ColumnType, NamespaceSchema, PartitionId, PartitionKey, TableId, TableSchema,
};
use generated_types::influxdata::iox::wal::v1::{
    value::Value as ProtoOneOfValue, PartitionBatch as ProtoPartitionBatch, Row as ProtoRow,
    TableBatch as ProtoTableBatch, Value as ProtoValue, WriteBatch as ProtoWriteBatch,
};
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use iox_catalog::interface::Catalog;
use iox_catalog::{interface::RepoCollection, TIME_COLUMN, table_load_or_create};
use schema::{InfluxColumnType, InfluxFieldType, SchemaBuilder};
use snafu::{ResultExt, Snafu};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    sync::Arc,
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("error from catalog: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[snafu(display(
        "column {} appears multiple times in a single line. must be unique",
        name
    ))]
    ColumnNameNotUnique { name: String },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema { source: schema::builder::Error },

    #[snafu(display("Unexpected column type"))]
    UnexpectedColumnType,

    #[snafu(display("Unexpected arrow error: {}", source))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("error parsing line {} (1-based): {}", line, source))]
    LineProtocol {
        source: influxdb_line_protocol::Error,
        line: usize,
    },
}

/// A specialized `Error` for conversion errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

const YEAR_MONTH_DAY_TIME_FORMAT: &str = "%Y-%m-%d";

/// Map of table id to partitions
#[derive(Default, Debug, Clone)]
pub struct TablePartitionMap {
    /// the mapping of table id to the partitions
    pub map: BTreeMap<TableId, PartitionMap>,
}

/// Map of partition key to partition id
#[derive(Default, Debug, Clone)]
pub struct PartitionMap {
    /// the mapping of partition key to partition id
    pub map: BTreeMap<String, PartitionId>,
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

/// Mapping of `TableId` to `TableBatch` returned as part of a `ValidationResult`
pub type TableBatchMap = HashMap<TableId, TableBatch>;

/// Result of the validation. If the NamespaceSchema or PartitionMap were updated, they will be
/// in the result.
#[derive(Debug, Default)]
pub struct ValidationResult {
    /// If the namespace schema is updated with new tables or columns it will be here, which
    /// can be used to update the cache.
    pub schema: Option<NamespaceSchema>,
    /// If new partitions are created, they will be in this updated map which can be used to
    /// update the cache.
    pub table_partition_map: Option<TablePartitionMap>,
    /// The data as a map of `TableId` to `TableBatch`
    pub table_batches: TableBatchMap,
    /// Number of lines passed in
    pub line_count: usize,
    /// Number of fields passed in
    pub field_count: usize,
    /// Number of tags passed in
    pub tag_count: usize,
}

/// Converts the table_batches part of a `ValidationResult` into the `WriteBatch` proto that
/// can be sent to ingesters or written into the WAL.
pub fn table_batches_map_to_proto(table_batches: TableBatchMap) -> ProtoWriteBatch {
    let table_batches: Vec<_> = table_batches
        .into_iter()
        .map(|(table_id, table_batch)| {
            let partition_batches: Vec<_> = table_batch
                .partition_batches
                .into_iter()
                .map(|(partition_id, rows)| ProtoPartitionBatch {
                    partition_id: partition_id.get() as u32,
                    rows,
                })
                .collect();

            ProtoTableBatch {
                table_id: table_id.get() as u32,
                partition_batches,
            }
        })
        .collect();

    ProtoWriteBatch { table_batches }
}

pub fn rows_persist_cost(rows: &[ProtoRow]) -> usize {
    // initialize the cost with the size of the sequence number
    let mut cost = rows.len() * mem::size_of::<u64>();

    for r in rows {
        for v in &r.values {
            let value_cost = match &v.value {
                Some(ProtoOneOfValue::I64Value(_)) => mem::size_of::<i64>(),
                Some(ProtoOneOfValue::F64Value(_)) => mem::size_of::<f64>(),
                Some(ProtoOneOfValue::U64Value(_)) => mem::size_of::<u64>(),
                Some(ProtoOneOfValue::StringValue(s)) => mem::size_of::<char>() * s.len(),
                Some(ProtoOneOfValue::TagValue(t)) => mem::size_of::<char>() * t.len(),
                Some(ProtoOneOfValue::BoolValue(_)) => mem::size_of::<bool>(),
                Some(ProtoOneOfValue::BytesValue(b)) => b.len(),
                Some(ProtoOneOfValue::TimeValue(_)) => mem::size_of::<i64>(),
                None => 0,
            };

            cost += value_cost;
        }
    }

    cost
}

pub fn rows_to_record_batch(table_schema: &TableSchema, rows: &[ProtoRow]) -> Result<RecordBatch> {
    let mut schema_builder = SchemaBuilder::new();
    let mut column_index: HashMap<u32, ColumnData> = HashMap::new();

    for (column_name, column_schema) in table_schema.columns.iter() {
        assert!(column_index
            .insert(
                column_schema.id.get() as u32,
                ColumnData::new(column_name, column_schema.column_type, rows.len())
            )
            .is_none());
        schema_builder.influx_column(column_name, column_schema.column_type.into());
    }

    let schema = schema_builder
        .build()
        .context(InternalSchemaSnafu)?
        .sort_fields_by_name();

    for r in rows {
        let mut value_added = HashSet::with_capacity(r.values.len());

        for v in &r.values {
            let column_data = column_index.get_mut(&v.column_id).unwrap();
            column_data.append_value(&v.value)?;
            value_added.insert(v.column_id);
        }

        // insert nulls into columns that didn't get values
        for (id, data) in column_index.iter_mut() {
            if value_added.get(id).is_none() {
                data.append_null();
            }
        }
    }

    let mut cols: Vec<_> = column_index.into_values().collect();
    cols.sort_by(|a, b| Ord::cmp(&a.name, &b.name));
    let cols: Vec<ArrayRef> = cols.into_iter().map(|c| c.into_arrow()).collect();

    let batch = RecordBatch::try_new(schema.as_arrow(), cols).context(ArrowSnafu)?;
    Ok(batch)
}

struct ColumnData<'a> {
    name: &'a str,
    builder: Builder,
}

impl<'a> ColumnData<'a> {
    fn new(name: &'a str, t: ColumnType, row_count: usize) -> Self {
        match t {
            ColumnType::I64 => Self {
                name,
                builder: Builder::I64(Int64Builder::with_capacity(row_count)),
            },
            ColumnType::F64 => Self {
                name,
                builder: Builder::F64(Float64Builder::with_capacity(row_count)),
            },
            ColumnType::U64 => Self {
                name,
                builder: Builder::U64(UInt64Builder::with_capacity(row_count)),
            },
            ColumnType::Tag => Self {
                name,
                builder: Builder::Tag(StringDictionaryBuilder::new()),
            },
            ColumnType::String => Self {
                name,
                builder: Builder::String(StringBuilder::new()),
            },
            ColumnType::Time => Self {
                name,
                builder: Builder::Time(TimestampNanosecondBuilder::with_capacity(row_count)),
            },
            ColumnType::Bool => Self {
                name,
                builder: Builder::Bool(BooleanBuilder::with_capacity(row_count)),
            },
        }
    }

    fn into_arrow(self) -> ArrayRef {
        match self.builder {
            Builder::Bool(mut b) => Arc::new(b.finish()),
            Builder::I64(mut b) => Arc::new(b.finish()),
            Builder::F64(mut b) => Arc::new(b.finish()),
            Builder::U64(mut b) => Arc::new(b.finish()),
            Builder::String(mut b) => Arc::new(b.finish()),
            Builder::Tag(mut b) => Arc::new(b.finish()),
            Builder::Time(mut b) => Arc::new(b.finish()),
        }
    }

    fn append_value(&mut self, v: &Option<ProtoOneOfValue>) -> Result<()> {
        match v {
            Some(v) => match (v, &mut self.builder) {
                (ProtoOneOfValue::I64Value(v), Builder::I64(b)) => b.append_value(*v),
                (ProtoOneOfValue::F64Value(v), Builder::F64(b)) => b.append_value(*v),
                (ProtoOneOfValue::U64Value(v), Builder::U64(b)) => b.append_value(*v),
                (ProtoOneOfValue::BoolValue(v), Builder::Bool(b)) => b.append_value(*v),
                (ProtoOneOfValue::TagValue(v), Builder::Tag(b)) => {
                    b.append(v).context(ArrowSnafu)?;
                }
                (ProtoOneOfValue::StringValue(v), Builder::String(b)) => b.append_value(v),
                (ProtoOneOfValue::TimeValue(v), Builder::Time(b)) => b.append_value(*v),
                (_, _) => return UnexpectedColumnTypeSnafu.fail(),
            },
            None => self.append_null(),
        }

        Ok(())
    }

    fn append_null(&mut self) {
        match &mut self.builder {
            Builder::Bool(b) => b.append_null(),
            Builder::I64(b) => b.append_null(),
            Builder::F64(b) => b.append_null(),
            Builder::U64(b) => b.append_null(),
            Builder::String(b) => b.append_null(),
            Builder::Tag(b) => b.append_null(),
            Builder::Time(b) => b.append_null(),
        }
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

/// Takes &str of line protocol, parses lines, validates the schema, and inserts new columns
/// and partitions if present. Assigns the default time to any lines that do not include a time
pub async fn parse_validate_and_update_schema_and_partitions<'a, R>(
    lp: &str,
    schema: &NamespaceSchema,
    table_partition_map: &TablePartitionMap,
    partitioner: &Partitioner,
    repos: &mut R,
    default_time: i64,
) -> Result<ValidationResult, Error>
where
    R: RepoCollection + ?Sized,
{
    let mut lines = vec![];
    for (line_idx, maybe_line) in parse_lines(lp).enumerate() {
        let line = maybe_line.context(LineProtocolSnafu { line: line_idx + 1 })?;
        lines.push(line);
    }

    validate_or_insert_schema_and_partitions(
        lines,
        schema,
        table_partition_map,
        partitioner,
        repos,
        default_time,
    )
    .await
}

/// Takes parsed lines, validates their schema, inserting new columns if present. Determines
/// the partition for each line and generates new partitions if present. Assigns the default
/// time to any lines that do not include a time.
pub async fn validate_or_insert_schema_and_partitions<'a, R>(
    lines: Vec<ParsedLine<'_>>,
    schema: &NamespaceSchema,
    table_partition_map: &TablePartitionMap,
    partitioner: &Partitioner,
    repos: &mut R,
    default_time: i64,
) -> Result<ValidationResult, Error>
where
    R: RepoCollection + ?Sized,
{
    // The (potentially updated) NamespaceSchema to return to the caller.
    let mut schema = Cow::Borrowed(schema);

    // The (potentially updated) PartitionMap to return to the caller
    let mut table_partition_map = Cow::Borrowed(table_partition_map);

    // The parsed and validated table_batches
    let mut table_batches: HashMap<TableId, TableBatch> = HashMap::new();

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
            &mut table_partition_map,
            partitioner,
            repos,
            default_time,
        )
        .await?;
    }

    let schema = match schema {
        Cow::Owned(s) => Some(s),
        Cow::Borrowed(_) => None,
    };

    let table_partition_map = match table_partition_map {
        Cow::Owned(p) => Some(p),
        Cow::Borrowed(_) => None,
    };

    Ok(ValidationResult {
        schema,
        table_partition_map,
        table_batches,
        line_count,
        field_count,
        tag_count,
    })
}

// &mut Cow is used to avoid a copy, so allow it
#[allow(clippy::ptr_arg)]
async fn validate_and_convert_parsed_line<R>(
    line: ParsedLine<'_>,
    table_batches: &mut HashMap<TableId, TableBatch>,
    schema: &mut Cow<'_, NamespaceSchema>,
    table_partition_map: &mut Cow<'_, TablePartitionMap>,
    partitioner: &Partitioner,
    repos: &mut R,
    default_time: i64,
) -> Result<()>
where
    R: RepoCollection + ?Sized,
{
    let table_name = line.series.measurement.as_str();

    // Check if the table exists in the schema.
    //
    // Because the entry API requires &mut it is not used to avoid a premature
    // clone of the Cow.
    let mut table = match schema.tables.get(table_name) {
        Some(t) => Cow::Borrowed(t),
        None => {
            let table = table_load_or_create(&mut *repos, schema.id, &schema.partition_template, table_name).await.context(CatalogSnafu)?;

            assert!(schema
                .to_mut()
                .tables
                .insert(table_name.to_string(), table)
                .is_none());

            Cow::Borrowed(schema.tables.get(table_name).unwrap())
        }
    };

    let mut partition_map = match table_partition_map.map.get(&table.id) {
        Some(m) => Cow::Borrowed(m),
        None => {
            assert!(table_partition_map
                .to_mut()
                .map
                .insert(table.id, PartitionMap::default())
                .is_none());

            Cow::Borrowed(table_partition_map.map.get(&table.id).unwrap())
        }
    };

    let partition_key = partitioner.partition_key_for_line(&line, default_time);
    // Check if the partition exists in the partition map
    //
    // Because the entry API requires &mut it is not used to avoid a premature
    // clone of the Cow.
    let partition_id = match partition_map.map.get(&partition_key) {
        Some(p) => *p,
        None => {
            let key = PartitionKey::from(partition_key.as_str());
            let partition = repos
                .partitions()
                .create_or_get(key, table.id)
                .await
                .context(CatalogSnafu)?;

            assert!(partition_map
                .to_mut()
                .map
                .insert(partition_key, partition.id)
                .is_none());

            partition.id
        }
    };

    // The table is now in the schema (either by virtue of it already existing,
    // or through adding it above).
    //
    // If the table itself needs to be updated during column validation it
    // becomes a Cow::owned() copy and the modified copy should be inserted into
    // the schema before returning.

    // First, see if any of the columns need to be inserted into the schema
    let mut new_cols = Vec::with_capacity(line.column_count() + 1);
    if let Some(tagset) = &line.series.tag_set {
        for (tag_key, _) in tagset {
            if table.columns.get(tag_key.as_str()).is_none() {
                new_cols.push((tag_key.to_string(), ColumnType::Tag));
            }
        }
    }
    for (field_name, value) in &line.field_set {
        if table.columns.get(field_name.as_str()).is_none() {
            new_cols.push((
                field_name.to_string(),
                ColumnType::from(field_type_to_column_type(value)),
            ));
        }
    }

    if !new_cols.is_empty() {
        let mut column_batch: HashMap<&str, ColumnType> = HashMap::new();
        for (name, col_type) in &new_cols {
            if column_batch.insert(name, *col_type).is_some() {
                return ColumnNameNotUniqueSnafu { name }.fail();
            }
        }

        repos
            .columns()
            .create_or_get_many_unchecked(table.id, column_batch)
            .await
            .context(CatalogSnafu)?
            .into_iter()
            .for_each(|c| table.to_mut().add_column(c));
    }

    // now that we've ensured all columns exist in the schema, construct the actual row and values
    // while validating the column types match.
    let mut values = Vec::with_capacity(line.column_count() + 1);

    // validate tags, collecting any new ones that must be inserted, or adding the values
    if let Some(tagset) = line.series.tag_set {
        for (tag_key, value) in tagset {
            match table.columns.get(tag_key.as_str()) {
                Some(existing) if existing.matches_type(InfluxColumnType::Tag) => {
                    let value = ProtoValue {
                        column_id: existing.id.get() as u32,
                        value: Some(ProtoOneOfValue::TagValue(value.to_string())),
                    };
                    values.push(value);
                }
                Some(existing) => {
                    return ColumnTypeMismatchSnafu {
                        name: tag_key.as_str(),
                        existing: existing.column_type,
                        new: InfluxColumnType::Tag,
                    }
                    .fail();
                }
                None => panic!("every tag should have been inserted as a column before this point"),
            }
        }
    }

    // validate fields, collecting any new ones that must be inserted, or adding values
    for (field_name, value) in line.field_set {
        match table.columns.get(field_name.as_str()) {
            Some(existing) if existing.matches_type(field_type_to_column_type(&value)) => {
                let one_of_proto = match value {
                    FieldValue::I64(v) => ProtoOneOfValue::I64Value(v),
                    FieldValue::F64(v) => ProtoOneOfValue::F64Value(v),
                    FieldValue::U64(v) => ProtoOneOfValue::U64Value(v),
                    FieldValue::Boolean(v) => ProtoOneOfValue::BoolValue(v),
                    FieldValue::String(v) => ProtoOneOfValue::StringValue(v.to_string()),
                };
                let value = ProtoValue {
                    column_id: existing.id.get() as u32,
                    value: Some(one_of_proto),
                };
                values.push(value);
            }
            Some(existing) => {
                return ColumnTypeMismatchSnafu {
                    name: field_name.as_str(),
                    existing: existing.column_type,
                    new: field_type_to_column_type(&value),
                }
                .fail();
            }
            None => panic!("every field should have been inserted as a column before this point"),
        }
    }

    // set the time value
    let time_column = table
        .columns
        .get(TIME_COLUMN)
        .expect("time column should always be here at this point");
    let time_value = line.timestamp.unwrap_or(default_time);
    values.push(ProtoValue {
        column_id: time_column.id.get() as u32,
        value: Some(ProtoOneOfValue::TimeValue(time_value)),
    });

    let table_batch = table_batches.entry(table.id).or_default();
    let partition_batch = table_batch
        .partition_batches
        .entry(partition_id)
        .or_default();

    // insert the row into the partition batch
    partition_batch.push(ProtoRow {
        values,
        sequence_number: 0,
    });

    if let Cow::Owned(partition_map) = partition_map {
        // The specific table's partition map was mutated and needs inserting into in
        // table_partition_map to make changes visible to the caller.
        assert!(table_partition_map
            .to_mut()
            .map
            .insert(table.id, partition_map)
            .is_some());
    }

    if let Cow::Owned(table) = table {
        // The table schema was mutated and needs inserting into the namespace
        // schema to make the changes visible to the caller.
        assert!(schema
            .to_mut()
            .tables
            .insert(table_name.to_string(), table)
            .is_some());
    }

    Ok(())
}

fn field_type_to_column_type(field: &FieldValue<'_>) -> InfluxColumnType {
    match field {
        FieldValue::I64(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
        FieldValue::F64(_) => InfluxColumnType::Field(InfluxFieldType::Float),
        FieldValue::U64(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
        FieldValue::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
        FieldValue::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
    }
}

/// Map of partition to the rows in the partition for the write
#[derive(Debug, Default)]
pub struct TableBatch {
    /// Map of partition_id to the rows for that partition
    pub partition_batches: HashMap<PartitionId, Vec<ProtoRow>>,
}

/// Structures commonly used in tests related to catalog and ingest data
#[derive(Debug)]
pub struct TestStructure {
    pub catalog: Arc<dyn Catalog>,
    pub schema: Arc<NamespaceSchema>,
    pub partition_map: Arc<TablePartitionMap>,
}

pub mod test_helpers {
    use super::*;
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use std::ops::DerefMut;
    use data_types::NamespaceName;

    pub async fn lp_to_record_batch(lp: &str) -> RecordBatch {
        let (test_structure, op) = lp_to_proto_initialize(lp).await;
        let (_, table_schema) = test_structure.schema.tables.first_key_value().unwrap();
        rows_to_record_batch(
            table_schema,
            &op.table_batches[0].partition_batches[0].rows,
        )
        .unwrap()
    }

    pub async fn lp_to_proto_initialize(lp: &str) -> (TestStructure, ProtoWriteBatch) {
        let metrics = Arc::new(metric::Registry::default());
        let repo = MemCatalog::new(Arc::clone(&metrics));
        let mut repositories = repo.repositories().await;
        const NAMESPACE_NAME: &str = "foo";
        let namespace_name = NamespaceName::new(NAMESPACE_NAME).unwrap();

        let res = {
            let namespace = repositories
                .namespaces()
                .create(&namespace_name, None, None, None)
                .await
                .unwrap();
            let schema = NamespaceSchema::new_empty_from(&namespace);
            let table_partition_map = TablePartitionMap::default();
            let partitioner = Partitioner::new_time_partitioner("%Y-%m-%d");

            parse_validate_and_update_schema_and_partitions(
                lp,
                &schema,
                &table_partition_map,
                &partitioner,
                repositories.deref_mut(),
                86401 * 1000000000,
            )
            .await
            .unwrap()
        };

        let schema = res.schema.unwrap();
        let table_partition_map = res.table_partition_map.unwrap();
        let batches = res.table_batches;

        (
            TestStructure {
                catalog: Arc::new(repo),
                schema: Arc::new(schema),
                partition_map: Arc::new(table_partition_map),
            },
            table_batches_map_to_proto(batches),
        )
    }

    pub async fn lp_to_proto_update(
        lp: &str,
        test_structure: &mut TestStructure,
    ) -> ProtoWriteBatch {
        let partitioner = Partitioner::new_time_partitioner("%Y-%m-%d");
        let mut repos = test_structure.catalog.repositories().await;

        let res = parse_validate_and_update_schema_and_partitions(
            lp,
            &test_structure.schema,
            &test_structure.partition_map,
            &partitioner,
            repos.deref_mut(),
            86401 * 1000000000,
        )
        .await
        .unwrap();

        if let Some(schema) = res.schema {
            test_structure.schema = Arc::new(schema);
        }

        if let Some(partition_map) = res.table_partition_map {
            test_structure.partition_map = Arc::new(partition_map);
        }

        table_batches_map_to_proto(res.table_batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use std::ops::DerefMut;
    use data_types::NamespaceName;

    #[tokio::test]
    async fn validate() {
        let metrics = Arc::new(metric::Registry::default());
        let repo = MemCatalog::new(metrics);
        let mut repos = repo.repositories().await;
        const NAMESPACE_NAME: &str = "foo";
        let namespace_name = NamespaceName::new(NAMESPACE_NAME).unwrap();

        let namespace = repos
            .namespaces()
            .create(&namespace_name, Default::default(), None, None)
            .await
            .unwrap();
        let schema = NamespaceSchema::new_empty_from(&namespace);
        let table_partition_map = TablePartitionMap::default();
        let partitioner = Partitioner::new_time_partitioner("%Y-%m-%d");

        let res = parse_validate_and_update_schema_and_partitions(
            "m1,t1=hi f1=12i 123\nm2 f1=true 1234\nm2,t2=asdf f2=1i",
            &schema,
            &table_partition_map,
            &partitioner,
            repos.deref_mut(),
            86401 * 1000000000,
        )
        .await
        .unwrap();
        let schema = res.schema.unwrap();
        let table_partition_map = res.table_partition_map.unwrap();
        let batches = res.table_batches;

        let m1 = schema.tables.get("m1").unwrap();
        assert_eq!(m1.columns.get("t1").unwrap().column_type, ColumnType::Tag);
        assert_eq!(m1.columns.get("f1").unwrap().column_type, ColumnType::I64);
        assert_eq!(
            m1.columns.get("time").unwrap().column_type,
            ColumnType::Time
        );
        assert_eq!(m1.columns.column_count(), 3);

        let m2 = schema.tables.get("m2").unwrap();
        assert_eq!(m2.columns.get("t2").unwrap().column_type, ColumnType::Tag);
        assert_eq!(m2.columns.get("f1").unwrap().column_type, ColumnType::Bool);
        assert_eq!(m2.columns.get("f2").unwrap().column_type, ColumnType::I64);
        assert_eq!(
            m2.columns.get("time").unwrap().column_type,
            ColumnType::Time
        );
        assert_eq!(m2.columns.column_count(), 4);

        let m1_rows = batches
            .get(&m1.id)
            .unwrap()
            .partition_batches
            .get(&PartitionId::new(1))
            .unwrap();
        assert_eq!(m1_rows.len(), 1);
        let m2_p1_rows = batches
            .get(&m2.id)
            .unwrap()
            .partition_batches
            .get(&PartitionId::new(2))
            .unwrap();
        assert_eq!(m2_p1_rows.len(), 1);
        let m2_p2_rows = batches
            .get(&m2.id)
            .unwrap()
            .partition_batches
            .get(&PartitionId::new(3))
            .unwrap();
        assert_eq!(m2_p2_rows.len(), 1);

        assert!(schema.tables.get("m1").is_some());
        assert!(schema.tables.get("m2").is_some());
        assert!(table_partition_map
            .map
            .get(&TableId::new(1))
            .unwrap()
            .map
            .get("1970-01-01")
            .is_some());
        assert!(table_partition_map
            .map
            .get(&TableId::new(2))
            .unwrap()
            .map
            .get("1970-01-01")
            .is_some());
        assert!(table_partition_map
            .map
            .get(&TableId::new(2))
            .unwrap()
            .map
            .get("1970-01-02")
            .is_some());
        assert_eq!(batches.len(), 2);

        // ensure that if we parse the same data we get none for the schema and partition map in the result
        let res = parse_validate_and_update_schema_and_partitions(
            "m1,t1=hi f1=12i 123\nm2 f1=true 1234\nm2,t2=asdf f2=1i",
            &schema,
            &table_partition_map,
            &partitioner,
            repos.deref_mut(),
            86401 * 1000000000,
        )
        .await
        .unwrap();
        assert!(res.table_partition_map.is_none());
        assert!(res.schema.is_none());
    }

    #[tokio::test]
    async fn proto_rows_to_record_batch() {
        let metrics = Arc::new(metric::Registry::default());
        let repo = MemCatalog::new(metrics);
        let mut repos = repo.repositories().await;
        const NAMESPACE_NAME: &str = "foo";
        let namespace_name = NamespaceName::new(NAMESPACE_NAME).unwrap();

        let namespace = repos
            .namespaces()
            .create(&namespace_name, Default::default(), None, None)
            .await
            .unwrap();
        let schema = NamespaceSchema::new_empty_from(&namespace);
        let table_partition_map = TablePartitionMap::default();
        let partitioner = Partitioner::new_time_partitioner("%Y-%m-%d");

        let result = parse_validate_and_update_schema_and_partitions(
            "foo,t1=hi,t2=asdf f1=3i,f2=1.2 123\nfoo,t1=world f1=4i,f3=true,f4=\"arf arf!\" 124",
            &schema,
            &table_partition_map,
            &partitioner,
            repos.deref_mut(),
            86401 * 1000000000,
        )
        .await
        .unwrap();
        let schema = result.schema.unwrap();
        let batches = result.table_batches;
        let proto = table_batches_map_to_proto(batches);
        let rows = &proto.table_batches[0].partition_batches[0].rows;
        let rb = rows_to_record_batch(schema.tables.get("foo").unwrap(), rows).unwrap();

        let expected_data = &[
            "+----+-----+------+----------+-------+------+--------------------------------+",
            "| f1 | f2  | f3   | f4       | t1    | t2   | time                           |",
            "+----+-----+------+----------+-------+------+--------------------------------+",
            "| 3  | 1.2 |      |          | hi    | asdf | 1970-01-01T00:00:00.000000123Z |",
            "| 4  |     | true | arf arf! | world |      | 1970-01-01T00:00:00.000000124Z |",
            "+----+-----+------+----------+-------+------+--------------------------------+",
        ];

        assert_batches_eq!(expected_data, &[rb]);
    }
}
