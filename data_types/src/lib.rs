//! Shared data types

#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use influxdb_line_protocol::FieldValue;
use observability_deps::tracing::warn;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use schema::{
    builder::SchemaBuilder, sort::SortKey, InfluxColumnType, InfluxFieldType, Schema,
    TIME_COLUMN_NAME,
};
use snafu::{ResultExt, Snafu};
use std::{
    borrow::{Borrow, Cow},
    collections::BTreeMap,
    convert::TryFrom,
    fmt::Write,
    mem::{self, size_of_val},
    num::{FpCategory, NonZeroU32, NonZeroU64},
    ops::{Add, Deref, RangeInclusive, Sub},
    sync::Arc,
};
use uuid::Uuid;

/// Unique ID for a `Namespace`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct NamespaceId(i64);

#[allow(missing_docs)]
impl NamespaceId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique ID for a `KafkaTopic`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct KafkaTopicId(i64);

#[allow(missing_docs)]
impl KafkaTopicId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for KafkaTopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique ID for a `QueryPool`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct QueryPoolId(i64);

#[allow(missing_docs)]
impl QueryPoolId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

/// Unique ID for a `Table`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct TableId(i64);

#[allow(missing_docs)]
impl TableId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique ID for a `Column`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ColumnId(i64);

#[allow(missing_docs)]
impl ColumnId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

/// Unique ID for a `Sequencer`. Note this is NOT the same as the
/// "sequencer_id" in the `write_buffer` which currently means
/// "kafka partition".
///
/// <https://github.com/influxdata/influxdb_iox/issues/4237>
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct SequencerId(i64);

#[allow(missing_docs)]
impl SequencerId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for SequencerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The kafka partition identifier. This is in the actual Kafka cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct KafkaPartition(i32);

#[allow(missing_docs)]
impl KafkaPartition {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for KafkaPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique ID for a `Partition`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct PartitionId(i64);

#[allow(missing_docs)]
impl PartitionId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Combination of Sequencer ID, Table ID, and Partition ID useful for identifying groups of
/// Parquet files to be compacted together.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct TablePartition {
    /// The sequencer ID
    pub sequencer_id: SequencerId,
    /// The table ID
    pub table_id: TableId,
    /// The partition ID
    pub partition_id: PartitionId,
}

impl TablePartition {
    /// Combine the relevant parts
    pub fn new(sequencer_id: SequencerId, table_id: TableId, partition_id: PartitionId) -> Self {
        Self {
            sequencer_id,
            table_id,
            partition_id,
        }
    }
}

/// Unique ID for a `Tombstone`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct TombstoneId(i64);

#[allow(missing_docs)]
impl TombstoneId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for TombstoneId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A sequence number from a `Sequencer` (kafka partition)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct SequenceNumber(i64);

#[allow(missing_docs)]
impl SequenceNumber {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl Add<i64> for SequenceNumber {
    type Output = Self;

    fn add(self, other: i64) -> Self {
        Self(self.0 + other)
    }
}

impl Sub<i64> for SequenceNumber {
    type Output = Self;

    fn sub(self, other: i64) -> Self {
        Self(self.0 - other)
    }
}

/// A time in nanoseconds from epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct Timestamp(i64);

#[allow(missing_docs)]
impl Timestamp {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl Add for Timestamp {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0.checked_add(other.0).expect("timestamp wraparound"))
    }
}

impl Sub for Timestamp {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0.checked_sub(other.0).expect("timestamp wraparound"))
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        self + Self(rhs)
    }
}

impl Sub<i64> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        self - Self(rhs)
    }
}

/// Unique ID for a `ParquetFile`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ParquetFileId(i64);

#[allow(missing_docs)]
impl ParquetFileId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for ParquetFileId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Use `self.number` to refer to each positional data point.
        write!(f, "{}", self.0)
    }
}

/// Data object for a kafka topic
#[derive(Debug, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct KafkaTopic {
    /// The id of the topic
    pub id: KafkaTopicId,
    /// The unique name of the topic
    pub name: String,
}

/// Data object for a query pool
#[derive(Debug, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct QueryPool {
    /// The id of the pool
    pub id: QueryPoolId,
    /// The unique name of the pool
    pub name: String,
}

/// Data object for a namespace
#[derive(Debug, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct Namespace {
    /// The id of the namespace
    pub id: NamespaceId,
    /// The unique name of the namespace
    pub name: String,
    /// The retention duration as a string. 'inf' or not present represents infinite duration (i.e. never drop data).
    #[sqlx(default)]
    pub retention_duration: Option<String>,
    /// The kafka topic that writes to this namespace will land in
    pub kafka_topic_id: KafkaTopicId,
    /// The query pool assigned to answer queries for this namespace
    pub query_pool_id: QueryPoolId,
    /// The maximum number of tables that can exist in this namespace
    pub max_tables: i32,
    /// The maximum number of columns per table in this namespace
    pub max_columns_per_table: i32,
}

/// Schema collection for a namespace. This is an in-memory object useful for a schema
/// cache.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NamespaceSchema {
    /// the namespace id
    pub id: NamespaceId,
    /// the kafka topic this namespace gets data written to
    pub kafka_topic_id: KafkaTopicId,
    /// the query pool assigned to answer queries for this namespace
    pub query_pool_id: QueryPoolId,
    /// the tables in the namespace by name
    pub tables: BTreeMap<String, TableSchema>,
}

impl NamespaceSchema {
    /// Create a new `NamespaceSchema`
    pub fn new(id: NamespaceId, kafka_topic_id: KafkaTopicId, query_pool_id: QueryPoolId) -> Self {
        Self {
            id,
            tables: BTreeMap::new(),
            kafka_topic_id,
            query_pool_id,
        }
    }

    /// Estimated Size in bytes including `self`.
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .tables
                .iter()
                .map(|(k, v)| size_of_val(k) + k.capacity() + v.size())
                .sum::<usize>()
    }
}

/// Data object for a table
#[derive(Debug, Clone, sqlx::FromRow, Eq, PartialEq)]
pub struct Table {
    /// The id of the table
    pub id: TableId,
    /// The namespace id that the table is in
    pub namespace_id: NamespaceId,
    /// The name of the table, which is unique within the associated namespace
    pub name: String,
}

/// Column definitions for a table
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// the table id
    pub id: TableId,
    /// the table's columns by their name
    pub columns: BTreeMap<String, ColumnSchema>,
}

impl TableSchema {
    /// Initialize new `TableSchema`
    pub fn new(id: TableId) -> Self {
        Self {
            id,
            columns: BTreeMap::new(),
        }
    }

    /// Add `col` to this table schema.
    ///
    /// # Panics
    ///
    /// This method panics if a column of the same name already exists in
    /// `self`, or the provided [`Column`] cannot be converted into a valid
    /// [`ColumnSchema`].
    pub fn add_column(&mut self, col: &Column) {
        let old = self.columns.insert(
            col.name.clone(),
            ColumnSchema::try_from(col).expect("column is invalid"),
        );
        assert!(old.is_none());
    }

    /// Estimated Size in bytes including `self`.
    pub fn size(&self) -> usize {
        size_of_val(self)
            + self
                .columns
                .iter()
                .map(|(k, v)| size_of_val(k) + k.capacity() + size_of_val(v))
                .sum::<usize>()
    }
}

/// Data object for a column
#[derive(Debug, Clone, sqlx::FromRow, Eq, PartialEq)]
pub struct Column {
    /// the column id
    pub id: ColumnId,
    /// the table id the column is in
    pub table_id: TableId,
    /// the name of the column, which is unique in the table
    pub name: String,
    /// the logical type of the column
    pub column_type: i16,
}

impl Column {
    /// returns true if the column type is a tag
    pub fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag as i16
    }

    /// returns true if the column type matches the line protocol field value type
    pub fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        match field_value {
            FieldValue::I64(_) => self.column_type == ColumnType::I64 as i16,
            FieldValue::U64(_) => self.column_type == ColumnType::U64 as i16,
            FieldValue::F64(_) => self.column_type == ColumnType::F64 as i16,
            FieldValue::String(_) => self.column_type == ColumnType::String as i16,
            FieldValue::Boolean(_) => self.column_type == ColumnType::Bool as i16,
        }
    }
}

/// The column id and its type for a column
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// the column id
    pub id: ColumnId,
    /// the column type
    pub column_type: ColumnType,
}

impl ColumnSchema {
    /// returns true if the column is a tag
    pub fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag
    }

    /// returns true if the column matches the line protocol field value type
    pub fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        matches!(
            (field_value, self.column_type),
            (FieldValue::I64(_), ColumnType::I64)
                | (FieldValue::U64(_), ColumnType::U64)
                | (FieldValue::F64(_), ColumnType::F64)
                | (FieldValue::String(_), ColumnType::String)
                | (FieldValue::Boolean(_), ColumnType::Bool)
        )
    }

    /// Returns true if `mb_column` is of the same type as `self`.
    pub fn matches_type(&self, mb_column_influx_type: InfluxColumnType) -> bool {
        self.column_type == mb_column_influx_type
    }
}

impl TryFrom<&Column> for ColumnSchema {
    type Error = Box<dyn std::error::Error>;

    fn try_from(c: &Column) -> Result<Self, Self::Error> {
        Ok(Self {
            id: c.id,
            column_type: ColumnType::try_from(c.column_type)?,
        })
    }
}

/// The column data type
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ColumnType {
    I64 = 1,
    U64 = 2,
    F64 = 3,
    Bool = 4,
    String = 5,
    Time = 6,
    Tag = 7,
}

impl ColumnType {
    /// the short string description of the type
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::I64 => "i64",
            Self::U64 => "u64",
            Self::F64 => "f64",
            Self::Bool => "bool",
            Self::String => "string",
            Self::Time => "time",
            Self::Tag => "tag",
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();

        write!(f, "{}", s)
    }
}

impl TryFrom<i16> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::I64 as i16 => Ok(Self::I64),
            x if x == Self::U64 as i16 => Ok(Self::U64),
            x if x == Self::F64 as i16 => Ok(Self::F64),
            x if x == Self::Bool as i16 => Ok(Self::Bool),
            x if x == Self::String as i16 => Ok(Self::String),
            x if x == Self::Time as i16 => Ok(Self::Time),
            x if x == Self::Tag as i16 => Ok(Self::Tag),
            _ => Err("invalid column value".into()),
        }
    }
}

impl From<InfluxColumnType> for ColumnType {
    fn from(value: InfluxColumnType) -> Self {
        match value {
            InfluxColumnType::Tag => Self::Tag,
            InfluxColumnType::Field(InfluxFieldType::Float) => Self::F64,
            InfluxColumnType::Field(InfluxFieldType::Integer) => Self::I64,
            InfluxColumnType::Field(InfluxFieldType::UInteger) => Self::U64,
            InfluxColumnType::Field(InfluxFieldType::String) => Self::String,
            InfluxColumnType::Field(InfluxFieldType::Boolean) => Self::Bool,
            InfluxColumnType::Timestamp => Self::Time,
        }
    }
}

impl From<ColumnType> for InfluxColumnType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::I64 => Self::Field(InfluxFieldType::Integer),
            ColumnType::U64 => Self::Field(InfluxFieldType::UInteger),
            ColumnType::F64 => Self::Field(InfluxFieldType::Float),
            ColumnType::Bool => Self::Field(InfluxFieldType::Boolean),
            ColumnType::String => Self::Field(InfluxFieldType::String),
            ColumnType::Time => Self::Timestamp,
            ColumnType::Tag => Self::Tag,
        }
    }
}

impl TryFrom<TableSchema> for Schema {
    type Error = schema::builder::Error;

    fn try_from(value: TableSchema) -> Result<Self, Self::Error> {
        let mut builder = SchemaBuilder::new();

        for (column_name, column_schema) in &value.columns {
            let t = InfluxColumnType::from(column_schema.column_type);
            builder.influx_column(column_name, t);
        }

        builder.build()
    }
}

impl PartialEq<InfluxColumnType> for ColumnType {
    fn eq(&self, got: &InfluxColumnType) -> bool {
        match self {
            Self::I64 => matches!(got, InfluxColumnType::Field(InfluxFieldType::Integer)),
            Self::U64 => matches!(got, InfluxColumnType::Field(InfluxFieldType::UInteger)),
            Self::F64 => matches!(got, InfluxColumnType::Field(InfluxFieldType::Float)),
            Self::Bool => matches!(got, InfluxColumnType::Field(InfluxFieldType::Boolean)),
            Self::String => matches!(got, InfluxColumnType::Field(InfluxFieldType::String)),
            Self::Time => matches!(got, InfluxColumnType::Timestamp),
            Self::Tag => matches!(got, InfluxColumnType::Tag),
        }
    }
}

/// Returns the `ColumnType` for the passed in line protocol `FieldValue` type
pub fn column_type_from_field(field_value: &FieldValue) -> ColumnType {
    match field_value {
        FieldValue::I64(_) => ColumnType::I64,
        FieldValue::U64(_) => ColumnType::U64,
        FieldValue::F64(_) => ColumnType::F64,
        FieldValue::String(_) => ColumnType::String,
        FieldValue::Boolean(_) => ColumnType::Bool,
    }
}

/// Data object for a sequencer. Only one sequencer record can exist for a given
/// kafka topic and partition (enforced via uniqueness constraint).
#[derive(Debug, Copy, Clone, PartialEq, sqlx::FromRow)]
pub struct Sequencer {
    /// the id of the sequencer
    pub id: SequencerId,
    /// the topic the sequencer is reading from
    pub kafka_topic_id: KafkaTopicId,
    /// the kafka partition the sequencer is reading from
    pub kafka_partition: KafkaPartition,
    /// The minimum unpersisted sequence number. Because different tables
    /// can be persisted at different times, it is possible some data has been persisted
    /// with a higher sequence number than this. However, all data with a sequence number
    /// lower than this must have been persisted to Parquet.
    pub min_unpersisted_sequence_number: i64,
}

/// Data object for a partition. The combination of sequencer, table and key are unique (i.e. only
/// one record can exist for each combo)
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Partition {
    /// the id of the partition
    pub id: PartitionId,
    /// the sequencer the data in the partition arrived from
    pub sequencer_id: SequencerId,
    /// the table the partition is under
    pub table_id: TableId,
    /// the string key of the partition
    pub partition_key: String,
    /// The sort key for the partition. Should be computed on the first persist operation for
    /// this partition and updated if new tag columns are added.
    pub sort_key: Option<String>,
}

impl Partition {
    /// The sort key for the partition, if present, structured as a `SortKey`
    pub fn sort_key(&self) -> Option<SortKey> {
        self.sort_key
            .as_ref()
            .map(|s| SortKey::from_columns(s.split(',')))
    }
}

/// Information for a partition from the catalog.
#[derive(Debug)]
#[allow(missing_docs)]
pub struct PartitionInfo {
    pub partition: Partition,
    pub namespace_name: String,
    pub table_name: String,
}

/// Data object for a tombstone.
#[derive(Debug, Clone, PartialEq, PartialOrd, sqlx::FromRow)]
pub struct Tombstone {
    /// the id of the tombstone
    pub id: TombstoneId,
    /// the table the tombstone is associated with
    pub table_id: TableId,
    /// the sequencer the tombstone was sent through
    pub sequencer_id: SequencerId,
    /// the sequence nubmer assigned to the tombstone from the sequencer
    pub sequence_number: SequenceNumber,
    /// the min time (inclusive) that the delete applies to
    pub min_time: Timestamp,
    /// the max time (exclusive) that the delete applies to
    pub max_time: Timestamp,
    /// the full delete predicate
    pub serialized_predicate: String,
}

impl Tombstone {
    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        // No additional heap allocations
        std::mem::size_of_val(self)
    }
}

/// Data for a parquet file reference that has been inserted in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, sqlx::FromRow)]
pub struct ParquetFile {
    /// the id of the file in the catalog
    pub id: ParquetFileId,
    /// the sequencer that sequenced writes that went into this file
    pub sequencer_id: SequencerId,
    /// the namespace
    pub namespace_id: NamespaceId,
    /// the table
    pub table_id: TableId,
    /// the partition
    pub partition_id: PartitionId,
    /// the uuid used in the object store path for this file
    pub object_store_id: Uuid,
    /// the minimum sequence number from a record in this file
    pub min_sequence_number: SequenceNumber,
    /// the maximum sequence number from a record in this file
    pub max_sequence_number: SequenceNumber,
    /// the min timestamp of data in this file
    pub min_time: Timestamp,
    /// the max timestamp of data in this file
    pub max_time: Timestamp,
    /// When this file was marked for deletion
    pub to_delete: Option<Timestamp>,
    /// file size in bytes
    pub file_size_bytes: i64,
    /// the number of rows of data in this file
    pub row_count: i64,
    /// the compaction level of the file
    pub compaction_level: i16,
    /// the creation time of the parquet file
    pub created_at: Timestamp,
}

impl ParquetFile {
    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        // No additional heap allocations
        std::mem::size_of_val(self)
    }
}

/// Data for a parquet file reference that has been inserted in the catalog, including the
/// `parquet_metadata` field that can be expensive to fetch.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ParquetFileWithMetadata {
    /// the id of the file in the catalog
    pub id: ParquetFileId,
    /// the sequencer that sequenced writes that went into this file
    pub sequencer_id: SequencerId,
    /// the namespace
    pub namespace_id: NamespaceId,
    /// the table
    pub table_id: TableId,
    /// the partition
    pub partition_id: PartitionId,
    /// the uuid used in the object store path for this file
    pub object_store_id: Uuid,
    /// the minimum sequence number from a record in this file
    pub min_sequence_number: SequenceNumber,
    /// the maximum sequence number from a record in this file
    pub max_sequence_number: SequenceNumber,
    /// the min timestamp of data in this file
    pub min_time: Timestamp,
    /// the max timestamp of data in this file
    pub max_time: Timestamp,
    /// When this file was marked for deletion
    pub to_delete: Option<Timestamp>,
    /// file size in bytes
    pub file_size_bytes: i64,
    /// thrift-encoded parquet metadata
    pub parquet_metadata: Vec<u8>,
    /// the number of rows of data in this file
    pub row_count: i64,
    /// the compaction level of the file
    pub compaction_level: i16,
    /// the creation time of the parquet file
    pub created_at: Timestamp,
}

impl ParquetFileWithMetadata {
    /// Create an instance from an instance of ParquetFile and metadata bytes fetched from the
    /// catalog.
    pub fn new(parquet_file: ParquetFile, parquet_metadata: Vec<u8>) -> Self {
        let ParquetFile {
            id,
            sequencer_id,
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time,
            max_time,
            to_delete,
            file_size_bytes,
            row_count,
            compaction_level,
            created_at,
        } = parquet_file;

        Self {
            id,
            sequencer_id,
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time,
            max_time,
            to_delete,
            file_size_bytes,
            parquet_metadata,
            row_count,
            compaction_level,
            created_at,
        }
    }

    /// Split the parquet_metadata off, leaving a regular ParquetFile and the bytes to transfer
    /// ownership separately.
    pub fn split_off_metadata(self) -> (ParquetFile, Vec<u8>) {
        let Self {
            id,
            sequencer_id,
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time,
            max_time,
            to_delete,
            file_size_bytes,
            parquet_metadata,
            row_count,
            compaction_level,
            created_at,
        } = self;

        (
            ParquetFile {
                id,
                sequencer_id,
                namespace_id,
                table_id,
                partition_id,
                object_store_id,
                min_sequence_number,
                max_sequence_number,
                min_time,
                max_time,
                to_delete,
                file_size_bytes,
                row_count,
                compaction_level,
                created_at,
            },
            parquet_metadata,
        )
    }
}

/// Data for a parquet file to be inserted into the catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetFileParams {
    /// the sequencer that sequenced writes that went into this file
    pub sequencer_id: SequencerId,
    /// the namespace
    pub namespace_id: NamespaceId,
    /// the table
    pub table_id: TableId,
    /// the partition
    pub partition_id: PartitionId,
    /// the uuid used in the object store path for this file
    pub object_store_id: Uuid,
    /// the minimum sequence number from a record in this file
    pub min_sequence_number: SequenceNumber,
    /// the maximum sequence number from a record in this file
    pub max_sequence_number: SequenceNumber,
    /// the min timestamp of data in this file
    pub min_time: Timestamp,
    /// the max timestamp of data in this file
    pub max_time: Timestamp,
    /// file size in bytes
    pub file_size_bytes: i64,
    /// thrift-encoded parquet metadata
    pub parquet_metadata: Vec<u8>,
    /// the number of rows of data in this file
    pub row_count: i64,
    /// the compaction level of the file
    pub compaction_level: i16,
    /// the creation time of the parquet file
    pub created_at: Timestamp,
}

/// Data for a processed tombstone reference in the catalog.
#[derive(Debug, Copy, Clone, PartialEq, sqlx::FromRow)]
pub struct ProcessedTombstone {
    /// the id of the tombstone applied to the parquet file
    pub tombstone_id: TombstoneId,
    /// the id of the parquet file the tombstone was applied
    pub parquet_file_id: ParquetFileId,
}

/// ID of a chunk.
///
/// This ID is unique within a single partition.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkId(Uuid);

impl ChunkId {
    /// Create new, random ID.
    #[allow(clippy::new_without_default)] // `new` creates non-deterministic result
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// **TESTING ONLY:** Create new ID from integer.
    ///
    /// Since this can easily lead to ID collissions (which in turn can lead to panics), this must
    /// only be used for testing purposes!
    pub fn new_test(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }

    /// The chunk id is only effective in case the chunk's order is the same with another chunk.
    /// Hence collisions are safe in that context.
    pub fn new_id(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }

    /// Get inner UUID.
    pub fn get(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if (self.0.get_variant() == Some(uuid::Variant::RFC4122))
            && (self.0.get_version() == Some(uuid::Version::Random))
        {
            f.debug_tuple("ChunkId").field(&self.0).finish()
        } else {
            f.debug_tuple("ChunkId").field(&self.0.as_u128()).finish()
        }
    }
}

impl From<Uuid> for ChunkId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Order of a chunk.
///
/// This is used for:
/// 1. **upsert order:** chunks with higher order overwrite data in chunks with lower order
/// 2. **locking order:** chunks must be locked in consistent (ascending) order
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkOrder(NonZeroU32);

impl ChunkOrder {
    /// The minimum ordering value a chunk could have. Currently only used in testing.
    // TODO: remove `unsafe` once https://github.com/rust-lang/rust/issues/51999 is fixed
    pub const MIN: Self = Self(unsafe { NonZeroU32::new_unchecked(1) });

    /// Create a ChunkOrder from the given value.
    pub fn new(order: u32) -> Option<Self> {
        NonZeroU32::new(order).map(Self)
    }
}

/// `PartitionTemplate` is used to compute the partition key of each row that
/// gets written. It can consist of the table name, a column name and its value,
/// a formatted time, or a string column and regex captures of its value. For
/// columns that do not appear in the input row, a blank value is output.
///
/// The key is constructed in order of the template parts; thus ordering changes
/// what partition key is generated.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
#[allow(missing_docs)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    /// The name of a table
    Table,
    /// The value in a named column
    Column(String),
    /// Applies a  `strftime` format to the "time" column.
    ///
    /// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
    /// partition key parts such as "2021-03-14 12:25:21" and
    /// "2021-04-14 12:24:21"
    TimeFormat(String),
    /// Applies a regex to the value in a string column
    RegexCapture(RegexCapture),
    /// Applies a `strftime` pattern to some column other than "time"
    StrftimeColumn(StrftimeColumn),
}

/// `RegexCapture` is for pulling parts of a string column into the partition
/// key.
#[derive(Debug, Eq, PartialEq, Clone)]
#[allow(missing_docs)]
pub struct RegexCapture {
    pub column: String,
    pub regex: String,
}

/// [`StrftimeColumn`] is used to create a time based partition key off some
/// column other than the builtin `time` column.
///
/// The value of the named column is formatted using a `strftime`
/// style string.
///
/// For example, a time format of "%Y-%m-%d %H:%M:%S" will produce
/// partition key parts such as "2021-03-14 12:25:21" and
/// "2021-04-14 12:24:21"
#[derive(Debug, Eq, PartialEq, Clone)]
#[allow(missing_docs)]
pub struct StrftimeColumn {
    pub column: String,
    pub format: String,
}

/// Represents a parsed delete predicate for evaluation by the InfluxDB IOx
/// query engine.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeletePredicate {
    /// Only rows within this range are included in
    /// results. Other rows are excluded.
    pub range: TimestampRange,

    /// Optional arbitrary predicates, represented as list of
    /// expressions applied a logical conjunction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<DeleteExpr>,
}

impl DeletePredicate {
    /// Format expr to SQL string.
    pub fn expr_sql_string(&self) -> String {
        let mut out = String::new();
        for expr in &self.exprs {
            if !out.is_empty() {
                write!(&mut out, " AND ").expect("writing to a string shouldn't fail");
            }
            write!(&mut out, "{}", expr).expect("writing to a string shouldn't fail");
        }
        out
    }

    /// Return the approximate memory size of the predicate, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.exprs.iter().map(|expr| expr.size()).sum::<usize>()
    }
}

/// Single expression to be used as parts of a predicate.
///
/// Only very simple expression of the type `<column> <op> <scalar>` are supported.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeleteExpr {
    /// Column (w/o table name).
    pub column: String,

    /// Operator.
    pub op: Op,

    /// Scalar value.
    pub scalar: Scalar,
}

impl DeleteExpr {
    /// Create a new [`DeleteExpr`]
    pub fn new(column: String, op: Op, scalar: Scalar) -> Self {
        Self { column, op, scalar }
    }

    /// Column (w/o table name).
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Operator.
    pub fn op(&self) -> Op {
        self.op
    }

    /// Scalar value.
    pub fn scalar(&self) -> &Scalar {
        &self.scalar
    }

    /// Return the approximate memory size of the expression, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.column.capacity() + self.scalar.size()
    }
}

impl std::fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#""{}"{}{}"#,
            self.column().replace('\\', r#"\\"#).replace('"', r#"\""#),
            self.op(),
            self.scalar(),
        )
    }
}

/// Binary operator that can be evaluated on a column and a scalar value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    /// Strict equality (`=`).
    Eq,

    /// Inequality (`!=`).
    Ne,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "!="),
        }
    }
}

/// Scalar value of a certain type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum Scalar {
    Bool(bool),
    I64(i64),
    F64(ordered_float::OrderedFloat<f64>),
    String(String),
}

impl Scalar {
    /// Return the approximate memory size of the scalar, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self {
                Self::Bool(_) | Self::I64(_) | Self::F64(_) => 0,
                Self::String(s) => s.capacity(),
            }
    }
}

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Bool(value) => value.fmt(f),
            Scalar::I64(value) => value.fmt(f),
            Scalar::F64(value) => match value.classify() {
                FpCategory::Nan => write!(f, "'NaN'"),
                FpCategory::Infinite if *value.as_ref() < 0.0 => write!(f, "'-Infinity'"),
                FpCategory::Infinite => write!(f, "'Infinity'"),
                _ => write!(f, "{:?}", value.as_ref()),
            },
            Scalar::String(value) => {
                write!(
                    f,
                    "'{}'",
                    value.replace('\\', r#"\\"#).replace('\'', r#"\'"#),
                )
            }
        }
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum OrgBucketMappingError {
    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("missing org/bucket value"))]
    NotSpecified,
}

/// Map an InfluxDB 2.X org & bucket into an IOx DatabaseName.
///
/// This function ensures the mapping is unambiguous by requiring both `org` and
/// `bucket` to not contain the `_` character in addition to the
/// [`DatabaseName`] validation.
pub fn org_and_bucket_to_database<'a, O: AsRef<str>, B: AsRef<str>>(
    org: O,
    bucket: B,
) -> Result<DatabaseName<'a>, OrgBucketMappingError> {
    const SEPARATOR: char = '_';

    let org: Cow<'_, str> = utf8_percent_encode(org.as_ref(), NON_ALPHANUMERIC).into();
    let bucket: Cow<'_, str> = utf8_percent_encode(bucket.as_ref(), NON_ALPHANUMERIC).into();

    // An empty org or bucket is not acceptable.
    if org.is_empty() || bucket.is_empty() {
        return Err(OrgBucketMappingError::NotSpecified);
    }

    let db_name = format!("{}{}{}", org.as_ref(), SEPARATOR, bucket.as_ref());

    DatabaseName::new(db_name).context(InvalidDatabaseNameSnafu)
}

/// A string that cannot be empty
///
/// This is particularly useful for types that map to/from protobuf, where string fields
/// are not nullable - that is they default to an empty string if not specified
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonEmptyString(Box<str>);

impl NonEmptyString {
    /// Create a new `NonEmptyString` from the provided `String`
    ///
    /// Returns None if empty
    pub fn new(s: impl Into<String>) -> Option<Self> {
        let s = s.into();
        match s.is_empty() {
            true => None,
            false => Some(Self(s.into_boxed_str())),
        }
    }
}

impl Deref for NonEmptyString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/// Length constraints for a database name.
///
/// A `RangeInclusive` is a closed interval, covering [1, 64]
const LENGTH_CONSTRAINT: RangeInclusive<usize> = 1..=64;

/// Database name validation errors.
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum DatabaseNameError {
    #[snafu(display(
        "Database name {} length must be between {} and {} characters",
        name,
        LENGTH_CONSTRAINT.start(),
        LENGTH_CONSTRAINT.end()
    ))]
    LengthConstraint { name: String },

    #[snafu(display(
        "Database name '{}' contains invalid character. \
        Character number {} is a control which is not allowed.",
        name,
        bad_char_offset
    ))]
    BadChars {
        bad_char_offset: usize,
        name: String,
    },
}

/// A correctly formed database name.
///
/// Using this wrapper type allows the consuming code to enforce the invariant
/// that only valid names are provided.
///
/// This type derefs to a `str` and therefore can be used in place of anything
/// that is expecting a `str`:
///
/// ```rust
/// # use data_types::DatabaseName;
/// fn print_database(s: &str) {
///     println!("database name: {}", s);
/// }
///
/// let db = DatabaseName::new("data").unwrap();
/// print_database(&db);
/// ```
///
/// But this is not reciprocal - functions that wish to accept only
/// pre-validated names can use `DatabaseName` as a parameter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseName<'a>(Cow<'a, str>);

impl<'a> DatabaseName<'a> {
    /// Create a new, valid DatabaseName.
    pub fn new<T: Into<Cow<'a, str>>>(name: T) -> Result<Self, DatabaseNameError> {
        let name: Cow<'a, str> = name.into();

        if !LENGTH_CONSTRAINT.contains(&name.len()) {
            return Err(DatabaseNameError::LengthConstraint {
                name: name.to_string(),
            });
        }

        // Validate the name contains only valid characters.
        //
        // NOTE: If changing these characters, please update the error message
        // above.
        if let Some(bad_char_offset) = name.chars().position(|c| c.is_control()) {
            return BadCharsSnafu {
                bad_char_offset,
                name,
            }
            .fail();
        };

        Ok(Self(name))
    }

    /// Borrow a string slice of the name.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'a> std::convert::From<DatabaseName<'a>> for String {
    fn from(name: DatabaseName<'a>) -> Self {
        name.0.to_string()
    }
}

impl<'a> std::convert::From<&DatabaseName<'a>> for String {
    fn from(name: &DatabaseName<'a>) -> Self {
        name.to_string()
    }
}

impl<'a> std::convert::TryFrom<&'a str> for DatabaseName<'a> {
    type Error = DatabaseNameError;

    fn try_from(v: &'a str) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::convert::TryFrom<String> for DatabaseName<'a> {
    type Error = DatabaseNameError;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::ops::Deref for DatabaseName<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<'a> std::fmt::Display for DatabaseName<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Column name, statistics which encode type information
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnSummary {
    /// Column name
    pub name: String,

    /// Column's Influx data model type (if any)
    pub influxdb_type: Option<InfluxDbType>,

    /// Per column
    pub stats: Statistics,
}

impl ColumnSummary {
    /// Returns the total number of rows (including nulls) in this column
    pub fn total_count(&self) -> u64 {
        self.stats.total_count()
    }

    /// Updates statistics from other if the same type, otherwise a noop
    pub fn update_from(&mut self, other: &Self) {
        match (&mut self.stats, &other.stats) {
            (Statistics::F64(s), Statistics::F64(o)) => {
                s.update_from(o);
            }
            (Statistics::I64(s), Statistics::I64(o)) => {
                s.update_from(o);
            }
            (Statistics::Bool(s), Statistics::Bool(o)) => {
                s.update_from(o);
            }
            (Statistics::String(s), Statistics::String(o)) => {
                s.update_from(o);
            }
            (Statistics::U64(s), Statistics::U64(o)) => {
                s.update_from(o);
            }
            // do catch alls for the specific types, that way if a new type gets added, the compiler
            // will complain.
            (Statistics::F64(_), _) => unreachable!(),
            (Statistics::I64(_), _) => unreachable!(),
            (Statistics::U64(_), _) => unreachable!(),
            (Statistics::Bool(_), _) => unreachable!(),
            (Statistics::String(_), _) => unreachable!(),
        }
    }

    /// Updates these statistics so that that the total length of this
    /// column is `len` rows, padding it with trailing NULLs if
    /// necessary
    pub fn update_to_total_count(&mut self, len: u64) {
        let total_count = self.total_count();
        assert!(
            total_count <= len,
            "trying to shrink column stats from {} to {}",
            total_count,
            len
        );
        let delta = len - total_count;
        self.stats.update_for_nulls(delta);
    }

    /// Return size in bytes of this Column metadata (not the underlying column)
    pub fn size(&self) -> usize {
        mem::size_of::<Self>() + self.name.len() + self.stats.size()
    }
}

// Replicate this enum here as it can't be derived from the existing statistics
#[derive(Debug, PartialEq, Clone, Copy)]
#[allow(missing_docs)]
pub enum InfluxDbType {
    Tag,
    Field,
    Timestamp,
}

/// Address of the chunk within the catalog
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionAddr {
    /// Database name
    pub db_name: Arc<str>,

    /// What table does the chunk belong to?
    pub table_name: Arc<str>,

    /// What partition does the chunk belong to?
    pub partition_key: Arc<str>,
}

/// Summary statistics for a column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StatValues<T> {
    /// minimum (non-NaN, non-NULL) value, if any
    pub min: Option<T>,

    /// maximum (non-NaN, non-NULL) value, if any
    pub max: Option<T>,

    /// total number of values in this column, including null values
    pub total_count: u64,

    /// number of null values in this column
    pub null_count: Option<u64>,

    /// number of distinct values in this column if known
    ///
    /// This includes NULLs and NANs
    pub distinct_count: Option<NonZeroU64>,
}

/// Represents the result of comparing the min/max ranges of two [`StatValues`]
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum StatOverlap {
    /// There is at least one value that exists in both ranges
    NonZero,

    /// There are zero values that exists in both ranges
    Zero,

    /// It is not known if there are any intersections (e.g. because
    /// one of the bounds is not Known / is None)
    Unknown,
}

impl<T> StatValues<T>
where
    T: PartialOrd,
{
    /// returns information about the overlap between two `StatValues`
    pub fn overlaps(&self, other: &Self) -> StatOverlap {
        match (&self.min, &self.max, &other.min, &other.max) {
            (Some(self_min), Some(self_max), Some(other_min), Some(other_max)) => {
                if self_min <= other_max && self_max >= other_min {
                    StatOverlap::NonZero
                } else {
                    StatOverlap::Zero
                }
            }
            // At least one of the values was None
            _ => StatOverlap::Unknown,
        }
    }
}

impl<T> Default for StatValues<T> {
    fn default() -> Self {
        Self {
            min: None,
            max: None,
            total_count: 0,
            null_count: None,
            distinct_count: None,
        }
    }
}

impl<T> StatValues<T> {
    /// Create new statistics with no values
    pub fn new_empty() -> Self {
        Self {
            min: None,
            max: None,
            total_count: 0,
            null_count: Some(0),
            distinct_count: None,
        }
    }

    /// Returns true if both the min and max values are None (aka not known)
    pub fn is_none(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    /// Update the statistics values to account for `num_nulls` additional null values
    pub fn update_for_nulls(&mut self, num_nulls: u64) {
        self.total_count += num_nulls;
        self.null_count = self.null_count.map(|x| x + num_nulls);
    }

    /// updates the statistics keeping the min, max and incrementing count.
    ///
    /// The type plumbing exists to allow calling with &str on a StatValues<String>
    pub fn update<U: ?Sized>(&mut self, other: &U)
    where
        T: Borrow<U>,
        U: ToOwned<Owned = T> + PartialOrd + IsNan,
    {
        self.total_count += 1;
        self.distinct_count = None;

        if !other.is_nan() {
            match &self.min {
                None => self.min = Some(other.to_owned()),
                Some(s) => {
                    if s.borrow() > other {
                        self.min = Some(other.to_owned());
                    }
                }
            }

            match &self.max {
                None => {
                    self.max = Some(other.to_owned());
                }
                Some(s) => {
                    if other > s.borrow() {
                        self.max = Some(other.to_owned());
                    }
                }
            }
        }
    }
}

impl<T> StatValues<T>
where
    T: Clone + PartialOrd,
{
    /// Updates statistics from other
    pub fn update_from(&mut self, other: &Self) {
        self.total_count += other.total_count;
        self.null_count = self.null_count.zip(other.null_count).map(|(a, b)| a + b);

        // No way to accurately aggregate counts
        self.distinct_count = None;

        match (&self.min, &other.min) {
            (None, None) | (Some(_), None) => {}
            (None, Some(o)) => self.min = Some(o.clone()),
            (Some(s), Some(o)) => {
                if s > o {
                    self.min = Some(o.clone());
                }
            }
        }

        match (&self.max, &other.max) {
            (None, None) | (Some(_), None) => {}
            (None, Some(o)) => self.max = Some(o.clone()),
            (Some(s), Some(o)) => {
                if o > s {
                    self.max = Some(o.clone());
                }
            }
        };
    }
}

impl<T> StatValues<T>
where
    T: IsNan + PartialOrd,
{
    /// Create new statistics with the specified count and null count
    pub fn new(min: Option<T>, max: Option<T>, total_count: u64, null_count: Option<u64>) -> Self {
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create statistics for a column that only has nulls up to now
    pub fn new_all_null(total_count: u64, distinct_count: Option<u64>) -> Self {
        let min = None;
        let max = None;
        let null_count = Some(total_count);

        if let Some(count) = distinct_count {
            assert!(count > 0);
        }
        Self::new_with_distinct(
            min,
            max,
            total_count,
            null_count,
            distinct_count.map(|c| NonZeroU64::new(c).unwrap()),
        )
    }

    /// Create statistics for a column with zero nulls and unknown distinct count
    pub fn new_non_null(min: Option<T>, max: Option<T>, total_count: u64) -> Self {
        let null_count = Some(0);
        let distinct_count = None;
        Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
    }

    /// Create new statistics with the specified count and null count and distinct values
    pub fn new_with_distinct(
        min: Option<T>,
        max: Option<T>,
        total_count: u64,
        null_count: Option<u64>,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
        if let Some(min) = &min {
            assert!(!min.is_nan());
        }
        if let Some(max) = &max {
            assert!(!max.is_nan());
        }
        if let (Some(min), Some(max)) = (&min, &max) {
            assert!(min <= max);
        }

        Self {
            min,
            max,
            total_count,
            null_count,
            distinct_count,
        }
    }
}

/// Whether a type is NaN or not.
pub trait IsNan {
    /// Test for NaNess.
    fn is_nan(&self) -> bool;
}

impl<T: IsNan> IsNan for &T {
    fn is_nan(&self) -> bool {
        (*self).is_nan()
    }
}

macro_rules! impl_is_nan_false {
    ($t:ty) => {
        impl IsNan for $t {
            fn is_nan(&self) -> bool {
                false
            }
        }
    };
}

impl_is_nan_false!(bool);
impl_is_nan_false!(str);
impl_is_nan_false!(String);
impl_is_nan_false!(i8);
impl_is_nan_false!(i16);
impl_is_nan_false!(i32);
impl_is_nan_false!(i64);
impl_is_nan_false!(u8);
impl_is_nan_false!(u16);
impl_is_nan_false!(u32);
impl_is_nan_false!(u64);

impl IsNan for f64 {
    fn is_nan(&self) -> bool {
        Self::is_nan(*self)
    }
}

/// Statistics and type information for a column.
#[derive(Debug, PartialEq, Clone)]
#[allow(missing_docs)]
pub enum Statistics {
    I64(StatValues<i64>),
    U64(StatValues<u64>),
    F64(StatValues<f64>),
    Bool(StatValues<bool>),
    String(StatValues<String>),
}

impl Statistics {
    /// Returns the total number of rows in this column
    pub fn total_count(&self) -> u64 {
        match self {
            Self::I64(s) => s.total_count,
            Self::U64(s) => s.total_count,
            Self::F64(s) => s.total_count,
            Self::Bool(s) => s.total_count,
            Self::String(s) => s.total_count,
        }
    }

    /// Returns true if both the min and max values are None (aka not known)
    pub fn is_none(&self) -> bool {
        match self {
            Self::I64(v) => v.is_none(),
            Self::U64(v) => v.is_none(),
            Self::F64(v) => v.is_none(),
            Self::Bool(v) => v.is_none(),
            Self::String(v) => v.is_none(),
        }
    }

    /// Returns the number of null rows in this column
    pub fn null_count(&self) -> Option<u64> {
        match self {
            Self::I64(s) => s.null_count,
            Self::U64(s) => s.null_count,
            Self::F64(s) => s.null_count,
            Self::Bool(s) => s.null_count,
            Self::String(s) => s.null_count,
        }
    }

    /// Returns the distinct count if known
    pub fn distinct_count(&self) -> Option<NonZeroU64> {
        match self {
            Self::I64(s) => s.distinct_count,
            Self::U64(s) => s.distinct_count,
            Self::F64(s) => s.distinct_count,
            Self::Bool(s) => s.distinct_count,
            Self::String(s) => s.distinct_count,
        }
    }

    /// Update the statistics values to account for `num_nulls` additional null values
    pub fn update_for_nulls(&mut self, num_nulls: u64) {
        match self {
            Self::I64(v) => v.update_for_nulls(num_nulls),
            Self::U64(v) => v.update_for_nulls(num_nulls),
            Self::F64(v) => v.update_for_nulls(num_nulls),
            Self::Bool(v) => v.update_for_nulls(num_nulls),
            Self::String(v) => v.update_for_nulls(num_nulls),
        }
    }

    /// Return the size in bytes of this stats instance
    pub fn size(&self) -> usize {
        match self {
            Self::String(v) => std::mem::size_of::<Self>() + v.string_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }

    /// Return a human interpretable description of this type
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::I64(_) => "I64",
            Self::U64(_) => "U64",
            Self::F64(_) => "F64",
            Self::Bool(_) => "Bool",
            Self::String(_) => "String",
        }
    }
}

impl StatValues<String> {
    /// Returns the bytes associated by storing min/max string values
    pub fn string_size(&self) -> usize {
        self.min.as_ref().map(|x| x.len()).unwrap_or(0)
            + self.max.as_ref().map(|x| x.len()).unwrap_or(0)
    }
}

/// Metadata and statistics information for a table. This can be
/// either for the portion of a Table stored within a single chunk or
/// aggregated across chunks.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct TableSummary {
    /// Per column statistics
    pub columns: Vec<ColumnSummary>,
}

impl TableSummary {
    /// Get the column summary by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSummary> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Returns the total number of rows in the columns of this summary
    pub fn total_count(&self) -> u64 {
        // Assumes that all tables have the same number of rows, so
        // pick the first one
        let count = self.columns.get(0).map(|c| c.total_count()).unwrap_or(0);

        // Validate that the counts are consistent across columns
        for c in &self.columns {
            // Restore to assert when https://github.com/influxdata/influxdb_iox/issues/2124 is fixed
            if c.total_count() != count {
                warn!(column_name=%c.name,
                      column_count=c.total_count(), previous_count=count,
                      "Mismatch in statistics count, see #2124");
            }
        }
        count
    }

    /// Updates the table summary with combined stats from the other. Counts are
    /// treated as non-overlapping so they're just added together. If the
    /// type of a column differs between the two tables, no update is done
    /// on that column. Columns that only exist in the other are cloned into
    /// this table summary.
    pub fn update_from(&mut self, other: &Self) {
        let new_total_count = self.total_count() + other.total_count();

        // update all existing columns
        for col in &mut self.columns {
            if let Some(other_col) = other.column(&col.name) {
                col.update_from(other_col);
            } else {
                col.update_to_total_count(new_total_count);
            }
        }

        // Add any columns that were new
        for col in &other.columns {
            if self.column(&col.name).is_none() {
                let mut new_col = col.clone();
                // ensure the count is consistent
                new_col.update_to_total_count(new_total_count);
                self.columns.push(new_col);
            }
        }
    }

    /// Total size of all ColumnSummaries that belong to this table which include
    /// column names and their stats
    pub fn size(&self) -> usize {
        let size: usize = self.columns.iter().map(|c| c.size()).sum();
        size + mem::size_of::<Self>() // Add size of this struct that points to
                                      // table and ColumnSummary
    }

    /// Extracts min/max values of the timestamp column, if possible
    pub fn time_range(&self) -> Option<TimestampMinMax> {
        self.column(TIME_COLUMN_NAME).and_then(|c| {
            if let Statistics::I64(StatValues {
                min: Some(min),
                max: Some(max),
                ..
            }) = &c.stats
            {
                Some(TimestampMinMax::new(*min, *max))
            } else {
                None
            }
        })
    }
}

/// Kafka partition ID plus offset
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Sequence {
    /// The sequencer id (kafka partition id)
    pub sequencer_id: u32,
    /// The sequence number (kafka offset)
    pub sequence_number: u64,
}

impl Sequence {
    /// Create a new Sequence
    pub fn new(sequencer_id: u32, sequence_number: u64) -> Self {
        Self {
            sequencer_id,
            sequence_number,
        }
    }
}

/// minimum time that can be represented.
///
/// 1677-09-21 00:12:43.145224194 +0000 UTC
///
/// The two lowest minimum integers are used as sentinel values.  The
/// minimum value needs to be used as a value lower than any other value for
/// comparisons and another separate value is needed to act as a sentinel
/// default value that is unusable by the user, but usable internally.
/// Because these two values need to be used for a special purpose, we do
/// not allow users to write points at these two times.
///
/// Source: [influxdb](https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34)
pub const MIN_NANO_TIME: i64 = i64::MIN + 2;

/// maximum time that can be represented.
///
/// 2262-04-11 23:47:16.854775806 +0000 UTC
///
/// The highest time represented by a nanosecond needs to be used for an
/// exclusive range in the shard group, so the maximum time needs to be one
/// less than the possible maximum number of nanoseconds representable by an
/// int64 so that we don't lose a point at that one time.
/// Source: [influxdb](https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34)
pub const MAX_NANO_TIME: i64 = i64::MAX - 1;

/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and IOx in particular, that they are handled
/// specially
///
/// Timestamp ranges are defined such that a value `v` is within the
/// range iff:
///
/// ```text
///  range.start <= v < range.end
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Debug, Hash)]
pub struct TimestampRange {
    /// Start defines the inclusive lower bound. Minimum value is [MIN_NANO_TIME]
    start: i64,
    /// End defines the inclusive upper bound. Maximum value is [MAX_NANO_TIME]
    end: i64,
}

impl TimestampRange {
    /// Create a new TimestampRange. Clamps to MIN_NANO_TIME/MAX_NANO_TIME.
    pub fn new(start: i64, end: i64) -> Self {
        debug_assert!(end >= start);
        let start = start.max(MIN_NANO_TIME);
        let end = end.min(MAX_NANO_TIME);
        Self { start, end }
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains(&self, v: i64) -> bool {
        self.start <= v && v < self.end
    }

    /// Return the timestamp range's end.
    pub fn end(&self) -> i64 {
        self.end
    }

    /// Return the timestamp range's start.
    pub fn start(&self) -> i64 {
        self.start
    }
}

/// Specifies a min/max timestamp value.
///
/// Note this differs subtlety (but critically) from a
/// `TimestampRange` as the minimum and maximum values are included
#[derive(Clone, Debug, Copy)]
pub struct TimestampMinMax {
    /// The minimum timestamp value
    pub min: i64,
    /// the maximum timestamp value
    pub max: i64,
}

impl TimestampMinMax {
    /// Create a new TimestampMinMax. Panics if min > max.
    pub fn new(min: i64, max: i64) -> Self {
        assert!(min <= max, "expected min ({}) <= max ({})", min, max);
        Self { min, max }
    }

    #[inline]
    /// Returns true if any of the values between min / max
    /// (inclusive) are contained within the specified timestamp range
    pub fn overlaps(&self, range: TimestampRange) -> bool {
        range.contains(self.min)
            || range.contains(self.max)
            || (self.min <= range.start && self.max >= range.end)
    }
}

/// Specifies the status of data in the ingestion process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KafkaPartitionWriteStatus {
    /// Nothing is known about this write (e.g. it refers to a kafka
    /// partition for which we have no information)
    KafkaPartitionUnknown,
    /// The data has not yet been processed by the ingester, and thus is unreadable
    Durable,
    /// The data is readable, but not yet persisted
    Readable,
    /// The data is both readable and persisted to parquet
    Persisted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;
    use test_helpers::assert_contains;

    #[test]
    fn test_chunk_id_new() {
        // `ChunkId::new()` create new random ID
        assert_ne!(ChunkId::new(), ChunkId::new());
    }

    #[test]
    fn test_chunk_id_new_test() {
        // `ChunkId::new_test(...)` creates deterministic ID
        assert_eq!(ChunkId::new_test(1), ChunkId::new_test(1));
        assert_ne!(ChunkId::new_test(1), ChunkId::new_test(2));
    }

    #[test]
    fn test_chunk_id_debug_and_display() {
        // Random chunk IDs use UUID-format
        let id_random = ChunkId::new();
        let inner: Uuid = id_random.get();
        assert_eq!(format!("{:?}", id_random), format!("ChunkId({})", inner));
        assert_eq!(format!("{}", id_random), format!("ChunkId({})", inner));

        // Deterministic IDs use integer format
        let id_test = ChunkId::new_test(42);
        assert_eq!(format!("{:?}", id_test), "ChunkId(42)");
        assert_eq!(format!("{}", id_test), "ChunkId(42)");
    }

    #[test]
    fn test_expr_to_sql_no_expressions() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        assert_eq!(&pred.expr_sql_string(), "");
    }

    #[test]
    fn test_expr_to_sql_operators() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Ne,
                    scalar: Scalar::I64(2),
                },
            ],
        };
        assert_eq!(&pred.expr_sql_string(), r#""col1"=1 AND "col2"!=2"#);
    }

    #[test]
    fn test_expr_to_sql_column_escape() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col 1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from(r#"col\2"#),
                    op: Op::Eq,
                    scalar: Scalar::I64(2),
                },
                DeleteExpr {
                    column: String::from(r#"col"3"#),
                    op: Op::Eq,
                    scalar: Scalar::I64(3),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col 1"=1 AND "col\\2"=2 AND "col\"3"=3"#
        );
    }

    #[test]
    fn test_expr_to_sql_bool() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::Bool(false),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::Bool(true),
                },
            ],
        };
        assert_eq!(&pred.expr_sql_string(), r#""col1"=false AND "col2"=true"#);
    }

    #[test]
    fn test_expr_to_sql_i64() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(0),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::I64(-1),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::I64(i64::MIN),
                },
                DeleteExpr {
                    column: String::from("col5"),
                    op: Op::Eq,
                    scalar: Scalar::I64(i64::MAX),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"=0 AND "col2"=-1 AND "col3"=1 AND "col4"=-9223372036854775808 AND "col5"=9223372036854775807"#
        );
    }

    #[test]
    fn test_expr_to_sql_f64() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(0.0)),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(-0.0)),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(1.0)),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::INFINITY)),
                },
                DeleteExpr {
                    column: String::from("col5"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::NEG_INFINITY)),
                },
                DeleteExpr {
                    column: String::from("col6"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::NAN)),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"=0.0 AND "col2"=-0.0 AND "col3"=1.0 AND "col4"='Infinity' AND "col5"='-Infinity' AND "col6"='NaN'"#
        );
    }

    #[test]
    fn test_expr_to_sql_string() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from("")),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from("foo")),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from(r#"fo\o"#)),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from(r#"fo'o"#)),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"='' AND "col2"='foo' AND "col3"='fo\\o' AND "col4"='fo\'o'"#
        );
    }

    #[test]
    fn test_org_bucket_map_db_ok() {
        let got = org_and_bucket_to_database("org", "bucket").expect("failed on valid DB mapping");

        assert_eq!(got.as_str(), "org_bucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore() {
        let got = org_and_bucket_to_database("my_org", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%5Forg_bucket");

        let got = org_and_bucket_to_database("org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "org_my%5Fbucket");

        let got = org_and_bucket_to_database("org", "my__bucket").unwrap();
        assert_eq!(got.as_str(), "org_my%5F%5Fbucket");

        let got = org_and_bucket_to_database("my_org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "my%5Forg_my%5Fbucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore_and_percent() {
        let got = org_and_bucket_to_database("my%5Forg", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%255Forg_bucket");

        let got = org_and_bucket_to_database("my%5Forg_", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%255Forg%5F_bucket");
    }

    #[test]
    fn test_bad_database_name_is_encoded() {
        let got = org_and_bucket_to_database("org", "bucket?").unwrap();
        assert_eq!(got.as_str(), "org_bucket%3F");

        let got = org_and_bucket_to_database("org!", "bucket").unwrap();
        assert_eq!(got.as_str(), "org%21_bucket");
    }

    #[test]
    fn test_empty_org_bucket() {
        let err = org_and_bucket_to_database("", "")
            .expect_err("should fail with empty org/bucket valuese");
        assert!(matches!(err, OrgBucketMappingError::NotSpecified));
    }

    #[test]
    fn test_deref() {
        let db = DatabaseName::new("my_example_name").unwrap();
        assert_eq!(&*db, "my_example_name");
    }

    #[test]
    fn test_too_short() {
        let name = "".to_string();
        let got = DatabaseName::try_from(name).unwrap_err();

        assert!(matches!(
            got,
            DatabaseNameError::LengthConstraint { name: _n }
        ));
    }

    #[test]
    fn test_too_long() {
        let name = "my_example_name_that_is_quite_a_bit_longer_than_allowed_even_though_database_names_can_be_quite_long_bananas".to_string();
        let got = DatabaseName::try_from(name).unwrap_err();

        assert!(matches!(
            got,
            DatabaseNameError::LengthConstraint { name: _n }
        ));
    }

    #[test]
    fn test_bad_chars_null() {
        let got = DatabaseName::new("example\x00").unwrap_err();
        assert_contains!(got.to_string() , "Database name 'example\x00' contains invalid character. Character number 7 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_high_control() {
        let got = DatabaseName::new("\u{007f}example").unwrap_err();
        assert_contains!(got.to_string() , "Database name '\u{007f}example' contains invalid character. Character number 0 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_tab() {
        let got = DatabaseName::new("example\tdb").unwrap_err();
        assert_contains!(got.to_string() , "Database name 'example\tdb' contains invalid character. Character number 7 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_newline() {
        let got = DatabaseName::new("my_example\ndb").unwrap_err();
        assert_contains!(got.to_string() , "Database name 'my_example\ndb' contains invalid character. Character number 10 is a control which is not allowed.");
    }

    #[test]
    fn test_ok_chars() {
        let db = DatabaseName::new("my-example-db_with_underscores and spaces").unwrap();
        assert_eq!(&*db, "my-example-db_with_underscores and spaces");
    }

    #[test]
    fn statistics_new_non_null() {
        let actual = StatValues::new_non_null(Some(-1i64), Some(1i64), 3);
        let expected = StatValues {
            min: Some(-1i64),
            max: Some(1i64),
            total_count: 3,
            null_count: Some(0),
            distinct_count: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn statistics_new_all_null() {
        // i64 values do not have a distinct count
        let actual = StatValues::<i64>::new_all_null(3, None);
        let expected = StatValues {
            min: None,
            max: None,
            total_count: 3,
            null_count: Some(3),
            distinct_count: None,
        };
        assert_eq!(actual, expected);

        // string columns can have a distinct count
        let actual = StatValues::<i64>::new_all_null(3, Some(1_u64));
        let expected = StatValues {
            min: None,
            max: None,
            total_count: 3,
            null_count: Some(3),
            distinct_count: Some(NonZeroU64::try_from(1_u64).unwrap()),
        };
        assert_eq!(actual, expected);
    }

    impl<T> StatValues<T>
    where
        T: IsNan + PartialOrd + Clone,
    {
        fn new_with_value(starting_value: T) -> Self {
            let starting_value = if starting_value.is_nan() {
                None
            } else {
                Some(starting_value)
            };

            let min = starting_value.clone();
            let max = starting_value;
            let total_count = 1;
            let null_count = Some(0);
            let distinct_count = None;
            Self::new_with_distinct(min, max, total_count, null_count, distinct_count)
        }
    }

    impl Statistics {
        /// Return the minimum value, if any, formatted as a string
        fn min_as_str(&self) -> Option<Cow<'_, str>> {
            match self {
                Self::I64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::U64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::F64(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::Bool(v) => v.min.map(|x| Cow::Owned(x.to_string())),
                Self::String(v) => v.min.as_deref().map(Cow::Borrowed),
            }
        }

        /// Return the maximum value, if any, formatted as a string
        fn max_as_str(&self) -> Option<Cow<'_, str>> {
            match self {
                Self::I64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::U64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::F64(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::Bool(v) => v.max.map(|x| Cow::Owned(x.to_string())),
                Self::String(v) => v.max.as_deref().map(Cow::Borrowed),
            }
        }
    }

    #[test]
    fn statistics_update() {
        let mut stat = StatValues::new_with_value(23);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(23));
        assert_eq!(stat.total_count, 1);

        stat.update(&55);
        assert_eq!(stat.min, Some(23));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 2);

        stat.update(&6);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 3);

        stat.update(&30);
        assert_eq!(stat.min, Some(6));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 4);
    }

    #[test]
    fn statistics_default() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update(&55);
        assert_eq!(stat.min, Some(55));
        assert_eq!(stat.max, Some(55));
        assert_eq!(stat.total_count, 1);

        let mut stat = StatValues::<String>::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update("cupcakes");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("cupcakes".to_string()));
        assert_eq!(stat.total_count, 1);

        stat.update("woo");
        assert_eq!(stat.min, Some("cupcakes".to_string()));
        assert_eq!(stat.max, Some("woo".to_string()));
        assert_eq!(stat.total_count, 2);
    }

    #[test]
    fn statistics_is_none() {
        let mut stat = StatValues::default();
        assert!(stat.is_none());
        stat.min = Some(0);
        assert!(!stat.is_none());
        stat.max = Some(1);
        assert!(!stat.is_none());
    }

    #[test]
    fn statistics_overlaps() {
        let stat1 = StatValues {
            min: Some(10),
            max: Some(20),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        // [--stat2--]
        let stat2 = StatValues {
            min: Some(5),
            max: Some(15),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::NonZero);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        //        [--stat3--]
        let stat3 = StatValues {
            min: Some(15),
            max: Some(25),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat3), StatOverlap::NonZero);
        assert_eq!(stat3.overlaps(&stat1), StatOverlap::NonZero);

        //    [--stat1--]
        //                [--stat4--]
        let stat4 = StatValues {
            min: Some(25),
            max: Some(35),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat4), StatOverlap::Zero);
        assert_eq!(stat4.overlaps(&stat1), StatOverlap::Zero);

        //              [--stat1--]
        // [--stat5--]
        let stat5 = StatValues {
            min: Some(0),
            max: Some(5),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat5), StatOverlap::Zero);
        assert_eq!(stat5.overlaps(&stat1), StatOverlap::Zero);
    }

    #[test]
    fn statistics_overlaps_none() {
        let stat1 = StatValues {
            min: Some(10),
            max: Some(20),
            ..Default::default()
        };

        let stat2 = StatValues {
            min: None,
            max: Some(20),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::Unknown);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::Unknown);

        let stat3 = StatValues {
            min: Some(10),
            max: None,
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat3), StatOverlap::Unknown);
        assert_eq!(stat3.overlaps(&stat1), StatOverlap::Unknown);

        let stat4 = StatValues {
            min: None,
            max: None,
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat4), StatOverlap::Unknown);
        assert_eq!(stat4.overlaps(&stat1), StatOverlap::Unknown);
    }

    #[test]
    fn statistics_overlaps_mixed_none() {
        let stat1 = StatValues {
            min: Some(10),
            max: None,
            ..Default::default()
        };

        let stat2 = StatValues {
            min: None,
            max: Some(5),
            ..Default::default()
        };
        assert_eq!(stat1.overlaps(&stat2), StatOverlap::Unknown);
        assert_eq!(stat2.overlaps(&stat1), StatOverlap::Unknown);
    }

    #[test]
    fn update_string() {
        let mut stat = StatValues::new_with_value("bbb".to_string());
        assert_eq!(stat.min, Some("bbb".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.total_count, 1);

        stat.update("aaa");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("bbb".to_string()));
        assert_eq!(stat.total_count, 2);

        stat.update("z");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.total_count, 3);

        stat.update("p");
        assert_eq!(stat.min, Some("aaa".to_string()));
        assert_eq!(stat.max, Some("z".to_string()));
        assert_eq!(stat.total_count, 4);
    }

    #[test]
    fn stats_is_none() {
        let stat = Statistics::I64(StatValues::new_non_null(Some(-1), Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new_non_null(None, Some(100), 1));
        assert!(!stat.is_none());

        let stat = Statistics::I64(StatValues::new_non_null(None, None, 0));
        assert!(stat.is_none());
    }

    #[test]
    fn stats_as_str_i64() {
        let stat = Statistics::I64(StatValues::new_non_null(Some(-1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("-1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::I64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_u64() {
        let stat = Statistics::U64(StatValues::new_non_null(Some(1), Some(100), 1));
        assert_eq!(stat.min_as_str(), Some("1".into()));
        assert_eq!(stat.max_as_str(), Some("100".into()));

        let stat = Statistics::U64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_f64() {
        let stat = Statistics::F64(StatValues::new_non_null(Some(99.0), Some(101.0), 1));
        assert_eq!(stat.min_as_str(), Some("99".into()));
        assert_eq!(stat.max_as_str(), Some("101".into()));

        let stat = Statistics::F64(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_bool() {
        let stat = Statistics::Bool(StatValues::new_non_null(Some(false), Some(true), 1));
        assert_eq!(stat.min_as_str(), Some("false".into()));
        assert_eq!(stat.max_as_str(), Some("true".into()));

        let stat = Statistics::Bool(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn stats_as_str_str() {
        let stat = Statistics::String(StatValues::new_non_null(
            Some("a".to_string()),
            Some("zz".to_string()),
            1,
        ));
        assert_eq!(stat.min_as_str(), Some("a".into()));
        assert_eq!(stat.max_as_str(), Some("zz".into()));

        let stat = Statistics::String(StatValues::new_non_null(None, None, 1));
        assert_eq!(stat.min_as_str(), None);
        assert_eq!(stat.max_as_str(), None);
    }

    #[test]
    fn table_update_from() {
        let mut string_stats = StatValues::new_with_value("foo".to_string());
        string_stats.update("bar");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(1);
        int_stats.update(&5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(int_stats),
        };

        let mut float_stats = StatValues::new_with_value(9.1);
        float_stats.update(&1.3);
        let float_col = ColumnSummary {
            name: "float".to_string(),
            influxdb_type: None,
            stats: Statistics::F64(float_stats),
        };

        let mut table_a = TableSummary {
            columns: vec![string_col, int_col, float_col],
        };

        let mut string_stats = StatValues::new_with_value("aaa".to_string());
        string_stats.update("zzz");
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let mut int_stats = StatValues::new_with_value(3);
        int_stats.update(&9);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(int_stats),
        };

        let mut table_b = TableSummary {
            columns: vec![int_col, string_col],
        };

        // keep this to test joining the other way
        let table_c = table_a.clone();

        table_a.update_from(&table_b);
        let col = table_a.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new_non_null(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4,
            ))
        );

        let col = table_a.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(9), 4))
        );

        let col = table_a.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 4, Some(2)))
        );

        table_b.update_from(&table_c);
        let col = table_b.column("string").unwrap();
        assert_eq!(
            col.stats,
            Statistics::String(StatValues::new_non_null(
                Some("aaa".to_string()),
                Some("zzz".to_string()),
                4,
            ))
        );

        let col = table_b.column("int").unwrap();
        assert_eq!(
            col.stats,
            Statistics::I64(StatValues::new_non_null(Some(1), Some(9), 4))
        );

        let col = table_b.column("float").unwrap();
        assert_eq!(
            col.stats,
            Statistics::F64(StatValues::new(Some(1.3), Some(9.1), 4, Some(2)))
        );
    }

    #[test]
    fn table_update_from_new_column() {
        let string_stats = StatValues::new_with_value("bar".to_string());
        let string_col = ColumnSummary {
            name: "string".to_string(),
            influxdb_type: None,
            stats: Statistics::String(string_stats),
        };

        let int_stats = StatValues::new_with_value(5);
        let int_col = ColumnSummary {
            name: "int".to_string(),
            influxdb_type: None,
            stats: Statistics::I64(int_stats),
        };

        // table summary that does not have the "string" col
        let table1 = TableSummary {
            columns: vec![int_col.clone()],
        };

        // table summary that has both columns
        let table2 = TableSummary {
            columns: vec![int_col, string_col],
        };

        // Statistics should be the same regardless of the order we update the stats

        let expected_string_stats = Statistics::String(StatValues::new(
            Some("bar".to_string()),
            Some("bar".to_string()),
            2,       // total count is 2 even though did not appear in the update
            Some(1), // 1 null
        ));

        let expected_int_stats = Statistics::I64(StatValues::new(
            Some(5),
            Some(5),
            2,
            Some(0), // no nulls
        ));

        // update table 1 with table 2
        let mut table = table1.clone();
        table.update_from(&table2);

        assert_eq!(
            &table.column("string").unwrap().stats,
            &expected_string_stats
        );

        assert_eq!(&table.column("int").unwrap().stats, &expected_int_stats);

        // update table 2 with table 1
        let mut table = table2;
        table.update_from(&table1);

        assert_eq!(
            &table.column("string").unwrap().stats,
            &expected_string_stats
        );

        assert_eq!(&table.column("int").unwrap().stats, &expected_int_stats);
    }

    #[test]
    fn column_update_from_boolean() {
        let bool_false = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
            stats: Statistics::Bool(StatValues::new(Some(false), Some(false), 1, Some(1))),
        };
        let bool_true = ColumnSummary {
            name: "b".to_string(),
            influxdb_type: None,
            stats: Statistics::Bool(StatValues::new(Some(true), Some(true), 1, Some(2))),
        };

        let expected_stats = Statistics::Bool(StatValues::new(Some(false), Some(true), 2, Some(3)));

        let mut b = bool_false.clone();
        b.update_from(&bool_true);
        assert_eq!(b.stats, expected_stats);

        let mut b = bool_true;
        b.update_from(&bool_false);
        assert_eq!(b.stats, expected_stats);
    }

    #[test]
    fn column_update_from_u64() {
        let mut min = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: None,
            stats: Statistics::U64(StatValues::new(Some(5), Some(23), 1, Some(1))),
        };

        let max = ColumnSummary {
            name: "foo".to_string(),
            influxdb_type: None,
            stats: Statistics::U64(StatValues::new(Some(6), Some(506), 43, Some(2))),
        };

        min.update_from(&max);

        let expected = Statistics::U64(StatValues::new(Some(5), Some(506), 44, Some(3)));
        assert_eq!(min.stats, expected);
    }

    #[test]
    fn nans() {
        let mut stat = StatValues::default();
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 0);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 1);

        stat.update(&1.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(1.0));
        assert_eq!(stat.total_count, 2);

        stat.update(&2.0);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(2.0));
        assert_eq!(stat.total_count, 3);

        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 4);

        stat.update(&-1.0);
        assert_eq!(stat.min, Some(-1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 5);

        // ===========

        let mut stat = StatValues::new_with_value(2.0);
        stat.update(&f64::INFINITY);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 2);

        stat.update(&f64::NAN);
        assert_eq!(stat.min, Some(2.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 3);

        // ===========

        let mut stat2 = StatValues::new_with_value(1.0);
        stat2.update_from(&stat);
        assert_eq!(stat2.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat2.total_count, 4);

        // ===========

        let stat2 = StatValues::new_with_value(1.0);
        stat.update_from(&stat2);
        assert_eq!(stat.min, Some(1.0));
        assert_eq!(stat.max, Some(f64::INFINITY));
        assert_eq!(stat.total_count, 4);

        // ===========

        let stat = StatValues::new_with_value(f64::NAN);
        assert_eq!(stat.min, None);
        assert_eq!(stat.max, None);
        assert_eq!(stat.total_count, 1);
    }

    #[test]
    fn test_timestamp_nano_min_max() {
        let cases = vec![
            (
                "MIN/MAX Nanos",
                TimestampRange::new(MIN_NANO_TIME, MAX_NANO_TIME),
            ),
            ("MIN/MAX i64", TimestampRange::new(i64::MIN, i64::MAX)),
        ];

        for (name, range) in cases {
            println!("case: {}", name);
            assert!(!range.contains(i64::MIN));
            assert!(range.contains(MIN_NANO_TIME));
            assert!(range.contains(MIN_NANO_TIME + 1));
            assert!(range.contains(MAX_NANO_TIME - 1));
            assert!(!range.contains(MAX_NANO_TIME));
            assert!(!range.contains(i64::MAX));
        }
    }

    #[test]
    fn test_timestamp_i64_min_max_offset() {
        let range = TimestampRange::new(MIN_NANO_TIME + 1, MAX_NANO_TIME - 1);

        assert!(!range.contains(i64::MIN));
        assert!(!range.contains(MIN_NANO_TIME));
        assert!(range.contains(MIN_NANO_TIME + 1));
        assert!(range.contains(MAX_NANO_TIME - 2));
        assert!(!range.contains(MAX_NANO_TIME - 1));
        assert!(!range.contains(MAX_NANO_TIME));
        assert!(!range.contains(i64::MAX));
    }

    #[test]
    fn test_timestamp_range_contains() {
        let range = TimestampRange::new(100, 200);
        assert!(!range.contains(99));
        assert!(range.contains(100));
        assert!(range.contains(101));
        assert!(range.contains(199));
        assert!(!range.contains(200));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_timestamp_range_overlaps() {
        let range = TimestampRange::new(100, 200);
        assert!(!TimestampMinMax::new(0, 99).overlaps(range));
        assert!(TimestampMinMax::new(0, 100).overlaps(range));
        assert!(TimestampMinMax::new(0, 101).overlaps(range));

        assert!(TimestampMinMax::new(0, 200).overlaps(range));
        assert!(TimestampMinMax::new(0, 201).overlaps(range));
        assert!(TimestampMinMax::new(0, 300).overlaps(range));

        assert!(TimestampMinMax::new(100, 101).overlaps(range));
        assert!(TimestampMinMax::new(100, 200).overlaps(range));
        assert!(TimestampMinMax::new(100, 201).overlaps(range));

        assert!(TimestampMinMax::new(101, 101).overlaps(range));
        assert!(TimestampMinMax::new(101, 200).overlaps(range));
        assert!(TimestampMinMax::new(101, 201).overlaps(range));

        assert!(!TimestampMinMax::new(200, 200).overlaps(range));
        assert!(!TimestampMinMax::new(200, 201).overlaps(range));

        assert!(!TimestampMinMax::new(201, 300).overlaps(range));
    }

    #[test]
    #[should_panic(expected = "expected min (2) <= max (1)")]
    fn test_timestamp_min_max_invalid() {
        TimestampMinMax::new(2, 1);
    }

    #[test]
    fn test_table_schema_size() {
        let schema1 = TableSchema {
            id: TableId::new(1),
            columns: BTreeMap::from([]),
        };
        let schema2 = TableSchema {
            id: TableId::new(2),
            columns: BTreeMap::from([(
                String::from("foo"),
                ColumnSchema {
                    id: ColumnId::new(1),
                    column_type: ColumnType::Bool,
                },
            )]),
        };
        assert!(schema1.size() < schema2.size());
    }

    #[test]
    fn test_namespace_schema_size() {
        let schema1 = NamespaceSchema {
            id: NamespaceId::new(1),
            kafka_topic_id: KafkaTopicId::new(2),
            query_pool_id: QueryPoolId::new(3),
            tables: BTreeMap::from([]),
        };
        let schema2 = NamespaceSchema {
            id: NamespaceId::new(1),
            kafka_topic_id: KafkaTopicId::new(2),
            query_pool_id: QueryPoolId::new(3),
            tables: BTreeMap::from([(String::from("foo"), TableSchema::new(TableId::new(1)))]),
        };
        assert!(schema1.size() < schema2.size());
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_add_i64() {
        let _ = Timestamp::new(i64::MAX) + 1;
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_sub_i64() {
        let _ = Timestamp::new(i64::MIN) - 1;
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_add_timestamp() {
        let _ = Timestamp::new(i64::MAX) + Timestamp::new(1);
    }

    #[test]
    #[should_panic = "timestamp wraparound"]
    fn test_timestamp_wraparound_panic_sub_timestamp() {
        let _ = Timestamp::new(i64::MIN) - Timestamp::new(1);
    }
}
