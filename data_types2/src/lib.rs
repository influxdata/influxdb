//! Shared data types in the Iox NG architecture

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
use schema::{builder::SchemaBuilder, sort::SortKey, InfluxColumnType, InfluxFieldType, Schema};
use std::{
    collections::BTreeMap,
    convert::TryFrom,
    fmt::{Debug, Formatter},
    num::NonZeroU32,
    ops::{Add, Sub},
    sync::Arc,
};
use uuid::Uuid;

pub use data_types::{
    delete_predicate::{DeleteExpr, DeletePredicate, Op, Scalar},
    names::{org_and_bucket_to_database, OrgBucketMappingError},
    non_empty::NonEmptyString,
    partition_metadata::{
        ColumnSummary, InfluxDbType, PartitionAddr, StatValues, Statistics, TableSummary,
    },
    sequence::Sequence,
    timestamp::{TimestampMinMax, TimestampRange, MAX_NANO_TIME, MIN_NANO_TIME},
    DatabaseName,
};

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
/// "sequencer_number" in the `write_buffer` which currently means
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

/// A time in nanoseconds from epoch
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

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, other: i64) -> Self {
        Self(self.0 + other)
    }
}

impl Sub<i64> for Timestamp {
    type Output = Self;

    fn sub(self, other: i64) -> Self {
        Self(self.0 - other)
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

/// Address of the chunk within the catalog
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct ChunkAddr {
    /// Database name
    pub db_name: Arc<str>,

    /// What table does the chunk belong to?
    pub table_name: Arc<str>,

    /// What partition does the chunk belong to?
    pub partition_key: Arc<str>,

    /// The ID of the chunk
    pub chunk_id: ChunkId,
}

impl std::fmt::Display for ChunkAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunk('{}':'{}':'{}':{})",
            self.db_name,
            self.table_name,
            self.partition_key,
            self.chunk_id.get()
        )
    }
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
    pub fn new_id_for_ng(id: u128) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
