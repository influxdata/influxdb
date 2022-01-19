//! This module contains the traits and data objects for the Catalog API.

use async_trait::async_trait;
use influxdb_line_protocol::FieldValue;
use snafu::{OptionExt, Snafu};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Name {} already exists", name))]
    NameExists { name: String },

    #[snafu(display("Unhandled sqlx error: {}", source))]
    SqlxError { source: sqlx::Error },

    #[snafu(display("Foreign key violation: {}", source))]
    ForeignKeyViolation { source: sqlx::Error },

    #[snafu(display("Column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: String,
        new: String,
    },

    #[snafu(display(
        "Column type {} is in the db for column {}, which is unknown",
        data_type,
        name
    ))]
    UnknownColumnType { data_type: i16, name: String },

    #[snafu(display("namespace {} not found", name))]
    NamespaceNotFound { name: String },

    #[snafu(display("parquet file with object_store_id {} already exists", object_store_id))]
    FileExists { object_store_id: Uuid },

    #[snafu(display("parquet_file record {} not found", id))]
    ParquetRecordNotFound { id: ParquetFileId },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Unique ID for a `Namespace`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct NamespaceId(i32);

#[allow(missing_docs)]
impl NamespaceId {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i32 {
        self.0
    }
}

/// Unique ID for a `KafkaTopic`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct KafkaTopicId(i32);

#[allow(missing_docs)]
impl KafkaTopicId {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i32 {
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
pub struct QueryPoolId(i16);

#[allow(missing_docs)]
impl QueryPoolId {
    pub fn new(v: i16) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i16 {
        self.0
    }
}

/// Unique ID for a `Table`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct TableId(i32);

#[allow(missing_docs)]
impl TableId {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i32 {
        self.0
    }
}

/// Unique ID for a `Column`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ColumnId(i32);

#[allow(missing_docs)]
impl ColumnId {
    pub fn new(v: i32) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i32 {
        self.0
    }
}

/// Unique ID for a `Sequencer`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct SequencerId(i16);

#[allow(missing_docs)]
impl SequencerId {
    pub fn new(v: i16) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i16 {
        self.0
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

/// Container that can return repos for each of the catalog data types.
#[async_trait]
pub trait RepoCollection {
    /// repo for kafka topics
    fn kafka_topic(&self) -> Arc<dyn KafkaTopicRepo + Sync + Send>;
    /// repo fo rquery pools
    fn query_pool(&self) -> Arc<dyn QueryPoolRepo + Sync + Send>;
    /// repo for namespaces
    fn namespace(&self) -> Arc<dyn NamespaceRepo + Sync + Send>;
    /// repo for tables
    fn table(&self) -> Arc<dyn TableRepo + Sync + Send>;
    /// repo for columns
    fn column(&self) -> Arc<dyn ColumnRepo + Sync + Send>;
    /// repo for sequencers
    fn sequencer(&self) -> Arc<dyn SequencerRepo + Sync + Send>;
    /// repo for partitions
    fn partition(&self) -> Arc<dyn PartitionRepo + Sync + Send>;
    /// repo for tombstones
    fn tombstone(&self) -> Arc<dyn TombstoneRepo + Sync + Send>;
    /// repo for parquet_files
    fn parquet_file(&self) -> Arc<dyn ParquetFileRepo + Sync + Send>;
}

/// Functions for working with Kafka topics in the catalog.
#[async_trait]
pub trait KafkaTopicRepo {
    /// Creates the kafka topic in the catalog or gets the existing record by name.
    async fn create_or_get(&self, name: &str) -> Result<KafkaTopic>;

    /// Gets the kafka topic by its unique name
    async fn get_by_name(&self, name: &str) -> Result<Option<KafkaTopic>>;
}

/// Functions for working with query pools in the catalog.
#[async_trait]
pub trait QueryPoolRepo {
    /// Creates the query pool in the catalog or gets the existing record by name.
    async fn create_or_get(&self, name: &str) -> Result<QueryPool>;
}

/// Functions for working with namespaces in the catalog
#[async_trait]
pub trait NamespaceRepo {
    /// Creates the namespace in the catalog. If one by the same name already exists, an
    /// error is returned.
    async fn create(
        &self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: KafkaTopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace>;

    /// Gets the namespace by its unique name.
    async fn get_by_name(&self, name: &str) -> Result<Option<Namespace>>;
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo {
    /// Creates the table in the catalog or get the existing record by name.
    async fn create_or_get(&self, name: &str, namespace_id: NamespaceId) -> Result<Table>;

    /// Lists all tables in the catalog for the given namespace id.
    async fn list_by_namespace_id(&self, namespace_id: NamespaceId) -> Result<Vec<Table>>;
}

/// Functions for working with columns in the catalog
#[async_trait]
pub trait ColumnRepo {
    /// Creates the column in the catalog or returns the existing column. Will return a
    /// `Error::ColumnTypeMismatch` if the existing column type doesn't match the type
    /// the caller is attempting to create.
    async fn create_or_get(
        &self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&self, namespace_id: NamespaceId) -> Result<Vec<Column>>;
}

/// Functions for working with sequencers in the catalog
#[async_trait]
pub trait SequencerRepo {
    /// create a sequencer record for the kafka topic and partition or return the existing record
    async fn create_or_get(
        &self,
        topic: &KafkaTopic,
        partition: KafkaPartition,
    ) -> Result<Sequencer>;

    /// get the sequencer record by `KafkaTopicId` and `KafkaPartition`
    async fn get_by_topic_id_and_partition(
        &self,
        topic_id: KafkaTopicId,
        partition: KafkaPartition,
    ) -> Result<Option<Sequencer>>;

    /// list all sequencers
    async fn list(&self) -> Result<Vec<Sequencer>>;

    /// list all sequencers for a given kafka topic
    async fn list_by_kafka_topic(&self, topic: &KafkaTopic) -> Result<Vec<Sequencer>>;
}

/// Functions for working with IOx partitions in the catalog. Note that these are how
/// IOx splits up data within a database, which is differenet than Kafka partitions.
#[async_trait]
pub trait PartitionRepo {
    /// create or get a partition record for the given partition key, sequencer and table
    async fn create_or_get(
        &self,
        key: &str,
        sequencer_id: SequencerId,
        table_id: TableId,
    ) -> Result<Partition>;

    /// return partitions for a given sequencer
    async fn list_by_sequencer(&self, sequencer_id: SequencerId) -> Result<Vec<Partition>>;
}

/// Functions for working with tombstones in the catalog
#[async_trait]
pub trait TombstoneRepo {
    /// create or get a tombstone
    async fn create_or_get(
        &self,
        table_id: TableId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone>;

    /// return all tombstones for the sequencer with a sequence number greater than that
    /// passed in. This will be used by the ingester on startup to see what tombstones
    /// might have to be applied to data that is read from the write buffer.
    async fn list_tombstones_by_sequencer_greater_than(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>>;
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo {
    /// create the parquet file
    #[allow(clippy::too_many_arguments)]
    async fn create(
        &self,
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        object_store_id: Uuid,
        min_sequence_number: SequenceNumber,
        max_sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<ParquetFile>;

    /// Flag the parquet file for deletion
    async fn flag_for_delete(&self, id: ParquetFileId) -> Result<()>;

    /// Get all parquet files for a sequencer with a max_sequence_number greater than the
    /// one passed in. The ingester will use this on startup to see which files were persisted
    /// that are greater than its min_unpersisted_number so that it can discard any data in
    /// these partitions on replay.
    async fn list_by_sequencer_greater_than(
        &self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>>;
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

    /// Adds tables and columns to the `NamespaceSchema`. These are created
    /// incrementally while validating the schema for a write and this helper
    /// method takes them in to add them to the schema.
    pub fn add_tables_and_columns(
        &mut self,
        new_tables: BTreeMap<String, TableId>,
        new_columns: BTreeMap<TableId, BTreeMap<String, ColumnSchema>>,
    ) {
        for (table_name, table_id) in new_tables {
            self.tables
                .entry(table_name)
                .or_insert_with(|| TableSchema::new(table_id));
        }

        for (table_id, new_columns) in new_columns {
            let table = self
                .get_table_mut(table_id)
                .expect("table must be in namespace to add columns");
            table.add_columns(new_columns);
        }
    }

    fn get_table_mut(&mut self, table_id: TableId) -> Option<&mut TableSchema> {
        for table in self.tables.values_mut() {
            if table.id == table_id {
                return Some(table);
            }
        }

        None
    }
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_name<T: RepoCollection + Send + Sync>(
    name: &str,
    repo: &T,
) -> Result<Option<NamespaceSchema>> {
    let namespace_repo = repo.namespace();
    let table_repo = repo.table();
    let column_repo = repo.column();

    let namespace = namespace_repo
        .get_by_name(name)
        .await?
        .context(NamespaceNotFoundSnafu { name })?;

    // get the columns first just in case someone else is creating schema while we're doing this.
    let columns = column_repo.list_by_namespace_id(namespace.id).await?;
    let tables = table_repo.list_by_namespace_id(namespace.id).await?;

    let mut namespace = NamespaceSchema::new(
        namespace.id,
        namespace.kafka_topic_id,
        namespace.query_pool_id,
    );

    let mut table_id_to_schema = BTreeMap::new();
    for t in tables {
        table_id_to_schema.insert(t.id, (t.name, TableSchema::new(t.id)));
    }

    for c in columns {
        let (_, t) = table_id_to_schema.get_mut(&c.table_id).unwrap();
        match ColumnType::try_from(c.column_type) {
            Ok(column_type) => {
                t.columns.insert(
                    c.name,
                    ColumnSchema {
                        id: c.id,
                        column_type,
                    },
                );
            }
            _ => {
                return Err(Error::UnknownColumnType {
                    data_type: c.column_type,
                    name: c.name.to_string(),
                });
            }
        }
    }

    for (_, (table_name, schema)) in table_id_to_schema {
        namespace.tables.insert(table_name, schema);
    }

    Ok(Some(namespace))
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

    /// Add the map of columns to the `TableSchema`
    pub fn add_columns(&mut self, columns: BTreeMap<String, ColumnSchema>) {
        for (name, column) in columns {
            self.columns.insert(name, column);
        }
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
            ColumnType::I64 => "i64",
            ColumnType::U64 => "u64",
            ColumnType::F64 => "f64",
            ColumnType::Bool => "bool",
            ColumnType::String => "string",
            ColumnType::Time => "time",
            ColumnType::Tag => "tag",
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

    fn try_from(value: i16) -> std::prelude::rust_2015::Result<Self, Self::Error> {
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

/// Data object for a partition. The combination of sequencer, table and key are unique (i.e. only one record can exist for each combo)
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
}

/// Data object for a tombstone.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
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

/// Data for a parquet file reference in the catalog.
#[derive(Debug, Copy, Clone, PartialEq, sqlx::FromRow)]
pub struct ParquetFile {
    /// the id of the file in the catalog
    pub id: ParquetFileId,
    /// the sequencer that sequenced writes that went into this file
    pub sequencer_id: SequencerId,
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
    /// flag to mark that this file should be deleted from object storage
    pub to_delete: bool,
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use futures::{stream::FuturesOrdered, StreamExt};

    pub(crate) async fn test_repo<T, F>(new_repo: F)
    where
        T: RepoCollection + Send + Sync,
        F: Fn() -> T + Send + Sync,
    {
        test_kafka_topic(&new_repo()).await;
        test_query_pool(&new_repo()).await;
        test_namespace(&new_repo()).await;
        test_table(&new_repo()).await;
        test_column(&new_repo()).await;
        test_sequencer(&new_repo()).await;
        test_partition(&new_repo()).await;
        test_tombstone(&new_repo()).await;
        test_parquet_file(&new_repo()).await;
    }

    async fn test_kafka_topic<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka_repo = repo.kafka_topic();
        let k = kafka_repo.create_or_get("foo").await.unwrap();
        assert!(k.id > KafkaTopicId::new(0));
        assert_eq!(k.name, "foo");
        let k2 = kafka_repo.create_or_get("foo").await.unwrap();
        assert_eq!(k, k2);
        let k3 = kafka_repo.get_by_name("foo").await.unwrap().unwrap();
        assert_eq!(k3, k);
        let k3 = kafka_repo.get_by_name("asdf").await.unwrap();
        assert!(k3.is_none());
    }

    async fn test_query_pool<T: RepoCollection + Send + Sync>(repo: &T) {
        let query_repo = repo.query_pool();
        let q = query_repo.create_or_get("foo").await.unwrap();
        assert!(q.id > QueryPoolId::new(0));
        assert_eq!(q.name, "foo");
        let q2 = query_repo.create_or_get("foo").await.unwrap();
        assert_eq!(q, q2);
    }

    async fn test_namespace<T: RepoCollection + Send + Sync>(repo: &T) {
        let namespace_repo = repo.namespace();
        let kafka = repo.kafka_topic().create_or_get("foo").await.unwrap();
        let pool = repo.query_pool().create_or_get("foo").await.unwrap();

        let namespace_name = "test_namespace";
        let namespace = namespace_repo
            .create(namespace_name, "inf", kafka.id, pool.id)
            .await
            .unwrap();
        assert!(namespace.id > NamespaceId::new(0));
        assert_eq!(namespace.name, namespace_name);

        let conflict = namespace_repo
            .create(namespace_name, "inf", kafka.id, pool.id)
            .await;
        assert!(matches!(
            conflict.unwrap_err(),
            Error::NameExists { name: _ }
        ));

        let found = namespace_repo
            .get_by_name(namespace_name)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);
    }

    async fn test_table<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka = repo.kafka_topic().create_or_get("foo").await.unwrap();
        let pool = repo.query_pool().create_or_get("foo").await.unwrap();
        let namespace = repo
            .namespace()
            .create("namespace_table_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();

        // test we can create or get a table
        let table_repo = repo.table();
        let t = table_repo
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let tt = table_repo
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        assert!(t.id > TableId::new(0));
        assert_eq!(t, tt);

        let tables = table_repo.list_by_namespace_id(namespace.id).await.unwrap();
        assert_eq!(vec![t], tables);
    }

    async fn test_column<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka = repo.kafka_topic().create_or_get("foo").await.unwrap();
        let pool = repo.query_pool().create_or_get("foo").await.unwrap();
        let namespace = repo
            .namespace()
            .create("namespace_column_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repo
            .table()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();

        // test we can create or get a column
        let column_repo = repo.column();
        let c = column_repo
            .create_or_get("column_test", table.id, ColumnType::Tag)
            .await
            .unwrap();
        let cc = column_repo
            .create_or_get("column_test", table.id, ColumnType::Tag)
            .await
            .unwrap();
        assert!(c.id > ColumnId::new(0));
        assert_eq!(c, cc);

        // test that attempting to create an already defined column of a different type returns error
        let err = column_repo
            .create_or_get("column_test", table.id, ColumnType::U64)
            .await
            .expect_err("should error with wrong column type");
        assert!(matches!(
            err,
            Error::ColumnTypeMismatch {
                name: _,
                existing: _,
                new: _
            }
        ));

        // test that we can create a column of the same name under a different table
        let table2 = repo
            .table()
            .create_or_get("test_table_2", namespace.id)
            .await
            .unwrap();
        let ccc = column_repo
            .create_or_get("column_test", table2.id, ColumnType::U64)
            .await
            .unwrap();
        assert_ne!(c, ccc);

        let columns = column_repo
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();
        assert_eq!(vec![c, ccc], columns);
    }

    async fn test_sequencer<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka = repo
            .kafka_topic()
            .create_or_get("sequencer_test")
            .await
            .unwrap();
        let sequencer_repo = repo.sequencer();

        // Create 10 sequencers
        let created = (1..=10)
            .map(|partition| sequencer_repo.create_or_get(&kafka, KafkaPartition::new(partition)))
            .collect::<FuturesOrdered<_>>()
            .map(|v| {
                let v = v.expect("failed to create sequencer");
                (v.id, v)
            })
            .collect::<BTreeMap<_, _>>()
            .await;

        // List them and assert they match
        let listed = sequencer_repo
            .list_by_kafka_topic(&kafka)
            .await
            .expect("failed to list sequencers")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);

        // get by the sequencer id and partition
        let kafka_partition = KafkaPartition::new(1);
        let sequencer = sequencer_repo
            .get_by_topic_id_and_partition(kafka.id, kafka_partition)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kafka.id, sequencer.kafka_topic_id);
        assert_eq!(kafka_partition, sequencer.kafka_partition);

        let sequencer = sequencer_repo
            .get_by_topic_id_and_partition(kafka.id, KafkaPartition::new(523))
            .await
            .unwrap();
        assert!(sequencer.is_none());
    }

    async fn test_partition<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka = repo.kafka_topic().create_or_get("foo").await.unwrap();
        let pool = repo.query_pool().create_or_get("foo").await.unwrap();
        let namespace = repo
            .namespace()
            .create("namespace_partition_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repo
            .table()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = repo
            .sequencer()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let other_sequencer = repo
            .sequencer()
            .create_or_get(&kafka, KafkaPartition::new(2))
            .await
            .unwrap();

        let partition_repo = repo.partition();

        let created = ["foo", "bar"]
            .iter()
            .map(|key| partition_repo.create_or_get(key, sequencer.id, table.id))
            .collect::<FuturesOrdered<_>>()
            .map(|v| {
                let v = v.expect("failed to create partition");
                (v.id, v)
            })
            .collect::<BTreeMap<_, _>>()
            .await;
        let _ = partition_repo
            .create_or_get("asdf", other_sequencer.id, table.id)
            .await
            .unwrap();

        // List them and assert they match
        let listed = partition_repo
            .list_by_sequencer(sequencer.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);
    }

    async fn test_tombstone<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka = repo.kafka_topic().create_or_get("foo").await.unwrap();
        let pool = repo.query_pool().create_or_get("foo").await.unwrap();
        let namespace = repo
            .namespace()
            .create("namespace_tombstone_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repo
            .table()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let other_table = repo
            .table()
            .create_or_get("other", namespace.id)
            .await
            .unwrap();
        let sequencer = repo
            .sequencer()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();

        let tombstone_repo = repo.tombstone();
        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);
        let t1 = tombstone_repo
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(1),
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();
        assert!(t1.id > TombstoneId::new(0));
        assert_eq!(t1.sequencer_id, sequencer.id);
        assert_eq!(t1.sequence_number, SequenceNumber::new(1));
        assert_eq!(t1.min_time, min_time);
        assert_eq!(t1.max_time, max_time);
        assert_eq!(t1.serialized_predicate, "whatevs");
        let t2 = tombstone_repo
            .create_or_get(
                other_table.id,
                sequencer.id,
                SequenceNumber::new(2),
                min_time,
                max_time,
                "bleh",
            )
            .await
            .unwrap();
        let t3 = tombstone_repo
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(3),
                min_time,
                max_time,
                "sdf",
            )
            .await
            .unwrap();

        let listed = tombstone_repo
            .list_tombstones_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![t2, t3], listed);
    }

    async fn test_parquet_file<T: RepoCollection + Send + Sync>(repo: &T) {
        let kafka = repo.kafka_topic().create_or_get("foo").await.unwrap();
        let pool = repo.query_pool().create_or_get("foo").await.unwrap();
        let namespace = repo
            .namespace()
            .create("namespace_parquet_file_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repo
            .table()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let other_table = repo
            .table()
            .create_or_get("other", namespace.id)
            .await
            .unwrap();
        let sequencer = repo
            .sequencer()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let partition = repo
            .partition()
            .create_or_get("one", sequencer.id, table.id)
            .await
            .unwrap();
        let other_partition = repo
            .partition()
            .create_or_get("one", sequencer.id, other_table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_repo = repo.parquet_file();
        let parquet_file = parquet_repo
            .create(
                sequencer.id,
                partition.table_id,
                partition.id,
                Uuid::new_v4(),
                SequenceNumber::new(10),
                SequenceNumber::new(140),
                min_time,
                max_time,
            )
            .await
            .unwrap();

        // verify that trying to create a file with the same UUID throws an error
        let err = parquet_repo
            .create(
                sequencer.id,
                partition.table_id,
                partition.id,
                parquet_file.object_store_id,
                SequenceNumber::new(10),
                SequenceNumber::new(140),
                min_time,
                max_time,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::FileExists { object_store_id: _ }));

        let other_file = parquet_repo
            .create(
                sequencer.id,
                other_partition.table_id,
                other_partition.id,
                Uuid::new_v4(),
                SequenceNumber::new(45),
                SequenceNumber::new(200),
                min_time,
                max_time,
            )
            .await
            .unwrap();

        let files = parquet_repo
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![parquet_file, other_file], files);
        let files = parquet_repo
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(150))
            .await
            .unwrap();
        assert_eq!(vec![other_file], files);

        // verify that to_delete is initially set to false and that it can be updated to true
        assert!(!parquet_file.to_delete);
        parquet_repo.flag_for_delete(parquet_file.id).await.unwrap();
        let files = parquet_repo
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert!(files.first().unwrap().to_delete);
    }
}
