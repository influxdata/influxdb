//! This module contains the traits and data objects for the Catalog API.

use async_trait::async_trait;
use influxdb_line_protocol::FieldValue;
use schema::{InfluxColumnType, InfluxFieldType};
use snafu::{OptionExt, Snafu};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::{collections::BTreeMap, fmt::Debug};
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

    #[snafu(display("parquet file with id {} does not exist. Foreign key violation", id))]
    FileNotFound { id: i64 },

    #[snafu(display("tombstone with id {} does not exist. Foreign key violation", id))]
    TombstoneNotFound { id: i64 },

    #[snafu(display("parquet_file record {} not found", id))]
    ParquetRecordNotFound { id: ParquetFileId },

    #[snafu(display("cannot derive valid column schema from column {}: {}", name, source))]
    InvalidColumn {
        source: Box<dyn std::error::Error + Send + Sync>,
        name: String,
    },

    #[snafu(display("Cannot start a transaction: {}", source))]
    StartTransaction { source: sqlx::Error },

    #[snafu(display("No transaction provided"))]
    NoTransaction,

    #[snafu(display(
        "the tombstone {} already processed for parquet file {}",
        tombstone_id,
        parquet_file_id
    ))]
    ProcessTombstoneExists {
        tombstone_id: i64,
        parquet_file_id: i64,
    },

    #[snafu(display("Error while converting usize {} to i64", value))]
    InvalidValue { value: usize },
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

impl std::fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

/// Trait that contains methods for working with the catalog
#[async_trait]
pub trait Catalog: Send + Sync + Debug {
    /// Setup catalog for usage and apply possible migrations.
    async fn setup(&self) -> Result<(), Error>;

    /// Create a new transaction.
    ///
    /// Creating transactions is potentially expensive. Holding one consumes resources. The number of parallel active
    /// transactions might be limited per catalog, so you MUST NOT rely on the ability to create multiple transactions in
    /// parallel for correctness but only for scaling.
    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error>;

    /// Access the repositories w/o a transaction scope.
    async fn repositories(&self) -> Box<dyn RepoCollection>;
}

/// Secret module for [sealed traits].
///
/// [sealed traits]: https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
pub(crate) mod sealed {
    use super::*;

    /// Helper trait to implement commit and abort of an transaction.
    ///
    /// The problem is that both methods cannot take `self` directly, otherwise the [`Transaction`] would not be object
    /// safe. Therefore we can only take a reference. To avoid that a user uses a transaction after calling one of the
    /// finalizers, we use a tiny trick and take `Box<dyn Transaction>` in our public interface and use a sealed trait
    /// for the actual implementation.
    #[async_trait]
    pub trait TransactionFinalize: Send + Sync + Debug {
        async fn commit_inplace(&mut self) -> Result<(), Error>;
        async fn abort_inplace(&mut self) -> Result<(), Error>;
    }
}

/// transaction of a [`Catalog`].
///
/// A transaction provides a consistent view on data and stages writes (this normally maps to a database transaction).
/// Repositories can cheaply be borrowed from it. To finalize a transaction, call [commit](Self::commit) or [abort](Self::abort).
///
/// Note that after any method in this transaction (including all repositories derived from it) returned an error, the
/// transaction MIGHT be poisoned and will return errors for all operations, depending on the backend.
///
///
/// # Drop
/// Dropping a transaction without calling [`commit`](Self::commit) or [`abort`](Self::abort) will abort the
/// transaction. However resources might not be released immediately, so it is adviced to always call
/// [`abort`](Self::abort) when you want to enforce that. Dropping w/o commiting/aborting will also log a warning.
#[async_trait]
pub trait Transaction: Send + Sync + Debug + sealed::TransactionFinalize + RepoCollection {
    /// Commit transaction.
    ///
    /// # Error Handling
    /// If successfull, all changes will be visible to other transactions.
    ///
    /// If an error is returned, the transaction may or or not be committed. This might be due to IO errors after the
    /// transaction was finished. However in either case, the transaction is atomic and can only succeed or fail
    /// entirely.
    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.commit_inplace().await
    }

    /// Abort transaction, throwing away all changes.
    async fn abort(mut self: Box<Self>) -> Result<(), Error> {
        self.abort_inplace().await
    }
}

impl<T> Transaction for T where T: Send + Sync + Debug + sealed::TransactionFinalize + RepoCollection
{}

/// Collection of the different repositories that the catalog offers.
///
/// The methods (e.g. "get or create") for handling entities (e.g. namespaces, tombstones, ...) are grouped into
/// *repositories* with one *repository* per entity. A repository can be thought of a collection of a single entity.
/// Getting repositories from the transaction is cheap.
///
/// Note that a repository might internally map to a wide range of different storage abstractions, ranging from one or
/// more SQL tables over key-value key spaces to simple in-memory vectors. The user should and must not care how these
/// are implemented.
#[async_trait]
pub trait RepoCollection: Send + Sync + Debug {
    /// repo for kafka topics
    fn kafka_topics(&mut self) -> &mut dyn KafkaTopicRepo;

    /// repo fo rquery pools
    fn query_pools(&mut self) -> &mut dyn QueryPoolRepo;

    /// repo for namespaces
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo;

    /// repo for tables
    fn tables(&mut self) -> &mut dyn TableRepo;

    /// repo for columns
    fn columns(&mut self) -> &mut dyn ColumnRepo;

    /// repo for sequencers
    fn sequencers(&mut self) -> &mut dyn SequencerRepo;

    /// repo for partitions
    fn partitions(&mut self) -> &mut dyn PartitionRepo;

    /// repo for tombstones
    fn tombstones(&mut self) -> &mut dyn TombstoneRepo;

    /// repo for parquet_files
    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo;

    /// repo for processed_tombstones
    fn processed_tombstones(&mut self) -> &mut dyn ProcessedTombstoneRepo;
}

/// Functions for working with Kafka topics in the catalog.
#[async_trait]
pub trait KafkaTopicRepo: Send + Sync {
    /// Creates the kafka topic in the catalog or gets the existing record by name.
    async fn create_or_get(&mut self, name: &str) -> Result<KafkaTopic>;

    /// Gets the kafka topic by its unique name
    async fn get_by_name(&mut self, name: &str) -> Result<Option<KafkaTopic>>;
}

/// Functions for working with query pools in the catalog.
#[async_trait]
pub trait QueryPoolRepo: Send + Sync {
    /// Creates the query pool in the catalog or gets the existing record by name.
    async fn create_or_get(&mut self, name: &str) -> Result<QueryPool>;
}

/// Functions for working with namespaces in the catalog
#[async_trait]
pub trait NamespaceRepo: Send + Sync {
    /// Creates the namespace in the catalog. If one by the same name already exists, an
    /// error is returned.
    async fn create(
        &mut self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: KafkaTopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace>;

    /// Gets the namespace by its unique name.
    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>>;
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo: Send + Sync {
    /// Creates the table in the catalog or get the existing record by name.
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table>;

    /// Lists all tables in the catalog for the given namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;
}

/// Functions for working with columns in the catalog
#[async_trait]
pub trait ColumnRepo: Send + Sync {
    /// Creates the column in the catalog or returns the existing column. Will return a
    /// `Error::ColumnTypeMismatch` if the existing column type doesn't match the type
    /// the caller is attempting to create.
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;
}

/// Functions for working with sequencers in the catalog
#[async_trait]
pub trait SequencerRepo: Send + Sync {
    /// create a sequencer record for the kafka topic and partition or return the existing record
    async fn create_or_get(
        &mut self,
        topic: &KafkaTopic,
        partition: KafkaPartition,
    ) -> Result<Sequencer>;

    /// get the sequencer record by `KafkaTopicId` and `KafkaPartition`
    async fn get_by_topic_id_and_partition(
        &mut self,
        topic_id: KafkaTopicId,
        partition: KafkaPartition,
    ) -> Result<Option<Sequencer>>;

    /// list all sequencers
    async fn list(&mut self) -> Result<Vec<Sequencer>>;

    /// list all sequencers for a given kafka topic
    async fn list_by_kafka_topic(&mut self, topic: &KafkaTopic) -> Result<Vec<Sequencer>>;
}

/// Functions for working with IOx partitions in the catalog. Note that these are how
/// IOx splits up data within a database, which is differenet than Kafka partitions.
#[async_trait]
pub trait PartitionRepo: Send + Sync {
    /// create or get a partition record for the given partition key, sequencer and table
    async fn create_or_get(
        &mut self,
        key: &str,
        sequencer_id: SequencerId,
        table_id: TableId,
    ) -> Result<Partition>;

    /// return partitions for a given sequencer
    async fn list_by_sequencer(&mut self, sequencer_id: SequencerId) -> Result<Vec<Partition>>;

    /// return the partition record, the namespace name it belongs to, and the table name it is under
    async fn partition_info_by_id(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionInfo>>;
}

/// Information for a partition from the catalog.
#[derive(Debug)]
#[allow(missing_docs)]
pub struct PartitionInfo {
    pub partition: Partition,
    pub namespace_name: String,
    pub table_name: String,
}

/// Functions for working with tombstones in the catalog
#[async_trait]
pub trait TombstoneRepo: Send + Sync {
    /// create or get a tombstone
    async fn create_or_get(
        &mut self,
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
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>>;
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// create the parquet file
    #[allow(clippy::too_many_arguments)]
    async fn create(
        &mut self,
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
    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()>;

    /// Get all parquet files for a sequencer with a max_sequence_number greater than the
    /// one passed in. The ingester will use this on startup to see which files were persisted
    /// that are greater than its min_unpersisted_number so that it can discard any data in
    /// these partitions on replay.
    async fn list_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>>;

    /// Verify if the parquet file exists by selecting its id
    async fn exist(&mut self, id: ParquetFileId) -> Result<bool>;

    /// Return count
    async fn count(&mut self) -> Result<i64>;
}

/// Functions for working with processed tombstone pointers in the catalog
#[async_trait]
pub trait ProcessedTombstoneRepo: Send + Sync {
    /// create a processed tombstone
    async fn create(
        &mut self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<ProcessedTombstone>;

    /// Verify if a processed tombstone exists in the catalog
    async fn exist(
        &mut self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<bool>;

    /// Return count
    async fn count(&mut self) -> Result<i64>;
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
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_name<R>(name: &str, repos: &mut R) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .get_by_name(name)
        .await?
        .context(NamespaceNotFoundSnafu { name })?;

    // get the columns first just in case someone else is creating schema while we're doing this.
    let columns = repos.columns().list_by_namespace_id(namespace.id).await?;
    let tables = repos.tables().list_by_namespace_id(namespace.id).await?;

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

    Ok(namespace)
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
    pub fn matches_type(&self, mb_column: &mutable_batch::column::Column) -> bool {
        self.column_type == mb_column.influx_type()
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

/// Data for a processed tombstone reference in the catalog.
#[derive(Debug, Copy, Clone, PartialEq, sqlx::FromRow)]
pub struct ProcessedTombstone {
    /// the id of the tombstone applied to the parquet file
    pub tombstone_id: TombstoneId,
    /// the id of the parquet file the tombstone was applied
    pub parquet_file_id: ParquetFileId,
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use ::test_helpers::{assert_contains, tracing::TracingCapture};

    use crate::add_parquet_file_with_tombstones;

    use super::*;
    use std::{ops::DerefMut, sync::Arc, time::Duration};

    pub(crate) async fn test_catalog(catalog: Arc<dyn Catalog>) {
        test_setup(Arc::clone(&catalog)).await;
        test_kafka_topic(Arc::clone(&catalog)).await;
        test_query_pool(Arc::clone(&catalog)).await;
        test_namespace(Arc::clone(&catalog)).await;
        test_table(Arc::clone(&catalog)).await;
        test_column(Arc::clone(&catalog)).await;
        test_sequencer(Arc::clone(&catalog)).await;
        test_partition(Arc::clone(&catalog)).await;
        test_tombstone(Arc::clone(&catalog)).await;
        test_parquet_file(Arc::clone(&catalog)).await;
        test_add_parquet_file_with_tombstones(Arc::clone(&catalog)).await;
        test_txn_isolation(Arc::clone(&catalog)).await;
        test_txn_drop(Arc::clone(&catalog)).await;
    }

    async fn test_setup(catalog: Arc<dyn Catalog>) {
        catalog.setup().await.expect("first catalog setup");
        catalog.setup().await.expect("second catalog setup");
    }

    async fn test_kafka_topic(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka_repo = repos.kafka_topics();

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

    async fn test_query_pool(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let query_repo = repos.query_pools();

        let q = query_repo.create_or_get("foo").await.unwrap();
        assert!(q.id > QueryPoolId::new(0));
        assert_eq!(q.name, "foo");
        let q2 = query_repo.create_or_get("foo").await.unwrap();
        assert_eq!(q, q2);
    }

    async fn test_namespace(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();

        let namespace_name = "test_namespace";
        let namespace = repos
            .namespaces()
            .create(namespace_name, "inf", kafka.id, pool.id)
            .await
            .unwrap();
        assert!(namespace.id > NamespaceId::new(0));
        assert_eq!(namespace.name, namespace_name);

        let conflict = repos
            .namespaces()
            .create(namespace_name, "inf", kafka.id, pool.id)
            .await;
        assert!(matches!(
            conflict.unwrap_err(),
            Error::NameExists { name: _ }
        ));

        let found = repos
            .namespaces()
            .get_by_name(namespace_name)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);
    }

    async fn test_table(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_table_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();

        // test we can create or get a table
        let t = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let tt = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        assert!(t.id > TableId::new(0));
        assert_eq!(t, tt);

        let tables = repos
            .tables()
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();
        assert_eq!(vec![t], tables);

        // test we can create a table of the same name in a different namespace
        let namespace2 = repos
            .namespaces()
            .create("two", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        assert_ne!(namespace, namespace2);
        let test_table = repos
            .tables()
            .create_or_get("test_table", namespace2.id)
            .await
            .unwrap();
        assert_ne!(tt, test_table);
        assert_eq!(test_table.namespace_id, namespace2.id);
    }

    async fn test_column(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_column_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        assert_eq!(table.namespace_id, namespace.id);

        // test we can create or get a column
        let c = repos
            .columns()
            .create_or_get("column_test", table.id, ColumnType::Tag)
            .await
            .unwrap();
        let cc = repos
            .columns()
            .create_or_get("column_test", table.id, ColumnType::Tag)
            .await
            .unwrap();
        assert!(c.id > ColumnId::new(0));
        assert_eq!(c, cc);

        // test that attempting to create an already defined column of a different type returns error
        let err = repos
            .columns()
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
        let table2 = repos
            .tables()
            .create_or_get("test_table_2", namespace.id)
            .await
            .unwrap();
        let ccc = repos
            .columns()
            .create_or_get("column_test", table2.id, ColumnType::U64)
            .await
            .unwrap();
        assert_ne!(c, ccc);

        let columns = repos
            .columns()
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();
        assert_eq!(vec![c, ccc], columns);
    }

    async fn test_sequencer(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos
            .kafka_topics()
            .create_or_get("sequencer_test")
            .await
            .unwrap();

        // Create 10 sequencers
        let mut created = BTreeMap::new();
        for partition in 1..=10 {
            let sequencer = repos
                .sequencers()
                .create_or_get(&kafka, KafkaPartition::new(partition))
                .await
                .expect("failed to create sequencer");
            created.insert(sequencer.id, sequencer);
        }

        // List them and assert they match
        let listed = repos
            .sequencers()
            .list_by_kafka_topic(&kafka)
            .await
            .expect("failed to list sequencers")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);

        // get by the sequencer id and partition
        let kafka_partition = KafkaPartition::new(1);
        let sequencer = repos
            .sequencers()
            .get_by_topic_id_and_partition(kafka.id, kafka_partition)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kafka.id, sequencer.kafka_topic_id);
        assert_eq!(kafka_partition, sequencer.kafka_partition);

        let sequencer = repos
            .sequencers()
            .get_by_topic_id_and_partition(kafka.id, KafkaPartition::new(523))
            .await
            .unwrap();
        assert!(sequencer.is_none());
    }

    async fn test_partition(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_partition_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let other_sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(2))
            .await
            .unwrap();

        let mut created = BTreeMap::new();
        for key in ["foo", "bar"] {
            let partition = repos
                .partitions()
                .create_or_get(key, sequencer.id, table.id)
                .await
                .expect("failed to create partition");
            created.insert(partition.id, partition);
        }
        let other_partition = repos
            .partitions()
            .create_or_get("asdf", other_sequencer.id, table.id)
            .await
            .unwrap();

        // List them and assert they match
        let listed = repos
            .partitions()
            .list_by_sequencer(sequencer.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);

        // test get_partition_info_by_id
        let info = repos
            .partitions()
            .partition_info_by_id(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(info.partition, other_partition);
        assert_eq!(info.table_name, "test_table");
        assert_eq!(info.namespace_name, "namespace_partition_test");
    }

    async fn test_tombstone(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_tombstone_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let other_table = repos
            .tables()
            .create_or_get("other", namespace.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);
        let t1 = repos
            .tombstones()
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
        let t2 = repos
            .tombstones()
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
        let t3 = repos
            .tombstones()
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

        let listed = repos
            .tombstones()
            .list_tombstones_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![t2, t3], listed);
    }

    async fn test_parquet_file(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_parquet_file_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let other_table = repos
            .tables()
            .create_or_get("other", namespace.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one", sequencer.id, table.id)
            .await
            .unwrap();
        let other_partition = repos
            .partitions()
            .create_or_get("one", sequencer.id, other_table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        // Must have no rows
        let row_count = repos.parquet_files().count().await.unwrap();
        assert_eq!(row_count, 0);

        let parquet_file = repos
            .parquet_files()
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
        let err = repos
            .parquet_files()
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

        let other_file = repos
            .parquet_files()
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

        // Must have 2 rows
        let row_count = repos.parquet_files().count().await.unwrap();
        assert_eq!(row_count, 2);

        let exist_id = parquet_file.id;
        let non_exist_id = ParquetFileId::new(other_file.id.get() + 10);
        // make sure exists_id != non_exist_id
        assert_ne!(exist_id, non_exist_id);
        assert!(repos.parquet_files().exist(exist_id).await.unwrap());
        assert!(!repos.parquet_files().exist(non_exist_id).await.unwrap());

        let files = repos
            .parquet_files()
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![parquet_file, other_file], files);
        let files = repos
            .parquet_files()
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(150))
            .await
            .unwrap();
        assert_eq!(vec![other_file], files);

        // verify that to_delete is initially set to false and that it can be updated to true
        assert!(!parquet_file.to_delete);
        repos
            .parquet_files()
            .flag_for_delete(parquet_file.id)
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert!(files.first().unwrap().to_delete);
    }

    async fn test_add_parquet_file_with_tombstones(catalog: Arc<dyn Catalog>) {
        let mut txn = catalog.start_transaction().await.unwrap();
        let kafka = txn.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = txn.query_pools().create_or_get("foo").await.unwrap();
        let namespace = txn
            .namespaces()
            .create(
                "namespace_parquet_file_with_tombstones_test",
                "inf",
                kafka.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = txn
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let partition = txn
            .partitions()
            .create_or_get("one", sequencer.id, table.id)
            .await
            .unwrap();

        // Add tombstones
        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);
        let t1 = txn
            .tombstones()
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
        let t2 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(2),
                min_time,
                max_time,
                "bleh",
            )
            .await
            .unwrap();
        let t3 = txn
            .tombstones()
            .create_or_get(
                table.id,
                sequencer.id,
                SequenceNumber::new(3),
                min_time,
                max_time,
                "meh",
            )
            .await
            .unwrap();

        // Prepare metadata in form of ParquetFile to get added with tombstone
        let parquet = ParquetFile {
            id: ParquetFileId::new(0), //fake id that will never be used
            sequencer_id: sequencer.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(10),
            min_time,
            max_time,
            to_delete: false,
        };
        let other_parquet = ParquetFile {
            id: ParquetFileId::new(0), //fake id that will never be used
            sequencer_id: sequencer.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(11),
            max_sequence_number: SequenceNumber::new(20),
            min_time,
            max_time,
            to_delete: false,
        };
        let another_parquet = ParquetFile {
            id: ParquetFileId::new(0), //fake id that will never be used
            sequencer_id: sequencer.id,
            table_id: table.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(21),
            max_sequence_number: SequenceNumber::new(30),
            min_time,
            max_time,
            to_delete: false,
        };

        let parquet_file_count_before = txn.parquet_files().count().await.unwrap();
        let pt_count_before = txn.processed_tombstones().count().await.unwrap();
        txn.commit().await.unwrap();

        // Add parquet and processed tombstone in one transaction
        let mut txn = catalog.start_transaction().await.unwrap();
        let (parquet_file, p_tombstones) =
            add_parquet_file_with_tombstones(&parquet, &[t1.clone(), t2.clone()], txn.deref_mut())
                .await
                .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(p_tombstones.len(), 2);
        assert_eq!(t1.id, p_tombstones[0].tombstone_id);
        assert_eq!(t2.id, p_tombstones[1].tombstone_id);

        // verify the catalog
        let mut txn = catalog.start_transaction().await.unwrap();
        let parquet_file_count_after = txn.parquet_files().count().await.unwrap();
        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        assert_eq!(pt_count_after - pt_count_before, 2);
        assert_eq!(parquet_file_count_after - parquet_file_count_before, 1);
        let pt_count_before = pt_count_after;
        let parquet_file_count_before = parquet_file_count_after;

        assert!(txn.parquet_files().exist(parquet_file.id).await.unwrap());
        assert!(txn
            .processed_tombstones()
            .exist(parquet_file.id, t1.id)
            .await
            .unwrap());
        assert!(txn
            .processed_tombstones()
            .exist(parquet_file.id, t1.id)
            .await
            .unwrap());
        txn.commit().await.unwrap();

        // Error due to duplicate parquet file
        let mut txn = catalog.start_transaction().await.unwrap();
        add_parquet_file_with_tombstones(&parquet, &[t3.clone(), t1.clone()], txn.deref_mut())
            .await
            .unwrap_err();
        txn.abort().await.unwrap();

        // Since the transaction is rollback, t3 is not yet added
        let mut txn = catalog.start_transaction().await.unwrap();
        assert!(!txn
            .processed_tombstones()
            .exist(parquet_file.id, t3.id)
            .await
            .unwrap());

        // Add new parquet and new tombstone. Should go trhough
        let (parquet_file, p_tombstones) =
            add_parquet_file_with_tombstones(&other_parquet, &[t3.clone()], txn.deref_mut())
                .await
                .unwrap();
        assert_eq!(p_tombstones.len(), 1);
        assert_eq!(t3.id, p_tombstones[0].tombstone_id);
        assert!(txn
            .processed_tombstones()
            .exist(parquet_file.id, t3.id)
            .await
            .unwrap());
        assert!(txn.parquet_files().exist(parquet_file.id).await.unwrap());

        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        let parquet_file_count_after = txn.parquet_files().count().await.unwrap();
        assert_eq!(pt_count_after - pt_count_before, 1);
        assert_eq!(parquet_file_count_after - parquet_file_count_before, 1);
        let pt_count_before = pt_count_after;
        let parquet_file_count_before = parquet_file_count_after;
        txn.commit().await.unwrap();

        // Add non-exist tombstone t4 and should fail
        let mut txn = catalog.start_transaction().await.unwrap();
        let mut t4 = t3.clone();
        t4.id = TombstoneId::new(t4.id.get() + 10);
        add_parquet_file_with_tombstones(&another_parquet, &[t4], txn.deref_mut())
            .await
            .unwrap_err();
        txn.abort().await.unwrap();

        // Still same count as before
        let mut txn = catalog.start_transaction().await.unwrap();
        let pt_count_after = txn.processed_tombstones().count().await.unwrap();
        let parquet_file_count_after = txn.parquet_files().count().await.unwrap();
        assert_eq!(pt_count_after - pt_count_before, 0);
        assert_eq!(parquet_file_count_after - parquet_file_count_before, 0);
        txn.commit().await.unwrap();
    }

    async fn test_txn_isolation(catalog: Arc<dyn Catalog>) {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let barrier_captured = Arc::clone(&barrier);
        let catalog_captured = Arc::clone(&catalog);
        let insertion_task = tokio::spawn(async move {
            barrier_captured.wait().await;

            let mut txn = catalog_captured.start_transaction().await.unwrap();
            txn.kafka_topics()
                .create_or_get("test_txn_isolation")
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(200)).await;
            txn.abort().await.unwrap();
        });

        let mut txn = catalog.start_transaction().await.unwrap();

        barrier.wait().await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let topic = txn
            .kafka_topics()
            .get_by_name("test_txn_isolation")
            .await
            .unwrap();
        assert!(topic.is_none());
        txn.abort().await.unwrap();

        insertion_task.await.unwrap();

        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn
            .kafka_topics()
            .get_by_name("test_txn_isolation")
            .await
            .unwrap();
        assert!(topic.is_none());
        txn.abort().await.unwrap();
    }

    async fn test_txn_drop(catalog: Arc<dyn Catalog>) {
        let capture = TracingCapture::new();
        let mut txn = catalog.start_transaction().await.unwrap();
        txn.kafka_topics()
            .create_or_get("test_txn_drop")
            .await
            .unwrap();
        drop(txn);

        // got a warning
        assert_contains!(capture.to_string(), "Dropping ");
        assert_contains!(capture.to_string(), " w/o finalizing (commit or abort)");

        // data is NOT committed
        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn
            .kafka_topics()
            .get_by_name("test_txn_drop")
            .await
            .unwrap();
        assert!(topic.is_none());
        txn.abort().await.unwrap();
    }
}
