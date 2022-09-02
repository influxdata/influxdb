//! Traits and data types for the IOx Catalog API.

use async_trait::async_trait;
use data_types::{
    Column, ColumnSchema, ColumnType, ColumnTypeCount, Namespace, NamespaceId, NamespaceSchema,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionInfo,
    PartitionKey, PartitionParam, ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber,
    Shard, ShardId, ShardIndex, Table, TableId, TablePartition, TableSchema, Timestamp, Tombstone,
    TombstoneId, TopicId, TopicMetadata,
};
use iox_time::TimeProvider;
use snafu::{OptionExt, Snafu};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::TryFrom,
    fmt::Debug,
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("name {} already exists", name))]
    NameExists { name: String },

    #[snafu(display("unhandled sqlx error: {}", source))]
    SqlxError { source: sqlx::Error },

    #[snafu(display("foreign key violation: {}", source))]
    ForeignKeyViolation { source: sqlx::Error },

    #[snafu(display("column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: String,
        new: String,
    },

    #[snafu(display(
        "column type {} is in the db for column {}, which is unknown",
        data_type,
        name
    ))]
    UnknownColumnType { data_type: i16, name: String },

    #[snafu(display("namespace {} not found", name))]
    NamespaceNotFoundByName { name: String },

    #[snafu(display("namespace {} not found", id))]
    NamespaceNotFoundById { id: NamespaceId },

    #[snafu(display("table {} not found", id))]
    TableNotFound { id: TableId },

    #[snafu(display("partition {} not found", id))]
    PartitionNotFound { id: PartitionId },

    #[snafu(display(
        "couldn't create column {} in table {}; limit reached on namespace",
        column_name,
        table_id,
    ))]
    ColumnCreateLimitError {
        column_name: String,
        table_id: TableId,
    },

    #[snafu(display(
        "couldn't create table {}; limit reached on namespace {}",
        table_name,
        namespace_id
    ))]
    TableCreateLimitError {
        table_name: String,
        namespace_id: NamespaceId,
    },

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

    #[snafu(display("cannot start a transaction: {}", source))]
    StartTransaction { source: sqlx::Error },

    #[snafu(display("no transaction provided"))]
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

    #[snafu(display("error while converting usize {} to i64", value))]
    InvalidValue { value: usize },

    #[snafu(display("database setup error: {}", source))]
    Setup { source: sqlx::Error },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Methods for working with the catalog.
#[async_trait]
pub trait Catalog: Send + Sync + Debug {
    /// Setup catalog for usage and apply possible migrations.
    async fn setup(&self) -> Result<(), Error>;

    /// Creates a new [`Transaction`].
    ///
    /// Creating transactions is potentially expensive. Holding one consumes resources. The number of parallel active
    /// transactions might be limited per catalog, so you MUST NOT rely on the ability to create multiple transactions in
    /// parallel for correctness. Parallel transactions must only be used for scaling.
    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error>;

    /// Accesses the repositories without a transaction scope.
    async fn repositories(&self) -> Box<dyn RepoCollection>;

    /// Gets metric registry associated with this catalog.
    fn metrics(&self) -> Arc<metric::Registry>;

    /// Gets the time provider associated with this catalog.
    fn time_provider(&self) -> Arc<dyn TimeProvider>;
}

/// Secret module for [sealed traits].
///
/// [sealed traits]: https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
#[doc(hidden)]
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

/// Transaction in a [`Catalog`] (similar to a database transaction).
///
/// A transaction provides a consistent view on data and stages writes.
/// To finalize a transaction, call [commit](Self::commit) or [abort](Self::abort).
///
/// Repositories can cheaply be borrowed from it.
///
/// Note that after any method in this transaction (including all repositories derived from it) returns an error, the
/// transaction MIGHT be poisoned and will return errors for all operations, depending on the backend.
///
///
/// # Drop
///
/// Dropping a transaction without calling [`commit`](Self::commit) or [`abort`](Self::abort) will abort the
/// transaction. However resources might not be released immediately, so it is adviced to always call
/// [`abort`](Self::abort) when you want to enforce that. Dropping w/o commiting/aborting will also log a warning.
#[async_trait]
pub trait Transaction: Send + Sync + Debug + sealed::TransactionFinalize + RepoCollection {
    /// Commits a transaction.
    ///
    /// # Error Handling
    ///
    /// If successfull, all changes will be visible to other transactions.
    ///
    /// If an error is returned, the transaction may or or not be committed. This might be due to IO errors after the
    /// transaction was finished. However in either case, the transaction is atomic and can only succeed or fail
    /// entirely.
    async fn commit(mut self: Box<Self>) -> Result<(), Error> {
        self.commit_inplace().await
    }

    /// Aborts the transaction, throwing away all changes.
    async fn abort(mut self: Box<Self>) -> Result<(), Error> {
        self.abort_inplace().await
    }
}

impl<T> Transaction for T where T: Send + Sync + Debug + sealed::TransactionFinalize + RepoCollection
{}

/// Methods for working with the catalog's various repositories (collections of entities).
///
/// # Repositories
///
/// The methods (e.g. `create_*` or `get_by_*`) for handling entities (namespaces, tombstones,
/// etc.) are grouped into *repositories* with one repository per entity. A repository can be
/// thought of a collection of a single kind of entity. Getting repositories from the transaction
/// is cheap.
///
/// A repository might internally map to a wide range of different storage abstractions, ranging
/// from one or more SQL tables over key-value key spaces to simple in-memory vectors. The user
/// should and must not care how these are implemented.
#[async_trait]
pub trait RepoCollection: Send + Sync + Debug {
    /// Repository for [topics](data_types::TopicMetadata).
    fn topics(&mut self) -> &mut dyn TopicMetadataRepo;

    /// Repository for [query pools](data_types::QueryPool).
    fn query_pools(&mut self) -> &mut dyn QueryPoolRepo;

    /// Repository for [namespaces](data_types::Namespace).
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo;

    /// Repository for [tables](data_types::Table).
    fn tables(&mut self) -> &mut dyn TableRepo;

    /// Repository for [columns](data_types::Column).
    fn columns(&mut self) -> &mut dyn ColumnRepo;

    /// Repository for [shards](data_types::Shard).
    fn shards(&mut self) -> &mut dyn ShardRepo;

    /// Repository for [partitions](data_types::Partition).
    fn partitions(&mut self) -> &mut dyn PartitionRepo;

    /// Repository for [tombstones](data_types::Tombstone).
    fn tombstones(&mut self) -> &mut dyn TombstoneRepo;

    /// Repository for [Parquet files](data_types::ParquetFile).
    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo;

    /// Repository for [processed tombstones](data_types::ProcessedTombstone).
    fn processed_tombstones(&mut self) -> &mut dyn ProcessedTombstoneRepo;
}

/// Functions for working with topics in the catalog.
#[async_trait]
pub trait TopicMetadataRepo: Send + Sync {
    /// Creates the topic in the catalog or gets the existing record by name.
    async fn create_or_get(&mut self, name: &str) -> Result<TopicMetadata>;

    /// Gets the topic by its unique name
    async fn get_by_name(&mut self, name: &str) -> Result<Option<TopicMetadata>>;
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
        topic_id: TopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace>;

    /// List all namespaces.
    async fn list(&mut self) -> Result<Vec<Namespace>>;

    /// Gets the namespace by its ID.
    async fn get_by_id(&mut self, id: NamespaceId) -> Result<Option<Namespace>>;

    /// Gets the namespace by its unique name.
    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>>;

    /// Update the limit on the number of tables that can exist per namespace.
    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;

    /// Update the limit on the number of columns that can exist per table in a given namespace.
    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo: Send + Sync {
    /// Creates the table in the catalog or get the existing record by name.
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table>;

    /// get table by ID
    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>>;

    /// get table by namespace ID and name
    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>>;

    /// Lists all tables in the catalog for the given namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>>;

    /// List all tables.
    async fn list(&mut self) -> Result<Vec<Table>>;

    /// Gets the table persistence info for the given shard
    async fn get_table_persist_info(
        &mut self,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_name: &str,
    ) -> Result<Option<TablePersistInfo>>;
}

/// Information for a table's persistence information for a specific shard from the catalog
#[derive(Debug, Copy, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct TablePersistInfo {
    /// shard the sequence numbers are associated with
    pub shard_id: ShardId,
    /// the global identifier for the table
    pub table_id: TableId,
    /// max sequence number from this table's tombstones for this shard
    pub tombstone_max_sequence_number: Option<SequenceNumber>,
}

/// Parameters necessary to perform a batch insert of
/// [`ColumnRepo::create_or_get()`].
#[derive(Debug)]
pub struct ColumnUpsertRequest<'a> {
    /// The name of the column.
    pub name: &'a str,
    /// The table ID to which it belongs.
    pub table_id: TableId,
    /// The data type of the column.
    pub column_type: ColumnType,
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

    /// Perform a bulk upsert of columns.
    ///
    /// Implementations make no guarantees as to the ordering or atomicity of
    /// the batch of column upsert operations - a batch upsert may partially
    /// commit, in which case an error MUST be returned by the implementation.
    async fn create_or_get_many(
        &mut self,
        columns: &[ColumnUpsertRequest<'_>],
    ) -> Result<Vec<Column>>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;

    /// List all columns for the given table ID.
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;

    /// List all columns.
    async fn list(&mut self) -> Result<Vec<Column>>;

    /// List column types and their count for a table
    async fn list_type_count_by_table_id(
        &mut self,
        table_id: TableId,
    ) -> Result<Vec<ColumnTypeCount>>;
}

/// Functions for working with shards in the catalog
#[async_trait]
pub trait ShardRepo: Send + Sync {
    /// create a shard record for the topic and shard index or return the existing record
    async fn create_or_get(
        &mut self,
        topic: &TopicMetadata,
        shard_index: ShardIndex,
    ) -> Result<Shard>;

    /// get the shard record by `TopicId` and `ShardIndex`
    async fn get_by_topic_id_and_shard_index(
        &mut self,
        topic_id: TopicId,
        shard_index: ShardIndex,
    ) -> Result<Option<Shard>>;

    /// list all shards
    async fn list(&mut self) -> Result<Vec<Shard>>;

    /// list all shards for a given topic
    async fn list_by_topic(&mut self, topic: &TopicMetadata) -> Result<Vec<Shard>>;

    /// updates the `min_unpersisted_sequence_number` for a shard
    async fn update_min_unpersisted_sequence_number(
        &mut self,
        shard: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<()>;
}

/// Functions for working with IOx partitions in the catalog. Note that these are how IOx splits up
/// data within a database, which is different than Kafka partitions.
#[async_trait]
pub trait PartitionRepo: Send + Sync {
    /// create or get a partition record for the given partition key, shard and table
    async fn create_or_get(
        &mut self,
        key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> Result<Partition>;

    /// get partition by ID
    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;

    /// return partitions for a given shard
    async fn list_by_shard(&mut self, shard_id: ShardId) -> Result<Vec<Partition>>;

    /// return partitions for a given namespace
    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>>;

    /// return the partitions by table id
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;

    /// return the partition record, the namespace name it belongs to, and the table name it is
    /// under
    async fn partition_info_by_id(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionInfo>>;

    /// Update the sort key for the partition
    async fn update_sort_key(
        &mut self,
        partition_id: PartitionId,
        sort_key: &[&str],
    ) -> Result<Partition>;
}

/// Functions for working with tombstones in the catalog
#[async_trait]
pub trait TombstoneRepo: Send + Sync {
    /// create or get a tombstone
    async fn create_or_get(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone>;

    /// list all tombstones for a given namespace
    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Tombstone>>;

    /// list all tombstones for a given table
    async fn list_by_table(&mut self, table_id: TableId) -> Result<Vec<Tombstone>>;

    /// get tombstones of the given id
    async fn get_by_id(&mut self, tombstone_id: TombstoneId) -> Result<Option<Tombstone>>;

    /// return all tombstones for the shard with a sequence number greater than that
    /// passed in. This will be used by the ingester on startup to see what tombstones
    /// might have to be applied to data that is read from the write buffer.
    async fn list_tombstones_by_shard_greater_than(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>>;

    /// Remove given tombstones
    async fn remove(&mut self, tombstone_ids: &[TombstoneId]) -> Result<()>;

    /// Return all tombstones that have:
    ///
    /// - the specified shard ID and table ID
    /// - a sequence number greater than the specified sequence number
    /// - a time period that overlaps with the specified time period
    ///
    /// Used during compaction.
    async fn list_tombstones_for_time_range(
        &mut self,
        shard_id: ShardId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<Tombstone>>;
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// create the parquet file
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;

    /// Flag the parquet file for deletion
    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()>;

    /// Get all parquet files for a shard with a max_sequence_number greater than the
    /// one passed in. The ingester will use this on startup to see which files were persisted
    /// that are greater than its min_unpersisted_number so that it can discard any data in
    /// these partitions on replay.
    async fn list_by_shard_greater_than(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>>;

    /// List all parquet files within a given namespace that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>>;

    /// List all parquet files within a given table that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;

    /// Delete all parquet files that were marked to be deleted earlier than the specified time.
    /// Returns the deleted records.
    async fn delete_old(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFile>>;

    /// List parquet files for a given shard with compaction level 0 and other criteria that
    /// define a file as a candidate for compaction
    async fn level_0(&mut self, shard_id: ShardId) -> Result<Vec<ParquetFile>>;

    /// List parquet files for a given table partition, in a given time range, with compaction
    /// level 1, and other criteria that define a file as a candidate for compaction with a level 0
    /// file
    async fn level_1(
        &mut self,
        table_partition: TablePartition,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<ParquetFile>>;

    /// List the most recent highest throughput partition for a given shard
    async fn recent_highest_throughput_partitions(
        &mut self,
        shard_id: ShardId,
        time_at_num_minutes_ago: Timestamp,
        min_num_files: usize,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>>;

    /// List partitions with the most level 0 files created earlier than `older_than_num_hours`
    /// hours ago for a given shard. In other words, "cold" partitions that need compaction.
    async fn most_level_0_files_partitions(
        &mut self,
        shard_id: ShardId,
        time_in_the_past: Timestamp,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>>;

    /// List parquet files for a given partition that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>>;

    /// Update the compaction level of the specified parquet files to
    /// `CompactionLevel::FileNonOverlapped`
    /// Returns the IDs of the files that were successfully updated.
    async fn update_to_level_1(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
    ) -> Result<Vec<ParquetFileId>>;

    /// Verify if the parquet file exists by selecting its id
    async fn exist(&mut self, id: ParquetFileId) -> Result<bool>;

    /// Return count
    async fn count(&mut self) -> Result<i64>;

    /// Return count of level-0 files of given tableId and shardId that
    /// overlap with the given min_time and max_time and have sequence number
    /// smaller the given one
    async fn count_by_overlaps_with_level_0(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        min_time: Timestamp,
        max_time: Timestamp,
        sequence_number: SequenceNumber,
    ) -> Result<i64>;

    /// Return count of level-1 files of given tableId and shardId that
    /// overlap with the given min_time and max_time
    async fn count_by_overlaps_with_level_1(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<i64>;

    /// Return the parquet file with the given object store id
    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>>;
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

    /// Return count for a given tombstone id
    async fn count_by_tombstone_id(&mut self, tombstone_id: TombstoneId) -> Result<i64>;
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_id<R>(id: NamespaceId, repos: &mut R) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .get_by_id(id)
        .await?
        .context(NamespaceNotFoundByIdSnafu { id })?;

    get_schema_internal(namespace, repos).await
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
        .context(NamespaceNotFoundByNameSnafu { name })?;

    get_schema_internal(namespace, repos).await
}

async fn get_schema_internal<R>(namespace: Namespace, repos: &mut R) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    // get the columns first just in case someone else is creating schema while we're doing this.
    let columns = repos.columns().list_by_namespace_id(namespace.id).await?;
    let tables = repos.tables().list_by_namespace_id(namespace.id).await?;

    let mut namespace =
        NamespaceSchema::new(namespace.id, namespace.topic_id, namespace.query_pool_id);

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

/// Gets the table schema including all columns.
pub async fn get_table_schema_by_id<R>(id: TableId, repos: &mut R) -> Result<TableSchema>
where
    R: RepoCollection + ?Sized,
{
    let columns = repos.columns().list_by_table_id(id).await?;
    let mut schema = TableSchema::new(id);

    for c in columns {
        match ColumnType::try_from(c.column_type) {
            Ok(column_type) => {
                schema.columns.insert(
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
                    name: c.name,
                });
            }
        }
    }

    Ok(schema)
}

/// Fetch all [`NamespaceSchema`] in the catalog.
///
/// This method performs the minimal number of queries needed to build the
/// result set. No table lock is obtained, nor are queries executed within a
/// transaction, but this method does return a point-in-time snapshot of the
/// catalog state.
pub async fn list_schemas(
    catalog: &dyn Catalog,
) -> Result<impl Iterator<Item = (Namespace, NamespaceSchema)>> {
    let mut repos = catalog.repositories().await;

    // In order to obtain a point-in-time snapshot, first fetch the columns,
    // then the tables, and then resolve the namespace IDs to Namespace in order
    // to construct the schemas.
    //
    // The set of columns returned forms the state snapshot, with the subsequent
    // queries resolving only what is needed to construct schemas for the
    // retrieved columns (ignoring any newly added tables/namespaces since the
    // column snapshot was taken).

    // First fetch all the columns - this is the state snapshot of the catalog
    // schemas.
    let columns = repos.columns().list().await?;

    // Construct the set of table IDs these columns belong to.
    let retain_table_ids = columns.iter().map(|c| c.table_id).collect::<HashSet<_>>();

    // Fetch all tables, and filter for those that are needed to construct
    // schemas for "columns" only.
    //
    // Discard any tables that have no columns or have been created since
    // the "columns" snapshot was retrieved, and construct a map of ID->Table.
    let tables = repos
        .tables()
        .list()
        .await?
        .into_iter()
        .filter_map(|t| {
            if !retain_table_ids.contains(&t.id) {
                return None;
            }

            Some((t.id, t))
        })
        .collect::<HashMap<_, _>>();

    // Drop the table ID set as it will not be referenced again.
    drop(retain_table_ids);

    // Do all the I/O to fetch the namespaces in the background, while this
    // thread constructs the NamespaceId->TableSchema map below.
    let namespaces = tokio::spawn(async move { repos.namespaces().list().await });

    // A set of tables within a single namespace.
    type NamespaceTables = BTreeMap<String, TableSchema>;

    let mut joined = HashMap::<NamespaceId, NamespaceTables>::default();
    for column in columns {
        // Resolve the table this column references
        let table = tables.get(&column.table_id).expect("no table for column");

        let table_schema = joined
            // Find or create a record in the joined <NamespaceId, Tables> map
            // for this namespace ID.
            .entry(table.namespace_id)
            .or_default()
            // Fetch the schema record for this table, or create an empty one.
            .entry(table.name.clone())
            .or_insert_with(|| TableSchema::new(column.table_id));

        table_schema.add_column(&column);
    }

    // The table map is no longer needed - immediately reclaim the memory.
    drop(tables);

    // Convert the Namespace instances into NamespaceSchema instances.
    let iter = namespaces
        .await
        .expect("namespace list task panicked")?
        .into_iter()
        // Ignore any namespaces that did not exist when the "columns" snapshot
        // was created, or have no tables/columns (and therefore have no entry
        // in "joined").
        .filter_map(move |v| {
            let mut ns = NamespaceSchema::new(v.id, v.topic_id, v.query_pool_id);
            ns.tables = joined.remove(&v.id)?;
            Some((v, ns))
        });

    Ok(iter)
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use crate::validate_or_insert_schema;

    use super::*;
    use ::test_helpers::{assert_contains, tracing::TracingCapture};
    use data_types::{ColumnId, ColumnSet, CompactionLevel};
    use metric::{Attributes, DurationHistogram, Metric};
    use std::{
        ops::{Add, DerefMut},
        sync::Arc,
        time::Duration,
    };

    pub(crate) async fn test_catalog(catalog: Arc<dyn Catalog>) {
        test_setup(Arc::clone(&catalog)).await;
        test_topic(Arc::clone(&catalog)).await;
        test_query_pool(Arc::clone(&catalog)).await;
        test_namespace(Arc::clone(&catalog)).await;
        test_table(Arc::clone(&catalog)).await;
        test_column(Arc::clone(&catalog)).await;
        test_shards(Arc::clone(&catalog)).await;
        test_partition(Arc::clone(&catalog)).await;
        test_tombstone(Arc::clone(&catalog)).await;
        test_tombstones_by_parquet_file(Arc::clone(&catalog)).await;
        test_parquet_file(Arc::clone(&catalog)).await;
        test_parquet_file_compaction_level_0(Arc::clone(&catalog)).await;
        test_parquet_file_compaction_level_1(Arc::clone(&catalog)).await;
        test_most_level_0_files_partitions(Arc::clone(&catalog)).await;
        test_recent_highest_throughput_partitions(Arc::clone(&catalog)).await;
        test_update_to_compaction_level_1(Arc::clone(&catalog)).await;
        test_processed_tombstones(Arc::clone(&catalog)).await;
        test_list_by_partiton_not_to_delete(Arc::clone(&catalog)).await;
        test_txn_isolation(Arc::clone(&catalog)).await;
        test_txn_drop(Arc::clone(&catalog)).await;
        test_list_schemas(Arc::clone(&catalog)).await;

        let metrics = catalog.metrics();
        assert_metric_hit(&*metrics, "topic_create_or_get");
        assert_metric_hit(&*metrics, "query_create_or_get");
        assert_metric_hit(&*metrics, "namespace_create");
        assert_metric_hit(&*metrics, "table_create_or_get");
        assert_metric_hit(&*metrics, "column_create_or_get");
        assert_metric_hit(&*metrics, "shard_create_or_get");
        assert_metric_hit(&*metrics, "partition_create_or_get");
        assert_metric_hit(&*metrics, "tombstone_create_or_get");
        assert_metric_hit(&*metrics, "parquet_create");
    }

    async fn test_setup(catalog: Arc<dyn Catalog>) {
        catalog.setup().await.expect("first catalog setup");
        catalog.setup().await.expect("second catalog setup");
    }

    async fn test_topic(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic_repo = repos.topics();

        let k = topic_repo.create_or_get("foo").await.unwrap();
        assert!(k.id > TopicId::new(0));
        assert_eq!(k.name, "foo");
        let k2 = topic_repo.create_or_get("foo").await.unwrap();
        assert_eq!(k, k2);
        let k3 = topic_repo.get_by_name("foo").await.unwrap().unwrap();
        assert_eq!(k3, k);
        let k3 = topic_repo.get_by_name("asdf").await.unwrap();
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
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();

        let namespace_name = "test_namespace";
        let namespace = repos
            .namespaces()
            .create(namespace_name, "inf", topic.id, pool.id)
            .await
            .unwrap();
        assert!(namespace.id > NamespaceId::new(0));
        assert_eq!(namespace.name, namespace_name);

        let conflict = repos
            .namespaces()
            .create(namespace_name, "inf", topic.id, pool.id)
            .await;
        assert!(matches!(
            conflict.unwrap_err(),
            Error::NameExists { name: _ }
        ));

        let found = repos
            .namespaces()
            .get_by_id(namespace.id)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_id(NamespaceId::new(i64::MAX))
            .await
            .unwrap();
        assert!(not_found.is_none());

        let found = repos
            .namespaces()
            .get_by_name(namespace_name)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_name("does_not_exist")
            .await
            .unwrap();
        assert!(not_found.is_none());

        let namespace2_name = "test_namespace2";
        let namespace2 = repos
            .namespaces()
            .create(namespace2_name, "inf", topic.id, pool.id)
            .await
            .unwrap();
        let mut namespaces = repos.namespaces().list().await.unwrap();
        namespaces.sort_by_key(|ns| ns.name.clone());
        assert_eq!(namespaces, vec![namespace, namespace2]);

        const NEW_TABLE_LIMIT: i32 = 15000;
        let modified = repos
            .namespaces()
            .update_table_limit(namespace_name, NEW_TABLE_LIMIT)
            .await
            .expect("namespace should be updateable");
        assert_eq!(NEW_TABLE_LIMIT, modified.max_tables);

        const NEW_COLUMN_LIMIT: i32 = 1500;
        let modified = repos
            .namespaces()
            .update_column_limit(namespace_name, NEW_COLUMN_LIMIT)
            .await
            .expect("namespace should be updateable");
        assert_eq!(NEW_COLUMN_LIMIT, modified.max_columns_per_table);
    }

    async fn test_table(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_table_test", "inf", topic.id, pool.id)
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

        // get by id
        assert_eq!(t, repos.tables().get_by_id(t.id).await.unwrap().unwrap());
        assert!(repos
            .tables()
            .get_by_id(TableId::new(i64::MAX))
            .await
            .unwrap()
            .is_none());

        let tables = repos
            .tables()
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();
        assert_eq!(vec![t.clone()], tables);

        // test we can create a table of the same name in a different namespace
        let namespace2 = repos
            .namespaces()
            .create("two", "inf", topic.id, pool.id)
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

        // test get by namespace and name
        let foo_table = repos
            .tables()
            .create_or_get("foo", namespace2.id)
            .await
            .unwrap();
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(NamespaceId::new(i64::MAX), "test_table")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace.id, "not_existing")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace.id, "test_table")
                .await
                .unwrap(),
            Some(t.clone())
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace2.id, "test_table")
                .await
                .unwrap()
                .as_ref(),
            Some(&test_table)
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace2.id, "foo")
                .await
                .unwrap()
                .as_ref(),
            Some(&foo_table)
        );

        // All tables should be returned by list(), regardless of namespace
        let list = repos.tables().list().await.unwrap();
        assert_eq!(list.as_slice(), [tt, test_table, foo_table]);

        // test we can get table persistence info with no persistence so far
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(555))
            .await
            .unwrap();
        let ti = repos
            .tables()
            .get_table_persist_info(shard.id, t.namespace_id, &t.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ti,
            TablePersistInfo {
                shard_id: shard.id,
                table_id: t.id,
                tombstone_max_sequence_number: None
            }
        );

        // and now with a tombstone persisted
        let tombstone = repos
            .tombstones()
            .create_or_get(
                t.id,
                shard.id,
                SequenceNumber::new(2001),
                Timestamp::new(1),
                Timestamp::new(10),
                "wahtevs",
            )
            .await
            .unwrap();
        let ti = repos
            .tables()
            .get_table_persist_info(shard.id, t.namespace_id, &t.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ti,
            TablePersistInfo {
                shard_id: shard.id,
                table_id: t.id,
                tombstone_max_sequence_number: Some(tombstone.sequence_number),
            }
        );

        // test per-namespace table limits
        let latest = repos
            .namespaces()
            .update_table_limit("namespace_table_test", 1)
            .await
            .expect("namespace should be updateable");
        let err = repos
            .tables()
            .create_or_get("definitely_unique", latest.id)
            .await
            .expect_err("should error with table create limit error");
        assert!(matches!(
            err,
            Error::TableCreateLimitError {
                table_name: _,
                namespace_id: _
            }
        ));
    }

    async fn test_column(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_column_test", "inf", topic.id, pool.id)
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

        let cols3 = repos
            .columns()
            .create_or_get_many(&[
                ColumnUpsertRequest {
                    name: "a",
                    table_id: table2.id,
                    column_type: ColumnType::U64,
                },
                ColumnUpsertRequest {
                    name: "a",
                    table_id: table.id,
                    column_type: ColumnType::U64,
                },
            ])
            .await
            .unwrap();

        let columns = repos
            .columns()
            .list_by_namespace_id(namespace.id)
            .await
            .unwrap();

        let mut want = vec![c.clone(), ccc];
        want.extend(cols3.clone());
        assert_eq!(want, columns);

        let columns = repos.columns().list_by_table_id(table.id).await.unwrap();

        let want2 = vec![c, cols3[1].clone()];
        assert_eq!(want2, columns);

        // Add another tag column into table2
        let c3 = repos
            .columns()
            .create_or_get("b", table2.id, ColumnType::Tag)
            .await
            .unwrap();
        // Listing count of column types
        let mut col_count = repos
            .columns()
            .list_type_count_by_table_id(table2.id)
            .await
            .unwrap();
        let mut expect = vec![
            ColumnTypeCount {
                col_type: ColumnType::Tag as i16,
                count: 1,
            },
            ColumnTypeCount {
                col_type: ColumnType::U64 as i16,
                count: 2,
            },
        ];
        expect.sort_by_key(|c| c.col_type);
        col_count.sort_by_key(|c| c.col_type);
        assert_eq!(expect, col_count);

        // Listing columns should return all columns in the catalog
        let list = repos.columns().list().await.unwrap();
        want.extend([c3]);
        assert_eq!(list, want);

        // test per-namespace column limits
        repos
            .namespaces()
            .update_column_limit("namespace_column_test", 1)
            .await
            .expect("namespace should be updateable");
        let err = repos
            .columns()
            .create_or_get("definitely unique", table.id, ColumnType::Tag)
            .await
            .expect_err("should error with table create limit error");
        assert!(matches!(
            err,
            Error::ColumnCreateLimitError {
                column_name: _,
                table_id: _,
            }
        ));
    }

    async fn test_shards(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("shard_test").await.unwrap();

        // Create 10 shards
        let mut created = BTreeMap::new();
        for shard_index in 1..=10 {
            let shard = repos
                .shards()
                .create_or_get(&topic, ShardIndex::new(shard_index))
                .await
                .expect("failed to create shard");
            created.insert(shard.id, shard);
        }

        // List them and assert they match
        let listed = repos
            .shards()
            .list_by_topic(&topic)
            .await
            .expect("failed to list shards")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);

        // get by the topic and shard index
        let shard_index = ShardIndex::new(1);
        let shard = repos
            .shards()
            .get_by_topic_id_and_shard_index(topic.id, shard_index)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(topic.id, shard.topic_id);
        assert_eq!(shard_index, shard.shard_index);

        // update the number
        repos
            .shards()
            .update_min_unpersisted_sequence_number(shard.id, SequenceNumber::new(53))
            .await
            .unwrap();
        let updated_shard = repos
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        assert_eq!(updated_shard.id, shard.id);
        assert_eq!(
            updated_shard.min_unpersisted_sequence_number,
            SequenceNumber::new(53)
        );

        let shard = repos
            .shards()
            .get_by_topic_id_and_shard_index(topic.id, ShardIndex::new(523))
            .await
            .unwrap();
        assert!(shard.is_none());
    }

    async fn test_partition(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_partition_test", "inf", topic.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let other_shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(2))
            .await
            .unwrap();

        let mut created = BTreeMap::new();
        for key in ["foo", "bar"] {
            let partition = repos
                .partitions()
                .create_or_get(key.into(), shard.id, table.id)
                .await
                .expect("failed to create partition");
            created.insert(partition.id, partition);
        }
        let other_partition = repos
            .partitions()
            .create_or_get("asdf".into(), other_shard.id, table.id)
            .await
            .unwrap();

        // partitions can be retrieved easily
        assert_eq!(
            other_partition,
            repos
                .partitions()
                .get_by_id(other_partition.id)
                .await
                .unwrap()
                .unwrap()
        );
        assert!(repos
            .partitions()
            .get_by_id(PartitionId::new(i64::MAX))
            .await
            .unwrap()
            .is_none());

        // List them and assert they match
        let listed = repos
            .partitions()
            .list_by_shard(shard.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);

        let listed = repos
            .partitions()
            .list_by_table_id(table.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        created.insert(other_partition.id, other_partition.clone());
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

        // test list_by_namespace
        let namespace2 = repos
            .namespaces()
            .create("namespace_partition_test2", "inf", topic.id, pool.id)
            .await
            .unwrap();
        let table2 = repos
            .tables()
            .create_or_get("test_table2", namespace2.id)
            .await
            .unwrap();
        repos
            .partitions()
            .create_or_get("some_key".into(), shard.id, table2.id)
            .await
            .expect("failed to create partition");
        let listed = repos
            .partitions()
            .list_by_namespace(namespace.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();
        let expected: BTreeMap<_, _> = created
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .chain(std::iter::once((
                other_partition.id,
                other_partition.clone(),
            )))
            .collect();
        assert_eq!(expected, listed);

        // sort_key should be empty on creation
        assert!(other_partition.sort_key.is_empty());

        // test update_sort_key from None to Some
        repos
            .partitions()
            .update_sort_key(other_partition.id, &["tag2", "tag1", "time"])
            .await
            .unwrap();

        // test getting the new sort key
        let updated_other_partition = repos
            .partitions()
            .get_by_id(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_other_partition.sort_key,
            vec!["tag2", "tag1", "time"]
        );

        // test update_sort_key from Some value to Some other value
        repos
            .partitions()
            .update_sort_key(
                other_partition.id,
                &["tag2", "tag1", "tag3 , with comma", "time"],
            )
            .await
            .unwrap();

        // test getting the new sort key
        let updated_other_partition = repos
            .partitions()
            .get_by_id(other_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_other_partition.sort_key,
            vec!["tag2", "tag1", "tag3 , with comma", "time"]
        );
    }

    async fn test_tombstone(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_tombstone_test", "inf", topic.id, pool.id)
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
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);
        let t1 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                SequenceNumber::new(1),
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();
        assert!(t1.id > TombstoneId::new(0));
        assert_eq!(t1.shard_id, shard.id);
        assert_eq!(t1.sequence_number, SequenceNumber::new(1));
        assert_eq!(t1.min_time, min_time);
        assert_eq!(t1.max_time, max_time);
        assert_eq!(t1.serialized_predicate, "whatevs");
        let t2 = repos
            .tombstones()
            .create_or_get(
                other_table.id,
                shard.id,
                SequenceNumber::new(2),
                min_time.add(10),
                max_time.add(10),
                "bleh",
            )
            .await
            .unwrap();
        let t3 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                SequenceNumber::new(3),
                min_time.add(10),
                max_time.add(10),
                "sdf",
            )
            .await
            .unwrap();

        let listed = repos
            .tombstones()
            .list_tombstones_by_shard_greater_than(shard.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![t2.clone(), t3.clone()], listed);

        // test list_by_table
        let listed = repos.tombstones().list_by_table(table.id).await.unwrap();
        assert_eq!(vec![t1.clone(), t3.clone()], listed);
        let listed = repos
            .tombstones()
            .list_by_table(other_table.id)
            .await
            .unwrap();
        assert_eq!(vec![t2.clone()], listed);

        // test list_by_namespace
        let namespace2 = repos
            .namespaces()
            .create("namespace_tombstone_test2", "inf", topic.id, pool.id)
            .await
            .unwrap();
        let table2 = repos
            .tables()
            .create_or_get("test_table2", namespace2.id)
            .await
            .unwrap();
        let t4 = repos
            .tombstones()
            .create_or_get(
                table2.id,
                shard.id,
                SequenceNumber::new(1),
                min_time.add(10),
                max_time.add(10),
                "whatevs",
            )
            .await
            .unwrap();
        let t5 = repos
            .tombstones()
            .create_or_get(
                table2.id,
                shard.id,
                SequenceNumber::new(2),
                min_time.add(10),
                max_time.add(10),
                "foo",
            )
            .await
            .unwrap();
        let listed = repos
            .tombstones()
            .list_by_namespace(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![t4.clone(), t5.clone()], listed);
        let listed = repos
            .tombstones()
            .list_by_namespace(NamespaceId::new(i64::MAX))
            .await
            .unwrap();
        assert!(listed.is_empty());

        // test get_by_id
        let ts = repos.tombstones().get_by_id(t1.id).await.unwrap().unwrap();
        assert_eq!(ts, t1.clone());
        let ts = repos.tombstones().get_by_id(t2.id).await.unwrap().unwrap();
        assert_eq!(ts, t2.clone());
        let ts = repos
            .tombstones()
            .get_by_id(TombstoneId::new(
                t1.id.get() + t2.id.get() + t3.id.get() + t4.id.get() + t5.id.get(),
            )) // not exist id
            .await
            .unwrap();
        assert!(ts.is_none());

        // test remove
        repos.tombstones().remove(&[t1.id, t3.id]).await.unwrap();
        let ts = repos.tombstones().get_by_id(t1.id).await.unwrap();
        assert!(ts.is_none()); // no longer there
        let ts = repos.tombstones().get_by_id(t3.id).await.unwrap();
        assert!(ts.is_none()); // no longer there
        let ts = repos.tombstones().get_by_id(t2.id).await.unwrap().unwrap();
        assert_eq!(ts, t2.clone()); // still there
        let ts = repos.tombstones().get_by_id(t4.id).await.unwrap().unwrap();
        assert_eq!(ts, t4.clone()); // still there
        let ts = repos.tombstones().get_by_id(t5.id).await.unwrap().unwrap();
        assert_eq!(ts, t5.clone()); // still there
    }

    async fn test_tombstones_by_parquet_file(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_tombstones_by_parquet_file_test",
                "inf",
                topic.id,
                pool.id,
            )
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
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(57))
            .await
            .unwrap();
        let other_shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(58))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(10);
        let max_time = Timestamp::new(20);
        let max_sequence_number = SequenceNumber::new(140);

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Create a tombstone with another shard
        repos
            .tombstones()
            .create_or_get(
                table.id,
                other_shard.id,
                max_sequence_number + 100,
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone with the same shard but a different table
        repos
            .tombstones()
            .create_or_get(
                other_table.id,
                shard.id,
                max_sequence_number + 101,
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone with a sequence number before the parquet file's max
        repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number - 10,
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone with a sequence number exactly equal to the parquet file's max
        repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number,
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone with a time range less than the parquet file's times
        repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number + 102,
                min_time - 5,
                min_time - 4,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone with a time range greater than the parquet file's times
        repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number + 103,
                max_time + 1,
                max_time + 2,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone that matches all criteria
        let matching_tombstone1 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number + 104,
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone that overlaps the file's min
        let matching_tombstone2 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number + 105,
                min_time - 1,
                min_time + 1,
                "whatevs",
            )
            .await
            .unwrap();

        // Create a tombstone that overlaps the file's max
        let matching_tombstone3 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                max_sequence_number + 106,
                max_time - 1,
                max_time + 1,
                "whatevs",
            )
            .await
            .unwrap();

        let tombstones = repos
            .tombstones()
            .list_tombstones_for_time_range(
                shard.id,
                table.id,
                max_sequence_number,
                min_time,
                max_time,
            )
            .await
            .unwrap();
        let mut tombstones_ids: Vec<_> = tombstones.iter().map(|t| t.id).collect();
        tombstones_ids.sort();
        let expected = vec![
            matching_tombstone1,
            matching_tombstone2,
            matching_tombstone3,
        ];
        let mut expected_ids: Vec<_> = expected.iter().map(|t| t.id).collect();
        expected_ids.sort();

        assert_eq!(
            tombstones_ids, expected_ids,
            "\ntombstones: {:#?}\nexpected: {:#?}\nparquet_file: {:#?}",
            tombstones, expected, parquet_file
        );
    }

    async fn test_parquet_file(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_parquet_file_test", "inf", topic.id, pool.id)
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
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let other_partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, other_table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // verify we can get it by its object store id
        let pfg = repos
            .parquet_files()
            .get_by_object_store_id(parquet_file.object_store_id)
            .await
            .unwrap();
        assert_eq!(parquet_file, pfg.unwrap());

        // verify that trying to create a file with the same UUID throws an error
        let err = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::FileExists { object_store_id: _ }));

        let other_params = ParquetFileParams {
            table_id: other_partition.table_id,
            partition_id: other_partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(200),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            ..parquet_file_params.clone()
        };
        let other_file = repos.parquet_files().create(other_params).await.unwrap();

        let exist_id = parquet_file.id;
        let non_exist_id = ParquetFileId::new(other_file.id.get() + 10);
        // make sure exists_id != non_exist_id
        assert_ne!(exist_id, non_exist_id);
        assert!(repos.parquet_files().exist(exist_id).await.unwrap());
        assert!(!repos.parquet_files().exist(non_exist_id).await.unwrap());

        let files = repos
            .parquet_files()
            .list_by_shard_greater_than(shard.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![parquet_file.clone(), other_file.clone()], files);
        let files = repos
            .parquet_files()
            .list_by_shard_greater_than(shard.id, SequenceNumber::new(150))
            .await
            .unwrap();
        assert_eq!(vec![other_file.clone()], files);

        // verify that to_delete is initially set to null and the file does not get deleted
        assert!(parquet_file.to_delete.is_none());
        let older_than = Timestamp::new(
            (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
        );
        let deleted_files = repos.parquet_files().delete_old(older_than).await.unwrap();
        assert!(deleted_files.is_empty());
        assert!(repos.parquet_files().exist(parquet_file.id).await.unwrap());

        // verify to_delete can be updated to a timestamp
        repos
            .parquet_files()
            .flag_for_delete(parquet_file.id)
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_shard_greater_than(shard.id, SequenceNumber::new(1))
            .await
            .unwrap();
        let marked_deleted = files.first().unwrap();
        assert!(marked_deleted.to_delete.is_some());

        // File is not deleted if it was marked to be deleted after the specified time
        let before_deleted = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(100)).timestamp_nanos(),
        );
        let deleted_files = repos
            .parquet_files()
            .delete_old(before_deleted)
            .await
            .unwrap();
        assert!(deleted_files.is_empty());
        assert!(repos.parquet_files().exist(parquet_file.id).await.unwrap());

        // File is deleted if it was marked to be deleted before the specified time
        let deleted_files = repos.parquet_files().delete_old(older_than).await.unwrap();
        assert_eq!(deleted_files.len(), 1);
        assert_eq!(marked_deleted, &deleted_files[0]);
        assert!(!repos.parquet_files().exist(parquet_file.id).await.unwrap());

        // test list_by_table_not_to_delete
        let files = repos
            .parquet_files()
            .list_by_table_not_to_delete(table.id)
            .await
            .unwrap();
        assert_eq!(files, vec![]);
        let files = repos
            .parquet_files()
            .list_by_table_not_to_delete(other_table.id)
            .await
            .unwrap();
        assert_eq!(files, vec![other_file.clone()]);

        // test list_by_namespace_not_to_delete
        let namespace2 = repos
            .namespaces()
            .create("namespace_parquet_file_test1", "inf", topic.id, pool.id)
            .await
            .unwrap();
        let table2 = repos
            .tables()
            .create_or_get("test_table2", namespace2.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("foo".into(), shard.id, table2.id)
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert!(files.is_empty());

        let f1_params = ParquetFileParams {
            table_id: partition2.table_id,
            partition_id: partition2.id,
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            max_sequence_number: SequenceNumber::new(10),
            ..parquet_file_params
        };
        let f1 = repos
            .parquet_files()
            .create(f1_params.clone())
            .await
            .unwrap();

        let f2_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            max_sequence_number: SequenceNumber::new(11),
            ..f1_params
        };
        let f2 = repos
            .parquet_files()
            .create(f2_params.clone())
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f2.clone()], files);

        let f3_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            max_sequence_number: SequenceNumber::new(12),
            ..f2_params
        };
        let f3 = repos.parquet_files().create(f3_params).await.unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f2.clone(), f3.clone()], files);

        repos.parquet_files().flag_for_delete(f2.id).await.unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f3.clone()], files);

        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(NamespaceId::new(i64::MAX))
            .await
            .unwrap();
        assert!(files.is_empty());

        // test count_by_overlaps_with_level_0
        // not time overlap
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_0(
                partition2.table_id,
                shard.id,
                Timestamp::new(11),
                Timestamp::new(20),
                SequenceNumber::new(20),
            )
            .await
            .unwrap();
        assert_eq!(count, 0);
        // overlaps with f1
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_0(
                partition2.table_id,
                shard.id,
                Timestamp::new(1),
                Timestamp::new(10),
                SequenceNumber::new(20),
            )
            .await
            .unwrap();
        assert_eq!(count, 1);
        // overlaps with f1 and f3
        // f2 is deleted and should not be counted
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_0(
                partition2.table_id,
                shard.id,
                Timestamp::new(7),
                Timestamp::new(55),
                SequenceNumber::new(20),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        // overlaps with f1 and f3 but on different time range
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_0(
                partition2.table_id,
                shard.id,
                Timestamp::new(1),
                Timestamp::new(100),
                SequenceNumber::new(20),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        // overlaps with f3
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_0(
                partition2.table_id,
                shard.id,
                Timestamp::new(15),
                Timestamp::new(100),
                SequenceNumber::new(20),
            )
            .await
            .unwrap();
        assert_eq!(count, 1);
        // no overlaps due to smaller sequence number
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_0(
                partition2.table_id,
                shard.id,
                Timestamp::new(15),
                Timestamp::new(100),
                SequenceNumber::new(2),
            )
            .await
            .unwrap();
        assert_eq!(count, 0);

        // test count_by_overlaps_with_level_1
        //
        // no level-1 file -> nothing overlap
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_1(
                partition2.table_id,
                shard.id,
                Timestamp::new(1),
                Timestamp::new(200),
            )
            .await
            .unwrap();
        assert_eq!(count, 0);
        //
        // Let upgarde all files (only f1 and f3 are not deleted) to level 1
        repos
            .parquet_files()
            .update_to_level_1(&[f1.id])
            .await
            .unwrap();
        repos
            .parquet_files()
            .update_to_level_1(&[f3.id])
            .await
            .unwrap();
        //
        // not overlap with any
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_1(
                partition2.table_id,
                shard.id,
                Timestamp::new(11),
                Timestamp::new(20),
            )
            .await
            .unwrap();
        assert_eq!(count, 0);
        // overlaps with f1
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_1(
                partition2.table_id,
                shard.id,
                Timestamp::new(1),
                Timestamp::new(10),
            )
            .await
            .unwrap();
        assert_eq!(count, 1);
        // overlaps with f1 and f3
        // f2 is deleted and should not be counted
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_1(
                partition2.table_id,
                shard.id,
                Timestamp::new(7),
                Timestamp::new(55),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        // overlaps with f1 and f3 but on different time range
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_1(
                partition2.table_id,
                shard.id,
                Timestamp::new(1),
                Timestamp::new(100),
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        // overlaps with f3
        let count = repos
            .parquet_files()
            .count_by_overlaps_with_level_1(
                partition2.table_id,
                shard.id,
                Timestamp::new(15),
                Timestamp::new(100),
            )
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    async fn test_parquet_file_compaction_level_0(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_parquet_file_compaction_level_0_test",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(100))
            .await
            .unwrap();
        let other_shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(101))
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Create a compaction level 0 file for some other shard
        let other_shard_params = ParquetFileParams {
            shard_id: other_shard.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };

        let _other_shard_file = repos
            .parquet_files()
            .create(other_shard_params)
            .await
            .unwrap();

        // Create a compaction level 0 file marked to delete
        let to_delete_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let to_delete_file = repos
            .parquet_files()
            .create(to_delete_params)
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(to_delete_file.id)
            .await
            .unwrap();

        // Create a compaction level 1 file
        let level_1_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let level_1_file = repos.parquet_files().create(level_1_params).await.unwrap();
        repos
            .parquet_files()
            .update_to_level_1(&[level_1_file.id])
            .await
            .unwrap();

        // Level 0 parquet files for a shard should contain only those that match the right
        // criteria
        let level_0 = repos.parquet_files().level_0(shard.id).await.unwrap();
        let mut level_0_ids: Vec<_> = level_0.iter().map(|pf| pf.id).collect();
        level_0_ids.sort();
        let expected = vec![parquet_file];
        let mut expected_ids: Vec<_> = expected.iter().map(|pf| pf.id).collect();
        expected_ids.sort();

        assert_eq!(
            level_0_ids, expected_ids,
            "\nlevel 0: {:#?}\nexpected: {:#?}",
            level_0, expected,
        );
    }

    async fn test_parquet_file_compaction_level_1(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_parquet_file_compaction_level_1_test",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let other_table = repos
            .tables()
            .create_or_get("test_table2", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(100))
            .await
            .unwrap();
        let other_shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(101))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let other_partition = repos
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();

        // Set up the window of times we're interested in level 1 files for
        let query_min_time = Timestamp::new(5);
        let query_max_time = Timestamp::new(10);

        // Create a file with times entirely within the window
        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time: query_min_time + 1,
            max_time: query_max_time - 1,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Create a file that will remain as level 0
        let level_0_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let _level_0_file = repos.parquet_files().create(level_0_params).await.unwrap();

        // Create a file completely before the window
        let too_early_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: query_min_time - 2,
            max_time: query_min_time - 1,
            ..parquet_file_params.clone()
        };
        let too_early_file = repos
            .parquet_files()
            .create(too_early_params)
            .await
            .unwrap();

        // Create a file overlapping the window on the lower end
        let overlap_lower_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: query_min_time - 1,
            max_time: query_min_time + 1,
            ..parquet_file_params.clone()
        };
        let overlap_lower_file = repos
            .parquet_files()
            .create(overlap_lower_params)
            .await
            .unwrap();

        // Create a file overlapping the window on the upper end
        let overlap_upper_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: query_max_time - 1,
            max_time: query_max_time + 1,
            ..parquet_file_params.clone()
        };
        let overlap_upper_file = repos
            .parquet_files()
            .create(overlap_upper_params)
            .await
            .unwrap();

        // Create a file completely after the window
        let too_late_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: query_max_time + 1,
            max_time: query_max_time + 2,
            ..parquet_file_params.clone()
        };
        let too_late_file = repos.parquet_files().create(too_late_params).await.unwrap();

        // Create a file for some other shard
        let other_shard_params = ParquetFileParams {
            shard_id: other_shard.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let other_shard_file = repos
            .parquet_files()
            .create(other_shard_params)
            .await
            .unwrap();

        // Create a file for the same shard but a different table
        let other_table_params = ParquetFileParams {
            table_id: other_table.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let other_table_file = repos
            .parquet_files()
            .create(other_table_params)
            .await
            .unwrap();

        // Create a file for the same shard and table but a different partition
        let other_partition_params = ParquetFileParams {
            partition_id: other_partition.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let other_partition_file = repos
            .parquet_files()
            .create(other_partition_params)
            .await
            .unwrap();

        // Create a file marked to be deleted
        let to_delete_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let to_delete_file = repos
            .parquet_files()
            .create(to_delete_params)
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(to_delete_file.id)
            .await
            .unwrap();

        // Make all but _level_0_file compaction level 1
        repos
            .parquet_files()
            .update_to_level_1(&[
                parquet_file.id,
                too_early_file.id,
                too_late_file.id,
                overlap_lower_file.id,
                overlap_upper_file.id,
                other_shard_file.id,
                other_table_file.id,
                other_partition_file.id,
                to_delete_file.id,
            ])
            .await
            .unwrap();

        // Level 1 parquet files for a shard should contain only those that match the right
        // criteria
        let table_partition = TablePartition::new(shard.id, table.id, partition.id);
        let level_1 = repos
            .parquet_files()
            .level_1(table_partition, query_min_time, query_max_time)
            .await
            .unwrap();
        let mut level_1_ids: Vec<_> = level_1.iter().map(|pf| pf.id).collect();
        level_1_ids.sort();
        let expected = vec![parquet_file, overlap_lower_file, overlap_upper_file];
        let mut expected_ids: Vec<_> = expected.iter().map(|pf| pf.id).collect();
        expected_ids.sort();

        assert_eq!(
            level_1_ids, expected_ids,
            "\nlevel 1: {:#?}\nexpected: {:#?}",
            level_1, expected,
        );
    }

    async fn test_most_level_0_files_partitions(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("most_level_0").await.unwrap();
        let pool = repos
            .query_pools()
            .create_or_get("most_level_0")
            .await
            .unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "test_most_level_0_files_partitions",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(100))
            .await
            .unwrap();

        let time_five_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 5)).timestamp_nanos(),
        );
        let time_38_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos(),
        );

        let num_partitions = 2;

        let time_24_hours_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 24)).timestamp_nanos(),
        );

        // Db has no partition
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // The DB has 1 partition but it does not have any files
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // The partition has one deleted file
        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: time_38_hour_ago,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let delete_l0_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(delete_l0_file.id)
            .await
            .unwrap();
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // A partition with one cold file and one hot file
        let hot_partition = repos
            .partitions()
            .create_or_get("hot".into(), shard.id, table.id)
            .await
            .unwrap();
        let cold_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: hot_partition.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(cold_file_params)
            .await
            .unwrap();
        let hot_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: hot_partition.id,
            created_at: time_five_hour_ago,
            ..parquet_file_params.clone()
        };
        repos.parquet_files().create(hot_file_params).await.unwrap();
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // The partition has one non-deleted level 0 file
        let l0_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_file_params.clone())
            .await
            .unwrap();
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);

        // The DB has 2 partitions; both have non-deleted L0 files
        let another_partition = repos
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();
        let another_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: another_partition.id,
            ..parquet_file_params.clone()
        };
        // The new partition has 2 non-deleted L0 files
        repos
            .parquet_files()
            .create(another_file_params.clone())
            .await
            .unwrap();
        let another_file_params_for_second_l0 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..another_file_params.clone()
        };
        repos
            .parquet_files()
            .create(another_file_params_for_second_l0.clone())
            .await
            .unwrap();
        // Must return 2 partitions
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        // They must be in order another_partition (more L0 files), partition
        assert_eq!(partitions[0].partition_id, another_partition.id); // 2 files
        assert_eq!(partitions[1].partition_id, partition.id); // 1 file

        // The DB has 3 partitions with non-deleted L0 files
        let third_partition = repos
            .partitions()
            .create_or_get("three".into(), shard.id, table.id)
            .await
            .unwrap();
        let file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            partition_id: third_partition.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(file_params.clone())
            .await
            .unwrap();
        // Still return 2 partitions the limit num_partitions=2
        let partitions = repos
            .parquet_files()
            .most_level_0_files_partitions(shard.id, time_24_hours_ago, num_partitions)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        // and the first one should still be the one with the most L0 files
        assert_eq!(partitions[0].partition_id, another_partition.id);
    }

    async fn test_recent_highest_throughput_partitions(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos
            .topics()
            .create_or_get("highest_throughput")
            .await
            .unwrap();
        let pool = repos
            .query_pools()
            .create_or_get("highest_throughput")
            .await
            .unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "test_recent_highest_throughput_partitions",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(100))
            .await
            .unwrap();

        // params for the tests
        let num_minutes = 4 * 60;
        let min_num_files = 2;
        let num_partitions = 2;

        let time_at_num_minutes_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * num_minutes))
                .timestamp_nanos(),
        );

        // Case 1
        // Db has no partition
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                min_num_files,
                num_partitions,
            )
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // Case 2
        // The DB has 1 partition but it does not have any file
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                min_num_files,
                num_partitions,
            )
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // Time for testing
        let time_now = Timestamp::new(catalog.time_provider().now().timestamp_nanos());
        let time_one_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60)).timestamp_nanos(),
        );
        let time_two_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 2)).timestamp_nanos(),
        );
        let time_three_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 3)).timestamp_nanos(),
        );
        let time_five_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 5)).timestamp_nanos(),
        );
        let time_ten_hour_ago = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(60 * 60 * 10)).timestamp_nanos(),
        );

        // Case 3
        // The partition has one deleted file
        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let delete_l0_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(delete_l0_file.id)
            .await
            .unwrap();
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                min_num_files,
                num_partitions,
            )
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // Case 4
        // Partition has only 1 file created recently
        let l0_one_hour_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_one_hour_ago,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_one_hour_ago_file_params.clone())
            .await
            .unwrap();
        // Case 4.1: min_num_files = 2
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                min_num_files,
                num_partitions,
            )
            .await
            .unwrap();
        // nothing return because the partition has only one recent L0 file which is smaller than min_num_files = 2
        assert!(partitions.is_empty());
        // Case 4.2: min_num_files = 1
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                1,
                num_partitions,
            )
            .await
            .unwrap();
        // and have one partition
        assert_eq!(partitions.len(), 1);

        // Case 5
        // Let us create another partition with 2 L0 recent files
        let another_partition = repos
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();
        let l0_2_hours_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_two_hour_ago,
            partition_id: another_partition.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_2_hours_ago_file_params)
            .await
            .unwrap();
        let l0_3_hours_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_three_hour_ago,
            partition_id: another_partition.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_3_hours_ago_file_params)
            .await
            .unwrap();
        // Case 5.1: min_num_files = 2
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                min_num_files,
                num_partitions,
            )
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].partition_id, another_partition.id); // must be the partition with 2 files
                                                                      //
                                                                      // Case 5.2: min_num_files = 1
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                1,
                num_partitions,
            )
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].partition_id, another_partition.id); // partition with 2 files must be first
        assert_eq!(partitions[1].partition_id, partition.id);

        // Case 6
        // Add 2 not-recent files to the first partition
        let l0_5_hours_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_five_hour_ago,
            partition_id: partition.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_5_hours_ago_file_params)
            .await
            .unwrap();
        let l0_10_hours_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_ten_hour_ago,
            partition_id: partition.id,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_10_hours_ago_file_params)
            .await
            .unwrap();
        // Case 6.1: min_num_files = 2
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                min_num_files,
                num_partitions,
            )
            .await
            .unwrap();
        // result still 1 partition because the old files do not contribute to recent throughput
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].partition_id, another_partition.id); // must be the partition with 2 files
                                                                      //
                                                                      // Case 6.2: min_num_files = 1
        let partitions = repos
            .parquet_files()
            .recent_highest_throughput_partitions(
                shard.id,
                time_at_num_minutes_ago,
                1,
                num_partitions,
            )
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].partition_id, another_partition.id); // partition with 2 files must be first
        assert_eq!(partitions[1].partition_id, partition.id);
    }

    async fn test_list_by_partiton_not_to_delete(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_parquet_file_test_list_by_partiton_not_to_delete",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(100))
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("two".into(), shard.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let delete_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let delete_file = repos
            .parquet_files()
            .create(delete_file_params)
            .await
            .unwrap();
        repos
            .parquet_files()
            .flag_for_delete(delete_file.id)
            .await
            .unwrap();
        let level1_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let mut level1_file = repos
            .parquet_files()
            .create(level1_file_params)
            .await
            .unwrap();
        repos
            .parquet_files()
            .update_to_level_1(&[level1_file.id])
            .await
            .unwrap();
        level1_file.compaction_level = CompactionLevel::FileNonOverlapped;

        let other_partition_params = ParquetFileParams {
            partition_id: partition2.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let _partition2_file = repos
            .parquet_files()
            .create(other_partition_params)
            .await
            .unwrap();

        let files = repos
            .parquet_files()
            .list_by_partition_not_to_delete(partition.id)
            .await
            .unwrap();
        assert_eq!(files, vec![parquet_file.clone(), level1_file.clone()]);
    }

    async fn test_update_to_compaction_level_1(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_update_to_compaction_level_1_test",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("update_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(1000))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        // Set up the window of times we're interested in level 1 files for
        let query_min_time = Timestamp::new(5);
        let query_max_time = Timestamp::new(10);

        // Create a file with times entirely within the window
        let parquet_file_params = ParquetFileParams {
            shard_id: shard.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),

            max_sequence_number: SequenceNumber::new(140),
            min_time: query_min_time + 1,
            max_time: query_max_time - 1,
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Create a file that will remain as level 0
        let level_0_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let level_0_file = repos.parquet_files().create(level_0_params).await.unwrap();

        // Create a ParquetFileId that doesn't actually exist in the catalog
        let nonexistent_parquet_file_id = ParquetFileId::new(level_0_file.id.get() + 1);

        // Level 0 parquet files should contain both existing files at this point
        let expected = vec![parquet_file.clone(), level_0_file.clone()];
        let level_0 = repos.parquet_files().level_0(shard.id).await.unwrap();
        let mut level_0_ids: Vec<_> = level_0.iter().map(|pf| pf.id).collect();
        level_0_ids.sort();
        let mut expected_ids: Vec<_> = expected.iter().map(|pf| pf.id).collect();
        expected_ids.sort();
        assert_eq!(
            level_0_ids, expected_ids,
            "\nlevel 0: {:#?}\nexpected: {:#?}",
            level_0, expected,
        );

        // Make parquet_file compaction level 1, attempt to mark the nonexistent file; operation
        // should succeed
        let updated = repos
            .parquet_files()
            .update_to_level_1(&[parquet_file.id, nonexistent_parquet_file_id])
            .await
            .unwrap();
        assert_eq!(updated, vec![parquet_file.id]);

        // Level 0 parquet files should only contain level_0_file
        let expected = vec![level_0_file];
        let level_0 = repos.parquet_files().level_0(shard.id).await.unwrap();
        let mut level_0_ids: Vec<_> = level_0.iter().map(|pf| pf.id).collect();
        level_0_ids.sort();
        let mut expected_ids: Vec<_> = expected.iter().map(|pf| pf.id).collect();
        expected_ids.sort();
        assert_eq!(
            level_0_ids, expected_ids,
            "\nlevel 0: {:#?}\nexpected: {:#?}",
            level_0, expected,
        );

        // Level 1 parquet files for a shard should only contain parquet_file
        let expected = vec![parquet_file];
        let table_partition = TablePartition::new(shard.id, table.id, partition.id);
        let level_1 = repos
            .parquet_files()
            .level_1(table_partition, query_min_time, query_max_time)
            .await
            .unwrap();
        let mut level_1_ids: Vec<_> = level_1.iter().map(|pf| pf.id).collect();
        level_1_ids.sort();
        let mut expected_ids: Vec<_> = expected.iter().map(|pf| pf.id).collect();
        expected_ids.sort();
        assert_eq!(
            level_1_ids, expected_ids,
            "\nlevel 1: {:#?}\nexpected: {:#?}",
            level_1, expected,
        );
    }

    async fn test_processed_tombstones(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_processed_tombstone_test",
                "inf",
                topic.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let shard = repos
            .shards()
            .create_or_get(&topic, ShardIndex::new(1))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), shard.id, table.id)
            .await
            .unwrap();

        // parquet files
        let parquet_file_params = ParquetFileParams {
            namespace_id: namespace.id,
            shard_id: shard.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(1),
            min_time: Timestamp::new(100),
            max_time: Timestamp::new(250),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };
        let p1 = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let parquet_file_params_2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(3),
            min_time: Timestamp::new(200),
            max_time: Timestamp::new(300),
            ..parquet_file_params
        };
        let p2 = repos
            .parquet_files()
            .create(parquet_file_params_2.clone())
            .await
            .unwrap();

        // tombstones
        let t1 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                SequenceNumber::new(10),
                Timestamp::new(1),
                Timestamp::new(10),
                "whatevs",
            )
            .await
            .unwrap();
        let t2 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                SequenceNumber::new(11),
                Timestamp::new(100),
                Timestamp::new(110),
                "whatevs",
            )
            .await
            .unwrap();
        let t3 = repos
            .tombstones()
            .create_or_get(
                table.id,
                shard.id,
                SequenceNumber::new(12),
                Timestamp::new(200),
                Timestamp::new(210),
                "whatevs",
            )
            .await
            .unwrap();

        // processed tombstones
        // p1, t2
        let _pt1 = repos
            .processed_tombstones()
            .create(p1.id, t2.id)
            .await
            .unwrap();
        // p1, t3
        let _pt2 = repos
            .processed_tombstones()
            .create(p1.id, t3.id)
            .await
            .unwrap();
        // p2, t3
        let _pt3 = repos
            .processed_tombstones()
            .create(p2.id, t3.id)
            .await
            .unwrap();

        // test exist
        let exist = repos
            .processed_tombstones()
            .exist(p1.id, t1.id)
            .await
            .unwrap();
        assert!(!exist);
        let exist = repos
            .processed_tombstones()
            .exist(p1.id, t2.id)
            .await
            .unwrap();
        assert!(exist);

        // test count
        let count = repos.processed_tombstones().count().await.unwrap();
        assert_eq!(count, 3);

        // test count_by_tombstone_id
        let count = repos
            .processed_tombstones()
            .count_by_tombstone_id(t1.id)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let count = repos
            .processed_tombstones()
            .count_by_tombstone_id(t2.id)
            .await
            .unwrap();
        assert_eq!(count, 1);
        let count = repos
            .processed_tombstones()
            .count_by_tombstone_id(t3.id)
            .await
            .unwrap();
        assert_eq!(count, 2);

        // test remove
        repos.tombstones().remove(&[t3.id]).await.unwrap();
        // should still be 1 because t2 was not deleted
        let count = repos
            .processed_tombstones()
            .count_by_tombstone_id(t2.id)
            .await
            .unwrap();
        assert_eq!(count, 1);
        // should be 0 because t3 was deleted
        let count = repos
            .processed_tombstones()
            .count_by_tombstone_id(t3.id)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    async fn test_txn_isolation(catalog: Arc<dyn Catalog>) {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let barrier_captured = Arc::clone(&barrier);
        let catalog_captured = Arc::clone(&catalog);
        let insertion_task = tokio::spawn(async move {
            barrier_captured.wait().await;

            let mut txn = catalog_captured.start_transaction().await.unwrap();
            txn.topics()
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
            .topics()
            .get_by_name("test_txn_isolation")
            .await
            .unwrap();
        assert!(topic.is_none());
        txn.abort().await.unwrap();

        insertion_task.await.unwrap();

        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn
            .topics()
            .get_by_name("test_txn_isolation")
            .await
            .unwrap();
        assert!(topic.is_none());
        txn.abort().await.unwrap();
    }

    async fn test_txn_drop(catalog: Arc<dyn Catalog>) {
        let capture = TracingCapture::new();
        let mut txn = catalog.start_transaction().await.unwrap();
        txn.topics().create_or_get("test_txn_drop").await.unwrap();
        drop(txn);

        // got a warning
        assert_contains!(capture.to_string(), "Dropping ");
        assert_contains!(capture.to_string(), " w/o finalizing (commit or abort)");

        // data is NOT committed
        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn.topics().get_by_name("test_txn_drop").await.unwrap();
        assert!(topic.is_none());
        txn.abort().await.unwrap();
    }

    /// Upsert a namespace called `namespace_name` and write `lines` to it.
    async fn populate_namespace<R>(
        repos: &mut R,
        namespace_name: &str,
        lines: &str,
    ) -> (Namespace, NamespaceSchema)
    where
        R: RepoCollection + ?Sized,
    {
        let topic = repos.topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(namespace_name, "inf", topic.id, pool.id)
            .await;

        let namespace = match namespace {
            Ok(v) => v,
            Err(Error::NameExists { .. }) => repos
                .namespaces()
                .get_by_name(namespace_name)
                .await
                .unwrap()
                .unwrap(),
            e @ Err(_) => e.unwrap(),
        };

        let batches = mutable_batch_lp::lines_to_batches(lines, 42).unwrap();
        let batches = batches.iter().map(|(table, batch)| (table.as_str(), batch));
        let ns = NamespaceSchema::new(namespace.id, topic.id, pool.id);

        let schema = validate_or_insert_schema(batches, &ns, repos)
            .await
            .expect("validate schema failed")
            .unwrap_or(ns);

        (namespace, schema)
    }

    async fn test_list_schemas(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;

        let ns1 = populate_namespace(
            repos.deref_mut(),
            "ns1",
            "cpu,tag=1 field=1i\nanother,tag=1 field=1.0",
        )
        .await;
        let ns2 = populate_namespace(
            repos.deref_mut(),
            "ns2",
            "cpu,tag=1 field=1i\nsomethingelse field=1u",
        )
        .await;

        // Otherwise the in-mem catalog deadlocks.... (but not postgres)
        drop(repos);

        let got = list_schemas(&*catalog)
            .await
            .expect("should be able to list the schemas")
            .collect::<Vec<_>>();

        assert!(got.contains(&ns1), "{:#?}\n\nwant{:#?}", got, &ns1);
        assert!(got.contains(&ns2), "{:#?}\n\nwant{:#?}", got, &ns2);
    }

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 1, "metric did not record any calls");
    }
}
