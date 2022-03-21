//! This module contains the traits and data objects for the Catalog API.

use async_trait::async_trait;
use data_types2::{
    Column, ColumnSchema, ColumnType, KafkaPartition, KafkaTopic, KafkaTopicId, Namespace,
    NamespaceId, NamespaceSchema, ParquetFile, ParquetFileId, ParquetFileParams, Partition,
    PartitionId, PartitionInfo, ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber,
    Sequencer, SequencerId, Table, TableId, TablePartition, TableSchema, Timestamp, Tombstone,
    TombstoneId,
};
use snafu::{OptionExt, Snafu};
use std::{collections::BTreeMap, convert::TryFrom, fmt::Debug, sync::Arc};
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

    #[snafu(display("table {} not found", id))]
    TableNotFound { id: TableId },

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

    #[snafu(display("Datbase setup error: {}", source))]
    Setup { source: sqlx::Error },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Get metric registry associated w/ this catalog.
    fn metrics(&self) -> Arc<metric::Registry>;
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

    /// Gets the table persistence info for the given sequencer
    async fn get_table_persist_info(
        &mut self,
        sequencer_id: SequencerId,
        namespace_id: NamespaceId,
        table_name: &str,
    ) -> Result<Option<TablePersistInfo>>;
}

/// Information for a table's persistence information for a specific sequencer from the catalog
#[derive(Debug, Copy, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct TablePersistInfo {
    /// sequencer the sequence numbers are associated with
    pub sequencer_id: SequencerId,
    /// the global identifier for the table
    pub table_id: TableId,
    /// max max_sequence_number from this table's parquet_files for this sequencer
    pub parquet_max_sequence_number: Option<SequenceNumber>,
    /// max sequence number from this table's tombstones for this sequencer
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

    /// updates the `min_unpersisted_sequence_number` for a sequencer
    async fn update_min_unpersisted_sequence_number(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<()>;
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

    /// get partition by ID
    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;

    /// return partitions for a given sequencer
    async fn list_by_sequencer(&mut self, sequencer_id: SequencerId) -> Result<Vec<Partition>>;

    /// return partitions for a given namespace
    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>>;

    /// return the partition record, the namespace name it belongs to, and the table name it is under
    async fn partition_info_by_id(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionInfo>>;
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

    /// list all tombstones for a given namespace
    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Tombstone>>;

    /// list all tombstones for a given table
    async fn list_by_table(&mut self, table_id: TableId) -> Result<Vec<Tombstone>>;

    /// return all tombstones for the sequencer with a sequence number greater than that
    /// passed in. This will be used by the ingester on startup to see what tombstones
    /// might have to be applied to data that is read from the write buffer.
    async fn list_tombstones_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>>;
}

/// The starting compaction level for parquet files is zero.
pub const INITIAL_COMPACTION_LEVEL: i16 = 0;

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// create the parquet file
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;

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

    /// List all parquet files within a given namespace that are NOT marked as [`to_delete`](ParquetFile::to_delete).
    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>>;

    /// List all parquet files within a given table that are NOT marked as [`to_delete`](ParquetFile::to_delete).
    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;

    /// List parquet files for a given sequencer with compaction level 0 and other criteria that
    /// define a file as a candidate for compaction
    async fn level_0(&mut self, sequencer_id: SequencerId) -> Result<Vec<ParquetFile>>;

    /// List parquet files for a given table partition, in a given time range, with compaction
    /// level 1, and other criteria that define a file as a candidate for compaction with a level 0
    /// file
    async fn level_1(
        &mut self,
        table_partition: TablePartition,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<ParquetFile>>;

    /// Update the compaction level of the specified parquet files to level 1. Returns the IDs
    /// of the files that were successfully updated.
    async fn update_to_level_1(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
    ) -> Result<Vec<ParquetFileId>>;

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

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use ::test_helpers::{assert_contains, tracing::TracingCapture};
    use data_types2::ColumnId;
    use metric::{Attributes, Metric, U64Histogram};
    use std::{sync::Arc, time::Duration};

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
        test_parquet_file_compaction_level_0(Arc::clone(&catalog)).await;
        test_parquet_file_compaction_level_1(Arc::clone(&catalog)).await;
        test_update_to_compaction_level_1(Arc::clone(&catalog)).await;
        test_txn_isolation(Arc::clone(&catalog)).await;
        test_txn_drop(Arc::clone(&catalog)).await;

        let metrics = catalog.metrics();
        assert_metric_hit(&*metrics, "kafka_create_or_get");
        assert_metric_hit(&*metrics, "query_create_or_get");
        assert_metric_hit(&*metrics, "namespace_create");
        assert_metric_hit(&*metrics, "table_create_or_get");
        assert_metric_hit(&*metrics, "column_create_or_get");
        assert_metric_hit(&*metrics, "sequencer_create_or_get");
        assert_metric_hit(&*metrics, "partition_create_or_get");
        assert_metric_hit(&*metrics, "tombstone_create_or_get");
        assert_metric_hit(&*metrics, "parquet_create");
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
            .get_by_id(namespace.id)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_id(NamespaceId::new(i32::MAX))
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
            .create(namespace2_name, "inf", kafka.id, pool.id)
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

        // get by id
        assert_eq!(t, repos.tables().get_by_id(t.id).await.unwrap().unwrap());
        assert!(repos
            .tables()
            .get_by_id(TableId::new(i32::MAX))
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

        // test get by namespace and name
        let foo_table = repos
            .tables()
            .create_or_get("foo", namespace2.id)
            .await
            .unwrap();
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(NamespaceId::new(i32::MAX), "test_table")
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
                .unwrap(),
            Some(test_table)
        );
        assert_eq!(
            repos
                .tables()
                .get_by_namespace_and_name(namespace2.id, "foo")
                .await
                .unwrap(),
            Some(foo_table)
        );

        // test we can get table persistence info with no persistence so far
        let seq = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(555))
            .await
            .unwrap();
        let ti = repos
            .tables()
            .get_table_persist_info(seq.id, t.namespace_id, &t.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ti,
            TablePersistInfo {
                sequencer_id: seq.id,
                table_id: t.id,
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: None
            }
        );

        // and now with a parquet file persisted
        let partition = repos
            .partitions()
            .create_or_get("1970-01-01", seq.id, t.id)
            .await
            .unwrap();
        let parquet_file_params = ParquetFileParams {
            sequencer_id: seq.id,
            table_id: t.id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(513),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(2),
            file_size_bytes: 0,
            parquet_metadata: vec![],
            row_count: 0,
            created_at: Timestamp::new(1),
        };
        let p1 = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let ti = repos
            .tables()
            .get_table_persist_info(seq.id, t.namespace_id, &t.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ti,
            TablePersistInfo {
                sequencer_id: seq.id,
                table_id: t.id,
                parquet_max_sequence_number: Some(p1.max_sequence_number),
                tombstone_max_sequence_number: None
            }
        );

        // and with another parquet file persisted
        let parquet_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(514),
            max_sequence_number: SequenceNumber::new(1008),
            ..parquet_file_params
        };
        let p1 = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let ti = repos
            .tables()
            .get_table_persist_info(seq.id, t.namespace_id, &t.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ti,
            TablePersistInfo {
                sequencer_id: seq.id,
                table_id: t.id,
                parquet_max_sequence_number: Some(p1.max_sequence_number),
                tombstone_max_sequence_number: None
            }
        );

        // and now with a tombstone persisted
        let tombstone = repos
            .tombstones()
            .create_or_get(
                t.id,
                seq.id,
                SequenceNumber::new(2001),
                Timestamp::new(1),
                Timestamp::new(10),
                "wahtevs",
            )
            .await
            .unwrap();
        let ti = repos
            .tables()
            .get_table_persist_info(seq.id, t.namespace_id, &t.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            ti,
            TablePersistInfo {
                sequencer_id: seq.id,
                table_id: t.id,
                parquet_max_sequence_number: Some(p1.max_sequence_number),
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

        let mut want = vec![c, ccc];
        want.extend(cols3);
        assert_eq!(want, columns);

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

        // update the number
        repos
            .sequencers()
            .update_min_unpersisted_sequence_number(sequencer.id, SequenceNumber::new(53))
            .await
            .unwrap();
        let updated_sequencer = repos
            .sequencers()
            .create_or_get(&kafka, kafka_partition)
            .await
            .unwrap();
        assert_eq!(updated_sequencer.id, sequencer.id);
        assert_eq!(updated_sequencer.min_unpersisted_sequence_number, 53);

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

        // test list_by_namespace
        let namespace2 = repos
            .namespaces()
            .create("namespace_partition_test2", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table2 = repos
            .tables()
            .create_or_get("test_table2", namespace2.id)
            .await
            .unwrap();
        repos
            .partitions()
            .create_or_get("some_key", sequencer.id, table2.id)
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
            .chain(std::iter::once((other_partition.id, other_partition)))
            .collect();
        assert_eq!(expected, listed);
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
        assert_eq!(vec![t2.clone(), t3.clone()], listed);

        // test list_by_table
        let listed = repos.tombstones().list_by_table(table.id).await.unwrap();
        assert_eq!(vec![t1, t3], listed);
        let listed = repos
            .tombstones()
            .list_by_table(other_table.id)
            .await
            .unwrap();
        assert_eq!(vec![t2], listed);

        // test list_by_namespace
        let namespace2 = repos
            .namespaces()
            .create("namespace_tombstone_test2", "inf", kafka.id, pool.id)
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
                sequencer.id,
                SequenceNumber::new(1),
                min_time,
                max_time,
                "whatevs",
            )
            .await
            .unwrap();
        let t5 = repos
            .tombstones()
            .create_or_get(
                table2.id,
                sequencer.id,
                SequenceNumber::new(2),
                min_time,
                max_time,
                "foo",
            )
            .await
            .unwrap();
        let listed = repos
            .tombstones()
            .list_by_namespace(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![t4, t5], listed);
        let listed = repos
            .tombstones()
            .list_by_namespace(NamespaceId::new(i32::MAX))
            .await
            .unwrap();
        assert_eq!(Vec::<Tombstone>::new(), listed);
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

        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            parquet_metadata: b"md1".to_vec(),
            row_count: 0,
            created_at: Timestamp::new(1),
        };
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

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
            min_sequence_number: SequenceNumber::new(45),
            max_sequence_number: SequenceNumber::new(200),
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
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(vec![parquet_file.clone(), other_file.clone()], files);
        let files = repos
            .parquet_files()
            .list_by_sequencer_greater_than(sequencer.id, SequenceNumber::new(150))
            .await
            .unwrap();
        assert_eq!(vec![other_file.clone()], files);

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
        assert_eq!(files, vec![other_file]);

        // test list_by_namespace_not_to_delete
        let namespace2 = repos
            .namespaces()
            .create("namespace_parquet_file_test1", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table2 = repos
            .tables()
            .create_or_get("test_table2", namespace2.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("foo", sequencer.id, table2.id)
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
            ..parquet_file_params
        };
        let f1 = repos
            .parquet_files()
            .create(f1_params.clone())
            .await
            .unwrap();

        let f2_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..f1_params
        };
        let f2 = repos.parquet_files().create(f2_params).await.unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f2.clone()], files);

        repos.parquet_files().flag_for_delete(f2.id).await.unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone()], files);

        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(NamespaceId::new(i32::MAX))
            .await
            .unwrap();
        assert!(files.is_empty());
    }

    async fn test_parquet_file_compaction_level_0(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_parquet_file_compaction_level_0_test",
                "inf",
                kafka.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(100))
            .await
            .unwrap();
        let other_sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(101))
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .create_or_get("one", sequencer.id, table.id)
            .await
            .unwrap();

        let min_time = Timestamp::new(1);
        let max_time = Timestamp::new(10);

        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(140),
            min_time,
            max_time,
            file_size_bytes: 1337,
            parquet_metadata: b"md1".to_vec(),
            row_count: 0,
            created_at: Timestamp::new(1),
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();

        // Create a compaction level 0 file for some other sequencer
        let other_sequencer_params = ParquetFileParams {
            sequencer_id: other_sequencer.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };

        let _other_sequencer_file = repos
            .parquet_files()
            .create(other_sequencer_params)
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

        // Level 0 parquet files for a sequencer should contain only those that match the right
        // criteria
        let level_0 = repos.parquet_files().level_0(sequencer.id).await.unwrap();
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
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_parquet_file_compaction_level_1_test",
                "inf",
                kafka.id,
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
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(100))
            .await
            .unwrap();
        let other_sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(101))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one", sequencer.id, table.id)
            .await
            .unwrap();
        let other_partition = repos
            .partitions()
            .create_or_get("two", sequencer.id, table.id)
            .await
            .unwrap();

        // Set up the window of times we're interested in level 1 files for
        let query_min_time = Timestamp::new(5);
        let query_max_time = Timestamp::new(10);

        // Create a file with times entirely within the window
        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(140),
            min_time: query_min_time + 1,
            max_time: query_max_time - 1,
            file_size_bytes: 1337,
            parquet_metadata: b"md1".to_vec(),
            row_count: 0,
            created_at: Timestamp::new(1),
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

        // Create a file for some other sequencer
        let other_sequencer_params = ParquetFileParams {
            sequencer_id: other_sequencer.id,
            object_store_id: Uuid::new_v4(),
            ..parquet_file_params.clone()
        };
        let other_sequencer_file = repos
            .parquet_files()
            .create(other_sequencer_params)
            .await
            .unwrap();

        // Create a file for the same sequencer but a different table
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

        // Create a file for the same sequencer and table but a different partition
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
                other_sequencer_file.id,
                other_table_file.id,
                other_partition_file.id,
                to_delete_file.id,
            ])
            .await
            .unwrap();

        // Level 1 parquet files for a sequencer should contain only those that match the right
        // criteria
        let table_partition = TablePartition::new(sequencer.id, table.id, partition.id);
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

    async fn test_update_to_compaction_level_1(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(
                "namespace_update_to_compaction_level_1_test",
                "inf",
                kafka.id,
                pool.id,
            )
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("update_table", namespace.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1000))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one", sequencer.id, table.id)
            .await
            .unwrap();

        // Set up the window of times we're interested in level 1 files for
        let query_min_time = Timestamp::new(5);
        let query_max_time = Timestamp::new(10);

        // Create a file with times entirely within the window
        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(140),
            min_time: query_min_time + 1,
            max_time: query_max_time - 1,
            file_size_bytes: 1337,
            parquet_metadata: b"md1".to_vec(),
            row_count: 0,
            created_at: Timestamp::new(1),
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
        let level_0 = repos.parquet_files().level_0(sequencer.id).await.unwrap();
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
        let level_0 = repos.parquet_files().level_0(sequencer.id).await.unwrap();
        let mut level_0_ids: Vec<_> = level_0.iter().map(|pf| pf.id).collect();
        level_0_ids.sort();
        let mut expected_ids: Vec<_> = expected.iter().map(|pf| pf.id).collect();
        expected_ids.sort();
        assert_eq!(
            level_0_ids, expected_ids,
            "\nlevel 0: {:#?}\nexpected: {:#?}",
            level_0, expected,
        );

        // Level 1 parquet files for a sequencer should only contain parquet_file
        let expected = vec![parquet_file];
        let table_partition = TablePartition::new(sequencer.id, table.id, partition.id);
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

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>("catalog_op_duration_ms")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.buckets.iter().fold(0, |acc, v| acc + v.count);
        assert!(hit_count > 1, "metric did not record any calls");
    }
}
