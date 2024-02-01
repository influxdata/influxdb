//! Traits and data types for the IOx Catalog API.

use async_trait::async_trait;
use data_types::snapshot::partition::PartitionSnapshot;
use data_types::snapshot::table::TableSnapshot;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    Column, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionKey, SkippedCompaction,
    SortKeyIds, Table, TableId, Timestamp,
};
use iox_time::TimeProvider;
use snafu::Snafu;
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

/// An error wrapper detailing the reason for a compare-and-swap failure.
#[derive(Debug)]
pub enum CasFailure<T> {
    /// The compare-and-swap failed because the current value differers from the
    /// comparator.
    ///
    /// Contains the new current value.
    ValueMismatch(T),
    /// A query error occurred.
    QueryError(Error),
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("unhandled external error: {source}"))]
    External {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("already exists: {descr}"))]
    AlreadyExists { descr: String },

    #[snafu(display("limit exceeded: {descr}"))]
    LimitExceeded { descr: String },

    #[snafu(display("not found: {descr}"))]
    NotFound { descr: String },
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::External {
            source: Box::new(e),
        }
    }
}

impl From<sqlx::migrate::MigrateError> for Error {
    fn from(e: sqlx::migrate::MigrateError) -> Self {
        Self::from(sqlx::Error::from(e))
    }
}

impl From<data_types::snapshot::partition::Error> for Error {
    fn from(e: data_types::snapshot::partition::Error) -> Self {
        Self::External {
            source: Box::new(e),
        }
    }
}

impl From<data_types::snapshot::table::Error> for Error {
    fn from(e: data_types::snapshot::table::Error) -> Self {
        Self::External {
            source: Box::new(e),
        }
    }
}

impl From<catalog_cache::api::quorum::Error> for Error {
    fn from(e: catalog_cache::api::quorum::Error) -> Self {
        Self::External {
            source: Box::new(e),
        }
    }
}

impl From<generated_types::prost::DecodeError> for Error {
    fn from(e: generated_types::prost::DecodeError) -> Self {
        Self::External {
            source: Box::new(e),
        }
    }
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Specify how soft-deleted entities should affect query results.
///
/// ```text
///
///                ExcludeDeleted          OnlyDeleted
///
///                       ┃                     ┃
///                 .─────╋─────.         .─────╋─────.
///              ,─'      ┃      '─.   ,─'      ┃      '─.
///            ,'         ●         `,'         ●         `.
///          ,'                    ,' `.                    `.
///         ;                     ;     :                     :
///         │      No deleted     │     │   Only deleted      │
///         │         rows        │  ●  │       rows          │
///         :                     :  ┃  ;                     ;
///          ╲                     ╲ ┃ ╱                     ╱
///           `.                    `┃'                    ,'
///             `.                 ,'┃`.                 ,'
///               '─.           ,─'  ┃  '─.           ,─'
///                  `─────────'     ┃     `─────────'
///                                  ┃
///
///                               AllRows
///
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoftDeletedRows {
    /// Return all rows.
    AllRows,

    /// Return all rows, except soft deleted rows.
    ExcludeDeleted,

    /// Return only soft deleted rows.
    OnlyDeleted,
}

impl SoftDeletedRows {
    pub(crate) fn as_sql_predicate(&self) -> &str {
        match self {
            Self::ExcludeDeleted => "deleted_at IS NULL",
            Self::OnlyDeleted => "deleted_at IS NOT NULL",
            Self::AllRows => "1=1",
        }
    }
}

/// Methods for working with the catalog.
#[async_trait]
pub trait Catalog: Send + Sync + Debug + Display {
    /// Setup catalog for usage and apply possible migrations.
    async fn setup(&self) -> Result<(), Error>;

    /// Accesses the repositories without a transaction scope.
    fn repositories(&self) -> Box<dyn RepoCollection>;

    /// Gets metric registry associated with this catalog for testing purposes.
    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry>;

    /// Gets the time provider associated with this catalog.
    fn time_provider(&self) -> Arc<dyn TimeProvider>;
}

/// Methods for working with the catalog's various repositories (collections of entities).
///
/// # Repositories
///
/// The methods (e.g. `create_*` or `get_by_*`) for handling entities (namespaces, partitions,
/// etc.) are grouped into *repositories* with one repository per entity. A repository can be
/// thought of a collection of a single kind of entity. Getting repositories from the transaction
/// is cheap.
///
/// A repository might internally map to a wide range of different storage abstractions, ranging
/// from one or more SQL tables over key-value key spaces to simple in-memory vectors. The user
/// should and must not care how these are implemented.
pub trait RepoCollection: Send + Sync + Debug {
    /// Repository for [namespaces](data_types::Namespace).
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo;

    /// Repository for [tables](data_types::Table).
    fn tables(&mut self) -> &mut dyn TableRepo;

    /// Repository for [columns](data_types::Column).
    fn columns(&mut self) -> &mut dyn ColumnRepo;

    /// Repository for [partitions](data_types::Partition).
    fn partitions(&mut self) -> &mut dyn PartitionRepo;

    /// Repository for [Parquet files](data_types::ParquetFile).
    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo;
}

/// Functions for working with namespaces in the catalog
#[async_trait]
pub trait NamespaceRepo: Send + Sync {
    /// Creates the namespace in the catalog. If one by the same name already exists, an
    /// error is returned.
    /// Specify `None` for `retention_period_ns` to get infinite retention.
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace>;

    /// Update retention period for a namespace
    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace>;

    /// List all namespaces.
    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>>;

    /// Gets the namespace by its ID.
    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>>;

    /// Gets the namespace by its unique name.
    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>>;

    /// Soft-delete a namespace by name
    async fn soft_delete(&mut self, name: &str) -> Result<()>;

    /// Update the limit on the number of tables that can exist per namespace.
    async fn update_table_limit(&mut self, name: &str, new_max: MaxTables) -> Result<Namespace>;

    /// Update the limit on the number of columns that can exist per table in a given namespace.
    async fn update_column_limit(
        &mut self,
        name: &str,
        new_max: MaxColumnsPerTable,
    ) -> Result<Namespace>;
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo: Send + Sync {
    /// Creates the table in the catalog. If one in the same namespace with the same name already
    /// exists, an error is returned.
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table>;

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

    /// Obtain a table snapshot
    async fn snapshot(&mut self, table_id: TableId) -> Result<TableSnapshot>;
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

    /// Perform a bulk upsert of columns specified by a map of column name to column type.
    ///
    /// Implementations make no guarantees as to the ordering or atomicity of
    /// the batch of column upsert operations - a batch upsert may partially
    /// commit, in which case an error MUST be returned by the implementation.
    ///
    /// Per-namespace limits on the number of columns allowed per table are explicitly NOT checked
    /// by this function, hence the name containing `unchecked`. It is expected that the caller
    /// will check this first-- and yes, this is racy.
    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>>;

    /// List all columns for the given table ID.
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>>;

    /// List all columns.
    async fn list(&mut self) -> Result<Vec<Column>>;
}

/// Extension trait for [`ParquetFileRepo`]
#[async_trait]
pub trait PartitionRepoExt {
    /// create the parquet file
    async fn get_by_id(self, partition_id: PartitionId) -> Result<Option<Partition>>;
}

#[async_trait]
impl PartitionRepoExt for &mut dyn PartitionRepo {
    async fn get_by_id(self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let iter = self.get_by_id_batch(&[partition_id]).await?;
        Ok(iter.into_iter().next())
    }
}

/// Functions for working with IOx partitions in the catalog. These are how IOx splits up
/// data within a namespace.
#[async_trait]
pub trait PartitionRepo: Send + Sync {
    /// create or get a partition record for the given partition key and table
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;

    /// get multiple partitions by ID.
    ///
    /// the output order is undefined, non-existing partitions are not part of the output.
    async fn get_by_id_batch(&mut self, partition_ids: &[PartitionId]) -> Result<Vec<Partition>>;

    /// return the partitions by table id
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;

    /// return all partitions IDs
    async fn list_ids(&mut self) -> Result<Vec<PartitionId>>;

    /// Update the sort key for the partition, setting it to `new_sort_key_ids` iff
    /// the current value matches `old_sort_key_ids`.
    ///
    /// NOTE: it is expected that ONLY the ingesters update sort keys for
    /// existing partitions.
    ///
    /// # Spurious failure
    ///
    /// Implementations are allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons in the presence of
    /// concurrent writers.
    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key_ids: Option<&SortKeyIds>,
        new_sort_key_ids: &SortKeyIds,
    ) -> Result<Partition, CasFailure<SortKeyIds>>;

    /// Record an instance of a partition being selected for compaction but compaction was not
    /// completed for the specified reason.
    #[allow(clippy::too_many_arguments)]
    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()>;

    /// Get the record of partitions being skipped.
    async fn get_in_skipped_compactions(
        &mut self,
        partition_id: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>>;

    /// List the records of compacting a partition being skipped. This is mostly useful for testing.
    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>>;

    /// Delete the records of skipping a partition being compacted.
    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>>;

    /// Return the N most recently created partitions.
    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>>;

    /// Select partitions with a `new_file_at` value greater than the minimum time value and, if specified, less than
    /// the maximum time value. Both range ends are exclusive; a timestamp exactly equal to either end will _not_ be
    /// included in the results.
    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>>;

    /// Return all partitions that do not have deterministic hash IDs in the catalog. Used in
    /// the ingester's `OldPartitionBloomFilter` to determine whether a catalog query is necessary.
    /// Can be removed when all partitions have hash IDs and support for old-style partitions is no
    /// longer needed.
    async fn list_old_style(&mut self) -> Result<Vec<Partition>>;

    /// Obtain a partition snapshot
    async fn snapshot(&mut self, partition_id: PartitionId) -> Result<PartitionSnapshot>;
}

/// Extension trait for [`ParquetFileRepo`]
#[async_trait]
pub trait ParquetFileRepoExt {
    /// create the parquet file
    async fn create(self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;
}

#[async_trait]
impl ParquetFileRepoExt for &mut dyn ParquetFileRepo {
    /// create the parquet file
    async fn create(self, params: ParquetFileParams) -> Result<ParquetFile> {
        let files = self
            .create_upgrade_delete(
                params.partition_id,
                &[],
                &[],
                &[params.clone()],
                CompactionLevel::Initial,
            )
            .await?;
        let id = files.into_iter().next().unwrap();
        Ok(ParquetFile::from_params(params, id))
    }
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// Flag all parquet files for deletion that are older than their namespace's retention period.
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<(PartitionId, ObjectStoreId)>>;

    /// Delete parquet files that were marked to be deleted earlier than the specified time.
    ///
    /// Returns the deleted IDs only.
    ///
    /// This deletion is limited to a certain (backend-specific) number of files to avoid overlarge
    /// changes. The caller MAY call this method again if the result was NOT empty.
    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ObjectStoreId>>;

    /// List parquet files for given partitions that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    ///
    /// The output order is undefined, non-existing partitions are not part of the output.
    async fn list_by_partition_not_to_delete_batch(
        &mut self,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Vec<ParquetFile>>;

    /// Return the parquet file with the given object store id
    // used heavily in tests for verification of catalog state.
    async fn get_by_object_store_id(
        &mut self,
        object_store_id: ObjectStoreId,
    ) -> Result<Option<ParquetFile>>;

    /// Test a batch of parquet files exist by object store ids
    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<ObjectStoreId>,
    ) -> Result<Vec<ObjectStoreId>>;

    /// Commit deletions, upgrades and creations in a single transaction.
    ///
    /// Returns IDs of created files.
    async fn create_upgrade_delete(
        &mut self,
        partition_id: PartitionId,
        delete: &[ObjectStoreId],
        upgrade: &[ObjectStoreId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>>;
}
