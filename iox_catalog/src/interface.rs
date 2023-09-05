//! Traits and data types for the IOx Catalog API.

use async_trait::async_trait;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    Column, ColumnType, ColumnsByName, CompactionLevel, Namespace, NamespaceId, NamespaceName,
    NamespaceSchema, NamespaceServiceProtectionLimitsOverride, ParquetFile, ParquetFileId,
    ParquetFileParams, Partition, PartitionHashId, PartitionId, PartitionKey, SkippedCompaction,
    SortedColumnSet, Table, TableId, TableSchema, Timestamp, TransitionPartitionId,
};
use iox_time::TimeProvider;
use snafu::{OptionExt, Snafu};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{Debug, Display},
    sync::Arc,
};
use uuid::Uuid;

/// Maximum number of files touched by [`ParquetFileRepo::flag_for_delete_by_retention`] at a time.
pub const MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION: i64 = 1_000;
/// Maximum number of files touched by [`ParquetFileRepo::delete_old_ids_only`] at a time.
pub const MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE: i64 = 10_000;

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
#[allow(missing_copy_implementations, missing_docs)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("invalid name: {}", name))]
    InvalidName { name: String },

    #[snafu(display("name {} already exists", name))]
    NameExists { name: String },

    #[snafu(display("A table named {name} already exists in namespace {namespace_id}"))]
    TableNameExists {
        name: String,
        namespace_id: NamespaceId,
    },

    #[snafu(display("unhandled sqlx error: {}", source))]
    SqlxError { source: sqlx::Error },

    #[snafu(display("foreign key violation: {}", source))]
    ForeignKeyViolation { source: sqlx::Error },

    #[snafu(display("column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
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
    PartitionNotFound { id: TransitionPartitionId },

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

    #[snafu(display("transaction failed to commit: {}", source))]
    FailedToCommit { source: sqlx::Error },

    #[snafu(display("error while converting usize {} to i64", value))]
    InvalidValue { value: usize },

    #[snafu(display("database setup error: {}", source))]
    Setup { source: sqlx::Error },

    #[snafu(display(
        "could not record a skipped compaction for partition {partition_id}: {source}"
    ))]
    CouldNotRecordSkippedCompaction {
        source: sqlx::Error,
        partition_id: PartitionId,
    },

    #[snafu(display("could not list skipped compactions: {source}"))]
    CouldNotListSkippedCompactions { source: sqlx::Error },

    #[snafu(display("could not delete skipped compactions: {source}"))]
    CouldNotDeleteSkippedCompactions { source: sqlx::Error },

    #[snafu(display("could not delete namespace: {source}"))]
    CouldNotDeleteNamespace { source: sqlx::Error },
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
#[derive(Debug, Clone, Copy)]
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
    async fn repositories(&self) -> Box<dyn RepoCollection>;

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
#[async_trait]
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
    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;

    /// Update the limit on the number of columns that can exist per table in a given namespace.
    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace>;
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

/// Functions for working with IOx partitions in the catalog. These are how IOx splits up
/// data within a namespace.
#[async_trait]
pub trait PartitionRepo: Send + Sync {
    /// create or get a partition record for the given partition key and table
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition>;

    /// get partition by ID
    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>>;

    /// get multiple partitions by ID.
    ///
    /// the output order is undefined, non-existing partitions are not part of the output.
    async fn get_by_id_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<Partition>>;

    /// get partition by deterministic hash ID
    async fn get_by_hash_id(
        &mut self,
        partition_hash_id: &PartitionHashId,
    ) -> Result<Option<Partition>>;

    /// get partition by deterministic hash ID
    ///
    /// the output order is undefined, non-existing partitions are not part of the output.
    async fn get_by_hash_id_batch(
        &mut self,
        partition_hash_ids: &[&PartitionHashId],
    ) -> Result<Vec<Partition>>;

    /// return the partitions by table id
    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>>;

    /// return all partitions IDs
    async fn list_ids(&mut self) -> Result<Vec<PartitionId>>;

    /// Update the sort key for the partition, setting it to `new_sort_key` iff
    /// the current value matches `old_sort_key`.
    ///
    /// NOTE: it is expected that ONLY the ingesters update sort keys for
    /// existing partitions.
    ///
    /// # Spurious failure
    ///
    /// Implementations are allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons in the presence of
    /// concurrent writers.
    // TODO: After the sort_key_ids field is converetd into NOT NULL, the implementation of this function
    // must be changed to compare old_sort_key_ids with the existing sort_key_ids instead of
    // comparing old_sort_key with existing sort_key
    async fn cas_sort_key(
        &mut self,
        partition_id: &TransitionPartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
        new_sort_key_ids: &SortedColumnSet,
    ) -> Result<Partition, CasFailure<Vec<String>>>;

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
}

/// Functions for working with parquet file pointers in the catalog
#[async_trait]
pub trait ParquetFileRepo: Send + Sync {
    /// create the parquet file
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile>;

    /// List all parquet files in implementation-defined, non-deterministic order.
    ///
    /// This includes files that were marked for deletion.
    ///
    /// This is mostly useful for testing and will likely not succeed in production.
    async fn list_all(&mut self) -> Result<Vec<ParquetFile>>;

    /// Flag all parquet files for deletion that are older than their namespace's retention period.
    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>>;

    /// List all parquet files within a given namespace that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>>;

    /// List all parquet files within a given table that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>>;

    /// Delete parquet files that were marked to be deleted earlier than the specified time.
    ///
    /// Returns the deleted IDs only.
    ///
    /// This deletion is limited to a certain (backend-specific) number of files to avoid overlarge
    /// changes. The caller MAY call this method again if the result was NOT empty.
    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>>;

    /// List parquet files for a given partition that are NOT marked as
    /// [`to_delete`](ParquetFile::to_delete).
    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: &TransitionPartitionId,
    ) -> Result<Vec<ParquetFile>>;

    /// Return the parquet file with the given object store id
    // used heavily in tests for verification of catalog state.
    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>>;

    /// Test a batch of parquet files exist by object store ids
    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<Uuid>,
    ) -> Result<Vec<Uuid>>;

    /// Commit deletions, upgrades and creations in a single transaction.
    ///
    /// Returns IDs of created files.
    async fn create_upgrade_delete(
        &mut self,
        delete: &[ParquetFileId],
        upgrade: &[ParquetFileId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>>;
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_id<R>(
    id: NamespaceId,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .get_by_id(id, deleted)
        .await?
        .context(NamespaceNotFoundByIdSnafu { id })?;

    get_schema_internal(namespace, repos).await
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_name<R>(
    name: &str,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<NamespaceSchema>
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .get_by_name(name, deleted)
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

    let mut namespace = NamespaceSchema::new_empty_from(&namespace);

    let mut table_id_to_schema = BTreeMap::new();
    for t in tables {
        let table_schema = TableSchema::new_empty_from(&t);
        table_id_to_schema.insert(t.id, (t.name, table_schema));
    }

    for c in columns {
        let (_, t) = table_id_to_schema.get_mut(&c.table_id).unwrap();
        t.add_column(c);
    }

    for (_, (table_name, schema)) in table_id_to_schema {
        namespace.tables.insert(table_name, schema);
    }

    Ok(namespace)
}

/// Gets all the table's columns.
pub async fn get_table_columns_by_id<R>(id: TableId, repos: &mut R) -> Result<ColumnsByName>
where
    R: RepoCollection + ?Sized,
{
    let columns = repos.columns().list_by_table_id(id).await?;

    Ok(ColumnsByName::new(columns))
}

/// Fetch all [`NamespaceSchema`] in the catalog.
///
/// This method performs the minimal number of queries needed to build the
/// result set. No table lock is obtained, nor are queries executed within a
/// transaction, but this method does return a point-in-time snapshot of the
/// catalog state.
///
/// # Soft Deletion
///
/// No schemas for soft-deleted namespaces are returned.
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
    //
    // This approach also tolerates concurrently deleted namespaces, which are
    // simply ignored at the end when joining to the namespace query result.

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
    let namespaces = tokio::spawn(async move {
        repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
    });

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
            .or_insert_with(|| TableSchema::new_empty_from(table));

        table_schema.add_column(column);
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
            // The catalog call explicitly asked for no soft deleted records.
            assert!(v.deleted_at.is_none());

            let mut ns = NamespaceSchema::new_empty_from(&v);

            ns.tables = joined.remove(&v.id)?;
            Some((v, ns))
        });

    Ok(iter)
}

/// panic if sort_key and sort_key_ids have different lengths
pub(crate) fn verify_sort_key_length(sort_key: &[&str], sort_key_ids: &SortedColumnSet) {
    assert_eq!(
        sort_key.len(),
        sort_key_ids.len(),
        "sort_key {:?} and sort_key_ids {:?} are not the same length",
        sort_key,
        sort_key_ids
    );
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use crate::{
        test_helpers::{arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table},
        validate_or_insert_schema, DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES,
    };

    use super::*;
    use ::test_helpers::assert_error;
    use assert_matches::assert_matches;
    use data_types::{ColumnId, CompactionLevel};
    use futures::Future;
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use metric::{Attributes, DurationHistogram, Metric};
    use std::{collections::BTreeSet, ops::DerefMut, sync::Arc, time::Duration};

    pub(crate) async fn test_catalog<R, F>(clean_state: R)
    where
        R: Fn() -> F + Send + Sync,
        F: Future<Output = Arc<dyn Catalog>> + Send,
    {
        test_setup(clean_state().await).await;
        test_namespace_soft_deletion(clean_state().await).await;
        test_partitions_new_file_between(clean_state().await).await;
        test_column(clean_state().await).await;
        test_partition(clean_state().await).await;
        test_parquet_file(clean_state().await).await;
        test_parquet_file_delete_broken(clean_state().await).await;
        test_update_to_compaction_level_1(clean_state().await).await;
        test_list_by_partiton_not_to_delete(clean_state().await).await;
        test_list_schemas(clean_state().await).await;
        test_list_schemas_soft_deleted_rows(clean_state().await).await;
        test_delete_namespace(clean_state().await).await;

        let catalog = clean_state().await;
        test_namespace(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "namespace_create");

        let catalog = clean_state().await;
        test_table(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "table_create");

        let catalog = clean_state().await;
        test_column(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "column_create_or_get");

        let catalog = clean_state().await;
        test_partition(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "partition_create_or_get");

        let catalog = clean_state().await;
        test_parquet_file(Arc::clone(&catalog)).await;
        assert_metric_hit(&catalog.metrics(), "parquet_create");
    }

    async fn test_setup(catalog: Arc<dyn Catalog>) {
        catalog.setup().await.expect("first catalog setup");
        catalog.setup().await.expect("second catalog setup");
    }

    async fn test_namespace(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace_name = NamespaceName::new("test_namespace").unwrap();
        let namespace = repos
            .namespaces()
            .create(&namespace_name, None, None, None)
            .await
            .unwrap();
        assert!(namespace.id > NamespaceId::new(0));
        assert_eq!(namespace.name, namespace_name.as_str());
        assert_eq!(
            namespace.partition_template,
            NamespacePartitionTemplateOverride::default()
        );
        let lookup_namespace = repos
            .namespaces()
            .get_by_name(&namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(namespace, lookup_namespace);

        // Assert default values for service protection limits.
        assert_eq!(namespace.max_tables, DEFAULT_MAX_TABLES);
        assert_eq!(
            namespace.max_columns_per_table,
            DEFAULT_MAX_COLUMNS_PER_TABLE
        );

        let conflict = repos
            .namespaces()
            .create(&namespace_name, None, None, None)
            .await;
        assert!(matches!(
            conflict.unwrap_err(),
            Error::NameExists { name: _ }
        ));

        let found = repos
            .namespaces()
            .get_by_id(namespace.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_id(NamespaceId::new(i64::MAX), SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(not_found.is_none());

        let found = repos
            .namespaces()
            .get_by_name(&namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .expect("namespace should be there");
        assert_eq!(namespace, found);

        let not_found = repos
            .namespaces()
            .get_by_name("does_not_exist", SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(not_found.is_none());

        let namespace2 = arbitrary_namespace(&mut *repos, "test_namespace2").await;
        let mut namespaces = repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        namespaces.sort_by_key(|ns| ns.name.clone());
        assert_eq!(namespaces, vec![namespace, namespace2]);

        const NEW_TABLE_LIMIT: i32 = 15000;
        let modified = repos
            .namespaces()
            .update_table_limit(namespace_name.as_str(), NEW_TABLE_LIMIT)
            .await
            .expect("namespace should be updateable");
        assert_eq!(NEW_TABLE_LIMIT, modified.max_tables);

        const NEW_COLUMN_LIMIT: i32 = 1500;
        let modified = repos
            .namespaces()
            .update_column_limit(namespace_name.as_str(), NEW_COLUMN_LIMIT)
            .await
            .expect("namespace should be updateable");
        assert_eq!(NEW_COLUMN_LIMIT, modified.max_columns_per_table);

        const NEW_RETENTION_PERIOD_NS: i64 = 5 * 60 * 60 * 1000 * 1000 * 1000;
        let modified = repos
            .namespaces()
            .update_retention_period(namespace_name.as_str(), Some(NEW_RETENTION_PERIOD_NS))
            .await
            .expect("namespace should be updateable");
        assert_eq!(
            NEW_RETENTION_PERIOD_NS,
            modified.retention_period_ns.unwrap()
        );

        let modified = repos
            .namespaces()
            .update_retention_period(namespace_name.as_str(), None)
            .await
            .expect("namespace should be updateable");
        assert!(modified.retention_period_ns.is_none());

        // create namespace with retention period NULL (the default)
        let namespace3 = arbitrary_namespace(&mut *repos, "test_namespace3").await;
        assert!(namespace3.retention_period_ns.is_none());

        // create namespace with retention period
        let namespace4_name = NamespaceName::new("test_namespace4").unwrap();
        let namespace4 = repos
            .namespaces()
            .create(&namespace4_name, None, Some(NEW_RETENTION_PERIOD_NS), None)
            .await
            .expect("namespace with 5-hour retention should be created");
        assert_eq!(
            NEW_RETENTION_PERIOD_NS,
            namespace4.retention_period_ns.unwrap()
        );
        // reset retention period to NULL to avoid affecting later tests
        repos
            .namespaces()
            .update_retention_period(&namespace4_name, None)
            .await
            .expect("namespace should be updateable");

        // create a namespace with a PartitionTemplate other than the default
        let tag_partition_template =
            NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("tag1".into())),
                }],
            })
            .unwrap();
        let namespace5_name = NamespaceName::new("test_namespace5").unwrap();
        let namespace5 = repos
            .namespaces()
            .create(
                &namespace5_name,
                Some(tag_partition_template.clone()),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(namespace5.partition_template, tag_partition_template);
        let lookup_namespace5 = repos
            .namespaces()
            .get_by_name(&namespace5_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(namespace5, lookup_namespace5);

        // remove namespace to avoid it from affecting later tests
        repos
            .namespaces()
            .soft_delete("test_namespace")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("test_namespace2")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("test_namespace3")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("test_namespace4")
            .await
            .expect("delete namespace should succeed");
    }

    /// Construct a set of two namespaces:
    ///
    ///  * deleted-ns: marked as soft-deleted
    ///  * active-ns: not marked as deleted
    ///
    /// And assert the expected "soft delete" semantics / correctly filter out
    /// the expected rows for all three states of [`SoftDeletedRows`].
    async fn test_namespace_soft_deletion(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;

        let deleted_ns = arbitrary_namespace(&mut *repos, "deleted-ns").await;
        let active_ns = arbitrary_namespace(&mut *repos, "active-ns").await;

        // Mark "deleted-ns" as soft-deleted.
        repos.namespaces().soft_delete("deleted-ns").await.unwrap();

        // Which should be idempotent (ignoring the timestamp change - when
        // changing this to "soft delete" it was idempotent, so I am preserving
        // that).
        repos.namespaces().soft_delete("deleted-ns").await.unwrap();

        // Listing should respect soft deletion.
        let got = repos
            .namespaces()
            .list(SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns", "active-ns"]);

        let got = repos
            .namespaces()
            .list(SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns"]);

        let got = repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);

        // As should get by ID
        let got = repos
            .namespaces()
            .get_by_id(deleted_ns.id, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_id(deleted_ns.id, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| {
                assert!(v.deleted_at.is_some());
                v.name
            });
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_id(deleted_ns.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_id(active_ns.id, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);
        let got = repos
            .namespaces()
            .get_by_id(active_ns.id, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_id(active_ns.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);

        // And get by name
        let got = repos
            .namespaces()
            .get_by_name(&deleted_ns.name, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_name(&deleted_ns.name, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| {
                assert!(v.deleted_at.is_some());
                v.name
            });
        assert_string_set_eq(got, ["deleted-ns"]);
        let got = repos
            .namespaces()
            .get_by_name(&deleted_ns.name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_name(&active_ns.name, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);
        let got = repos
            .namespaces()
            .get_by_name(&active_ns.name, SoftDeletedRows::OnlyDeleted)
            .await
            .unwrap();
        assert!(got.is_none());
        let got = repos
            .namespaces()
            .get_by_name(&active_ns.name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.name);
        assert_string_set_eq(got, ["active-ns"]);
    }

    // Assert the set of strings "a" is equal to the set "b", tolerating
    // duplicates.
    #[track_caller]
    fn assert_string_set_eq<T, U>(a: impl IntoIterator<Item = T>, b: impl IntoIterator<Item = U>)
    where
        T: Into<String>,
        U: Into<String>,
    {
        let mut a = a.into_iter().map(Into::into).collect::<Vec<String>>();
        a.sort_unstable();
        let mut b = b.into_iter().map(Into::into).collect::<Vec<String>>();
        b.sort_unstable();
        assert_eq!(a, b);
    }

    async fn test_table(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "namespace_table_test").await;

        // test we can create a table
        let t = arbitrary_table(&mut *repos, "test_table", &namespace).await;
        assert!(t.id > TableId::new(0));
        assert_eq!(
            t.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // The default template doesn't use any tag values, so no columns need to be created.
        let table_columns = repos.columns().list_by_table_id(t.id).await.unwrap();
        assert!(table_columns.is_empty());

        // test we get an error if we try to create it again
        let err = repos
            .tables()
            .create(
                "test_table",
                TablePartitionTemplateOverride::try_new(None, &namespace.partition_template)
                    .unwrap(),
                namespace.id,
            )
            .await;
        assert_error!(
            err,
            Error::TableNameExists { ref name, namespace_id }
                if name == "test_table" && namespace_id == namespace.id
        );

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
        let namespace2 = arbitrary_namespace(&mut *repos, "two").await;
        assert_ne!(namespace, namespace2);
        let test_table = arbitrary_table(&mut *repos, "test_table", &namespace2).await;
        assert_ne!(t.id, test_table.id);
        assert_eq!(test_table.namespace_id, namespace2.id);

        // test get by namespace and name
        let foo_table = arbitrary_table(&mut *repos, "foo", &namespace2).await;
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
        let mut list = repos.tables().list().await.unwrap();
        list.sort_by_key(|t| t.id);
        let mut expected = [t, test_table, foo_table];
        expected.sort_by_key(|t| t.id);
        assert_eq!(&list, &expected);

        // test per-namespace table limits
        let latest = repos
            .namespaces()
            .update_table_limit("namespace_table_test", 1)
            .await
            .expect("namespace should be updateable");
        let err = repos
            .tables()
            .create(
                "definitely_unique",
                TablePartitionTemplateOverride::try_new(None, &latest.partition_template).unwrap(),
                latest.id,
            )
            .await
            .expect_err("should error with table create limit error");
        assert!(matches!(
            err,
            Error::TableCreateLimitError {
                table_name: _,
                namespace_id: _
            }
        ));

        // Create a table with a partition template other than the default
        let custom_table_template = TablePartitionTemplateOverride::try_new(
            Some(proto::PartitionTemplate {
                parts: vec![
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue("tag1".into())),
                    },
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                    },
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue("tag2".into())),
                    },
                ],
            }),
            &namespace2.partition_template,
        )
        .unwrap();
        let templated = repos
            .tables()
            .create(
                "use_a_template",
                custom_table_template.clone(),
                namespace2.id,
            )
            .await
            .unwrap();
        assert_eq!(templated.partition_template, custom_table_template);

        // Tag columns should be created for tags used in the template
        let table_columns = repos
            .columns()
            .list_by_table_id(templated.id)
            .await
            .unwrap();
        assert_eq!(table_columns.len(), 2);
        assert!(table_columns.iter().all(|c| c.is_tag()));
        let mut column_names: Vec<_> = table_columns.iter().map(|c| &c.name).collect();
        column_names.sort();
        assert_eq!(column_names, &["tag1", "tag2"]);

        let lookup_templated = repos
            .tables()
            .get_by_namespace_and_name(namespace2.id, "use_a_template")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(templated, lookup_templated);

        // Create a namespace with a partition template other than the default
        let custom_namespace_template =
            NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                parts: vec![
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue("zzz".into())),
                    },
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue("aaa".into())),
                    },
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                    },
                ],
            })
            .unwrap();
        let custom_namespace_name = NamespaceName::new("custom_namespace").unwrap();
        let custom_namespace = repos
            .namespaces()
            .create(
                &custom_namespace_name,
                Some(custom_namespace_template.clone()),
                None,
                None,
            )
            .await
            .unwrap();
        // Create a table without specifying the partition template
        let custom_table_template =
            TablePartitionTemplateOverride::try_new(None, &custom_namespace.partition_template)
                .unwrap();
        let table_templated_by_namespace = repos
            .tables()
            .create(
                "use_namespace_template",
                custom_table_template,
                custom_namespace.id,
            )
            .await
            .unwrap();
        assert_eq!(
            table_templated_by_namespace.partition_template,
            TablePartitionTemplateOverride::try_new(None, &custom_namespace_template).unwrap()
        );

        // Tag columns should be created for tags used in the template
        let table_columns = repos
            .columns()
            .list_by_table_id(table_templated_by_namespace.id)
            .await
            .unwrap();
        assert_eq!(table_columns.len(), 2);
        assert!(table_columns.iter().all(|c| c.is_tag()));
        let mut column_names: Vec<_> = table_columns.iter().map(|c| &c.name).collect();
        column_names.sort();
        assert_eq!(column_names, &["aaa", "zzz"]);

        repos
            .namespaces()
            .soft_delete("namespace_table_test")
            .await
            .expect("delete namespace should succeed");
        repos
            .namespaces()
            .soft_delete("two")
            .await
            .expect("delete namespace should succeed");
    }

    async fn test_column(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "namespace_column_test").await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
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

        // test that attempting to create an already defined column of a different type returns
        // error
        let err = repos
            .columns()
            .create_or_get("column_test", table.id, ColumnType::U64)
            .await
            .expect_err("should error with wrong column type");
        assert!(matches!(err, Error::ColumnTypeMismatch { .. }));

        // test that we can create a column of the same name under a different table
        let table2 = arbitrary_table(&mut *repos, "test_table_2", &namespace).await;
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

        let mut want = vec![c.clone(), ccc];
        assert_eq!(want, columns);

        let columns = repos.columns().list_by_table_id(table.id).await.unwrap();

        let want2 = vec![c];
        assert_eq!(want2, columns);

        // Add another tag column into table2
        let c3 = repos
            .columns()
            .create_or_get("b", table2.id, ColumnType::Tag)
            .await
            .unwrap();

        // Listing columns should return all columns in the catalog
        let list = repos.columns().list().await.unwrap();
        want.extend([c3]);
        assert_eq!(list, want);

        // test create_or_get_many_unchecked, below column limit
        let mut columns = HashMap::new();
        columns.insert("column_test", ColumnType::Tag);
        columns.insert("new_column", ColumnType::Tag);
        let table1_columns = repos
            .columns()
            .create_or_get_many_unchecked(table.id, columns)
            .await
            .unwrap();
        let mut table1_column_names: Vec<_> = table1_columns.iter().map(|c| &c.name).collect();
        table1_column_names.sort();
        assert_eq!(table1_column_names, vec!["column_test", "new_column"]);

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

        // test per-namespace column limits are NOT enforced with create_or_get_many_unchecked
        let table3 = arbitrary_table(&mut *repos, "test_table_3", &namespace).await;
        let mut columns = HashMap::new();
        columns.insert("apples", ColumnType::Tag);
        columns.insert("oranges", ColumnType::Tag);
        let table3_columns = repos
            .columns()
            .create_or_get_many_unchecked(table3.id, columns)
            .await
            .unwrap();
        let mut table3_column_names: Vec<_> = table3_columns.iter().map(|c| &c.name).collect();
        table3_column_names.sort();
        assert_eq!(table3_column_names, vec!["apples", "oranges"]);

        repos
            .namespaces()
            .soft_delete("namespace_column_test")
            .await
            .expect("delete namespace should succeed");
    }

    async fn test_partition(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "namespace_partition_test").await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;

        let mut created = BTreeMap::new();
        // partition to use
        let partition = repos
            .partitions()
            .create_or_get("foo".into(), table.id)
            .await
            .expect("failed to create partition");
        // Test: sort_key_ids from create_or_get
        assert!(partition.sort_key_ids().unwrap().is_empty());
        created.insert(partition.id, partition.clone());
        // partition to use
        let partition_bar = repos
            .partitions()
            .create_or_get("bar".into(), table.id)
            .await
            .expect("failed to create partition");
        created.insert(partition_bar.id, partition_bar);
        // partition to be skipped later
        let to_skip_partition = repos
            .partitions()
            .create_or_get("asdf".into(), table.id)
            .await
            .unwrap();
        created.insert(to_skip_partition.id, to_skip_partition.clone());
        // partition to be skipped later
        let to_skip_partition_too = repos
            .partitions()
            .create_or_get("asdf too".into(), table.id)
            .await
            .unwrap();
        created.insert(to_skip_partition_too.id, to_skip_partition_too.clone());

        // partitions can be retrieved easily
        let mut created_sorted = created.values().cloned().collect::<Vec<_>>();
        created_sorted.sort_by_key(|p| p.id);
        assert_eq!(
            to_skip_partition,
            repos
                .partitions()
                .get_by_id(to_skip_partition.id)
                .await
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            to_skip_partition,
            repos
                .partitions()
                .get_by_hash_id(to_skip_partition.hash_id().unwrap())
                .await
                .unwrap()
                .unwrap()
        );
        let non_existing_partition_id = PartitionId::new(i64::MAX);
        let non_existing_partition_hash_id =
            PartitionHashId::new(TableId::new(i64::MAX), &PartitionKey::from("arbitrary"));
        assert!(repos
            .partitions()
            .get_by_id(non_existing_partition_id)
            .await
            .unwrap()
            .is_none());
        assert!(repos
            .partitions()
            .get_by_hash_id(&non_existing_partition_hash_id)
            .await
            .unwrap()
            .is_none());
        let mut batch = repos
            .partitions()
            .get_by_id_batch(
                created
                    .keys()
                    .cloned()
                    .chain([non_existing_partition_id])
                    .collect(),
            )
            .await
            .unwrap();
        batch.sort_by_key(|p| p.id);
        assert_eq!(created_sorted, batch);
        // Test: sort_key_ids from get_by_id_batch
        assert!(batch.iter().all(|p| p.sort_key_ids().unwrap().is_empty()));
        let mut batch = repos
            .partitions()
            .get_by_hash_id_batch(
                &created
                    .values()
                    .map(|p| p.hash_id().unwrap())
                    .chain([&non_existing_partition_hash_id])
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        batch.sort_by_key(|p| p.id);
        // Test: sort_key_ids from get_by_hash_id_batch
        assert!(batch.iter().all(|p| p.sort_key_ids().unwrap().is_empty()));
        assert_eq!(created_sorted, batch);

        let listed = repos
            .partitions()
            .list_by_table_id(table.id)
            .await
            .expect("failed to list partitions")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();
        // Test: sort_key_ids from list_by_table_id
        assert!(listed
            .values()
            .all(|p| p.sort_key_ids().unwrap().is_empty()));

        assert_eq!(created, listed);

        let listed = repos
            .partitions()
            .list_ids()
            .await
            .expect("failed to list partitions")
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(created.keys().copied().collect::<BTreeSet<_>>(), listed);

        // The code no longer supports creating old-style partitions, so this list is always empty
        // in these tests. See each catalog implementation for tests that insert old-style
        // partitions directly and verify they're returned.
        let old_style = repos.partitions().list_old_style().await.unwrap();
        assert!(
            old_style.is_empty(),
            "Expected no old-style partitions, got {old_style:?}"
        );

        // sort_key should be empty on creation
        assert!(to_skip_partition.sort_key.is_empty());

        // test update_sort_key from None to Some
        let updated_partition = repos
            .partitions()
            .cas_sort_key(
                &to_skip_partition.transition_partition_id(),
                None,
                &["tag2", "tag1", "time"],
                &SortedColumnSet::from([2, 1, 3]),
            )
            .await
            .unwrap();

        // verify sort key and sort key ids  are updated
        assert_eq!(updated_partition.sort_key, &["tag2", "tag1", "time"]);
        assert_eq!(
            updated_partition.sort_key_ids,
            Some(SortedColumnSet::from([2, 1, 3]))
        );

        // test sort key CAS with an incorrect value
        let err = repos
            .partitions()
            .cas_sort_key(
                &to_skip_partition.transition_partition_id(),
                Some(["bananas".to_string()].to_vec()),
                &["tag2", "tag1", "tag3 , with comma", "time"],
                &SortedColumnSet::from([1, 2, 3, 4]),
            )
            .await
            .expect_err("CAS with incorrect value should fail");
        assert_matches!(err, CasFailure::ValueMismatch(old) => {
            assert_eq!(old, &["tag2", "tag1", "time"]);
        });

        // test getting the new sort key
        let updated_other_partition = repos
            .partitions()
            .get_by_id(to_skip_partition.id)
            .await
            .unwrap()
            .unwrap();
        // still has the old sort key
        assert_eq!(
            updated_other_partition.sort_key,
            vec!["tag2", "tag1", "time"]
        );
        assert_eq!(
            updated_other_partition.sort_key_ids,
            Some(SortedColumnSet::from([2, 1, 3]))
        );

        let updated_other_partition = repos
            .partitions()
            .get_by_hash_id(to_skip_partition.hash_id().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_other_partition.sort_key,
            vec!["tag2", "tag1", "time"]
        );
        // Test: sort_key_ids from get_by_hash_id
        assert_eq!(
            updated_other_partition.sort_key_ids,
            Some(SortedColumnSet::from([2, 1, 3]))
        );

        // test sort key CAS with no value
        let err = repos
            .partitions()
            .cas_sort_key(
                &to_skip_partition.transition_partition_id(),
                None,
                &["tag2", "tag1", "tag3 , with comma", "time"],
                &SortedColumnSet::from([1, 2, 3, 4]),
            )
            .await
            .expect_err("CAS with incorrect value should fail");
        assert_matches!(err, CasFailure::ValueMismatch(old) => {
            assert_eq!(old, ["tag2", "tag1", "time"]);
        });

        // test sort key CAS with an incorrect value
        let err = repos
            .partitions()
            .cas_sort_key(
                &to_skip_partition.transition_partition_id(),
                Some(["bananas".to_string()].to_vec()),
                &["tag2", "tag1", "tag3 , with comma", "time"],
                &SortedColumnSet::from([1, 2, 3, 4]),
            )
            .await
            .expect_err("CAS with incorrect value should fail");
        assert_matches!(err, CasFailure::ValueMismatch(old) => {
            assert_eq!(old, ["tag2", "tag1", "time"]);
        });

        // test update_sort_key from Some value to Some other value
        let updated_partition = repos
            .partitions()
            .cas_sort_key(
                &to_skip_partition.transition_partition_id(),
                Some(
                    ["tag2", "tag1", "time"]
                        .into_iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
                &["tag2", "tag1", "tag3 , with comma", "time"],
                &SortedColumnSet::from([2, 1, 4, 3]),
            )
            .await
            .unwrap();
        assert_eq!(
            updated_partition.sort_key,
            vec!["tag2", "tag1", "tag3 , with comma", "time"]
        );
        assert_eq!(
            updated_partition.sort_key_ids,
            Some(SortedColumnSet::from([2, 1, 4, 3]))
        );

        // test getting the new sort key
        let updated_partition = repos
            .partitions()
            .get_by_id(to_skip_partition.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_partition.sort_key,
            vec!["tag2", "tag1", "tag3 , with comma", "time"]
        );
        assert_eq!(
            updated_partition.sort_key_ids,
            Some(SortedColumnSet::from([2, 1, 4, 3]))
        );

        let updated_partition = repos
            .partitions()
            .get_by_hash_id(to_skip_partition.hash_id().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_partition.sort_key,
            vec!["tag2", "tag1", "tag3 , with comma", "time"]
        );
        assert_eq!(
            updated_partition.sort_key_ids,
            Some(SortedColumnSet::from([2, 1, 4, 3]))
        );

        // The compactor can log why compaction was skipped
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert!(
            skipped_compactions.is_empty(),
            "Expected no skipped compactions, got: {skipped_compactions:?}"
        );
        repos
            .partitions()
            .record_skipped_compaction(to_skip_partition.id, "I am le tired", 1, 2, 4, 10, 20)
            .await
            .unwrap();
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert_eq!(skipped_compactions.len(), 1);
        assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
        assert_eq!(skipped_compactions[0].reason, "I am le tired");
        assert_eq!(skipped_compactions[0].num_files, 1);
        assert_eq!(skipped_compactions[0].limit_num_files, 2);
        assert_eq!(skipped_compactions[0].estimated_bytes, 10);
        assert_eq!(skipped_compactions[0].limit_bytes, 20);
        //
        let skipped_partition_records = repos
            .partitions()
            .get_in_skipped_compactions(&[to_skip_partition.id])
            .await
            .unwrap();
        assert_eq!(
            skipped_partition_records[0].partition_id,
            to_skip_partition.id
        );
        assert_eq!(skipped_partition_records[0].reason, "I am le tired");

        // Only save the last reason that any particular partition was skipped (really if the
        // partition appears in the skipped compactions, it shouldn't become a compaction candidate
        // again, but race conditions and all that)
        repos
            .partitions()
            .record_skipped_compaction(to_skip_partition.id, "I'm on fire", 11, 12, 24, 110, 120)
            .await
            .unwrap();
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert_eq!(skipped_compactions.len(), 1);
        assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
        assert_eq!(skipped_compactions[0].reason, "I'm on fire");
        assert_eq!(skipped_compactions[0].num_files, 11);
        assert_eq!(skipped_compactions[0].limit_num_files, 12);
        assert_eq!(skipped_compactions[0].estimated_bytes, 110);
        assert_eq!(skipped_compactions[0].limit_bytes, 120);
        //
        let skipped_partition_records = repos
            .partitions()
            .get_in_skipped_compactions(&[to_skip_partition.id])
            .await
            .unwrap();
        assert_eq!(
            skipped_partition_records[0].partition_id,
            to_skip_partition.id
        );
        assert_eq!(skipped_partition_records[0].reason, "I'm on fire");

        // Can receive multiple skipped compactions for different partitions
        repos
            .partitions()
            .record_skipped_compaction(
                to_skip_partition_too.id,
                "I am le tired too",
                1,
                2,
                4,
                10,
                20,
            )
            .await
            .unwrap();
        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert_eq!(skipped_compactions.len(), 2);
        assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
        assert_eq!(
            skipped_compactions[1].partition_id,
            to_skip_partition_too.id
        );
        // confirm can fetch subset of skipped compactions (a.k.a. have two, only fetch 1)
        let skipped_partition_records = repos
            .partitions()
            .get_in_skipped_compactions(&[to_skip_partition.id])
            .await
            .unwrap();
        assert_eq!(skipped_partition_records.len(), 1);
        assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
        let skipped_partition_records = repos
            .partitions()
            .get_in_skipped_compactions(&[to_skip_partition_too.id])
            .await
            .unwrap();
        assert_eq!(skipped_partition_records.len(), 1);
        assert_eq!(
            skipped_partition_records[0].partition_id,
            to_skip_partition_too.id
        );
        // confirm can fetch both skipped compactions, and not the unskipped one
        // also confirm will not error on non-existing partition
        let non_existing_partition_id = PartitionId::new(9999);
        let skipped_partition_records = repos
            .partitions()
            .get_in_skipped_compactions(&[
                partition.id,
                to_skip_partition.id,
                to_skip_partition_too.id,
                non_existing_partition_id,
            ])
            .await
            .unwrap();
        assert_eq!(skipped_partition_records.len(), 2);
        assert_eq!(
            skipped_partition_records[0].partition_id,
            to_skip_partition.id
        );
        assert_eq!(
            skipped_partition_records[1].partition_id,
            to_skip_partition_too.id
        );

        // Delete the skipped compactions
        let deleted_skipped_compaction = repos
            .partitions()
            .delete_skipped_compactions(to_skip_partition.id)
            .await
            .unwrap()
            .expect("The skipped compaction should have been returned");
        assert_eq!(
            deleted_skipped_compaction.partition_id,
            to_skip_partition.id
        );
        assert_eq!(deleted_skipped_compaction.reason, "I'm on fire");
        assert_eq!(deleted_skipped_compaction.num_files, 11);
        assert_eq!(deleted_skipped_compaction.limit_num_files, 12);
        assert_eq!(deleted_skipped_compaction.estimated_bytes, 110);
        assert_eq!(deleted_skipped_compaction.limit_bytes, 120);
        //
        let deleted_skipped_compaction = repos
            .partitions()
            .delete_skipped_compactions(to_skip_partition_too.id)
            .await
            .unwrap()
            .expect("The skipped compaction should have been returned");
        assert_eq!(
            deleted_skipped_compaction.partition_id,
            to_skip_partition_too.id
        );
        assert_eq!(deleted_skipped_compaction.reason, "I am le tired too");
        //
        let skipped_partition_records = repos
            .partitions()
            .get_in_skipped_compactions(&[to_skip_partition.id])
            .await
            .unwrap();
        assert!(skipped_partition_records.is_empty());

        let not_deleted_skipped_compaction = repos
            .partitions()
            .delete_skipped_compactions(to_skip_partition.id)
            .await
            .unwrap();

        assert!(
            not_deleted_skipped_compaction.is_none(),
            "There should be no skipped compation",
        );

        let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
        assert!(
            skipped_compactions.is_empty(),
            "Expected no skipped compactions, got: {skipped_compactions:?}"
        );

        let recent = repos
            .partitions()
            .most_recent_n(10)
            .await
            .expect("should list most recent");
        assert_eq!(recent.len(), 4);

        // Test: sort_key_ids from most_recent_n
        // Only the second one has vallues, the other 3 are empty
        let empty_vec_string: Vec<String> = vec![];
        assert_eq!(recent[0].sort_key, empty_vec_string);
        assert_eq!(recent[0].sort_key_ids, Some(SortedColumnSet::from(vec![])));

        assert_eq!(
            recent[1].sort_key,
            vec![
                "tag2".to_string(),
                "tag1".to_string(),
                "tag3 , with comma".to_string(),
                "time".to_string()
            ]
        );
        assert_eq!(
            recent[1].sort_key_ids,
            Some(SortedColumnSet::from(vec![2, 1, 4, 3]))
        );

        assert_eq!(recent[2].sort_key, empty_vec_string);
        assert_eq!(recent[2].sort_key_ids, Some(SortedColumnSet::from(vec![])));

        assert_eq!(recent[3].sort_key, empty_vec_string);
        assert_eq!(recent[3].sort_key_ids, Some(SortedColumnSet::from(vec![])));

        let recent = repos
            .partitions()
            .most_recent_n(4)
            .await
            .expect("should list most recent");
        assert_eq!(recent.len(), 4); // no off by one error

        let recent = repos
            .partitions()
            .most_recent_n(2)
            .await
            .expect("should list most recent");
        assert_eq!(recent.len(), 2);

        repos
            .namespaces()
            .soft_delete("namespace_partition_test")
            .await
            .expect("delete namespace should succeed");
    }

    /// tests many interactions with the catalog and parquet files. See the individual conditions
    /// herein
    async fn test_parquet_file(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "namespace_parquet_file_test").await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
        let other_table = arbitrary_table(&mut *repos, "other", &namespace).await;
        let partition = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();
        let other_partition = repos
            .partitions()
            .create_or_get("one".into(), other_table.id)
            .await
            .unwrap();

        let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);
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
            partition_id: other_partition.transition_partition_id(),
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(50),
            max_time: Timestamp::new(60),
            ..parquet_file_params.clone()
        };
        let other_file = repos.parquet_files().create(other_params).await.unwrap();

        let exist_id = parquet_file.id;
        let non_exist_id = ParquetFileId::new(other_file.id.get() + 10);
        // make sure exists_id != non_exist_id
        assert_ne!(exist_id, non_exist_id);

        // verify that to_delete is initially set to null and the file does not get deleted
        assert!(parquet_file.to_delete.is_none());
        let older_than = Timestamp::new(
            (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
        );
        let deleted = repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .unwrap();
        assert!(deleted.is_empty());

        // test list_all that includes soft-deleted file
        // at this time the file is not soft-deleted yet and will be included in the returned list
        let files = repos.parquet_files().list_all().await.unwrap();
        assert_eq!(files.len(), 2);

        // verify to_delete can be updated to a timestamp
        repos
            .parquet_files()
            .create_upgrade_delete(&[parquet_file.id], &[], &[], CompactionLevel::Initial)
            .await
            .unwrap();

        // test list_all that includes soft-deleted file
        // at this time the file is soft-deleted and will be included in the returned list
        let files = repos.parquet_files().list_all().await.unwrap();
        assert_eq!(files.len(), 2);
        let marked_deleted = files
            .iter()
            .find(|f| f.to_delete.is_some())
            .cloned()
            .unwrap();

        // File is not deleted if it was marked to be deleted after the specified time
        let before_deleted = Timestamp::new(
            (catalog.time_provider().now() - Duration::from_secs(100)).timestamp_nanos(),
        );
        let deleted = repos
            .parquet_files()
            .delete_old_ids_only(before_deleted)
            .await
            .unwrap();
        assert!(deleted.is_empty());

        // test list_all that includes soft-deleted file
        // at this time the file is not actually hard deleted yet and stay as soft deleted
        // and will be returned in the list
        let files = repos.parquet_files().list_all().await.unwrap();
        assert_eq!(files.len(), 2);

        // File is deleted if it was marked to be deleted before the specified time
        let deleted = repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(marked_deleted.id, deleted[0]);

        // test list_all that includes soft-deleted file
        // at this time the file is hard deleted -> the returned list is empty
        let files = repos.parquet_files().list_all().await.unwrap();
        assert_eq!(files.len(), 1);

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

        // test list_all
        let files = repos.parquet_files().list_all().await.unwrap();
        assert_eq!(vec![other_file.clone()], files);

        // test list_by_namespace_not_to_delete
        let namespace2 = arbitrary_namespace(&mut *repos, "namespace_parquet_file_test1").await;
        let table2 = arbitrary_table(&mut *repos, "test_table2", &namespace2).await;
        let partition2 = repos
            .partitions()
            .create_or_get("foo".into(), table2.id)
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
            partition_id: partition2.transition_partition_id(),
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
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
            ..f1_params.clone()
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
            ..f2_params
        };
        let f3 = repos
            .parquet_files()
            .create(f3_params.clone())
            .await
            .unwrap();
        let files = repos
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace2.id)
            .await
            .unwrap();
        assert_eq!(vec![f1.clone(), f2.clone(), f3.clone()], files);

        repos
            .parquet_files()
            .create_upgrade_delete(&[f2.id], &[], &[], CompactionLevel::Initial)
            .await
            .unwrap();
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

        // test delete_old_ids_only
        let older_than = Timestamp::new(
            (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
        );
        let ids = repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);

        // test retention-based flagging for deletion
        // Since mem catalog has default retention 1 hour, let us first set it to 0 means infinite
        let namespaces = repos
            .namespaces()
            .list(SoftDeletedRows::AllRows)
            .await
            .expect("listing namespaces");
        for namespace in namespaces {
            repos
                .namespaces()
                .update_retention_period(&namespace.name, None) // infinite
                .await
                .unwrap();
        }

        // 1. with no retention period set on the ns, nothing should get flagged
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert!(ids.is_empty());
        // 2. set ns retention period to one hour then create some files before and after and
        //    ensure correct files get deleted
        repos
            .namespaces()
            .update_retention_period(&namespace.name, Some(60 * 60 * 1_000_000_000)) // 1 hour
            .await
            .unwrap();
        let f4_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            max_time: Timestamp::new(
                // a bit over an hour ago
                (catalog.time_provider().now() - Duration::from_secs(60 * 65)).timestamp_nanos(),
            ),
            ..f3_params
        };
        let f4 = repos
            .parquet_files()
            .create(f4_params.clone())
            .await
            .unwrap();
        let f5_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            max_time: Timestamp::new(
                // a bit under an hour ago
                (catalog.time_provider().now() - Duration::from_secs(60 * 55)).timestamp_nanos(),
            ),
            ..f4_params
        };
        let f5 = repos
            .parquet_files()
            .create(f5_params.clone())
            .await
            .unwrap();
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert!(ids.len() > 1); // it's also going to flag f1, f2 & f3 because they have low max
                                // timestamps but i don't want this test to be brittle if those
                                // values change so i'm not asserting len == 4
        let f4 = repos
            .parquet_files()
            .get_by_object_store_id(f4.object_store_id)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f4.to_delete, Some(_)); // f4 is > 1hr old
        let f5 = repos
            .parquet_files()
            .get_by_object_store_id(f5.object_store_id)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f5.to_delete, None); // f5 is < 1hr old

        // call flag_for_delete_by_retention() again and nothing should be flagged because they've
        // already been flagged
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert!(ids.is_empty());

        // test that flag_for_delete_by_retention respects UPDATE LIMIT
        // create limit + the meaning of life parquet files that are all older than the retention (>1hr)
        const LIMIT: usize = 1000;
        const MOL: usize = 42;
        for _ in 0..LIMIT + MOL {
            let params = ParquetFileParams {
                object_store_id: Uuid::new_v4(),
                max_time: Timestamp::new(
                    // a bit over an hour ago
                    (catalog.time_provider().now() - Duration::from_secs(60 * 65))
                        .timestamp_nanos(),
                ),
                ..f1_params.clone()
            };
            repos.parquet_files().create(params.clone()).await.unwrap();
        }
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids.len(), LIMIT);
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids.len(), MOL); // second call took remainder
        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids.len(), 0); // none left

        // test create_update_delete
        let f6_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            ..f5_params
        };
        let f1_uuid = f1.object_store_id;
        let f5_uuid = f5.object_store_id;
        let cud = repos
            .parquet_files()
            .create_upgrade_delete(
                &[f5.id],
                &[f1.id],
                &[f6_params.clone()],
                CompactionLevel::Final,
            )
            .await
            .unwrap();

        assert_eq!(cud.len(), 1);
        let f5_delete = repos
            .parquet_files()
            .get_by_object_store_id(f5_uuid)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f5_delete.to_delete, Some(_));

        let f1_compaction_level = repos
            .parquet_files()
            .get_by_object_store_id(f1_uuid)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f1_compaction_level.compaction_level, CompactionLevel::Final);

        let f6 = repos
            .parquet_files()
            .get_by_object_store_id(f6_params.object_store_id)
            .await
            .unwrap()
            .unwrap();

        let f6_uuid = f6.object_store_id;

        // test create_update_delete transaction (rollsback because f6 already exists)
        let cud = repos
            .parquet_files()
            .create_upgrade_delete(
                &[f5.id],
                &[f2.id],
                &[f6_params.clone()],
                CompactionLevel::Final,
            )
            .await;

        assert_matches!(
            cud,
            Err(Error::FileExists {
                object_store_id
            }) if object_store_id == f6_params.object_store_id
        );

        let f6_not_delete = repos
            .parquet_files()
            .get_by_object_store_id(f6_uuid)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(f6_not_delete.to_delete, None);

        // test exists_by_object_store_id_batch returns parquet files by object store id
        let does_not_exist = Uuid::new_v4();
        let mut present = repos
            .parquet_files()
            .exists_by_object_store_id_batch(vec![f6_uuid, f1_uuid, does_not_exist])
            .await
            .unwrap();
        assert_eq!(present.len(), 2);
        let mut expected = vec![f6_uuid, f1_uuid];
        present.sort();
        expected.sort();
        assert_eq!(present, expected);
    }

    async fn test_parquet_file_delete_broken(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace_1 = arbitrary_namespace(&mut *repos, "retention_broken_1").await;
        let namespace_2 = repos
            .namespaces()
            .create(
                &NamespaceName::new("retention_broken_2").unwrap(),
                None,
                Some(1),
                None,
            )
            .await
            .unwrap();
        let table_1 = arbitrary_table(&mut *repos, "test_table", &namespace_1).await;
        let table_2 = arbitrary_table(&mut *repos, "test_table", &namespace_2).await;
        let partition_1 = repos
            .partitions()
            .create_or_get("one".into(), table_1.id)
            .await
            .unwrap();
        let partition_2 = repos
            .partitions()
            .create_or_get("one".into(), table_2.id)
            .await
            .unwrap();

        let parquet_file_params_1 =
            arbitrary_parquet_file_params(&namespace_1, &table_1, &partition_1);
        let parquet_file_params_2 =
            arbitrary_parquet_file_params(&namespace_2, &table_2, &partition_2);
        let _parquet_file_1 = repos
            .parquet_files()
            .create(parquet_file_params_1)
            .await
            .unwrap();
        let parquet_file_2 = repos
            .parquet_files()
            .create(parquet_file_params_2)
            .await
            .unwrap();

        let ids = repos
            .parquet_files()
            .flag_for_delete_by_retention()
            .await
            .unwrap();
        assert_eq!(ids, vec![parquet_file_2.id]);
    }

    async fn test_partitions_new_file_between(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "test_partitions_new_file_between").await;
        let table =
            arbitrary_table(&mut *repos, "test_table_for_new_file_between", &namespace).await;

        // param for the tests
        let time_now = Timestamp::from(catalog.time_provider().now());
        let time_one_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(1));
        let time_two_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(2));
        let time_three_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(3));
        let time_five_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(5));
        let time_six_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(6));

        // Db has no partitions
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // -----------------
        // PARTITION one
        // The DB has 1 partition but it does not have any file
        let partition1 = repos
            .partitions()
            .create_or_get("one".into(), table.id)
            .await
            .unwrap();
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // create files for partition one
        let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition1);

        // create a deleted L0 file that was created 3 hours ago
        let delete_l0_file = repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        repos
            .parquet_files()
            .create_upgrade_delete(&[delete_l0_file.id], &[], &[], CompactionLevel::Initial)
            .await
            .unwrap();
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert!(partitions.is_empty());
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, Some(time_one_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_one_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // create a deleted L0 file that was created 1 hour ago
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
        // partition one should be returned
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_two_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // -----------------
        // PARTITION two
        // Partition two without any file
        let partition2 = repos
            .partitions()
            .create_or_get("two".into(), table.id)
            .await
            .unwrap();
        // should return partition one only
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);

        // Add a L0 file created 5 hours ago
        let l0_five_hour_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_five_hour_ago,
            partition_id: partition2.transition_partition_id(),
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_five_hour_ago_file_params.clone())
            .await
            .unwrap();
        // still return partition one only
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        // Between six and three hours ago, return only partition 2
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition2.id);

        // Add an L1 file created just now
        let l1_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_now,
            partition_id: partition2.transition_partition_id(),
            compaction_level: CompactionLevel::FileNonOverlapped,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l1_file_params.clone())
            .await
            .unwrap();
        // should return both partitions
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Only return partition1: the creation time must be strictly less than the maximum time,
        // not equal
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // -----------------
        // PARTITION three
        // Partition three without any file
        let partition3 = repos
            .partitions()
            .create_or_get("three".into(), table.id)
            .await
            .unwrap();
        // should return partition one and two only
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Only return partition1: the creation time must be strictly less than the maximum time,
        // not equal
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        // When the maximum time is greater than the creation time of partition2, return it
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now + 1))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // Add an L2 file created just now for partition three
        let l2_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_now,
            partition_id: partition3.transition_partition_id(),
            compaction_level: CompactionLevel::Final,
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l2_file_params.clone())
            .await
            .unwrap();
        // now should return partition one two and three
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 3);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        assert_eq!(partitions[2], partition3.id);
        // Only return partition1: the creation time must be strictly less than the maximum time,
        // not equal
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], partition1.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());

        // add an L0 file created one hour ago for partition three
        let l0_one_hour_ago_file_params = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            created_at: time_one_hour_ago,
            partition_id: partition3.transition_partition_id(),
            ..parquet_file_params.clone()
        };
        repos
            .parquet_files()
            .create(l0_one_hour_ago_file_params.clone())
            .await
            .unwrap();
        // should return all partitions
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_two_hour_ago, None)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 3);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition2.id);
        assert_eq!(partitions[2], partition3.id);
        // Only return partitions 1 and 3; 2 was created just now
        let mut partitions = repos
            .partitions()
            .partitions_new_file_between(time_three_hour_ago, Some(time_now))
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        partitions.sort();
        assert_eq!(partitions[0], partition1.id);
        assert_eq!(partitions[1], partition3.id);
        // Between six and three hours ago, return none
        let partitions = repos
            .partitions()
            .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
            .await
            .unwrap();
        assert!(partitions.is_empty());
    }

    async fn test_list_by_partiton_not_to_delete(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(
            &mut *repos,
            "namespace_parquet_file_test_list_by_partiton_not_to_delete",
        )
        .await;
        let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;

        let partition = repos
            .partitions()
            .create_or_get("test_list_by_partiton_not_to_delete_one".into(), table.id)
            .await
            .unwrap();
        let partition2 = repos
            .partitions()
            .create_or_get("test_list_by_partiton_not_to_delete_two".into(), table.id)
            .await
            .unwrap();

        let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);

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
            .create_upgrade_delete(&[delete_file.id], &[], &[], CompactionLevel::Initial)
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
            .create_upgrade_delete(
                &[],
                &[level1_file.id],
                &[],
                CompactionLevel::FileNonOverlapped,
            )
            .await
            .unwrap();
        level1_file.compaction_level = CompactionLevel::FileNonOverlapped;

        let other_partition_params = ParquetFileParams {
            partition_id: partition2.transition_partition_id(),
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
            .list_by_partition_not_to_delete(&partition.transition_partition_id())
            .await
            .unwrap();
        assert_eq!(files.len(), 2);

        let mut file_ids: Vec<_> = files.into_iter().map(|f| f.id).collect();
        file_ids.sort();
        let mut expected_ids = vec![parquet_file.id, level1_file.id];
        expected_ids.sort();
        assert_eq!(file_ids, expected_ids);

        // Using the catalog partition ID should return the same files, even if the Parquet file
        // records don't have the partition ID on them (which is the default now)
        let files = repos
            .parquet_files()
            .list_by_partition_not_to_delete(&TransitionPartitionId::Deprecated(partition.id))
            .await
            .unwrap();
        assert_eq!(files.len(), 2);

        let mut file_ids: Vec<_> = files.into_iter().map(|f| f.id).collect();
        file_ids.sort();
        let mut expected_ids = vec![parquet_file.id, level1_file.id];
        expected_ids.sort();
        assert_eq!(file_ids, expected_ids);
    }

    async fn test_update_to_compaction_level_1(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace =
            arbitrary_namespace(&mut *repos, "namespace_update_to_compaction_level_1_test").await;
        let table = arbitrary_table(&mut *repos, "update_table", &namespace).await;
        let partition = repos
            .partitions()
            .create_or_get("test_update_to_compaction_level_1_one".into(), table.id)
            .await
            .unwrap();

        // Set up the window of times we're interested in level 1 files for
        let query_min_time = Timestamp::new(5);
        let query_max_time = Timestamp::new(10);

        // Create a file with times entirely within the window
        let mut parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);
        parquet_file_params.min_time = query_min_time + 1;
        parquet_file_params.max_time = query_max_time - 1;
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

        // Make parquet_file compaction level 1, attempt to mark the nonexistent file; operation
        // should succeed
        let created = repos
            .parquet_files()
            .create_upgrade_delete(
                &[],
                &[parquet_file.id, nonexistent_parquet_file_id],
                &[],
                CompactionLevel::FileNonOverlapped,
            )
            .await
            .unwrap();
        assert_eq!(created, vec![]);

        // remove namespace to avoid it from affecting later tests
        repos
            .namespaces()
            .soft_delete("namespace_update_to_compaction_level_1_test")
            .await
            .expect("delete namespace should succeed");
    }

    /// Assert that a namespace deletion does NOT cascade to the tables/schema
    /// items/parquet files/etc.
    ///
    /// Removal of this entities breaks the invariant that once created, a row
    /// always exists for the lifetime of an IOx process, and causes the system
    /// to panic in multiple components. It's also ineffective, because most
    /// components maintain a cache of at least one of these entities.
    ///
    /// Instead soft deleted namespaces should have their files GC'd like a
    /// normal parquet file deletion, removing the rows once they're no longer
    /// being actively used by the system. This is done by waiting a long time
    /// before deleting records, and whilst isn't perfect, it is largely
    /// effective.
    async fn test_delete_namespace(catalog: Arc<dyn Catalog>) {
        let mut repos = catalog.repositories().await;
        let namespace_1 =
            arbitrary_namespace(&mut *repos, "namespace_test_delete_namespace_1").await;
        let table_1 = arbitrary_table(&mut *repos, "test_table_1", &namespace_1).await;
        let _c = repos
            .columns()
            .create_or_get("column_test_1", table_1.id, ColumnType::Tag)
            .await
            .unwrap();
        let partition_1 = repos
            .partitions()
            .create_or_get("test_delete_namespace_one".into(), table_1.id)
            .await
            .unwrap();

        // parquet files
        let parquet_file_params =
            arbitrary_parquet_file_params(&namespace_1, &table_1, &partition_1);
        repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let parquet_file_params_2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(200),
            max_time: Timestamp::new(300),
            ..parquet_file_params
        };
        repos
            .parquet_files()
            .create(parquet_file_params_2.clone())
            .await
            .unwrap();

        // we've now created a namespace with a table and parquet files. before we test deleting
        // it, let's create another so we can ensure that doesn't get deleted.
        let namespace_2 =
            arbitrary_namespace(&mut *repos, "namespace_test_delete_namespace_2").await;
        let table_2 = arbitrary_table(&mut *repos, "test_table_2", &namespace_2).await;
        let _c = repos
            .columns()
            .create_or_get("column_test_2", table_2.id, ColumnType::Tag)
            .await
            .unwrap();
        let partition_2 = repos
            .partitions()
            .create_or_get("test_delete_namespace_two".into(), table_2.id)
            .await
            .unwrap();

        // parquet files
        let parquet_file_params =
            arbitrary_parquet_file_params(&namespace_2, &table_2, &partition_2);
        repos
            .parquet_files()
            .create(parquet_file_params.clone())
            .await
            .unwrap();
        let parquet_file_params_2 = ParquetFileParams {
            object_store_id: Uuid::new_v4(),
            min_time: Timestamp::new(200),
            max_time: Timestamp::new(300),
            ..parquet_file_params
        };
        repos
            .parquet_files()
            .create(parquet_file_params_2.clone())
            .await
            .unwrap();

        // now delete namespace_1 and assert it's all gone and none of
        // namespace_2 is gone
        repos
            .namespaces()
            .soft_delete("namespace_test_delete_namespace_1")
            .await
            .expect("delete namespace should succeed");
        // assert that namespace is soft-deleted, but the table, column, and parquet files are all
        // still there.
        assert!(repos
            .namespaces()
            .get_by_id(namespace_1.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("get namespace should succeed")
            .is_none());
        assert_eq!(
            repos
                .namespaces()
                .get_by_id(namespace_1.id, SoftDeletedRows::AllRows)
                .await
                .expect("get namespace should succeed")
                .map(|mut v| {
                    // The only change after soft-deletion should be the deleted_at
                    // field being set - this block normalises that field, so that
                    // the before/after can be asserted as equal.
                    v.deleted_at = None;
                    v
                })
                .expect("should see soft-deleted row"),
            namespace_1
        );
        assert_eq!(
            repos
                .tables()
                .get_by_id(table_1.id)
                .await
                .expect("get table should succeed")
                .expect("should return row"),
            table_1
        );
        assert_eq!(
            repos
                .columns()
                .list_by_namespace_id(namespace_1.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );
        assert_eq!(
            repos
                .columns()
                .list_by_table_id(table_1.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );

        // partition's get_by_id should succeed
        repos
            .partitions()
            .get_by_id(partition_1.id)
            .await
            .unwrap()
            .unwrap();

        // assert that the namespace, table, column, and parquet files for namespace_2 are still
        // there
        assert!(repos
            .namespaces()
            .get_by_id(namespace_2.id, SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("get namespace should succeed")
            .is_some());

        assert!(repos
            .tables()
            .get_by_id(table_2.id)
            .await
            .expect("get table should succeed")
            .is_some());
        assert_eq!(
            repos
                .columns()
                .list_by_namespace_id(namespace_2.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );
        assert_eq!(
            repos
                .columns()
                .list_by_table_id(table_2.id)
                .await
                .expect("listing columns should succeed")
                .len(),
            1
        );

        // partition's get_by_id should succeed
        repos
            .partitions()
            .get_by_id(partition_2.id)
            .await
            .unwrap()
            .unwrap();
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
        let namespace = repos
            .namespaces()
            .create(
                &NamespaceName::new(namespace_name).unwrap(),
                None,
                None,
                None,
            )
            .await;

        let namespace = match namespace {
            Ok(v) => v,
            Err(Error::NameExists { .. }) => repos
                .namespaces()
                .get_by_name(namespace_name, SoftDeletedRows::AllRows)
                .await
                .unwrap()
                .unwrap(),
            e @ Err(_) => e.unwrap(),
        };

        let batches = mutable_batch_lp::lines_to_batches(lines, 42).unwrap();
        let batches = batches.iter().map(|(table, batch)| (table.as_str(), batch));
        let ns = NamespaceSchema::new_empty_from(&namespace);

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

    async fn test_list_schemas_soft_deleted_rows(catalog: Arc<dyn Catalog>) {
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

        repos
            .namespaces()
            .soft_delete(&ns2.0.name)
            .await
            .expect("failed to soft delete namespace");

        // Otherwise the in-mem catalog deadlocks.... (but not postgres)
        drop(repos);

        let got = list_schemas(&*catalog)
            .await
            .expect("should be able to list the schemas")
            .collect::<Vec<_>>();

        assert!(got.contains(&ns1), "{:#?}\n\nwant{:#?}", got, &ns1);
        assert!(!got.contains(&ns2), "{:#?}\n\n do not want{:#?}", got, &ns2);
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
