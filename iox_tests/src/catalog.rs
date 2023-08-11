//! Utils of the tests

use arrow::{
    compute::{lexsort, SortColumn, SortOptions},
    record_batch::RecordBatch,
};
use data_types::{
    partition_template::TablePartitionTemplateOverride, Column, ColumnSet, ColumnType,
    ColumnsByName, CompactionLevel, Namespace, NamespaceName, NamespaceSchema, ParquetFile,
    ParquetFileParams, Partition, PartitionId, Table, TableId, TableSchema, Timestamp,
    TransitionPartitionId,
};
use datafusion::physical_plan::metrics::Count;
use datafusion_util::{unbounded_memory_pool, MemoryStream};
use generated_types::influxdata::iox::partition_template::v1::PartitionTemplate;
use iox_catalog::{
    interface::{
        get_schema_by_id, get_table_columns_by_id, Catalog, RepoCollection, SoftDeletedRows,
    },
    mem::MemCatalog,
    partition_lookup,
    test_helpers::arbitrary_table,
};
use iox_query::{
    exec::{DedicatedExecutors, Executor, ExecutorConfig},
    provider::RecordBatchDeduplicator,
    util::arrow_sort_key_exprs,
};
use iox_time::{MockProvider, Time, TimeProvider};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use object_store::{memory::InMemory, DynObjectStore};
use observability_deps::tracing::debug;
use parquet_file::{
    chunk::ParquetChunk,
    metadata::IoxMetadata,
    storage::{ParquetStorage, StorageId},
};
use schema::{
    sort::{adjust_sort_key_columns, compute_sort_key, SortKey},
    Projection, Schema,
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

/// Common retention period used throughout tests
pub const TEST_RETENTION_PERIOD_NS: Option<i64> = Some(3_600 * 1_000_000_000);

/// Catalog for tests
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestCatalog {
    pub catalog: Arc<dyn Catalog>,
    pub metric_registry: Arc<metric::Registry>,
    pub object_store: Arc<DynObjectStore>,
    pub parquet_store: ParquetStorage,
    pub time_provider: Arc<MockProvider>,
    pub exec: Arc<Executor>,
}

impl TestCatalog {
    /// Initialize the catalog
    ///
    /// All test catalogs use the same [`Executor`]. Use [`with_execs`](Self::with_execs) if you need a special or
    /// dedicated executor.
    pub fn new() -> Arc<Self> {
        let exec = Arc::new(DedicatedExecutors::new_testing());
        Self::with_execs(exec, NonZeroUsize::new(1).unwrap())
    }

    /// Initialize with partitions
    pub fn with_target_query_partitions(target_query_partitions: NonZeroUsize) -> Arc<Self> {
        let exec = Arc::new(DedicatedExecutors::new_testing());
        Self::with_execs(exec, target_query_partitions)
    }

    /// Initialize with given executors and partitions
    pub fn with_execs(
        exec: Arc<DedicatedExecutors>,
        target_query_partitions: NonZeroUsize,
    ) -> Arc<Self> {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let object_store = Arc::new(InMemory::new());
        let parquet_store =
            ParquetStorage::new(Arc::clone(&object_store) as _, StorageId::from("iox"));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0).unwrap()));
        let exec = Arc::new(Executor::new_with_config_and_executors(
            ExecutorConfig {
                num_threads: exec.num_threads(),
                target_query_partitions,
                object_stores: HashMap::from([(
                    parquet_store.id(),
                    Arc::clone(parquet_store.object_store()),
                )]),
                metric_registry: Arc::clone(&metric_registry),
                mem_pool_size: 1024 * 1024 * 1024,
            },
            exec,
        ));

        Arc::new(Self {
            metric_registry,
            catalog,
            object_store,
            parquet_store,
            time_provider,
            exec,
        })
    }

    /// Return the catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Return the catalog's metric registry
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// Return the catalog's  object store
    pub fn object_store(&self) -> Arc<DynObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Return the mockable version of the catalog's time provider.
    ///
    /// If you need a generic time provider, use [`time_provider`](Self::time_provider) instead.
    pub fn mock_time_provider(&self) -> &MockProvider {
        self.time_provider.as_ref()
    }

    /// Return the catalog's time provider
    ///
    /// If you need to mock the time, use [`mock_time_provider`](Self::mock_time_provider) instead.
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider) as _
    }

    /// Return the catalog's executor
    pub fn exec(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }

    /// Create namespace with specified retention
    pub async fn create_namespace_with_retention(
        self: &Arc<Self>,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Arc<TestNamespace> {
        let mut repos = self.catalog.repositories().await;
        let namespace_name = NamespaceName::new(name).unwrap();
        let namespace = repos
            .namespaces()
            .create(&namespace_name, None, retention_period_ns, None)
            .await
            .unwrap();

        Arc::new(TestNamespace {
            catalog: Arc::clone(self),
            namespace,
        })
    }

    /// Create a namespace in the catalog
    pub async fn create_namespace_1hr_retention(
        self: &Arc<Self>,
        name: &str,
    ) -> Arc<TestNamespace> {
        self.create_namespace_with_retention(name, TEST_RETENTION_PERIOD_NS)
            .await
    }

    /// List all non-deleted files
    pub async fn list_by_table_not_to_delete(
        self: &Arc<Self>,
        table_id: TableId,
    ) -> Vec<ParquetFile> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_table_not_to_delete(table_id)
            .await
            .unwrap()
    }

    /// Add a partition into skipped compaction
    pub async fn add_to_skipped_compaction(
        self: &Arc<Self>,
        partition_id: PartitionId,
        reason: &str,
    ) {
        let mut repos = self.catalog.repositories().await;

        repos
            .partitions()
            .record_skipped_compaction(partition_id, reason, 0, 0, 0, 0, 0)
            .await
            .unwrap();
    }
}

/// A test namespace
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestNamespace {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Namespace,
}

impl TestNamespace {
    /// Create a table in this namespace
    pub async fn create_table(self: &Arc<Self>, name: &str) -> Arc<TestTable> {
        let mut repos = self.catalog.catalog.repositories().await;

        let table = arbitrary_table(&mut *repos, name, &self.namespace).await;

        Arc::new(TestTable {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(self),
            table,
        })
    }

    /// Create a table in this namespace w/ given partition template
    pub async fn create_table_with_partition_template(
        self: &Arc<Self>,
        name: &str,
        template: Option<PartitionTemplate>,
    ) -> Arc<TestTable> {
        let mut repos = self.catalog.catalog.repositories().await;

        let table = repos
            .tables()
            .create(
                name,
                TablePartitionTemplateOverride::try_new(
                    template,
                    &self.namespace.partition_template,
                )
                .unwrap(),
                self.namespace.id,
            )
            .await
            .unwrap();

        Arc::new(TestTable {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(self),
            table,
        })
    }

    /// Get namespace schema for this namespace.
    pub async fn schema(&self) -> NamespaceSchema {
        let mut repos = self.catalog.catalog.repositories().await;
        get_schema_by_id(
            self.namespace.id,
            repos.as_mut(),
            SoftDeletedRows::ExcludeDeleted,
        )
        .await
        .unwrap()
    }

    /// Set the number of columns per table allowed in this namespace.
    pub async fn update_column_limit(&self, new_max: i32) {
        let mut repos = self.catalog.catalog.repositories().await;
        repos
            .namespaces()
            .update_column_limit(&self.namespace.name, new_max)
            .await
            .unwrap();
    }

    /// Set the number of tables allowed in this namespace.
    pub async fn update_table_limit(&self, new_max: i32) {
        let mut repos = self.catalog.catalog.repositories().await;
        repos
            .namespaces()
            .update_table_limit(&self.namespace.name, new_max)
            .await
            .unwrap();
    }
}

/// A test table of a namespace in the catalog
#[allow(missing_docs)]
#[derive(Debug)]
pub struct TestTable {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Table,
}

impl TestTable {
    /// Creat a partition for the table
    pub async fn create_partition(self: &Arc<Self>, key: &str) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let partition = repos
            .partitions()
            .create_or_get(key.into(), self.table.id)
            .await
            .unwrap();

        Arc::new(TestPartition {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            partition,
        })
    }

    /// Create a partition with a specified sort key for the table
    pub async fn create_partition_with_sort_key(
        self: &Arc<Self>,
        key: &str,
        sort_key: &[&str],
    ) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let partition = repos
            .partitions()
            .create_or_get(key.into(), self.table.id)
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .cas_sort_key(
                &TransitionPartitionId::Deprecated(partition.id),
                None,
                sort_key,
            )
            .await
            .unwrap();

        Arc::new(TestPartition {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            partition,
        })
    }

    /// Create a column for the table
    pub async fn create_column(
        self: &Arc<Self>,
        name: &str,
        column_type: ColumnType,
    ) -> Arc<TestColumn> {
        let mut repos = self.catalog.catalog.repositories().await;

        let column = repos
            .columns()
            .create_or_get(name, self.table.id, column_type)
            .await
            .unwrap();

        Arc::new(TestColumn {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            column,
        })
    }

    /// Get the TableSchema from the catalog.
    pub async fn catalog_schema(&self) -> TableSchema {
        TableSchema {
            id: self.table.id,
            partition_template: Default::default(),
            columns: self.catalog_columns().await,
        }
    }

    /// Get columns from the catalog.
    pub async fn catalog_columns(&self) -> ColumnsByName {
        let mut repos = self.catalog.catalog.repositories().await;

        get_table_columns_by_id(self.table.id, repos.as_mut())
            .await
            .unwrap()
    }

    /// Get schema for this table.
    pub async fn schema(&self) -> Schema {
        self.catalog_columns().await.try_into().unwrap()
    }

    /// Read the record batches from the specified Parquet File associated with this table.
    pub async fn read_parquet_file(&self, file: ParquetFile) -> Vec<RecordBatch> {
        // get schema
        let table_catalog_columns = self.catalog_columns().await;
        let column_id_lookup = table_catalog_columns.id_map();
        let table_schema = self.schema().await;
        let selection: Vec<_> = file
            .column_set
            .iter()
            .map(|id| *column_id_lookup.get(id).unwrap())
            .collect();
        let schema = table_schema.select_by_names(&selection).unwrap();

        let chunk = ParquetChunk::new(Arc::new(file), schema, self.catalog.parquet_store.clone());
        chunk
            .parquet_exec_input()
            .read_to_batches(
                chunk.schema().as_arrow(),
                Projection::All,
                &chunk.store().test_df_context(),
            )
            .await
            .unwrap()
    }
}

/// A test column.
#[allow(missing_docs)]
pub struct TestColumn {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub column: Column,
}

/// A test catalog with specified namespace, table, partition
#[allow(missing_docs)]
#[derive(Debug)]
pub struct TestPartition {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub partition: Partition,
}

impl TestPartition {
    /// Update sort key.
    pub async fn update_sort_key(self: &Arc<Self>, sort_key: SortKey) -> Arc<Self> {
        let old_sort_key = partition_lookup(
            self.catalog.catalog.repositories().await.as_mut(),
            &self.partition.transition_partition_id(),
        )
        .await
        .unwrap()
        .unwrap()
        .sort_key;

        let partition = self
            .catalog
            .catalog
            .repositories()
            .await
            .partitions()
            .cas_sort_key(
                &self.partition.transition_partition_id(),
                Some(old_sort_key),
                &sort_key.to_columns().collect::<Vec<_>>(),
            )
            .await
            .unwrap();

        Arc::new(Self {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            partition,
        })
    }

    /// Create a Parquet file in this partition in object storage and the catalog with attributes
    /// specified by the builder
    pub async fn create_parquet_file(
        self: &Arc<Self>,
        builder: TestParquetFileBuilder,
    ) -> TestParquetFile {
        let TestParquetFileBuilder {
            record_batch,
            table,
            schema,
            min_time,
            max_time,
            file_size_bytes,
            size_override,
            creation_time,
            compaction_level,
            to_delete,
            object_store_id,
            row_count,
            max_l0_created_at,
        } = builder;

        let record_batch = record_batch.expect("A record batch is required");
        let table = table.expect("A table is required");
        let schema = schema.expect("A schema is required");
        assert_eq!(
            table, self.table.table.name,
            "Table name of line protocol and partition should have matched",
        );

        assert!(
            row_count.is_none(),
            "Cannot have both a record batch and a manually set row_count!"
        );
        let row_count = record_batch.num_rows();
        assert!(row_count > 0, "Parquet file must have at least 1 row");
        let (record_batch, sort_key) = sort_batch(record_batch, &schema);
        let record_batch = dedup_batch(record_batch, &sort_key);

        let object_store_id = object_store_id.unwrap_or_else(Uuid::new_v4);

        let metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: now(),
            namespace_id: self.namespace.namespace.id,
            namespace_name: self.namespace.namespace.name.clone().into(),
            table_id: self.table.table.id,
            table_name: self.table.table.name.clone().into(),
            partition_key: self.partition.partition_key.clone(),
            compaction_level: CompactionLevel::Initial,
            sort_key: Some(sort_key.clone()),
            max_l0_created_at: Time::from_timestamp_nanos(max_l0_created_at),
        };
        let real_file_size_bytes = create_parquet_file(
            ParquetStorage::new(
                Arc::clone(&self.catalog.object_store),
                StorageId::from("iox"),
            ),
            &self.partition.transition_partition_id(),
            &metadata,
            record_batch.clone(),
        )
        .await;

        let builder = TestParquetFileBuilder {
            record_batch: Some(record_batch),
            table: Some(table),
            schema: Some(schema),
            min_time,
            max_time,
            file_size_bytes: Some(file_size_bytes.unwrap_or(real_file_size_bytes as u64)),
            size_override,
            creation_time,
            compaction_level,
            to_delete,
            object_store_id: Some(object_store_id),
            row_count: None, // will be computed from the record batch again
            max_l0_created_at,
        };

        let result = self.create_parquet_file_catalog_record(builder).await;
        let mut repos = self.catalog.catalog.repositories().await;
        update_catalog_sort_key_if_needed(
            repos.as_mut(),
            &self.partition.transition_partition_id(),
            sort_key,
        )
        .await;
        result
    }

    /// Only update the catalog with the builder's info, don't create anything in object storage.
    /// Record batch is not required in this case.
    pub async fn create_parquet_file_catalog_record(
        self: &Arc<Self>,
        builder: TestParquetFileBuilder,
    ) -> TestParquetFile {
        let TestParquetFileBuilder {
            record_batch,
            min_time,
            max_time,
            file_size_bytes,
            size_override,
            creation_time,
            compaction_level,
            to_delete,
            object_store_id,
            row_count,
            max_l0_created_at,
            ..
        } = builder;

        let table_catalog_columns = self.table.catalog_columns().await;

        let (row_count, column_set) = if let Some(record_batch) = record_batch {
            let column_set = ColumnSet::new(record_batch.schema().fields().iter().map(|f| {
                table_catalog_columns
                    .get(f.name())
                    .unwrap_or_else(|| panic!("Column {} is not registered", f.name()))
                    .id
            }));

            assert!(
                row_count.is_none(),
                "Cannot have both a record batch and a manually set row_count!"
            );

            (record_batch.num_rows(), column_set)
        } else {
            let column_set = ColumnSet::new(table_catalog_columns.ids());
            (row_count.unwrap_or(0), column_set)
        };

        let parquet_file_params = ParquetFileParams {
            namespace_id: self.namespace.namespace.id,
            table_id: self.table.table.id,
            partition_id: self.partition.transition_partition_id(),
            object_store_id: object_store_id.unwrap_or_else(Uuid::new_v4),
            min_time: Timestamp::new(min_time),
            max_time: Timestamp::new(max_time),
            file_size_bytes: file_size_bytes.unwrap_or(0) as i64,
            row_count: row_count as i64,
            created_at: Timestamp::new(creation_time),
            compaction_level,
            column_set,
            max_l0_created_at: Timestamp::new(max_l0_created_at),
        };

        let mut repos = self.catalog.catalog.repositories().await;
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        if to_delete {
            repos
                .parquet_files()
                .create_upgrade_delete(&[parquet_file.id], &[], &[], CompactionLevel::Initial)
                .await
                .unwrap();
        }

        TestParquetFile {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            partition: Arc::clone(self),
            parquet_file,
            size_override,
        }
    }
}

/// A builder for creating parquet files within partitions.
#[derive(Debug, Clone)]
pub struct TestParquetFileBuilder {
    record_batch: Option<RecordBatch>,
    table: Option<String>,
    schema: Option<Schema>,
    min_time: i64,
    max_time: i64,
    file_size_bytes: Option<u64>,
    size_override: Option<i64>,
    creation_time: i64,
    compaction_level: CompactionLevel,
    to_delete: bool,
    object_store_id: Option<Uuid>,
    row_count: Option<usize>,
    max_l0_created_at: i64,
}

impl Default for TestParquetFileBuilder {
    fn default() -> Self {
        Self {
            record_batch: None,
            table: None,
            schema: None,
            min_time: now().timestamp_nanos(),
            max_time: now().timestamp_nanos(),
            file_size_bytes: None,
            size_override: None,
            creation_time: 1,
            compaction_level: CompactionLevel::Initial,
            to_delete: false,
            object_store_id: None,
            row_count: None,
            max_l0_created_at: 1,
        }
    }
}

impl TestParquetFileBuilder {
    /// Specify the line protocol that should become the record batch in this parquet file.
    pub fn with_line_protocol(self, line_protocol: &str) -> Self {
        let (table, batch) = lp_to_mutable_batch(line_protocol);

        let schema = batch.schema(Projection::All).unwrap();
        let record_batch = batch.to_arrow(Projection::All).unwrap();

        self.with_record_batch(record_batch)
            .with_table(table)
            .with_schema(schema)
    }

    fn with_record_batch(mut self, record_batch: RecordBatch) -> Self {
        self.record_batch = Some(record_batch);
        self
    }

    fn with_table(mut self, table: String) -> Self {
        self.table = Some(table);
        self
    }

    fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Specify the minimum time for the parquet file metadata.
    pub fn with_min_time(mut self, min_time: i64) -> Self {
        self.min_time = min_time;
        self
    }

    /// Specify the maximum time for the parquet file metadata.
    pub fn with_max_time(mut self, max_time: i64) -> Self {
        self.max_time = max_time;
        self
    }

    /// Specify the creation time for the parquet file metadata.
    pub fn with_creation_time(mut self, creation_time: iox_time::Time) -> Self {
        self.creation_time = creation_time.timestamp_nanos();
        self
    }

    /// specify max creation time of all L0 this file was created from
    pub fn with_max_l0_created_at(mut self, time: iox_time::Time) -> Self {
        self.max_l0_created_at = time.timestamp_nanos();
        self
    }

    /// Specify the compaction level for the parquet file metadata.
    pub fn with_compaction_level(mut self, compaction_level: CompactionLevel) -> Self {
        self.compaction_level = compaction_level;
        self
    }

    /// Specify whether the parquet file should be marked as deleted or not.
    pub fn with_to_delete(mut self, to_delete: bool) -> Self {
        self.to_delete = to_delete;
        self
    }

    /// Specify the number of rows in this parquet file. If line protocol/record batch are also
    /// set, this will panic! Only use this when you're not specifying any rows!
    pub fn with_row_count(mut self, row_count: usize) -> Self {
        self.row_count = Some(row_count);
        self
    }

    /// Specify the size override to use for a CompactorParquetFile
    pub fn with_size_override(mut self, size_override: i64) -> Self {
        self.size_override = Some(size_override);
        self
    }

    /// Specify the file size to use for a CompactorParquetFile
    pub fn with_file_size_bytes(mut self, file_size_bytes: u64) -> Self {
        self.file_size_bytes = Some(file_size_bytes);
        self
    }
}

async fn update_catalog_sort_key_if_needed<R>(
    repos: &mut R,
    id: &TransitionPartitionId,
    sort_key: SortKey,
) where
    R: RepoCollection + ?Sized,
{
    // Fetch the latest partition info from the catalog
    let partition = partition_lookup(repos, id).await.unwrap().unwrap();

    // Similarly to what the ingester does, if there's an existing sort key in the catalog, add new
    // columns onto the end
    match partition.sort_key() {
        Some(catalog_sort_key) => {
            let new_sort_key = sort_key.to_columns().collect::<Vec<_>>();
            let (_metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &new_sort_key);
            if let Some(new_sort_key) = update {
                let new_columns = new_sort_key.to_columns().collect::<Vec<_>>();
                debug!(
                    "Updating sort key from {:?} to {:?}",
                    catalog_sort_key.to_columns().collect::<Vec<_>>(),
                    &new_columns,
                );
                repos
                    .partitions()
                    .cas_sort_key(
                        id,
                        Some(
                            catalog_sort_key
                                .to_columns()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>(),
                        ),
                        &new_columns,
                    )
                    .await
                    .unwrap();
            }
        }
        None => {
            let new_columns = sort_key.to_columns().collect::<Vec<_>>();
            debug!("Updating sort key from None to {:?}", &new_columns);
            repos
                .partitions()
                .cas_sort_key(id, None, &new_columns)
                .await
                .unwrap();
        }
    }
}

/// Create parquet file and return file size.
async fn create_parquet_file(
    store: ParquetStorage,
    partition_id: &TransitionPartitionId,
    metadata: &IoxMetadata,
    record_batch: RecordBatch,
) -> usize {
    let stream = Box::pin(MemoryStream::new(vec![record_batch]));
    let (_meta, file_size) = store
        .upload(stream, partition_id, metadata, unbounded_memory_pool())
        .await
        .expect("persisting parquet file should succeed");
    file_size
}

/// A test parquet file of the catalog
#[allow(missing_docs)]
pub struct TestParquetFile {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub partition: Arc<TestPartition>,
    pub parquet_file: ParquetFile,
    pub size_override: Option<i64>,
}

impl From<TestParquetFile> for ParquetFile {
    fn from(tpf: TestParquetFile) -> Self {
        let TestParquetFile { parquet_file, .. } = tpf;

        parquet_file
    }
}

impl TestParquetFile {
    /// Make the parquet file deletable
    pub async fn flag_for_delete(&self) {
        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .parquet_files()
            .create_upgrade_delete(&[self.parquet_file.id], &[], &[], CompactionLevel::Initial)
            .await
            .unwrap();
    }

    /// Get Parquet file schema.
    pub async fn schema(&self) -> Schema {
        let table_columns = self.table.catalog_columns().await;
        let column_id_lookup = table_columns.id_map();
        let selection: Vec<_> = self
            .parquet_file
            .column_set
            .iter()
            .map(|id| *column_id_lookup.get(id).unwrap())
            .collect();
        let table_schema: Schema = table_columns.clone().try_into().unwrap();
        table_schema.select_by_names(&selection).unwrap()
    }
}

/// Return the current time
pub fn now() -> Time {
    Time::from_timestamp(0, 0).unwrap()
}

/// Sort arrow record batch into arrow record batch and sort key.
fn sort_batch(record_batch: RecordBatch, schema: &Schema) -> (RecordBatch, SortKey) {
    // calculate realistic sort key
    let sort_key = compute_sort_key(schema, std::iter::once(&record_batch));

    // set up sorting
    let mut sort_columns = Vec::with_capacity(record_batch.num_columns());
    let mut reverse_index: Vec<_> = (0..record_batch.num_columns()).map(|_| None).collect();
    for (column_name, _options) in sort_key.iter() {
        let index = record_batch
            .schema()
            .column_with_name(column_name.as_ref())
            .unwrap()
            .0;
        reverse_index[index] = Some(sort_columns.len());
        sort_columns.push(SortColumn {
            values: Arc::clone(record_batch.column(index)),
            options: Some(SortOptions::default()),
        });
    }
    for (index, reverse_index) in reverse_index.iter_mut().enumerate() {
        if reverse_index.is_none() {
            *reverse_index = Some(sort_columns.len());
            sort_columns.push(SortColumn {
                values: Arc::clone(record_batch.column(index)),
                options: None,
            });
        }
    }

    // execute sorting
    let arrays = lexsort(&sort_columns, None).unwrap();

    // re-create record batch
    let arrays: Vec<_> = reverse_index
        .into_iter()
        .map(|index| {
            let index = index.unwrap();
            Arc::clone(&arrays[index])
        })
        .collect();
    let record_batch = RecordBatch::try_new(record_batch.schema(), arrays).unwrap();

    (record_batch, sort_key)
}

fn dedup_batch(record_batch: RecordBatch, sort_key: &SortKey) -> RecordBatch {
    let schema = record_batch.schema();
    let sort_keys = arrow_sort_key_exprs(sort_key, &schema);
    let mut deduplicator = RecordBatchDeduplicator::new(sort_keys, Count::default(), None);

    let mut batches = vec![deduplicator.push(record_batch).unwrap()];
    if let Some(batch) = deduplicator.finish().unwrap() {
        batches.push(batch);
    }

    arrow::compute::concat_batches(&schema, &batches).unwrap()
}
