//! Namespace within the whole database.
use crate::{cache::CatalogCache, chunk::ParquetChunkAdapter};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types2::{ChunkSummary, NamespaceId, PartitionAddr};
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use db::{access::QueryCatalogAccess, catalog::Catalog as DbCatalog, chunk::DbChunk};
use iox_catalog::interface::{get_schema_by_name, Catalog};
use job_registry::JobRegistry;
use object_store::ObjectStore;
use observability_deps::tracing::{info, warn};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate};
use query::{
    exec::{ExecutionContextProvider, Executor, ExecutorType, IOxExecutionContext},
    QueryCompletedToken, QueryDatabase, QueryText,
};
use schema::Schema;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use time::TimeProvider;
use trace::ctx::SpanContext;

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
///
/// # Data Structures & Sync
/// The in-memory data structure that is used for queries is the [`DbCatalog`] (via [`QueryCatalogAccess`]). The
/// main (and currently only) cluster-wide data source is the [IOx Catalog](Catalog). The cluster-wide view and
/// in-memory data structure are synced regularly via [`sync`](Self::sync).
///
/// To speed up the sync process and reduce the load on the [IOx Catalog](Catalog) we try to use rather large-scoped
/// queries as well as a [`CatalogCache`].
#[derive(Debug)]
pub struct QuerierNamespace {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// The catalog.
    catalog: Arc<dyn Catalog>,

    /// Catalog IO cache.
    catalog_cache: Arc<CatalogCache>,

    /// Old-gen DB catalog.
    db_catalog: Arc<DbCatalog>,

    /// Adapter to create old-gen chunks.
    chunk_adapter: ParquetChunkAdapter,

    /// ID of this namespace.
    id: NamespaceId,

    /// Name of this namespace.
    name: Arc<str>,

    /// Catalog interface for query
    catalog_access: Arc<QueryCatalogAccess>,

    /// Executor for queries.
    exec: Arc<Executor>,
}

impl QuerierNamespace {
    /// Create new, empty namespace.
    ///
    /// You may call [`sync`](Self::sync) to fill the namespace with chunks.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        name: Arc<str>,
        id: NamespaceId,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        exec: Arc<Executor>,
    ) -> Self {
        let catalog = catalog_cache.catalog();
        let db_catalog = Arc::new(DbCatalog::new(
            Arc::clone(&name),
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));

        // no real job registration system
        let jobs = Arc::new(JobRegistry::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));
        let catalog_access = Arc::new(QueryCatalogAccess::new(
            name.to_string(),
            Arc::clone(&db_catalog),
            jobs,
            Arc::clone(&time_provider),
            &metric_registry,
        ));

        Self {
            backoff_config: BackoffConfig::default(),
            catalog,
            catalog_cache: Arc::clone(&catalog_cache),
            db_catalog,
            chunk_adapter: ParquetChunkAdapter::new(
                catalog_cache,
                object_store,
                metric_registry,
                time_provider,
            ),
            id,
            name,
            catalog_access,
            exec,
        }
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Sync entire namespace state.
    ///
    /// This includes:
    /// - tables
    /// - schemas
    /// - partitions
    /// - chunks
    ///
    /// Should be called regularly.
    pub async fn sync(&self) {
        self.sync_tables_and_schemas().await;
        self.sync_partitions().await;
        self.sync_chunks().await;
    }

    /// Sync tables and schemas.
    async fn sync_tables_and_schemas(&self) {
        let catalog_schema_desired = Backoff::new(&self.backoff_config)
            .retry_all_errors("get schema", || async {
                let mut repos = self.catalog.repositories().await;
                match get_schema_by_name(&self.name, repos.as_mut()).await {
                    Ok(schema) => Ok(Some(schema)),
                    Err(iox_catalog::interface::Error::NamespaceNotFound { .. }) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await
            .expect("retry forever");
        let catalog_schema_desired = match catalog_schema_desired {
            Some(schema) => schema,
            None => {
                warn!(
                    namespace = self.name.as_ref(),
                    "Cannot sync namespace because it is gone",
                );
                return;
            }
        };

        let table_names_actual: HashSet<_> = self.db_catalog.table_names().into_iter().collect();
        let to_delete: Vec<_> = table_names_actual
            .iter()
            .filter_map(|table| {
                (!catalog_schema_desired.tables.contains_key(table)).then(|| table.clone())
            })
            .collect();
        let to_add: Vec<_> = catalog_schema_desired
            .tables
            .keys()
            .filter_map(|table| (!table_names_actual.contains(table)).then(|| table.clone()))
            .collect();
        info!(
            add = to_add.len(),
            delete = to_delete.len(),
            actual = table_names_actual.len(),
            desired = catalog_schema_desired.tables.len(),
            namespace = self.name.as_ref(),
            "Syncing tables",
        );

        for _name in to_delete {
            // TODO: implement and test table deletion
            unimplemented!("table deletion");
        }

        for name in to_add {
            // we don't need the returned lock so we immediately drop it (otherwise clippy will also complain)
            drop(self.db_catalog.get_or_create_table(name));
        }

        for (name, table_schema) in catalog_schema_desired.tables {
            let table = match self.db_catalog.table(&name) {
                Ok(table) => table,
                Err(e) => {
                    // this might happen if some other process (e.g. management API) just removed the table
                    warn!(
                        %e,
                        namespace = self.name.as_ref(),
                        table = name.as_str(),
                        "Cannot check table schema",
                    );
                    continue;
                }
            };

            let desired_schema = Schema::try_from(table_schema).expect("cannot build schema");

            let schema = table.schema();
            let schema = schema.upgradable_read();
            if schema.as_ref() != &desired_schema {
                let mut schema = RwLockUpgradableReadGuard::upgrade(schema);
                info!(
                    namespace = self.name.as_ref(),
                    table = name.as_str(),
                    "table schema update",
                );
                *schema = Arc::new(desired_schema);
            }
        }
    }

    async fn sync_partitions(&self) {
        let partitions = Backoff::new(&self.backoff_config)
            .retry_all_errors("get schema", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .list_by_namespace(self.id)
                    .await
            })
            .await
            .expect("retry forever");

        let mut desired_partitions = HashSet::with_capacity(partitions.len());
        for partition in partitions {
            let table = self.catalog_cache.table_name(partition.table_id).await;
            let key = self.catalog_cache.old_gen_partition_key(partition.id).await;
            desired_partitions.insert((table, key));
        }

        let actual_partitions: HashSet<_> = self
            .db_catalog
            .partitions()
            .into_iter()
            .map(|p| {
                let p = p.read();
                let addr = p.addr();
                (
                    Arc::clone(&addr.table_name),
                    Arc::clone(&addr.partition_key),
                )
            })
            .collect();

        let to_delete: Vec<_> = actual_partitions
            .iter()
            .filter(|x| !desired_partitions.contains(x))
            .cloned()
            .collect();
        let to_add: Vec<_> = desired_partitions
            .iter()
            .filter(|x| !actual_partitions.contains(x))
            .cloned()
            .collect();
        info!(
            add = to_add.len(),
            delete = to_delete.len(),
            actual = actual_partitions.len(),
            desired = desired_partitions.len(),
            namespace = self.name.as_ref(),
            "Syncing partitions",
        );

        // Map table name to two lists of "old gen" partition keys (`<sequencer_id>-<partition_key>`), one with
        // partiions to add (within that table) and one with partiions to delete (within that table).
        //
        // The per-table grouping is done so that we don't need to lock the table for every partition we want to add/delete.
        let mut per_table_add_delete: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
        for (table, key) in to_add {
            per_table_add_delete.entry(table).or_default().0.push(key);
        }
        for (table, key) in to_delete {
            per_table_add_delete.entry(table).or_default().1.push(key);
        }

        for (table, (to_add, to_delete)) in per_table_add_delete {
            let mut table = match self.db_catalog.table_mut(Arc::clone(&table)) {
                Ok(table) => table,
                Err(e) => {
                    // this might happen if some other process (e.g. management API) just removed the table
                    warn!(
                        %e,
                        namespace = self.name.as_ref(),
                        table = table.as_ref(),
                        "Cannot add/remove partitions to/from table",
                    );
                    continue;
                }
            };

            for key in to_add {
                table.get_or_create_partition(key);
            }

            for _key in to_delete {
                // TODO: implement partition deletation (currently iox_catalog cannot delete partitions)
                unimplemented!("partition deletion");
            }
        }
    }

    async fn sync_chunks(&self) {
        let parquet_files = Backoff::new(&self.backoff_config)
            .retry_all_errors("get parquet files", || async {
                self.catalog
                    .repositories()
                    .await
                    .parquet_files()
                    .list_by_namespace_not_to_delete(self.id)
                    .await
            })
            .await
            .expect("retry forever");

        let mut desired_chunks: HashMap<_, _> = HashMap::with_capacity(parquet_files.len());
        for parquet_file in parquet_files {
            let addr = self.chunk_adapter.old_gen_chunk_addr(&parquet_file).await;
            desired_chunks.insert(addr, parquet_file);
        }

        let actual_chunk_addresses: HashSet<_> = self
            .db_catalog
            .chunks()
            .into_iter()
            .map(|c| {
                let c = c.read();
                c.addr().clone()
            })
            .collect();

        let to_add: Vec<_> = desired_chunks
            .iter()
            .filter_map(|(addr, file)| {
                (!actual_chunk_addresses.contains(addr)).then(|| (addr.clone(), file.clone()))
            })
            .collect();
        let to_delete: Vec<_> = actual_chunk_addresses
            .iter()
            .filter(|addr| !desired_chunks.contains_key(addr))
            .cloned()
            .collect();
        info!(
            add = to_add.len(),
            delete = to_delete.len(),
            actual = actual_chunk_addresses.len(),
            desired = desired_chunks.len(),
            namespace = self.name.as_ref(),
            "Syncing chunks",
        );

        // prepare to-be-added chunks, so we don't have to perform any IO while holding locks
        let to_add2 = to_add;
        let mut to_add = Vec::with_capacity(to_add2.len());
        for (addr, file) in to_add2 {
            let parts = self.chunk_adapter.new_catalog_chunk_parts(file).await;
            to_add.push((addr, parts));
        }

        // group by table and partition to reduce locking attempts
        // table name => (partition key => (list of parts to be added, list of chunk IDs to be removed))
        let mut per_partition_add_delete: HashMap<_, HashMap<_, (Vec<_>, Vec<_>)>> = HashMap::new();
        for (addr, file) in to_add {
            per_partition_add_delete
                .entry(addr.table_name)
                .or_default()
                .entry(addr.partition_key)
                .or_default()
                .0
                .push(file);
        }
        for addr in to_delete {
            per_partition_add_delete
                .entry(addr.table_name)
                .or_default()
                .entry(addr.partition_key)
                .or_default()
                .1
                .push(addr.chunk_id);
        }

        for (table, sub) in per_partition_add_delete {
            let table = match self.db_catalog.table_mut(Arc::clone(&table)) {
                Ok(table) => table,
                Err(e) => {
                    // this might happen if some other process (e.g. management API) just removed the table
                    warn!(
                        %e,
                        namespace = self.name.as_ref(),
                        table = table.as_ref(),
                        "Cannot add/remove chunks to/from table",
                    );
                    continue;
                }
            };

            for (partition, (to_add, to_delete)) in sub {
                let partition = match table.partition(&partition) {
                    Some(partition) => Arc::clone(partition),
                    None => {
                        // this might happen if some other process (e.g. management API) just removed the table
                        warn!(
                            namespace = self.name.as_ref(),
                            table = table.name().as_ref(),
                            partition = partition.as_ref(),
                            "Cannot add/remove chunks to/from partition",
                        );
                        continue;
                    }
                };
                let mut partition = partition.write();

                for (addr, chunk_order, metadata, chunk) in to_add {
                    let chunk_id = addr.chunk_id;
                    partition.insert_object_store_only_chunk(
                        chunk_id,
                        chunk_order,
                        metadata,
                        chunk,
                    );
                }

                for chunk_id in to_delete {
                    // it's OK if the chunk is already gone
                    partition.force_drop_chunk(chunk_id).ok();
                }
            }
        }
    }
}

impl QueryDatabaseMeta for QuerierNamespace {
    fn table_names(&self) -> Vec<String> {
        self.catalog_access.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.catalog_access.table_schema(table_name)
    }
}

#[async_trait]
impl QueryDatabase for QuerierNamespace {
    type Chunk = DbChunk;

    fn partition_addrs(&self) -> Vec<PartitionAddr> {
        self.catalog_access.partition_addrs()
    }

    fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.catalog_access.chunks(table_name, predicate)
    }

    fn chunk_summaries(&self) -> Vec<ChunkSummary> {
        self.catalog_access.chunk_summaries()
    }

    fn record_query(
        &self,
        ctx: &IOxExecutionContext,
        query_type: impl Into<String>,
        query_text: QueryText,
    ) -> QueryCompletedToken {
        self.catalog_access
            .record_query(ctx, query_type, query_text)
    }
}

impl CatalogProvider for QuerierNamespace {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_access.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.catalog_access.schema(name)
    }
}

impl ExecutionContextProvider for QuerierNamespace {
    fn new_query_context(self: &Arc<Self>, span_ctx: Option<SpanContext>) -> IOxExecutionContext {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::<Self>::clone(self))
            .with_span_context(span_ctx)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{TestCatalog, TestNamespace, TestParquetFile};
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_sorted_eq;
    use data_types2::{ChunkAddr, ChunkId, ColumnType};
    use query::frontend::sql::SqlQueryPlanner;
    use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_sync_namespace_gone() {
        let catalog = TestCatalog::new();

        let querier_namespace = QuerierNamespace::new(
            Arc::new(CatalogCache::new(catalog.catalog())),
            "ns".into(),
            NamespaceId::new(1),
            catalog.metric_registry(),
            catalog.object_store(),
            catalog.time_provider(),
            catalog.exec(),
        );

        // The container (`QuerierDatabase`) should prune the namespace if it's gone, however the `sync` might still be
        // in-progress and must not block or panic.
        querier_namespace.sync().await;
    }

    #[tokio::test]
    async fn test_sync_tables() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;

        let querier_namespace = querier_namespace(&catalog, &ns);

        querier_namespace.sync().await;
        assert_eq!(
            querier_namespace.db_catalog.table_names(),
            Vec::<String>::new()
        );

        ns.create_table("table1").await;
        ns.create_table("table2").await;
        querier_namespace.sync().await;
        assert_eq!(
            sorted(querier_namespace.db_catalog.table_names()),
            vec![String::from("table1"), String::from("table2")]
        );

        ns.create_table("table3").await;
        querier_namespace.sync().await;
        assert_eq!(
            sorted(querier_namespace.db_catalog.table_names()),
            vec![
                String::from("table1"),
                String::from("table2"),
                String::from("table3")
            ]
        );
    }

    #[tokio::test]
    async fn test_sync_schemas() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;

        let querier_namespace = querier_namespace(&catalog, &ns);

        querier_namespace.sync().await;
        let expected_schema = SchemaBuilder::new().build().unwrap();
        let actual_schema = schema(&querier_namespace, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        table.create_column("col1", ColumnType::I64).await;
        table.create_column("col2", ColumnType::Bool).await;
        table.create_column("col3", ColumnType::Tag).await;
        querier_namespace.sync().await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let actual_schema = schema(&querier_namespace, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        table.create_column("col4", ColumnType::Tag).await;
        table.create_column("col5", ColumnType::Time).await;
        querier_namespace.sync().await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .influx_column("col4", InfluxColumnType::Tag)
            .influx_column("col5", InfluxColumnType::Timestamp)
            .build()
            .unwrap();
        let actual_schema = schema(&querier_namespace, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        // schema not updated => Arc not changed
        querier_namespace.sync().await;
        let actual_schema2 = schema(&querier_namespace, "table");
        assert!(Arc::ptr_eq(&actual_schema, &actual_schema2));
    }

    #[tokio::test]
    async fn test_sync_partitions() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table1 = ns.create_table("table1").await;
        let table2 = ns.create_table("table2").await;

        let querier_namespace = querier_namespace(&catalog, &ns);

        querier_namespace.sync().await;
        assert_eq!(partitions(&querier_namespace), vec![],);

        table1.create_partition("k2", 1).await;
        table1.create_partition("k1", 2).await;
        table2.create_partition("k1", 1).await;
        querier_namespace.sync().await;
        assert_eq!(
            partitions(&querier_namespace),
            vec![
                (String::from("table1"), String::from("1-k2")),
                (String::from("table1"), String::from("2-k1")),
                (String::from("table2"), String::from("1-k1")),
            ],
        );
        let partition_a = querier_namespace
            .db_catalog
            .partition("table1", "1-k2")
            .unwrap();

        table1.create_partition("k2", 2).await;
        querier_namespace.sync().await;
        assert_eq!(
            partitions(&querier_namespace),
            vec![
                (String::from("table1"), String::from("1-k2")),
                (String::from("table1"), String::from("2-k1")),
                (String::from("table1"), String::from("2-k2")),
                (String::from("table2"), String::from("1-k1")),
            ],
        );
        let partition_b = querier_namespace
            .db_catalog
            .partition("table1", "1-k2")
            .unwrap();
        assert!(Arc::ptr_eq(&partition_a, &partition_b));
    }

    #[tokio::test]
    async fn test_sync_chunks() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;
        let partition = table.create_partition("k", 1).await;

        let querier_namespace = querier_namespace(&catalog, &ns);
        querier_namespace.sync().await;
        assert_eq!(chunks(&querier_namespace), vec![],);

        let file1 = partition.create_parquet_file("table foo=1 11").await;
        let file2 = partition.create_parquet_file("table foo=2 22").await;
        querier_namespace.sync().await;
        let partition_addr = PartitionAddr {
            db_name: Arc::from("ns"),
            table_name: Arc::from("table"),
            partition_key: Arc::from("1-k"),
        };
        assert_eq!(
            chunks(&querier_namespace),
            vec![
                ChunkAddr::new(&partition_addr, chunk_id(&file1)),
                ChunkAddr::new(&partition_addr, chunk_id(&file2)),
            ],
        );
        let chunk_a = querier_namespace
            .db_catalog
            .chunk("table", "1-k", chunk_id(&file1))
            .unwrap()
            .0;

        file2.flag_for_delete().await;
        let file3 = partition.create_parquet_file("table foo=3 33").await;
        querier_namespace.sync().await;
        assert_eq!(
            chunks(&querier_namespace),
            vec![
                ChunkAddr::new(&partition_addr, chunk_id(&file1)),
                ChunkAddr::new(&partition_addr, chunk_id(&file3)),
            ],
        );
        let chunk_b = querier_namespace
            .db_catalog
            .chunk("table", "1-k", chunk_id(&file1))
            .unwrap()
            .0;
        assert!(Arc::ptr_eq(&chunk_a, &chunk_b));
    }

    #[tokio::test]
    async fn test_query() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;

        let table_cpu = ns.create_table("cpu").await;
        let table_mem = ns.create_table("mem").await;

        table_cpu.create_column("host", ColumnType::Tag).await;
        table_cpu.create_column("time", ColumnType::Time).await;
        table_cpu.create_column("load", ColumnType::F64).await;
        table_cpu.create_column("foo", ColumnType::I64).await;
        table_mem.create_column("host", ColumnType::Tag).await;
        table_mem.create_column("time", ColumnType::Time).await;
        table_mem.create_column("perc", ColumnType::F64).await;

        let partition_cpu_a_1 = table_cpu.create_partition("a", 1).await;
        let partition_cpu_a_2 = table_cpu.create_partition("a", 2).await;
        let partition_cpu_b_1 = table_cpu.create_partition("b", 1).await;
        let partition_mem_c_1 = table_mem.create_partition("c", 1).await;
        let partition_mem_c_2 = table_mem.create_partition("c", 2).await;

        partition_cpu_a_1
            .create_parquet_file("cpu,host=a load=1 11")
            .await;
        partition_cpu_a_1
            .create_parquet_file("cpu,host=a load=2 22")
            .await
            .flag_for_delete()
            .await;
        partition_cpu_a_1
            .create_parquet_file("cpu,host=a load=3 33")
            .await;
        partition_cpu_a_2
            .create_parquet_file("cpu,host=a load=4 10001")
            .await;
        partition_cpu_b_1
            .create_parquet_file("cpu,host=b load=5 11")
            .await;
        partition_mem_c_1
            .create_parquet_file("mem,host=c perc=50 11\nmem,host=c perc=51 12")
            .await;
        partition_mem_c_2
            .create_parquet_file("mem,host=c perc=50 1001")
            .await
            .flag_for_delete()
            .await;

        let querier_namespace = Arc::new(querier_namespace(&catalog, &ns));
        querier_namespace.sync().await;

        assert_query(
            &querier_namespace,
            "SELECT * FROM cpu ORDER BY host,time",
            &[
                "+-----+------+------+--------------------------------+",
                "| foo | host | load | time                           |",
                "+-----+------+------+--------------------------------+",
                "|     | a    | 1    | 1970-01-01T00:00:00.000000011Z |",
                "|     | a    | 3    | 1970-01-01T00:00:00.000000033Z |",
                "|     | a    | 4    | 1970-01-01T00:00:00.000010001Z |",
                "|     | b    | 5    | 1970-01-01T00:00:00.000000011Z |",
                "+-----+------+------+--------------------------------+",
            ],
        )
        .await;
        assert_query(
            &querier_namespace,
            "SELECT * FROM mem ORDER BY host,time",
            &[
                "+------+------+--------------------------------+",
                "| host | perc | time                           |",
                "+------+------+--------------------------------+",
                "| c    | 50   | 1970-01-01T00:00:00.000000011Z |",
                "| c    | 51   | 1970-01-01T00:00:00.000000012Z |",
                "+------+------+--------------------------------+",
            ],
        )
        .await;
    }

    fn querier_namespace(catalog: &Arc<TestCatalog>, ns: &Arc<TestNamespace>) -> QuerierNamespace {
        QuerierNamespace::new(
            Arc::new(CatalogCache::new(catalog.catalog())),
            ns.namespace.name.clone().into(),
            ns.namespace.id,
            catalog.metric_registry(),
            catalog.object_store(),
            catalog.time_provider(),
            catalog.exec(),
        )
    }

    fn sorted<T>(mut v: Vec<T>) -> Vec<T>
    where
        T: Ord,
    {
        v.sort();
        v
    }

    fn schema(querier_namespace: &QuerierNamespace, table: &str) -> Arc<Schema> {
        Arc::clone(
            &querier_namespace
                .db_catalog
                .table(table)
                .unwrap()
                .schema()
                .read(),
        )
    }

    fn partitions(querier_namespace: &QuerierNamespace) -> Vec<(String, String)> {
        sorted(
            querier_namespace
                .db_catalog
                .partitions()
                .into_iter()
                .map(|p| {
                    let p = p.read();
                    let addr = p.addr();
                    (addr.table_name.to_string(), addr.partition_key.to_string())
                })
                .collect(),
        )
    }

    fn chunks(querier_namespace: &QuerierNamespace) -> Vec<ChunkAddr> {
        sorted(
            querier_namespace
                .db_catalog
                .chunks()
                .into_iter()
                .map(|c| {
                    let c = c.read();
                    c.addr().clone()
                })
                .collect(),
        )
    }

    fn chunk_id(file: &Arc<TestParquetFile>) -> ChunkId {
        ChunkId::from(Uuid::from_u128(file.parquet_file.id.get() as _))
    }

    async fn assert_query(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        expected_lines: &[&str],
    ) {
        let planner = SqlQueryPlanner::default();
        let ctx = querier_namespace.new_query_context(None);

        let physical_plan = planner
            .query(sql, &ctx)
            .await
            .expect("built plan successfully");

        let results: Vec<RecordBatch> = ctx.collect(physical_plan).await.expect("Running plan");
        assert_batches_sorted_eq!(expected_lines, &results);
    }
}
