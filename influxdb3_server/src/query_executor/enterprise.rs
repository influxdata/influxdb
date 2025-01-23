use std::{any::Any, collections::HashMap, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
};
use datafusion_util::config::DEFAULT_SCHEMA;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_config::EnterpriseConfig;
use influxdb3_enterprise_compactor::compacted_data::CompactedDataSystemTableView;
use influxdb3_internal_api::query_executor::{QueryExecutor, QueryExecutorError};
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use influxdb_influxql_parser::statement::Statement;
use iox_query::{
    exec::{Executor, IOxSessionContext, QueryConfig},
    query_log::{QueryCompletedToken, QueryLog, QueryLogEntries, QueryText, StateReceived},
    QueryDatabase, QueryNamespace,
};
use iox_query_params::StatementParams;
use iox_system_tables::SystemTableProvider;
use metric::Registry;
use observability_deps::tracing::{debug, info};
use tokio::sync::{RwLock, Semaphore};
use trace::span::{Span, SpanExt};
use trace::{ctx::SpanContext, span::SpanRecorder};
use trace_http::ctx::RequestLogContext;
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

use crate::{
    query_executor::{acquire_semaphore, query_database_sql},
    system_tables::{
        enterprise::{
            compacted_data::CompactedDataTable, compaction_events::CompactionEventsSysTable,
            COMPACTED_DATA_TABLE_NAME, COMPACTION_EVENTS_TABLE_NAME,
        },
        SystemSchemaProvider, SYSTEM_SCHEMA_NAME,
    },
};

use super::{
    AllSystemSchemaTablesProvider, CreateDatabaseArgs, CreateQueryExecutorArgs, Database,
    QueryExecutorImpl,
};

#[derive(Debug, Clone)]
pub struct QueryExecutorEnterprise {
    core: QueryExecutorImpl,
    compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
    enterprise_config: Arc<RwLock<EnterpriseConfig>>,
}

impl QueryExecutorEnterprise {
    pub fn new(
        core_args: CreateQueryExecutorArgs,
        compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
        enterprise_config: Arc<RwLock<EnterpriseConfig>>,
    ) -> Self {
        Self {
            core: QueryExecutorImpl::new(core_args),
            compacted_data,
            enterprise_config,
        }
    }
}

#[async_trait]
impl QueryExecutor for QueryExecutorEnterprise {
    async fn query_sql(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        // this can't call its core impl because for SQL we need
        // to ensure the enterprise-specific system tables are
        // injected in the QueryNamespace provider, i.e., that we
        // call `QueryExecutor::get_db_namespace` on self, and not
        // on self.core.
        info!(%database, %query, ?params, "executing sql query");
        let db = self.get_db_namespace(database, &span_ctx).await?;
        query_database_sql(
            db,
            query,
            params,
            span_ctx,
            external_span_ctx,
            Arc::clone(&self.core.telemetry_store),
        )
        .await
    }

    async fn query_influxql(
        &self,
        database: &str,
        query: &str,
        influxql_statement: Statement,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        self.core
            .query_influxql(
                database,
                query,
                influxql_statement,
                params,
                span_ctx,
                external_span_ctx,
            )
            .await
    }

    fn show_databases(
        &self,
        include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        self.core.show_databases(include_deleted)
    }

    async fn show_retention_policies(
        &self,
        database: Option<&str>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        self.core.show_retention_policies(database, span_ctx).await
    }

    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)> {
        // NB: This clone is required to get compiler to be happy
        //     to convert `self` to dyn QueryDatabase. This wasn't
        //     possible without getting owned value of self.
        //     TODO: see if this clone can be removed satisfying
        //           grpc setup in `make_flight_server`
        let cloned_self = (*self).clone();
        Arc::new(cloned_self) as _
    }
}

// This implementation is for the Flight service
#[async_trait]
impl QueryDatabase for QueryExecutorEnterprise {
    async fn namespace(
        &self,
        name: &str,
        span: Option<Span>,
        // We expose the `system` tables by default in the monolithic versions of InfluxDB 3
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        let _span_recorder = SpanRecorder::new(span);

        let db_schema = self.core.catalog.db_schema(name).ok_or_else(|| {
            DataFusionError::External(Box::new(QueryExecutorError::DatabaseNotFound {
                db_name: name.into(),
            }))
        })?;

        let system_schema_provider = Arc::new(SystemSchemaProvider::AllSystemSchemaTables(
            AllSystemSchemaTablesProvider::new(
                Arc::clone(&db_schema),
                Arc::clone(&self.core.query_log),
                Arc::clone(&self.core.write_buffer),
                Arc::clone(&self.core.sys_events_store),
            )
            .add_enterprise_tables(
                self.compacted_data.clone(),
                Arc::clone(&self.enterprise_config),
                Arc::clone(&self.core.sys_events_store),
            ),
        ));
        Ok(Some(Arc::new(Database::new(CreateDatabaseArgs {
            db_schema,
            write_buffer: Arc::clone(&__self.core.write_buffer),
            exec: Arc::clone(&__self.core.exec),
            datafusion_config: Arc::clone(&__self.core.datafusion_config),
            query_log: Arc::clone(&__self.core.query_log),
            system_schema_provider,
        }))))
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        acquire_semaphore(Arc::clone(&self.core.query_execution_semaphore), span).await
    }

    fn query_log(&self) -> QueryLogEntries {
        self.core.query_log.entries()
    }
}

#[derive(Debug)]
pub struct CompactionSysTableQueryExecutorArgs {
    pub exec: Arc<Executor>,
    pub metrics: Arc<Registry>,
    pub datafusion_config: Arc<HashMap<String, String>>,
    pub query_log_size: usize,
    pub telemetry_store: Arc<TelemetryStore>,
    pub compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
    pub sys_events_store: Arc<SysEventStore>,
}

#[derive(Debug, Clone)]
pub struct CompactionSysTableQueryExecutorImpl {
    pub exec: Arc<Executor>,
    pub datafusion_config: Arc<HashMap<String, String>>,
    pub query_execution_semaphore: Arc<InstrumentedAsyncSemaphore>,
    pub query_log: Arc<QueryLog>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
    pub sys_events_store: Arc<SysEventStore>,
}

impl CompactionSysTableQueryExecutorImpl {
    pub fn new(
        CompactionSysTableQueryExecutorArgs {
            exec,
            metrics,
            datafusion_config,
            query_log_size,
            telemetry_store,
            compacted_data,
            sys_events_store,
        }: CompactionSysTableQueryExecutorArgs,
    ) -> Self {
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metrics,
            &[("semaphore", "query_execution")],
        ));
        let query_execution_semaphore =
            Arc::new(semaphore_metrics.new_semaphore(Semaphore::MAX_PERMITS));
        let query_log = Arc::new(QueryLog::new(
            query_log_size,
            Arc::new(iox_time::SystemProvider::new()),
        ));

        Self {
            exec: Arc::clone(&exec),
            query_log,
            query_execution_semaphore,
            datafusion_config: Arc::clone(&datafusion_config),
            telemetry_store: Arc::clone(&telemetry_store),
            compacted_data,
            sys_events_store: Arc::clone(&sys_events_store),
        }
    }
}

#[async_trait]
impl QueryExecutor for CompactionSysTableQueryExecutorImpl {
    async fn query_sql(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        info!(%database, %query, ?params, "executing sql query");
        let db = self
            .namespace(database, span_ctx.child_span("get database"), false)
            .await
            .map_err(|_| QueryExecutorError::DatabaseNotFound {
                db_name: database.to_string(),
            })?
            .ok_or_else(|| QueryExecutorError::DatabaseNotFound {
                db_name: database.to_string(),
            })?;
        query_database_sql(
            db,
            query,
            params,
            span_ctx,
            external_span_ctx,
            Arc::clone(&self.telemetry_store),
        )
        .await
    }

    async fn query_influxql(
        &self,
        _database: &str,
        _query: &str,
        _influxql_statement: Statement,
        _params: Option<StatementParams>,
        _span_ctx: Option<SpanContext>,
        _external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("query_influxql"))
    }

    fn show_databases(
        &self,
        _include_deleted: bool,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented("show_databases"))
    }

    async fn show_retention_policies(
        &self,
        _database: Option<&str>,
        _span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, QueryExecutorError> {
        Err(QueryExecutorError::MethodNotImplemented(
            "show_retention_policies",
        ))
    }

    fn upcast(&self) -> Arc<(dyn QueryDatabase + 'static)> {
        let cloned_self = (*self).clone();
        Arc::new(cloned_self)
    }
}

#[async_trait]
impl QueryDatabase for CompactionSysTableQueryExecutorImpl {
    /// Get namespace if it exists.
    ///
    /// System tables may contain debug information depending on `include_debug_info_tables`.
    async fn namespace(
        &self,
        name: &str,
        _span: Option<Span>,
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        // NOTE: Here we're not using `Catalog` instead a `CompactedCatalog` to find
        //       the db schema
        let compacted_data = self
            .compacted_data
            .clone()
            .ok_or_else(|| {
                DataFusionError::External(Box::new(QueryExecutorError::Anyhow(anyhow!("there was no compacted data available to the compaction system table query executor"))))
            })?
        ;
        let db_schema = compacted_data.catalog().db_schema(name).ok_or_else(|| {
            DataFusionError::External(Box::new(QueryExecutorError::DatabaseNotFound {
                db_name: name.into(),
            }))
        })?;
        let database = CompactionEventsQueryableDatabase::new(
            db_schema,
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
            Arc::clone(&self.query_log),
            compacted_data,
            Arc::clone(&self.sys_events_store),
        );
        Ok(Some(Arc::new(database)))
    }

    /// Acquire concurrency-limiting semapahore
    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        acquire_semaphore(Arc::clone(&self.query_execution_semaphore), span).await
    }

    /// Return the query log entries
    fn query_log(&self) -> QueryLogEntries {
        self.query_log.entries()
    }
}

#[derive(Debug)]
pub struct CompactionEventsQueryableDatabase {
    db_schema: Arc<DatabaseSchema>,
    exec: Arc<Executor>,
    datafusion_config: Arc<HashMap<String, String>>,
    query_log: Arc<QueryLog>,
    system_schema_provider: Arc<SystemSchemaProvider>,
}

#[async_trait]
impl QueryNamespace for CompactionEventsQueryableDatabase {
    fn retention_time_ns(&self) -> Option<i64> {
        None
    }

    fn record_query(
        &self,
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
    ) -> QueryCompletedToken<StateReceived> {
        let trace_id = span_ctx.map(|ctx| ctx.trace_id);
        let namespace_name: Arc<str> = Arc::from("influxdb3 edge");
        self.query_log.push(
            NamespaceId::new(0),
            namespace_name,
            query_type,
            query_text,
            query_params,
            trace_id,
        )
    }

    fn new_query_context(
        &self,
        span_ctx: Option<SpanContext>,
        _config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        let mut cfg = self
            .exec
            .new_session_config()
            .with_default_catalog(Arc::new(Self::from_namespace(self)))
            .with_span_context(span_ctx);

        for (k, v) in self.datafusion_config.as_ref() {
            cfg = cfg.with_config_option(k, v);
        }

        cfg.build()
    }

    fn new_extended_query_context(
        &self,
        _extension: std::option::Option<std::sync::Arc<(dyn iox_query::Extension + 'static)>>,
        _span_ctx: Option<SpanContext>,
        _query_config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        unimplemented!();
    }
}

/// No DEFAULT_SCHEMA support / QueryTable
impl CatalogProvider for CompactionEventsQueryableDatabase {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        debug!("CompactionEventsQueryableDatabase as CatalogProvider::schema_names");
        vec![DEFAULT_SCHEMA.to_string(), SYSTEM_SCHEMA_NAME.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        debug!(schema_name = %name, "CompactionEventsQueryableDatabase as CatalogProvider::schema");
        match name {
            SYSTEM_SCHEMA_NAME => Some(Arc::clone(&self.system_schema_provider) as _),
            _ => None,
        }
    }
}

impl CompactionEventsQueryableDatabase {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        exec: Arc<Executor>,
        datafusion_config: Arc<HashMap<String, String>>,
        query_log: Arc<QueryLog>,
        compacted_data: Arc<dyn CompactedDataSystemTableView>,
        sys_events_store: Arc<SysEventStore>,
    ) -> Self {
        let system_schema_provider = Arc::new(SystemSchemaProvider::CompactionSystemTables(
            CompactionSystemTablesProvider::new(
                Arc::clone(&db_schema),
                compacted_data,
                Arc::clone(&sys_events_store),
            ),
        ));
        Self {
            db_schema,
            exec,
            datafusion_config,
            query_log,
            system_schema_provider,
        }
    }

    fn from_namespace(db: &Self) -> Self {
        Self {
            db_schema: Arc::clone(&db.db_schema),
            exec: Arc::clone(&db.exec),
            datafusion_config: Arc::clone(&db.datafusion_config),
            query_log: Arc::clone(&db.query_log),
            system_schema_provider: Arc::clone(&db.system_schema_provider),
        }
    }
}

pub(crate) struct CompactionSystemTablesProvider {
    tables: HashMap<&'static str, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for CompactionSystemTablesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = self.tables.keys().copied().collect::<Vec<_>>();
        keys.sort_unstable();

        f.debug_struct("CompactionSystemTablesProvider")
            .field("tables", &keys.join(", "))
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for CompactionSystemTablesProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self
            .tables
            .keys()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl CompactionSystemTablesProvider {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        compacted_data: Arc<dyn CompactedDataSystemTableView>,
        sys_events_store: Arc<SysEventStore>,
    ) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();
        let compacted_data_table = Arc::new(SystemTableProvider::new(Arc::new(
            CompactedDataTable::new(db_schema, Some(compacted_data)),
        )));
        tables.insert(COMPACTED_DATA_TABLE_NAME, compacted_data_table);
        tables.insert(
            COMPACTION_EVENTS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                CompactionEventsSysTable::new(Arc::clone(&sys_events_store)),
            ))),
        );
        // let file_index_table = FileIndex
        Self { tables }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow_array::RecordBatch;
    use data_types::NamespaceName;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq, error::DataFusionError};
    use futures::TryStreamExt;
    use influxdb3_cache::{
        distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider,
        parquet_cache::test_cached_obj_store_and_oracle,
    };
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_enterprise_compactor::{
        catalog::CompactedCatalog,
        compacted_data::CompactedDataSystemTableView,
        sys_events::{
            catalog_fetched,
            compaction_completed::{self, PlanIdentifier},
            compaction_consumed, compaction_planned,
            snapshot_fetched::{FailedInfo, SuccessInfo},
            CompactionEventStore,
        },
    };
    use influxdb3_enterprise_data_layout::CompactedDataSystemTableQueryResult;
    use influxdb3_id::{DbId, ParquetFileId, TableId};
    use influxdb3_internal_api::query_executor::QueryExecutor;
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_telemetry::store::TelemetryStore;
    use influxdb3_wal::{Gen1Duration, WalConfig};
    use influxdb3_write::{
        persister::Persister,
        write_buffer::{persisted_files::PersistedFiles, WriteBufferImpl, WriteBufferImplArgs},
        ParquetFile, WriteBuffer,
    };
    use iox_time::{MockProvider, Time};
    use metric::Registry;
    use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStore};
    use observability_deps::tracing::debug;
    use tokio::sync::RwLock;

    use crate::query_executor::{
        enterprise::{
            CompactionSysTableQueryExecutorArgs, CompactionSysTableQueryExecutorImpl,
            QueryExecutorEnterprise,
        },
        tests::make_exec,
        CreateQueryExecutorArgs,
    };

    #[derive(Debug)]
    struct MockCompactedDataSysTable {
        catalog: CompactedCatalog,
    }

    impl MockCompactedDataSysTable {
        fn new(catalog: Arc<Catalog>) -> Self {
            Self {
                catalog: CompactedCatalog::new_for_testing("mock-compactor", catalog),
            }
        }
    }

    impl CompactedDataSystemTableView for MockCompactedDataSysTable {
        fn query(
            &self,
            _db_name: &str,
            _table_name: &str,
        ) -> Option<Vec<influxdb3_enterprise_data_layout::CompactedDataSystemTableQueryResult>>
        {
            Some(vec![
                CompactedDataSystemTableQueryResult {
                    generation_id: 1,
                    generation_level: 2,
                    generation_time: "2024-01-02/23-00".to_owned(),
                    parquet_files: vec![Arc::new(ParquetFile {
                        id: ParquetFileId::new(),
                        path: "/some/path.parquet".to_owned(),
                        size_bytes: 450_000,
                        row_count: 100_000,
                        chunk_time: 1234567890000000000,
                        min_time: 1234567890000000000,
                        max_time: 1234567890000000000,
                    })],
                },
                CompactedDataSystemTableQueryResult {
                    generation_id: 2,
                    generation_level: 3,
                    generation_time: "2024-01-02/23-00".to_owned(),
                    parquet_files: vec![Arc::new(ParquetFile {
                        id: ParquetFileId::new(),
                        path: "/some/path2.parquet".to_owned(),
                        size_bytes: 450_000,
                        row_count: 100_000,
                        chunk_time: 1234567890000000000,
                        min_time: 1234567890000000000,
                        max_time: 1234567890000000000,
                    })],
                },
            ])
        }

        fn catalog(&self) -> &CompactedCatalog {
            &self.catalog
        }
    }

    // This is copy/pasted from the core module that is super to this, to replace the query
    // executor impl with the enterprise one
    async fn setup() -> (
        Arc<dyn WriteBuffer>,
        QueryExecutorEnterprise,
        Arc<MockProvider>,
        Arc<SysEventStore>,
    ) {
        // Set up QueryExecutor
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider) as _,
            Default::default(),
        );
        let node_id = "test_host";
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), node_id));
        let exec = make_exec(Arc::clone(&object_store));
        let node_id = Arc::from(node_id);
        let instance_id = Arc::from("instance-id");
        let catalog = Arc::new(Catalog::new(node_id, instance_id));
        let write_buffer_impl = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::<MockProvider>::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .unwrap(),
            time_provider: Arc::<MockProvider>::clone(&time_provider),
            executor: Arc::clone(&exec),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            parquet_cache: Some(parquet_cache),
            metric_registry: Default::default(),
            snapshotted_wal_files_to_keep: 1,
            query_file_limit: None,
        })
        .await
        .unwrap();

        let persisted_files: Arc<PersistedFiles> = Arc::clone(&write_buffer_impl.persisted_files());
        let telemetry_store = TelemetryStore::new_without_background_runners(Some(persisted_files));
        let sys_events_store = Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
            &time_provider,
        )));
        let write_buffer: Arc<dyn WriteBuffer> = write_buffer_impl;
        let metrics = Arc::new(Registry::new());
        let datafusion_config = Arc::new(Default::default());
        let query_executor = QueryExecutorEnterprise::new(
            CreateQueryExecutorArgs {
                catalog: write_buffer.catalog(),
                write_buffer: Arc::clone(&write_buffer),
                exec,
                metrics,
                datafusion_config,
                query_log_size: 10,
                telemetry_store,
                sys_events_store: Arc::clone(&sys_events_store),
            },
            Some(Arc::new(MockCompactedDataSysTable::new(Arc::clone(
                &catalog,
            )))),
            Arc::new(RwLock::new(Default::default())),
        );

        (
            write_buffer,
            query_executor,
            time_provider,
            sys_events_store,
        )
    }

    #[test_log::test(tokio::test)]
    async fn test_sys_table_compaction_events_snapshot_success() {
        let (write_buffer, query_executor, _, sys_events_store) = setup().await;
        let sys_events_store: Arc<dyn CompactionEventStore> = sys_events_store;
        let host = "sample-host";
        let db_name = "foo";
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
                Time::from_timestamp_nanos(0),
                false,
                influxdb3_write::Precision::Nanosecond,
            )
            .await
            .unwrap();

        let event = SuccessInfo {
            node_id: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(1234),
            db_count: 2,
            table_count: 1000,
            file_count: 100_000,
        };
        sys_events_store.record_snapshot_success(event);

        let query = "SELECT split_part(event_time, 'T', 1) as event_time, event_type, event_duration, event_status, event_data FROM system.compaction_events";
        let batch_stream = query_executor
            .query_sql(db_name, query, None, None, None)
            .await
            .unwrap();
        let batches: Result<Vec<RecordBatch>, DataFusionError> = batch_stream.try_collect().await;
        debug!(batches = ?batches, "result from collecting batch stream");
        assert_batches_sorted_eq!(
            [
                "+------------+------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------+",
                "| event_time | event_type       | event_duration | event_status | event_data                                                                                          |",
                "+------------+------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------+",
                "| 1970-01-01 | snapshot_fetched | 1234           | success      | {\"node_id\":\"sample-host\",\"sequence_number\":123,\"db_count\":2,\"table_count\":1000,\"file_count\":100000} |",
                "+------------+------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------+",
            ],
            &batches.unwrap());
    }

    #[test_log::test(tokio::test)]
    async fn test_sys_table_compaction_events_snapshot_error() {
        let (write_buffer, query_executor, _, sys_events_store) = setup().await;
        let sys_events_store: Arc<dyn CompactionEventStore> = sys_events_store;
        let host = "sample-host";
        let db_name = "foo";
        let event = FailedInfo {
            node_id: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(10),
            error: "Foo failed".to_string(),
        };
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
                Time::from_timestamp_nanos(0),
                false,
                influxdb3_write::Precision::Nanosecond,
            )
            .await
            .unwrap();

        sys_events_store.record_snapshot_failed(event);

        let query = "SELECT split_part(event_time, 'T', 1) as event_time, event_type, event_duration, event_status, event_data FROM system.compaction_events";
        let batch_stream = query_executor
            .query_sql(db_name, query, None, None, None)
            .await
            .unwrap();
        let batches: Result<Vec<RecordBatch>, DataFusionError> = batch_stream.try_collect().await;
        debug!(batches = ?batches, "result from collecting batch stream");
        assert_batches_sorted_eq!(
            [
                "+------------+------------------+----------------+--------------+----------------------------------------------------------------------+",
                "| event_time | event_type       | event_duration | event_status | event_data                                                           |",
                "+------------+------------------+----------------+--------------+----------------------------------------------------------------------+",
                "| 1970-01-01 | snapshot_fetched | 10             | failed       | {\"node_id\":\"sample-host\",\"sequence_number\":123,\"error\":\"Foo failed\"} |",
                "+------------+------------------+----------------+--------------+----------------------------------------------------------------------+",
            ],
            &batches.unwrap());
    }

    #[test_log::test(tokio::test)]
    async fn test_sys_table_compaction_events_multiple_types() {
        let (write_buffer, query_executor, _, sys_events_store) = setup().await;
        let sys_events_store: Arc<dyn CompactionEventStore> = sys_events_store;
        let host = "sample-host";
        let db_name = "foo";
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                "\
            cpu,host=a,region=us-east usage=250\n\
            mem,host=a,region=us-east usage=150000\n\
            ",
                Time::from_timestamp_nanos(0),
                false,
                influxdb3_write::Precision::Nanosecond,
            )
            .await
            .unwrap();

        // success event - snapshot
        let snapshot_success_event = SuccessInfo {
            node_id: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(1234),
            db_count: 2,
            table_count: 1000,
            file_count: 100_000,
        };
        sys_events_store.record_snapshot_success(snapshot_success_event);

        // failed event - snapshot
        let snapshot_failed_event = FailedInfo {
            node_id: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(10),
            error: "Foo failed".to_string(),
        };
        sys_events_store.record_snapshot_failed(snapshot_failed_event);

        // success event - catalog
        let catalog_success_event = catalog_fetched::SuccessInfo {
            node_id: Arc::from(host),
            catalog_sequence_number: 123,
            duration: Duration::from_millis(10),
        };
        sys_events_store.record_catalog_success(catalog_success_event);

        // failed events (x 2) - catalog
        let catalog_failed_event = catalog_fetched::FailedInfo {
            node_id: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(10),
            error: "catalog failed".to_string(),
        };
        sys_events_store.record_catalog_failed(catalog_failed_event);

        let catalog_failed_event = catalog_fetched::FailedInfo {
            node_id: Arc::from(host),
            sequence_number: 124,
            duration: Duration::from_millis(100),
            error: "catalog failed 2".to_string(),
        };
        sys_events_store.record_catalog_failed(catalog_failed_event);

        // success event - plan
        let plan_success_event = compaction_planned::SuccessInfo {
            input_generations: vec![2],
            input_paths: vec![Arc::from("/path1")],
            output_level: 1,
            db_name: Arc::from("db"),
            table_name: Arc::from("table"),
            left_over_gen1_files: 10,
            duration: Duration::from_millis(10),
        };
        sys_events_store.record_compaction_plan_success(plan_success_event);
        // failed event - plan
        let plan_failed_event = compaction_planned::FailedInfo {
            duration: Duration::from_millis(10),
            error: "db schema is missing".to_owned(),
        };
        sys_events_store.record_compaction_plan_failed(plan_failed_event);

        let plan_run_success_event = compaction_completed::PlanRunSuccessInfo {
            input_generations: vec![2],
            input_paths: vec![Arc::from("/path1")],
            output_level: 1,
            db_name: Arc::from("db"),
            table_name: Arc::from("table"),
            left_over_gen1_files: 10,
            duration: Duration::from_millis(10),
        };
        sys_events_store.record_compaction_plan_run_success(plan_run_success_event);

        let plan_run_failed_event = compaction_completed::PlanRunFailedInfo {
            duration: Duration::from_millis(100),
            error: "db schema is missing".to_owned(),
            identifier: PlanIdentifier {
                db_name: Arc::from("db"),
                table_name: Arc::from("table"),
                output_generation: 2,
            },
        };
        sys_events_store.record_compaction_plan_run_failed(plan_run_failed_event);

        let plan_group_run_success_event = compaction_completed::PlanGroupRunSuccessInfo {
            duration: Duration::from_millis(200),
            plans_ran: vec![PlanIdentifier {
                db_name: Arc::from("db"),
                table_name: Arc::from("table"),
                output_generation: 2,
            }],
        };
        sys_events_store.record_compaction_plan_group_run_success(plan_group_run_success_event);

        let plan_group_run_failed_event = compaction_completed::PlanGroupRunFailedInfo {
            duration: Duration::from_millis(120),
            error: "plan group run failed".to_string(),
            plans_ran: vec![PlanIdentifier {
                db_name: Arc::from("db"),
                table_name: Arc::from("table"),
                output_generation: 2,
            }],
        };
        sys_events_store.record_compaction_plan_group_run_failed(plan_group_run_failed_event);

        let consumer_success_event = compaction_consumed::SuccessInfo {
            duration: Duration::from_millis(200),
            db_name: Arc::from("db"),
            table_name: Arc::from("table"),
            new_generations: vec![2, 3],
            removed_generations: vec![1],
            summary_sequence_number: 1,
        };
        sys_events_store.record_compaction_consumed_success(consumer_success_event);

        let consumer_failed_event = compaction_consumed::FailedInfo {
            duration: Duration::from_millis(200),
            summary_sequence_number: 1,
            error: "foo failed".to_owned(),
        };
        sys_events_store.record_compaction_consumed_failed(consumer_failed_event);

        let query = "SELECT split_part(event_time, 'T', 1) as event_time, event_type, event_duration, event_status, event_data FROM system.compaction_events";
        let batch_stream = query_executor
            .query_sql(db_name, query, None, None, None)
            .await
            .unwrap();
        let batches: Result<Vec<RecordBatch>, DataFusionError> = batch_stream.try_collect().await;
        debug!(batches = ?batches, "result from collecting batch stream");
        assert_batches_eq!(
            [
                "+------------+--------------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------+",
                "| event_time | event_type               | event_duration | event_status | event_data                                                                                                                        |",
                "+------------+--------------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------+",
                "| 1970-01-01 | snapshot_fetched         | 1234           | success      | {\"node_id\":\"sample-host\",\"sequence_number\":123,\"db_count\":2,\"table_count\":1000,\"file_count\":100000}                               |",
                "| 1970-01-01 | snapshot_fetched         | 10             | failed       | {\"node_id\":\"sample-host\",\"sequence_number\":123,\"error\":\"Foo failed\"}                                                              |",
                "| 1970-01-01 | catalog_fetched          | 10             | success      | {\"node_id\":\"sample-host\",\"catalog_sequence_number\":123}                                                                           |",
                "| 1970-01-01 | catalog_fetched          | 10             | failed       | {\"node_id\":\"sample-host\",\"sequence_number\":123,\"error\":\"catalog failed\"}                                                          |",
                "| 1970-01-01 | catalog_fetched          | 100            | failed       | {\"node_id\":\"sample-host\",\"sequence_number\":124,\"error\":\"catalog failed 2\"}                                                        |",
                "| 1970-01-01 | compaction_planned       | 10             | success      | {\"input_generations\":[2],\"input_paths\":[\"/path1\"],\"output_level\":1,\"db_name\":\"db\",\"table_name\":\"table\",\"left_over_gen1_files\":10} |",
                "| 1970-01-01 | compaction_planned       | 10             | failed       | {\"duration\":{\"secs\":0,\"nanos\":10000000},\"error\":\"db schema is missing\"}                                                           |",
                "| 1970-01-01 | plan_run_completed       | 10             | success      | {\"input_generations\":[2],\"input_paths\":[\"/path1\"],\"output_level\":1,\"db_name\":\"db\",\"table_name\":\"table\",\"left_over_gen1_files\":10} |",
                "| 1970-01-01 | plan_run_completed       | 100            | failed       | {\"error\":\"db schema is missing\",\"identifier\":{\"db_name\":\"db\",\"table_name\":\"table\",\"output_generation\":2}}                         |",
                "| 1970-01-01 | plan_group_run_completed | 200            | success      | {\"plans_ran\":[{\"db_name\":\"db\",\"table_name\":\"table\",\"output_generation\":2}]}                                                       |",
                "| 1970-01-01 | plan_group_run_completed | 120            | failed       | {\"error\":\"plan group run failed\",\"plans_ran\":[{\"db_name\":\"db\",\"table_name\":\"table\",\"output_generation\":2}]}                       |",
                "| 1970-01-01 | compaction_consumed      | 200            | success      | {\"db_name\":\"db\",\"table_name\":\"table\",\"new_generations\":[2,3],\"removed_generations\":[1],\"summary_sequence_number\":1}               |",
                "| 1970-01-01 | compaction_consumed      | 200            | failed       | {\"error\":\"foo failed\",\"summary_sequence_number\":1}                                                                                |",
                "+------------+--------------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------+",
            ],
            &batches.unwrap());
    }

    #[test_log::test(tokio::test)]
    async fn system_table_compacted_data_success() {
        let (write_buffer, query_executor, _, _) = setup().await;
        // Perform some writes to multiple tables
        let db_name = "test_db";
        // perform writes over time to generate WAL files and some snapshots
        // the time provider is bumped to trick the system into persisting files:
        for i in 0..10 {
            let time = i * 10;
            let _ = write_buffer
                .write_lp(
                    NamespaceName::new(db_name).unwrap(),
                    "\
                cpu,host=a,region=us-east usage=250\n\
                mem,host=a,region=us-east usage=150000\n\
                ",
                    Time::from_timestamp_nanos(time),
                    false,
                    influxdb3_write::Precision::Nanosecond,
                )
                .await
                .unwrap();
        }
        let _ = write_buffer.watch_persisted_snapshots().changed().await;

        // don't add parquet_id as it's changes everytime it's ran now
        let query = "SELECT
            table_name,
            generation_id,
            generation_level,
            generation_time,
            parquet_path,
            parquet_size_bytes,
            parquet_row_count,
            parquet_chunk_time,
            parquet_min_time,
            parquet_max_time
            FROM system.compacted_data WHERE table_name = 'cpu'";
        let batch_stream = query_executor
            .query_sql(db_name, query, None, None, None)
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        assert_batches_sorted_eq!(
            [
                "+------------+---------------+------------------+------------------+---------------------+--------------------+-------------------+---------------------+---------------------+---------------------+",
                "| table_name | generation_id | generation_level | generation_time  | parquet_path        | parquet_size_bytes | parquet_row_count | parquet_chunk_time  | parquet_min_time    | parquet_max_time    |",
                "+------------+---------------+------------------+------------------+---------------------+--------------------+-------------------+---------------------+---------------------+---------------------+",
                "| cpu        | 1             | 2                | 2024-01-02/23-00 | /some/path.parquet  | 450000             | 100000            | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 |",
                "| cpu        | 2             | 3                | 2024-01-02/23-00 | /some/path2.parquet | 450000             | 100000            | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 |",
                "+------------+---------------+------------------+------------------+---------------------+--------------------+-------------------+---------------------+---------------------+---------------------+",
            ],
            &batches);
    }

    #[test_log::test(tokio::test)]
    async fn compacted_data_sys_table_in_compactor_query_exec() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let exec = make_exec(Arc::clone(&object_store));
        let sys_events_store = Arc::new(SysEventStore::new(time_provider));
        let catalog = Catalog::new("test".into(), "test".into());
        let db_id = DbId::new();
        catalog
            .apply_catalog_batch(&influxdb3_wal::create::catalog_batch(
                db_id,
                "test_db",
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    "test_db",
                    TableId::new(),
                    "test_table",
                    [],
                    [],
                )],
            ))
            .unwrap();
        let query_exec =
            CompactionSysTableQueryExecutorImpl::new(CompactionSysTableQueryExecutorArgs {
                exec,
                metrics: Default::default(),
                datafusion_config: Default::default(),
                query_log_size: 10,
                telemetry_store: TelemetryStore::new_without_background_runners(None),
                compacted_data: Some(Arc::new(MockCompactedDataSysTable::new(Arc::new(catalog)))),
                sys_events_store,
            });

        // don't add parquet_id as it's changes everytime it's ran now
        let query = "SELECT
            table_name,
            generation_id,
            generation_level,
            generation_time,
            parquet_path,
            parquet_size_bytes,
            parquet_row_count,
            parquet_chunk_time,
            parquet_min_time,
            parquet_max_time
            FROM system.compacted_data WHERE table_name = 'test_table'";
        let stream = query_exec
            .query_sql("test_db", query, None, None, None)
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_batches_sorted_eq!([
            "+------------+---------------+------------------+------------------+---------------------+--------------------+-------------------+---------------------+---------------------+---------------------+",
            "| table_name | generation_id | generation_level | generation_time  | parquet_path        | parquet_size_bytes | parquet_row_count | parquet_chunk_time  | parquet_min_time    | parquet_max_time    |",
            "+------------+---------------+------------------+------------------+---------------------+--------------------+-------------------+---------------------+---------------------+---------------------+",
            "| test_table | 1             | 2                | 2024-01-02/23-00 | /some/path.parquet  | 450000             | 100000            | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 |",
            "| test_table | 2             | 3                | 2024-01-02/23-00 | /some/path2.parquet | 450000             | 100000            | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 | 2009-02-13T23:31:30 |",
            "+------------+---------------+------------------+------------------+---------------------+--------------------+-------------------+---------------------+---------------------+---------------------+",
        ], &batches);
    }
}
