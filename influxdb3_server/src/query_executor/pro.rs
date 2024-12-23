use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
};
use datafusion_util::config::DEFAULT_SCHEMA;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_pro_compactor::compacted_data::CompactedDataSystemTableView;
use influxdb3_sys_events::SysEventStore;
use influxdb3_telemetry::store::TelemetryStore;
use iox_query::{
    exec::{Executor, IOxSessionContext, QueryConfig},
    query_log::{QueryCompletedToken, QueryLog, QueryLogEntries, QueryText, StateReceived},
    QueryDatabase, QueryNamespace,
};
use iox_query_params::StatementParams;
use iox_system_tables::SystemTableProvider;
use metric::Registry;
use observability_deps::tracing::{debug, info};
use tokio::sync::Semaphore;
use trace::ctx::SpanContext;
use trace::span::{Span, SpanExt};
use trace_http::ctx::RequestLogContext;
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

use crate::{
    query_executor::{acquire_semaphore, query_database, Error},
    system_tables::{
        compaction_events::CompactionEventsSysTable, SystemSchemaProvider,
        COMPACTION_EVENTS_TABLE_NAME, SYSTEM_SCHEMA_NAME,
    },
    QueryExecutor, QueryKind,
};

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
    type Error = Error;

    async fn query(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        kind: QueryKind,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        info!(%database, %query, ?params, ?kind, "CompactionSysTableQueryExecutorImpl as QueryExecutor::query");
        let db = self
            .namespace(database, span_ctx.child_span("get database"), false)
            .await
            .map_err(|_| Error::DatabaseNotFound {
                db_name: database.to_string(),
            })?
            .ok_or_else(|| Error::DatabaseNotFound {
                db_name: database.to_string(),
            })?;
        query_database(
            db,
            query,
            params,
            kind,
            span_ctx,
            external_span_ctx,
            Arc::clone(&self.telemetry_store),
        )
        .await
    }

    fn show_databases(&self) -> Result<SendableRecordBatchStream, Self::Error> {
        Err(Error::MethodNotImplemented)
    }

    async fn show_retention_policies(
        &self,
        _database: Option<&str>,
        _span_ctx: Option<SpanContext>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        Err(Error::MethodNotImplemented)
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
        let db_schema = self
            .compacted_data
            .clone()
            .and_then(|compacted_data| compacted_data.catalog().db_schema(name))
            .ok_or_else(|| {
                DataFusionError::External(Box::new(Error::DatabaseNotFound {
                    db_name: name.into(),
                }))
            })?;
        let database = CompactionEventsQueryableDatabase::new(
            db_schema,
            Arc::clone(&self.exec),
            Arc::clone(&self.datafusion_config),
            Arc::clone(&self.query_log),
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
        sys_events_store: Arc<SysEventStore>,
    ) -> Self {
        let system_schema_provider = Arc::new(SystemSchemaProvider::CompactionSystemTables(
            CompactionSystemTablesProvider::new(Arc::clone(&sys_events_store)),
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
    pub fn new(sys_events_store: Arc<SysEventStore>) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();
        tables.insert(
            COMPACTION_EVENTS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                CompactionEventsSysTable::new(Arc::clone(&sys_events_store)),
            ))),
        );
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
    use iox_time::Time;
    use observability_deps::tracing::debug;

    use crate::{query_executor::tests::setup, QueryExecutor};
    use influxdb3_sys_events::events::snapshot_fetched::{FailedInfo, SuccessInfo};
    use influxdb3_sys_events::events::{
        catalog_fetched,
        compaction_completed::{self, PlanIdentifier},
        compaction_consumed, compaction_planned, CompactionEventStore,
    };

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
            host: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(1234),
            db_count: 2,
            table_count: 1000,
            file_count: 100_000,
        };
        sys_events_store.record_snapshot_success(event);

        let query = "SELECT split_part(event_time, 'T', 1) as event_time, event_type, event_duration, event_status, event_data FROM system.compaction_events";
        let batch_stream = query_executor
            .query(db_name, query, None, crate::QueryKind::Sql, None, None)
            .await
            .unwrap();
        let batches: Result<Vec<RecordBatch>, DataFusionError> = batch_stream.try_collect().await;
        debug!(batches = ?batches, "result from collecting batch stream");
        assert_batches_sorted_eq!(
            [
                "+------------+------------------+----------------+--------------+--------------------------------------------------------------------------------------------------+",
                "| event_time | event_type       | event_duration | event_status | event_data                                                                                       |",
                "+------------+------------------+----------------+--------------+--------------------------------------------------------------------------------------------------+",
                "| 1970-01-01 | snapshot_fetched | 1234           | success      | {\"host\":\"sample-host\",\"sequence_number\":123,\"db_count\":2,\"table_count\":1000,\"file_count\":100000} |",
                "+------------+------------------+----------------+--------------+--------------------------------------------------------------------------------------------------+",
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
            host: Arc::from(host),
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
            .query(db_name, query, None, crate::QueryKind::Sql, None, None)
            .await
            .unwrap();
        let batches: Result<Vec<RecordBatch>, DataFusionError> = batch_stream.try_collect().await;
        debug!(batches = ?batches, "result from collecting batch stream");
        assert_batches_sorted_eq!(
            [
                "+------------+------------------+----------------+--------------+-------------------------------------------------------------------+",
                "| event_time | event_type       | event_duration | event_status | event_data                                                        |",
                "+------------+------------------+----------------+--------------+-------------------------------------------------------------------+",
                "| 1970-01-01 | snapshot_fetched | 10             | failed       | {\"host\":\"sample-host\",\"sequence_number\":123,\"error\":\"Foo failed\"} |",
                "+------------+------------------+----------------+--------------+-------------------------------------------------------------------+",
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
            host: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(1234),
            db_count: 2,
            table_count: 1000,
            file_count: 100_000,
        };
        sys_events_store.record_snapshot_success(snapshot_success_event);

        // failed event - snapshot
        let snapshot_failed_event = FailedInfo {
            host: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(10),
            error: "Foo failed".to_string(),
        };
        sys_events_store.record_snapshot_failed(snapshot_failed_event);

        // success event - catalog
        let catalog_success_event = catalog_fetched::SuccessInfo {
            host: Arc::from(host),
            catalog_sequence_number: 123,
            duration: Duration::from_millis(10),
        };
        sys_events_store.record_catalog_success(catalog_success_event);

        // failed events (x 2) - catalog
        let catalog_failed_event = catalog_fetched::FailedInfo {
            host: Arc::from(host),
            sequence_number: 123,
            duration: Duration::from_millis(10),
            error: "catalog failed".to_string(),
        };
        sys_events_store.record_catalog_failed(catalog_failed_event);

        let catalog_failed_event = catalog_fetched::FailedInfo {
            host: Arc::from(host),
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
            .query(db_name, query, None, crate::QueryKind::Sql, None, None)
            .await
            .unwrap();
        let batches: Result<Vec<RecordBatch>, DataFusionError> = batch_stream.try_collect().await;
        debug!(batches = ?batches, "result from collecting batch stream");
        assert_batches_eq!(
            [
                "+------------+--------------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------+",
                "| event_time | event_type               | event_duration | event_status | event_data                                                                                                                        |",
                "+------------+--------------------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------+",
                "| 1970-01-01 | snapshot_fetched         | 1234           | success      | {\"host\":\"sample-host\",\"sequence_number\":123,\"db_count\":2,\"table_count\":1000,\"file_count\":100000}                                  |",
                "| 1970-01-01 | snapshot_fetched         | 10             | failed       | {\"host\":\"sample-host\",\"sequence_number\":123,\"error\":\"Foo failed\"}                                                                 |",
                "| 1970-01-01 | catalog_fetched          | 10             | success      | {\"host\":\"sample-host\",\"catalog_sequence_number\":123}                                                                              |",
                "| 1970-01-01 | catalog_fetched          | 10             | failed       | {\"host\":\"sample-host\",\"sequence_number\":123,\"error\":\"catalog failed\"}                                                             |",
                "| 1970-01-01 | catalog_fetched          | 100            | failed       | {\"host\":\"sample-host\",\"sequence_number\":124,\"error\":\"catalog failed 2\"}                                                           |",
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
}
