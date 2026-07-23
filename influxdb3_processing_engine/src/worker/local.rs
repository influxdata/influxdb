use crate::logging::WriteLogEndpoint;
use crate::plugins::{PluginError, ProcessingEngineEnvironmentManager};
use crate::scheduler::TriggerKey;
use crate::scheduler_worker_protocol::{
    RequestTriggerWork, TriggerExecutionError, TriggerResponse, TriggerScheduler, TriggerWork,
    TriggerWorkId, TriggerWorkOutput, TriggerWorkPayload, TriggerWorkResult, TriggerWorker,
};
use crate::{PluginCode, read_plugin_code};
use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use humantime::format_duration;
use influxdb3_catalog::catalog::{Catalog, TriggerDefinition, TriggerSpecificationDefinition};
use influxdb3_id::DbId;
use influxdb3_processing_engine_telemetry::{
    PluginTriggerEntrypoint, PluginTriggerInvocationKey, PluginTriggerInvocationRegistry,
};
use influxdb3_py_api::{
    cache::{CacheStore, PyCache},
    error::ExecutePluginResult,
    logging::{LogLevel, PluginLogger, ProcessingEngineLogger},
    query::QueryEndpoint,
    system_py::{
        PluginReturnState, execute_request_trigger, execute_schedule_trigger,
        execute_wal_flush_trigger,
    },
    wal::WalFlushElement,
    write::{WriteEndpoint, WriteTarget},
};
use influxdb3_types::{DatabaseName, logging::ErrorOneLine};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, error};
use parking_lot::Mutex;
use std::{
    fmt::Debug,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};
use tokio_util::sync::CancellationToken;

/// Await a spawned blocking plugin run, returning `None` if `cancel` fires
/// first. On cancellation the blocking task is abandoned — it is not awaited to
/// completion — so a stuck or slow run cannot delay trigger shutdown (the thread
/// unwinds on its next host-API callback into Rust, or at process exit).
pub(super) async fn run_until_cancelled<T>(
    join: tokio::task::JoinHandle<T>,
    cancel: &CancellationToken,
) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::select! {
        joined = join => Some(joined),
        _ = cancel.cancelled() => None,
    }
}

pub(crate) struct TriggerWorkerContext {
    pub(crate) environment_manager: ProcessingEngineEnvironmentManager,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) node_id: Arc<str>,
    pub(crate) write_endpoint: Arc<dyn WriteEndpoint>,
    pub(crate) query_endpoint: Arc<dyn QueryEndpoint>,
    pub(crate) time_provider: Arc<dyn TimeProvider>,
    pub(crate) cache: Arc<Mutex<CacheStore>>,
    pub(crate) plugin_shutdown: CancellationToken,
    pub(crate) plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
}

pub(crate) fn make_trigger_worker(context: TriggerWorkerContext) -> Arc<PythonTriggerWorker> {
    Arc::new(PythonTriggerWorker {
        environment_manager: context.environment_manager,
        catalog: context.catalog,
        node_id: context.node_id,
        write_endpoint: context.write_endpoint,
        query_endpoint: context.query_endpoint,
        time_provider: context.time_provider,
        cache: context.cache,
        plugin_shutdown: context.plugin_shutdown,
        plugin_trigger_invocation_registry: context.plugin_trigger_invocation_registry,
        plugins: Default::default(),
        active_work: Default::default(),
        schedulers: Default::default(),
    })
}

pub(crate) struct PythonTriggerWorker {
    environment_manager: ProcessingEngineEnvironmentManager,
    catalog: Arc<Catalog>,
    node_id: Arc<str>,
    write_endpoint: Arc<dyn WriteEndpoint>,
    query_endpoint: Arc<dyn QueryEndpoint>,
    time_provider: Arc<dyn TimeProvider>,
    cache: Arc<Mutex<CacheStore>>,
    plugin_shutdown: CancellationToken,
    plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
    plugins: Mutex<HashMap<TriggerKey, Arc<TriggerPlugin>>>,
    active_work: ActiveWorkRegistry,
    schedulers: Mutex<HashMap<Arc<str>, Weak<dyn TriggerScheduler>>>,
}

impl Debug for PythonTriggerWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonTriggerWorker")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ActiveWork {
    generation: u64,
    cancel: CancellationToken,
}

#[derive(Debug, Default)]
struct ActiveWorkRegistry {
    next_generation: AtomicU64,
    work: Mutex<HashMap<TriggerWorkId, ActiveWork>>,
}

impl ActiveWorkRegistry {
    fn submit(&self, work_id: TriggerWorkId, cancel: CancellationToken) -> Option<u64> {
        let mut work = self.work.lock();
        if work.contains_key(&work_id) {
            return None;
        }
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        work.insert(work_id, ActiveWork { generation, cancel });
        Some(generation)
    }

    fn cancel(&self, work_id: TriggerWorkId) {
        if let Some(active) = self.work.lock().get(&work_id) {
            active.cancel.cancel();
        }
    }

    fn finish(&self, work_id: TriggerWorkId, generation: u64) {
        let mut work = self.work.lock();
        if work
            .get(&work_id)
            .is_some_and(|active| active.generation == generation)
        {
            work.remove(&work_id);
        }
    }

    #[cfg(test)]
    fn contains(&self, work_id: TriggerWorkId) -> bool {
        self.work.lock().contains_key(&work_id)
    }
}

impl PythonTriggerWorker {
    /// Register a scheduler endpoint as part of node-lifetime orchestration.
    pub(crate) fn register_scheduler(&self, scheduler: Arc<dyn TriggerScheduler>) {
        let scheduler_node_id = scheduler.node_id();
        self.schedulers
            .lock()
            .insert(scheduler_node_id, Arc::downgrade(&scheduler));
    }

    fn scheduler_for(&self, scheduler_node_id: &str) -> Option<Arc<dyn TriggerScheduler>> {
        let mut schedulers = self.schedulers.lock();
        let scheduler = schedulers.get(scheduler_node_id).and_then(Weak::upgrade);
        if scheduler.is_none() {
            schedulers.remove(scheduler_node_id);
        }
        scheduler
    }

    async fn plugin_for_key(
        &self,
        key: TriggerKey,
    ) -> Result<Arc<TriggerPlugin>, TriggerExecutionError> {
        let db_schema = self
            .catalog
            .db_schema_by_id(&key.db_id)
            .ok_or_else(|| anyhow!("database not found for trigger {key:?}"))?;
        let trigger_definition = db_schema
            .processing_engine_triggers
            .get_by_id(&key.trigger_id)
            .ok_or_else(|| anyhow!("trigger not found: {key:?}"))?;
        let db_name = db_schema.name.to_string();

        if let Some(plugin) = self.plugins.lock().get(&key).cloned()
            && plugin.matches(&db_name, &trigger_definition)
        {
            return Ok(plugin);
        }

        let plugin_code = Arc::new(
            read_plugin_code(
                &self.environment_manager,
                &trigger_definition.plugin_filename,
            )
            .await?,
        );
        let plugin = Arc::new(TriggerPlugin::new(
            key,
            db_name,
            trigger_definition,
            plugin_code,
            self,
        ));
        self.plugins.lock().insert(key, Arc::clone(&plugin));
        Ok(plugin)
    }

    async fn execute_once(
        &self,
        work: TriggerWork,
    ) -> Result<TriggerWorkOutput, TriggerExecutionError> {
        self.plugin_for_key(work.key)
            .await?
            .execute_once(work.payload)
            .await
    }
}

#[derive(Debug)]
struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Debug)]
struct TriggerPlugin {
    trigger_definition: Arc<TriggerDefinition>,
    plugin_code: Arc<PluginCode>,
    db_id: DbId,
    db_name: String,
    write_endpoint: Arc<dyn WriteEndpoint>,
    query_endpoint: Arc<dyn QueryEndpoint>,
    catalog: Arc<Catalog>,
    cache: Arc<Mutex<CacheStore>>,
    logger: ProcessingEngineLogger,
    plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
    plugin_trigger_invocation_key: PluginTriggerInvocationKey,
    plugin_shutdown: CancellationToken,
}

impl TriggerPlugin {
    fn new(
        key: TriggerKey,
        db_name: String,
        trigger_definition: Arc<TriggerDefinition>,
        plugin_code: Arc<PluginCode>,
        worker: &PythonTriggerWorker,
    ) -> Self {
        let database = DatabaseName::new(db_name.clone())
            .expect("trigger database name from catalog should be valid");
        let log_endpoint = Arc::new(WriteLogEndpoint::new(Arc::clone(&worker.write_endpoint)));
        let logger = ProcessingEngineLogger::new(
            database,
            Arc::clone(&trigger_definition.trigger_name),
            Arc::<str>::from(trigger_definition.plugin_filename.as_str()),
            Arc::clone(&worker.node_id),
            Arc::clone(&worker.time_provider),
            log_endpoint,
        );
        let plugin_trigger_invocation_key = PluginTriggerInvocationKey::new(
            Arc::clone(&trigger_definition.database_name),
            Arc::clone(&trigger_definition.trigger_name),
            &trigger_definition.plugin_filename,
            PluginTriggerEntrypoint::from_spec(&trigger_definition.trigger),
        );
        Self {
            trigger_definition,
            plugin_code,
            db_id: key.db_id,
            db_name,
            write_endpoint: Arc::clone(&worker.write_endpoint),
            query_endpoint: Arc::clone(&worker.query_endpoint),
            catalog: Arc::clone(&worker.catalog),
            cache: Arc::clone(&worker.cache),
            logger,
            plugin_trigger_invocation_registry: worker.plugin_trigger_invocation_registry.clone(),
            plugin_trigger_invocation_key,
            plugin_shutdown: worker.plugin_shutdown.clone(),
        }
    }

    fn matches(&self, db_name: &str, trigger_definition: &TriggerDefinition) -> bool {
        self.db_name == db_name && self.trigger_definition.as_ref() == trigger_definition
    }

    fn record_trigger_invocation(&self) {
        if let Some(registry) = &self.plugin_trigger_invocation_registry {
            registry.record_invocation(&self.plugin_trigger_invocation_key);
        }
    }

    fn trigger_cache(&self) -> PyCache {
        PyCache::new_trigger_cache(
            Arc::clone(&self.cache),
            self.db_id,
            self.trigger_definition.trigger_id,
        )
    }

    async fn handle_successful_run(
        &self,
        plugin_return_state: PluginReturnState,
        run_logger: &ProcessingEngineLogger,
        context: &str,
    ) {
        let errors = self.handle_return_state(plugin_return_state).await;
        self.log_return_state_errors(run_logger, &errors, context);
    }

    async fn finish_return_state_join(
        &self,
        join: tokio::task::JoinHandle<ExecutePluginResult<PluginReturnState>>,
        cancel: &CancellationToken,
        run_logger: &ProcessingEngineLogger,
        context: &str,
    ) -> Result<(), PluginError> {
        let Some(joined) = run_until_cancelled(join, cancel).await else {
            debug!(
                trigger_name = %self.trigger_definition.trigger_name,
                %context,
                "plugin run cancelled before completion"
            );
            return Ok(());
        };
        match joined? {
            Ok(return_state) => {
                self.handle_successful_run(return_state, run_logger, context)
                    .await;
                Ok(())
            }
            Err(error) => Err(PluginError::PluginExecutionError(error)),
        }
    }

    async fn execute_wal_once(
        &self,
        database_name: Arc<str>,
        wal_contents: Arc<[WalFlushElement]>,
        cancel: &CancellationToken,
    ) -> Result<(), PluginError> {
        if database_name != self.trigger_definition.database_name {
            return Ok(());
        }

        if wal_contents.is_empty() {
            return Ok(());
        }

        let Some(schema) = self.catalog.db_schema(self.db_name.as_str()) else {
            return Err(PluginError::MissingDb);
        };
        let table_filter = self.make_wal_table_filter(&schema)?;

        let plugin_code = self.plugin_code.code();
        let plugin_root = self.plugin_code.plugin_root().cloned();
        let trigger_arguments = self.trigger_definition.trigger_arguments.clone();
        let run_logger = self.logger.for_run();
        let logger = PluginLogger::production(run_logger.clone());
        let query_endpoint = Arc::clone(&self.query_endpoint);
        let write_endpoint = Arc::clone(&self.write_endpoint);
        let py_cache = self.trigger_cache();
        let plugin_cancel = cancel.clone();

        self.record_trigger_invocation();
        let join = tokio::task::spawn_blocking(move || {
            execute_wal_flush_trigger(
                plugin_code.as_ref(),
                &wal_contents,
                schema,
                query_endpoint,
                write_endpoint,
                logger,
                table_filter,
                &trigger_arguments,
                py_cache,
                plugin_root.as_deref(),
                plugin_cancel,
            )
        });
        self.finish_return_state_join(join, cancel, &run_logger, "wal plugin")
            .await
    }

    async fn execute_schedule_once(
        &self,
        scheduled_at: DateTime<Utc>,
        cancel: &CancellationToken,
    ) -> Result<(), PluginError> {
        let Some(schema) = self.catalog.db_schema(self.db_name.as_str()) else {
            return Err(PluginError::MissingDb);
        };

        let query_endpoint = Arc::clone(&self.query_endpoint);
        let run_logger = self.logger.for_run();
        let logger = PluginLogger::production(run_logger.clone());
        let trigger_arguments = self.trigger_definition.trigger_arguments.clone();
        let py_cache = self.trigger_cache();
        let plugin_code = self.plugin_code.code();
        let plugin_root = self.plugin_code.plugin_root().cloned();
        let write_endpoint = Arc::clone(&self.write_endpoint);
        let plugin_cancel = cancel.clone();

        self.record_trigger_invocation();
        let join = tokio::task::spawn_blocking(move || {
            execute_schedule_trigger(
                plugin_code.as_ref(),
                scheduled_at,
                schema,
                query_endpoint,
                write_endpoint,
                logger,
                &trigger_arguments,
                py_cache,
                plugin_root.as_deref(),
                plugin_cancel,
            )
        });
        self.finish_return_state_join(join, cancel, &run_logger, "schedule plugin")
            .await
    }

    async fn execute_request_once(
        &self,
        request: RequestTriggerWork,
        cancel: &CancellationToken,
    ) -> Result<TriggerResponse, PluginError> {
        let Some(schema) = self.catalog.db_schema(self.db_name.as_str()) else {
            error!(?self.trigger_definition, "missing db schema");
            return Err(PluginError::MissingDb);
        };

        let query_endpoint = Arc::clone(&self.query_endpoint);
        let run_logger = self.logger.for_run();
        let logger = PluginLogger::production(run_logger.clone());
        let trigger_arguments = self.trigger_definition.trigger_arguments.clone();
        let py_cache = self.trigger_cache();
        let plugin_code_str = self.plugin_code.code();
        let plugin_root = self.plugin_code.plugin_root().cloned();
        let write_endpoint = Arc::clone(&self.write_endpoint);
        let plugin_cancel = cancel.clone();
        let RequestTriggerWork {
            query_params,
            headers,
            body,
        } = request;

        self.record_trigger_invocation();
        let plugin = tokio::task::spawn_blocking(move || {
            execute_request_trigger(
                plugin_code_str.as_ref(),
                schema,
                query_endpoint,
                write_endpoint,
                logger,
                &trigger_arguments,
                query_params,
                headers,
                body,
                py_cache,
                plugin_root.as_deref(),
                plugin_cancel,
            )
        });

        let result = tokio::select! {
            joined = plugin => joined?,
            _ = cancel.cancelled() => {
                return Err(anyhow!("request trigger execution cancelled").into());
            }
        };

        match result {
            Ok((response_code, response_headers, response_body, plugin_return_state)) => {
                self.handle_successful_run(plugin_return_state, &run_logger, "request plugin")
                    .await;

                Ok(TriggerResponse {
                    status_code: response_code,
                    headers: response_headers,
                    body: response_body,
                })
            }
            Err(error) => Err(PluginError::PluginExecutionError(error)),
        }
    }

    /// Get the table filter for WAL flush triggers.
    ///
    /// Returns `Ok(None)` for all-tables triggers, `Ok(Some(table_id))` for single-table triggers,
    /// or an error if the trigger specification is not valid for WAL flush (e.g., scheduled triggers).
    fn make_wal_table_filter(
        &self,
        schema: &influxdb3_catalog::catalog::DatabaseSchema,
    ) -> Result<Option<influxdb3_id::TableId>, PluginError> {
        match &self.trigger_definition.trigger {
            TriggerSpecificationDefinition::AllTablesWalWrite => Ok(None),
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                let table_id = schema
                    .table_name_to_id(table_name)
                    .context("table not found")?;
                Ok(Some(table_id))
            }
            TriggerSpecificationDefinition::Schedule { schedule } => Err(anyhow!(
                "unexpectedly found scheduled trigger specification cron:{} for WAL plugin {}",
                schedule,
                self.trigger_definition.trigger_name
            )
            .into()),
            TriggerSpecificationDefinition::Every { duration } => Err(anyhow!(
                "unexpectedly found every trigger specification every:{} for WAL plugin {}",
                format_duration(*duration),
                self.trigger_definition.trigger_name
            )
            .into()),
            TriggerSpecificationDefinition::RequestPath { path } => Err(anyhow!(
                "unexpectedly found request path trigger specification {} for WAL plugin {}",
                path,
                self.trigger_definition.trigger_name
            )
            .into()),
        }
    }

    fn log_return_state_errors(
        &self,
        logger: &ProcessingEngineLogger,
        errors: &[anyhow::Error],
        context: &str,
    ) {
        for error in errors {
            logger.log(
                LogLevel::Error,
                format!("error running {context}: {error:#}"),
            );
            error!(error = %ErrorOneLine(error), ?self.trigger_definition, %context, "error running plugin");
        }
    }

    /// Handles the return state from the plugin, writing back lines and handling any errors.
    /// It returns a vec of error messages that can be used to log or report back to the user.
    async fn handle_return_state(
        &self,
        plugin_return_state: influxdb3_py_api::system_py::PluginReturnState,
    ) -> Vec<anyhow::Error> {
        let ingest_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        let mut errors = Vec::new();

        for (db_name, lines) in plugin_return_state.write_db_lines {
            let Ok(database_name) = DatabaseName::new(db_name.clone()) else {
                errors.push(anyhow!("invalid database name: {db_name}"));
                continue;
            };

            if let Err(e) = self
                .write_endpoint
                .write_lp(
                    WriteTarget::User(database_name),
                    lines.join("\n").as_str(),
                    Time::from_timestamp_nanos(ingest_time.as_nanos() as i64),
                    false,
                )
                .await
                .with_context(|| format!("error writing back lines to {db_name}"))
            {
                errors.push(e);
            }
        }

        errors
    }
}

impl TriggerPlugin {
    async fn execute_once(
        &self,
        payload: TriggerWorkPayload,
    ) -> Result<TriggerWorkOutput, TriggerExecutionError> {
        let cancel = self.plugin_shutdown.child_token();
        let _cancel_on_drop = CancelOnDrop(cancel.clone());

        match payload {
            TriggerWorkPayload::Wal {
                database_name,
                wal_contents,
            } => self
                .execute_wal_once(database_name, wal_contents, &cancel)
                .await
                .map(|()| TriggerWorkOutput::Complete)
                .map_err(Into::into),
            TriggerWorkPayload::Schedule { scheduled_at } => self
                .execute_schedule_once(scheduled_at, &cancel)
                .await
                .map(|()| TriggerWorkOutput::Complete)
                .map_err(Into::into),
            TriggerWorkPayload::Request(request) => self
                .execute_request_once(request, &cancel)
                .await
                .map(TriggerWorkOutput::RequestResponse)
                .map_err(Into::into),
        }
    }
}

impl TriggerWorker for PythonTriggerWorker {
    fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    fn submit_work(self: Arc<Self>, scheduler_node_id: Arc<str>, work: TriggerWork) {
        let work_id = work.id;
        let Some(scheduler) = self.scheduler_for(&scheduler_node_id) else {
            error!(
                ?scheduler_node_id,
                ?work_id,
                "trigger worker submission dropped because scheduler is not registered"
            );
            return;
        };
        let cancel = self.plugin_shutdown.child_token();
        let Some(generation) = self.active_work.submit(work_id, cancel.clone()) else {
            debug!(?work_id, "duplicate trigger worker submission ignored");
            return;
        };

        tokio::spawn(async move {
            scheduler.work_progressed(Arc::clone(&self.node_id), work_id);
            let worker = Arc::clone(&self);
            let mut join = tokio::spawn(async move { worker.execute_once(work).await });

            let result = tokio::select! {
                joined = &mut join => {
                    if cancel.is_cancelled() {
                        Err(TriggerExecutionError::cancelled())
                    } else {
                        match joined {
                            Ok(result) => result,
                            Err(error) => Err(TriggerExecutionError::new(format!(
                                "trigger worker task failed: {error}"
                            ))),
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    join.abort();
                    Err(TriggerExecutionError::cancelled())
                }
            };

            scheduler.work_finished(
                Arc::clone(&self.node_id),
                TriggerWorkResult { work_id, result },
            );
            self.active_work.finish(work_id, generation);
        });
    }

    fn cancel_work(self: Arc<Self>, scheduler_node_id: Arc<str>, work_id: TriggerWorkId) {
        if self.scheduler_for(&scheduler_node_id).is_none() {
            error!(
                ?scheduler_node_id,
                ?work_id,
                "trigger worker cancellation dropped because scheduler is not registered"
            );
            return;
        }
        self.active_work.cancel(work_id);
    }
}

#[cfg(test)]
mod tests;
