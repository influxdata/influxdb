use crate::PluginCode;
use crate::ProcessingEngineManagerImpl;
use crate::environment::PythonEnvironmentManager;
use crate::{RequestEvent, ScheduleEvent, WalEvent};
use data_types::NamespaceName;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::log::TriggerDefinition;
use influxdb3_catalog::log::TriggerSpecificationDefinition;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_py_api::system_py::{CacheStore, PluginLogger, ProcessingEngineLogger, PyCache};

use influxdb3_sys_events::SysEventStore;

use influxdb3_types::http::{WalPluginTestRequest, WalPluginTestResponse};

use futures_util::future::BoxFuture;
use influxdb3_write::{BufferedWriteRequest, Bufferer, Precision, write_buffer};
use iox_time::Time;
use iox_time::TimeProvider;
use observability_deps::tracing::error;
use std::fmt::Debug;
use std::path::PathBuf;

use anyhow::Context;
use parking_lot::Mutex;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

use tokio::sync::mpsc;

/// A buffering implementation of Bufferer for dry-run testing.
/// Collects writes without persisting them.
#[derive(Debug, Default)]
pub struct DryRunBufferer {
    writes: std::sync::Mutex<Vec<(String, String)>>,
}

impl DryRunBufferer {
    pub fn new() -> Self {
        Self {
            writes: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Get all collected writes as (database, lp) pairs
    pub fn get_writes(&self) -> Vec<(String, String)> {
        self.writes.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Bufferer for DryRunBufferer {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        _ingest_time: Time,
        _accept_partial: bool,
        _precision: Precision,
        _no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        self.writes
            .lock()
            .unwrap()
            .push((database.to_string(), lp.to_string()));
        Ok(BufferedWriteRequest {
            db_name: database,
            invalid_lines: vec![],
            line_count: 1,
            field_count: 0,
            index_count: 0,
        })
    }

    fn catalog(&self) -> Arc<Catalog> {
        unimplemented!("DryRunBufferer::catalog is not supported in dry-run mode")
    }

    fn wal(&self) -> Arc<dyn influxdb3_wal::Wal> {
        unimplemented!("DryRunBufferer::wal is not supported in dry-run mode")
    }

    fn parquet_files_filtered(
        &self,
        _db_id: influxdb3_id::DbId,
        _table_id: influxdb3_id::TableId,
        _filter: &influxdb3_write::ChunkFilter<'_>,
    ) -> Vec<influxdb3_write::ParquetFile> {
        unimplemented!("DryRunBufferer::parquet_files_filtered is not supported in dry-run mode")
    }

    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<influxdb3_write::PersistedSnapshotVersion>> {
        unimplemented!("DryRunBufferer::watch_persisted_snapshots is not supported in dry-run mode")
    }
}

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("invalid database {0}")]
    InvalidDatabase(String),

    #[error("couldn't find db")]
    MissingDb,

    #[error(transparent)]
    PyError(#[from] pyo3::PyErr),

    #[error(transparent)]
    WriteBufferError(#[from] write_buffer::Error),

    #[error("failed to send shutdown message back")]
    FailedToShutdown,

    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),

    #[error("reading plugin file: {0}")]
    ReadPluginError(#[from] std::io::Error),

    #[error("error executing plugin: {0}")]
    PluginExecutionError(#[from] influxdb3_py_api::ExecutePluginError),

    #[error("invalid cron syntax: {0}")]
    InvalidCronSyntax(#[from] cron::error::Error),

    #[error("cron schedule never triggers: {0}")]
    CronScheduleNeverTriggers(String),

    #[error("tried to run a schedule plugin but the schedule iterator is over.")]
    ScheduledMissingTime,

    #[error("non-schedule plugin with schedule trigger: {0}")]
    NonSchedulePluginWithScheduleTrigger(String),

    #[error(
        "Trigger schedule type {schedule_type} invalid for trigger type {trigger_type} and type mismatch"
    )]
    TriggerScheduleTypeMismatch {
        schedule_type: String,
        trigger_type: String,
    },

    #[error("error fetching plugin from repository: {0} {1}")]
    FetchingFromRepository(reqwest::StatusCode, String),

    #[error("Join error, please report: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Node not configured with plugin directory")]
    NoPluginDir,

    #[error(
        "Path traversal detected: plugin filename '{0}' attempts to access files outside the plugin directory"
    )]
    PathTraversal(String),
}

pub(crate) fn run_wal_contents_plugin(
    db_name: String,
    plugin_code: Arc<PluginCode>,
    trigger_definition: Arc<TriggerDefinition>,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<WalEvent>,
) {
    let trigger_plugin = TriggerPlugin::new(db_name, plugin_code, trigger_definition, context);

    tokio::task::spawn(async move {
        trigger_plugin
            .run_wal_flush_plugin(plugin_receiver)
            .await
            .expect("trigger plugin failed");
    });
}

#[derive(Debug, Clone)]
pub struct ProcessingEngineEnvironmentManager {
    pub plugin_dir: Option<PathBuf>,
    pub virtual_env_location: Option<PathBuf>,
    pub package_manager: Arc<dyn PythonEnvironmentManager>,
    pub plugin_repo: Option<String>,
}

pub(crate) fn run_schedule_plugin(
    db_name: String,
    plugin_code: Arc<PluginCode>,
    trigger_definition: Arc<TriggerDefinition>,
    time_provider: Arc<dyn TimeProvider>,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<ScheduleEvent>,
) -> Result<(), PluginError> {
    // Ensure that the plugin is a schedule plugin
    let plugin_type = trigger_definition.trigger.plugin_type();
    if !matches!(plugin_type, influxdb3_catalog::log::PluginType::Schedule) {
        return Err(PluginError::NonSchedulePluginWithScheduleTrigger(format!(
            "{trigger_definition:?}"
        )));
    }

    let trigger_plugin = TriggerPlugin::new(db_name, plugin_code, trigger_definition, context);

    let runner = python_plugin::ScheduleTriggerRunner::try_new(
        &trigger_plugin.trigger_definition.trigger,
        Arc::clone(&time_provider),
    )?;
    tokio::task::spawn(async move {
        trigger_plugin
            .run_schedule_plugin(plugin_receiver, runner, time_provider)
            .await
            .expect("cron trigger plugin failed");
    });

    Ok(())
}

pub(crate) fn run_request_plugin(
    db_name: String,
    plugin_code: Arc<PluginCode>,
    trigger_definition: Arc<TriggerDefinition>,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<RequestEvent>,
) {
    let trigger_plugin = TriggerPlugin::new(db_name, plugin_code, trigger_definition, context);
    tokio::task::spawn(async move {
        trigger_plugin
            .run_request_plugin(plugin_receiver)
            .await
            .expect("trigger plugin failed");
    });
}

pub(crate) struct PluginContext {
    // handler to write data back to the DB.
    pub(crate) write_buffer: Arc<dyn Bufferer>,
    // query executor to hand off to the plugin
    pub(crate) query_executor: Arc<dyn QueryExecutor>,
    // processing engine manager for disabling plugins if they fail.
    pub(crate) manager: Arc<ProcessingEngineManagerImpl>,
    // sys events for writing logs to ring buffers
    pub(crate) sys_event_store: Arc<SysEventStore>,
}

#[derive(Debug, Clone)]
struct TriggerPlugin {
    trigger_definition: Arc<TriggerDefinition>,
    plugin_code: Arc<PluginCode>,
    db_name: String,
    write_buffer: Arc<dyn Bufferer>,
    query_executor: Arc<dyn QueryExecutor>,
    manager: Arc<ProcessingEngineManagerImpl>,
    logger: ProcessingEngineLogger,
}

mod python_plugin {
    use super::*;
    use anyhow::{Context, anyhow};
    use chrono::{DateTime, Duration, Utc};
    use cron::{OwnedScheduleIterator, Schedule as CronSchedule};
    use futures_util::StreamExt;
    use futures_util::stream::FuturesUnordered;
    use humantime::{format_duration, parse_duration};
    use hyper::StatusCode;
    use hyper::http::HeaderValue;
    use influxdb3_catalog::catalog::DatabaseSchema;
    use influxdb3_catalog::log::ErrorBehavior;
    use influxdb3_py_api::logging::LogLevel;
    use influxdb3_py_api::system_py::{
        PluginLogger, ProcessingEngineLogger, PyCache, execute_request_trigger,
        execute_schedule_trigger, execute_wal_flush_trigger,
    };
    use influxdb3_types::logging::ErrorOneLine;
    use influxdb3_wal::{WalContents, WalOp};
    use iox_http_util::{ResponseBuilder, bytes_to_response_body};
    use iox_time::Time;
    use observability_deps::tracing::{info, warn};
    use std::str::FromStr;
    use std::time::SystemTime;
    use tokio::sync::mpsc::Receiver;

    impl TriggerPlugin {
        pub(crate) fn new(
            db_name: String,
            plugin_code: Arc<PluginCode>,
            trigger_definition: Arc<TriggerDefinition>,
            context: PluginContext,
        ) -> Self {
            let logger = ProcessingEngineLogger::new(
                context.sys_event_store,
                Arc::clone(&trigger_definition.trigger_name),
            );
            Self {
                trigger_definition,
                plugin_code,
                db_name,
                write_buffer: Arc::clone(&context.write_buffer),
                query_executor: Arc::clone(&context.query_executor),
                manager: Arc::clone(&context.manager),
                logger,
            }
        }

        /// Create a boxed future for processing WAL data events.
        /// Returns the future without awaiting it, allowing caller to choose sync/async execution.
        fn make_wal_process_future(
            &self,
            event: WalEvent,
        ) -> BoxFuture<'static, Result<PluginNextState, PluginError>> {
            let clone = self.clone();
            match event {
                WalEvent::WriteWalContents(wal_contents) => {
                    Box::pin(async move { clone.process_wal_contents(wal_contents).await })
                }
                WalEvent::Shutdown(_) => {
                    // Caller should handle Shutdown before calling this function
                    debug_assert!(
                        false,
                        "Shutdown event should be handled by caller, not passed to make_wal_process_future"
                    );
                    Box::pin(async { Ok(PluginNextState::SuccessfulRun) })
                }
            }
        }

        pub(crate) async fn run_wal_flush_plugin(
            &self,
            mut receiver: Receiver<WalEvent>,
        ) -> Result<(), PluginError> {
            info!(?self.trigger_definition.trigger_name, ?self.trigger_definition.database_name, ?self.trigger_definition.plugin_filename,
                "starting wal flush plugin");
            let mut futures: FuturesUnordered<
                BoxFuture<'static, Result<PluginNextState, PluginError>>,
            > = FuturesUnordered::new();
            loop {
                tokio::select! {
                    event = receiver.recv() => {
                        let process = match event {
                            Some(WalEvent::Shutdown(sender)) => {
                                sender.send(()).map_err(|_| PluginError::FailedToShutdown)?;
                                break;
                            }
                            Some(wal_data_event) => {
                                self.make_wal_process_future(wal_data_event)
                            }
                            None => { break; }
                        };

                        if self.trigger_definition.trigger_settings.run_async {
                            futures.push(process);
                        } else {
                            match process.await? {
                            PluginNextState::SuccessfulRun => {}
                            PluginNextState::LogError(error_log) => {
                                    self.logger.log(LogLevel::Error, error_log);
                                }
                            PluginNextState::Disable(trigger_definition) => {
                                    warn!("disabling trigger {}", trigger_definition.trigger_name);
                                    self.send_disable_trigger();
                                    // todo(pjb): I think there's a bug as any async plugins in futures
                                    //  are not driven to completion before disabling. They will be never
                                    //  be polled again. Something like while futures.next().await.is_some()
                                    //  is needed, with error handling.
                                    while let Some(event) = receiver.recv().await {
                                        match event {
                                            WalEvent::WriteWalContents(_) => {
                                                warn!("skipping wal contents because trigger is being disabled")
                                            }
                                            WalEvent::Shutdown(shutdown) => {
                                                if shutdown.send(()).is_err() {
                                                    error!(trigger_name = %trigger_definition.trigger_name, "failed to send back shutdown for trigger");
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(result) = futures.next() => {
                        match result {
                            Ok(result) => {
                                match result {
                                    PluginNextState::SuccessfulRun => {}
                                    PluginNextState::LogError(error_log) => {
                                        error!(error = %error_log, "trigger failed");
                                        self.logger.log(LogLevel::Error, error_log);
                                    },
                                    PluginNextState::Disable(_) => {
                                        self.send_disable_trigger();
                                        while let Some(event) = receiver.recv().await {
                                            match event {
                                                WalEvent::WriteWalContents(_) => {
                                                    warn!("skipping wal contents because trigger is being disabled")
                                                }
                                                WalEvent::Shutdown(shutdown) => {
                                                    if shutdown.send(()).is_err() {
                                                        error!(trigger_name = %self.trigger_definition.trigger_name, "failed to send back shutdown for trigger");
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                error!(error = %err, ?self.trigger_definition, "error processing wal contents");
                            }
                        }
                    }
                }
            }

            Ok(())
        }

        /// This sends the disable trigger command to the processing engine manager,
        /// it is done in a separate task so that the caller can send back shutdown.
        pub(crate) fn send_disable_trigger(&self) {
            let manager = Arc::clone(&self.manager);
            let db_name = Arc::clone(&self.trigger_definition.database_name);
            let trigger_name = Arc::clone(&self.trigger_definition.trigger_name);
            let fut = async move { manager.stop_trigger(&db_name, &trigger_name).await };
            // start the disable call, then look for the shutdown message
            tokio::spawn(fut);
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

        /// Handle the result of a trigger execution, returning the appropriate control flow action.
        ///
        /// This centralizes the error handling logic for WAL flush triggers, supporting:
        /// - Log: log the error and continue to next batch
        /// - Retry: stay in the retry loop
        /// - Disable: return immediately to disable the plugin
        async fn handle_trigger_result(
            &self,
            result: Result<
                influxdb3_py_api::system_py::PluginReturnState,
                influxdb3_py_api::ExecutePluginError,
            >,
            context: &str,
        ) -> TriggerResultAction {
            match result {
                Ok(return_state) => {
                    let errors = self.handle_return_state(return_state).await;
                    self.log_return_state_errors(&errors, context);
                    TriggerResultAction::Success
                }
                Err(err) => match self.trigger_definition.trigger_settings.error_behavior {
                    ErrorBehavior::Log => {
                        self.logger
                            .log(LogLevel::Error, format!("error executing {context}: {err}"));
                        error!(error = %err, ?self.trigger_definition, "trigger execution error");
                        TriggerResultAction::LogError(err.to_string())
                    }
                    ErrorBehavior::Retry => {
                        info!("error executing {context}: {err}, will retry");
                        TriggerResultAction::Retry
                    }
                    ErrorBehavior::Disable => {
                        TriggerResultAction::Disable(Arc::clone(&self.trigger_definition))
                    }
                },
            }
        }

        fn log_return_state_errors(&self, errors: &[anyhow::Error], context: &str) {
            for error in errors {
                self.logger.log(
                    LogLevel::Error,
                    format!("error running {context}: {error:#}"),
                );
                error!(error = %ErrorOneLine(error), ?self.trigger_definition, %context, "error running plugin");
            }
        }

        pub(crate) async fn run_schedule_plugin(
            &self,
            mut receiver: Receiver<ScheduleEvent>,
            mut runner: ScheduleTriggerRunner,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Result<(), PluginError> {
            let mut futures = FuturesUnordered::new();
            loop {
                let Some(next_run_instant) = runner.next_run_time() else {
                    break;
                };

                tokio::select! {
                    _ = time_provider.sleep_until(next_run_instant) => {
                        let Some(schema) = self.manager.catalog.db_schema(self.db_name.as_str()) else {
                            return Err(PluginError::MissingDb);
                        };

                        let Some(trigger_time) = runner.next_trigger_time else {
                            return Err(anyhow!("running a cron trigger that is finished.").into());
                        };

                        runner.advance_time();
                        if self.trigger_definition.trigger_settings.run_async {
                            let trigger =self.clone();
                            let fut = async move {ScheduleTriggerRunner::run_at_time(trigger, trigger_time, schema).await};
                            futures.push(fut);
                        } else {
                            match ScheduleTriggerRunner::run_at_time(self.clone(), trigger_time, schema).await {
                                Ok(plugin_state) => {
                                    match plugin_state {
                                        PluginNextState::SuccessfulRun => {}
                                        PluginNextState::LogError(err) => {
                                            self.logger.log(LogLevel::Error, format!("error running scheduled plugin: {err}"));
                                            error!(error = %err, ?self.trigger_definition, "error running scheduled plugin");
                                        }
                                        PluginNextState::Disable(trigger_definition) => {
                                            warn!("disabling trigger {} due to error", trigger_definition.trigger_name);
                                            self.send_disable_trigger();
                                            let Some(ScheduleEvent::Shutdown(sender)) = receiver.recv().await else {
                                                warn!("didn't receive shutdown notification from receiver");
                                                break;
                                            };

                                            if sender.send(()).is_err() {
                                                error!("failed to send shutdown message back");
                                            }
                                            break;
                                        }
                                    }
                                }
                                Err(err) => {
                                    self.logger.log(LogLevel::Error, format!("error running scheduled plugin: {err}"));
                                    error!(error = %err, ?self.trigger_definition, "error running scheduled plugin");
                                }
                            }

                        }
                    }
                    event = receiver.recv() => {
                        match event {
                            None => {
                                warn!(?self.trigger_definition, "trigger plugin receiver closed");
                                break;
                            }
                            Some(ScheduleEvent::Shutdown(sender)) => {
                                sender.send(()).map_err(|_| PluginError::FailedToShutdown)?;
                                break;
                            }
                        }
                    }
                    Some(result) = futures.next() => {
                        match result {
                            Err(e) => {
                                self.logger.log(LogLevel::Error, format!("error running async scheduled plugin: {e}"));
                                error!(error = %e, ?self.trigger_definition, "error running async scheduled plugin");
                            }
                            Ok(result) => {
                                match result {
                                    PluginNextState::SuccessfulRun => {}
                                    PluginNextState::LogError(err) => {
                                        self.logger.log(LogLevel::Error, format!("error running async scheduled plugin: {err}"));
                                        error!(error = %err, ?self.trigger_definition, "error running async scheduled plugin");
                                    }
                                    PluginNextState::Disable(trigger_definition) => {
                                        warn!("disabling trigger {} due to error", trigger_definition.trigger_name);
                                        self.send_disable_trigger();

                                        let Some(ScheduleEvent::Shutdown(sender)) = receiver.recv().await else {
                                            warn!("didn't receive shutdown notification from receiver");
                                            break;
                                        };

                                        if sender.send(()).is_err() {
                                            error!("failed to send shutdown message back");
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        }

        /// Create a boxed future for processing HTTP request events.
        fn make_request_process_future(
            &self,
            request: crate::Request,
        ) -> BoxFuture<'static, Result<(), PluginError>> {
            let clone = self.clone();
            Box::pin(async move { clone.process_request(request).await })
        }

        /// Process a single HTTP request, execute the plugin, and send the response.
        async fn process_request(&self, request: crate::Request) -> Result<(), PluginError> {
            let Some(schema) = self.manager.catalog.db_schema(self.db_name.as_str()) else {
                error!(?self.trigger_definition, "missing db schema");
                let body = serde_json::json!({"error": "database not found"}).to_string();
                let response = ResponseBuilder::new()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(bytes_to_response_body(body))
                    .context("building error response")?;
                let _ = request.response_tx.send(response);
                return Err(PluginError::MissingDb);
            };

            let query_executor = Arc::clone(&self.query_executor);
            let logger = PluginLogger::production(self.logger.clone());
            let trigger_arguments = self.trigger_definition.trigger_arguments.clone();
            let py_cache = PyCache::new_trigger_cache(
                Arc::clone(&self.manager.cache),
                self.trigger_definition.database_name.to_string(),
                self.trigger_definition.trigger_name.to_string(),
            );

            let plugin_code_str = self.plugin_code.code();
            let plugin_root = self.plugin_code.plugin_root().cloned();
            let write_buffer = Arc::clone(&self.write_buffer);
            let result = tokio::task::spawn_blocking(move || {
                execute_request_trigger(
                    plugin_code_str.as_ref(),
                    schema,
                    query_executor,
                    write_buffer,
                    logger,
                    &trigger_arguments,
                    request.query_params,
                    request.headers,
                    request.body,
                    py_cache,
                    plugin_root.as_deref(),
                )
            })
            .await?;

            let response = match result {
                Ok((response_code, response_headers, response_body, plugin_return_state)) => {
                    let errors = self.handle_return_state(plugin_return_state).await;
                    // TODO: here is one spot we'll pick up errors to put into the plugin system table
                    self.log_return_state_errors(&errors, "request plugin");

                    let response_status = StatusCode::from_u16(response_code)
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                    let mut response = ResponseBuilder::new().status(response_status);

                    for (key, value) in response_headers {
                        response = response.header(
                            key.as_str(),
                            HeaderValue::from_str(&value)
                                .unwrap_or_else(|_| HeaderValue::from_static("")),
                        );
                    }

                    response
                        .body(bytes_to_response_body(response_body))
                        .context("building response")?
                }
                Err(e) => {
                    self.logger.log(
                        LogLevel::Error,
                        format!("error running request plugin: {e}"),
                    );
                    error!(error = %e, ?self.trigger_definition, "error running request plugin");
                    let body = serde_json::json!({"error": e.to_string()}).to_string();
                    ResponseBuilder::new()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(bytes_to_response_body(body))
                        .context("building response")?
                }
            };

            if request.response_tx.send(response).is_err() {
                error!(?self.trigger_definition, "error sending response");
            }

            Ok(())
        }

        pub(crate) async fn run_request_plugin(
            &self,
            mut receiver: Receiver<RequestEvent>,
        ) -> Result<(), PluginError> {
            info!(?self.trigger_definition.trigger_name, ?self.trigger_definition.database_name, ?self.trigger_definition.plugin_filename, "starting request plugin");

            let mut futures: FuturesUnordered<BoxFuture<'static, Result<(), PluginError>>> =
                FuturesUnordered::new();

            loop {
                tokio::select! {
                    event = receiver.recv() => {
                        match event {
                            None => {
                                warn!(?self.trigger_definition, "trigger plugin receiver closed");
                                break;
                            }
                            Some(RequestEvent::Request(request)) => {
                                let process = self.make_request_process_future(request);
                                if self.trigger_definition.trigger_settings.run_async {
                                    futures.push(process);
                                } else if let Err(e) = process.await {
                                    error!(error = %e, ?self.trigger_definition, "error processing request");
                                }
                            }
                            Some(RequestEvent::Shutdown(sender)) => {
                                sender.send(()).map_err(|_| PluginError::FailedToShutdown)?;
                                break;
                            }
                        }
                    }
                    Some(result) = futures.next() => {
                        if let Err(e) = result {
                            error!(error = %e, ?self.trigger_definition, "error processing async request");
                        }
                    }
                }
            }

            Ok(())
        }

        async fn process_wal_contents(
            &self,
            wal_contents: Arc<WalContents>,
        ) -> Result<PluginNextState, PluginError> {
            let Some(schema) = self.manager.catalog.db_schema(self.db_name.as_str()) else {
                return Err(PluginError::MissingDb);
            };

            // Hoist loop-invariant values that come from self (retries are rare)
            let plugin_code = self.plugin_code.code();
            let plugin_root = self.plugin_code.plugin_root().cloned();
            let trigger_arguments = self.trigger_definition.trigger_arguments.clone();

            for (op_index, wal_op) in wal_contents.ops.iter().enumerate() {
                match wal_op {
                    WalOp::Write(write_batch) => {
                        // determine if this write batch is for this database
                        if write_batch.database_name != self.trigger_definition.database_name {
                            continue;
                        }
                        let table_filter = self.make_wal_table_filter(&schema)?;

                        // loop for retries, in general it will only run once.
                        loop {
                            let logger = PluginLogger::production(self.logger.clone());
                            let plugin_code_str = Arc::clone(&plugin_code);
                            let plugin_root_clone = plugin_root.clone();
                            let query_executor = Arc::clone(&self.query_executor);
                            let write_buffer = Arc::clone(&self.write_buffer);
                            let schema_clone = Arc::clone(&schema);
                            let trigger_arguments = trigger_arguments.clone();
                            let wal_contents_clone = Arc::clone(&wal_contents);
                            let py_cache = PyCache::new_trigger_cache(
                                Arc::clone(&self.manager.cache),
                                self.trigger_definition.database_name.to_string(),
                                self.trigger_definition.trigger_name.to_string(),
                            );

                            let result = tokio::task::spawn_blocking(move || {
                                let write_batch = match &wal_contents_clone.ops[op_index] {
                                    WalOp::Write(wb) => wb,
                                    _ => unreachable!("Index was checked."),
                                };
                                execute_wal_flush_trigger(
                                    plugin_code_str.as_ref(),
                                    write_batch,
                                    schema_clone,
                                    query_executor,
                                    write_buffer,
                                    logger,
                                    table_filter,
                                    &trigger_arguments,
                                    py_cache,
                                    plugin_root_clone.as_deref(),
                                )
                            })
                            .await?;

                            match self.handle_trigger_result(result, "wal plugin").await {
                                TriggerResultAction::Success | TriggerResultAction::LogError(_) => {
                                    break;
                                }
                                TriggerResultAction::Retry => continue,
                                TriggerResultAction::Disable(def) => {
                                    return Ok(PluginNextState::Disable(def));
                                }
                            }
                        }
                    }
                    WalOp::Noop(_) => {}
                }
            }
            Ok(PluginNextState::SuccessfulRun)
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

            if !plugin_return_state.write_back_lines.is_empty() {
                let Ok(namespace_name) = NamespaceName::new(self.db_name.clone()) else {
                    errors.push(anyhow!("invalid database name: {}", self.db_name));
                    return errors;
                };

                if let Err(e) = self
                    .write_buffer
                    .write_lp(
                        namespace_name,
                        plugin_return_state.write_back_lines.join("\n").as_str(),
                        Time::from_timestamp_nanos(ingest_time.as_nanos() as i64),
                        false,
                        Precision::Nanosecond,
                        false,
                    )
                    .await
                    .context("error writing back lines")
                {
                    errors.push(e);
                }
            }

            for (db_name, lines) in plugin_return_state.write_db_lines {
                let Ok(namespace_name) = NamespaceName::new(db_name.clone()) else {
                    errors.push(anyhow!("invalid database name: {db_name}"));
                    continue;
                };

                if let Err(e) = self
                    .write_buffer
                    .write_lp(
                        namespace_name,
                        lines.join("\n").as_str(),
                        Time::from_timestamp_nanos(ingest_time.as_nanos() as i64),
                        false,
                        Precision::Nanosecond,
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

    enum Schedule {
        Cron(Box<OwnedScheduleIterator<Utc>>),
        Every(Duration),
    }

    enum PluginNextState {
        SuccessfulRun,
        LogError(String),
        Disable(Arc<TriggerDefinition>),
    }

    /// Control flow action for trigger execution retry loops.
    enum TriggerResultAction {
        /// Plugin executed successfully
        Success,
        /// Error occurred but was logged (error_behavior = Log)
        LogError(String),
        /// Stay in retry loop
        Retry,
        /// Disable the plugin
        Disable(Arc<TriggerDefinition>),
    }

    pub(crate) struct ScheduleTriggerRunner {
        schedule: Schedule,
        next_trigger_time: Option<DateTime<Utc>>,
    }

    impl ScheduleTriggerRunner {
        pub(crate) fn try_new(
            trigger_spec: &TriggerSpecificationDefinition,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Result<Self, PluginError> {
            match trigger_spec {
                TriggerSpecificationDefinition::AllTablesWalWrite
                | TriggerSpecificationDefinition::SingleTableWalWrite { .. } => {
                    Err(anyhow!("shouldn't have table trigger for scheduled plugin").into())
                }
                TriggerSpecificationDefinition::RequestPath { .. } => {
                    Err(anyhow!("shouldn't have request path trigger for scheduled plugin").into())
                }
                TriggerSpecificationDefinition::Schedule { schedule } => {
                    let schedule = CronSchedule::from_str(schedule.as_str())
                        .context("cron schedule should be parsable")?;
                    Ok(Self::new_cron(schedule, time_provider))
                }
                TriggerSpecificationDefinition::Every { duration } => {
                    // check that duration isn't longer than a year, so we avoid overflows.
                    if *duration > parse_duration("1 year").unwrap() {
                        return Err(
                            anyhow!("schedule duration cannot be greater than 1 year").into()
                        );
                    }
                    Ok(Self::new_every(
                        Duration::from_std(*duration)
                            .context("should be able to convert durations. ")?,
                        time_provider,
                    ))
                }
            }
        }
        fn new_cron(cron_schedule: CronSchedule, time_provider: Arc<dyn TimeProvider>) -> Self {
            let mut schedule = Box::new(cron_schedule.after_owned(time_provider.now().date_time()));
            let next_trigger_time = schedule.next();
            Self {
                schedule: Schedule::Cron(schedule),
                next_trigger_time,
            }
        }

        fn new_every(duration: Duration, time_provider: Arc<dyn TimeProvider>) -> Self {
            let now = time_provider.now().date_time();
            let duration_millis = duration.num_milliseconds();
            let now_millis = now.timestamp_millis();
            let next_trigger_millis = ((now_millis / duration_millis) + 1) * duration_millis;
            let next_trigger_time = Some(
                DateTime::from_timestamp_millis(next_trigger_millis)
                    .expect("can't be out of range"),
            );
            Self {
                schedule: Schedule::Every(duration),
                next_trigger_time,
            }
        }

        async fn run_at_time(
            plugin: TriggerPlugin,
            trigger_time: DateTime<Utc>,
            db_schema: Arc<DatabaseSchema>,
        ) -> Result<PluginNextState, PluginError> {
            // This loop is here just for the retry case.
            loop {
                let query_executor = Arc::clone(&plugin.query_executor);
                let logger = PluginLogger::production(plugin.logger.clone());
                let trigger_arguments = plugin.trigger_definition.trigger_arguments.clone();
                let schema = Arc::clone(&db_schema);
                let py_cache = PyCache::new_trigger_cache(
                    Arc::clone(&plugin.manager.cache),
                    plugin.trigger_definition.database_name.to_string(),
                    plugin.trigger_definition.trigger_name.to_string(),
                );

                let plugin_code_str = plugin.plugin_code.code();
                let plugin_root = plugin.plugin_code.plugin_root().cloned();
                let write_buffer = Arc::clone(&plugin.write_buffer);
                let result = tokio::task::spawn_blocking(move || {
                    execute_schedule_trigger(
                        plugin_code_str.as_ref(),
                        trigger_time,
                        schema,
                        query_executor,
                        write_buffer,
                        logger,
                        &trigger_arguments,
                        py_cache,
                        plugin_root.as_deref(),
                    )
                })
                .await?;
                match plugin
                    .handle_trigger_result(result, "schedule plugin")
                    .await
                {
                    TriggerResultAction::Success => {
                        return Ok(PluginNextState::SuccessfulRun);
                    }
                    TriggerResultAction::LogError(msg) => {
                        return Ok(PluginNextState::LogError(msg));
                    }
                    TriggerResultAction::Retry => {
                        warn!(
                            "retrying trigger {} on error",
                            plugin.trigger_definition.trigger_name
                        );
                    }
                    TriggerResultAction::Disable(def) => {
                        return Ok(PluginNextState::Disable(def));
                    }
                }
            }
        }

        fn advance_time(&mut self) {
            self.next_trigger_time = match &mut self.schedule {
                Schedule::Cron(schedule) => schedule.next(),
                Schedule::Every(duration) => self.next_trigger_time.map(|time| time + *duration),
            };
        }

        /// A funky little method to get a tokio Instant that we can call `tokio::time::sleep_until()` on.
        fn next_run_time(&self) -> Option<Time> {
            let next_trigger_time = Time::from_datetime(*self.next_trigger_time.as_ref()?);
            Some(next_trigger_time)
        }
    }
}

/// Execute a WAL plugin in dry-run mode for testing purposes.
///
/// Runs the plugin with real queries but buffers all writes without persisting them.
/// The buffered writes are returned in the response for inspection. This allows testing
/// plugin behavior without affecting the database.
///
/// # Differences from production
///
/// In production, writes are validated synchronously and errors are thrown as Python
/// exceptions, stopping plugin execution on the first error. In dry-run mode, all writes
/// are accepted during execution and validated afterwards. This means all errors are
/// collected and returned in the response, allowing developers to see all issues at once.
/// However, plugins with error-handling logic may behave differently than in production.
pub(crate) fn run_dry_run_wal_plugin(
    now_time: iox_time::Time,
    catalog: Arc<Catalog>,
    query_executor: Arc<dyn QueryExecutor>,
    buffering_writer: Arc<DryRunBufferer>,
    code: String,
    cache: Arc<Mutex<CacheStore>>,
    request: WalPluginTestRequest,
) -> Result<WalPluginTestResponse, PluginError> {
    use influxdb3_wal::Gen1Duration;
    use influxdb3_write::write_buffer::validator::WriteValidator;

    let database = request.database;
    let namespace = NamespaceName::new(database.clone())
        .map_err(|_e| PluginError::InvalidDatabase(database.clone()))?;
    // parse the lp into a write batch
    let validator = WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog))?;
    let parsed = validator.v1_parse_lines_and_catalog_updates(
        &request.input_lp,
        false,
        now_time,
        Precision::Nanosecond,
    )?;
    let mut inner = catalog.clone_inner();
    let db = parsed
        .inner()
        .txn()
        .apply_to_inner(&mut inner)
        .context("apply_to_inner failed")?;
    let data = parsed.ignore_catalog_changes_and_convert_lines_to_buffer(Gen1Duration::new_1m());

    let return_state = influxdb3_py_api::system_py::execute_wal_flush_trigger(
        &code,
        &data.valid_data,
        db,
        Arc::clone(&query_executor),
        Arc::clone(&buffering_writer) as Arc<dyn Bufferer>,
        PluginLogger::dry_run(),
        None,
        &request.input_arguments,
        PyCache::new_test_cache(
            cache,
            request
                .cache_name
                .unwrap_or_else(|| "_shared_test".to_string()),
        ),
        None,
    )?;

    // Collect writes in production order: synchronous writes (write_sync/write_sync_to_db)
    // happen during plugin execution, then legacy batched writes (write/write_to_db) are
    // processed after execution completes.
    let writes_vec = buffering_writer.get_writes();
    let mut database_writes: HashMap<String, Vec<String>> =
        HashMap::with_capacity(writes_vec.len().max(return_state.write_db_lines.len() + 1));
    for (db, lp) in writes_vec {
        database_writes.entry(db).or_default().push(lp);
    }
    for (db, lp) in return_state.write_db_lines {
        database_writes.entry(db).or_default().extend(lp);
    }
    if !return_state.write_back_lines.is_empty() {
        database_writes
            .entry(database)
            .or_default()
            .extend(return_state.write_back_lines);
    }

    let log_lines = return_state
        .log_lines
        .iter()
        .map(|l| l.to_string())
        .collect();

    let validator = DryRunWriteHandler::new(Arc::clone(&catalog), now_time);
    let errors = validator.validate_all_writes(&database_writes);

    Ok(WalPluginTestResponse {
        log_lines,
        database_writes,
        errors,
    })
}

/// Execute a schedule plugin in dry-run mode for testing purposes.
///
/// Runs the plugin with real queries but buffers all writes without persisting them.
/// The buffered writes are returned in the response for inspection. This allows testing
/// plugin behavior without affecting the database's data.
///
/// # Differences from production
///
/// In production, writes are validated synchronously and errors are thrown as Python
/// exceptions, stopping plugin execution on the first error. In dry-run mode, all writes
/// are accepted during execution and validated afterwards. This means all errors are
/// collected and returned in the response, allowing developers to see all issues at once.
/// However, plugins with error-handling logic may behave differently than in production.
pub(crate) fn run_dry_run_schedule_plugin(
    now_time: iox_time::Time,
    catalog: Arc<Catalog>,
    query_executor: Arc<dyn QueryExecutor>,
    buffering_writer: Arc<DryRunBufferer>,
    code: String,
    cache: Arc<Mutex<CacheStore>>,
    request: influxdb3_types::http::SchedulePluginTestRequest,
) -> Result<influxdb3_types::http::SchedulePluginTestResponse, PluginError> {
    let database = request.database;
    let db = catalog.db_schema(&database).ok_or(PluginError::MissingDb)?;

    let cron_schedule = request.schedule.as_deref().unwrap_or("* * * * * *");

    let schedule = cron::Schedule::from_str(cron_schedule)?;
    let Some(schedule_time) = schedule.after(&now_time.date_time()).next() else {
        return Err(PluginError::CronScheduleNeverTriggers(
            cron_schedule.to_string(),
        ));
    };

    let return_state = influxdb3_py_api::system_py::execute_schedule_trigger(
        &code,
        schedule_time,
        db,
        Arc::clone(&query_executor),
        Arc::clone(&buffering_writer) as Arc<dyn Bufferer>,
        PluginLogger::dry_run(),
        &request.input_arguments,
        PyCache::new_test_cache(
            cache,
            request
                .cache_name
                .unwrap_or_else(|| "_shared_test".to_string()),
        ),
        None,
    )?;

    let log_lines: Vec<String> = return_state
        .log_lines
        .iter()
        .map(|l| l.to_string())
        .collect();

    // Collect writes in production order: synchronous writes (write_sync/write_sync_to_db)
    // happen during plugin execution, then legacy batched writes (write/write_to_db) are
    // processed after execution completes.
    let writes_vec = buffering_writer.get_writes();
    let mut database_writes: HashMap<String, Vec<String>> =
        HashMap::with_capacity(writes_vec.len().max(return_state.write_db_lines.len() + 1));
    for (db, lp) in writes_vec {
        database_writes.entry(db).or_default().push(lp);
    }
    for (db, lp) in return_state.write_db_lines {
        database_writes.entry(db).or_default().extend(lp);
    }
    if !return_state.write_back_lines.is_empty() {
        database_writes
            .entry(database)
            .or_default()
            .extend(return_state.write_back_lines);
    }

    let validator = DryRunWriteHandler::new(Arc::clone(&catalog), now_time);
    let errors = validator.validate_all_writes(&database_writes);
    let trigger_time = schedule_time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);

    Ok(influxdb3_types::http::SchedulePluginTestResponse {
        trigger_time: Some(trigger_time),
        log_lines,
        database_writes,
        errors,
    })
}

/// Validates writes in dry-run mode using the catalog and line protocol parser.
struct DryRunWriteHandler {
    catalog: Arc<Catalog>,
    now_time: iox_time::Time,
}

impl DryRunWriteHandler {
    fn new(catalog: Arc<Catalog>, now_time: iox_time::Time) -> Self {
        Self { catalog, now_time }
    }

    /// Validates a vec of lines for a namespace, returning any errors as strings.
    fn validate_write_lines(
        &self,
        namespace: NamespaceName<'static>,
        lines: &[String],
    ) -> Vec<String> {
        use influxdb3_wal::Gen1Duration;
        use influxdb3_write::write_buffer::validator::WriteValidator;

        let mut errors = Vec::new();
        let db_name = namespace.as_str();

        let validator =
            match WriteValidator::initialize(namespace.clone(), Arc::clone(&self.catalog)) {
                Ok(v) => v,
                Err(e) => {
                    errors.push(format!(
                        "Failed to initialize validator for db {db_name}: {e}"
                    ));
                    return errors;
                }
            };

        let lp = lines.join("\n");
        match validator.v1_parse_lines_and_catalog_updates(
            &lp,
            false,
            self.now_time,
            Precision::Nanosecond,
        ) {
            Ok(data) => {
                let data =
                    data.ignore_catalog_changes_and_convert_lines_to_buffer(Gen1Duration::new_1m());
                for err in data.errors {
                    errors.push(format!("{err:?}"));
                }
            }
            Err(write_buffer::Error::ParseError(e)) => {
                errors.push(format!(
                    "line protocol parse error on write to db {db_name}: {e:?}"
                ));
            }
            Err(e) => {
                errors.push(format!(
                    "Failed to validate output lines to db {db_name}: {e}"
                ));
            }
        }
        errors
    }

    fn validate_all_writes(&self, writes: &HashMap<String, Vec<String>>) -> Vec<String> {
        let mut all_errors = Vec::new();
        for (db_name, lines) in writes {
            let namespace = match NamespaceName::new(db_name.to_string()) {
                Ok(namespace) => namespace,
                Err(e) => {
                    all_errors.push(format!("database name {db_name} is invalid: {e}"));
                    continue;
                }
            };

            let db_errors = self.validate_write_lines(namespace, lines);
            all_errors.extend(db_errors);
        }

        all_errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtualenv::init_pyo3;
    use chrono::{TimeZone, Utc};
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
    use influxdb3_id::DbId;
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_write::Precision;
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_plugin() {
        init_pyo3();
        let now = Time::from_timestamp_nanos(1);
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
        let cache = Arc::new(Mutex::new(CacheStore::new(
            Arc::clone(&time_provider),
            Duration::from_secs(10),
        )));
        let catalog = Catalog::new(
            "foo",
            Arc::new(InMemory::new()),
            time_provider,
            Default::default(),
        )
        .await
        .unwrap();
        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("arg1: " + args["arg1"])

    for table_batch in table_batches:
        influxdb3_local.info("table: " + table_batch["table_name"])

        for row in table_batch["rows"]:
            influxdb3_local.info("row: " + str(row))

    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    other_line = LineBuilder("other_table")
    other_line.int64_field("other_field", 1)
    other_line.float64_field("other_field2", 3.14)
    other_line.time_ns(1302)

    influxdb3_local.write_to_db("mytestdb", other_line)

    influxdb3_local.info("done")"#;

        let lp = [
            "cpu,host=A,region=west usage=1i,system=23.2 100",
            "mem,host=B user=43.1 120",
        ]
        .join("\n");

        let request = WalPluginTestRequest {
            filename: "test".into(),
            database: "_testdb".into(),
            input_lp: lp,
            cache_name: None,
            input_arguments: Some(HashMap::from([(
                String::from("arg1"),
                String::from("val1"),
            )])),
        };

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);
        let buffering_writer = Arc::new(DryRunBufferer::new());

        let response = run_dry_run_wal_plugin(
            now,
            Arc::new(catalog),
            executor,
            Arc::clone(&buffering_writer),
            code.to_string(),
            cache,
            request,
        )
        .unwrap();

        let plugin_log_lines: Vec<_> = response
            .log_lines
            .iter()
            .filter(|l| {
                !l.starts_with("INFO: starting execution")
                    && !l.starts_with("INFO: finished execution")
            })
            .cloned()
            .collect();
        let expected_log_lines = vec![
            "INFO: arg1: val1",
            "INFO: table: cpu",
            "INFO: row: {'host': 'A', 'region': 'west', 'usage': 1, 'system': 23.2, 'time': 100}",
            "INFO: table: mem",
            "INFO: row: {'host': 'B', 'user': 43.1, 'time': 120}",
            "INFO: done",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        assert_eq!(plugin_log_lines, expected_log_lines);

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            response.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );
        let expected_mytestdb_lines =
            vec!["other_table other_field=1i,other_field2=3.14 1302".to_string()];
        assert_eq!(
            response.database_writes.get("mytestdb").unwrap(),
            &expected_mytestdb_lines
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wal_plugin_invalid_lines() {
        init_pyo3();
        // set up a catalog and write some data into it to create a schema
        let now = Time::from_timestamp_nanos(1);
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
        let cache = Arc::new(Mutex::new(CacheStore::new(
            Arc::clone(&time_provider),
            Duration::from_secs(10),
        )));
        let catalog = Arc::new(
            Catalog::new(
                "foo",
                Arc::new(InMemory::new()),
                time_provider,
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let namespace = NamespaceName::new("foodb").unwrap();
        let validator =
            WriteValidator::initialize(namespace.clone(), Arc::clone(&catalog)).unwrap();
        let _data = validator
            .v1_parse_lines_and_catalog_updates(
                "cpu,host=A f1=10i 100",
                false,
                now,
                Precision::Nanosecond,
            )
            .unwrap();

        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    cpu_valid = LineBuilder("cpu")\
        .tag("host", "A")\
        .int64_field("f1", 10)\
        .uint64_field("f2", 20)\
        .bool_field("f3", True)
    influxdb3_local.write_to_db("foodb", cpu_valid)

    cpu_invalid = LineBuilder("cpu")\
        .tag("host", "A")\
        .string_field("f1", "not_an_int")
    influxdb3_local.write_to_db("foodb", cpu_invalid)"#;

        let lp = ["mem,host=B user=43.1 120"].join("\n");

        let request = WalPluginTestRequest {
            filename: "test".into(),
            database: "_testdb".into(),
            input_lp: lp,
            cache_name: None,
            input_arguments: None,
        };

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);
        let buffering_writer = Arc::new(DryRunBufferer::new());

        let response = run_dry_run_wal_plugin(
            now,
            Arc::clone(&catalog),
            executor,
            Arc::clone(&buffering_writer),
            code.to_string(),
            cache,
            request,
        )
        .unwrap();

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            response.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );

        // Both lines to foodb should be captured
        let expected_foodb_lines = vec![
            "cpu,host=A f1=10i,f2=20u,f3=t".to_string(),
            "cpu,host=A f1=\"not_an_int\"".to_string(),
        ];
        assert_eq!(
            response.database_writes.get("foodb").unwrap(),
            &expected_foodb_lines
        );

        // Validator catches the schema mismatch: f1 was defined as int but second write uses string
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].contains("f1"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_schedule_plugin_py_api_surface_area() {
        init_pyo3();

        let code = r#"
def process_scheduled_call(influxdb3_local, call_time, args=None):
    allowed = {"info", "warn", "error", "query", "write", "cache", "write_to_db", "write_sync", "write_sync_to_db"}
    attrs = {name for name in dir(influxdb3_local) if not name.startswith("__")}
    extras = attrs - allowed
    missing = allowed - attrs
    if extras or missing:
        raise RuntimeError(f"unexpected attributes: extras={sorted(extras)}, missing={sorted(missing)}")
"#;

        let cache = Arc::new(Mutex::new(CacheStore::new(
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Duration::from_secs(10),
        )));
        let buffering_writer = Arc::new(DryRunBufferer::new());

        let result = influxdb3_py_api::system_py::execute_schedule_trigger(
            code,
            Utc.timestamp_opt(0, 0).unwrap(),
            Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
            Arc::new(UnimplementedQueryExecutor),
            buffering_writer,
            influxdb3_py_api::system_py::PluginLogger::dry_run(),
            &None::<HashMap<String, String>>,
            PyCache::new_test_cache(cache, "_shared_test".to_string()),
            None,
        );

        assert!(
            result.is_ok(),
            "PyPluginCallApi exposes unexpected Python methods: {result:?}"
        );
    }

    /// Tests that single-file plugins execute in isolated namespaces.
    ///
    /// Without namespace isolation, concurrent plugins could overwrite each other's
    /// function definitions (e.g., both define `foo()` with different signatures).
    /// This test runs two plugins sequentially and verifies the second doesn't see
    /// the first's helper function leaked into `__main__`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_plugin_namespace_isolation() {
        init_pyo3();
        let now = Time::from_timestamp_nanos(1);
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
        let cache = Arc::new(Mutex::new(CacheStore::new(
            Arc::clone(&time_provider),
            Duration::from_secs(10),
        )));
        let catalog = Arc::new(
            Catalog::new(
                "foo",
                Arc::new(InMemory::new()),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );

        // Plugin A defines helper_a
        let code_a = r#"
def helper_a():
    return 42

def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info(f"plugin_a: helper_a() = {helper_a()}")"#;

        // Plugin B checks if helper_a leaked into __main__ (it shouldn't have)
        let code_b = r#"
def helper_b():
    return 99

def process_writes(influxdb3_local, table_batches, args=None):
    import __main__
    # If isolation is broken, helper_a from plugin A would be in __main__
    helper_a_leaked = 'helper_a' in dir(__main__)
    helper_b_leaked = 'helper_b' in dir(__main__)
    influxdb3_local.info(f"helper_a_leaked: {helper_a_leaked}")
    influxdb3_local.info(f"helper_b_leaked: {helper_b_leaked}")
    influxdb3_local.info(f"plugin_b: helper_b() = {helper_b()}")"#;

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);
        let buffering_writer = Arc::new(DryRunBufferer::new());
        let lp = "cpu,host=A usage=1i 100";

        // Run plugin A first
        let request_a = WalPluginTestRequest {
            filename: "plugin_a".into(),
            database: "_testdb".into(),
            input_lp: lp.into(),
            cache_name: None,
            input_arguments: None,
        };
        let response_a = run_dry_run_wal_plugin(
            now,
            Arc::clone(&catalog),
            Arc::clone(&executor),
            Arc::clone(&buffering_writer),
            code_a.to_string(),
            Arc::clone(&cache),
            request_a,
        )
        .unwrap();
        assert!(
            response_a
                .log_lines
                .iter()
                .any(|l| l.contains("plugin_a: helper_a() = 42")),
            "Plugin A should have executed successfully"
        );

        // Run plugin B and check for leakage from plugin A
        let request_b = WalPluginTestRequest {
            filename: "plugin_b".into(),
            database: "_testdb".into(),
            input_lp: lp.into(),
            cache_name: None,
            input_arguments: None,
        };
        let response_b = run_dry_run_wal_plugin(
            now,
            Arc::clone(&catalog),
            executor,
            Arc::clone(&buffering_writer),
            code_b.to_string(),
            cache,
            request_b,
        )
        .unwrap();

        // Verify plugin B executed and neither helper leaked into __main__
        assert!(
            response_b
                .log_lines
                .iter()
                .any(|l| l.contains("helper_a_leaked: False")),
            "Plugin A's helper_a should NOT have leaked into __main__. Got: {:?}",
            response_b.log_lines
        );
        assert!(
            response_b
                .log_lines
                .iter()
                .any(|l| l.contains("helper_b_leaked: False")),
            "Plugin B's helper_b should NOT have leaked into __main__. Got: {:?}",
            response_b.log_lines
        );
        assert!(
            response_b
                .log_lines
                .iter()
                .any(|l| l.contains("plugin_b: helper_b() = 99")),
            "Plugin B's helper should still be callable from its own namespace"
        );
    }
}
