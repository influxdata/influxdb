use crate::{
    PluginCode, ProcessingEngineManagerImpl, RequestEvent, ScheduleEvent, WalEvent,
    environment::PythonEnvironmentManager,
};
use chrono::{DateTime, Duration, Utc};
use cron::{OwnedScheduleIterator, Schedule as CronSchedule};
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use hashbrown::HashMap;
use humantime::{format_duration, parse_duration};
use hyper::{StatusCode, http::HeaderValue};
use influxdb3_catalog::catalog::{
    Catalog, DatabaseSchema, ErrorBehavior, TriggerDefinition, TriggerSpecificationDefinition,
};
use influxdb3_id::DbId;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_processing_engine_telemetry::{
    PluginTriggerEntrypoint, PluginTriggerInvocationKey, PluginTriggerInvocationRegistry,
};
use influxdb3_py_api::{
    cache::{CacheStore, PyCache},
    logging::{LogLevel, PluginLogger, ProcessingEngineLogger},
    system_py::{execute_request_trigger, execute_schedule_trigger, execute_wal_flush_trigger},
    write::{WriteAccumulator, WriteEndpoint},
};
use influxdb3_sys_events::SysEventStore;
use influxdb3_types::{
    DatabaseName,
    http::{WalPluginTestRequest, WalPluginTestResponse},
    logging::ErrorOneLine,
};
use influxdb3_wal::{WalContents, WalOp};
use influxdb3_write::{Precision, write_buffer};
use iox_http_util::{ResponseBuilder, bytes_to_response_body};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, error, info, warn};
use std::{fmt::Debug, path::PathBuf, str::FromStr, sync::Arc, time::SystemTime};
use tokio::sync::mpsc::{self, Receiver};
use tokio_util::sync::CancellationToken;

use anyhow::{Context, anyhow};
use parking_lot::Mutex;
use thiserror::Error;

/// Await a spawned blocking plugin run, returning `None` if `cancel` fires
/// first. On cancellation we stop awaiting the `JoinHandle` and drop it: the
/// blocking task is *not* interrupted — it keeps running to completion and its
/// result is discarded — but the caller no longer waits on it, so a stuck or
/// slow run cannot delay trigger shutdown. (Core's plugin execution is not
/// cancellation-aware, so detaching is the only lever available here.)
async fn run_until_cancelled<T>(
    join: tokio::task::JoinHandle<T>,
    cancel: &CancellationToken,
) -> Option<Result<T, tokio::task::JoinError>> {
    tokio::select! {
        joined = join => Some(joined),
        _ = cancel.cancelled() => None,
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
    PluginExecutionError(#[from] influxdb3_py_api::error::ExecutePluginError),

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

    #[error(
        "plugin installation is disabled; plugins must already exist in the configured plugin directory"
    )]
    PluginInstallationDisabled,

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
    db_id: DbId,
) {
    let trigger_plugin =
        TriggerPlugin::new(db_name, plugin_code, trigger_definition, context, db_id);

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
    pub plugin_dir_only: bool,
    pub plugin_repo: Option<String>,
}

pub(crate) fn run_schedule_plugin(
    db_name: String,
    plugin_code: Arc<PluginCode>,
    trigger_definition: Arc<TriggerDefinition>,
    time_provider: Arc<dyn TimeProvider>,
    context: PluginContext,
    plugin_receiver: mpsc::Receiver<ScheduleEvent>,
    db_id: DbId,
) -> Result<(), PluginError> {
    // Ensure that the plugin is a schedule plugin
    let plugin_type = trigger_definition.trigger.plugin_type();
    if !matches!(
        plugin_type,
        influxdb3_catalog::catalog::PluginType::Schedule
    ) {
        return Err(PluginError::NonSchedulePluginWithScheduleTrigger(format!(
            "{trigger_definition:?}"
        )));
    }

    let trigger_plugin =
        TriggerPlugin::new(db_name, plugin_code, trigger_definition, context, db_id);

    let runner = ScheduleTriggerRunner::try_new(
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
    db_id: DbId,
) {
    let trigger_plugin =
        TriggerPlugin::new(db_name, plugin_code, trigger_definition, context, db_id);
    tokio::task::spawn(async move {
        trigger_plugin
            .run_request_plugin(plugin_receiver)
            .await
            //todo(pjb): expect is unpleasant here
            .expect("trigger plugin failed");
    });
}

pub(crate) struct PluginContext {
    // handler to write data back to the DB.
    pub(crate) write_endpoint: Arc<dyn WriteEndpoint>,
    // query executor to hand off to the plugin
    pub(crate) query_executor: Arc<dyn QueryExecutor>,
    // processing engine manager for disabling plugins if they fail.
    pub(crate) manager: Arc<ProcessingEngineManagerImpl>,
    // sys events for writing logs to ring buffers
    pub(crate) sys_event_store: Arc<SysEventStore>,
    // plugin invocation telemetry, when enabled by serve
    pub(crate) plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
    // per-trigger cancellation token; cancelled when the trigger is stopped, to
    // interrupt an in-flight scheduled run (see run_at_time).
    pub(crate) cancel: CancellationToken,
}

#[derive(Debug, Clone)]
struct TriggerPlugin {
    trigger_definition: Arc<TriggerDefinition>,
    plugin_code: Arc<PluginCode>,
    db_id: DbId,
    db_name: String,
    write_endpoint: Arc<dyn WriteEndpoint>,
    query_executor: Arc<dyn QueryExecutor>,
    manager: Arc<ProcessingEngineManagerImpl>,
    logger: ProcessingEngineLogger,
    plugin_trigger_invocation_registry: Option<Arc<PluginTriggerInvocationRegistry>>,
    plugin_trigger_invocation_key: PluginTriggerInvocationKey,
    /// Cancelled when this trigger is stopped (disable/force-delete), to
    /// interrupt an in-flight scheduled run so shutdown isn't blocked.
    cancel: CancellationToken,
}

impl TriggerPlugin {
    pub(crate) fn new(
        db_name: String,
        plugin_code: Arc<PluginCode>,
        trigger_definition: Arc<TriggerDefinition>,
        context: PluginContext,
        db_id: DbId,
    ) -> Self {
        let logger = ProcessingEngineLogger::new(
            context.sys_event_store,
            Arc::clone(&trigger_definition.trigger_name),
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
            db_id,
            db_name,
            write_endpoint: Arc::clone(&context.write_endpoint),
            query_executor: Arc::clone(&context.query_executor),
            manager: Arc::clone(&context.manager),
            logger,
            plugin_trigger_invocation_registry: context.plugin_trigger_invocation_registry,
            plugin_trigger_invocation_key,
            cancel: context.cancel,
        }
    }

    fn record_trigger_invocation(&self) {
        if let Some(registry) = &self.plugin_trigger_invocation_registry {
            registry.record_invocation(&self.plugin_trigger_invocation_key);
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
        let db_id = self.db_id;
        let trigger_id = self.trigger_definition.trigger_id;
        let fut = async move { manager.stop_trigger(db_id, trigger_id).await };
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
        result: influxdb3_py_api::error::ExecutePluginResult<
            influxdb3_py_api::system_py::PluginReturnState,
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
        while let Some(next_run_instant) = runner.next_run_time() {
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
                        let trigger = self.clone();
                        let fut = async move {
                            ScheduleTriggerRunner::run_at_time(trigger, trigger_time, schema).await
                        };
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
            self.db_id,
            self.trigger_definition.trigger_id,
        );

        let plugin_code_str = self.plugin_code.code();
        let plugin_root = self.plugin_code.plugin_root().cloned();
        let write_endpoint = Arc::clone(&self.write_endpoint);
        self.record_trigger_invocation();
        let result = tokio::task::spawn_blocking(move || {
            execute_request_trigger(
                plugin_code_str.as_ref(),
                schema,
                query_executor,
                write_endpoint,
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

                    self.record_trigger_invocation();

                    // loop for retries, in general it will only run once.
                    loop {
                        let logger = PluginLogger::production(self.logger.clone());
                        let plugin_code_str = Arc::clone(&plugin_code);
                        let plugin_root_clone = plugin_root.clone();
                        let query_executor = Arc::clone(&self.query_executor);
                        let write_endpoint = Arc::clone(&self.write_endpoint);
                        let schema_clone = Arc::clone(&schema);
                        let trigger_arguments = trigger_arguments.clone();
                        let wal_contents_clone = Arc::clone(&wal_contents);
                        let py_cache = PyCache::new_trigger_cache(
                            Arc::clone(&self.manager.cache),
                            self.db_id,
                            self.trigger_definition.trigger_id,
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
                                write_endpoint,
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

        for (db_name, lines) in plugin_return_state.write_db_lines {
            let Ok(database_name) = DatabaseName::new(db_name.clone()) else {
                errors.push(anyhow!("invalid database name: {db_name}"));
                continue;
            };

            if let Err(e) = self
                .write_endpoint
                .write_lp(
                    database_name,
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

struct ScheduleTriggerRunner {
    schedule: Schedule,
    next_trigger_time: Option<DateTime<Utc>>,
}

impl ScheduleTriggerRunner {
    fn try_new(
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
                    return Err(anyhow!("schedule duration cannot be greater than 1 year").into());
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
            DateTime::from_timestamp_millis(next_trigger_millis).expect("can't be out of range"),
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
        plugin.record_trigger_invocation();

        // This loop is here just for the retry case.
        loop {
            let query_executor = Arc::clone(&plugin.query_executor);
            let logger = PluginLogger::production(plugin.logger.clone());
            let trigger_arguments = plugin.trigger_definition.trigger_arguments.clone();
            let schema = Arc::clone(&db_schema);
            let py_cache = PyCache::new_trigger_cache(
                Arc::clone(&plugin.manager.cache),
                plugin.db_id,
                plugin.trigger_definition.trigger_id,
            );

            let plugin_code_str = plugin.plugin_code.code();
            let plugin_root = plugin.plugin_code.plugin_root().cloned();
            let write_endpoint = Arc::clone(&plugin.write_endpoint);
            let join = tokio::task::spawn_blocking(move || {
                execute_schedule_trigger(
                    plugin_code_str.as_ref(),
                    trigger_time,
                    schema,
                    query_executor,
                    write_endpoint,
                    logger,
                    &trigger_arguments,
                    py_cache,
                    plugin_root.as_deref(),
                )
            });
            // Race the in-flight run against cancellation. If the trigger is
            // disabled/force-deleted while a run_async=false run is in flight,
            // return promptly so the schedule loop can service the shutdown event
            // instead of blocking on the run — otherwise the run blocks the
            // trigger's shutdown ACK and wedges all trigger management on the node
            // (the disable/`delete --force` hang). This does not interrupt the
            // run: the detached spawn_blocking task keeps executing to completion
            // and its result is dropped; we simply stop awaiting it.
            let Some(joined) = run_until_cancelled(join, &plugin.cancel).await else {
                debug!(
                    trigger_name = %plugin.trigger_definition.trigger_name,
                    "scheduled plugin run cancelled before completion"
                );
                return Ok(PluginNextState::SuccessfulRun);
            };
            let result = joined?;
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
    code: String,
    cache: Arc<Mutex<CacheStore>>,
    request: WalPluginTestRequest,
) -> Result<WalPluginTestResponse, PluginError> {
    use influxdb3_wal::Gen1Duration;
    use influxdb3_write::write_buffer::validator::WriteValidator;

    let database = request.database;
    let db = DatabaseName::new(database.clone())
        .map_err(|_e| PluginError::InvalidDatabase(database.clone()))?;
    // parse the lp into a write batch
    let validator = WriteValidator::initialize(db.clone(), Arc::clone(&catalog))?;
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

    let write_accu = Arc::new(WriteAccumulator::default());

    let return_state = influxdb3_py_api::system_py::execute_wal_flush_trigger(
        &code,
        &data.valid_data,
        db,
        Arc::clone(&query_executor),
        Arc::clone(&write_accu) as _,
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
    let writes_map = write_accu.flush();
    let mut database_writes: HashMap<String, Vec<String>> =
        HashMap::with_capacity(writes_map.len().max(return_state.write_db_lines.len()));
    for (db, lp) in writes_map {
        database_writes.entry(db).or_default().extend(lp);
    }
    for (db, lp) in return_state.write_db_lines {
        database_writes.entry(db).or_default().extend(lp);
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

    let write_accu = Arc::new(WriteAccumulator::default());

    let return_state = influxdb3_py_api::system_py::execute_schedule_trigger(
        &code,
        schedule_time,
        db,
        Arc::clone(&query_executor),
        Arc::clone(&write_accu) as _,
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
    let writes_map = write_accu.flush();
    let mut database_writes: HashMap<String, Vec<String>> =
        HashMap::with_capacity(writes_map.len().max(return_state.write_db_lines.len()));
    for (db, lp) in writes_map {
        database_writes.entry(db).or_default().extend(lp);
    }
    for (db, lp) in return_state.write_db_lines {
        database_writes.entry(db).or_default().extend(lp);
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

    /// Validates a vec of lines for a database, returning any errors as strings.
    fn validate_write_lines(&self, database: DatabaseName, lines: &[String]) -> Vec<String> {
        use influxdb3_wal::Gen1Duration;
        use influxdb3_write::write_buffer::validator::WriteValidator;

        let mut errors = Vec::new();
        let db_name = database.as_str();

        let validator =
            match WriteValidator::initialize(database.clone(), Arc::clone(&self.catalog)) {
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
            let database = match DatabaseName::new(db_name.to_string()) {
                Ok(database) => database,
                Err(e) => {
                    all_errors.push(format!("database name {db_name} is invalid: {e}"));
                    continue;
                }
            };

            let db_errors = self.validate_write_lines(database, lines);
            all_errors.extend(db_errors);
        }

        all_errors
    }
}

#[cfg(test)]
mod tests;
