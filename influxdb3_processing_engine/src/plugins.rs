use crate::{
    environment::PythonEnvironmentManager,
    scheduler::{Scheduler, TriggerInvocation, TriggerKey, TriggerPayload},
    wal::write_batch_to_wal_content,
};
use chrono::{DateTime, Duration, Utc};
use cron::{OwnedScheduleIterator, Schedule as CronSchedule};
use hashbrown::HashMap;
use humantime::parse_duration;
#[cfg(test)]
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_catalog::catalog::{Catalog, TriggerDefinition, TriggerSpecificationDefinition};
use influxdb3_py_api::{
    cache::{CacheStore, PyCache},
    logging::PluginLogger,
    query::QueryEndpoint,
    write::WriteAccumulator,
};
use influxdb3_types::{
    DatabaseName,
    http::{WalPluginTestRequest, WalPluginTestResponse},
};
use influxdb3_write::{Precision, write_buffer};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::error;
use std::{fmt::Debug, path::PathBuf, str::FromStr, sync::Arc};
use tokio_util::sync::CancellationToken;

use anyhow::{Context, anyhow};
use parking_lot::Mutex;
use thiserror::Error;

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

    #[error("non-schedule plugin with schedule trigger: {0}")]
    NonSchedulePluginWithScheduleTrigger(String),

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

#[derive(Debug, Clone)]
pub struct ProcessingEngineEnvironmentManager {
    pub plugin_dir: Option<PathBuf>,
    pub virtual_env_location: Option<PathBuf>,
    pub package_manager: Arc<dyn PythonEnvironmentManager>,
    pub plugin_dir_only: bool,
    pub plugin_repo: Option<String>,
}

pub(crate) fn run_schedule_event_source(
    trigger_definition: Arc<TriggerDefinition>,
    time_provider: Arc<dyn TimeProvider>,
    scheduler: Scheduler,
    key: TriggerKey,
    cancel: CancellationToken,
) -> Result<(), PluginError> {
    if !matches!(
        trigger_definition.trigger.plugin_type(),
        influxdb3_catalog::catalog::PluginType::Schedule
    ) {
        return Err(PluginError::NonSchedulePluginWithScheduleTrigger(format!(
            "{trigger_definition:?}"
        )));
    }

    let runner =
        ScheduleTriggerRunner::try_new(&trigger_definition.trigger, Arc::clone(&time_provider))?;
    tokio::task::spawn(async move {
        if let Err(error) =
            run_schedule_event_source_loop(runner, time_provider, scheduler, key, cancel).await
        {
            error!(%error, "schedule event source failed");
        }
    });

    Ok(())
}

async fn run_schedule_event_source_loop(
    mut runner: ScheduleTriggerRunner,
    time_provider: Arc<dyn TimeProvider>,
    scheduler: Scheduler,
    key: TriggerKey,
    cancel: CancellationToken,
) -> Result<(), PluginError> {
    while let Some(next_run_instant) = runner.next_run_time() {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = time_provider.sleep_until(next_run_instant) => {
                let Some(scheduled_at) = runner.next_trigger_time else {
                    return Err(anyhow!("running a cron trigger that is finished.").into());
                };
                runner.advance_time();
                if scheduler
                    .enqueue(TriggerInvocation::new(
                        key,
                        TriggerPayload::Schedule { scheduled_at },
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    Ok(())
}

enum Schedule {
    Cron(Box<OwnedScheduleIterator<Utc>>),
    Every(Duration),
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

/// Collect writes in production order: synchronous writes
/// (`write_sync`/`write_sync_to_db`) happen during plugin execution, then legacy
/// batched writes (`write`/`write_to_db`) are processed after execution
/// completes.
fn collect_dry_run_writes(
    synchronous_writes: HashMap<String, Vec<String>>,
    legacy_writes: HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    let mut database_writes: HashMap<String, Vec<String>> =
        HashMap::with_capacity(synchronous_writes.len().max(legacy_writes.len()));
    for (db, lp) in synchronous_writes.into_iter().chain(legacy_writes) {
        database_writes.entry(db).or_default().extend(lp);
    }
    database_writes
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
    query_endpoint: Arc<dyn QueryEndpoint>,
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

    let wal_content = write_batch_to_wal_content(&data.valid_data, &db)?;

    let write_accu = Arc::new(WriteAccumulator::default());

    let return_state = influxdb3_py_api::system_py::execute_wal_flush_trigger(
        &code,
        &wal_content,
        db,
        Arc::clone(&query_endpoint),
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
        CancellationToken::new(),
    )?;

    let log_lines = return_state
        .log_lines
        .iter()
        .map(|l| l.to_string())
        .collect();
    let database_writes = collect_dry_run_writes(write_accu.flush(), return_state.write_db_lines);

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
    query_endpoint: Arc<dyn QueryEndpoint>,
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
        Arc::clone(&query_endpoint),
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
        CancellationToken::new(),
    )?;

    let log_lines: Vec<String> = return_state
        .log_lines
        .iter()
        .map(|l| l.to_string())
        .collect();
    let database_writes = collect_dry_run_writes(write_accu.flush(), return_state.write_db_lines);

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
