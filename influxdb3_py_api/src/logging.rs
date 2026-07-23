use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use influxdb3_types::DatabaseName;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::warn;
use parking_lot::Mutex;
use std::fmt::{Debug, Display};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use thiserror::Error;
use tokio::sync::mpsc;

pub const PROCESSING_ENGINE_LOGS_TABLE_NAME: &str = "processing_engine_logs";

const LOG_QUEUE_CAPACITY: usize = 1_024;
static RUN_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct ProcessingEngineLog {
    event_time: Time,
    database: DatabaseName,
    trigger_name: Arc<str>,
    plugin_filename: Arc<str>,
    log_level: LogLevel,
    log_text: String,
    run_id: Arc<str>,
    node_id: Arc<str>,
    error_details: Arc<str>,
}

#[derive(Debug, Copy, Clone)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

impl ProcessingEngineLog {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_time: Time,
        database: DatabaseName,
        trigger_name: Arc<str>,
        plugin_filename: Arc<str>,
        log_level: LogLevel,
        log_text: String,
        run_id: Arc<str>,
        node_id: Arc<str>,
        error_details: Arc<str>,
    ) -> Self {
        Self {
            event_time,
            database,
            trigger_name,
            plugin_filename,
            log_level,
            log_text,
            run_id,
            node_id,
            error_details,
        }
    }

    pub fn schema() -> Schema {
        processing_engine_logs_schema()
    }

    pub fn event_time(&self) -> Time {
        self.event_time
    }

    pub fn database(&self) -> &DatabaseName {
        &self.database
    }

    pub fn trigger_name(&self) -> &str {
        self.trigger_name.as_ref()
    }

    pub fn plugin_filename(&self) -> &str {
        self.plugin_filename.as_ref()
    }

    pub fn log_level(&self) -> LogLevel {
        self.log_level
    }

    pub fn log_text(&self) -> &str {
        &self.log_text
    }

    pub fn run_id(&self) -> &str {
        self.run_id.as_ref()
    }

    pub fn node_id(&self) -> &str {
        self.node_id.as_ref()
    }

    pub fn error_details(&self) -> &str {
        self.error_details.as_ref()
    }
}

pub fn processing_engine_logs_schema() -> Schema {
    Schema::new(vec![
        Field::new("database_name", DataType::Utf8, true),
        Field::new("error_details", DataType::Utf8, true),
        Field::new("log_level", DataType::Utf8, true),
        Field::new("log_text", DataType::Utf8, true),
        Field::new("node_id", DataType::Utf8, true),
        Field::new("plugin_filename", DataType::Utf8, true),
        Field::new("run_id", DataType::Utf8, true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("trigger_name", DataType::Utf8, true),
    ])
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("Cannot log: {0}")]
    Fail(Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait LogEndpoint: Debug + Send + Sync + 'static {
    async fn log(&self, log: ProcessingEngineLog) -> Result<(), LogError>;
}

#[derive(Debug, Clone)]
pub struct ProcessingEngineLogger {
    database: DatabaseName,
    trigger_name: Arc<str>,
    plugin_filename: Arc<str>,
    run_id: Arc<str>,
    node_id: Arc<str>,
    time_provider: Arc<dyn TimeProvider>,
    sender: mpsc::Sender<ProcessingEngineLog>,
    dropped_logs: Arc<AtomicU64>,
}

impl ProcessingEngineLogger {
    pub fn new(
        database: DatabaseName,
        trigger_name: impl Into<Arc<str>>,
        plugin_filename: impl Into<Arc<str>>,
        node_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        log_endpoint: Arc<dyn LogEndpoint>,
    ) -> Self {
        Self::new_with_capacity(
            database,
            trigger_name,
            plugin_filename,
            node_id,
            time_provider,
            log_endpoint,
            LOG_QUEUE_CAPACITY,
        )
    }

    fn new_with_capacity(
        database: DatabaseName,
        trigger_name: impl Into<Arc<str>>,
        plugin_filename: impl Into<Arc<str>>,
        node_id: impl Into<Arc<str>>,
        time_provider: Arc<dyn TimeProvider>,
        log_endpoint: Arc<dyn LogEndpoint>,
        capacity: usize,
    ) -> Self {
        let trigger_name = trigger_name.into();
        let plugin_filename = plugin_filename.into();
        let node_id = node_id.into();
        let (sender, receiver) = mpsc::channel(capacity);
        tokio::spawn(persist_logs(receiver, log_endpoint));
        let run_id = next_run_id(time_provider.now());
        Self {
            database,
            trigger_name,
            plugin_filename,
            run_id,
            node_id,
            time_provider,
            sender,
            dropped_logs: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn for_run(&self) -> Self {
        Self {
            database: self.database.clone(),
            trigger_name: Arc::clone(&self.trigger_name),
            plugin_filename: Arc::clone(&self.plugin_filename),
            run_id: next_run_id(self.time_provider.now()),
            node_id: Arc::clone(&self.node_id),
            time_provider: Arc::clone(&self.time_provider),
            sender: self.sender.clone(),
            dropped_logs: Arc::clone(&self.dropped_logs),
        }
    }

    pub fn log(&self, log_level: LogLevel, log_line: impl Into<String>) {
        let log_line = log_line.into();
        let error_details = match log_level {
            LogLevel::Error => Arc::from(log_line.as_str()),
            LogLevel::Info | LogLevel::Warn => Arc::from(""),
        };
        let log = ProcessingEngineLog::new(
            self.time_provider.now(),
            self.database.clone(),
            Arc::clone(&self.trigger_name),
            Arc::clone(&self.plugin_filename),
            log_level,
            log_line,
            Arc::clone(&self.run_id),
            Arc::clone(&self.node_id),
            error_details,
        );

        match self.sender.try_send(log) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped = self.dropped_logs.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped == 1 || dropped.is_power_of_two() {
                    warn!(
                        database = %self.database,
                        trigger_name = %self.trigger_name,
                        plugin_filename = %self.plugin_filename,
                        node_id = %self.node_id,
                        dropped,
                        "dropping processing engine logs because the persistence queue is full"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let dropped = self.dropped_logs.fetch_add(1, Ordering::Relaxed) + 1;
                warn!(
                    database = %self.database,
                    trigger_name = %self.trigger_name,
                    plugin_filename = %self.plugin_filename,
                    node_id = %self.node_id,
                    dropped,
                    "dropping processing engine log because the persistence queue is closed"
                );
            }
        }
    }

    #[cfg(test)]
    fn dropped_logs(&self) -> u64 {
        self.dropped_logs.load(Ordering::Relaxed)
    }
}

fn next_run_id(time: Time) -> Arc<str> {
    let counter = RUN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{counter}", time.timestamp_nanos()).into()
}

async fn persist_logs(
    mut receiver: mpsc::Receiver<ProcessingEngineLog>,
    log_endpoint: Arc<dyn LogEndpoint>,
) {
    while let Some(log) = receiver.recv().await {
        let source_database = log.database.clone();
        if let Err(error) = log_endpoint.log(log).await {
            warn!(
                %error,
                database = %source_database,
                table = PROCESSING_ENGINE_LOGS_TABLE_NAME,
                "failed to persist processing engine log"
            );
        }
    }
}

/// Logger abstraction for plugin execution.
///
/// In production mode, logs are persisted through the ingest path.
/// In dry run mode, logs are accumulated for the response (no persisted writes).
/// Tracing macros (info!, warn!, error!) are called in PyPluginCallApi methods for both modes.
#[derive(Debug)]
pub enum PluginLogger {
    /// Production mode: logs through the processing engine log endpoint.
    Production(ProcessingEngineLogger),
    /// Dry run mode: accumulates log_lines only.
    /// Note: Mutex is required for Send+Sync bounds (PyO3's #[macro@pyo3::pyclass] requires Send),
    /// not for actual concurrent access - execution is single-threaded within the GIL.
    // todo(pjb): potential memory issue - unbounded log accumulation
    DryRun { log_lines: Mutex<Vec<LogLine>> },
}

impl PluginLogger {
    /// Create a production logger that persists log lines.
    pub fn production(logger: ProcessingEngineLogger) -> Self {
        Self::Production(logger)
    }

    /// Create a dry run logger that accumulates log lines in memory.
    pub fn dry_run() -> Self {
        Self::DryRun {
            log_lines: Mutex::new(Vec::new()),
        }
    }

    /// Log a message at the specified level.
    ///
    /// In production mode, enqueues a log line for persistence.
    /// In dry run mode, accumulates the log line for later retrieval via `take_log_lines()`.
    pub fn log(&self, level: LogLevel, line: String) {
        match self {
            Self::Production(logger) => {
                logger.log(level, line);
            }
            Self::DryRun { log_lines } => {
                let log_line = match level {
                    LogLevel::Info => LogLine::Info(line),
                    LogLevel::Warn => LogLine::Warn(line),
                    LogLevel::Error => LogLine::Error(line),
                };
                log_lines.lock().push(log_line);
            }
        }
    }

    /// Take and return accumulated log lines.
    ///
    /// Returns an empty Vec for production loggers.
    /// Returns and clears the accumulated log lines for dry run loggers.
    pub fn take_log_lines(&self) -> Vec<LogLine> {
        match self {
            Self::Production(_) => Vec::new(),
            Self::DryRun { log_lines } => std::mem::take(&mut *log_lines.lock()),
        }
    }
}

pub enum LogLine {
    Info(String),
    Warn(String),
    Error(String),
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLine::Info(s) => write!(f, "INFO: {s}"),
            LogLine::Warn(s) => write!(f, "WARN: {s}"),
            LogLine::Error(s) => write!(f, "ERROR: {s}"),
        }
    }
}

impl std::fmt::Debug for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests;
