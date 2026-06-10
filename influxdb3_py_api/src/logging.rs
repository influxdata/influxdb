use arrow_array::builder::{StringBuilder, TimestampNanosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use influxdb3_sys_events::{Event, RingBuffer, SysEventStore, ToRecordBatch};
use iox_time::Time;
use parking_lot::Mutex;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug)]
pub struct ProcessingEngineLog {
    event_time: Time,
    log_level: LogLevel,
    trigger_name: Arc<str>,
    log_line: String,
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
    pub fn new(
        event_time: Time,
        log_level: LogLevel,
        trigger_name: Arc<str>,
        log_line: String,
    ) -> Self {
        Self {
            event_time,
            log_level,
            trigger_name,
            log_line,
        }
    }
}

impl ToRecordBatch<ProcessingEngineLog> for ProcessingEngineLog {
    fn schema() -> Schema {
        let fields = vec![
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("trigger_name", DataType::Utf8, false),
            Field::new("log_level", DataType::Utf8, false),
            Field::new("log_text", DataType::Utf8, false),
        ];
        Schema::new(fields)
    }

    fn to_record_batch(
        items: Option<&RingBuffer<Event<ProcessingEngineLog>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        let items = items?;
        let capacity = items.len();
        let mut event_time_builder = TimestampNanosecondBuilder::with_capacity(capacity);
        let mut trigger_name_builder = StringBuilder::new();
        let mut log_level_builder = StringBuilder::new();
        let mut log_text_builder = StringBuilder::new();
        for item in items.in_order() {
            let event = &item.data;
            event_time_builder.append_value(event.event_time.timestamp_nanos());
            trigger_name_builder.append_value(&event.trigger_name);
            log_level_builder.append_value(event.log_level.to_string());
            log_text_builder.append_value(event.log_line.as_str());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(event_time_builder.finish()),
            Arc::new(trigger_name_builder.finish()),
            Arc::new(log_level_builder.finish()),
            Arc::new(log_text_builder.finish()),
        ];

        Some(RecordBatch::try_new(Arc::new(Self::schema()), columns))
    }
}

#[derive(Debug, Clone)]
pub struct ProcessingEngineLogger {
    sys_event_store: Arc<SysEventStore>,
    trigger_name: Arc<str>,
}

impl ProcessingEngineLogger {
    pub fn new(sys_event_store: Arc<SysEventStore>, trigger_name: impl Into<Arc<str>>) -> Self {
        Self {
            sys_event_store,
            trigger_name: trigger_name.into(),
        }
    }

    pub fn log(&self, log_level: LogLevel, log_line: impl Into<String>) {
        self.sys_event_store.record(ProcessingEngineLog::new(
            self.sys_event_store.time_provider().now(),
            log_level,
            Arc::clone(&self.trigger_name),
            log_line.into(),
        ))
    }
}

/// Logger abstraction for plugin execution.
///
/// In production mode, logs are written to the sys_event_store only.
/// In dry run mode, logs are accumulated for the response (no sys_event_store writes).
/// Tracing macros (info!, warn!, error!) are called in PyPluginCallApi methods for both modes.
#[derive(Debug)]
pub enum PluginLogger {
    /// Production mode: logs to sys_event_store only
    Production(ProcessingEngineLogger),
    /// Dry run mode: accumulates log_lines only.
    /// Note: Mutex is required for Send+Sync bounds (PyO3's #[macro@pyo3::pyclass] requires Send),
    /// not for actual concurrent access - execution is single-threaded within the GIL.
    // todo(pjb): potential memory issue - unbounded log accumulation
    DryRun { log_lines: Mutex<Vec<LogLine>> },
}

impl PluginLogger {
    /// Create a production logger that writes to sys_event_store.
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
    /// In production mode, writes to the sys_event_store for persistence.
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
