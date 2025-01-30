use arrow_array::builder::{StringBuilder, TimestampNanosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use influxdb3_sys_events::{Event, RingBuffer, ToRecordBatch};
use iox_time::Time;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug)]
pub struct ProcessingEngineLog {
    event_time: Time,
    log_level: LogLevel,
    trigger_name: String,
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
        trigger_name: String,
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
            trigger_name_builder.append_value(event.trigger_name.as_str());
            log_level_builder.append_value(event.log_level.to_string().as_str());
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
