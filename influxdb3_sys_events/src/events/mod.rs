use std::{sync::Arc, time::Duration};

use arrow::{
    array::StringViewBuilder,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
use arrow_array::{ArrayRef, RecordBatch};
use serde::Serialize;

use crate::{Event, RingBuffer, ToRecordBatch};

#[derive(Debug, Clone, Serialize)]
pub struct SuccessInfo {
    pub host: Arc<str>,
    pub sequence_number: u64,
    pub fetch_duration: Duration,
    pub db_count: u64,
    pub table_count: u64,
    pub file_count: u64,
}

impl SuccessInfo {
    pub fn new(
        host: &str,
        sequence_number: u64,
        duration: Duration,
        db_table_file_counts: (u64, u64, u64),
    ) -> Self {
        Self {
            host: Arc::from(host),
            sequence_number,
            fetch_duration: duration,
            db_count: db_table_file_counts.0,
            table_count: db_table_file_counts.1,
            file_count: db_table_file_counts.2,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedInfo {
    pub host: Arc<str>,
    pub sequence_number: u64,
    // TODO: add duration for failed events
    // Failed events happen through out the code,
    // this may require some sort of boundary to
    // be able to track time it ran for before
    // hitting an error.
    // pub duration: Duration,
    pub error: String,
}

#[derive(Debug, Clone)]
pub enum CompactionEvent {
    SnapshotFetched(SnapshotFetched),
}

impl CompactionEvent {
    pub fn snapshot_success(success_info: SuccessInfo) -> Self {
        CompactionEvent::SnapshotFetched(SnapshotFetched::Success(success_info))
    }

    pub fn snapshot_failed(failed_info: FailedInfo) -> Self {
        CompactionEvent::SnapshotFetched(SnapshotFetched::Failed(failed_info))
    }
}

#[derive(Debug, Clone)]
pub enum SnapshotFetched {
    Success(SuccessInfo),
    Failed(FailedInfo),
}

impl ToRecordBatch<CompactionEvent> for CompactionEvent {
    fn schema() -> arrow::datatypes::Schema {
        let columns = struct_fields();
        Schema::new(columns)
    }

    fn to_record_batch(
        buffer: Option<&RingBuffer<Event<CompactionEvent>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        buffer.map(|buf| {
            let capacity = buf.in_order().count();
            let mut event_time_arr = StringViewBuilder::with_capacity(capacity);
            let mut event_type_arr = StringViewBuilder::with_capacity(capacity);
            let mut event_duration_arr = StringViewBuilder::with_capacity(capacity);
            let mut event_status_arr = StringViewBuilder::with_capacity(capacity);
            let mut event_data_arr = StringViewBuilder::with_capacity(capacity);

            for event in buf.in_order() {
                event_time_arr.append_value(event.format_event_time());
                // TODO: externalise this in subsequent PR
                event_type_arr.append_value("SNAPSHOT_FETCHED");
                match &event.data {
                    CompactionEvent::SnapshotFetched(snapshot_ev_info) => match snapshot_ev_info {
                        SnapshotFetched::Success(success_info) => {
                            event_status_arr.append_value("Success");
                            event_duration_arr.append_value(
                                humantime::format_duration(success_info.fetch_duration).to_string(),
                            );
                            event_data_arr
                                .append_value(serde_json::to_string(success_info).unwrap());
                        }
                        SnapshotFetched::Failed(failed_info) => {
                            event_status_arr.append_value("Failed");
                            event_duration_arr.append_null();
                            event_data_arr
                                .append_value(serde_json::to_string(failed_info).unwrap());
                        }
                    },
                }
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(event_time_arr.finish()),
                Arc::new(event_type_arr.finish()),
                Arc::new(event_duration_arr.finish()),
                Arc::new(event_status_arr.finish()),
                Arc::new(event_data_arr.finish()),
            ];
            RecordBatch::try_new(Arc::new(Self::schema()), columns)
        })
    }
}

fn struct_fields() -> Vec<Field> {
    vec![
        Field::new("event_time", DataType::Utf8View, false),
        Field::new("event_type", DataType::Utf8View, false),
        // TODO: once failure event duration tracking is fixed,
        //       event_duration can be non-nullable
        Field::new("event_duration", DataType::Utf8View, true),
        Field::new("event_status", DataType::Utf8View, false),
        Field::new("event_data", DataType::Utf8View, false),
    ]
}
