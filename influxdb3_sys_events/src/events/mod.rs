use std::sync::Arc;

use arrow::{
    array::{StringViewBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
use arrow_array::{ArrayRef, RecordBatch};

use crate::{
    events::{
        catalog_fetched::CatalogFetched, compaction_planned::CompactionPlanned,
        snapshot_fetched::SnapshotFetched,
    },
    Event, RingBuffer, ToRecordBatch,
};

pub mod catalog_fetched;
pub mod compaction_planned;
pub mod snapshot_fetched;

/// This is the root of all compaction related events. Compaction itself has
/// - Producer side which requires following steps
///   1. Snapshot / Catalog fetching
///   2. Planning
///   3. Running compaction
/// - Consumer side which uses the compacted data in queries
///
/// The variants are stored in single buffer. If needed (due to contention) we
/// can separate the compaction related events saved separately (in separate buffers).
/// This will require at the time of querying to pull events from separate buffers
/// and put them together (sorted by event time). If compaction itself runs concurrently
/// (i.e for different generations?) then this might be a nice addition.
#[derive(Debug, Clone)]
pub enum CompactionEvent {
    SnapshotFetched(SnapshotFetched),
    CatalogFetched(CatalogFetched),
    CompactionPlanned(CompactionPlanned),
}

impl CompactionEvent {
    pub fn snapshot_success(success_info: snapshot_fetched::SuccessInfo) -> Self {
        CompactionEvent::SnapshotFetched(SnapshotFetched::Success(success_info))
    }

    pub fn snapshot_failed(failed_info: snapshot_fetched::FailedInfo) -> Self {
        CompactionEvent::SnapshotFetched(SnapshotFetched::Failed(failed_info))
    }

    pub fn catalog_success(success_info: catalog_fetched::SuccessInfo) -> Self {
        CompactionEvent::CatalogFetched(CatalogFetched::Success(success_info))
    }

    pub fn catalog_failed(failed_info: catalog_fetched::FailedInfo) -> Self {
        CompactionEvent::CatalogFetched(CatalogFetched::Failed(failed_info))
    }

    pub fn compaction_planned_success(success_info: compaction_planned::SuccessInfo) -> Self {
        CompactionEvent::CompactionPlanned(CompactionPlanned::SuccessInfo(success_info))
    }

    pub fn compaction_planned_failed(failed_info: compaction_planned::FailedInfo) -> Self {
        CompactionEvent::CompactionPlanned(CompactionPlanned::FailedInfo(failed_info))
    }
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
            let mut event_duration_arr = UInt64Builder::with_capacity(capacity);
            let mut event_status_arr = StringViewBuilder::with_capacity(capacity);
            let mut event_data_arr = StringViewBuilder::with_capacity(capacity);

            for event in buf.in_order() {
                event_time_arr.append_value(event.format_event_time());
                match &event.data {
                    CompactionEvent::SnapshotFetched(snapshot_ev_info) => {
                        event_type_arr.append_value("SNAPSHOT_FETCHED");
                        match snapshot_ev_info {
                            SnapshotFetched::Success(success_info) => {
                                event_status_arr.append_value("Success");
                                event_duration_arr
                                    .append_value(success_info.fetch_duration.as_millis() as u64);
                                event_data_arr
                                    .append_value(serde_json::to_string(success_info).unwrap());
                            }
                            SnapshotFetched::Failed(failed_info) => {
                                event_status_arr.append_value("Failed");
                                event_duration_arr
                                    .append_value(failed_info.duration.as_millis() as u64);
                                event_data_arr
                                    .append_value(serde_json::to_string(failed_info).unwrap());
                            }
                        }
                    }
                    CompactionEvent::CatalogFetched(catalog_ev_info) => {
                        event_type_arr.append_value("CATALOG_FETCHED");
                        match catalog_ev_info {
                            CatalogFetched::Success(success_info) => {
                                event_status_arr.append_value("Success");
                                event_duration_arr
                                    .append_value(success_info.duration.as_millis() as u64);
                                event_data_arr
                                    .append_value(serde_json::to_string(success_info).unwrap());
                            }
                            CatalogFetched::Failed(failed_info) => {
                                event_status_arr.append_value("Failed");
                                event_duration_arr
                                    .append_value(failed_info.duration.as_millis() as u64);
                                event_data_arr
                                    .append_value(serde_json::to_string(failed_info).unwrap());
                            }
                        }
                    }
                    CompactionEvent::CompactionPlanned(planned_event_info) => {
                        event_type_arr.append_value("COMPACTION_PLANNED");
                        match planned_event_info {
                            CompactionPlanned::SuccessInfo(success_info) => {
                                event_status_arr.append_value("Success");
                                event_duration_arr
                                    .append_value(success_info.duration.as_millis() as u64);
                                event_data_arr.append_value(
                                    serde_json::to_string(planned_event_info).unwrap(),
                                );
                            }
                            CompactionPlanned::FailedInfo(failed_info) => {
                                event_status_arr.append_value("Failed");
                                event_duration_arr
                                    .append_value(failed_info.duration.as_millis() as u64);
                                event_data_arr.append_value(
                                    serde_json::to_string(planned_event_info).unwrap(),
                                );
                            }
                        }
                    }
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
        Field::new("event_duration", DataType::UInt64, false),
        Field::new("event_status", DataType::Utf8View, false),
        Field::new("event_data", DataType::Utf8View, false),
    ]
}
