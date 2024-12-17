use std::{fmt::Display, sync::Arc, time::Duration};

use arrow::{
    array::{StringViewBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
use arrow_array::{ArrayRef, RecordBatch};
use serde::Serialize;

use crate::{
    events::{
        catalog_fetched::CatalogFetched,
        compaction_completed::{PlanCompactionCompleted, PlanGroupCompactionCompleted},
        compaction_consumed::CompactionConsumed,
        compaction_planned::CompactionPlanned,
        snapshot_fetched::SnapshotFetched,
    },
    Event, RingBuffer, ToRecordBatch,
};

pub mod catalog_fetched;
pub mod compaction_completed;
pub mod compaction_consumed;
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
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum CompactionEvent {
    SnapshotFetched(SnapshotFetched),
    CatalogFetched(CatalogFetched),
    CompactionPlanned(CompactionPlanned),
    PlanCompactionCompleted(PlanCompactionCompleted),
    PlanGroupCompactionPlanned(PlanGroupCompactionCompleted),
    CompactionConsumed(CompactionConsumed),
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
        CompactionEvent::CompactionPlanned(CompactionPlanned::Success(success_info))
    }

    pub fn compaction_planned_failed(failed_info: compaction_planned::FailedInfo) -> Self {
        CompactionEvent::CompactionPlanned(CompactionPlanned::Failed(failed_info))
    }

    pub fn compaction_plan_run_completed_success(
        success_info: compaction_completed::PlanRunSuccessInfo,
    ) -> Self {
        CompactionEvent::PlanCompactionCompleted(PlanCompactionCompleted::Success(
            success_info,
        ))
    }

    pub fn compaction_plan_run_completed_failed(
        failed_info: compaction_completed::PlanRunFailedInfo,
    ) -> Self {
        CompactionEvent::PlanCompactionCompleted(PlanCompactionCompleted::Failed(
            failed_info,
        ))
    }

    pub fn compaction_plan_group_run_completed_success(
        success_info: compaction_completed::PlanGroupRunSuccessInfo,
    ) -> Self {
        CompactionEvent::PlanGroupCompactionPlanned(
            PlanGroupCompactionCompleted::Success(success_info),
        )
    }

    pub fn compaction_plan_group_run_completed_failed(
        failed_info: compaction_completed::PlanGroupRunFailedInfo,
    ) -> Self {
        CompactionEvent::PlanGroupCompactionPlanned(
            PlanGroupCompactionCompleted::Failed(failed_info),
        )
    }

    pub fn compaction_consumed_success(success_info: compaction_consumed::SuccessInfo) -> Self {
        CompactionEvent::CompactionConsumed(CompactionConsumed::Success(success_info))
    }

    pub fn compaction_consumed_failed(failed_info: compaction_consumed::FailedInfo) -> Self {
        CompactionEvent::CompactionConsumed(CompactionConsumed::Failed(failed_info))
    }
}

impl EventData for CompactionEvent {
    fn name(&self) -> &'static str {
        match self {
            CompactionEvent::SnapshotFetched(info) => info.name(),
            CompactionEvent::CatalogFetched(info) => info.name(),
            CompactionEvent::CompactionPlanned(info) => info.name(),
            CompactionEvent::PlanCompactionCompleted(info) => info.name(),
            CompactionEvent::PlanGroupCompactionPlanned(info) => info.name(),
            CompactionEvent::CompactionConsumed(info) => info.name(),
        }
    }

    fn outcome(&self) -> EventOutcome {
        match self {
            CompactionEvent::SnapshotFetched(info) => info.outcome(),
            CompactionEvent::CatalogFetched(info) => info.outcome(),
            CompactionEvent::CompactionPlanned(info) => info.outcome(),
            CompactionEvent::PlanCompactionCompleted(info) => info.outcome(),
            CompactionEvent::PlanGroupCompactionPlanned(info) => info.outcome(),
            CompactionEvent::CompactionConsumed(info) => info.outcome(),
        }
    }

    fn duration(&self) -> Duration {
        match self {
            CompactionEvent::SnapshotFetched(info) => info.duration(),
            CompactionEvent::CatalogFetched(info) => info.duration(),
            CompactionEvent::CompactionPlanned(info) => info.duration(),
            CompactionEvent::PlanCompactionCompleted(info) => info.duration(),
            CompactionEvent::PlanGroupCompactionPlanned(info) => info.duration(),
            CompactionEvent::CompactionConsumed(info) => info.duration(),
        }
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
            let mut event_time_arr = StringViewBuilder::with_capacity(buf.len());
            let mut event_type_arr = StringViewBuilder::with_capacity(buf.len());
            let mut event_duration_arr = UInt64Builder::with_capacity(buf.len());
            let mut event_status_arr = StringViewBuilder::with_capacity(buf.len());
            let mut event_data_arr = StringViewBuilder::with_capacity(buf.len());

            for event in buf.in_order() {
                event_time_arr.append_value(event.format_event_time());
                build_event_data(
                    &event.data,
                    &mut event_type_arr,
                    &mut event_status_arr,
                    &mut event_duration_arr,
                    &mut event_data_arr,
                );
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

pub(crate) enum EventOutcome {
    Success,
    Failed,
}

impl Display for EventOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventOutcome::Success => write!(f, "Success"),
            EventOutcome::Failed => write!(f, "Failed"),
        }
    }
}

pub(crate) trait EventData {
    fn name(&self) -> &'static str;
    fn outcome(&self) -> EventOutcome;
    fn duration(&self) -> Duration;
    fn data(&self) -> String
    where
        Self: Serialize,
    {
        serde_json::to_string(&self).unwrap()
    }
}

impl<T: EventData> EventData for &T {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn outcome(&self) -> EventOutcome {
        (**self).outcome()
    }

    fn duration(&self) -> Duration {
        (**self).duration()
    }
}

fn build_event_data<T>(
    info: T,
    event_type_arr: &mut StringViewBuilder,
    event_status_arr: &mut StringViewBuilder,
    event_duration_arr: &mut UInt64Builder,
    event_data_arr: &mut StringViewBuilder,
) where
    T: EventData + Serialize,
{
    event_type_arr.append_value(info.name());
    event_status_arr.append_value(info.outcome().to_string());
    event_duration_arr.append_value(info.duration().as_millis() as u64);
    event_data_arr.append_value(info.data());
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
