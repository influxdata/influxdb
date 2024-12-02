use std::sync::Arc;

use arrow::{
    array::{Int64Builder, StringBuilder, StringViewBuilder, StructBuilder, UInt64Builder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
};
use arrow_array::{ArrayRef, RecordBatch};

use crate::{Event, RingBuffer, ToRecordBatch};

#[derive(Debug, Clone)]
pub struct SuccessInfo {
    pub host: Arc<str>,
    pub sequence_number: u64,
    pub fetch_duration_ms: i64,
    pub db_count: u64,
    pub table_count: u64,
    pub file_count: u64,
}

impl SuccessInfo {
    pub fn new(
        host: &str,
        sequence_number: u64,
        duration: i64,
        db_table_file_counts: (u64, u64, u64),
    ) -> Self {
        Self {
            host: Arc::from(host),
            sequence_number,
            fetch_duration_ms: duration,
            db_count: db_table_file_counts.0,
            table_count: db_table_file_counts.1,
            file_count: db_table_file_counts.2,
        }
    }

    fn append_value(&self, struct_builder: &mut StructBuilder) {
        let host_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
        host_builder.append_value(&self.host);

        let sequence_number_builder = struct_builder.field_builder::<UInt64Builder>(1).unwrap();
        sequence_number_builder.append_value(self.sequence_number);

        let time_taken_builder = struct_builder.field_builder::<Int64Builder>(2).unwrap();
        time_taken_builder.append_value(self.fetch_duration_ms);
        let db_count_builder = struct_builder.field_builder::<UInt64Builder>(3).unwrap();
        db_count_builder.append_value(self.db_count);

        let table_count_builder = struct_builder.field_builder::<UInt64Builder>(4).unwrap();
        table_count_builder.append_value(self.table_count);

        let file_count_builder = struct_builder.field_builder::<UInt64Builder>(5).unwrap();
        file_count_builder.append_value(self.file_count);

        let error_builder = struct_builder.field_builder::<StringBuilder>(6).unwrap();
        error_builder.append_null();
    }
}

#[derive(Debug, Clone)]
pub struct FailedInfo {
    pub host: Arc<str>,
    pub sequence_number: u64,
    pub error: String,
}

impl FailedInfo {
    fn append_value(&self, struct_builder: &mut StructBuilder) {
        let host_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
        host_builder.append_value(&self.host);

        let sequence_number_builder = struct_builder.field_builder::<UInt64Builder>(1).unwrap();
        sequence_number_builder.append_value(self.sequence_number);

        let time_taken_builder = struct_builder.field_builder::<Int64Builder>(2).unwrap();
        time_taken_builder.append_null();

        let db_count_builder = struct_builder.field_builder::<UInt64Builder>(3).unwrap();
        db_count_builder.append_null();

        let table_count_builder = struct_builder.field_builder::<UInt64Builder>(4).unwrap();
        table_count_builder.append_null();

        let file_count_builder = struct_builder.field_builder::<UInt64Builder>(5).unwrap();
        file_count_builder.append_null();

        let error_builder = struct_builder.field_builder::<StringBuilder>(6).unwrap();
        error_builder.append_value(&self.error);
    }
}

#[derive(Debug, Clone)]
pub enum SnapshotFetchedEvent {
    Success(SuccessInfo),
    Failed(FailedInfo),
}

impl ToRecordBatch<SnapshotFetchedEvent> for SnapshotFetchedEvent {
    fn schema() -> arrow::datatypes::Schema {
        let columns = vec![
            Field::new("event_time", DataType::Utf8View, false),
            Field::new("event_type", DataType::Utf8View, false),
            Field::new(
                "event_data",
                // Currently there is no way to set the schema to empty and
                // swap it for actual schema later. So this implies using
                // `system` tables (system.xxxx) ends up always requiring
                // a static schema. It would be nice to only include
                // `SuccessInfo` fields in success event type and `FailedInfo`
                // fields for failed event type. For now, success events have
                // error field set to null and it's the other way around for
                // failed event type.
                //
                // This is maybe possible using SchemaProvider to dynamically
                // swap the TableProvider for a table name, but that would
                // mean moving away from `IoxSystemTable` trait.
                DataType::Struct(Fields::from(struct_fields())),
                false,
            ),
        ];
        Schema::new(columns)
    }

    fn to_record_batch(
        buffer: Option<&RingBuffer<Event<SnapshotFetchedEvent>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        buffer.map(|buf| {
            let mut event_time_arr = StringViewBuilder::with_capacity(crate::MAX_CAPACITY);
            let mut event_type_arr = StringViewBuilder::with_capacity(crate::MAX_CAPACITY);
            let mut struct_builder =
                StructBuilder::from_fields(struct_fields(), crate::MAX_CAPACITY);

            for event in buf.in_order() {
                event_time_arr.append_value(event.format_event_time());
                match &event.data {
                    SnapshotFetchedEvent::Success(success_info) => {
                        event_type_arr.append_value("Success");
                        success_info.append_value(&mut struct_builder);
                    }
                    SnapshotFetchedEvent::Failed(failed_info) => {
                        event_type_arr.append_value("Failed");
                        failed_info.append_value(&mut struct_builder);
                    }
                }
                struct_builder.append(true);
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(event_time_arr.finish()),
                Arc::new(event_type_arr.finish()),
                Arc::new(struct_builder.finish()),
            ];
            RecordBatch::try_new(Arc::new(Self::schema()), columns)
        })
    }
}

fn struct_fields() -> Vec<Field> {
    vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("sequence_number", DataType::UInt64, false),
        Field::new("fetch_duration_ms", DataType::Int64, true),
        Field::new("db_count", DataType::UInt64, true),
        Field::new("table_count", DataType::UInt64, true),
        Field::new("file_count", DataType::UInt64, true),
        Field::new("error", DataType::Utf8, true),
    ]
}
