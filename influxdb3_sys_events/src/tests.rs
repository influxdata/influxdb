use std::sync::Arc;

use arrow::{
    array::{StringViewBuilder, StructBuilder, UInt64Builder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
};
use arrow_array::{ArrayRef, RecordBatch};
use iox_time::{MockProvider, Time};
use observability_deps::tracing::debug;

use crate::{Event, MAX_CAPACITY, RingBuffer, SysEventStore, ToRecordBatch};

#[allow(dead_code)]
#[derive(Default, Clone, Debug)]
struct SampleEvent1 {
    pub start_time: i64,
    pub time_taken: u64,
    pub total_fetched: u64,
    pub random_name: String,
}

impl ToRecordBatch<SampleEvent1> for SampleEvent1 {
    fn to_record_batch(
        items: Option<&RingBuffer<Event<SampleEvent1>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        items.map(|buf| {
            let iter = buf.in_order();
            let mut event_time_arr = StringViewBuilder::with_capacity(MAX_CAPACITY);
            let mut struct_builder = StructBuilder::from_fields(
                vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ],
                MAX_CAPACITY,
            );
            for event in iter {
                event_time_arr.append_value("2024-12-01T23:59:59.000Z");
                let time_taken_builder = struct_builder.field_builder::<UInt64Builder>(0).unwrap();
                time_taken_builder.append_value(event.data.time_taken);

                let num_files_fetched_builder =
                    struct_builder.field_builder::<UInt64Builder>(1).unwrap();
                num_files_fetched_builder.append_value(event.data.total_fetched);

                struct_builder.append(true);
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(event_time_arr.finish()),
                Arc::new(struct_builder.finish()),
            ];
            RecordBatch::try_new(Arc::new(Self::schema()), columns)
        })
    }

    fn schema() -> Schema {
        let columns = vec![
            Field::new("event_time", DataType::Utf8View, false),
            Field::new(
                "event_data",
                DataType::Struct(Fields::from(vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ])),
                false,
            ),
        ];
        Schema::new(columns)
    }
}

#[allow(dead_code)]
#[derive(Default, Clone, Debug)]
struct SampleEvent2 {
    pub start_time: i64,
    pub time_taken: u64,
    pub generation_id: u64,
}

#[test]
fn test_ring_buffer_not_full_at_less_than_max() {
    let mut buf = RingBuffer::new(2);
    buf.push(1);

    let all_results: Vec<&u64> = buf.in_order().collect();
    let first = *all_results.first().unwrap();

    assert_eq!(1, all_results.len());
    assert_eq!(&1, first);
}

#[test]
fn test_ring_buffer_not_full_at_max() {
    let mut buf = RingBuffer::new(2);
    buf.push(1);
    buf.push(2);

    let all_results: Vec<&u64> = buf.in_order().collect();
    let first = *all_results.first().unwrap();
    let second = *all_results.get(1).unwrap();

    assert_eq!(2, all_results.len());
    assert_eq!(&1, first);
    assert_eq!(&2, second);
}

#[test]
fn test_ring_buffer() {
    let mut buf = RingBuffer::new(2);
    buf.push(1);
    buf.push(2);
    buf.push(3);

    let all_results: Vec<&u64> = buf.in_order().collect();
    let first = *all_results.first().unwrap();
    let second = *all_results.get(1).unwrap();

    assert_eq!(2, all_results.len());
    assert_eq!(&2, first);
    assert_eq!(&3, second);
}

#[test_log::test(test)]
fn test_event_store() {
    let event_data = SampleEvent1 {
        start_time: 0,
        time_taken: 10,
        total_fetched: 10,
        random_name: "foo".to_owned(),
    };

    let event_data2 = SampleEvent2 {
        start_time: 0,
        time_taken: 10,
        generation_id: 100,
    };

    let event_data3 = SampleEvent1 {
        start_time: 0,
        time_taken: 10,
        total_fetched: 10,
        random_name: "boo".to_owned(),
    };

    let time_provider = MockProvider::new(Time::from_timestamp_nanos(100));

    let event_store = SysEventStore::new(Arc::new(time_provider));
    event_store.record(event_data);

    event_store.record(event_data2);
    event_store.record(event_data3);
    assert_eq!(2, event_store.events.len());

    let all_events = event_store.as_vec::<SampleEvent1>();
    assert_eq!(2, all_events.len());
    debug!(all_events = ?all_events, "all events in sys events for type SampleEvent1");

    let all_events = event_store.as_vec::<SampleEvent2>();
    assert_eq!(1, all_events.len());
    debug!(all_events = ?all_events, "all events in sys events for type SampleEvent2");
}

#[test_log::test(test)]
fn test_event_store_2() {
    let event_data = SampleEvent1 {
        start_time: 0,
        time_taken: 10,
        total_fetched: 10,
        random_name: "foo".to_owned(),
    };

    let event_data2 = SampleEvent2 {
        start_time: 0,
        time_taken: 10,
        generation_id: 100,
    };

    let event_data3 = SampleEvent1 {
        start_time: 0,
        time_taken: 10,
        total_fetched: 10,
        random_name: "boo".to_owned(),
    };

    let time_provider = MockProvider::new(Time::from_timestamp_nanos(100));

    let event_store = SysEventStore::new(Arc::new(time_provider));
    event_store.record(event_data);

    event_store.record(event_data2);
    event_store.record(event_data3);
    assert_eq!(2, event_store.events.len());

    let all_events = event_store.as_record_batch::<SampleEvent1>();
    assert_eq!(
        2,
        all_events
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .columns()
            .len()
    );
    debug!(all_events = ?all_events, "all SampleEvent1 events as record batch");
}
