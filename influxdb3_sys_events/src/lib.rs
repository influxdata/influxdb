use std::fmt::Debug;
use std::{
    any::{Any, TypeId},
    mem::replace,
    sync::Arc,
};

use arrow::{datatypes::Schema, error::ArrowError};
use arrow_array::RecordBatch;
use dashmap::DashMap;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::info;

const MAX_CAPACITY: usize = 10_000;

/// This trait is not dyn compatible
pub trait ToRecordBatch<E> {
    /// set the schema for the event
    fn schema() -> Schema;
    /// takes reference to `RingBuffer` and creates `RecordBatch` for the events
    /// in the buffer.
    fn to_record_batch(
        items: Option<&RingBuffer<Event<E>>>,
    ) -> Option<Result<RecordBatch, ArrowError>>;
}

/// This store captures the events for different types of instrumentation.
/// It is backed by a ring buffer per event type. Every new event type that
/// can be added by calling [`SysEventStore::record`]. And in order to find
/// all the events per event type [`SysEventStore::as_vec`] method can
/// be used. This returns a `Vec<Event<E>>` which internally clones to get
/// values out of the `Ref` guard. There is a convenient method,
/// [`SysEventStore::as_record_batch`] in order to get a record batch directly
/// avoiding clones.
///
/// Every time a new event is introduced, the system table had to be setup
/// following the same pattern as in `influxdb3_server::system_tables`
#[derive(Debug)]
pub struct SysEventStore {
    events: dashmap::DashMap<TypeId, Box<dyn Any + Send + Sync>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl SysEventStore {
    pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            events: DashMap::new(),
            time_provider,
        }
    }

    pub fn time_provider(&self) -> &Arc<dyn TimeProvider> {
        &self.time_provider
    }

    /// records an event by adding it to this event store
    pub fn record<E>(&self, val: E)
    where
        E: 'static + Debug + Sync + Send,
    {
        let wrapped = Event {
            time: self.time_provider.now().timestamp_nanos(),
            data: val,
        };
        info!(sys_event = ?wrapped, "compaction sys event added to store");
        let mut buf = self
            .events
            .entry(TypeId::of::<RingBuffer<Event<E>>>())
            .or_insert_with(|| Box::new(RingBuffer::<Event<E>>::new(MAX_CAPACITY)));

        // unwrap here is fine, we just used the same type above for
        // get or insert
        buf.downcast_mut::<RingBuffer<Event<E>>>()
            .unwrap()
            .push(wrapped);
    }

    /// Creates an intermediate `Vec` by cloning events. To
    /// create a record batch instead use [`Self::as_record_batch`]
    pub fn as_vec<E>(&self) -> Vec<Event<E>>
    where
        E: 'static + Clone + Debug + Sync + Send,
    {
        self.events
            .get(&TypeId::of::<RingBuffer<Event<E>>>())
            .map(|buf| {
                // unwrap here is fine, we just used the same type above to
                // get
                buf.downcast_ref::<RingBuffer<Event<E>>>()
                    .unwrap()
                    .in_order()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Creates record batch for given event type `E`, this avoids
    /// any unnecessary allocation but events need to implement
    /// [`ToRecordBatch`] trait
    pub fn as_record_batch<E>(&self) -> Option<Result<RecordBatch, ArrowError>>
    where
        E: 'static + Debug + Sync + Send + ToRecordBatch<E>,
    {
        let map_ref = self.events.get(&TypeId::of::<RingBuffer<Event<E>>>());
        let buf_ref = map_ref
            .as_ref()
            // unwrap here is fine, we just used the same type above to get
            .map(|buf| buf.downcast_ref::<RingBuffer<Event<E>>>().unwrap());
        E::to_record_batch(buf_ref)
    }
}

// we've increased the max capacity to 10k by default, it makes
// sense to use heap.
pub type RingBuffer<T> = RingBufferVec<T>;

#[derive(Debug)]
pub struct RingBufferVec<T> {
    buf: Vec<T>,
    max: usize,
    write_index: usize,
}

impl<T> RingBufferVec<T> {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            max: capacity,
            write_index: 0,
        }
    }

    pub fn in_order(&self) -> impl Iterator<Item = &T> {
        let (head, tail) = self.buf.split_at(self.write_index);
        tail.iter().chain(head.iter())
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn push(&mut self, val: T) {
        if !self.is_at_max() {
            self.buf.push(val);
        } else {
            let _ = replace(&mut self.buf[self.write_index], val);
        }
        self.write_index = (self.write_index + 1) % self.max;
    }

    fn is_at_max(&mut self) -> bool {
        self.buf.len() >= self.max
    }
}

/// This is wrapper type adds the time of event
#[allow(dead_code)]
#[derive(Default, Clone, Debug)]
pub struct Event<D> {
    time: i64,
    pub data: D,
}

impl<D> Event<D> {
    pub fn new(time: i64, data: D) -> Self {
        Self { time, data }
    }

    pub fn format_event_time(&self) -> String {
        Time::from_timestamp_nanos(self.time)
            .date_time()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{StringViewBuilder, StructBuilder, UInt64Builder},
        datatypes::{DataType, Field, Fields, Schema},
        error::ArrowError,
    };
    use arrow_array::{ArrayRef, RecordBatch};
    use iox_time::{MockProvider, Time};
    use observability_deps::tracing::debug;

    use crate::{Event, RingBuffer, SysEventStore, ToRecordBatch, MAX_CAPACITY};

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
                    let time_taken_builder =
                        struct_builder.field_builder::<UInt64Builder>(0).unwrap();
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
}
