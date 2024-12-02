use std::fmt::Debug;
use std::{
    any::{Any, TypeId},
    mem::replace,
    sync::Arc,
};

use dashmap::DashMap;
use iox_time::TimeProvider;

const MAX_CAPACITY: usize = 1000;

/// This store captures the events for different types of instrumentation.
/// It is backed by a ring buffer per event type. Every new event type that
/// is added can call [`SysEventStore::add`] directly. And in order to find
/// all the events per event type [`SysEventStore::query`] method can be used.
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

    pub fn add<E>(&self, val: E)
    where
        E: 'static + Debug + Sync + Send,
    {
        let wrapped = Event {
            time: self.time_provider.now().timestamp_nanos(),
            data: val,
        };
        let mut buf = self
            .events
            .entry(TypeId::of::<RingBuffer<Event<E>>>())
            .or_insert_with(|| Box::new(RingBuffer::<Event<E>>::new(MAX_CAPACITY)));

        buf.downcast_mut::<RingBuffer<Event<E>>>()
            .unwrap()
            .push(wrapped);
    }

    pub fn query<E>(&self) -> Vec<Event<E>>
    where
        E: 'static + Clone + Debug + Sync + Send,
    {
        let mut vec = vec![];
        if let Some(buf) = self.events.get(&TypeId::of::<RingBuffer<Event<E>>>()) {
            let iter = buf
                .downcast_ref::<RingBuffer<Event<E>>>()
                .unwrap()
                .in_order();
            for i in iter {
                vec.push(i.clone());
            }
        };
        vec
    }
}

struct RingBuffer<T> {
    buf: Vec<T>,
    max: usize,
    write_index: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            max: capacity,
            write_index: 0,
        }
    }

    pub fn push(&mut self, val: T) {
        if !self.reached_max() {
            self.buf.push(val);
        } else {
            let _ = replace(&mut self.buf[self.write_index], val);
        }
        self.write_index = (self.write_index + 1) % self.max;
    }

    pub fn in_order(&self) -> impl Iterator<Item = &'_ T> {
        let (head, tail) = self.buf.split_at(self.write_index);
        tail.iter().chain(head.iter())
    }

    fn reached_max(&mut self) -> bool {
        self.buf.len() >= self.max
    }
}

/// This is wrapper type adds the time of event
#[allow(dead_code)]
#[derive(Default, Clone, Debug)]
pub struct Event<D> {
    time: i64,
    data: D,
}

impl<D> Event<D> {
    pub fn new(time: i64, data: D) -> Self {
        Self { time, data }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iox_time::{MockProvider, Time};
    use observability_deps::tracing::debug;

    use crate::{RingBuffer, SysEventStore};

    #[allow(dead_code)]
    #[derive(Default, Clone, Debug)]
    struct SampleEvent1 {
        pub start_time: i64,
        pub time_taken: u64,
        pub total_fetched: u64,
        pub random_name: String,
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
        event_store.add(event_data);

        event_store.add(event_data2);
        event_store.add(event_data3);
        assert_eq!(2, event_store.events.len());

        let all_events = event_store.query::<SampleEvent1>();
        assert_eq!(2, all_events.len());
        debug!(all_events = ?all_events, "all events in sys events for type SampleEvent1");

        let all_events = event_store.query::<SampleEvent2>();
        assert_eq!(1, all_events.len());
        debug!(all_events = ?all_events, "all events in sys events for type SampleEvent2");
    }
}
