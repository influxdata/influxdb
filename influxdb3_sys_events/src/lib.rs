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
use observability_deps::tracing::trace;

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
        trace!(sys_event = ?wrapped, "sys event added to store");
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
mod tests;
