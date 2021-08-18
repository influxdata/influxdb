use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use hashbrown::HashMap;

use crate::KeyValue;
use hashbrown::hash_map::RawEntryMut;
use opentelemetry::labels::{DefaultLabelEncoder, LabelSet};

/// A `Gauge` allows tracking multiple usize values by label set
///
/// Metrics can be recorded directly on the Gauge or when the labels are
/// known ahead of time a `GaugeValue` can be obtained with `Gauge::gauge_value`
///
/// When a `Gauge` is dropped any contributions it made to any label sets
/// will be deducted
#[derive(Debug)]
pub struct Gauge {
    /// `GaugeState` stores the underlying state for this `Gauge`
    state: Arc<GaugeState>,
    /// Any contributions made by this instance of `Gauge` to the totals
    /// stored in `GaugeState`
    values: HashMap<String, GaugeValue>,
    default_labels: Vec<KeyValue>,
}

impl Gauge {
    /// Creates a new Gauge that isn't registered with and consequently
    /// won't report to any metrics registry
    ///
    /// Observations made to this Gauge, and any GaugeValues it creates,
    /// will still be computed correctly and visible but will not be
    /// reported to a central metric registry, and will not be visible
    /// to any other Gauge instance
    pub fn new_unregistered() -> Self {
        Self {
            state: Arc::new(Default::default()),
            values: Default::default(),
            default_labels: vec![],
        }
    }

    pub(crate) fn new(state: Arc<GaugeState>, default_labels: Vec<KeyValue>) -> Self {
        Self {
            values: Default::default(),
            state,
            default_labels,
        }
    }

    /// Gets a `GaugeValue` for a given set of labels
    /// This allows fast value updates and retrieval when the labels are known in advance
    pub fn gauge_value(&self, labels: &[KeyValue]) -> GaugeValue {
        let (encoded, keys) = self.encode_labels(labels);
        self.state.gauge_value_encoded(encoded, keys)
    }

    pub fn inc(&mut self, delta: usize, labels: &[KeyValue]) {
        self.call(labels, |observer| observer.inc(delta))
    }

    pub fn decr(&mut self, delta: usize, labels: &[KeyValue]) {
        self.call(labels, |observer| observer.decr(delta))
    }

    pub fn set(&mut self, value: usize, labels: &[KeyValue]) {
        self.call(labels, |observer| observer.set(value))
    }

    fn encode_labels(&self, labels: &[KeyValue]) -> (String, LabelSet) {
        GaugeState::encode_labels(self.default_labels.iter().chain(labels).cloned())
    }

    fn call(&mut self, labels: &[KeyValue], f: impl Fn(&mut GaugeValue)) {
        let (encoded, keys) = self.encode_labels(labels);

        match self.values.raw_entry_mut().from_key(&encoded) {
            RawEntryMut::Occupied(mut occupied) => f(occupied.get_mut()),
            RawEntryMut::Vacant(vacant) => {
                let (_, observer) = vacant.insert(
                    encoded.clone(),
                    self.state.gauge_value_encoded(encoded, keys),
                );
                f(observer)
            }
        }
    }
}

/// The shared state for a `Gauge`
#[derive(Debug, Default)]
pub(crate) struct GaugeState {
    /// Keep tracks of every label set observed by this gauge
    ///
    /// The key is a sorted encoding of this label set, that reconciles
    /// duplicates in the same manner as OpenTelemetry (it uses the same encoder)
    /// which appears to be last-writer-wins
    values: DashMap<String, (GaugeValue, Vec<KeyValue>)>,
}

impl GaugeState {
    /// Visits the totals for all recorded label sets
    pub fn visit_values(&self, f: impl Fn(usize, &[KeyValue])) {
        for data in self.values.iter() {
            let (data, labels) = data.value();
            f(data.get_total(), labels)
        }
    }

    fn encode_labels(labels: impl IntoIterator<Item = KeyValue>) -> (String, LabelSet) {
        let set = LabelSet::from_labels(labels);
        let encoded = set.encoded(Some(&DefaultLabelEncoder));

        (encoded, set)
    }

    fn gauge_value_encoded(&self, encoded: String, keys: LabelSet) -> GaugeValue {
        self.values
            .entry(encoded)
            .or_insert_with(|| {
                (
                    // It is created unregistered, adding it to values "registers" it
                    GaugeValue::new_unregistered(),
                    keys.iter()
                        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
                        .collect(),
                )
            })
            .value()
            .0
            .clone_empty()
    }
}

#[derive(Debug, Default)]
struct GaugeValueShared {
    /// The total contribution from all associated `GaugeValue`s
    total: AtomicUsize,
}

/// A `GaugeValue` is a single measurement associated with a `Gauge`
///
/// When the `GaugeValue` is dropped any contribution it made to the total
/// will be deducted from the `GaugeValue` total
///
/// Each `GaugeValue` stores a reference to `GaugeValueShared`. The construction
/// of `Gauge` ensures that all registered `GaugeValue`s for the same label set
/// refer to the same `GaugeValueShared` that isn't shared with any others
///
/// Each `GaugeValue` also stores its local observation, when updated it also
/// updates the total in `GaugeValueShared` by the same amount. When dropped it
/// removes any contribution it made to the total in `GaugeValueShared`
///
#[derive(Debug)]
pub struct GaugeValue {
    /// A reference to the shared pool
    shared: Arc<GaugeValueShared>,
    /// The locally observed value
    local: usize,
}

impl GaugeValue {
    /// Creates a new GaugeValue that isn't associated with any Gauge
    ///
    /// Observations made to this GaugeValue, and any GaugeValue it creates,
    /// will still be computed correctly and visible but will not be reported
    /// to a Gauge, nor a metric registry, or any independently created GaugeValue
    pub fn new_unregistered() -> Self {
        Self {
            shared: Arc::new(Default::default()),
            local: 0,
        }
    }

    /// Creates a new GaugeValue with no local observation that refers to the same
    /// underlying backing store as this GaugeValue
    pub fn clone_empty(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            local: 0,
        }
    }

    /// Gets the total value from across all `GaugeValue` associated
    /// with this `Gauge`
    pub fn get_total(&self) -> usize {
        self.shared.total.load(Ordering::Relaxed)
    }

    /// Gets the local contribution from this instance
    pub fn get_local(&self) -> usize {
        self.local
    }

    /// Increment the local value for this GaugeValue
    pub fn inc(&mut self, delta: usize) {
        self.local += delta;
        self.shared.total.fetch_add(delta, Ordering::Relaxed);
    }

    /// Decrement the local value for this GaugeValue
    pub fn decr(&mut self, delta: usize) {
        self.local -= delta;
        self.shared.total.fetch_sub(delta, Ordering::Relaxed);
    }

    /// Sets the local value for this GaugeValue
    pub fn set(&mut self, new: usize) {
        match new.cmp(&self.local) {
            std::cmp::Ordering::Less => self.decr(self.local - new),
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => self.inc(new - self.local),
        }
    }
}

impl Drop for GaugeValue {
    fn drop(&mut self) {
        self.shared.total.fetch_sub(self.local, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker() {
        let start = GaugeValue::new_unregistered();
        let mut t1 = start.clone_empty();
        let mut t2 = start.clone_empty();

        t1.set(200);

        assert_eq!(t1.get_total(), 200);
        assert_eq!(t2.get_total(), 200);
        assert_eq!(start.get_total(), 200);

        t1.set(100);

        assert_eq!(t1.get_total(), 100);
        assert_eq!(t2.get_total(), 100);
        assert_eq!(start.get_total(), 100);

        t2.set(300);
        assert_eq!(t1.get_total(), 400);
        assert_eq!(t2.get_total(), 400);
        assert_eq!(start.get_total(), 400);

        t2.set(400);
        assert_eq!(t1.get_total(), 500);
        assert_eq!(t2.get_total(), 500);
        assert_eq!(start.get_total(), 500);

        std::mem::drop(t2);
        assert_eq!(t1.get_total(), 100);
        assert_eq!(start.get_total(), 100);

        std::mem::drop(t1);
        assert_eq!(start.get_total(), 0);
    }

    #[test]
    fn test_mixed() {
        let gauge_state = Arc::new(GaugeState::default());
        let mut gauge = Gauge::new(Arc::clone(&gauge_state), vec![KeyValue::new("foo", "bar")]);
        let mut gauge2 = Gauge::new(gauge_state, vec![KeyValue::new("foo", "bar")]);

        gauge.set(32, &[KeyValue::new("bingo", "bongo")]);
        gauge2.set(64, &[KeyValue::new("bingo", "bongo")]);

        let mut gauge_value = gauge.gauge_value(&[KeyValue::new("bingo", "bongo")]);
        gauge_value.set(12);

        assert_eq!(gauge_value.get_local(), 12);
        assert_eq!(gauge_value.get_total(), 32 + 64 + 12);

        std::mem::drop(gauge2);
        assert_eq!(gauge_value.get_total(), 32 + 12);

        gauge.inc(5, &[KeyValue::new("no", "match")]);
        assert_eq!(gauge_value.get_total(), 32 + 12);

        gauge.inc(5, &[KeyValue::new("bingo", "bongo")]);
        assert_eq!(gauge_value.get_total(), 32 + 12 + 5);

        std::mem::drop(gauge);

        assert_eq!(gauge_value.get_total(), 12);
        assert_eq!(gauge_value.get_local(), 12);
    }
}
