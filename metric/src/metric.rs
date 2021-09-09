use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};

use super::{Attributes, Instrument, MetricKind, Observation, Reporter};

/// A `Metric` collects `Observation` for each unique set of `Attributes`
///
/// It is templated by `T: MetricObserver` which determines the type of
/// `Observation` made by this `Metric` along with its semantics
#[derive(Debug)]
pub struct Metric<T: MetricObserver> {
    name: &'static str,
    description: &'static str,
    shared: Arc<MetricShared<T>>,
}

#[derive(Debug)]
struct MetricShared<T: MetricObserver> {
    options: T::Options,
    values: Mutex<BTreeMap<Attributes, T>>,
}

/// Manually implement Clone to avoid constraint T: Clone
impl<T: MetricObserver> Clone for Metric<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            description: self.description,
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: MetricObserver> Metric<T> {
    pub(crate) fn new(name: &'static str, description: &'static str, options: T::Options) -> Self {
        Self {
            name,
            description,
            shared: Arc::new(MetricShared {
                options,
                values: Default::default(),
            }),
        }
    }

    /// Retrieves a type that can be used to report observations for a given set of attributes
    ///
    /// If this is the first time this method has been called with this set of attributes,
    /// it will initialize the corresponding `MetricObserver` with the default observation
    ///
    /// ```
    /// use ::metric::{U64Gauge, Registry, Metric};
    ///
    /// let registry = Registry::new();
    /// let metric: Metric<U64Gauge> = registry.register_metric("metric_name", "description");
    ///
    /// metric.recorder(&[("foo", "bar")]).set(34);
    ///
    /// let recorder = metric.recorder(&[("foo", "biz")]);
    /// recorder.set(34);
    ///
    /// ```
    pub fn recorder(&self, attributes: impl Into<Attributes>) -> T::Recorder {
        self.shared
            .values
            .lock()
            .entry(attributes.into())
            .or_insert_with(|| T::create(&self.shared.options))
            .recorder()
    }

    /// Gets the observer for a given set of attributes if one has
    /// been registered by a call to `Metric::recorder`
    ///
    /// This is primarily useful for testing
    pub fn get_observer(&self, attributes: &Attributes) -> Option<MappedMutexGuard<'_, T>> {
        MutexGuard::try_map(self.shared.values.lock(), |values| {
            values.get_mut(attributes)
        })
        .ok()
    }
}

impl<T: MetricObserver> Instrument for Metric<T> {
    fn report(&self, reporter: &mut dyn Reporter) {
        reporter.start_metric(self.name, self.description, T::kind());

        let values = self.shared.values.lock();
        for (attributes, metric_value) in values.iter() {
            reporter.report_observation(attributes, metric_value.observe())
        }

        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Types that wish to be used with `Metric` must implement this trait
/// that exposes the necessary reporting API
///
/// `Metric` maintains a distinct `MetricObserver` for each unique set of `Attributes`
pub trait MetricObserver: MakeMetricObserver + std::fmt::Debug + Send + 'static {
    /// The type that is used to modify the value reported by this MetricObserver
    ///
    /// Most commonly this will be `Self` but see `CumulativeGauge` for an example
    /// of where it is not
    type Recorder;

    /// The `MetricKind` reported by this `MetricObserver`
    fn kind() -> MetricKind;

    /// Return a `Self::Recorder` that can be used to mutate the value reported
    /// by this `MetricObserver`
    fn recorder(&self) -> Self::Recorder;

    /// Return the current value for this
    fn observe(&self) -> Observation;
}

/// All `MetricObserver` must also implement `MakeMetricObserver` which defines
/// how to construct new instances of `Self`
///
/// A blanket impl is provided for types that implement Default
///
/// See `U64Histogram` for an example of how this is used
pub trait MakeMetricObserver {
    type Options: Sized + Send + Sync + std::fmt::Debug;

    fn create(options: &Self::Options) -> Self;
}

impl<T: Default> MakeMetricObserver for T {
    type Options = ();

    fn create(_: &Self::Options) -> Self {
        Default::default()
    }
}

/// In most cases the recorder for a `MetricObserver` is stateless, in fact in many cases
/// `MetricObserver::Recorder = Self`. This means applications wishing to record observations
/// for many different sets of attributes can just use `Metric<T>` and construct reporters
/// dynamically
///
/// ```
/// use metric::{Registry, Metric, U64Gauge, Attributes};
///
/// let registry = Registry::new();
/// let metric: Metric<U64Gauge> = registry.register_metric("foo", "description");
///
/// metric.recorder(&[("foo", "bar")]).set(21);
/// metric.recorder(&[("fiz", "bar")]).set(34);
///
/// assert_eq!(metric.get_observer(&Attributes::from(&[("foo", "bar")])).unwrap().fetch(), 21);
/// assert_eq!(metric.get_observer(&Attributes::from(&[("fiz", "bar")])).unwrap().fetch(), 34);
/// ```
///
/// However, some `MetricObserver` are stateful, for example, `CumulativeGauge`. In this case
/// dropping the recorder clears any contribution it made to the metric's total
///
/// ```
/// use metric::{Registry, Metric, CumulativeGauge, Attributes};
///
/// let registry = Registry::new();
/// let metric: Metric<CumulativeGauge> = registry.register_metric("foo", "description");
///
/// metric.recorder(&[("foo", "bar")]).set(21);
/// metric.recorder(&[("fiz", "bar")]).set(34);
///
/// // Recorders dropped immediately and so they don't record anything!
/// assert_eq!(metric.get_observer(&Attributes::from(&[("foo", "bar")])).unwrap().fetch(), 0);
/// assert_eq!(metric.get_observer(&Attributes::from(&[("fiz", "bar")])).unwrap().fetch(), 0);
/// ```
///
/// `RecorderCollection` exists to address this situation, as unlike `Metric` it retains the
/// `MetricObserver::Recorder` and ensures they live as long as the `RecorderCollection`
///
/// ```
/// use metric::{Registry, Metric, CumulativeGauge, RecorderCollection, Attributes};
///
/// let registry = Registry::new();
/// let metric: Metric<CumulativeGauge> = registry.register_metric("foo", "description");
///
/// let mut r1 = RecorderCollection::new(metric.clone());
/// let mut r2 = RecorderCollection::new(metric.clone());
///
/// r1.recorder(&[("foo", "bar")]).set(21);
/// r1.recorder(&[("fiz", "bar")]).set(34);
/// r2.recorder(&[("foo", "bar")]).set(12);
///
/// assert_eq!(metric.get_observer(&Attributes::from(&[("foo", "bar")])).unwrap().fetch(), 21 + 12);
/// assert_eq!(metric.get_observer(&Attributes::from(&[("fiz", "bar")])).unwrap().fetch(), 34);
///
/// std::mem::drop(r1);
///
/// assert_eq!(metric.get_observer(&Attributes::from(&[("foo", "bar")])).unwrap().fetch(), 12);
/// assert_eq!(metric.get_observer(&Attributes::from(&[("fiz", "bar")])).unwrap().fetch(), 0);
///
/// std::mem::drop(r2);
///
/// assert_eq!(metric.get_observer(&Attributes::from(&[("foo", "bar")])).unwrap().fetch(), 0);
/// assert_eq!(metric.get_observer(&Attributes::from(&[("fiz", "bar")])).unwrap().fetch(), 0);
/// ```
///
#[derive(Debug)]
pub struct RecorderCollection<T: MetricObserver> {
    metric: Metric<T>,
    recorders: BTreeMap<Attributes, T::Recorder>,
}

impl<T: MetricObserver> RecorderCollection<T> {
    /// Create a new `RecorderCollection` from the provided `Metric`
    pub fn new(metric: Metric<T>) -> Self {
        Self {
            metric,
            recorders: Default::default(),
        }
    }

    /// Retrieves a type that can be used to report observations for a given set of attributes
    ///
    /// The value returned is cached on this `RecorderCollection` and lives as long as it does
    pub fn recorder(&mut self, attributes: impl Into<Attributes>) -> &mut T::Recorder {
        let metric = &self.metric;
        self.recorders
            .entry(attributes.into())
            .or_insert_with_key(|key| metric.recorder(key.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::U64Counter;

    #[test]
    fn test_metric() {
        let metric: Metric<U64Counter> = Metric::new("foo", "description", ());

        let r1 = metric.recorder(&[("tag1", "val1"), ("tag2", "val2")]);
        let r2 = metric.recorder(&[("tag1", "val1")]);
        let r3 = metric.recorder(&[("tag1", "val2")]);
        let r4 = metric.recorder(&[("tag1", "val1"), ("tag2", "val2")]);

        assert_eq!(r1.fetch(), 0);
        assert_eq!(r2.fetch(), 0);
        assert_eq!(r3.fetch(), 0);
        assert_eq!(r4.fetch(), 0);

        r2.inc(32);

        assert_eq!(r1.fetch(), 0);
        assert_eq!(r2.fetch(), 32);
        assert_eq!(r3.fetch(), 0);
        assert_eq!(r4.fetch(), 0);

        r1.inc(30);

        assert_eq!(r1.fetch(), 30);
        assert_eq!(r2.fetch(), 32);
        assert_eq!(r3.fetch(), 0);
        assert_eq!(r4.fetch(), 30);

        r4.inc(21);

        assert_eq!(r1.fetch(), 51);
        assert_eq!(r2.fetch(), 32);
        assert_eq!(r3.fetch(), 0);
        assert_eq!(r4.fetch(), 51);
    }
}
