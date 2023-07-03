use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use metric::{Attributes, Instrument, MetricKind, Observation, Registry, Reporter};
use parking_lot::RwLock;
use tokio::runtime::RuntimeMetrics;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

/// Register metric instrumentation for given tokio runtime.
///
/// # Lifecycle
/// When the [`Runtime`] linked to the given metrics is shut down, the metrics will be frozen in time (i.e. they keep
/// their last values). This instrumentation does NOT prevent the runtime from shutting down.
///
/// # Panics
/// Panics if a runtime with the same name is already registered in this registry.
///
///
/// [`Runtime`]: tokio::runtime::Runtime
pub fn setup_tokio_metrics(
    runtime_metrics: RuntimeMetrics,
    runtime_name: &'static str,
    registry: Arc<Registry>,
) {
    // Don't use the runtime_name directly as a instrument name, because this may confuse users when the name conflicts.
    // Since we cannot create a static string by concatenation, use an additional dispatch layer.
    let dispatcher = registry.register_instrument("tokio", TokioInstrumentDispatcher::default);
    let instrument = TokioInstrument::new(runtime_metrics, runtime_name);
    let mut guard = dispatcher.instruments.write();

    match guard.entry(runtime_name) {
        Entry::Vacant(v) => {
            v.insert(instrument);
        }
        Entry::Occupied(_) => {
            panic!("metrics for runtime '{runtime_name}' already registered");
        }
    }
}

/// Dispatcher from a single [`Instrument`] to the per-runtime [`TokioInstrument`]. This is used so we can use a
/// predictable instrument name and so that we only need to emit the same metric name/type/description once.
#[derive(Debug, Clone, Default)]
struct TokioInstrumentDispatcher {
    /// Maps from runtime name to the actual instrument.
    instruments: Arc<RwLock<HashMap<&'static str, TokioInstrument>>>,
}

macro_rules! rt_metric {
    (
        this = $this:expr,
        reporter = $reporter:expr,
        metric = $metric:ident,
        descr = $descr:literal,
        t = $t:ident,
    ) => {
        $reporter.start_metric(
            concat!("tokio_runtime_", stringify!($metric)),
            $descr,
            MetricKind::$t,
        );
        for sub in $this.values() {
            $reporter.report_observation(
                &sub.attr_rt,
                Observation::$t(sub.runtime_metrics.$metric() as _),
            );
        }
        $reporter.finish_metric();
    };
}

macro_rules! worker_metric {
    (
        this = $this:expr,
        reporter = $reporter:expr,
        metric = $metric:ident,
        descr = $descr:literal,
        t = $t:ident,
    ) => {
        $reporter.start_metric(
            // do NOT use worker_ prefix here because all metric names already contain it
            concat!("tokio_", stringify!($metric)),
            $descr,
            MetricKind::$t,
        );
        for sub in $this.values() {
            for (w, attr) in sub.attr_worker.iter().enumerate() {
                $reporter
                    .report_observation(attr, Observation::$t(sub.runtime_metrics.$metric(w) as _));
            }
        }
        $reporter.finish_metric();
    };
}

impl Instrument for TokioInstrumentDispatcher {
    fn report(&self, reporter: &mut dyn Reporter) {
        let guard = self.instruments.read();

        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = active_tasks_count,
            descr = "The number of active tasks in the runtime.",
            t = U64Gauge,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = blocking_queue_depth,
            descr = "The number of tasks currently scheduled in the blocking thread pool, spawned using `spawn_blocking`.",
            t = U64Gauge,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = budget_forced_yield_count,
            descr = "Number of times that tasks have been forced to yield back to the scheduler after exhausting their task budgets.",
            t = U64Counter,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = injection_queue_depth,
            descr = "The number of tasks currently scheduled in the runtime's injection queue.",
            t = U64Gauge,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = io_driver_ready_count,
            descr = "The number of ready events processed by the runtime's I/O driver.",
            t = U64Counter,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = io_driver_fd_deregistered_count,
            descr = "The number of file descriptors that have been deregistered by the runtime's I/O driver.",
            t = U64Counter,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = io_driver_fd_registered_count,
            descr = "The number of file descriptors that have been registered with the runtime's I/O driver.",
            t = U64Counter,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = num_blocking_threads,
            descr = "Number of additional threads spawned by the runtime.",
            t = U64Gauge,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = num_idle_blocking_threads,
            descr = "Number of idle threads, which have spawned by the runtime for `spawn_blocking` calls.",
            t = U64Gauge,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = num_workers,
            descr = "Number of worker threads used by the runtime",
            t = U64Gauge,
        );
        rt_metric!(
            this = guard,
            reporter = reporter,
            metric = remote_schedule_count,
            descr = "Number of tasks scheduled from **outside** of the runtime.",
            t = U64Counter,
        );

        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_local_queue_depth,
            descr = "The number of tasks currently scheduled in the given worker's local queue.",
            t = U64Gauge,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_local_schedule_count,
            descr = "The number of tasks scheduled from **within** the runtime on the given worker's local queue.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_noop_count,
            descr = "The number of times the given worker thread unparked but performed no work before parking again.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_overflow_count,
            descr = "The number of times the given worker thread saturated its local queue.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_park_count,
            descr = "The total number of times the given worker thread has parked.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_poll_count,
            descr = "The number of tasks the given worker thread has polled.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_steal_count,
            descr = "The number of tasks the given worker thread stole from another worker thread.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_steal_operations,
            descr = "The number of times the given worker thread stole tasks from another worker thread.",
            t = U64Counter,
        );
        worker_metric!(
            this = guard,
            reporter = reporter,
            metric = worker_total_busy_duration,
            descr = "The amount of time the given worker thread has been busy.",
            t = DurationCounter,
        );
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// [`Instrument`] for a single tokio [`Runtime`].
///
///
/// [`Runtime`]: tokio::runtime::Runtime
#[derive(Debug, Clone)]
struct TokioInstrument {
    /// Handler for the runtime metrics.
    ///
    /// This can be considered a WEAK reference to the tokio runtime. [`RuntimeMetrics`] is internally just based on
    /// [`Handle`] which -- according to its own method documentation -- is useless if the runtime was shut down.
    /// Especially it does NOT prevent the runtime from shutting down.
    ///
    ///
    /// [`Handle`]: tokio::runtime::Handle
    runtime_metrics: RuntimeMetrics,

    /// [`Attributes`] that are used for all runtime-scoped metrics.
    attr_rt: Attributes,

    /// [`Attributes`] that are used for all worker-scoped metrics.
    attr_worker: Vec<Attributes>,
}

impl TokioInstrument {
    fn new(runtime_metrics: RuntimeMetrics, runtime_name: &'static str) -> Self {
        let workers = runtime_metrics.num_workers();
        Self {
            runtime_metrics,
            attr_rt: Attributes::from(&[("runtime", runtime_name)]),
            attr_worker: (0..workers)
                .map(|w| {
                    Attributes::from([
                        ("runtime", Cow::<'static, str>::from(runtime_name)),
                        ("worker", Cow::<'static, str>::from(w.to_string())),
                    ])
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metric::RawReporter;
    use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

    use super::*;

    #[test]
    #[should_panic(expected = "metrics for runtime 'foo' already registered")]
    fn test_panic_register_twice() {
        let registry = Arc::new(Registry::default());
        let rt = Runtime::new().unwrap();
        setup_tokio_metrics(rt.metrics(), "foo", Arc::clone(&registry));
        setup_tokio_metrics(rt.metrics(), "foo", registry);
    }

    /// Test that runtimes are scoped per [`Registry`] and are NOT inserted into some global static state.
    ///
    /// If there would be a global state, this would panic like [`test_panic_register_twice`].
    #[test]
    fn test_runtimes_registered_per_registry() {
        let registry1 = Arc::new(Registry::default());
        let registry2 = Arc::new(Registry::default());
        let rt = Runtime::new().unwrap();
        setup_tokio_metrics(rt.metrics(), "foo", Arc::clone(&registry1));
        setup_tokio_metrics(rt.metrics(), "foo", Arc::clone(&registry2));

        // ensure that registries are only dropped at the end of the test
        drop(registry1);
        drop(registry2);
    }

    #[test]
    fn test_metrics() {
        let registry = Arc::new(Registry::default());
        let rt1 = RuntimeBuilder::new_multi_thread()
            .worker_threads(3)
            .build()
            .unwrap();
        let rt2 = RuntimeBuilder::new_multi_thread()
            .worker_threads(5)
            .build()
            .unwrap();
        setup_tokio_metrics(rt1.metrics(), "foo", Arc::clone(&registry));
        setup_tokio_metrics(rt2.metrics(), "bar", Arc::clone(&registry));

        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("tokio_runtime_num_workers")
                .unwrap()
                .observation(&[("runtime", "foo")])
                .unwrap(),
            &Observation::U64Gauge(3),
        );
        assert_eq!(
            reporter
                .metric("tokio_runtime_num_workers")
                .unwrap()
                .observation(&[("runtime", "bar")])
                .unwrap(),
            &Observation::U64Gauge(5),
        );
        assert_eq!(
            reporter
                .metric("tokio_worker_steal_operations")
                .unwrap()
                .observation(&[("runtime", "foo"), ("worker", "0")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("tokio_worker_steal_operations")
                .unwrap()
                .observation(&[("runtime", "foo"), ("worker", "2")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("tokio_worker_steal_operations")
                .unwrap()
                .observation(&[("runtime", "bar"), ("worker", "0")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("tokio_worker_steal_operations")
                .unwrap()
                .observation(&[("runtime", "bar"), ("worker", "4")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
    }

    #[test]
    fn test_runtime_shutdown() {
        let registry = Arc::new(Registry::default());
        let rt = RuntimeBuilder::new_multi_thread()
            .worker_threads(3)
            .build()
            .unwrap();
        setup_tokio_metrics(rt.metrics(), "foo", Arc::clone(&registry));

        rt.shutdown_timeout(Duration::from_secs(5));

        // can still report
        let mut reporter = RawReporter::default();
        registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("tokio_runtime_num_workers")
                .unwrap()
                .observation(&[("runtime", "foo")])
                .unwrap(),
            &Observation::U64Gauge(3),
        );

        // ensure that registry is only dropped at the end of the test
        drop(registry);
    }
}
