//! Metrics used to measure [`ObjectStore`] interactions.
//!
//! # Grouping
//! Interactions are grouped by [`OpResult`] -- which is determined at the end of an operation. This means AFTER the
//! respective [`Future`] and [`Stream`] was fully polled OR dropped.
//!
//! # Metrics
//! Different [`ObjectStore`] operations require different metrics, i.e. [`ObjectStore::get`] measures end-to-end
//! latency, traffic in bytes, and time-to-first byte; while [`ObjectStore::list`] measures end-to-end latency, and
//! number of elements. To account for that while avoiding code duplication and potential inconsistencies, metrics form
//! a hierarchy[^hierarchy]:
//!
//! - [`Metrics`]
//!   - [`MetricsWithCount`]
//!   - [`MetricsWithBytes`]
//!     - [`MetricsWithBytesAndTtfb`]
//!
//! # Recorder
//! To account for [Async Cancellation] metrics are never directly observed but are handled through recorders. A
//! recorder is created BEFORE the first async interaction (via a [`Future`] or [`Stream`]). Each [metric
//! type](#metrics) has a one-to-one relationship with a recorder type (e.g. [`MetricsWithCount`] ⇔
//! [`MetricsWithCountRecorder`]) [^recorder_type].
//!
//! The recorder has the following API:
//!
//! - `new`: Create the recorder and measure the start timestamp. The result is initially set to [`OpResult::Canceled`].
//!   Data (e.g. number of bytes) is initially set to [`None`]. No metric data is written as this point (see `drop` for
//!   the actual write operation). Both -- the result and the data -- will likely be changed/overridden by `submit`.
//!   However if `submit` will never be called, then this [`OpResult::Canceled`] will actually be written by `drop`.
//! - `freeze`: Determines the end timestamp. Calling it a 2nd time will have no effect. This returns the end timestamp
//!   (in case of subsequent calls this is the first measurement).
//! - `submit`: Submit data (e.g. number of items) alongside with the [`OpResult`] to the recorder. This will `freeze`
//!   the recorder. Note that the metric will NOT be written at this point. Instead this will be done during `drop`.
//! - `drop`: The metric is actually written to the metric sub-system based on the currently set [`OpResult`]
//!   (initially [`OpResult::Canceled`] but something else after `submit`).
//!
//!
//! [^hierarchy]: Think of it like a class hierarchy if Rust would have classes -- which it does not. It involves
//!     inheritance of both data and behavior.
//! [^recorder_type]: Recorders are generic to allow code sharing along the class hierarchy.
//!
//!
//! [Async Cancellation]: https://blog.yoshuawuyts.com/async-cancellation-1/
//! [`Future`]: std::future::Future
//! [`ObjectStore`]: object_store::ObjectStore
//! [`ObjectStore::get`]: object_store::ObjectStore::get
//! [`ObjectStore::list`]: object_store::ObjectStore::list
//! [`Stream`]: futures::stream::Stream
use std::{borrow::Cow, sync::Arc};

use iox_time::{Time, TimeProvider};
use metric::{
    Attributes, DurationHistogram, Metric, MetricObserver, U64Counter, U64Histogram,
    U64HistogramOptions,
};

use object_store::Result;

use crate::{
    StoreType,
    log::{LogContext, LogRecord},
};

/// The way an [`ObjectStore`] operation finished.
///
///
/// [`ObjectStore`]: object_store::ObjectStore
#[derive(Debug, Clone, Copy)]
pub(crate) enum OpResult {
    /// Operation finished successfully.
    Success,

    /// Operation encountered an error.
    Error,

    /// Operation was canceled.
    Canceled,
}

impl<T, E> From<&Result<T, E>> for OpResult {
    fn from(res: &Result<T, E>) -> Self {
        match res {
            Ok(_) => Self::Success,
            Err(_) => Self::Error,
        }
    }
}

/// Metric keyed by [`OpResult`].
#[derive(Debug, Clone)]
pub(crate) struct OpResultMetric<T>
where
    T: MetricObserver<Recorder = T>,
{
    /// Metric that corresponds to [`OpResult::Success`].
    success: T,

    /// Metric that corresponds to [`OpResult::Error`].
    error: T,

    /// Metric that corresponds to [`OpResult::Canceled`].
    canceled: T,
}

impl<T> OpResultMetric<T>
where
    T: MetricObserver<Recorder = T>,
{
    pub(crate) fn new(
        metric: Metric<T>,
        store_type: &StoreType,
        op: &'static str,
        bucket: &Option<String>,
    ) -> Self {
        let attributes = |result| {
            let mut attributes = Attributes::from([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed(result)),
            ]);
            if let Some(bucket) = bucket {
                attributes.insert("bucket", bucket.clone());
            }
            attributes
        };

        Self {
            success: metric.recorder(attributes("success")),
            error: metric.recorder(attributes("error")),
            canceled: metric.recorder(attributes("canceled")),
        }
    }

    fn get(&self, op_res: OpResult) -> &T {
        match op_res {
            OpResult::Success => &self.success,
            OpResult::Error => &self.error,
            OpResult::Canceled => &self.canceled,
        }
    }
}

/// Metric for end-to-end latency.
#[derive(Debug)]
pub(crate) struct Metrics {
    duration: OpResultMetric<DurationHistogram>,
    op: &'static str,
    store_type: StoreType,
}

impl Metrics {
    pub(crate) fn new(
        registry: &metric::Registry,
        store_type: &StoreType,
        op: &'static str,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            duration: OpResultMetric::new(
                registry.register_metric(
                    "object_store_op_duration",
                    "object store operation duration",
                ),
                store_type,
                op,
                bucket,
            ),
            op,
            store_type: store_type.clone(),
        }
    }
}

impl AsRef<Self> for Metrics {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Recorder for [`Metrics`].
#[derive(Debug)]
pub(crate) struct MetricsRecorder<M>
where
    M: AsRef<Metrics> + std::fmt::Debug + Send + Sync,
{
    metrics: Arc<M>,
    time_provider: Arc<dyn TimeProvider>,
    t_begin: Time,
    t_end: Option<Time>,
    op_res: OpResult,
    log_record: LogRecord,
}

impl<M> MetricsRecorder<M>
where
    M: AsRef<Metrics> + std::fmt::Debug + Send + Sync,
{
    #[must_use]
    pub(crate) fn new(
        metrics: Arc<M>,
        time_provider: &Arc<dyn TimeProvider>,
        context: LogContext,
        bucket: &Option<String>,
    ) -> Self {
        let log_record = LogRecord::new(
            metrics.as_ref().as_ref().store_type.clone(),
            metrics.as_ref().as_ref().op,
            context,
            bucket,
        );

        Self {
            metrics,
            time_provider: Arc::clone(time_provider),
            t_begin: time_provider.now(),
            t_end: None,
            op_res: OpResult::Canceled,
            log_record,
        }
    }

    pub(crate) fn submit(&mut self, op_res: OpResult) {
        self.op_res = op_res;
        self.freeze();
    }

    fn freeze(&mut self) -> Time {
        match self.t_end {
            Some(t_end) => t_end,
            None => {
                let t_end = self.time_provider.now();
                self.t_end = Some(t_end);
                t_end
            }
        }
    }
}

impl<M> Drop for MetricsRecorder<M>
where
    M: AsRef<Metrics> + std::fmt::Debug + Send + Sync,
{
    fn drop(&mut self) {
        let t_end = self.freeze();

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        let Some(delta) = t_end.checked_duration_since(self.t_begin) else {
            return;
        };

        self.metrics
            .as_ref()
            .as_ref()
            .duration
            .get(self.op_res)
            .record(delta);
        self.log_record.duration = Some(delta);
        self.log_record.op_res = self.op_res;
    }
}

/// Metric for end-to-end latency (derived from [`Metrics`]), and number of bytes.
#[derive(Debug)]
pub(crate) struct MetricsWithBytes {
    inner: Metrics,
    // keep the counter in addition to the histogram to NOT break all the dashboards
    bytes: OpResultMetric<U64Counter>,
    bytes_hist: OpResultMetric<U64Histogram>,
}

impl MetricsWithBytes {
    pub(crate) fn new(
        registry: &metric::Registry,
        store_type: &StoreType,
        op: &'static str,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            inner: Metrics::new(registry, store_type, op, bucket),
            bytes: OpResultMetric::new(
                registry.register_metric::<U64Counter>(
                    "object_store_transfer_bytes",
                    "cumulative count of file content bytes transferred to/from the object store",
                ),
                store_type,
                op,
                bucket,
            ),
            bytes_hist: OpResultMetric::new(
                registry.register_metric_with_options::<U64Histogram, _>(
                    "object_store_transfer_bytes_hist",
                    "count of file content bytes transferred to/from the object store",
                    || {
                        U64HistogramOptions::new(vec![
                            1,
                            10,
                            100,
                            1_000,
                            10_000,
                            100_000,
                            1_000_000,
                            10_000_000,
                            100_000_000,
                            1_000_000_000,
                            u64::MAX,
                        ])
                    },
                ),
                store_type,
                op,
                bucket,
            ),
        }
    }
}

impl AsRef<Metrics> for MetricsWithBytes {
    fn as_ref(&self) -> &Metrics {
        &self.inner
    }
}

impl AsRef<Self> for MetricsWithBytes {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Recorder for [`MetricsWithBytes`].
#[derive(Debug)]
pub(crate) struct MetricsWithBytesRecorder<M = MetricsWithBytes>
where
    M: AsRef<Metrics> + AsRef<MetricsWithBytes> + std::fmt::Debug + Send + Sync,
{
    inner: MetricsRecorder<M>,
    bytes: Option<u64>,
}

impl<M> MetricsWithBytesRecorder<M>
where
    M: AsRef<Metrics> + AsRef<MetricsWithBytes> + std::fmt::Debug + Send + Sync,
{
    #[must_use]
    pub(crate) fn new(
        metrics: Arc<M>,
        time_provider: &Arc<dyn TimeProvider>,
        context: LogContext,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            inner: MetricsRecorder::new(metrics, time_provider, context, bucket),
            bytes: None,
        }
    }

    fn freeze(&mut self) -> Time {
        self.inner.freeze()
    }

    pub(crate) fn submit(&mut self, op_res: OpResult, bytes: Option<u64>) {
        self.bytes = bytes;
        self.inner.submit(op_res);
    }
}

impl<M> Drop for MetricsWithBytesRecorder<M>
where
    M: AsRef<Metrics> + AsRef<MetricsWithBytes> + std::fmt::Debug + Send + Sync,
{
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes {
            let metrics: &MetricsWithBytes = self.inner.metrics.as_ref().as_ref();
            metrics.bytes.get(self.inner.op_res).inc(bytes);
            metrics.bytes_hist.get(self.inner.op_res).record(bytes);
            self.inner.log_record.bytes = Some(bytes);
        }
    }
}

/// Metric for end-to-end latency (derived from [`Metrics`]), number of bytes (derived from [`MetricsWithBytes`]), and
/// time-to-first-byte.
#[derive(Debug)]
pub(crate) struct MetricsWithBytesAndTtfb {
    inner: MetricsWithBytes,
    duration_headers: OpResultMetric<DurationHistogram>,
    duration_ttfb: OpResultMetric<DurationHistogram>,
}

impl MetricsWithBytesAndTtfb {
    pub(crate) fn new(
        registry: &metric::Registry,
        store_type: &StoreType,
        op: &'static str,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            inner: MetricsWithBytes::new(registry, store_type, op, bucket),
            duration_headers: OpResultMetric::new(
                registry.register_metric(
                    "object_store_op_headers",
                    "Time to response headers for object store operation",
                ),
                store_type,
                op,
                bucket,
            ),
            duration_ttfb: OpResultMetric::new(
                registry.register_metric(
                    "object_store_op_ttfb",
                    "Time to first byte for object store operation",
                ),
                store_type,
                op,
                bucket,
            ),
        }
    }
}

impl AsRef<Metrics> for MetricsWithBytesAndTtfb {
    fn as_ref(&self) -> &Metrics {
        &self.inner.inner
    }
}

impl AsRef<MetricsWithBytes> for MetricsWithBytesAndTtfb {
    fn as_ref(&self) -> &MetricsWithBytes {
        &self.inner
    }
}

impl AsRef<Self> for MetricsWithBytesAndTtfb {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Recorder for [`MetricsWithBytesAndTtfb`].
#[derive(Debug)]
pub(crate) struct MetricsWithBytesAndTtfbRecorder<M = MetricsWithBytesAndTtfb>
where
    M: AsRef<Metrics>
        + AsRef<MetricsWithBytes>
        + AsRef<MetricsWithBytesAndTtfb>
        + std::fmt::Debug
        + Send
        + Sync,
{
    inner: MetricsWithBytesRecorder<M>,
    t_headers: Option<Time>,
    t_first_byte: Option<Time>,
}

impl<M> MetricsWithBytesAndTtfbRecorder<M>
where
    M: AsRef<Metrics>
        + AsRef<MetricsWithBytes>
        + AsRef<MetricsWithBytesAndTtfb>
        + std::fmt::Debug
        + Send
        + Sync,
{
    #[must_use]
    pub(crate) fn new(
        metrics: Arc<M>,
        time_provider: &Arc<dyn TimeProvider>,
        context: LogContext,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            inner: MetricsWithBytesRecorder::new(metrics, time_provider, context, bucket),
            t_headers: None,
            t_first_byte: None,
        }
    }

    pub(crate) fn freeze(&mut self) -> Time {
        self.inner.freeze()
    }

    pub(crate) fn submit(
        &mut self,
        t_headers: Option<Time>,
        t_first_byte: Option<Time>,
        op_res: OpResult,
        bytes: Option<u64>,
    ) {
        self.t_headers = t_headers;
        self.t_first_byte = t_first_byte;
        self.inner.submit(op_res, bytes);
    }

    pub(crate) fn time_provider(&self) -> &Arc<dyn TimeProvider> {
        &self.inner.inner.time_provider
    }
}

impl<M> Drop for MetricsWithBytesAndTtfbRecorder<M>
where
    M: AsRef<Metrics>
        + AsRef<MetricsWithBytes>
        + AsRef<MetricsWithBytesAndTtfb>
        + std::fmt::Debug
        + Send
        + Sync,
{
    fn drop(&mut self) {
        if let Some(delta) = self
            .t_headers
            .and_then(|t_headers| t_headers.checked_duration_since(self.inner.inner.t_begin))
        {
            let metrics: &MetricsWithBytesAndTtfb = self.inner.inner.metrics.as_ref().as_ref();
            metrics
                .duration_headers
                .get(self.inner.inner.op_res)
                .record(delta);
            self.inner.inner.log_record.d_headers = Some(delta);
        }
        if let Some(delta) = self
            .t_first_byte
            .and_then(|t_first_byte| t_first_byte.checked_duration_since(self.inner.inner.t_begin))
        {
            let metrics: &MetricsWithBytesAndTtfb = self.inner.inner.metrics.as_ref().as_ref();
            metrics
                .duration_ttfb
                .get(self.inner.inner.op_res)
                .record(delta);
            self.inner.inner.log_record.d_first_byte = Some(delta);
        }
    }
}

/// Metric for end-to-end latency (derived from [`Metrics`]), and number of elements/objects/things.
#[derive(Debug)]
pub(crate) struct MetricsWithCount {
    inner: Metrics,
    // keep the counter in addition to the histogram to NOT break all the dashboards
    count: OpResultMetric<U64Counter>,
    count_hist: OpResultMetric<U64Histogram>,
}

impl MetricsWithCount {
    pub(crate) fn new(
        registry: &metric::Registry,
        store_type: &StoreType,
        op: &'static str,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            inner: Metrics::new(registry, store_type, op, bucket),
            count: OpResultMetric::new(
                registry.register_metric::<U64Counter>(
                    "object_store_transfer_objects",
                    "cumulative count of objects transferred to/from the object store",
                ),
                store_type,
                op,
                bucket,
            ),
            count_hist: OpResultMetric::new(
                registry.register_metric_with_options::<U64Histogram, _>(
                    "object_store_transfer_objects_hist",
                    "count of objects transferred to/from the object store",
                    || {
                        U64HistogramOptions::new(vec![
                            1,
                            10,
                            100,
                            1_000,
                            10_000,
                            100_000,
                            1_000_000,
                            10_000_000,
                            100_000_000,
                            1_000_000_000,
                            u64::MAX,
                        ])
                    },
                ),
                store_type,
                op,
                bucket,
            ),
        }
    }
}

impl AsRef<Metrics> for MetricsWithCount {
    fn as_ref(&self) -> &Metrics {
        &self.inner
    }
}

impl AsRef<Self> for MetricsWithCount {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// Recorder for [`MetricsWithCount`].
#[derive(Debug)]
pub(crate) struct MetricsWithCountRecorder<M = MetricsWithCount>
where
    M: AsRef<Metrics> + AsRef<MetricsWithCount> + std::fmt::Debug + Send + Sync,
{
    inner: MetricsRecorder<M>,
    count: Option<u64>,
}

impl<M> MetricsWithCountRecorder<M>
where
    M: AsRef<Metrics> + AsRef<MetricsWithCount> + std::fmt::Debug + Send + Sync,
{
    #[must_use]
    pub(crate) fn new(
        metrics: Arc<M>,
        time_provider: &Arc<dyn TimeProvider>,
        context: LogContext,
        bucket: &Option<String>,
    ) -> Self {
        Self {
            inner: MetricsRecorder::new(metrics, time_provider, context, bucket),
            count: None,
        }
    }

    pub(crate) fn submit(&mut self, op_res: OpResult, count: Option<u64>) {
        self.count = count;
        self.inner.submit(op_res);
    }
}

impl<M> Drop for MetricsWithCountRecorder<M>
where
    M: AsRef<Metrics> + AsRef<MetricsWithCount> + std::fmt::Debug + Send + Sync,
{
    fn drop(&mut self) {
        if let Some(count) = self.count {
            let metrics: &MetricsWithCount = self.inner.metrics.as_ref().as_ref();
            metrics.count.get(self.inner.op_res).inc(count);
            metrics.count_hist.get(self.inner.op_res).record(count);
            self.inner.log_record.count = Some(count);
        }
    }
}
