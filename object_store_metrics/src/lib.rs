//! A metric instrumentation wrapper over [`ObjectStore`] implementations.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::clone_on_ref_ptr)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

use object_store::{GetOptions, GetResultPayload, PutOptions, PutResult};
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::ops::Range;
use std::sync::Arc;
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, Stream, StreamExt};
use iox_time::{SystemProvider, Time, TimeProvider};
use metric::{DurationHistogram, Metric, U64Counter};
use pin_project::{pin_project, pinned_drop};

use object_store::{
    path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result,
};
use tokio::io::AsyncWrite;

#[cfg(test)]
mod dummy;

#[derive(Debug, Clone)]
struct Metrics {
    success_duration: DurationHistogram,
    error_duration: DurationHistogram,
}

impl Metrics {
    fn new(registry: &metric::Registry, op: &'static str) -> Self {
        // Call durations broken down by op & result
        let duration: Metric<DurationHistogram> = registry.register_metric(
            "object_store_op_duration",
            "object store operation duration",
        );

        Self {
            success_duration: duration.recorder(&[("op", op), ("result", "success")]),
            error_duration: duration.recorder(&[("op", op), ("result", "error")]),
        }
    }

    fn record(&self, t_begin: Time, t_end: Time, success: bool) {
        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        let Some(delta) = t_end.checked_duration_since(t_begin) else {
            return;
        };

        if success {
            self.success_duration.record(delta);
        } else {
            self.error_duration.record(delta);
        }
    }
}

#[derive(Debug, Clone)]
struct MetricsWithBytes {
    inner: Metrics,
    success_bytes: U64Counter,
    error_bytes: U64Counter,
}

impl MetricsWithBytes {
    fn new(registry: &metric::Registry, op: &'static str) -> Self {
        // Byte counts up/down
        let bytes = registry.register_metric::<U64Counter>(
            "object_store_transfer_bytes",
            "cumulative count of file content bytes transferred to/from the object store",
        );

        Self {
            inner: Metrics::new(registry, op),
            success_bytes: bytes.recorder(&[("op", op), ("result", "success")]),
            error_bytes: bytes.recorder(&[("op", op), ("result", "error")]),
        }
    }

    fn record_bytes_only(&self, success: bool, bytes: u64) {
        if success {
            self.success_bytes.inc(bytes);
        } else {
            self.error_bytes.inc(bytes);
        }
    }

    fn record(&self, t_begin: Time, t_end: Time, success: bool, bytes: Option<u64>) {
        if let Some(bytes) = bytes {
            self.record_bytes_only(success, bytes);
        }

        self.inner.record(t_begin, t_end, success);
    }
}

#[derive(Debug, Clone)]
struct MetricsWithCount {
    inner: Metrics,
    success_count: U64Counter,
    error_count: U64Counter,
}

impl MetricsWithCount {
    fn new(registry: &metric::Registry, op: &'static str) -> Self {
        let count = registry.register_metric::<U64Counter>(
            "object_store_transfer_objects",
            "cumulative count of objects transferred to/from the object store",
        );

        Self {
            inner: Metrics::new(registry, op),
            success_count: count.recorder(&[("op", op), ("result", "success")]),
            error_count: count.recorder(&[("op", op), ("result", "error")]),
        }
    }

    fn record_count_only(&self, success: bool, count: u64) {
        if success {
            self.success_count.inc(count);
        } else {
            self.error_count.inc(count);
        }
    }

    fn record(&self, t_begin: Time, t_end: Time, success: bool, count: Option<u64>) {
        if let Some(count) = count {
            self.record_count_only(success, count);
        }

        self.inner.record(t_begin, t_end, success);
    }
}

/// An instrumentation decorator, wrapping an underlying [`ObjectStore`]
/// implementation and recording bytes transferred and call latency.
///
/// # Stream Duration
///
/// The [`ObjectStore::get()`] call can return a [`Stream`] which is polled
/// by the caller and may yield chunks of a file over a series of polls (as
/// opposed to all of the file data in one go). Because the caller drives the
/// polling and therefore fetching of data from the object store over the
/// lifetime of the [`Stream`], the duration of a [`ObjectStore::get()`]
/// request is measured to be the wall clock difference between the moment the
/// caller executes the [`ObjectStore::get()`] call, up until the last chunk
/// of data is yielded to the caller.
///
/// This means the duration metrics measuring consumption of returned streams
/// are recording the rate at which the application reads the data, as opposed
/// to the duration of time taken to fetch that data.
///
/// # Stream Errors
///
/// The [`ObjectStore::get()`] method can return a [`Stream`] of [`Result`]
/// instances, and returning an error when polled is not necessarily a terminal
/// state. The metric recorder allows for a caller to observe a transient error
/// and subsequently go on to complete reading the stream, recording this read
/// in the "success" histogram.
///
/// If a stream is not polled again after observing an error, the operation is
/// recorded in the "error" histogram.
///
/// A stream can return an arbitrary sequence of success and error states before
/// terminating, with the last observed poll result that yields a [`Result`]
/// dictating which histogram the operation is recorded in.
///
/// # Bytes Transferred
///
/// The metric recording bytes transferred accounts for only object data, and
/// not object metadata (such as that returned by list methods).
///
/// The total data transferred will be greater than the metric value due to
/// metadata queries, read errors, etc. The metric tracks the amount of object
/// data successfully yielded to the caller.
///
/// # Backwards Clocks
///
/// If the system clock is observed as moving backwards in time, call durations
/// are not recorded. The bytes transferred metric is not affected.
#[derive(Debug)]
pub struct ObjectStoreMetrics {
    inner: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,

    put: MetricsWithBytes,
    get: MetricsWithBytes,
    get_range: MetricsWithBytes,
    get_ranges: MetricsWithBytes,
    head: Metrics,
    delete: Metrics,
    delete_stream: MetricsWithCount,
    list: MetricsWithCount,
    list_with_offset: MetricsWithCount,
    list_with_delimiter: MetricsWithCount,
    copy: Metrics,
    rename: Metrics,
    copy_if_not_exists: Metrics,
    rename_if_not_exists: Metrics,
}

impl ObjectStoreMetrics {
    /// Instrument `T`, pushing to `registry`.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        registry: &metric::Registry,
    ) -> Self {
        Self {
            inner,
            time_provider,

            put: MetricsWithBytes::new(registry, "put"),
            get: MetricsWithBytes::new(registry, "get"),
            get_range: MetricsWithBytes::new(registry, "get_range"),
            get_ranges: MetricsWithBytes::new(registry, "get_ranges"),
            head: Metrics::new(registry, "head"),
            delete: Metrics::new(registry, "delete"),
            delete_stream: MetricsWithCount::new(registry, "delete_stream"),
            list: MetricsWithCount::new(registry, "list"),
            list_with_offset: MetricsWithCount::new(registry, "list_with_offset"),
            list_with_delimiter: MetricsWithCount::new(registry, "list_with_delimiter"),
            copy: Metrics::new(registry, "copy"),
            rename: Metrics::new(registry, "rename"),
            copy_if_not_exists: Metrics::new(registry, "copy_if_not_exists"),
            rename_if_not_exists: Metrics::new(registry, "rename_if_not_exists"),
        }
    }
}

impl std::fmt::Display for ObjectStoreMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreMetrics({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreMetrics {
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        let t = self.time_provider.now();
        let size = bytes.len();
        let res = self.inner.put_opts(location, bytes, opts).await;
        self.put
            .record(t, self.time_provider.now(), res.is_ok(), Some(size as _));
        res
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        unimplemented!()
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        unimplemented!()
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let started_at = self.time_provider.now();

        let res = self.inner.get_opts(location, options).await;

        match res {
            Ok(mut res) => {
                res.payload = match res.payload {
                    GetResultPayload::File(file, path) => {
                        self.get.record(
                            started_at,
                            self.time_provider.now(),
                            true,
                            file.metadata().ok().map(|m| m.len()),
                        );
                        GetResultPayload::File(file, path)
                    }
                    GetResultPayload::Stream(s) => {
                        // Wrap the object store data stream in a decorator to track the
                        // yielded data / wall clock, inclusive of the inner call above.
                        GetResultPayload::Stream(Box::pin(Box::new(
                            StreamMetricRecorder::new(
                                s,
                                started_at,
                                BytesStreamDelegate::new(self.get.clone()),
                            )
                            .fuse(),
                        )))
                    }
                };
                Ok(res)
            }
            Err(e) => {
                self.get
                    .record(started_at, self.time_provider.now(), false, None);
                Err(e)
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let t = self.time_provider.now();
        let res = self.inner.get_range(location, range).await;
        self.get_range.record(
            t,
            self.time_provider.now(),
            res.is_ok(),
            res.as_ref().ok().map(|b| b.len() as _),
        );
        res
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        let t = self.time_provider.now();
        let res = self.inner.get_ranges(location, ranges).await;
        self.get_ranges.record(
            t,
            self.time_provider.now(),
            res.is_ok(),
            res.as_ref()
                .ok()
                .map(|b| b.iter().map(|b| b.len() as u64).sum()),
        );
        res
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let t = self.time_provider.now();
        let res = self.inner.head(location).await;
        self.head.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.delete(location).await;
        self.delete.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let started_at = self.time_provider.now();

        let s = self.inner.delete_stream(locations);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(
            s,
            started_at,
            CountStreamDelegate::new(self.delete_stream.clone()),
        )
        .fuse()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let started_at = self.time_provider.now();

        let s = self.inner.list(prefix);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(s, started_at, CountStreamDelegate::new(self.list.clone()))
            .fuse()
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        let started_at = self.time_provider.now();

        let s = self.inner.list_with_offset(prefix, offset);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(
            s,
            started_at,
            CountStreamDelegate::new(self.list_with_offset.clone()),
        )
        .fuse()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let t = self.time_provider.now();
        let res = self.inner.list_with_delimiter(prefix).await;
        self.list_with_delimiter.record(
            t,
            self.time_provider.now(),
            res.is_ok(),
            res.as_ref().ok().map(|res| res.objects.len() as _),
        );
        res
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.copy(from, to).await;
        self.copy.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.rename(from, to).await;
        self.rename.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.copy_if_not_exists(from, to).await;
        self.copy_if_not_exists
            .record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.rename_if_not_exists(from, to).await;
        self.rename_if_not_exists
            .record(t, self.time_provider.now(), res.is_ok());
        res
    }
}

/// A [`MetricDelegate`] is called whenever the [`StreamMetricRecorder`]
/// observes an `Ok(Item)` in the stream.
trait MetricDelegate {
    /// The type this delegate observes.
    type Item;

    /// Invoked when the stream yields an `Ok(Item)`.
    fn observe_ok(&self, value: &Self::Item);

    /// Finish stream.
    fn finish(&self, t_begin: Time, t_end: Time, success: bool);
}

/// A [`MetricDelegate`] for instrumented streams of [`Bytes`].
///
/// This impl is used to record the number of bytes yielded for
/// [`ObjectStore::get()`] calls.
#[derive(Debug)]
struct BytesStreamDelegate(MetricsWithBytes);

impl BytesStreamDelegate {
    fn new(metrics: MetricsWithBytes) -> Self {
        Self(metrics)
    }
}

impl MetricDelegate for BytesStreamDelegate {
    type Item = Bytes;

    fn observe_ok(&self, bytes: &Self::Item) {
        self.0.record_bytes_only(true, bytes.len() as _);
    }

    fn finish(&self, t_begin: Time, t_end: Time, success: bool) {
        self.0.record(t_begin, t_end, success, None);
    }
}

#[derive(Debug)]
struct CountStreamDelegate<T>(MetricsWithCount, PhantomData<T>);

impl<T> CountStreamDelegate<T> {
    fn new(metrics: MetricsWithCount) -> Self {
        Self(metrics, Default::default())
    }
}

impl<T> MetricDelegate for CountStreamDelegate<T> {
    type Item = T;

    fn observe_ok(&self, _value: &Self::Item) {
        self.0.record_count_only(true, 1);
    }

    fn finish(&self, t_begin: Time, t_end: Time, success: bool) {
        self.0.record(t_begin, t_end, success, None);
    }
}

/// [`StreamMetricRecorder`] decorates an underlying [`Stream`] for "get" /
/// "list" catalog operations, recording the wall clock duration and invoking
/// the metric delegate with the `Ok(T)` values.
///
/// For "gets" using the [`BytesStreamDelegate`], the bytes read counter is
/// incremented each time [`Self::poll_next()`] yields a buffer, and once the
/// [`StreamMetricRecorder`] is read to completion (specifically, until it
/// yields `Poll::Ready(None)`), or when it is dropped (whichever is sooner) the
/// decorator emits the wall clock measurement into the relevant histogram,
/// bucketed by operation result.
///
/// A stream may return a transient error when polled, and later successfully
/// emit all data in subsequent polls - therefore the duration is logged as an
/// error only if the last poll performed by the caller returned an error.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
struct StreamMetricRecorder<S, D, P = SystemProvider>
where
    P: TimeProvider,
    D: MetricDelegate,
{
    #[pin]
    inner: S,

    time_provider: P,

    // The timestamp at which the read request began, inclusive of the work
    // required to acquire the inner stream (which may involve fetching all the
    // data if the result is only pretending to be a stream).
    started_at: Time,
    // The time at which the last part of the data stream (or error) was
    // returned to the caller.
    //
    // The total get operation duration is calculated as this timestamp minus
    // the started_at timestamp.
    //
    // This field is always Some, until the end of the stream is observed at
    // which point the metrics are emitted and this field is set to None,
    // preventing the drop impl duplicating them.
    last_yielded_at: Option<Time>,
    // The error state of the last poll - true if OK, false if an error
    // occurred.
    //
    // This is used to select the correct success/error histogram which records
    // the operation duration.
    last_call_ok: bool,

    // Called when the stream yields an `Ok(T)` to allow the delegate to inspect
    // the `T`.
    metric_delegate: D,
}

impl<S, D> StreamMetricRecorder<S, D>
where
    S: Stream,
    D: MetricDelegate,
{
    fn new(stream: S, started_at: Time, metric_delegate: D) -> Self {
        let time_provider = SystemProvider::default();
        Self {
            inner: stream,

            // Set the last_yielded_at to now, ensuring the duration of work
            // already completed acquiring the steam is correctly recorded even
            // if the stream is never polled / data never read.
            last_yielded_at: Some(time_provider.now()),
            // Acquiring the stream was successful, even if the data was never
            // read.
            last_call_ok: true,

            started_at,
            time_provider,

            metric_delegate,
        }
    }
}

impl<S, T, D, P, E> Stream for StreamMetricRecorder<S, D, P>
where
    S: Stream<Item = Result<T, E>>,
    P: TimeProvider,
    D: MetricDelegate<Item = T>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = this.inner.poll_next(cx);

        match res {
            Poll::Ready(Some(Ok(value))) => {
                *this.last_call_ok = true;
                *this.last_yielded_at.as_mut().unwrap() = this.time_provider.now();

                // Allow the pluggable metric delegate to record the value of T
                this.metric_delegate.observe_ok(&value);

                Poll::Ready(Some(Ok(value)))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.last_call_ok = false;
                *this.last_yielded_at.as_mut().unwrap() = this.time_provider.now();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // The stream has terminated - record the wall clock duration
                // immediately.
                this.metric_delegate.finish(
                    *this.started_at,
                    this.last_yielded_at
                        .take()
                        .expect("no last_yielded_at value for fused stream"),
                    *this.last_call_ok,
                );

                Poll::Ready(None)
            }
            v => v,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Impl the default size_hint() so this wrapper doesn't mask the size
        // hint from the inner stream, if any.
        self.inner.size_hint()
    }
}

#[pinned_drop]
impl<S, D, P> PinnedDrop for StreamMetricRecorder<S, D, P>
where
    P: TimeProvider,
    D: MetricDelegate,
{
    fn drop(self: Pin<&mut Self>) {
        // Only emit metrics if the end of the stream was not observed (and
        // therefore last_yielded_at is still Some).
        if let Some(last) = self.last_yielded_at {
            self.metric_delegate
                .finish(self.started_at, last, self.last_call_ok);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Error, ErrorKind},
        sync::Arc,
        time::Duration,
    };

    use futures::{stream, TryStreamExt};
    use metric::Attributes;
    use std::io::Read;

    use dummy::DummyObjectStore;
    use object_store::{local::LocalFileSystem, memory::InMemory};

    use super::*;

    #[track_caller]
    fn assert_histogram_hit<const N: usize>(
        metrics: &metric::Registry,
        name: &'static str,
        attr: [(&'static str, &'static str); N],
    ) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>(name)
            .expect("failed to read histogram")
            .get_observer(&Attributes::from(&attr))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 0, "metric {name} did not record any calls");
    }

    #[track_caller]
    fn assert_histogram_not_hit<const N: usize>(
        metrics: &metric::Registry,
        name: &'static str,
        attr: [(&'static str, &'static str); N],
    ) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>(name)
            .expect("failed to read histogram")
            .get_observer(&Attributes::from(&attr))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count == 0, "metric {name} did record {hit_count} calls");
    }

    #[track_caller]
    fn assert_counter_value<const N: usize>(
        metrics: &metric::Registry,
        name: &'static str,
        attr: [(&'static str, &'static str); N],
        value: u64,
    ) {
        let count = metrics
            .get_instrument::<Metric<U64Counter>>(name)
            .expect("failed to read counter")
            .get_observer(&Attributes::from(&attr))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(count, value);
    }

    #[tokio::test]
    async fn test_put() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .put(
                &Path::from("test"),
                Bytes::from([42_u8, 42, 42, 42, 42].as_slice()),
            )
            .await
            .expect("put should succeed");

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "put"), ("result", "success")],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "put"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_put_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(DummyObjectStore::new("s3"));
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .put(
                &Path::from("test"),
                Bytes::from([42_u8, 42, 42, 42, 42].as_slice()),
            )
            .await
            .expect_err("put should error");

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "put"), ("result", "error")],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "put"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_list() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        store
            .put(&Path::from("bar"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store.list(None).try_collect::<Vec<_>>().await.unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [("op", "list"), ("result", "success")],
            2,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "list"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_list_with_offset() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        store
            .put(&Path::from("bar"), Bytes::default())
            .await
            .unwrap();
        store
            .put(&Path::from("baz"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .list_with_offset(None, &Path::from("bar"))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [("op", "list_with_offset"), ("result", "success")],
            2,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "list_with_offset"), ("result", "success")],
        );

        // NOT raw `list` call
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "list"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_list_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(DummyObjectStore::new("s3"));
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        assert!(
            store.list(None).try_collect::<Vec<_>>().await.is_err(),
            "mock configured to fail"
        );

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "list"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .list_with_delimiter(Some(&Path::from("test")))
            .await
            .expect("list should succeed");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "list_with_delimiter"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_list_with_delimiter_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(DummyObjectStore::new("s3"));
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        assert!(
            store
                .list_with_delimiter(Some(&Path::from("test")))
                .await
                .is_err(),
            "mock configured to fail"
        );

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "list_with_delimiter"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_head_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(DummyObjectStore::new("s3"));
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .head(&Path::from("test"))
            .await
            .expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "head"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_get_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(DummyObjectStore::new("s3"));
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .get(&Path::from("test"))
            .await
            .expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_getrange_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(DummyObjectStore::new("s3"));
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .get_range(&Path::from("test"), 0..1000)
            .await
            .expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get_range"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_getranges() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::from_static(b"bar"))
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .get_ranges(&Path::from("foo"), &[0..2, 1..2, 0..1])
            .await
            .unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "get_ranges"), ("result", "success")],
            4,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get_ranges"), ("result", "success")],
        );

        // NO `get_range` used!
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get_range"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_copy() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .copy(&Path::from("foo"), &Path::from("bar"))
            .await
            .unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "copy"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_copy_if_not_exists() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .copy_if_not_exists(&Path::from("foo"), &Path::from("bar"))
            .await
            .unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "copy_if_not_exists"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_rename() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .rename(&Path::from("foo"), &Path::from("bar"))
            .await
            .unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "rename"), ("result", "success")],
        );

        // NO `copy`/`delete` used!
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "copy"), ("result", "success")],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "delete"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_rename_if_not_exists() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .rename_if_not_exists(&Path::from("foo"), &Path::from("bar"))
            .await
            .unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "rename_if_not_exists"), ("result", "success")],
        );

        // NO `copy`/`copy_if_not_exists`/`delete` used!
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "copy"), ("result", "success")],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "copy_if_not_exists"), ("result", "success")],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "delete"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_delete_stream() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("foo"), Bytes::default())
            .await
            .unwrap();
        store
            .put(&Path::from("bar"), Bytes::default())
            .await
            .unwrap();
        store
            .put(&Path::from("baz"), Bytes::default())
            .await
            .unwrap();
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        store
            .delete_stream(
                stream::iter(["foo", "baz"])
                    .map(|s| Ok(Path::from(s)))
                    .boxed(),
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [("op", "delete_stream"), ("result", "success")],
            2,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "delete_stream"), ("result", "success")],
        );

        // NOT raw `delete` call
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "delete"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_put_get_getrange_head_delete_file() {
        let metrics = Arc::new(metric::Registry::default());
        // Temporary workaround for https://github.com/apache/arrow-rs/issues/2370
        let path = std::fs::canonicalize(".").unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        let data = [42_u8, 42, 42, 42, 42];
        let path = Path::from("test");
        store
            .put(&path, Bytes::copy_from_slice(&data))
            .await
            .expect("put should succeed");

        let got = store.get(&path).await.expect("should read file");
        match got.payload {
            GetResultPayload::File(mut file, _) => {
                let mut contents = vec![];
                file.read_to_end(&mut contents)
                    .expect("failed to read file data");
                assert_eq!(contents, &data);
            }
            v => panic!("not a file: {v:?}"),
        }

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "get"), ("result", "success")],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get"), ("result", "success")],
        );

        store
            .get_range(&path, 1..4)
            .await
            .expect("should clean up test file");
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "get_range"), ("result", "success")],
            3,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get_range"), ("result", "success")],
        );

        store.head(&path).await.expect("should clean up test file");
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "head"), ("result", "success")],
        );

        store
            .delete(&path)
            .await
            .expect("should clean up test file");
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "delete"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_get_stream() {
        let metrics = Arc::new(metric::Registry::default());
        let store = Arc::new(InMemory::new());
        let time = Arc::new(SystemProvider::new());
        let store = ObjectStoreMetrics::new(store, time, &metrics);

        let data = [42_u8, 42, 42, 42, 42];
        let path = Path::from("test");
        store
            .put(&path, Bytes::copy_from_slice(&data))
            .await
            .expect("put should succeed");

        let got = store.get(&path).await.expect("should read stream");
        match got.payload {
            GetResultPayload::Stream(mut stream) => while (stream.next().await).is_some() {},
            v => panic!("not a stream: {v:?}"),
        }

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "get"), ("result", "success")],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [("op", "get"), ("result", "success")],
        );
    }

    // Ensures the stream decorator correctly records the wall-clock time taken
    // for the caller to consume all the streamed data, and incrementally tracks
    // the number of bytes observed.
    #[tokio::test]
    async fn test_stream_decorator() {
        let inner = stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = SystemProvider::default();

        let metrics = Arc::new(metric::Registry::default());
        let m = MetricsWithBytes::new(&metrics, "test");

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            BytesStreamDelegate::new(m.clone()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            1,
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        const SLEEP: Duration = Duration::from_millis(20);
        tokio::time::sleep(SLEEP).await;

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 3);
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            4,
        );

        let success_hist = &m.inner.success_duration;

        // Until the stream is fully consumed, there should be no wall clock
        // metrics emitted.
        assert!(!success_hist.fetch().buckets.iter().any(|b| b.count > 0));

        // The stream should complete and cause metrics to be emitted.
        assert!(stream.next().await.is_none());

        // Now the stream is complete, the wall clock duration must have been
        // recorded.
        let hit_count = success_hist.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            4,
        );

        // And it must be in a SLEEP or higher bucket.
        let hit_count: u64 = success_hist
            .fetch()
            .buckets
            .iter()
            .skip_while(|b| b.le < SLEEP) // Skip buckets less than the sleep duration
            .map(|v| v.count)
            .sum();
        assert_eq!(
            hit_count, 1,
            "wall clock duration not recorded in correct bucket"
        );

        // Metrics must not be duplicated when the decorator is dropped
        drop(stream);
        let hit_count = success_hist.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration duplicated");
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            4,
        );
    }

    // Ensures the stream decorator correctly records the wall clock duration
    // and consumed byte count for a partially drained stream that is then
    // dropped.
    #[tokio::test]
    async fn test_stream_decorator_drop_incomplete() {
        let inner = stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = SystemProvider::default();

        let metrics = Arc::new(metric::Registry::default());
        let m = MetricsWithBytes::new(&metrics, "test");

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            BytesStreamDelegate::new(m.clone()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            1,
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        const SLEEP: Duration = Duration::from_millis(20);
        tokio::time::sleep(SLEEP).await;

        // Drop the stream without consuming the rest of the data.
        drop(stream);

        // Now the stream is complete, the wall clock duration must have been
        // recorded.
        let hit_count = m.inner.success_duration.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match the pre-drop value.
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            1,
        );
    }

    // Ensures the stream decorator records the wall clock duration into the
    // "error" histogram after the stream is dropped after emitting an error.
    #[tokio::test]
    async fn test_stream_decorator_transient_error_dropped() {
        let inner = stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Err(Error::new(ErrorKind::Other, "oh no!")),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = SystemProvider::default();

        let metrics = Arc::new(metric::Registry::default());
        let m = MetricsWithBytes::new(&metrics, "test");

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            BytesStreamDelegate::new(m.clone()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            1,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "error")],
            0,
        );

        let _err = stream
            .next()
            .await
            .expect("should yield an error")
            .expect_err("error configured in underlying stream");

        // Drop after observing an error
        drop(stream);

        // Ensure the wall clock was added to the "error" histogram.
        let hit_count = m.inner.error_duration.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            1,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "error")],
            0,
        );
    }

    // Ensures the stream decorator records the wall clock duration into the
    // "success" histogram after the stream progresses past a transient error.
    #[tokio::test]
    async fn test_stream_decorator_transient_error_progressed() {
        let inner = stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Err(Error::new(ErrorKind::Other, "oh no!")),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = SystemProvider::default();

        let metrics = Arc::new(metric::Registry::default());
        let m = MetricsWithBytes::new(&metrics, "test");

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            BytesStreamDelegate::new(m.clone()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            1,
        );

        let _err = stream
            .next()
            .await
            .expect("should yield an error")
            .expect_err("error configured in underlying stream");

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 3);
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            4,
        );

        // Drop after observing an error
        drop(stream);

        // Ensure the wall clock was added to the "success" histogram after
        // progressing past the transient error.
        let hit_count = m.inner.success_duration.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            4,
        );
    }

    // Ensures the wall clock time recorded by the stream decorator includes the
    // initial get even if never polled.
    #[tokio::test]
    async fn test_stream_immediate_drop() {
        let inner = stream::iter(
            [Ok(Bytes::copy_from_slice(&[1]))]
                .into_iter()
                .collect::<Vec<Result<Bytes, std::io::Error>>>(),
        );

        let time_provider = SystemProvider::default();

        let metrics = Arc::new(metric::Registry::default());
        let m = MetricsWithBytes::new(&metrics, "test");

        let stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            BytesStreamDelegate::new(m.clone()),
        );

        // Drop immediately
        drop(stream);

        // Ensure the wall clock was added to the "success" histogram
        let hit_count = m.inner.success_duration.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            0,
        );
    }

    // Ensures the wall clock time recorded by the stream decorator emits a wall
    // clock duration even if it never yields any data.
    #[tokio::test]
    async fn test_stream_empty() {
        let inner = stream::iter(
            [].into_iter()
                .collect::<Vec<Result<Bytes, std::io::Error>>>(),
        );

        let time_provider = SystemProvider::default();

        let metrics = Arc::new(metric::Registry::default());
        let m = MetricsWithBytes::new(&metrics, "test");

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            BytesStreamDelegate::new(m.clone()),
        );

        assert!(stream.next().await.is_none());

        // Ensure the wall clock was added to the "success" histogram even
        // though it yielded no data.
        let hit_count = m.inner.success_duration.fetch().sample_count();
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [("op", "test"), ("result", "success")],
            0,
        );
    }
}
