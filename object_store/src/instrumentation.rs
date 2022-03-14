//! A metric instrumentation wrapper over [`ObjectStoreApi`] implementations.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, Stream, StreamExt};
use metric::{Metric, U64Counter, U64Histogram, U64HistogramOptions};
use pin_project::{pin_project, pinned_drop};
use time::{SystemProvider, Time, TimeProvider};

use crate::{GetResult, ListResult, ObjectStoreApi};

/// An instrumentation decorator, wrapping an underlying [`ObjectStoreApi`]
/// implementation and recording bytes transferred and call latency.
///
/// # Stream Duration
///
/// The [`ObjectStoreApi::get()`] call can return a [`Stream`] which is polled
/// by the caller and may yield chunks of a file over a series of polls (as
/// opposed to all of the file data in one go). Because the caller drives the
/// polling and therefore fetching of data from the object store over the
/// lifetime of the [`Stream`], the duration of a [`ObjectStoreApi::get()`]
/// request is measured to be the wall clock difference between the moment the
/// caller executes the [`ObjectStoreApi::get()`] call, up until the last chunk
/// of data is yielded to the caller.
///
/// This means the duration metrics measuring consumption of returned streams
/// are recording the rate at which the application reads the data, as opposed
/// to the duration of time taken to fetch that data.
///
/// # Stream Errors
///
/// The [`ObjectStoreApi::get()`] method can return a [`Stream`] of [`Result`]
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
pub struct ObjectStoreMetrics<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    put_success_duration_ms: U64Histogram,
    put_error_duration_ms: U64Histogram,
    put_bytes: U64Counter,

    get_success_duration_ms: U64Histogram,
    get_error_duration_ms: U64Histogram,
    get_bytes: U64Counter,

    delete_success_duration_ms: U64Histogram,
    delete_error_duration_ms: U64Histogram,

    list_success_duration_ms: U64Histogram,
    list_error_duration_ms: U64Histogram,
}

impl<T> ObjectStoreMetrics<T> {
    /// Instrument `T`, pushing to `registry`.
    pub fn new(inner: T, registry: &metric::Registry) -> Self {
        let buckets = || {
            U64HistogramOptions::new([
                5,
                10,
                20,
                40,
                80,
                160,
                320,
                640,
                1280,
                2560,
                5120,
                10240,
                20480,
                u64::MAX,
            ])
        };

        // Byte counts up/down
        let bytes = registry.register_metric::<U64Counter>(
            "object_store_transfer_bytes",
            "cumulative count of file content bytes transferred to/from the object store",
        );
        let put_bytes = bytes.recorder(&[("op", "put")]);
        let get_bytes = bytes.recorder(&[("op", "get")]);

        // Call durations broken down by op & result
        let duration: Metric<U64Histogram> = registry.register_metric_with_options(
            "object_store_op_duration_ms",
            "object store operation duration in milliseconds",
            buckets,
        );
        let put_success_duration = duration.recorder(&[("op", "put"), ("result", "success")]);
        let put_error_duration = duration.recorder(&[("op", "put"), ("result", "error")]);
        let get_success_duration = duration.recorder(&[("op", "get"), ("result", "success")]);
        let get_error_duration = duration.recorder(&[("op", "get"), ("result", "error")]);
        let delete_success_duration = duration.recorder(&[("op", "delete"), ("result", "success")]);
        let delete_error_duration = duration.recorder(&[("op", "delete"), ("result", "error")]);
        let list_success_duration = duration.recorder(&[("op", "list"), ("result", "success")]);
        let list_error_duration = duration.recorder(&[("op", "list"), ("result", "error")]);

        Self {
            inner,
            time_provider: Default::default(),

            put_success_duration_ms: put_success_duration,
            put_error_duration_ms: put_error_duration,
            put_bytes,

            get_bytes,
            get_success_duration_ms: get_success_duration,
            get_error_duration_ms: get_error_duration,

            delete_success_duration_ms: delete_success_duration,
            delete_error_duration_ms: delete_error_duration,

            list_success_duration_ms: list_success_duration,
            list_error_duration_ms: list_error_duration,
        }
    }
}

#[async_trait]
impl<T, P> ObjectStoreApi for ObjectStoreMetrics<T, P>
where
    T: ObjectStoreApi,
    P: TimeProvider,
{
    type Path = T::Path;
    type Error = T::Error;

    fn new_path(&self) -> Self::Path {
        self.inner.new_path()
    }

    fn path_from_raw(&self, raw: &str) -> Self::Path {
        self.inner.path_from_raw(raw)
    }

    async fn put(&self, location: &Self::Path, bytes: Bytes) -> Result<(), Self::Error> {
        let t = self.time_provider.now();

        let size = bytes.len();
        let res = self.inner.put(location, bytes).await;
        self.put_bytes.inc(size as _);

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.put_success_duration_ms.record(delta.as_millis() as _),
                Err(_) => self.put_error_duration_ms.record(delta.as_millis() as _),
            };
        }

        res
    }

    async fn get(&self, location: &Self::Path) -> Result<GetResult<Self::Error>, Self::Error> {
        let started_at = self.time_provider.now();

        let res = self.inner.get(location).await;

        match res {
            Ok(GetResult::File(file, path)) => {
                // Record the file size in bytes and time the inner call took.
                if let Ok(m) = file.metadata().await {
                    self.get_bytes.inc(m.len());
                    if let Some(d) = self.time_provider.now().checked_duration_since(started_at) {
                        self.get_success_duration_ms.record(d.as_millis() as _)
                    }
                }
                Ok(GetResult::File(file, path))
            }
            Ok(GetResult::Stream(s)) => {
                // Wrap the object store data stream in a decorator to track the
                // yielded data / wall clock, inclusive of the inner call above.
                Ok(GetResult::Stream(Box::pin(Box::new(
                    StreamMetricRecorder::new(
                        s,
                        started_at,
                        self.get_success_duration_ms.clone(),
                        self.get_error_duration_ms.clone(),
                        self.get_bytes.clone(),
                    )
                    .fuse(),
                ))))
            }
            Err(e) => {
                // Record the call duration in the error histogram.
                if let Some(delta) = self.time_provider.now().checked_duration_since(started_at) {
                    self.get_error_duration_ms.record(delta.as_millis() as _);
                }
                Err(e)
            }
        }
    }

    async fn delete(&self, location: &Self::Path) -> Result<(), Self::Error> {
        let t = self.time_provider.now();

        let res = self.inner.delete(location).await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self
                    .delete_success_duration_ms
                    .record(delta.as_millis() as _),
                Err(_) => self.delete_error_duration_ms.record(delta.as_millis() as _),
            };
        }

        res
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>, Self::Error>>, Self::Error> {
        let t = self.time_provider.now();

        let res = self.inner.list(prefix).await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.list_success_duration_ms.record(delta.as_millis() as _),
                Err(_) => self.list_error_duration_ms.record(delta.as_millis() as _),
            };
        }

        res
    }

    async fn list_with_delimiter(
        &self,
        prefix: &Self::Path,
    ) -> Result<ListResult<Self::Path>, Self::Error> {
        let t = self.time_provider.now();

        let res = self.inner.list_with_delimiter(prefix).await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.list_success_duration_ms.record(delta.as_millis() as _),
                Err(_) => self.list_error_duration_ms.record(delta.as_millis() as _),
            };
        }

        res
    }
}

/// [`StreamMetricRecorder`] decorates an underlying [`Stream`] for "get"
/// catalog operations, recording the wall clock duration and number of bytes
/// read over the lifetime of the stream.
///
/// The bytes read counter is incremented each time [`Self::poll_next()`] yields
/// a buffer, and once the [`StreamMetricRecorder`] is read to completion
/// (specifically, until it yields `Poll::Ready(None)`), or when it is dropped
/// (whichever is sooner) the decorator emits the wall clock measurement into
/// the relevant histogram, bucketed by operation result.
///
/// A stream may return a transient error when polled, and later successfully
/// emit all data in subsequent polls - therefore the duration is logged as an
/// error only if the last poll performed by the caller returned an error.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
struct StreamMetricRecorder<S, P = SystemProvider>
where
    P: TimeProvider,
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

    get_success_duration: U64Histogram,
    get_error_duration: U64Histogram,
    get_bytes: U64Counter,
}

impl<S> StreamMetricRecorder<S>
where
    S: Stream,
{
    fn new(
        stream: S,
        started_at: Time,
        get_success_duration: U64Histogram,
        get_error_duration: U64Histogram,
        get_bytes: U64Counter,
    ) -> Self {
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

            get_success_duration,
            get_error_duration,
            get_bytes,
        }
    }
}

impl<S, P, E> Stream for StreamMetricRecorder<S, P>
where
    S: Stream<Item = Result<Bytes, E>>,
    P: TimeProvider,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = this.inner.poll_next(cx);

        match res {
            Poll::Ready(Some(Ok(bytes))) => {
                *this.last_call_ok = true;
                *this.last_yielded_at.as_mut().unwrap() = this.time_provider.now();
                this.get_bytes.inc(bytes.len() as _);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.last_call_ok = false;
                *this.last_yielded_at.as_mut().unwrap() = this.time_provider.now();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // The stream has terminated - record the wall clock duration
                // immediately.
                let hist = match this.last_call_ok {
                    true => this.get_success_duration,
                    false => this.get_error_duration,
                };

                // Take the last_yielded_at option, marking metrics as emitted
                // so the drop impl does not duplicate them.
                if let Some(d) = this
                    .last_yielded_at
                    .take()
                    .expect("no last_yielded_at value for fused stream")
                    .checked_duration_since(*this.started_at)
                {
                    hist.record(d.as_millis() as _)
                }

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
impl<S, P> PinnedDrop for StreamMetricRecorder<S, P>
where
    P: TimeProvider,
{
    fn drop(self: Pin<&mut Self>) {
        // Only emit metrics if the end of the stream was not observed (and
        // therefore last_yielded_at is still Some).
        if let Some(last) = self.last_yielded_at {
            let hist = match self.last_call_ok {
                true => &self.get_success_duration,
                false => &self.get_error_duration,
            };

            if let Some(d) = last.checked_duration_since(self.started_at) {
                hist.record(d.as_millis() as _)
            }
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

    use futures::stream;
    use metric::Attributes;
    use tokio::io::AsyncReadExt;

    use crate::{dummy, ObjectStore};

    use super::*;

    fn assert_histogram_hit<const N: usize>(
        metrics: &metric::Registry,
        name: &'static str,
        attr: [(&'static str, &'static str); N],
    ) {
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>(name)
            .expect("failed to read histogram")
            .get_observer(&Attributes::from(&attr))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.buckets.iter().fold(0, |acc, v| acc + v.count);
        assert!(hit_count > 0, "metric {} did not record any calls", name);
    }

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
        let store = ObjectStore::new_in_memory();
        let store = ObjectStoreMetrics::new(store, &metrics);

        store
            .put(
                &store.path_from_raw("test"),
                Bytes::from([42_u8, 42, 42, 42, 42].as_slice()),
            )
            .await
            .expect("put should succeed");

        assert_counter_value(&metrics, "object_store_transfer_bytes", [("op", "put")], 5);
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "put"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_put_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = dummy::new_failing_s3().expect("cannot init failing store");
        let store = ObjectStoreMetrics::new(store, &metrics);

        store
            .put(
                &store.path_from_raw("test"),
                Bytes::from([42_u8, 42, 42, 42, 42].as_slice()),
            )
            .await
            .expect_err("put should error");

        assert_counter_value(&metrics, "object_store_transfer_bytes", [("op", "put")], 5);
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "put"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_list() {
        let metrics = Arc::new(metric::Registry::default());
        let store = ObjectStore::new_in_memory();
        let store = ObjectStoreMetrics::new(store, &metrics);

        store.list(None).await.expect("list should succeed");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "list"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_list_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = dummy::new_failing_s3().expect("cannot init failing store");
        let store = ObjectStoreMetrics::new(store, &metrics);

        assert!(store.list(None).await.is_err(), "mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "list"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let metrics = Arc::new(metric::Registry::default());
        let store = ObjectStore::new_in_memory();
        let store = ObjectStoreMetrics::new(store, &metrics);

        store
            .list_with_delimiter(&store.path_from_raw("test"))
            .await
            .expect("list should succeed");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "list"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_list_with_delimiter_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = dummy::new_failing_s3().expect("cannot init failing store");
        let store = ObjectStoreMetrics::new(store, &metrics);

        assert!(
            store
                .list_with_delimiter(&store.path_from_raw("test"))
                .await
                .is_err(),
            "mock configured to fail"
        );

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "list"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_get_fails() {
        let metrics = Arc::new(metric::Registry::default());
        let store = dummy::new_failing_s3().expect("cannot init failing store");
        let store = ObjectStoreMetrics::new(store, &metrics);

        store
            .get(&store.path_from_raw("test"))
            .await
            .expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "get"), ("result", "error")],
        );
    }

    #[tokio::test]
    async fn test_put_get_delete_file() {
        let metrics = Arc::new(metric::Registry::default());
        let store = ObjectStore::new_file("./");
        let store = ObjectStoreMetrics::new(store, &metrics);

        let data = [42_u8, 42, 42, 42, 42];
        let path = store.path_from_raw("test");
        store
            .put(&path, Bytes::copy_from_slice(&data))
            .await
            .expect("put should succeed");

        let got = store.get(&path).await.expect("should read file");
        match got {
            GetResult::File(mut file, _) => {
                let mut contents = vec![];
                file.read_to_end(&mut contents)
                    .await
                    .expect("failed to read file data");
                assert_eq!(contents, &data);
            }
            v => panic!("not a file: {:?}", v),
        }

        assert_counter_value(&metrics, "object_store_transfer_bytes", [("op", "get")], 5);
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "get"), ("result", "success")],
        );

        store
            .delete(&path)
            .await
            .expect("should clean up test file");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
            [("op", "delete"), ("result", "success")],
        );
    }

    #[tokio::test]
    async fn test_get_stream() {
        let metrics = Arc::new(metric::Registry::default());
        let store = ObjectStore::new_in_memory();
        let store = ObjectStoreMetrics::new(store, &metrics);

        let data = [42_u8, 42, 42, 42, 42];
        let path = store.path_from_raw("test");
        store
            .put(&path, Bytes::copy_from_slice(&data))
            .await
            .expect("put should succeed");

        let got = store.get(&path).await.expect("should read stream");
        match got {
            GetResult::Stream(mut stream) => while (stream.next().await).is_some() {},
            v => panic!("not a stream: {:?}", v),
        }

        assert_counter_value(&metrics, "object_store_transfer_bytes", [("op", "get")], 5);
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration_ms",
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
        let hist: Metric<U64Histogram> =
            metrics.register_metric_with_options("wall_clock", "", || {
                U64HistogramOptions::new([1, 100, u64::MAX])
            });

        let bytes = metrics
            .register_metric::<U64Counter>(
                "object_store_transfer_bytes",
                "cumulative count of file content bytes transferred to/from the object store",
            )
            .recorder(&[]);

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            hist.recorder(&[("result", "success")]),
            hist.recorder(&[("result", "error")]),
            bytes,
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 1);

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
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 4);

        let success_hist = hist
            .get_observer(&metric::Attributes::from(&[("result", "success")]))
            .expect("failed to get observer");

        // Until the stream is fully consumed, there should be no wall clock
        // metrics emitted.
        assert!(!success_hist.fetch().buckets.iter().any(|b| b.count > 0));

        // The stream should complete and cause metrics to be emitted.
        assert!(stream.next().await.is_none());

        // Now the stream is complete, the wall clock duration must have been
        // recorded.
        let hit_count = success_hist
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 4);

        // And it must be in a SLEEP or higher bucket.
        let hit_count = success_hist
            .fetch()
            .buckets
            .iter()
            .skip_while(|b| b.le < SLEEP.as_millis() as _) // Skip buckets less than the sleep duration
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(
            hit_count, 1,
            "wall clock duration not recorded in correct bucket"
        );

        // Metrics must not be duplicated when the decorator is dropped
        drop(stream);
        let hit_count = success_hist
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration duplicated");
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 4);
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
        let hist: Metric<U64Histogram> =
            metrics.register_metric_with_options("wall_clock", "", || {
                U64HistogramOptions::new([1, 100, u64::MAX])
            });

        let bytes = metrics
            .register_metric::<U64Counter>(
                "object_store_transfer_bytes",
                "cumulative count of file content bytes transferred to/from the object store",
            )
            .recorder(&[]);

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            hist.recorder(&[("result", "success")]),
            hist.recorder(&[("result", "error")]),
            bytes,
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 1);

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        const SLEEP: Duration = Duration::from_millis(20);
        tokio::time::sleep(SLEEP).await;

        // Drop the stream without consuming the rest of the data.
        drop(stream);

        // Now the stream is complete, the wall clock duration must have been
        // recorded.
        let hit_count = hist
            .get_observer(&metric::Attributes::from(&[("result", "success")]))
            .expect("failed to get observer")
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match the pre-drop value.
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 1);
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
        let hist: Metric<U64Histogram> =
            metrics.register_metric_with_options("wall_clock", "", || {
                U64HistogramOptions::new([1, 100, u64::MAX])
            });

        let bytes = metrics
            .register_metric::<U64Counter>(
                "object_store_transfer_bytes",
                "cumulative count of file content bytes transferred to/from the object store",
            )
            .recorder(&[]);

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            hist.recorder(&[("result", "success")]),
            hist.recorder(&[("result", "error")]),
            bytes,
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 1);

        let _err = stream
            .next()
            .await
            .expect("should yield an error")
            .expect_err("error configured in underlying stream");

        // Drop after observing an error
        drop(stream);

        // Ensure the wall clock was added to the "error" histogram.
        let hit_count = hist
            .get_observer(&metric::Attributes::from(&[("result", "error")]))
            .expect("failed to get observer")
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 1);
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
        let hist: Metric<U64Histogram> =
            metrics.register_metric_with_options("wall_clock", "", || {
                U64HistogramOptions::new([1, 100, u64::MAX])
            });

        let bytes = metrics
            .register_metric::<U64Counter>(
                "object_store_transfer_bytes",
                "cumulative count of file content bytes transferred to/from the object store",
            )
            .recorder(&[]);

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            hist.recorder(&[("result", "success")]),
            hist.recorder(&[("result", "error")]),
            bytes,
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 1);

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
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 4);

        // Drop after observing an error
        drop(stream);

        // Ensure the wall clock was added to the "success" histogram after
        // progressing past the transient error.
        let hit_count = hist
            .get_observer(&metric::Attributes::from(&[("result", "success")]))
            .expect("failed to get observer")
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 4);
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
        let hist: Metric<U64Histogram> =
            metrics.register_metric_with_options("wall_clock", "", || {
                U64HistogramOptions::new([1, 100, u64::MAX])
            });

        let bytes = metrics
            .register_metric::<U64Counter>(
                "object_store_transfer_bytes",
                "cumulative count of file content bytes transferred to/from the object store",
            )
            .recorder(&[]);

        let stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            hist.recorder(&[("result", "success")]),
            hist.recorder(&[("result", "error")]),
            bytes,
        );

        // Drop immediately
        drop(stream);

        // Ensure the wall clock was added to the "success" histogram
        let hit_count = hist
            .get_observer(&metric::Attributes::from(&[("result", "success")]))
            .expect("failed to get observer")
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 0);
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
        let hist: Metric<U64Histogram> =
            metrics.register_metric_with_options("wall_clock", "", || {
                U64HistogramOptions::new([1, 100, u64::MAX])
            });

        let bytes = metrics
            .register_metric::<U64Counter>(
                "object_store_transfer_bytes",
                "cumulative count of file content bytes transferred to/from the object store",
            )
            .recorder(&[]);

        let mut stream = StreamMetricRecorder::new(
            inner,
            time_provider.now(),
            hist.recorder(&[("result", "success")]),
            hist.recorder(&[("result", "error")]),
            bytes,
        );

        assert!(stream.next().await.is_none());

        // Ensure the wall clock was added to the "success" histogram even
        // though it yielded no data.
        let hit_count = hist
            .get_observer(&metric::Attributes::from(&[("result", "success")]))
            .expect("failed to get observer")
            .fetch()
            .buckets
            .iter()
            .fold(0, |acc, v| acc + v.count);
        assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

        // And the number of bytes read must match
        assert_counter_value(&metrics, "object_store_transfer_bytes", [], 0);
    }
}
