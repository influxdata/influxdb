use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::Stream;
use iox_time::{Time, TimeProvider};
use pin_project::{pin_project, pinned_drop};

use object_store::Result;

use crate::metrics::{MetricsWithBytesAndTtfbRecorder, MetricsWithCountRecorder, OpResult};

/// A [`MetricDelegate`] is called whenever the [`StreamMetricRecorder`]
/// observes an `Ok(Item)` in the stream.
pub(crate) trait MetricDelegate {
    /// The type this delegate observes.
    type Item;

    /// Invoked when the stream yields an `Ok(Item)`.
    fn observe_ok(&mut self, value: &Self::Item);

    /// Finish stream.
    fn finish(&mut self, op_res: OpResult);
}

/// A [`MetricDelegate`] for instrumented streams of [`Bytes`].
///
/// This impl is used to record the number of bytes yielded for
/// [`ObjectStore::get`] calls.
///
///
/// [`ObjectStore::get`]: object_store::ObjectStore::get
#[derive(Debug)]
pub(crate) struct BytesStreamDelegate {
    recorder: MetricsWithBytesAndTtfbRecorder,
    headers: Time,
    first_byte: Option<Time>,
    sum_bytes: usize,
}

impl BytesStreamDelegate {
    pub(crate) fn new(recorder: MetricsWithBytesAndTtfbRecorder, headers: Time) -> Self {
        Self {
            recorder,
            headers,
            first_byte: None,
            sum_bytes: 0,
        }
    }
}

impl MetricDelegate for BytesStreamDelegate {
    type Item = Bytes;

    fn observe_ok(&mut self, bytes: &Self::Item) {
        if self.first_byte.is_none() {
            self.first_byte = Some(self.recorder.time_provider().now());
        }
        self.sum_bytes += bytes.len();
    }

    fn finish(&mut self, op_res: OpResult) {
        let first_byte = self.first_byte.unwrap_or_else(|| self.recorder.freeze());
        self.recorder.submit(
            Some(self.headers),
            Some(first_byte),
            op_res,
            Some(self.sum_bytes as u64),
        );
    }
}

#[derive(Debug)]
pub(crate) struct CountStreamDelegate<T> {
    recorder: MetricsWithCountRecorder,
    element_phantom: PhantomData<T>,
    elements: usize,
}

impl<T> CountStreamDelegate<T> {
    pub(crate) fn new(recorder: MetricsWithCountRecorder) -> Self {
        Self {
            recorder,
            element_phantom: Default::default(),
            elements: 0,
        }
    }
}

impl<T> MetricDelegate for CountStreamDelegate<T> {
    type Item = T;

    fn observe_ok(&mut self, _value: &Self::Item) {
        self.elements += 1;
    }

    fn finish(&mut self, op_res: OpResult) {
        self.recorder.submit(op_res, Some(self.elements as u64));
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
pub(crate) struct StreamMetricRecorder<S, D>
where
    D: MetricDelegate,
{
    #[pin]
    inner: S,

    // The error state of the last poll - true if OK, false if an error
    // occurred.
    //
    // This is used to select the correct success/error histogram which records
    // the operation duration.
    last_call_ok: bool,

    /// Called when the stream yields an `Ok(T)` to allow the delegate to inspect the `T`.
    ///
    /// Also called at the end. Will be `None` aftewards.
    metric_delegate: Option<D>,
}

impl<S, D> StreamMetricRecorder<S, D>
where
    S: Stream,
    D: MetricDelegate,
{
    pub(crate) fn new(stream: S, metric_delegate: D) -> Self {
        Self {
            inner: stream,

            // Acquiring the stream was successful, even if the data was never
            // read.
            last_call_ok: true,

            metric_delegate: Some(metric_delegate),
        }
    }
}

impl<S, T, D, E> Stream for StreamMetricRecorder<S, D>
where
    S: Stream<Item = Result<T, E>>,
    D: MetricDelegate<Item = T>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = this.inner.poll_next(cx);

        match res {
            Poll::Ready(Some(Ok(value))) => {
                *this.last_call_ok = true;

                // Allow the pluggable metric delegate to record the value of T
                if let Some(metric_delegate) = this.metric_delegate.as_mut() {
                    metric_delegate.observe_ok(&value);
                }

                Poll::Ready(Some(Ok(value)))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.last_call_ok = false;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                if let Some(mut metric_delegate) = this.metric_delegate.take() {
                    // The stream has terminated - record the wall clock duration
                    // immediately.
                    let op_res = match *this.last_call_ok {
                        true => OpResult::Success,
                        false => OpResult::Error,
                    };

                    metric_delegate.finish(op_res);
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
impl<S, D> PinnedDrop for StreamMetricRecorder<S, D>
where
    D: MetricDelegate,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();

        // Only emit metrics if the end of the stream was not observed
        if let Some(mut metric_delegate) = this.metric_delegate.take() {
            let op_res = match *this.last_call_ok {
                // no error so far => treat as "canceled"
                true => OpResult::Canceled,
                // last operation was an error => treat as "error" (because canceling after the first error is reasonable)
                false => OpResult::Error,
            };
            metric_delegate.finish(op_res);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::StreamExt;
    use iox_time::{MockProvider, SystemProvider};

    use crate::{
        LogContext, MetricsWithBytesAndTtfb, StoreType,
        test_utils::{
            assert_counter_value, assert_histogram_hit, assert_histogram_not_hit,
            assert_histogram_total,
        },
    };

    use super::*;

    // Ensures the stream decorator correctly records the wall-clock time taken
    // for the caller to consume all the streamed data, and incrementally tracks
    // the number of bytes observed.
    #[tokio::test]
    async fn test_stream_decorator() {
        let inner = futures::stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));

        let metrics = metric::Registry::default();
        let m = Arc::new(MetricsWithBytesAndTtfb::new(
            &metrics,
            &StoreType("bananas".into()),
            "test",
            &None,
        ));
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&m),
            &(Arc::clone(&time_provider) as _),
            LogContext::default(),
            &None,
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        const SLEEP: Duration = Duration::from_millis(20);
        time_provider.inc(SLEEP);

        let mut stream = StreamMetricRecorder::new(
            inner,
            BytesStreamDelegate::new(recorder, time_provider.now()),
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        time_provider.inc(SLEEP);

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);

        // data NOT accounted before drop
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            0,
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        time_provider.inc(SLEEP);

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 3);

        // data NOT accounted before drop
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            0,
        );

        // Until the stream is fully consumed, there should be no wall clock
        // metrics emitted.
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );

        // The stream should complete and cause metrics to be emitted.
        assert!(stream.next().await.is_none());

        // Now the stream is complete, the wall clock duration must have been
        // recorded.
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            4,
        );

        // Wall clock duration it must be in a total SLEEP.
        assert_histogram_total(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            SLEEP * 3,
        );

        // headers after first sleep
        assert_histogram_total(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            SLEEP,
        );

        // TTFB after second sleep
        assert_histogram_total(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            SLEEP * 2,
        );

        // Metrics must not be duplicated when the decorator is dropped
        drop(stream);
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            4,
        );
    }

    // Ensures the stream decorator correctly records the wall clock duration
    // and consumed byte count for a partially drained stream that is then
    // dropped.
    #[tokio::test]
    async fn test_stream_decorator_drop_incomplete() {
        let inner = futures::stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));

        let metrics = metric::Registry::default();
        let m = Arc::new(MetricsWithBytesAndTtfb::new(
            &metrics,
            &StoreType("bananas".into()),
            "test",
            &None,
        ));
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&m),
            &(Arc::clone(&time_provider) as _),
            LogContext::default(),
            &None,
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        const SLEEP: Duration = Duration::from_millis(20);
        time_provider.inc(SLEEP);

        let mut stream = StreamMetricRecorder::new(
            inner,
            BytesStreamDelegate::new(recorder, time_provider.now()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);

        // data NOT accounted before drop
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
            0,
        );

        // Sleep at least 10ms to assert the recorder to captures the wall clock
        // time.
        time_provider.inc(SLEEP);

        // Drop the stream without consuming the rest of the data.
        drop(stream);

        // Now the stream is complete, the wall clock duration must have been
        // recorded.
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );

        // And the number of bytes read must match the pre-drop value.
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
            1,
        );
    }

    // Ensures the stream decorator records the wall clock duration into the
    // "error" histogram after the stream is dropped after emitting an error.
    #[tokio::test]
    async fn test_stream_decorator_transient_error_dropped() {
        let inner = futures::stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Err(std::io::Error::other("oh no!")),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = Arc::new(SystemProvider::default());

        let metrics = metric::Registry::default();
        let m = Arc::new(MetricsWithBytesAndTtfb::new(
            &metrics,
            &StoreType("bananas".into()),
            "test",
            &None,
        ));
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&m),
            &(Arc::clone(&time_provider) as _),
            LogContext::default(),
            &None,
        );

        let mut stream = StreamMetricRecorder::new(
            inner,
            BytesStreamDelegate::new(recorder, time_provider.now()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);

        // data NOT accounted before drop
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            0,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "error"),
            ],
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
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "error"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "error"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "error"),
            ],
        );

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            0,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "error"),
            ],
            1,
        );
    }

    // Ensures the stream decorator records the wall clock duration into the
    // "canceled" histogram after the stream progresses past a transient error.
    #[tokio::test]
    async fn test_stream_decorator_transient_error_progressed() {
        let inner = futures::stream::iter(
            [
                Ok(Bytes::copy_from_slice(&[1])),
                Err(std::io::Error::other("oh no!")),
                Ok(Bytes::copy_from_slice(&[2, 3, 4])),
            ]
            .into_iter()
            .collect::<Vec<Result<_, std::io::Error>>>(),
        );

        let time_provider = Arc::new(SystemProvider::default());

        let metrics = metric::Registry::default();
        let m = Arc::new(MetricsWithBytesAndTtfb::new(
            &metrics,
            &StoreType("bananas".into()),
            "test",
            &None,
        ));
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&m),
            &(Arc::clone(&time_provider) as _),
            LogContext::default(),
            &None,
        );

        let mut stream = StreamMetricRecorder::new(
            inner,
            BytesStreamDelegate::new(recorder, time_provider.now()),
        );

        let got = stream
            .next()
            .await
            .expect("should yield data")
            .expect("should succeed");
        assert_eq!(got.len(), 1);

        // data NOT accounted before drop
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
            0,
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

        // data NOT accounted before drop
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
            0,
        );

        // Drop after observing an error
        drop(stream);

        // Ensure the wall clock was added to the "success" histogram after
        // progressing past the transient error.
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
            4,
        );
    }

    // Ensures the wall clock time recorded by the stream decorator includes the
    // initial get even if never polled.
    #[tokio::test]
    async fn test_stream_immediate_drop() {
        let inner = futures::stream::iter(
            [Ok(Bytes::copy_from_slice(&[1]))]
                .into_iter()
                .collect::<Vec<Result<Bytes, std::io::Error>>>(),
        );

        let time_provider = Arc::new(SystemProvider::default());

        let metrics = metric::Registry::default();
        let m = Arc::new(MetricsWithBytesAndTtfb::new(
            &metrics,
            &StoreType("bananas".into()),
            "test",
            &None,
        ));
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&m),
            &(Arc::clone(&time_provider) as _),
            LogContext::default(),
            &None,
        );

        let stream = StreamMetricRecorder::new(
            inner,
            BytesStreamDelegate::new(recorder, time_provider.now()),
        );

        // Drop immediately
        drop(stream);

        // Ensure the wall clock was added to the "success" histogram
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
        );

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "canceled"),
            ],
            0,
        );
    }

    // Ensures the wall clock time recorded by the stream decorator emits a wall
    // clock duration even if it never yields any data.
    #[tokio::test]
    async fn test_stream_empty() {
        let inner = futures::stream::iter(
            [].into_iter()
                .collect::<Vec<Result<Bytes, std::io::Error>>>(),
        );

        let time_provider = Arc::new(SystemProvider::default());

        let metrics = metric::Registry::default();
        let m = Arc::new(MetricsWithBytesAndTtfb::new(
            &metrics,
            &StoreType("bananas".into()),
            "test",
            &None,
        ));
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&m),
            &(Arc::clone(&time_provider) as _),
            LogContext::default(),
            &None,
        );

        let mut stream = StreamMetricRecorder::new(
            inner,
            BytesStreamDelegate::new(recorder, time_provider.now()),
        );

        assert!(stream.next().await.is_none());

        // Ensure the wall clock was added to the "success" histogram even
        // though it yielded no data.
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
        );

        // And the number of bytes read must match
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "test"),
                ("result", "success"),
            ],
            0,
        );
    }
}
