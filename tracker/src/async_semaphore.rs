//! Tooling to track/instrument [`tokio::sync::Semaphore`]s.
use std::{future::Future, marker::PhantomData, sync::Arc, task::Poll, time::Instant};

use futures::{future::BoxFuture, FutureExt};
use metric::{Attributes, DurationHistogram, MakeMetricObserver, U64Counter, U64Gauge};
use pin_project::{pin_project, pinned_drop};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub use tokio::sync::AcquireError;
use trace::span::{Span, SpanRecorder};

/// Metrics that can be used to create a [`InstrumentedAsyncSemaphore`].
#[derive(Debug)]
pub struct AsyncSemaphoreMetrics {
    permits_acquired: U64Gauge,
    permits_total: U64Gauge,
    permits_pending: U64Gauge,
    permits_cancelled_while_pending: U64Counter,
    holders_acquired: U64Gauge,
    holders_pending: U64Gauge,
    holders_cancelled_while_pending: U64Counter,
    acquire_duration: DurationHistogram,
}

impl AsyncSemaphoreMetrics {
    /// Create new metrics that are linked to the given registry and carry the given attributes.
    pub fn new(registry: &metric::Registry, attributes: impl Into<Attributes>) -> Self {
        let attributes: Attributes = attributes.into();

        let permits_acquired = registry
            .register_metric::<U64Gauge>(
                "iox_async_semaphore_permits_acquired",
                "Number of currently acquired permits",
            )
            .recorder(attributes.clone());
        let permits_total = registry
            .register_metric::<U64Gauge>(
                "iox_async_semaphore_permits_total",
                "Number of total permits",
            )
            .recorder(attributes.clone());
        let permits_pending = registry
            .register_metric::<U64Gauge>(
                "iox_async_semaphore_permits_pending",
                "Number of pending permits",
            )
            .recorder(attributes.clone());
        let permits_cancelled_while_pending = registry
            .register_metric::<U64Counter>(
                "iox_async_semaphore_permits_cancelled_while_pending",
                "Counter for permits that were cancelled while they were waiting for the semaphore.",
            )
            .recorder(attributes.clone());
        let holders_acquired = registry
            .register_metric::<U64Gauge>(
                "iox_async_semaphore_holders_acquired",
                "Number of currently acquired semaphore holders. Each holder might have multiple permits",
            )
            .recorder(attributes.clone());
        let holders_pending = registry
            .register_metric::<U64Gauge>(
                "iox_async_semaphore_holders_pending",
                "Number of pending semaphore holders. Each holder might have multiple permits",
            )
            .recorder(attributes.clone());
        let holders_cancelled_while_pending = registry
            .register_metric::<U64Counter>(
                "iox_async_semaphore_holders_cancelled_while_pending",
                "Number of pending semaphore holders that were cancelled while they were waiting for the semaphore. Each holder might have multiple permits",
            )
            .recorder(attributes.clone());
        let acquire_duration = registry
            .register_metric::<DurationHistogram>(
                "iox_async_semaphore_acquire_duration",
                "Duration it takes to acquire a semaphore",
            )
            .recorder(attributes);

        Self {
            permits_acquired,
            permits_total,
            permits_pending,
            permits_cancelled_while_pending,
            holders_acquired,
            holders_pending,
            holders_cancelled_while_pending,
            acquire_duration,
        }
    }

    /// Create metrics that are not associated with any registry.
    pub fn new_unregistered() -> Self {
        Self {
            permits_acquired: Default::default(),
            permits_total: Default::default(),
            permits_pending: Default::default(),
            permits_cancelled_while_pending: Default::default(),
            holders_acquired: Default::default(),
            holders_pending: Default::default(),
            holders_cancelled_while_pending: Default::default(),
            acquire_duration: DurationHistogram::create(&Default::default()),
        }
    }

    /// Create new instrumented semaphore.
    pub fn new_semaphore(self: &Arc<Self>, permits: usize) -> InstrumentedAsyncSemaphore {
        self.permits_total.inc(permits as u64);

        InstrumentedAsyncSemaphore {
            inner: Arc::new(Semaphore::new(permits)),
            permits,
            metrics: Arc::clone(self),
        }
    }
}

/// Instrumented version of [`tokio::sync::Semaphore`].
///
/// # Tracing
/// All `acquire*` methods take an optional span. This span will be exported as:
///
/// - **happy acquire path:** `<start>...<acquire event>...<end>` with OK status
/// - **acquire failure:** `<start>...<end>` with ERROR status
/// - **canceled during acquire:** `<start>...<end>` with UNKNOWN status
#[derive(Debug)]
pub struct InstrumentedAsyncSemaphore {
    /// Underlying semaphore implementation.
    ///
    /// This is wrapped into an [`Arc`] so we can use a single implementation for the owned and non-owned implementation.
    inner: Arc<Semaphore>,

    /// Number of total permits (acquired and available).
    permits: usize,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,
}

impl InstrumentedAsyncSemaphore {
    /// Acquire a single permit.
    ///
    /// See [`tokio::sync::Semaphore::acquire`] for details.
    pub async fn acquire(
        &self,
        span: Option<Span>,
    ) -> Result<InstrumentedAsyncSemaphorePermit<'_>, AcquireError> {
        self.acquire_many(1, span).await
    }

    /// Acquire `n` permits.
    ///
    /// See [`tokio::sync::Semaphore::acquire_many`] for details.
    pub async fn acquire_many(
        &self,
        n: u32,
        span: Option<Span>,
    ) -> Result<InstrumentedAsyncSemaphorePermit<'_>, AcquireError> {
        let owned_permit = self.acquire_impl(n, span).await?;
        Ok(InstrumentedAsyncSemaphorePermit {
            owned_permit,
            phantom: Default::default(),
        })
    }

    pub async fn acquire_owned(
        self: &Arc<Self>,
        span: Option<Span>,
    ) -> Result<InstrumentedAsyncOwnedSemaphorePermit, AcquireError> {
        // NOTE: We deliberately take `self: &Arc<Self>` here even though we strictly don't need it so we have use a
        //       single implementation for the owned and non-owned variant while still providing a comparable API to
        //       the ordinary tokio semaphore.
        self.acquire_impl(1, span).await
    }

    pub async fn acquire_many_owned(
        self: &Arc<Self>,
        n: u32,
        span: Option<Span>,
    ) -> Result<InstrumentedAsyncOwnedSemaphorePermit, AcquireError> {
        // NOTE: We deliberately take `self: &Arc<Self>` here even though we strictly don't need it so we have use a
        //       single implementation for the owned and non-owned variant while still providing a comparable API to
        //       the ordinary tokio semaphore.
        self.acquire_impl(n, span).await
    }

    fn acquire_impl(
        &self,
        n: u32,
        span: Option<Span>,
    ) -> impl Future<Output = Result<InstrumentedAsyncOwnedSemaphorePermit, AcquireError>> {
        InstrumentedAsyncSemaphoreAcquire {
            inner: Arc::clone(&self.inner).acquire_many_owned(n).boxed(),
            metrics: Arc::clone(&self.metrics),
            n,
            reported_pending: false,
            t_start: Instant::now(),
            span_recorder: Some(SpanRecorder::new(span)),
        }
    }

    /// return the total number of permits (available + already acquired).
    pub fn total_permits(self: &Arc<Self>) -> usize {
        self.permits
    }

    /// return the number of pending permits
    pub fn permits_pending(self: &Arc<Self>) -> u64 {
        self.metrics.permits_pending.fetch()
    }

    /// return the number of acquired permits
    pub fn permits_acquired(self: &Arc<Self>) -> u64 {
        self.metrics.permits_acquired.fetch()
    }

    /// return the number of pending holders
    pub fn holders_pending(self: &Arc<Self>) -> u64 {
        self.metrics.holders_pending.fetch()
    }

    /// return the number of acquired holders
    pub fn holders_acquired(self: &Arc<Self>) -> u64 {
        self.metrics.holders_acquired.fetch()
    }
}

impl Drop for InstrumentedAsyncSemaphore {
    fn drop(&mut self) {
        self.metrics.permits_total.dec(self.permits as u64);
    }
}

/// Future that wraps [`tokio::sync::Semaphore::acquire_many`] with metrics.
///
/// This type is private so we don't leak too many implementation details.
#[pin_project(PinnedDrop)]
struct InstrumentedAsyncSemaphoreAcquire<'a> {
    /// The actual `acquire_many` future.
    #[pin]
    inner: BoxFuture<'a, Result<OwnedSemaphorePermit, AcquireError>>,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,

    /// Number of requested permits.
    ///
    /// This was already passed to the `acquire_many` future but we need to store it separately for our metrics.
    n: u32,

    /// Flags if we already reported a "pending" state for this future.
    ///
    /// This is toggled back from `true` to `false` when we clear the "pending" metrics, e.g. when the future completes.
    reported_pending: bool,

    /// Start time of the "acquire" action.
    t_start: Instant,

    /// Span recorder for the entire semaphore interaction.
    ///
    /// Wrapped into an [`Option`] to allow the handover between the acquire-future and the permit.
    span_recorder: Option<SpanRecorder>,
}

impl<'a> std::fmt::Debug for InstrumentedAsyncSemaphoreAcquire<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedAsyncSemaphoreAcquire")
            .field("metrics", &self.metrics)
            .field("n", &self.n)
            .field("reported_pending", &self.reported_pending)
            .field("t_start", &self.t_start)
            .finish_non_exhaustive()
    }
}

impl<'a> Future for InstrumentedAsyncSemaphoreAcquire<'a> {
    type Output = Result<InstrumentedAsyncOwnedSemaphorePermit, AcquireError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Ready(res) => match res {
                Ok(permit) => {
                    this.metrics.permits_acquired.inc(*this.n as u64);
                    this.metrics.holders_acquired.inc(1);

                    let elapsed = this.t_start.elapsed();
                    this.metrics.acquire_duration.record(elapsed);

                    // reset "pending" metrics if we've reported any
                    if *this.reported_pending {
                        this.metrics.permits_pending.dec(*this.n as u64);
                        this.metrics.holders_pending.dec(1);

                        // Ensure that `Drop` doesn't decrease these metrics a 2nd time. Don't solely rely on `Drop`
                        // however since this future might be referenced somewhere in the stack even when the result was
                        // already produced.
                        *this.reported_pending = false;
                    }

                    let mut span_recorder = this
                        .span_recorder
                        .take()
                        .expect("span recorder should still be present");
                    span_recorder.ok("acquired");

                    Poll::Ready(Ok(InstrumentedAsyncOwnedSemaphorePermit {
                        inner: permit,
                        n: *this.n,
                        metrics: Arc::clone(this.metrics),
                        span_recorder,
                    }))
                }
                Err(e) => {
                    this.span_recorder
                        .take()
                        .expect("span recorder should still be present")
                        .error("AcquireError");

                    Poll::Ready(Err(e))
                }
            },
            Poll::Pending => {
                // report "pending" metrics once
                if !*this.reported_pending {
                    this.metrics.permits_pending.inc(*this.n as u64);
                    this.metrics.holders_pending.inc(1);

                    *this.reported_pending = true;
                }

                Poll::Pending
            }
        }
    }
}

#[pinned_drop]
#[allow(clippy::needless_lifetimes)]
impl<'a> PinnedDrop for InstrumentedAsyncSemaphoreAcquire<'a> {
    fn drop(self: std::pin::Pin<&mut Self>) {
        let this = self.project();

        // reset "pending" metrics if we've reported any
        if *this.reported_pending {
            this.metrics.permits_pending.dec(*this.n as u64);
            this.metrics.holders_pending.dec(1);

            this.metrics
                .permits_cancelled_while_pending
                .inc(*this.n as u64);
            this.metrics.holders_cancelled_while_pending.inc(1);
        }
    }
}

/// An instrumented wrapper around [`tokio::sync::OwnedSemaphorePermit`].
///
/// Normally you should use the non-owned
/// [`InstrumentedAsyncSemaphorePermit`] version because the semaphore
/// is attached to an object and moving it around independently from
/// object can cause state confusion.
///
/// In certain distributed scenarios however, it may make sense to
/// detach the permit from its origin (with the risk that this
/// introduces state confusion) and use this owned version.
#[derive(Debug)]
pub struct InstrumentedAsyncOwnedSemaphorePermit {
    /// The actual permit.
    ///
    /// This permit is never accessed but we hold it here because dropping it clears the permit.
    #[allow(dead_code)]
    inner: OwnedSemaphorePermit,

    /// Number of permits that we hold.
    ///
    /// This is required for metric purposes.
    n: u32,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,

    /// Span recorder for the entire semaphore interaction.
    ///
    /// No direct interaction, will be exported during drop (aka the end of the span will be set).
    #[allow(dead_code)]
    span_recorder: SpanRecorder,
}

impl Drop for InstrumentedAsyncOwnedSemaphorePermit {
    fn drop(&mut self) {
        self.metrics.holders_acquired.dec(1);
        self.metrics.permits_acquired.dec(self.n as u64);
    }
}

/// An instrumented wrapper around [`tokio::sync::SemaphorePermit`].
#[derive(Debug)]
pub struct InstrumentedAsyncSemaphorePermit<'a> {
    /// Use the owned variant so we can use a single implementation.
    #[allow(dead_code)]
    owned_permit: InstrumentedAsyncOwnedSemaphorePermit,

    /// Phantom data to track the livetime.
    #[allow(dead_code)]
    phantom: PhantomData<&'a ()>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{pin, sync::Barrier};
    use trace::{ctx::SpanContext, span::SpanStatus, RingBufferTraceCollector};

    use super::*;

    #[tokio::test]
    async fn test_send_sync() {
        let metrics = AsyncSemaphoreMetrics::new_unregistered();
        assert_send(&metrics);
        assert_sync(&metrics);

        let metrics = Arc::new(metrics);
        let semaphore = metrics.new_semaphore(10);
        assert_send(&semaphore);
        assert_sync(&semaphore);

        let acquire_fut = semaphore.acquire(None);
        let acquire_many_fut = semaphore.acquire_many(1, None);
        assert_send(&acquire_fut);
        assert_send(&acquire_many_fut);
        // futures itself are NOT Sync

        let permit_acquire = acquire_fut.await.unwrap();
        let permit_acquire_many = acquire_many_fut.await.unwrap();
        assert_send(&permit_acquire);
        assert_send(&permit_acquire_many);
        assert_sync(&permit_acquire);
        assert_sync(&permit_acquire_many);
    }

    #[tokio::test]
    async fn test_send_sync_owned() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(10));

        assert_eq!(10, semaphore.total_permits());

        let acquire_fut = semaphore.acquire_owned(None);
        let acquire_many_fut = semaphore.acquire_many_owned(1, None);
        assert_send(&acquire_fut);
        assert_send(&acquire_many_fut);
        // futures itself are NOT Sync

        let permit_acquire = acquire_fut.await.unwrap();
        assert_eq!(10, semaphore.total_permits());
        let permit_acquire_many = acquire_many_fut.await.unwrap();
        assert_eq!(10, semaphore.total_permits());
        assert_send(&permit_acquire);
        assert_send(&permit_acquire_many);
        assert_sync(&permit_acquire);
        assert_sync(&permit_acquire_many);
    }

    #[tokio::test]
    async fn test_total_permits() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        assert_eq!(metrics.permits_total.fetch(), 0);

        let semaphore1 = metrics.new_semaphore(10);
        assert_eq!(metrics.permits_total.fetch(), 10);

        let semaphore2 = metrics.new_semaphore(3);
        assert_eq!(metrics.permits_total.fetch(), 13);

        drop(semaphore1);
        assert_eq!(metrics.permits_total.fetch(), 3);

        drop(semaphore2);
        assert_eq!(metrics.permits_total.fetch(), 0);
    }

    #[tokio::test]
    async fn test_permits_acquired_and_holders_acquired() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(10));

        assert_eq!(metrics.holders_acquired.fetch(), 0);
        assert_eq!(metrics.permits_acquired.fetch(), 0);

        let p1 = semaphore.acquire(None).await.unwrap();
        let p2 = semaphore.acquire_many(5, None).await.unwrap();

        let pending_barrier = Arc::new(Barrier::new(2));
        let pending_barrier_captured = Arc::clone(&pending_barrier);
        let permit_acquired_barrier = Arc::new(Barrier::new(2));
        let permit_acquired_barrier_captured = Arc::clone(&permit_acquired_barrier);
        let drop_permit_barrier = Arc::new(Barrier::new(2));
        let semaphore_captured = Arc::clone(&semaphore);
        let drop_permit_barrier_captured = Arc::clone(&drop_permit_barrier);
        let task = tokio::task::spawn(async move {
            let fut = semaphore_captured.acquire_many(7, None).fuse();
            pin!(fut);
            futures::select_biased! {
                _ = fut => panic!("should be pending"),
                _ = pending_barrier_captured.wait().fuse() => (),
            };

            let permit = fut.await.unwrap();
            permit_acquired_barrier_captured.wait().await;

            drop_permit_barrier_captured.wait().await;
            drop(permit);
        });
        pending_barrier.wait().await;

        assert_eq!(metrics.holders_acquired.fetch(), 2);
        assert_eq!(metrics.permits_acquired.fetch(), 6); // = 1 + 5

        drop(p2);
        permit_acquired_barrier.wait().await;

        assert_eq!(metrics.holders_acquired.fetch(), 2);
        assert_eq!(metrics.permits_acquired.fetch(), 8); // = 1 + 5 - 5 + 7

        drop(p1);

        assert_eq!(metrics.holders_acquired.fetch(), 1);
        assert_eq!(metrics.permits_acquired.fetch(), 7); // = 1 + 5 - 5 + 7 - 1

        drop_permit_barrier.wait().await;
        task.await.unwrap();

        assert_eq!(metrics.holders_acquired.fetch(), 0);
        assert_eq!(metrics.permits_acquired.fetch(), 0); // = 1 + 5 - 5 + 7 - 1 - 7
    }

    #[tokio::test]
    async fn test_permits_pending_and_holders_pending() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(10));

        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);
        assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 0);
        assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 0);

        let p1 = semaphore.acquire_many(5, None).await.unwrap();

        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);
        assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 0);
        assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 0);

        {
            let fut = semaphore.acquire_many(6, None);
            pin!(fut);
            assert_fut_pending(&mut fut).await;

            assert_eq!(metrics.holders_pending.fetch(), 1);
            assert_eq!(metrics.permits_pending.fetch(), 6);
            assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 0);
            assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 0);

            // `fut` is dropped here
        }

        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);
        assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 1);
        assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 6);

        {
            let fut = semaphore.acquire_many(6, None);
            pin!(fut);
            assert_fut_pending(&mut fut).await;

            assert_eq!(metrics.holders_pending.fetch(), 1);
            assert_eq!(metrics.permits_pending.fetch(), 6);
            assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 1);
            assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 6);

            drop(p1);

            let _p2 = (&mut fut).await.unwrap();

            assert_eq!(metrics.holders_pending.fetch(), 0);
            assert_eq!(metrics.permits_pending.fetch(), 0);
            assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 1);
            assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 6);

            // `fut` is finally dropped here
        }

        // dropping the future should NOT decrease the pending counter a 2nd time and should NOT be considered as "cancelled"
        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);
        assert_eq!(metrics.holders_cancelled_while_pending.fetch(), 1);
        assert_eq!(metrics.permits_cancelled_while_pending.fetch(), 6);
    }

    #[tokio::test]
    async fn test_acquire_duration() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(10));

        assert_eq!(
            metrics.acquire_duration.fetch().total,
            Duration::from_millis(0)
        );

        let p1 = semaphore.acquire_many(5, None).await.unwrap();

        let fut = semaphore.acquire_many(6, None);
        pin!(fut);
        assert_fut_pending(&mut fut).await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(p1);
        fut.await.unwrap();

        assert!(metrics.acquire_duration.fetch().total >= Duration::from_millis(10));
    }

    #[tokio::test]
    #[should_panic(expected = "`async fn` resumed after completion")]
    async fn test_poll_ready_future_panics() {
        // ensure that polling a "ready" future panics. This is the normal behavior for futures and we should follow
        // that (instead of silently doing something silly with the metrics).
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(1));

        let fut = semaphore.acquire(None);

        pin!(fut);
        (&mut fut).await.unwrap();
        (&mut fut).await.unwrap();
    }

    #[tokio::test]
    async fn test_tracing() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(1));

        // happy path
        let traces = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces) as _).child("semaphore");
        semaphore.acquire(Some(span)).await.unwrap();

        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == "semaphore")
            .expect("tracing span not found");

        assert_eq!(span.status, SpanStatus::Ok);
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].msg, "acquired");

        // canceled
        let traces = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces) as _).child("semaphore");
        let _permit = semaphore.acquire(None).await.unwrap();
        {
            let fut = semaphore.acquire(Some(span));
            pin!(fut);
            assert_fut_pending(&mut fut).await;
            // `fut` is dropped here
        }

        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == "semaphore")
            .expect("tracing span not found");

        assert_eq!(span.status, SpanStatus::Unknown);
        assert_eq!(span.events.len(), 0);
    }

    /// Check that a given object implements [`Send`].
    fn assert_send<T: Send>(_: &T) {}

    /// Check that a given object implements [`Sync`].
    fn assert_sync<T: Sync>(_: &T) {}

    /// Assert that given future is pending.
    ///
    /// This will try to poll the future a bit to ensure that it is not stuck in tokios task preemption.
    async fn assert_fut_pending<F>(fut: &mut F)
    where
        F: Future + Send + Unpin,
        F::Output: std::fmt::Debug,
    {
        tokio::select! {
            x = fut => panic!("future is not pending, yielded: {x:?}"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        };
    }
}
