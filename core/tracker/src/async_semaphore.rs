//! Tooling to track/instrument [`tokio::sync::Semaphore`]s.
use std::{
    future::Future,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::Deref,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use futures::{FutureExt, future::BoxFuture};
use metric::{Attributes, DurationHistogram, MakeMetricObserver, U64Counter, U64Gauge};
use parking_lot::RwLock;
use pin_project::{pin_project, pinned_drop};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub use tokio::sync::AcquireError;
use trace::span::{Span, SpanRecorder};

const SPAN_NAME_ACQUIRE: &str = "acquire";
const SPAN_MSG_ACQUIRED: &str = "acquired";
const SPAN_NAME_PERMIT: &str = "permit";

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
    first_poll_wait_duration: DurationHistogram,
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
        let first_poll_wait_duration = registry.register_metric::<DurationHistogram>(
            "iox_async_semaphore_first_poll_wait_duration",
            "Duration between the time the acquire future is created and until it is polled for the first time"
        ).recorder(attributes.clone());
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
            first_poll_wait_duration,
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
            first_poll_wait_duration: DurationHistogram::create(&Default::default()),
            acquire_duration: DurationHistogram::create(&Default::default()),
        }
    }

    /// Create new instrumented semaphore.
    pub fn new_semaphore(self: &Arc<Self>, permits: usize) -> InstrumentedAsyncSemaphore {
        self.permits_total.inc(permits as u64);

        InstrumentedAsyncSemaphore {
            inner: Arc::new(Semaphore::new(permits)),
            permits: RwLock::new(permits),
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
    permits: RwLock<usize>,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,
}

impl InstrumentedAsyncSemaphore {
    /// Ensure that this semaphore has at least this amount of permits in total.
    ///
    /// If the semaphore size was changed the delta will be returned.
    pub fn ensure_total_permits(&self, permits: usize) -> Option<NonZeroUsize> {
        // fast-path
        {
            let guard = self.permits.read();
            if permits <= *guard {
                return None;
            }
        }

        let mut guard = self.permits.write();
        let delta = NonZeroUsize::new(permits.saturating_sub(*guard))?;
        self.inner.add_permits(delta.get());
        self.metrics.permits_total.inc(delta.get() as u64);
        *guard = permits;
        Some(delta)
    }

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
        let span_recorder_all = SpanRecorder::new(span);
        let span_recorder_acquire =
            SpanRecorder::new(span_recorder_all.child_span(SPAN_NAME_ACQUIRE));

        InstrumentedAsyncSemaphoreAcquire {
            inner: Arc::clone(&self.inner).acquire_many_owned(n).boxed(),
            metrics: Arc::clone(&self.metrics),
            n,
            time_until_first_poll: None,
            t_start: Instant::now(),
            span_recorder_acquire: Some(span_recorder_acquire),
            span_recorder_all: Some(span_recorder_all),
        }
    }

    /// return the total number of permits (available + already acquired).
    pub fn total_permits(&self) -> usize {
        *self.permits.read()
    }

    /// return the number of pending permits
    pub fn permits_pending(&self) -> u64 {
        self.metrics.permits_pending.fetch()
    }

    /// return the number of acquired permits
    pub fn permits_acquired(&self) -> u64 {
        self.metrics.permits_acquired.fetch()
    }

    /// return the number of pending holders
    pub fn holders_pending(&self) -> u64 {
        self.metrics.holders_pending.fetch()
    }

    /// return the number of acquired holders
    pub fn holders_acquired(&self) -> u64 {
        self.metrics.holders_acquired.fetch()
    }
}

impl Drop for InstrumentedAsyncSemaphore {
    fn drop(&mut self) {
        let permits = self.permits.read();
        self.metrics.permits_total.dec(*permits as u64);
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

    /// The amount of time that passed between `t_start` and the first time that this future is
    /// polled at all.
    ///
    /// This is set back to `None` after the future returns `Poll::Ready(Ok(_))` so that we don't
    /// falsely record this future as cancelled-while-pending in the `Drop` impl
    time_until_first_poll: Option<Duration>,

    /// Start time of the "acquire" action.
    t_start: Instant,

    /// Span recorder for the acquire part of the permit.
    ///
    /// Wrapped into an [`Option`] so we can drop/finalize it when acquiring is finished.
    ///
    /// This is declared BEFORE [`span_recorder_all`](Self::span_recorder_all) so it is dropped before.
    span_recorder_acquire: Option<SpanRecorder>,

    /// Span recorder for the entire semaphore interaction.
    ///
    /// Wrapped into an [`Option`] to allow the handover between the acquire-future and the permit.
    ///
    /// This is declared AFTER [`span_recorder_acquire`](Self::span_recorder_acquire) so it is dropped after.
    span_recorder_all: Option<SpanRecorder>,
}

impl std::fmt::Debug for InstrumentedAsyncSemaphoreAcquire<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedAsyncSemaphoreAcquire")
            .field("metrics", &self.metrics)
            .field("n", &self.n)
            .field("time_until_first_poll", &self.time_until_first_poll)
            .field("t_start", &self.t_start)
            .finish_non_exhaustive()
    }
}

impl Future for InstrumentedAsyncSemaphoreAcquire<'_> {
    type Output = Result<InstrumentedAsyncOwnedSemaphorePermit, AcquireError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let is_first_poll = this.time_until_first_poll.is_none();
        let time_until_first_poll = this
            .time_until_first_poll
            .take()
            .unwrap_or_else(|| this.t_start.elapsed());

        let ret = match this.inner.poll(cx) {
            Poll::Ready(res) => match res {
                Ok(permit) => {
                    this.metrics.permits_acquired.inc(*this.n as u64);
                    this.metrics.holders_acquired.inc(1);

                    let acquire_duration = this.t_start.elapsed();
                    this.metrics.acquire_duration.record(acquire_duration);
                    this.metrics
                        .first_poll_wait_duration
                        .record(time_until_first_poll);

                    // reset "pending" metrics if we've reported any
                    if !is_first_poll {
                        this.metrics.permits_pending.dec(*this.n as u64);
                        this.metrics.holders_pending.dec(1);
                    }

                    let mut span_recorder_acquire = this
                        .span_recorder_acquire
                        .take()
                        .expect("span recorder should still be present");
                    span_recorder_acquire.ok(SPAN_MSG_ACQUIRED);
                    drop(span_recorder_acquire);

                    let span_recorder_all = this
                        .span_recorder_all
                        .take()
                        .expect("span recorder should still be present");

                    let span_recorder_permit =
                        SpanRecorder::new(span_recorder_all.child_span(SPAN_NAME_PERMIT));

                    // we return this early specifically so that we don't re-insert
                    // `time_until_first_poll` since we need it to be `None` if we reach this path
                    // so that we don't double-record the pending metrics in the `Drop` impl.
                    return Poll::Ready(Ok(InstrumentedAsyncOwnedSemaphorePermit {
                        inner: permit,
                        n: *this.n,
                        metrics: Arc::clone(this.metrics),
                        acquire_duration,
                        span_recorder_permit,
                        span_recorder_all,
                    }));
                }
                Err(e) => {
                    this.span_recorder_all
                        .take()
                        .expect("span recorder should still be present")
                        .error("AcquireError");

                    Poll::Ready(Err(e))
                }
            },
            Poll::Pending => {
                // report "pending" metrics once
                if is_first_poll {
                    this.metrics.permits_pending.inc(*this.n as u64);
                    this.metrics.holders_pending.inc(1);
                }

                Poll::Pending
            }
        };

        *this.time_until_first_poll = Some(time_until_first_poll);
        ret
    }
}

#[pinned_drop]
impl<'a> PinnedDrop for InstrumentedAsyncSemaphoreAcquire<'a> {
    fn drop(self: std::pin::Pin<&mut Self>) {
        let this = self.project();

        // reset "pending" metrics if we've reported any
        if this.time_until_first_poll.is_some() {
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
    #[expect(dead_code)]
    inner: OwnedSemaphorePermit,

    /// Number of permits that we hold.
    ///
    /// This is required for metric purposes.
    n: u32,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,

    /// The time it took to acquire this permit.
    acquire_duration: Duration,

    /// Span recorder for the "hold/permit" part of the semaphore.
    ///
    /// No direct interaction, will be exported during drop (aka the end of the span will be set).
    ///
    /// This is declared BEFORE [`span_recorder_all`](Self::span_recorder_all) so it is dropped before.
    #[expect(dead_code)]
    span_recorder_permit: SpanRecorder,

    /// Span recorder for the entire semaphore interaction.
    ///
    /// No direct interaction, will be exported during drop (aka the end of the span will be set).
    ///
    /// This is declared AFTER [`span_recorder_permit`](Self::span_recorder_permit) so it is dropped after.
    #[expect(dead_code)]
    span_recorder_all: SpanRecorder,
}

impl InstrumentedAsyncOwnedSemaphorePermit {
    /// The time it took to acquire this permit.
    pub fn acquire_duration(&self) -> Duration {
        self.acquire_duration
    }
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
    owned_permit: InstrumentedAsyncOwnedSemaphorePermit,

    /// Phantom data to track the livetime.
    phantom: PhantomData<&'a ()>,
}

impl Deref for InstrumentedAsyncSemaphorePermit<'_> {
    type Target = InstrumentedAsyncOwnedSemaphorePermit;

    fn deref(&self) -> &Self::Target {
        &self.owned_permit
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_helpers::timeout::FutureTimeout;
    use tokio::{pin, sync::Barrier};
    use trace::{RingBufferTraceCollector, ctx::SpanContext, span::SpanStatus};

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
        let p1_duration = p1.acquire_duration();

        let fut = semaphore.acquire_many(6, None);
        pin!(fut);
        assert_fut_pending(&mut fut).await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(p1);
        let p2 = fut.await.unwrap();
        let acquire_duration_method = p1_duration + p2.acquire_duration();
        let acquire_duration_metric = metrics.acquire_duration.fetch().total;

        assert_eq!(acquire_duration_method, acquire_duration_metric);
        assert!(acquire_duration_method >= Duration::from_millis(10));
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
        const ROOT_NAME: &str = "my_semaphore";

        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(1));

        // happy path
        let traces = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces) as _).child(ROOT_NAME);
        semaphore.acquire(Some(span)).await.unwrap();

        let span_all = traces
            .spans()
            .into_iter()
            .find(|s| s.name == ROOT_NAME)
            .expect("tracing span not found");
        assert_eq!(span_all.status, SpanStatus::Unknown);
        assert_eq!(span_all.events.len(), 0);

        let span_acquire = traces
            .spans()
            .into_iter()
            .find(|s| s.name == SPAN_NAME_ACQUIRE)
            .expect("tracing span not found");
        assert_eq!(span_acquire.status, SpanStatus::Ok);
        assert_eq!(span_acquire.events.len(), 1);
        assert_eq!(span_acquire.events[0].msg, SPAN_MSG_ACQUIRED);
        assert_eq!(span_acquire.ctx.parent_span_id, Some(span_all.ctx.span_id));

        let span_permit = traces
            .spans()
            .into_iter()
            .find(|s| s.name == SPAN_NAME_PERMIT)
            .expect("tracing span not found");
        assert_eq!(span_permit.status, SpanStatus::Unknown);
        assert_eq!(span_permit.ctx.parent_span_id, Some(span_all.ctx.span_id));

        // canceled
        let traces = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces) as _).child(ROOT_NAME);
        let _permit = semaphore.acquire(None).await.unwrap();
        {
            let fut = semaphore.acquire(Some(span));
            pin!(fut);
            assert_fut_pending(&mut fut).await;
            // `fut` is dropped here
        }

        let span_all = traces
            .spans()
            .into_iter()
            .find(|s| s.name == ROOT_NAME)
            .expect("tracing span not found");
        assert_eq!(span_all.status, SpanStatus::Unknown);
        assert_eq!(span_all.events.len(), 0);

        let span_acquire = traces
            .spans()
            .into_iter()
            .find(|s| s.name == SPAN_NAME_ACQUIRE)
            .expect("tracing span not found");
        assert_eq!(span_acquire.status, SpanStatus::Unknown);
        assert_eq!(span_acquire.events.len(), 0);
        assert_eq!(span_acquire.ctx.parent_span_id, Some(span_all.ctx.span_id));

        assert!(
            !traces
                .spans()
                .into_iter()
                .any(|s| s.name == SPAN_NAME_PERMIT)
        );
    }

    #[tokio::test]
    async fn test_ensure_total_permits() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());

        let semaphore = metrics.new_semaphore(10);
        assert_eq!(semaphore.total_permits(), 10);
        assert_eq!(metrics.permits_total.fetch(), 10);
        semaphore
            .acquire_many(10, None)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .unwrap();
        semaphore
            .acquire_many(12, None)
            .with_timeout(Duration::from_secs(1))
            .await
            .unwrap_err();

        assert_eq!(semaphore.ensure_total_permits(10), None);
        assert_eq!(semaphore.total_permits(), 10);
        assert_eq!(metrics.permits_total.fetch(), 10);

        assert_eq!(
            semaphore.ensure_total_permits(12),
            Some(NonZeroUsize::new(2).unwrap())
        );
        assert_eq!(semaphore.total_permits(), 12);
        assert_eq!(metrics.permits_total.fetch(), 12);
        semaphore
            .acquire_many(12, None)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(semaphore.ensure_total_permits(10), None);
        assert_eq!(semaphore.total_permits(), 12);
        assert_eq!(metrics.permits_total.fetch(), 12);
        semaphore
            .acquire_many(12, None)
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .unwrap();

        drop(semaphore);
        assert_eq!(metrics.permits_total.fetch(), 0);
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
