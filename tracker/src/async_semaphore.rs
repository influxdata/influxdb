//! Tooling to track/instrument [`tokio::sync::Semaphore`]s.
use std::{future::Future, sync::Arc, task::Poll, time::Instant};

use futures::{future::BoxFuture, FutureExt};
use metric::{Attributes, DurationHistogram, MakeMetricObserver, U64Gauge};
use pin_project::{pin_project, pinned_drop};
use tokio::sync::{Semaphore, SemaphorePermit};

pub use tokio::sync::AcquireError;

/// Metrics that can be used to create a [`InstrumentedAsyncSemaphore`].
#[derive(Debug)]
pub struct AsyncSemaphoreMetrics {
    permits_acquired: U64Gauge,
    permits_total: U64Gauge,
    permits_pending: U64Gauge,
    holders_acquired: U64Gauge,
    holders_pending: U64Gauge,
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
            holders_acquired,
            holders_pending,
            acquire_duration,
        }
    }

    /// Create metrics that are not associated with any registry.
    pub fn new_unregistered() -> Self {
        Self {
            permits_acquired: Default::default(),
            permits_total: Default::default(),
            permits_pending: Default::default(),
            holders_acquired: Default::default(),
            holders_pending: Default::default(),
            acquire_duration: DurationHistogram::create(&Default::default()),
        }
    }

    /// Create new instrumented semaphore.
    pub fn new_semaphore(self: &Arc<Self>, permits: usize) -> InstrumentedAsyncSemaphore {
        self.permits_total.inc(permits as u64);

        InstrumentedAsyncSemaphore {
            inner: Semaphore::new(permits),
            permits,
            metrics: Arc::clone(self),
        }
    }
}

/// Instrumented version of [`tokio::sync::Semaphore`].
#[derive(Debug)]
pub struct InstrumentedAsyncSemaphore {
    /// Underlying sempahore implementation.
    inner: Semaphore,

    /// Number of total permits (acquired and available).
    permits: usize,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,
}

impl InstrumentedAsyncSemaphore {
    /// Acquire a single permit.
    ///
    /// See [`tokio::sync::Semaphore::acquire`] for details.
    pub async fn acquire(&self) -> Result<InstrumentedAsyncSemaphorePermit<'_>, AcquireError> {
        self.acquire_many(1).await
    }

    /// Acquire `n` permits.
    ///
    /// See [`tokio::sync::Semaphore::acquire_many`] for details.
    pub fn acquire_many(
        &self,
        n: u32,
    ) -> impl Future<Output = Result<InstrumentedAsyncSemaphorePermit<'_>, AcquireError>> {
        InstrumentedAsyncSemaphoreAcquire {
            inner: self.inner.acquire_many(n).boxed(),
            metrics: Arc::clone(&self.metrics),
            n,
            reported_pending: false,
            t_start: Instant::now(),
        }
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
    inner: BoxFuture<'a, Result<SemaphorePermit<'a>, AcquireError>>,

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
    type Output = Result<InstrumentedAsyncSemaphorePermit<'a>, AcquireError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Ready(res) => match res {
                Ok(permit) => {
                    this.metrics.permits_acquired.inc(*this.n as u64);
                    this.metrics.holders_acquired.inc(1);

                    let elapsed = this.t_start.elapsed();
                    this.metrics.acquire_duration.record(elapsed);

                    // reset "pendig" metrics if we've reported any
                    if *this.reported_pending {
                        this.metrics.permits_pending.dec(*this.n as u64);
                        this.metrics.holders_pending.dec(1);

                        // Ensure that `Drop` doesn't decrease these metrics a 2nd time. Don't solely rely on `Drop`
                        // however since this future might be referenced somewhere in the stack even when the result was
                        // already produced.
                        *this.reported_pending = false;
                    }

                    Poll::Ready(Ok(InstrumentedAsyncSemaphorePermit {
                        inner: permit,
                        n: *this.n,
                        metrics: Arc::clone(this.metrics),
                    }))
                }
                Err(e) => Poll::Ready(Err(e)),
            },
            Poll::Pending => {
                // report "pendig" metrics once
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

        // reset "pendig" metrics if we've reported any
        if *this.reported_pending {
            this.metrics.permits_pending.dec(*this.n as u64);
            this.metrics.holders_pending.dec(1);
        }
    }
}

/// An instrumented wrapper around [`tokio::sync::SemaphorePermit`].
#[derive(Debug)]
pub struct InstrumentedAsyncSemaphorePermit<'a> {
    /// The actual permit.
    ///
    /// This permit is never accessed but we hold it here because dropping it clears the permit.
    #[allow(dead_code)]
    inner: SemaphorePermit<'a>,

    /// Number of permits that we hold.
    ///
    /// This is required for metric purposes.
    n: u32,

    /// Metrics.
    metrics: Arc<AsyncSemaphoreMetrics>,
}

impl<'a> Drop for InstrumentedAsyncSemaphorePermit<'a> {
    fn drop(&mut self) {
        self.metrics.holders_acquired.dec(1);
        self.metrics.permits_acquired.dec(self.n as u64);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{pin, sync::Barrier};

    use super::*;

    #[tokio::test]
    async fn test_send_sync() {
        let metrics = AsyncSemaphoreMetrics::new_unregistered();
        assert_send(&metrics);
        assert_sync(&metrics);

        let metrics = Arc::new(metrics);
        let semaphore = metrics.new_semaphore(1);
        assert_send(&semaphore);
        assert_sync(&semaphore);

        let acquire_fut = semaphore.acquire();
        assert_send(&acquire_fut);
        // future itself is NOT Sync

        let permit = acquire_fut.await.unwrap();
        assert_send(&permit);
        assert_sync(&permit);
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

        let p1 = semaphore.acquire().await.unwrap();
        let p2 = semaphore.acquire_many(5).await.unwrap();

        let pending_barrier = Arc::new(Barrier::new(2));
        let pending_barrier_captured = Arc::clone(&pending_barrier);
        let permit_acquired_barrier = Arc::new(Barrier::new(2));
        let permit_acquired_barrier_captured = Arc::clone(&permit_acquired_barrier);
        let drop_permit_barrier = Arc::new(Barrier::new(2));
        let semaphore_captured = Arc::clone(&semaphore);
        let drop_permit_barrier_captured = Arc::clone(&drop_permit_barrier);
        let task = tokio::task::spawn(async move {
            let fut = semaphore_captured.acquire_many(7).fuse();
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

        let p1 = semaphore.acquire_many(5).await.unwrap();

        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);

        let mut fut = semaphore.acquire_many(6);
        assert_fut_pending(&mut fut).await;

        assert_eq!(metrics.holders_pending.fetch(), 1);
        assert_eq!(metrics.permits_pending.fetch(), 6);

        drop(fut);

        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);

        let mut fut = semaphore.acquire_many(6);
        assert_fut_pending(&mut fut).await;

        assert_eq!(metrics.holders_pending.fetch(), 1);
        assert_eq!(metrics.permits_pending.fetch(), 6);

        drop(p1);

        {
            // pin in local scope so that `.await` does not consume the future
            pin!(fut);
            let _p2 = (&mut fut).await.unwrap();

            assert_eq!(metrics.holders_pending.fetch(), 0);
            assert_eq!(metrics.permits_pending.fetch(), 0);

            // `fut` is finally dropped here
        }

        // dropping the future should not decrease the counter a 2nd time
        assert_eq!(metrics.holders_pending.fetch(), 0);
        assert_eq!(metrics.permits_pending.fetch(), 0);
    }

    #[tokio::test]
    async fn test_acquire_duration() {
        let metrics = Arc::new(AsyncSemaphoreMetrics::new_unregistered());
        let semaphore = Arc::new(metrics.new_semaphore(10));

        assert_eq!(
            metrics.acquire_duration.fetch().total,
            Duration::from_millis(0)
        );

        let p1 = semaphore.acquire_many(5).await.unwrap();

        let mut fut = semaphore.acquire_many(6);
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

        let fut = semaphore.acquire();

        pin!(fut);
        (&mut fut).await.unwrap();
        (&mut fut).await.unwrap();
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
