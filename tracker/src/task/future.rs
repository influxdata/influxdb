use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::{future::BoxFuture, prelude::*};
use pin_project::{pin_project, pinned_drop};

use super::{TaskRegistration, TrackerState};
use std::sync::Arc;

/// An extension trait that provides `self.track(registration)` allowing
/// associating this future with a `TrackerRegistration`
pub trait TrackedFutureExt: TryFuture {
    fn track(self, registration: TaskRegistration) -> TrackedFuture<Self>
    where
        Self: Sized,
    {
        let tracker = Arc::clone(&registration.state);
        let token = tracker.cancel_token.clone();

        tracker.created_futures.fetch_add(1, Ordering::Relaxed);
        tracker.pending_futures.fetch_add(1, Ordering::Relaxed);

        // This must occur after the increment of pending_futures
        std::mem::drop(registration);

        // The future returned by CancellationToken::cancelled borrows the token
        // In order to ensure we get a future with a static lifetime
        // we box them up together and let async work its magic
        let cancel = Box::pin(async move { token.cancelled().await });

        TrackedFuture {
            inner: self,
            cancel,
            tracker,
            complete: false,
        }
    }
}

impl<T: ?Sized> TrackedFutureExt for T where T: TryFuture {}

/// The `Future` returned by `TrackedFutureExt::track()`
/// Unregisters the future from the registered `TrackerRegistry` on drop
/// and provides the early termination functionality used by
/// `TrackerRegistry::terminate`
#[pin_project(PinnedDrop)]
#[allow(missing_debug_implementations)]
pub struct TrackedFuture<F: TryFuture> {
    #[pin]
    inner: F,
    #[pin]
    cancel: BoxFuture<'static, ()>,
    tracker: Arc<TrackerState>,
    complete: bool,
}

impl<F: TryFuture> Future for TrackedFuture<F> {
    type Output = Result<Result<F::Ok, F::Error>, future::Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.complete, "It is illegal to poll a completed future");
        if self.as_mut().project().cancel.poll(cx).is_ready() {
            *self.as_mut().project().complete = true;
            self.tracker
                .cancelled_futures
                .fetch_add(1, Ordering::Relaxed);
            return Poll::Ready(Err(future::Aborted {}));
        }

        let start = Instant::now();
        let poll = self.as_mut().project().inner.try_poll(cx);
        let delta = start.elapsed().as_nanos() as usize;

        self.tracker.cpu_nanos.fetch_add(delta, Ordering::Relaxed);

        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => {
                match v.is_ok() {
                    true => self.tracker.ok_futures.fetch_add(1, Ordering::Relaxed),
                    false => self.tracker.err_futures.fetch_add(1, Ordering::Relaxed),
                };

                *self.as_mut().project().complete = true;
                Poll::Ready(Ok(v))
            }
        }
    }
}

#[pinned_drop]
impl<F: TryFuture> PinnedDrop for TrackedFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        let state: &TrackerState = self.project().tracker;

        let wall_nanos = state.start_instant.elapsed().as_nanos() as usize;

        state.wall_nanos.fetch_max(wall_nanos, Ordering::Relaxed);

        // This synchronizes with the Acquire load in Tracker::get_status
        let previous = state.pending_futures.fetch_sub(1, Ordering::Release);

        // Failure implies a TrackedFuture has somehow been created
        // without it incrementing the pending_futures counter
        assert_ne!(previous, 0);

        // Need to signal potential completion
        if previous == 1 {
            // Perform an acquire load to establish ordering with respect
            // to all other decrements
            state.pending_futures.load(Ordering::Acquire);

            state.notify.notify_waiters();
        }
    }
}
