use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::prelude::*;
use pin_project::{pin_project, pinned_drop};

/// Every future registered with a `TrackerRegistry` is assigned a unique
/// `TrackerId`
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TrackerId(usize);

#[derive(Debug)]
struct Tracker<T> {
    data: T,
    abort: future::AbortHandle,
}

#[derive(Debug)]
struct TrackerContextInner<T> {
    id: AtomicUsize,
    trackers: Mutex<HashMap<TrackerId, Tracker<T>>>,
}

/// Allows tracking the lifecycle of futures registered by
/// `TrackedFutureExt::track` with an accompanying metadata payload of type T
///
/// Additionally can trigger graceful termination of registered futures
#[derive(Debug)]
pub struct TrackerRegistry<T> {
    inner: Arc<TrackerContextInner<T>>,
}

// Manual Clone to workaround https://github.com/rust-lang/rust/issues/26925
impl<T> Clone for TrackerRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for TrackerRegistry<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(TrackerContextInner {
                id: AtomicUsize::new(0),
                trackers: Mutex::new(Default::default()),
            }),
        }
    }
}

impl<T> TrackerRegistry<T> {
    pub fn new() -> Self {
        Default::default()
    }

    /// Trigger graceful termination of a registered future
    ///
    /// Returns false if no future found with the provided ID
    ///
    /// Note: If the future is currently executing, termination
    /// will only occur when the future yields (returns from poll)
    #[allow(dead_code)]
    pub fn terminate(&self, id: TrackerId) -> bool {
        if let Some(meta) = self
            .inner
            .trackers
            .lock()
            .expect("lock poisoned")
            .get_mut(&id)
        {
            meta.abort.abort();
            true
        } else {
            false
        }
    }

    fn untrack(&self, id: &TrackerId) {
        self.inner
            .trackers
            .lock()
            .expect("lock poisoned")
            .remove(id);
    }

    fn track(&self, metadata: T) -> (TrackerId, future::AbortRegistration) {
        let id = TrackerId(self.inner.id.fetch_add(1, Ordering::Relaxed));
        let (abort_handle, abort_registration) = future::AbortHandle::new_pair();

        self.inner.trackers.lock().expect("lock poisoned").insert(
            id,
            Tracker {
                abort: abort_handle,
                data: metadata,
            },
        );

        (id, abort_registration)
    }
}

impl<T: Clone> TrackerRegistry<T> {
    /// Returns a list of tracked futures, with their accompanying IDs and
    /// metadata
    #[allow(dead_code)]
    pub fn tracked(&self) -> Vec<(TrackerId, T)> {
        // TODO: Improve this - (#711)
        self.inner
            .trackers
            .lock()
            .expect("lock poisoned")
            .iter()
            .map(|(id, value)| (*id, value.data.clone()))
            .collect()
    }
}

/// An extension trait that provides `self.track(reg, {})` allowing
/// registering this future with a `TrackerRegistry`
pub trait TrackedFutureExt: Future {
    fn track<T>(self, reg: &TrackerRegistry<T>, metadata: T) -> TrackedFuture<Self, T>
    where
        Self: Sized,
    {
        let (id, registration) = reg.track(metadata);

        TrackedFuture {
            inner: future::Abortable::new(self, registration),
            reg: reg.clone(),
            id,
        }
    }
}

impl<T: ?Sized> TrackedFutureExt for T where T: Future {}

/// The `Future` returned by `TrackedFutureExt::track()`
/// Unregisters the future from the registered `TrackerRegistry` on drop
/// and provides the early termination functionality used by
/// `TrackerRegistry::terminate`
#[pin_project(PinnedDrop)]
pub struct TrackedFuture<F: Future, T> {
    #[pin]
    inner: future::Abortable<F>,

    reg: TrackerRegistry<T>,
    id: TrackerId,
}

impl<F: Future, T> Future for TrackedFuture<F, T> {
    type Output = Result<F::Output, future::Aborted>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

#[pinned_drop]
impl<F: Future, T> PinnedDrop for TrackedFuture<F, T> {
    fn drop(self: Pin<&mut Self>) {
        // Note: This could cause a double-panic in an extreme situation where
        // the internal `TrackerRegistry` lock is poisoned and drop was
        // called as part of unwinding the stack to handle another panic
        let this = self.project();
        this.reg.untrack(this.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_lifecycle() {
        let (sender, receive) = oneshot::channel();
        let reg = TrackerRegistry::new();

        let task = tokio::spawn(receive.track(&reg, ()));

        assert_eq!(reg.tracked().len(), 1);

        sender.send(()).unwrap();
        task.await.unwrap().unwrap().unwrap();

        assert_eq!(reg.tracked().len(), 0);
    }

    #[tokio::test]
    async fn test_interleaved() {
        let (sender1, receive1) = oneshot::channel();
        let (sender2, receive2) = oneshot::channel();
        let reg = TrackerRegistry::new();

        let task1 = tokio::spawn(receive1.track(&reg, 1));
        let task2 = tokio::spawn(receive2.track(&reg, 2));

        let mut tracked: Vec<_> = reg.tracked().iter().map(|x| x.1).collect();
        tracked.sort_unstable();
        assert_eq!(tracked, vec![1, 2]);

        sender2.send(()).unwrap();
        task2.await.unwrap().unwrap().unwrap();

        let tracked: Vec<_> = reg.tracked().iter().map(|x| x.1).collect();
        assert_eq!(tracked, vec![1]);

        sender1.send(42).unwrap();
        let ret = task1.await.unwrap().unwrap().unwrap();

        assert_eq!(ret, 42);
        assert_eq!(reg.tracked().len(), 0);
    }

    #[tokio::test]
    async fn test_drop() {
        let reg = TrackerRegistry::new();

        {
            let f = futures::future::pending::<()>().track(&reg, ());

            assert_eq!(reg.tracked().len(), 1);

            std::mem::drop(f);
        }

        assert_eq!(reg.tracked().len(), 0);
    }

    #[tokio::test]
    async fn test_terminate() {
        let reg = TrackerRegistry::new();

        let task = tokio::spawn(futures::future::pending::<()>().track(&reg, ()));

        let tracked = reg.tracked();
        assert_eq!(tracked.len(), 1);

        reg.terminate(tracked[0].0);
        let result = task.await.unwrap();

        assert!(result.is_err());
        assert_eq!(reg.tracked().len(), 0);
    }
}
