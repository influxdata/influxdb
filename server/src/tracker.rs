//! This module contains a future tracking system supporting fanout,
//! cancellation and asynchronous signalling of completion
//!
//! A Tracker is created by calling TrackerRegistry::register. TrackedFutures
//! can then be associated with this Tracker and monitored and/or cancelled.
//!
//! This is used within IOx to track futures spawned as multiple tokio tasks.
//!
//! For example, when migrating a chunk from the mutable buffer to the read
//! buffer:
//!
//! - There is a single over-arching Job being performed
//! - A single tracker is allocated from a TrackerRegistry in Server and
//!   associated with the Job metadata
//! - This tracker is registered with every future that is spawned as a tokio
//!   task
//!
//! This same system may in future form part of a query tracking story
//!
//! # Correctness
//!
//! The key correctness property of the Tracker system is Tracker::is_complete
//! only returns true when all futures associated with the tracker have
//! completed and no more can be spawned. Additionally at such a point
//! all metrics - cpu_nanos, wall_nanos, created_futures should be visible
//! to the calling thread
//!
//! Note: there is no guarantee that pending_registrations or pending_futures
//! ever reaches 0, a program could call mem::forget on a TrackerRegistration,
//! leak the TrackerRegistration, spawn a future that never finishes, etc...
//! Such a program would never consider the Tracker complete and therefore this
//! doesn't violate the correctness property
//!
//! ## Proof
//!
//! 1. pending_registrations starts at 1, and is incremented on
//! TrackerRegistration::clone. A TrackerRegistration cannot be created from an
//! existing TrackerState, only another TrackerRegistration
//!
//! 2. pending_registrations is decremented with release semantics on
//! TrackerRegistration::drop
//!
//! 3. pending_futures is only incremented with a TrackerRegistration in scope
//!
//! 4. 2. + 3. -> A thread that increments pending_futures, decrements
//! pending_registrations with release semantics afterwards. By definition of
//! release semantics these writes to pending_futures cannot be reordered to
//! come after the atomic decrement of pending_registrations
//!
//! 5. 1. + 2. + drop cannot be called multiple times on the same object -> once
//! pending_registrations is decremented to 0 it can never be incremented again
//!
//! 6. 4. + 5. -> the decrement to 0 of pending_registrations must commit after
//! the last increment of pending_futures
//!
//! 7. pending_registrations is loaded with acquire semantics
//!
//! 8. By definition of acquire semantics, any thread that reads
//! pending_registrations is guaranteed to see any increments to pending_futures
//! performed before the most recent decrement of pending_registrations
//!
//! 9. 6. + 8. -> A thread that observes a pending_registrations of 0 cannot
//! subsequently observe pending_futures to increase
//!
//! 10. Tracker::is_complete returns if it observes pending_registrations to be
//! 0 and then pending_futures to be 0
//!
//! 11. 9 + 10 -> A thread can only observe Tracker::is_complete() == true
//! after all futures have been dropped and no more can be created
//!
//! 12. pending_futures is decremented with Release semantics on
//! TrackedFuture::drop after any associated metrics have been incremented
//!
//! 13. pending_futures is loaded with acquire semantics
//!
//! 14. 12. + 13. -> A thread that observes a pending_futures of 0 is guaranteed
//! to see any metrics from any dropped TrackedFuture
//!
//! Note: this proof ignores the complexity of moving Trackers, TrackedFutures,
//! etc... between threads as any such functionality must perform the necessary
//! synchronisation to be well-formed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio_util::sync::CancellationToken;
use tracing::warn;

pub use future::{TrackedFuture, TrackedFutureExt};
pub use registry::{TrackerId, TrackerRegistry};

mod future;
mod registry;

/// The state shared between all sibling tasks
#[derive(Debug)]
struct TrackerState {
    start_instant: Instant,
    cancel_token: CancellationToken,
    cpu_nanos: AtomicUsize,
    wall_nanos: AtomicUsize,

    created_futures: AtomicUsize,
    pending_futures: AtomicUsize,
    pending_registrations: AtomicUsize,

    watch: tokio::sync::watch::Receiver<bool>,
}

/// A Tracker can be used to monitor/cancel/wait for a set of associated futures
#[derive(Debug)]
pub struct Tracker<T> {
    id: TrackerId,
    state: Arc<TrackerState>,
    metadata: Arc<T>,
}

impl<T> Clone for Tracker<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state: Arc::clone(&self.state),
            metadata: Arc::clone(&self.metadata),
        }
    }
}

impl<T> Tracker<T> {
    /// Returns the ID of the Tracker - these are unique per TrackerRegistry
    pub fn id(&self) -> TrackerId {
        self.id
    }

    /// Returns a reference to the metadata stored within this Tracker
    pub fn metadata(&self) -> &T {
        &self.metadata
    }

    /// Trigger graceful termination of any futures tracked by
    /// this tracker
    ///
    /// Note: If the future is currently executing, termination
    /// will only occur when the future yields (returns from poll)
    /// and is then scheduled to run again
    pub fn cancel(&self) {
        self.state.cancel_token.cancel();
    }

    /// Returns the number of outstanding futures
    pub fn pending_futures(&self) -> usize {
        self.state.pending_futures.load(Ordering::Relaxed)
    }

    /// Returns the number of TrackedFutures created with this Tracker
    pub fn created_futures(&self) -> usize {
        self.state.created_futures.load(Ordering::Relaxed)
    }

    /// Returns the number of nanoseconds futures tracked by this
    /// tracker have spent executing
    pub fn cpu_nanos(&self) -> usize {
        self.state.cpu_nanos.load(Ordering::Relaxed)
    }

    /// Returns the number of nanoseconds since the Tracker was registered
    /// to the time the last TrackedFuture was dropped
    ///
    /// Returns 0 if there are still pending tasks
    pub fn wall_nanos(&self) -> usize {
        if !self.is_complete() {
            return 0;
        }
        self.state.wall_nanos.load(Ordering::Relaxed)
    }

    /// Returns true if all futures associated with this tracker have
    /// been dropped and no more can be created
    pub fn is_complete(&self) -> bool {
        // The atomic decrement in TrackerRegistration::drop has release semantics
        // acquire here ensures that if a thread observes the tracker to have
        // no pending_registrations it cannot subsequently observe pending_futures
        // to increase. If it could, observing pending_futures==0 would be insufficient
        // to conclude there are no outstanding futures
        let pending_registrations = self.state.pending_registrations.load(Ordering::Acquire);

        // The atomic decrement in TrackedFuture::drop has release semantics
        // acquire therefore ensures that if a thread observes the completion of
        // a TrackedFuture, it is guaranteed to see its updates (e.g. wall_nanos)
        let pending_futures = self.state.pending_futures.load(Ordering::Acquire);

        pending_registrations == 0 && pending_futures == 0
    }

    /// Returns if this tracker has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.state.cancel_token.is_cancelled()
    }

    /// Blocks until all futures associated with the tracker have been
    /// dropped and no more can be created
    pub async fn join(&self) {
        let mut watch = self.state.watch.clone();

        // Wait until watch is set to true or the tx side is dropped
        while !*watch.borrow() {
            if watch.changed().await.is_err() {
                // tx side has been dropped
                warn!("tracker watch dropped without signalling");
                break;
            }
        }
    }
}

/// A TrackerRegistration is returned by TrackerRegistry::register and can be
/// used to register new TrackedFutures
///
/// A tracker will not be considered completed until all TrackerRegistrations
/// referencing it have been dropped. This is to prevent a race where further
/// TrackedFutures are registered with a Tracker that has already signalled
/// completion
#[derive(Debug)]
pub struct TrackerRegistration {
    state: Arc<TrackerState>,
}

impl Clone for TrackerRegistration {
    fn clone(&self) -> Self {
        self.state
            .pending_registrations
            .fetch_add(1, Ordering::Relaxed);

        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl TrackerRegistration {
    fn new(watch: tokio::sync::watch::Receiver<bool>) -> Self {
        let state = Arc::new(TrackerState {
            start_instant: Instant::now(),
            cpu_nanos: AtomicUsize::new(0),
            wall_nanos: AtomicUsize::new(0),
            cancel_token: CancellationToken::new(),
            created_futures: AtomicUsize::new(0),
            pending_futures: AtomicUsize::new(0),
            pending_registrations: AtomicUsize::new(1),
            watch,
        });

        Self { state }
    }
}

impl Drop for TrackerRegistration {
    fn drop(&mut self) {
        let previous = self
            .state
            .pending_registrations
            .fetch_sub(1, Ordering::Release);
        assert_ne!(previous, 0);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_lifecycle() {
        let (sender, receive) = oneshot::channel();
        let registry = TrackerRegistry::new();
        let (_, registration) = registry.register(());

        let task = tokio::spawn(receive.track(registration));

        assert_eq!(registry.running().len(), 1);

        sender.send(()).unwrap();
        task.await.unwrap().unwrap().unwrap();

        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_interleaved() {
        let (sender1, receive1) = oneshot::channel();
        let (sender2, receive2) = oneshot::channel();
        let registry = TrackerRegistry::new();
        let (_, registration1) = registry.register(1);
        let (_, registration2) = registry.register(2);

        let task1 = tokio::spawn(receive1.track(registration1));
        let task2 = tokio::spawn(receive2.track(registration2));

        let tracked = sorted(registry.running());
        assert_eq!(get_metadata(&tracked), vec![1, 2]);

        sender2.send(()).unwrap();
        task2.await.unwrap().unwrap().unwrap();

        let tracked: Vec<_> = sorted(registry.running());
        assert_eq!(get_metadata(&tracked), vec![1]);

        sender1.send(42).unwrap();
        let ret = task1.await.unwrap().unwrap().unwrap();

        assert_eq!(ret, 42);
        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_drop() {
        let registry = TrackerRegistry::new();
        let (_, registration) = registry.register(());

        {
            let f = futures::future::pending::<()>().track(registration);

            assert_eq!(registry.running().len(), 1);

            std::mem::drop(f);
        }

        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_drop_multiple() {
        let registry = TrackerRegistry::new();
        let (_, registration) = registry.register(());

        {
            let f = futures::future::pending::<()>().track(registration.clone());
            {
                let f = futures::future::pending::<()>().track(registration);
                assert_eq!(registry.running().len(), 1);
                std::mem::drop(f);
            }
            assert_eq!(registry.running().len(), 1);
            std::mem::drop(f);
        }

        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_terminate() {
        let registry = TrackerRegistry::new();
        let (_, registration) = registry.register(());

        let task = tokio::spawn(futures::future::pending::<()>().track(registration));

        let tracked = registry.running();
        assert_eq!(tracked.len(), 1);

        tracked[0].cancel();
        let result = task.await.unwrap();

        assert!(result.is_err());
        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_terminate_early() {
        let registry = TrackerRegistry::new();
        let (tracker, registration) = registry.register(());
        tracker.cancel();

        let task1 = tokio::spawn(futures::future::pending::<()>().track(registration));
        let result1 = task1.await.unwrap();

        assert!(result1.is_err());
        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_terminate_multiple() {
        let registry = TrackerRegistry::new();
        let (_, registration) = registry.register(());

        let task1 = tokio::spawn(futures::future::pending::<()>().track(registration.clone()));
        let task2 = tokio::spawn(futures::future::pending::<()>().track(registration));

        let tracked = registry.running();
        assert_eq!(tracked.len(), 1);

        tracked[0].cancel();

        let result1 = task1.await.unwrap();
        let result2 = task2.await.unwrap();

        assert!(result1.is_err());
        assert!(result2.is_err());
        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_reclaim() {
        let registry = TrackerRegistry::new();

        let (_, registration1) = registry.register(1);
        let (_, registration2) = registry.register(2);
        let (_, registration3) = registry.register(3);

        let task1 = tokio::spawn(futures::future::pending::<()>().track(registration1.clone()));
        let task2 = tokio::spawn(futures::future::pending::<()>().track(registration1));
        let task3 = tokio::spawn(futures::future::ready(()).track(registration2.clone()));
        let task4 = tokio::spawn(futures::future::pending::<()>().track(registration2));
        let task5 = tokio::spawn(futures::future::pending::<()>().track(registration3));

        let running = sorted(registry.running());
        let tracked = sorted(registry.tracked());

        assert_eq!(running.len(), 3);
        assert_eq!(get_metadata(&running), vec![1, 2, 3]);
        assert_eq!(tracked.len(), 3);
        assert_eq!(get_metadata(&tracked), vec![1, 2, 3]);

        // Trigger termination of task1 and task2
        running[0].cancel();

        let result1 = task1.await.unwrap();
        let result2 = task2.await.unwrap();

        assert!(result1.is_err());
        assert!(result2.is_err());

        let running = sorted(registry.running());
        let tracked = sorted(registry.tracked());

        assert_eq!(running.len(), 2);
        assert_eq!(get_metadata(&running), vec![2, 3]);
        assert_eq!(tracked.len(), 3);
        assert_eq!(get_metadata(&tracked), vec![1, 2, 3]);

        // Expect reclaim to find now finished registration1
        let reclaimed = sorted(registry.reclaim());
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(get_metadata(&reclaimed), vec![1]);

        // Now expect tracked to match running
        let running = sorted(registry.running());
        let tracked = sorted(registry.tracked());

        assert_eq!(running.len(), 2);
        assert_eq!(get_metadata(&running), vec![2, 3]);
        assert_eq!(tracked.len(), 2);
        assert_eq!(get_metadata(&tracked), vec![2, 3]);

        // Wait for task3 to finish
        let result3 = task3.await.unwrap();
        assert!(result3.is_ok());

        assert_eq!(tracked[0].pending_futures(), 1);
        assert_eq!(tracked[0].created_futures(), 2);
        assert!(!tracked[0].is_complete());

        // Trigger termination of task5
        running[1].cancel();

        let result5 = task5.await.unwrap();
        assert!(result5.is_err());

        let running = sorted(registry.running());
        let tracked = sorted(registry.tracked());

        assert_eq!(running.len(), 1);
        assert_eq!(get_metadata(&running), vec![2]);
        assert_eq!(tracked.len(), 2);
        assert_eq!(get_metadata(&tracked), vec![2, 3]);

        // Trigger termination of task4
        running[0].cancel();

        let result4 = task4.await.unwrap();
        assert!(result4.is_err());

        assert_eq!(running[0].pending_futures(), 0);
        assert_eq!(running[0].created_futures(), 2);
        assert!(running[0].is_complete());

        let reclaimed = sorted(registry.reclaim());

        assert_eq!(reclaimed.len(), 2);
        assert_eq!(get_metadata(&reclaimed), vec![2, 3]);
        assert_eq!(registry.tracked().len(), 0);
    }

    // Use n+1 threads where n is the number of "blocking" tasks
    // to prevent stalling the tokio executor
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_timing() {
        let registry = TrackerRegistry::new();
        let (tracker1, registration1) = registry.register(1);
        let (tracker2, registration2) = registry.register(2);
        let (tracker3, registration3) = registry.register(3);

        let task1 =
            tokio::spawn(tokio::time::sleep(Duration::from_millis(100)).track(registration1));
        let task2 = tokio::spawn(
            async move { std::thread::sleep(Duration::from_millis(100)) }.track(registration2),
        );

        let task3 = tokio::spawn(
            async move { std::thread::sleep(Duration::from_millis(100)) }
                .track(registration3.clone()),
        );

        let task4 = tokio::spawn(
            async move { std::thread::sleep(Duration::from_millis(100)) }.track(registration3),
        );

        task1.await.unwrap().unwrap();
        task2.await.unwrap().unwrap();
        task3.await.unwrap().unwrap();
        task4.await.unwrap().unwrap();

        assert_eq!(tracker1.pending_futures(), 0);
        assert_eq!(tracker2.pending_futures(), 0);
        assert_eq!(tracker3.pending_futures(), 0);

        assert!(tracker1.is_complete());
        assert!(tracker2.is_complete());
        assert!(tracker3.is_complete());

        assert_eq!(tracker2.created_futures(), 1);
        assert_eq!(tracker2.created_futures(), 1);
        assert_eq!(tracker3.created_futures(), 2);

        let assert_fuzzy = |actual: usize, expected: std::time::Duration| {
            // Number of milliseconds of toleration
            let epsilon = Duration::from_millis(10).as_nanos() as usize;
            let expected = expected.as_nanos() as usize;

            assert!(
                actual > expected.saturating_sub(epsilon),
                "Expected {} got {}",
                expected,
                actual
            );
            assert!(
                actual < expected.saturating_add(epsilon),
                "Expected {} got {}",
                expected,
                actual
            );
        };

        assert_fuzzy(tracker1.cpu_nanos(), Duration::from_millis(0));
        assert_fuzzy(tracker1.wall_nanos(), Duration::from_millis(100));
        assert_fuzzy(tracker2.cpu_nanos(), Duration::from_millis(100));
        assert_fuzzy(tracker2.wall_nanos(), Duration::from_millis(100));
        assert_fuzzy(tracker3.cpu_nanos(), Duration::from_millis(200));
        assert_fuzzy(tracker3.wall_nanos(), Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_register_race() {
        let registry = TrackerRegistry::new();
        let (_, registration) = registry.register(());

        let task1 = tokio::spawn(futures::future::ready(()).track(registration.clone()));
        task1.await.unwrap().unwrap();

        // Should only consider tasks complete once cannot register more Futures
        let reclaimed = registry.reclaim();
        assert_eq!(reclaimed.len(), 0);

        let task2 = tokio::spawn(futures::future::ready(()).track(registration));
        task2.await.unwrap().unwrap();

        let reclaimed = registry.reclaim();
        assert_eq!(reclaimed.len(), 1);
    }

    fn sorted(mut input: Vec<Tracker<i32>>) -> Vec<Tracker<i32>> {
        input.sort_unstable_by_key(|x| *x.metadata());
        input
    }

    fn get_metadata(input: &[Tracker<i32>]) -> Vec<i32> {
        let mut ret: Vec<_> = input.iter().map(|x| *x.metadata()).collect();
        ret.sort_unstable();
        ret
    }
}
