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
//! The key correctness property of the Tracker system is Tracker::get_status
//! only returns Complete when all futures associated with the tracker have
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
//! 10. Tracker::get_status returns Complete if it observes
//! pending_registrations to be 0 and then pending_futures to be 0
//!
//! 11. 9 + 10 -> A thread can only observe a tracker to be complete
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

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio_util::sync::CancellationToken;

pub use future::{TrackedFuture, TrackedFutureExt};
pub use history::TaskRegistryWithHistory;
pub use registry::{TaskId, TaskRegistry};
use tokio::sync::Notify;

mod future;
mod history;
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

    notify: Notify,
}

/// The status of the tracked task
#[derive(Debug, Clone)]
pub enum TaskStatus {
    /// More futures can be registered
    Creating,

    /// No more futures can be registered
    ///
    /// `pending_count` and `cpu_nanos` are best-effort -
    /// they may not be the absolute latest values.
    ///
    /// `total_count` is guaranteed to be the final value
    Running {
        /// The number of created futures
        total_count: usize,
        /// The number of pending futures
        pending_count: usize,
        /// The total amount of CPU time spent executing the futures
        cpu_nanos: usize,
    },

    /// All futures have been dropped and no more can be registered
    ///
    /// All values are guaranteed to be the final values
    Complete {
        /// The number of created futures
        total_count: usize,
        /// The total amount of CPU time spent executing the futures
        cpu_nanos: usize,
        /// The number of nanoseconds between tracker registration and
        /// the last TrackedFuture being dropped
        wall_nanos: usize,
    },
}

impl TaskStatus {
    /// return a human readable name for this status
    pub fn name(&self) -> &'static str {
        match self {
            Self::Creating => "Creating",
            Self::Running { .. } => "Running",
            Self::Complete { .. } => "Complete",
        }
    }

    /// If the job is running or competed, returns the total amount of CPU time
    /// spent executing futures
    pub fn cpu_nanos(&self) -> Option<usize> {
        match self {
            Self::Creating => None,
            Self::Running { cpu_nanos, .. } => Some(*cpu_nanos),
            Self::Complete { cpu_nanos, .. } => Some(*cpu_nanos),
        }
    }

    /// If the job has competed, returns the total amount of wall clock time
    /// spent executing futures
    pub fn wall_nanos(&self) -> Option<usize> {
        match self {
            Self::Creating => None,
            Self::Running { .. } => None,
            Self::Complete { wall_nanos, .. } => Some(*wall_nanos),
        }
    }
}

/// A Tracker can be used to monitor/cancel/wait for a set of associated futures
#[derive(Debug)]
pub struct TaskTracker<T> {
    id: TaskId,
    state: Arc<TrackerState>,
    metadata: Arc<T>,
}

impl<T> Clone for TaskTracker<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state: Arc::clone(&self.state),
            metadata: Arc::clone(&self.metadata),
        }
    }
}

impl<T> TaskTracker<T> {
    /// Creates a new task tracker from the provided registration
    pub fn new(id: TaskId, registration: &TaskRegistration, metadata: T) -> Self {
        Self {
            id,
            metadata: Arc::new(metadata),
            state: Arc::clone(&registration.state),
        }
    }

    /// Returns a complete task tracker
    pub fn complete(metadata: T) -> Self {
        let registration = TaskRegistration::new();
        Self::new(TaskId(0), &registration, metadata)
    }

    /// Returns the ID of the Tracker - these are unique per TrackerRegistry
    pub fn id(&self) -> TaskId {
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

    /// Returns true if all futures associated with this tracker have
    /// been dropped and no more can be created
    pub fn is_complete(&self) -> bool {
        matches!(self.get_status(), TaskStatus::Complete { .. })
    }

    /// Gets the status of the tracker
    pub fn get_status(&self) -> TaskStatus {
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

        match (pending_registrations == 0, pending_futures == 0) {
            (false, _) => TaskStatus::Creating,
            (true, false) => TaskStatus::Running {
                total_count: self.state.created_futures.load(Ordering::Relaxed),
                pending_count: self.state.pending_futures.load(Ordering::Relaxed),
                cpu_nanos: self.state.cpu_nanos.load(Ordering::Relaxed),
            },
            (true, true) => TaskStatus::Complete {
                total_count: self.state.created_futures.load(Ordering::Relaxed),
                cpu_nanos: self.state.cpu_nanos.load(Ordering::Relaxed),
                wall_nanos: self.state.wall_nanos.load(Ordering::Relaxed),
            },
        }
    }

    /// Returns if this tracker has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.state.cancel_token.is_cancelled()
    }

    /// Blocks until all futures associated with the tracker have been
    /// dropped and no more can be created
    pub async fn join(&self) {
        // Notify is notified when pending_futures hits 0 AND when pending_registrations
        // hits 0. In almost all cases join won't be called before pending_registrations
        // has already hit 0, but in the extremely rare case this occurs the loop
        // handles the spurious wakeup
        loop {
            // Request notification before checking if complete
            // to avoid a race condition
            let notify = self.state.notify.notified();

            if self.is_complete() {
                return;
            }

            notify.await
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
pub struct TaskRegistration {
    state: Arc<TrackerState>,
}

impl Clone for TaskRegistration {
    fn clone(&self) -> Self {
        self.state
            .pending_registrations
            .fetch_add(1, Ordering::Relaxed);

        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl Default for TaskRegistration {
    fn default() -> Self {
        let state = Arc::new(TrackerState {
            start_instant: Instant::now(),
            cpu_nanos: AtomicUsize::new(0),
            wall_nanos: AtomicUsize::new(0),
            cancel_token: CancellationToken::new(),
            created_futures: AtomicUsize::new(0),
            pending_futures: AtomicUsize::new(0),
            pending_registrations: AtomicUsize::new(1),
            notify: Notify::new(),
        });

        Self { state }
    }
}

impl TaskRegistration {
    pub fn new() -> Self {
        Self::default()
    }

    /// Converts the registration into a tracker with id 0 and specified metadata
    pub fn into_tracker<T>(self, metadata: T) -> TaskTracker<T> {
        TaskTracker::new(TaskId(0), &self, metadata)
    }
}

impl Drop for TaskRegistration {
    fn drop(&mut self) {
        // This synchronizes with the Acquire load in Tracker::get_status
        let previous = self
            .state
            .pending_registrations
            .fetch_sub(1, Ordering::Release);

        // This implies a TrackerRegistration has been cloned without it incrementing
        // the pending_registration counter
        assert_ne!(previous, 0);

        // Need to signal potential completion
        if previous == 1 {
            // Perform an acquire load to establish ordering with respect
            // to all other decrements
            self.state.pending_futures.load(Ordering::Acquire);

            self.state.notify.notify_waiters();
        }
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
        let mut registry = TaskRegistry::new();
        let (tracker, registration) = registry.register(());

        tokio::spawn(receive.track(registration));

        assert_eq!(registry.running().len(), 1);

        sender.send(()).unwrap();
        tracker.join().await;

        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_interleaved() {
        let (sender1, receive1) = oneshot::channel();
        let (sender2, receive2) = oneshot::channel();
        let mut registry = TaskRegistry::new();
        let (t1, registration1) = registry.register(1);
        let (t2, registration2) = registry.register(2);

        tokio::spawn(receive1.track(registration1));
        tokio::spawn(receive2.track(registration2));

        let tracked = sorted(registry.running());
        assert_eq!(get_metadata(&tracked), vec![1, 2]);

        sender2.send(()).unwrap();
        t2.join().await;

        let tracked: Vec<_> = sorted(registry.running());
        assert_eq!(get_metadata(&tracked), vec![1]);

        sender1.send(42).unwrap();
        t1.join().await;

        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_drop() {
        let mut registry = TaskRegistry::new();
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
        let mut registry = TaskRegistry::new();
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
        let mut registry = TaskRegistry::new();
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
        let mut registry = TaskRegistry::new();
        let (tracker, registration) = registry.register(());
        tracker.cancel();

        let task1 = tokio::spawn(futures::future::pending::<()>().track(registration));
        let result1 = task1.await.unwrap();

        assert!(result1.is_err());
        assert_eq!(registry.running().len(), 0);
    }

    #[tokio::test]
    async fn test_terminate_multiple() {
        let mut registry = TaskRegistry::new();
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
        let mut registry = TaskRegistry::new();

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
        let reclaimed = sorted(registry.reclaim().collect());
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

        assert!(matches!(
            tracked[0].get_status(),
            TaskStatus::Running {
                pending_count: 1,
                total_count: 2,
                ..
            }
        ));

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
        assert!(matches!(
            running[0].get_status(),
            TaskStatus::Complete { total_count: 2, .. }
        ));

        let reclaimed = sorted(registry.reclaim().collect());

        assert_eq!(reclaimed.len(), 2);
        assert_eq!(get_metadata(&reclaimed), vec![2, 3]);
        assert_eq!(registry.tracked().len(), 0);
    }

    // Use n+1 threads where n is the number of "blocking" tasks
    // to prevent stalling the tokio executor
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_timing() {
        let mut registry = TaskRegistry::new();
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

        let assert_fuzzy = |actual: usize, expected: std::time::Duration| {
            // Number of milliseconds of toleration
            let epsilon = Duration::from_millis(25).as_nanos() as usize;
            let expected = expected.as_nanos() as usize;

            // std::thread::sleep is guaranteed to take at least as long as requested
            assert!(actual > expected, "Expected {} got {}", expected, actual);
            assert!(
                actual < expected.saturating_add(epsilon),
                "Expected {} got {}",
                expected,
                actual
            );
        };

        let assert_complete = |status: TaskStatus,
                               expected_cpu: std::time::Duration,
                               expected_wall: std::time::Duration| {
            match status {
                TaskStatus::Complete {
                    cpu_nanos,
                    wall_nanos,
                    ..
                } => {
                    assert_fuzzy(cpu_nanos, expected_cpu);
                    assert_fuzzy(wall_nanos, expected_wall);
                }
                _ => panic!("expected complete got {:?}", status),
            }
        };

        assert_complete(
            tracker1.get_status(),
            Duration::from_millis(0),
            Duration::from_millis(100),
        );
        assert_complete(
            tracker2.get_status(),
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        assert_complete(
            tracker3.get_status(),
            Duration::from_millis(200),
            Duration::from_millis(100),
        );
    }

    #[tokio::test]
    async fn test_register_race() {
        let mut registry = TaskRegistry::new();
        let (_, registration) = registry.register(());

        let task1 = tokio::spawn(futures::future::ready(()).track(registration.clone()));
        task1.await.unwrap().unwrap();

        let tracked = registry.tracked();
        assert_eq!(tracked.len(), 1);
        assert!(matches!(&tracked[0].get_status(), TaskStatus::Creating));

        // Should only consider tasks complete once cannot register more Futures
        let reclaimed: Vec<_> = registry.reclaim().collect();
        assert_eq!(reclaimed.len(), 0);

        let task2 = tokio::spawn(futures::future::ready(()).track(registration));
        task2.await.unwrap().unwrap();

        let reclaimed: Vec<_> = registry.reclaim().collect();
        assert_eq!(reclaimed.len(), 1);
    }

    #[tokio::test]
    async fn test_join() {
        use std::future::Future;
        use std::task::Poll;

        let mut registry = TaskRegistry::new();
        let (tracker, registration) = registry.register(());

        let (s1, r1) = oneshot::channel();
        let task1 = tokio::spawn(
            async move {
                r1.await.unwrap();
            }
            .track(registration.clone()),
        );

        let (s2, r2) = oneshot::channel();
        let task2 = tokio::spawn(
            async move {
                r2.await.unwrap();
            }
            .track(registration.clone()),
        );

        // This executor goop is necessary to get a future into
        // a state where it is waiting on the Notify resource

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);
        let fut_tracker = tracker.clone();
        let fut = fut_tracker.join();
        futures::pin_mut!(fut);

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);
        assert_eq!(poll, Poll::Pending);

        assert!(matches!(tracker.get_status(), TaskStatus::Creating));

        s1.send(()).unwrap();
        task1.await.unwrap().unwrap();

        assert!(matches!(tracker.get_status(), TaskStatus::Creating));

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);
        assert_eq!(poll, Poll::Pending);

        s2.send(()).unwrap();
        task2.await.unwrap().unwrap();

        assert!(matches!(tracker.get_status(), TaskStatus::Creating));

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);
        assert_eq!(poll, Poll::Pending);

        std::mem::drop(registration);

        assert!(matches!(tracker.get_status(), TaskStatus::Complete { .. }));

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);
        assert_eq!(poll, Poll::Ready(()));
    }

    #[tokio::test]
    async fn test_join_no_registration() {
        use std::future::Future;
        use std::task::Poll;

        let mut registry = TaskRegistry::new();
        let (tracker, registration) = registry.register(());

        // This executor goop is necessary to get a future into
        // a state where it is waiting on the Notify resource

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);
        let fut = tracker.join();
        futures::pin_mut!(fut);

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);

        assert_eq!(poll, Poll::Pending);

        std::mem::drop(registration);

        let poll = std::pin::Pin::new(&mut fut).poll(&mut cx);

        assert_eq!(poll, Poll::Ready(()));
    }

    fn sorted(mut input: Vec<TaskTracker<i32>>) -> Vec<TaskTracker<i32>> {
        input.sort_unstable_by_key(|x| *x.metadata());
        input
    }

    fn get_metadata(input: &[TaskTracker<i32>]) -> Vec<i32> {
        let mut ret: Vec<_> = input.iter().map(|x| *x.metadata()).collect();
        ret.sort_unstable();
        ret
    }
}
