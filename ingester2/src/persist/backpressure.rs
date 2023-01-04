use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crossbeam_utils::CachePadded;
use metric::DurationCounter;
use observability_deps::tracing::*;
use parking_lot::Mutex;
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{Instant, Interval, MissedTickBehavior},
};

/// The interval of time between evaluations of the state of the persist system
/// when [`CurrentState::Saturated`].
const EVALUATE_SATURATION_INTERVAL: Duration = Duration::from_secs(1);

/// A state of the persist system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurrentState {
    /// The system is operating normally.
    Ok,
    /// The persist system is overloaded.
    Saturated,
}

/// A handle to read (and set, within the persist module) the state of the
/// persist system.
///
/// Clone operations are cheap, and state read operations are very cheap.
///
/// # Saturation Recovery
///
/// Once the persist system is marked as [`CurrentState::Saturated`], it remains
/// in that state until the following conditions are satisfied:
///
///   * There are no outstanding enqueue operations (no thread is blocked adding
///     an item to any work queue).
///
///   * The number of outstanding persist jobs is less than 50% of
///     `persist_queue_depth`
///
/// These conditions are evaluated periodically, at the interval specified in
/// [`EVALUATE_SATURATION_INTERVAL`].
#[derive(Debug)]
pub(crate) struct PersistState {
    /// The actual state value.
    ///
    /// The value of this variable is set to the [`CurrentState`] discriminant
    /// for the respective state that was [`PersistState::set()`] in it.
    ///
    /// This is cache padded due to the high read volume, preventing any
    /// unfortunate false-sharing of cache lines from impacting the hot-path
    /// reads.
    state: CachePadded<AtomicUsize>,

    /// Tracks the number of [`WaitGuard`] instances, which in turn tracks the
    /// number of async tasks waiting within `PersistHandle::enqueue()` to
    /// obtain a semaphore permit and enqueue a persist job.
    ///
    /// This is modified using [`Ordering::SeqCst`] as performance is not a
    /// priority for code paths that modify it.
    waiting_to_enqueue: Arc<AtomicUsize>,

    /// The persist task semaphore with a maximum of `persist_queue_depth`
    /// permits allocatable.
    sem: Arc<Semaphore>,
    persist_queue_depth: usize,

    /// The handle to the current saturation evaluation/recovery task, if any.
    recovery_handle: Mutex<Option<JoinHandle<()>>>,

    /// A counter tracking the number of nanoseconds the state value is set to
    /// [`CurrentState::Saturated`].
    saturated_duration: DurationCounter,
}

impl PersistState {
    /// Initialise a [`PersistState`] with [`CurrentState::Ok`], with a total
    /// number of tasks bounded to `persist_queue_depth` and permits issued from
    /// `sem`.
    pub(crate) fn new(
        persist_queue_depth: usize,
        sem: Arc<Semaphore>,
        metrics: &metric::Registry,
    ) -> Self {
        // The persist_queue_depth should be the maximum number of permits
        // available in the semaphore.
        assert!(persist_queue_depth >= sem.available_permits());
        // This makes no sense and later we divide by this value.
        assert!(
            persist_queue_depth > 0,
            "persist queue depth must be non-zero"
        );

        let saturated_duration = metrics
            .register_metric::<DurationCounter>(
                "ingester_persist_saturated_duration",
                "the duration of time the persist system was marked as saturated",
            )
            .recorder(&[]);

        let s = Self {
            state: Default::default(),
            waiting_to_enqueue: Arc::new(AtomicUsize::new(0)),
            recovery_handle: Default::default(),
            persist_queue_depth,
            sem,
            saturated_duration,
        };
        s.set(CurrentState::Ok);
        s
    }

    /// Set the reported state of the [`PersistState`].
    fn set(&self, s: CurrentState) -> bool {
        // Set the new state, retaining the most recent state.
        //
        // SeqCst is absolute overkill, but is used here due to the strong
        // ordering guarantees providing minimal risk of bugs. The low volume of
        // writes to this variable means the overhead is more than acceptable.
        let last = self.state.swap(s as usize, Ordering::SeqCst);

        // If "s" does not match the old state, this is the first thread to
        // switch the state from "last", to "s", since setting it to "last".
        //
        // Subsequent calls setting the state to "s" will return false, until a
        // different state is set.
        s as usize != last
    }

    /// Get the current reported state of the [`PersistState`].
    ///
    /// Reading this value is extremely cheap and can be done without
    /// performance concern.
    ///
    /// This value is eventually consistent, with a presumption of being visible
    /// in a reasonable amount of time.
    #[inline(always)]
    pub(crate) fn get(&self) -> CurrentState {
        // Correctness: relaxed as reading the current state is allowed to be
        // racy for performance reasons; this call should be as cheap as
        // possible due to it being squarely in the hot path.
        //
        // Any value change will "eventually" be made visible to all threads, at
        // which point this read converges to the latest value. A potential
        // extra write or two arriving before this value is visible to all
        // threads is acceptable in the "saturated" cold path, prioritising
        // latency of the hot path.
        match self.state.load(Ordering::Relaxed) {
            v if v == CurrentState::Ok as usize => CurrentState::Ok,
            v if v == CurrentState::Saturated as usize => CurrentState::Saturated,
            _ => unreachable!(),
        }
    }

    /// A convenience method that returns true if `self` is
    /// [`CurrentState::Saturated`].
    #[inline(always)]
    pub(crate) fn is_saturated(&self) -> bool {
        self.get() == CurrentState::Saturated
    }

    /// Mark the persist system as saturated, returning a [`WaitGuard`] that
    /// MUST be held during any subsequent async-blocking to acquire a permit
    /// from the persist semaphore.
    ///
    /// Holding the guard over the `acquire()` await allows the saturation
    /// evaluation to track the number of threads with an ongoing enqueue wait.
    pub(super) fn set_saturated(s: Arc<Self>) -> WaitGuard {
        // Increment the number of tasks waiting to obtain a permit and push
        // into any queue.
        //
        // INVARIANT: this increment MUST happen-before returning the guard, and
        // waiting on the semaphore acquire(), and before starting the
        // saturation monitor task so that it observes this waiter.
        let _ = s.waiting_to_enqueue.fetch_add(1, Ordering::SeqCst);

        // Attempt to set the system to "saturated".
        let first = s.set(CurrentState::Saturated);
        if first {
            // This is the first thread to mark the system as saturated.
            warn!("persist queue saturated, blocking ingest");

            // Always check the state of the system EVALUATE_SATURATION_INTERVAL
            // duration of time after the last completed evaluation - do not
            // attempt to check continuously should the check fall behind the
            // ticker.
            let mut interval = tokio::time::interval(EVALUATE_SATURATION_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            // Spawn a task that marks the system as not saturated after the
            // workers have processed some of the backlog.
            let h = tokio::spawn(saturation_monitor_task(
                interval,
                Arc::clone(&s),
                s.persist_queue_depth,
                Arc::clone(&s.sem),
            ));
            // Retain the task handle to avoid leaking it if dropped.
            *s.recovery_handle.lock() = Some(h);
        }

        WaitGuard(Arc::clone(&s.waiting_to_enqueue))
    }

    /// A test-only helper that sets the state of `self` only. It does not spawn
    /// a recovery task.
    #[cfg(test)]
    pub(crate) fn test_set_state(&self, s: CurrentState) {
        self.set(s);
    }
}

impl Drop for PersistState {
    fn drop(&mut self) {
        if let Some(h) = self.recovery_handle.lock().as_ref() {
            h.abort();
        }
    }
}

/// A guard that decrements the number of writers waiting to obtain a permit
/// from the persistence semaphore.
///
/// This MUST be held whilst calling [`Semaphore::acquire()`].
#[must_use = "must hold wait guard while waiting for enqueue"]
pub(super) struct WaitGuard(Arc<AtomicUsize>);

impl Drop for WaitGuard {
    fn drop(&mut self) {
        let _ = self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A task that monitors the `waiters` and `sem` to determine when the persist
/// system is no longer saturated.
///
/// Once the system is no longer saturated (as determined according to the
/// documentation for [`PersistState`]), the [`PersistState`] is set to
/// [`CurrentState::Ok`].
async fn saturation_monitor_task(
    mut interval: Interval,
    state: Arc<PersistState>,
    persist_queue_depth: usize,
    sem: Arc<Semaphore>,
) {
    let mut last = Instant::now();
    loop {
        // Wait before evaluating the state of the system.
        interval.tick().await;

        // Update the saturation metric after the tick.
        //
        // For the first tick, this covers the tick wait itself. For subsequent
        // ticks, this duration covers the evaluation time + tick wait.
        let now = Instant::now();
        state.saturated_duration.inc(now.duration_since(last));
        last = now;

        // INVARIANT: this task only ever runs when the system is saturated.
        assert!(state.is_saturated());

        // First check if any tasks are waiting to obtain a permit and enqueue
        // an item (an indication that one or more queues is full).
        let n_waiting = state.waiting_to_enqueue.load(Ordering::SeqCst);
        if n_waiting > 0 {
            warn!(
                n_waiting,
                "waiting for outstanding persist jobs to be enqueued"
            );
            continue;
        }

        // No async task WAS currently waiting for a permit to enqueue a persist
        // job when checking above, but one may want to immediately await one
        // now (or later).
        //
        // In order to minimise health flip-flopping, only mark the persist
        // system as healthy once there is some capacity in the semaphore to
        // accept new persist jobs. This avoids the semaphore having 1 permit
        // free, only to be immediately acquired and the system pause again.
        //
        // This check below ensures that the semaphore is at least half capacity
        // before marking the system as recovered.
        let available = sem.available_permits();
        let outstanding = persist_queue_depth.checked_sub(available).unwrap();
        if !has_sufficient_capacity(available, persist_queue_depth) {
            warn!(
                available,
                outstanding, "waiting for outstanding persist jobs to reduce"
            );
            continue;
        }

        // There are no outstanding enqueue waiters, and all queues are at half
        // capacity or better.
        info!(
            available,
            outstanding, "persist queue saturation reduced, resuming ingest"
        );

        // INVARIANT: there is only ever one task that monitors the queue state
        // and transitions the persist state to OK, therefore this task is
        // always the first to set the state to OK.
        assert!(state.set(CurrentState::Ok));

        // The task MUST immediately stop so any subsequent saturation is
        // handled by the newly spawned task, upholding the above invariant.
        return;
    }
}

/// Returns true if `capacity` is sufficient to be considered ready for more
/// requests to be enqueued.
fn has_sufficient_capacity(capacity: usize, max_capacity: usize) -> bool {
    // Did this fire? You have your arguments the wrong way around.
    assert!(capacity <= max_capacity);

    let want_at_least = (max_capacity + 1) / 2;

    capacity >= want_at_least
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metric::Metric;
    use test_helpers::timeout::FutureTimeout;

    use super::*;

    const QUEUE_DEPTH: usize = 42;
    const POLL_INTERVAL: Duration = Duration::from_millis(5);

    /// Execute `f` with the current value of the
    /// "ingester_persist_saturated_duration" metric.
    #[track_caller]
    fn assert_saturation_time<F>(metrics: &metric::Registry, f: F)
    where
        F: FnOnce(Duration) -> bool,
    {
        // Get the saturated duration counter that tracks the time spent in the
        // "saturated" state.
        let duration_counter = metrics
            .get_instrument::<Metric<DurationCounter>>("ingester_persist_saturated_duration")
            .expect("constructor did not create required duration metric")
            .recorder(&[]);

        // Call the assert closure
        assert!(f(duration_counter.fetch()));
    }

    #[test]
    fn test_has_sufficient_capacity() {
        // A queue of minimal depth (1).
        //
        // Validates there are no off-by-one errors.
        assert!(!has_sufficient_capacity(0, 1));
        assert!(has_sufficient_capacity(1, 1));

        // Even queues
        assert!(!has_sufficient_capacity(0, 2));
        assert!(has_sufficient_capacity(1, 2));
        assert!(has_sufficient_capacity(2, 2));

        // Odd queues
        assert!(!has_sufficient_capacity(0, 3));
        assert!(!has_sufficient_capacity(1, 3));
        assert!(has_sufficient_capacity(2, 3));
        assert!(has_sufficient_capacity(3, 3));
    }

    /// Validate the state setters and getters are correct, and that only the
    /// first thread that changes the state observes the "first=true" response.
    #[test]
    fn test_state_transitions() {
        let metrics = metric::Registry::default();
        let sem = Arc::new(Semaphore::new(QUEUE_DEPTH));
        let s = PersistState::new(QUEUE_DEPTH, sem, &metrics);
        assert_eq!(s.get(), CurrentState::Ok);
        assert!(!s.is_saturated());

        assert!(!s.set(CurrentState::Ok)); // Already OK
        assert_eq!(s.get(), CurrentState::Ok);
        assert!(!s.is_saturated());

        assert!(!s.set(CurrentState::Ok)); // Already OK
        assert_eq!(s.get(), CurrentState::Ok);
        assert!(!s.is_saturated());

        assert!(s.set(CurrentState::Saturated)); // First to change
        assert!(s.is_saturated());
        assert!(!s.set(CurrentState::Saturated)); // Not first
        assert!(s.is_saturated());
        assert_eq!(s.get(), CurrentState::Saturated);
        assert!(s.is_saturated());

        assert!(!s.set(CurrentState::Saturated)); // Not first
        assert_eq!(s.get(), CurrentState::Saturated);
        assert!(s.is_saturated());

        assert!(s.set(CurrentState::Ok)); // First to change
        assert_eq!(s.get(), CurrentState::Ok);
        assert!(!s.is_saturated());
    }

    /// Ensure that the saturation evaluation checks for outstanding enqueue
    /// waiters (as tracked by the [`WaitGuard`]).
    #[tokio::test]
    async fn test_saturation_recovery_enqueue_waiters() {
        let metrics = metric::Registry::default();
        let sem = Arc::new(Semaphore::new(QUEUE_DEPTH));
        let s = Arc::new(PersistState::new(QUEUE_DEPTH, Arc::clone(&sem), &metrics));

        // Use no queues to ensure only the waiters are blocking recovery.

        assert!(!s.is_saturated());
        assert_saturation_time(&metrics, |d| d == Duration::ZERO);

        // Obtain the current timestamp, and use it as an upper-bound on the
        // duration of saturation.
        let duration_upper_bound = Instant::now();

        let w1 = PersistState::set_saturated(Arc::clone(&s));
        let w2 = PersistState::set_saturated(Arc::clone(&s));

        assert!(s.is_saturated());

        // Kill the actual recovery task (there must be one running at this
        // point).
        s.recovery_handle.lock().take().unwrap().abort();

        // Spawn a replacement that ticks way more often to speed up the test.
        let h = tokio::spawn(saturation_monitor_task(
            tokio::time::interval(POLL_INTERVAL),
            Arc::clone(&s),
            QUEUE_DEPTH,
            sem,
        ));

        // Drop a waiter and ensure the system is still saturated.
        drop(w1);
        assert!(s.is_saturated());

        // Sleep a little to ensure it remains saturated with 1 outstanding
        // waiter.
        //
        // This is false-negative racy - if this assert fires, there is a
        // legitimate problem - one outstanding waiter should prevent the system
        // from ever transitioning to a healthy state.
        tokio::time::sleep(POLL_INTERVAL * 4).await;
        assert!(s.is_saturated());
        assert_saturation_time(&metrics, |d| d > Duration::ZERO);

        // Drop the other waiter.
        drop(w2);

        // Wait up to 5 seconds to observe the system recovery.
        async {
            loop {
                if !s.is_saturated() {
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Assert the saturation metric reports a duration of at least 1 poll
        // interval (the lower bound necessary for the above recovery to occur)
        // and the maximum bound (the time since the system entered the
        // saturated state).
        assert_saturation_time(&metrics, |d| d >= POLL_INTERVAL);
        assert_saturation_time(&metrics, |d| {
            d < Instant::now().duration_since(duration_upper_bound)
        });

        // Wait up to 60 seconds to observe the recovery task finish.
        //
        // The recovery task sets the system state as healthy, and THEN exits,
        // so there exists a window of time where the system has passed the
        // saturation check above, but the recovery task MAY still be running.
        //
        // By waiting an excessive duration of time, we ensure the task does
        // indeed finish.
        async {
            loop {
                if h.is_finished() {
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(60))
        .await;

        // No task panic occurred.
        assert!(h.with_timeout_panic(Duration::from_secs(5)).await.is_ok());
        assert!(!s.is_saturated());
    }

    /// Ensure that the saturation evaluation checks for free queue slots before
    /// marking the system as healthy.
    #[tokio::test]
    async fn test_saturation_recovery_queue_capacity() {
        let metrics = metric::Registry::default();
        let sem = Arc::new(Semaphore::new(QUEUE_DEPTH));
        let s = Arc::new(PersistState::new(QUEUE_DEPTH, Arc::clone(&sem), &metrics));

        // Use no waiters to ensure only the queue slots are blocking recovery.

        assert!(!s.is_saturated());
        assert_saturation_time(&metrics, |d| d == Duration::ZERO);

        // Obtain the current timestamp, and use it as an upper-bound on the
        // duration of saturation.
        let duration_upper_bound = Instant::now();

        // Take half the permits. Holding this number of permits should allow
        // the state to transition to healthy.
        let _half_the_permits = sem.acquire_many(QUEUE_DEPTH as u32 / 2).await.unwrap();

        // Obtain a permit, pushing it over the "healthy" limit.
        let permit = sem.acquire().await.unwrap();

        assert!(!s.is_saturated());
        assert!(s.set(CurrentState::Saturated));
        assert!(s.is_saturated());

        // Spawn the recovery task directly, not via set_saturated() for
        // simplicity - the test above asserts the task is started by a call to
        // set_saturated().
        let h = tokio::spawn(saturation_monitor_task(
            tokio::time::interval(POLL_INTERVAL),
            Arc::clone(&s),
            QUEUE_DEPTH,
            Arc::clone(&sem),
        ));

        // Wait a little and ensure the state hasn't changed.
        //
        // While this could be a false negative, if this assert fires there is a
        // legitimate problem.
        tokio::time::sleep(POLL_INTERVAL * 4).await;
        assert!(s.is_saturated());

        // Drop the permit so that the outstanding permits drops below the threshold for recovery.
        drop(permit);

        // Wait up to 5 seconds to observe the system recovery.
        async {
            loop {
                if !s.is_saturated() {
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Assert the saturation metric reports a duration of at least 1 poll
        // interval (the lower bound necessary for the above recovery to occur)
        // and the maximum bound (the time since the system entered the
        // saturated state).
        assert_saturation_time(&metrics, |d| d >= POLL_INTERVAL);
        assert_saturation_time(&metrics, |d| {
            d < Instant::now().duration_since(duration_upper_bound)
        });

        // Wait up to 60 seconds to observe the recovery task finish.
        //
        // The recovery task sets the system state as healthy, and THEN exits,
        // so there exists a window of time where the system has passed the
        // saturation check above, but the recovery task MAY still be running.
        //
        // By waiting an excessive duration of time, we ensure the task does
        // indeed finish.
        async {
            loop {
                if h.is_finished() {
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(60))
        .await;

        // No task panic occurred.
        assert!(h.with_timeout_panic(Duration::from_secs(5)).await.is_ok());
        assert!(!s.is_saturated());
    }
}
