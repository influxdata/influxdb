//! Generic deferred execution of arbitrary [`Future`]'s.

use std::{fmt::Display, sync::Arc, time::Duration};

use futures::Future;
use metric::{self, U64Counter};
use observability_deps::tracing::*;
use parking_lot::Mutex;
use rand::Rng;
use tokio::{
    sync::{
        oneshot::{self, Sender},
        Notify,
    },
    task::JoinHandle,
};

/// [`UNRESOLVED_DISPLAY_STRING`] defines the string shown when invoking the
/// [`Display`] implementation on a [`DeferredLoad`] that has not yet resolved
/// the deferred value.
pub(crate) const UNRESOLVED_DISPLAY_STRING: &str = "<unresolved>";

/// The states of a [`DeferredLoad`] instance.
#[derive(Debug)]
enum State<T> {
    /// The value has not yet been fetched by the background task.
    ///
    /// Sending a value will wake the background task.
    Unresolved(Sender<()>),
    /// The value is being actively resolved by the background task.
    ///
    /// Callers can subscribe to a completion event by waiting on the
    /// [`Notify`].
    Loading(Arc<Notify>),
    /// The value was fetched by the background task and is read to be consumed.
    ///
    /// Only the background task ever sets this state.
    Resolved(T),
}

/// A deferred resolver of `T` in the background, or on demand.
///
/// This implementation combines lazy / deferred loading of `T`, and a
/// background timer that pre-fetches `T` after some random duration of time.
/// Combined, these behaviours provide random jitter for the execution of the
/// resolve [`Future`] across the allowable time range.
///
/// If the [`DeferredLoad`] is dropped and the background task is still
/// incomplete (sleeping / actively fetching `T`) it is aborted immediately. The
/// background task exists once it has successfully resolved `T`.
///
/// # Stale Cached Values
///
/// This is effectively a cache that is pre-fetched in the background - this
/// necessitates that the caller can tolerate, or detect, stale values.
pub(crate) struct DeferredLoad<T> {
    /// The inner state of the [`DeferredLoad`].
    ///
    /// The [`Option`] facilitates taking ownership of the state to transition,
    /// and MUST always be [`Some`] once the mutex is released.
    value: Arc<Mutex<Option<State<T>>>>,
    handle: JoinHandle<()>,
}

impl<T> std::fmt::Debug for DeferredLoad<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredLoad")
            .field("value", &self.value)
            .field("handle", &self.handle)
            .finish()
    }
}

impl<T> Display for DeferredLoad<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value.lock().as_ref().unwrap() {
            State::Unresolved(_) | State::Loading(_) => f.write_str(UNRESOLVED_DISPLAY_STRING),
            State::Resolved(v) => v.fmt(f),
        }
    }
}

impl<T> DeferredLoad<T> {
    /// Provide a hint to the [`DeferredLoad`] that the value will be used soon.
    ///
    /// This allows the value to be materialised in the background, in parallel
    /// while the caller is executing code that will eventually call
    /// [`Self::get()`].
    pub(crate) fn prefetch_now(&self) {
        let mut state = self.value.lock();

        // If the value has already resolved, this call is a NOP.
        if let Some(State::Resolved(_)) = &*state {
            return;
        }

        // Potentially transition the state, discarding the waker.
        let (_waker, new_state) = self.get_load_waker(state.take().unwrap());
        *state = Some(new_state);
    }

    /// Potentially transition `state`, returning the [`Notify`] that will be
    /// signalled when loading the value completes, and the (potentially
    /// changed) state.
    ///
    /// # Panics
    ///
    /// This method panics if `state` is [`State::Resolved`].
    fn get_load_waker(&self, state: State<T>) -> (Arc<Notify>, State<T>) {
        let waker = match state {
            // This caller is the first to demand the value - wake the
            // background task, initialise the notification mechanism and
            // wait for the task to complete.
            State::Unresolved(task_waker) => {
                // Wake the running background task, ignoring any send error
                // as the background task may have concurrently woken up due
                // to the sleep timer and stopped listening on the waker
                // channel.
                let _ = task_waker.send(());

                // Replace the state with a notification for this thread
                // (and others that call get()) to wait on for the
                // concurrent fetch to complete.
                Arc::new(Notify::default())
            }

            // If the value is already being fetched, wait for the fetch to
            // complete.
            State::Loading(waker) => waker,

            // This was checked above before take()-ing the state.
            State::Resolved(_) => unreachable!(),
        };

        // Ensure any subsequent callers can subscribe to the completion
        // event by transitioning to the loading state.
        let state = State::Loading(Arc::clone(&waker));

        // Whenever there is a waker for the caller, the background task
        // MUST be running.
        //
        // This check happens before the state lock is released, ensuring
        // the background task doesn't concurrently finish (it would be
        // blocked waiting to update the state).
        assert!(!self.handle.is_finished());

        (waker, state)
    }
}

impl<T> DeferredLoad<T>
where
    T: Send + Sync + 'static,
{
    /// Construct a [`DeferredLoad`] instance that fetches `T` after at most
    /// `max_wait` duration of time, by executing `F`.
    ///
    /// The background task will wait a uniformly random duration of time
    /// between `[0, max_wait)` before attempting to pre-fetch `T` by executing
    /// the provided future.
    pub(crate) fn new<F>(max_wait: Duration, loader: F, metrics: &metric::Registry) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let ingester_deferred_load_metric = metrics.register_metric::<U64Counter>(
            "ingester_deferred_load",
            "Wrapped loader function completed in background.",
        );
        let background_load_metric =
            ingester_deferred_load_metric.recorder(&[("outcome", "background_load")]);
        let on_demand_metric = ingester_deferred_load_metric.recorder(&[("outcome", "on_demand")]);

        // Init the value container the background thread populates, and
        // populate the starting state with a handle to immediately wake the
        // background task.
        let (tx, rx) = oneshot::channel();
        let value = Arc::new(Mutex::new(Some(State::Unresolved(tx))));

        // Select random duration from a uniform distribution, up to the
        // configured maximum.
        let wait_for = rand::thread_rng().gen_range(Duration::ZERO..max_wait);

        // Spawn the background task, sleeping for the random duration of time
        // before fetching the sort key.
        let handle = tokio::spawn({
            let value = Arc::clone(&value);
            async move {
                // Sleep for the random duration, or until a demand call is
                // made.
                tokio::select! {
                    _ = tokio::time::sleep(wait_for) => {
                        background_load_metric.inc(1);
                        trace!("timeout woke loader task");
                    }
                    _ = rx => {
                        on_demand_metric.inc(1);
                        trace!("demand call woke loader task");
                    }
                }

                // Execute the user-provided future to resolve the actual value.
                let v = loader.await;

                // And attempt to update the value container, if it hasn't
                // already resolved.
                //
                // This will panic if the value has already been resolved, but
                // that should be impossible because this task is the one that
                // resolves it.
                let callers = {
                    let mut guard = value.lock();
                    match guard.take().unwrap() {
                        State::Unresolved(_) => {
                            // The background task woke and completed before any
                            // caller demanded the value.
                            *guard = Some(State::Resolved(v));
                            None
                        }
                        State::Loading(callers) => {
                            // At least one caller is demanding the value, and
                            // must be woken after the lock is released.
                            *guard = Some(State::Resolved(v));
                            Some(callers)
                        }
                        State::Resolved(_) => unreachable!(),
                    }
                };

                // Wake the waiters, if any, outside of the lock to avoid
                // unnecessary contention. If there are >1 threads waiting for
                // the value, they make contend for the value lock however.
                if let Some(callers) = callers {
                    callers.notify_waiters();
                }
            }
        });

        Self { value, handle }
    }
}

impl<T> DeferredLoad<T>
where
    T: Clone + Send + Sync,
{
    /// Read `T`.
    ///
    /// If `T` was pre-fetched in the background, it is returned immediately. If
    /// `T` has not yet been resolved, this call blocks while the [`Future`]
    /// provided at construction is executed.
    ///
    /// # Concurrency
    ///
    /// If this method requires resolving `T`, all callers to this method will
    /// wait for completion while the single background task resolves `T`.
    ///
    /// # Cancellation
    ///
    /// This method is cancellation safe.
    pub(crate) async fn get(&self) -> T {
        let waker = {
            let mut state = self.value.lock();

            // The happy path - the value has been resolved already.
            if let Some(State::Resolved(v)) = &*state {
                return v.clone();
            }

            // If execution reaches here, this call will have to wait for the
            // value to be resolved, and potentially must wake the background
            // task to do so.
            let (waker, new_state) = self.get_load_waker(state.take().unwrap());
            *state = Some(new_state);

            waker
        };

        // Wait for the background task to complete resolving the value.
        waker.notified().await;

        match self.value.lock().as_ref().unwrap() {
            State::Unresolved(_) | State::Loading(_) => unreachable!(),
            State::Resolved(v) => v.clone(),
        }
    }

    /// Optimistically return the deferred value, if it is immediately
    /// available.
    ///
    /// If the value is currently unresolved, or in the process of being
    /// resolved, [`None`] is returned.
    pub(crate) fn peek(&self) -> Option<T> {
        match self.value.lock().as_ref().expect("no deferred load state") {
            State::Unresolved(_) => None,
            State::Loading(_) => None,
            State::Resolved(v) => Some(v.clone()),
        }
    }
}

impl<T> Drop for DeferredLoad<T> {
    fn drop(&mut self) {
        // Attempt to abort the background task, regardless of it having
        // completed or not.
        self.handle.abort()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, future, pin_mut};
    use test_helpers::timeout::FutureTimeout;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Tests that want to exercise the demand loading configure the
    /// DeferredLoad delay to this value, encouraging the background load to
    /// happen a long time in the future.
    ///
    /// Because the delay before the background task runs is selected from a
    /// uniform distribution, it is certainly possible the background task runs
    /// before the demand call, invalidating the test - it's just statistically
    /// very unlikely. Over multiple test runs, the odds of the background task
    /// running before the demand call is so low, that if you see it happen
    /// multiple times, I'd suggest buying a lottery ticket.
    const LONG_LONG_TIME: Duration = Duration::from_secs(100_000_000);

    #[tokio::test]
    async fn test_demand() {
        let d = DeferredLoad::new(LONG_LONG_TIME, async { 42 }, &metric::Registry::default());

        assert_eq!(d.peek(), None);
        assert_eq!(d.get().with_timeout_panic(TIMEOUT).await, 42);
        assert_eq!(d.peek(), Some(42));

        // Subsequent calls also succeed
        assert_eq!(d.get().with_timeout_panic(TIMEOUT).await, 42);
        assert_eq!(d.peek(), Some(42));
    }

    #[tokio::test]
    async fn test_concurrent_demand() {
        let d = Arc::new(DeferredLoad::new(
            LONG_LONG_TIME,
            async {
                // Add a little delay to induce some contention
                tokio::time::sleep(Duration::from_millis(50)).await;
                42
            },
            &metric::Registry::default(),
        ));

        // Synchronise the attempts of the threads.
        let barrier = Arc::new(std::sync::Barrier::new(2));

        // In a different thread (because the barrier is blocking) try to
        // resolve the value in parallel.
        let h = std::thread::spawn({
            let d = Arc::clone(&d);
            let barrier = Arc::clone(&barrier);
            move || {
                let got = block_on(async {
                    barrier.wait();
                    let _ = d.peek(); // Drive some concurrent peeking behaviour
                    d.get().await
                });
                assert_eq!(got, 42);
            }
        });

        barrier.wait();
        assert_eq!(d.get().with_timeout_panic(TIMEOUT).await, 42);

        // Assert the thread didn't panic.
        h.join().expect("second thread panicked resolving value")
    }

    #[tokio::test]
    async fn test_background_load() {
        // Configure the background load to fire (practically) immediately.
        let d = Arc::new(DeferredLoad::new(
            Duration::from_millis(1),
            async { 42 },
            &metric::Registry::default(),
        ));

        // Spin and wait for the state to change to resolved WITHOUT a demand
        // call being made.
        let peek = {
            let d = Arc::clone(&d);
            async {
                loop {
                    match d.value.lock().as_ref().unwrap() {
                        State::Unresolved(_) | State::Loading(_) => {}
                        State::Resolved(v) => return *v,
                    }

                    tokio::task::yield_now().await
                }
            }
            .with_timeout_panic(TIMEOUT)
            .await
        };

        let got = d.get().with_timeout_panic(TIMEOUT).await;
        assert_eq!(got, peek);
        assert_eq!(got, 42);
    }

    #[tokio::test]
    async fn test_background_load_concurrent_demand() {
        // This channel is used to signal the background load has begun.
        let (signal_start, started) = oneshot::channel();

        // This channel is used to block the background task from completing
        // after the above channel has signalled it has begun.
        let (allow_complete, can_complete) = oneshot::channel();

        // Configure the background load to fire (practically) immediately but
        // block waiting for rx to be unblocked.
        //
        // This allows the current thread time to issue a demand and wait on the
        // result before the background load completes.
        let d = Arc::new(DeferredLoad::new(
            Duration::from_millis(1),
            async {
                // Signal the background task has begun.
                signal_start.send(()).expect("test task died");
                // Wait for the test thread to issue the demand call and unblock
                // this fn.
                can_complete.await.expect("sender died");
                42
            },
            &metric::Registry::default(),
        ));

        // Wait for the background task to begin.
        started
            .with_timeout_panic(TIMEOUT)
            .await
            .expect("background task died");

        // Issue a demand call
        let fut = future::maybe_done(d.get());
        pin_mut!(fut);
        assert_eq!(fut.as_mut().take_output(), None);

        // Peek at the current value, which should immediately return None
        assert_eq!(d.peek(), None);

        // Unblock the background task.
        allow_complete.send(()).expect("background task died");

        // And await the demand call
        fut.as_mut().with_timeout_panic(TIMEOUT).await;
        assert_eq!(fut.as_mut().take_output(), Some(42));
        assert_eq!(d.peek(), Some(42));
    }

    #[tokio::test]
    async fn test_display() {
        // This channel is used to block the background task from completing
        // after the above channel has signalled it has begun.
        let (allow_complete, can_complete) = oneshot::channel();

        // Configure the background load to fire (practically) immediately but
        // block waiting for rx to be unblocked.
        let d = Arc::new(DeferredLoad::new(
            Duration::from_millis(1),
            async {
                // Wait for the test thread to issue the demand call and unblock
                // this fn.
                can_complete.await.expect("sender died");
                42
            },
            &metric::Registry::default(),
        ));

        assert_eq!("<unresolved>", d.to_string());

        // Issue a demand call
        let fut = future::maybe_done(d.get());
        pin_mut!(fut);
        assert_eq!(fut.as_mut().take_output(), None);

        assert_eq!("<unresolved>", d.to_string());

        // Unblock the background task.
        allow_complete.send(()).expect("background task died");

        // And await the demand call
        fut.as_mut().await;
        assert_eq!(fut.as_mut().take_output(), Some(42));

        // And assert Display is delegated to the resolved value
        assert_eq!("42", d.to_string());
    }

    #[tokio::test]
    async fn test_prefetch_concurrent_demand() {
        // This channel is used to signal the background load has begun.
        let (signal_start, started) = oneshot::channel();

        // This channel is used to block the background task from completing
        // after the above channel has signalled it has begun.
        let (allow_complete, can_complete) = oneshot::channel();

        // Configure the background load to fire (practically) immediately but
        // block waiting for rx to be unblocked.
        //
        // This allows the current thread time to issue a demand and wait on the
        // result before the background load completes.
        let d = Arc::new(DeferredLoad::new(
            LONG_LONG_TIME,
            async {
                // Signal the background task has begun.
                signal_start.send(()).expect("test task died");
                // Wait for the test thread to issue the demand call and unblock
                // this fn.
                can_complete.await.expect("sender died");
                42
            },
            &metric::Registry::default(),
        ));

        d.prefetch_now();

        // Wait for the background task to begin.
        started
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("background task died");

        // Issue a demand call
        let fut = future::maybe_done(d.get());
        pin_mut!(fut);
        assert_eq!(fut.as_mut().take_output(), None);

        // Unblock the background task.
        allow_complete.send(()).expect("background task died");

        // And await the demand call
        fut.as_mut().await;
        assert_eq!(fut.as_mut().take_output(), Some(42));
    }

    #[tokio::test]
    async fn test_prefetch_already_loaded() {
        let d = Arc::new(DeferredLoad::new(
            LONG_LONG_TIME,
            async { 42 },
            &metric::Registry::default(),
        ));

        let _ = d.get().with_timeout_panic(TIMEOUT).await;
        d.prefetch_now();
    }

    macro_rules! assert_counts {
        (
            $metrics:ident,
            expected_on_demand_cnt = $expected_on_demand_cnt:expr,
            expected_background_load_cnt = $expected_background_load_cnt:expr,
        ) => {
            metric::assert_counter!(
                $metrics,
                U64Counter,
                "ingester_deferred_load",
                labels = metric::Attributes::from(&[("outcome", "on_demand")]),
                value = $expected_on_demand_cnt,
            );
            metric::assert_counter!(
                $metrics,
                U64Counter,
                "ingester_deferred_load",
                labels = metric::Attributes::from(&[("outcome", "background_load")]),
                value = $expected_background_load_cnt,
            );
        };
    }

    #[tokio::test]
    async fn test_on_demand_metric() {
        let metrics = Arc::new(metric::Registry::default());
        let d = DeferredLoad::new(LONG_LONG_TIME, async { 42 }, &metrics);

        assert_eq!(d.get().with_timeout_panic(TIMEOUT).await, 42);
        assert_eq!(d.peek(), Some(42));
        assert_counts!(
            metrics,
            expected_on_demand_cnt = 1,
            expected_background_load_cnt = 0,
        );

        // after resolution, will not increment
        assert_eq!(d.get().with_timeout_panic(TIMEOUT).await, 42);
        assert_counts!(
            metrics,
            expected_on_demand_cnt = 1,
            expected_background_load_cnt = 0,
        );
    }

    #[tokio::test]
    async fn test_background_load_metric() {
        let metrics = Arc::new(metric::Registry::default());
        let d = Arc::new(DeferredLoad::new(
            Duration::from_millis(1),
            async { 42 },
            &metrics,
        ));

        let peek = {
            let d = Arc::clone(&d);
            async {
                loop {
                    match d.peek() {
                        None => {}
                        Some(v) => return v,
                    }
                    tokio::task::yield_now().await
                }
            }
            .with_timeout_panic(TIMEOUT)
            .await
        };
        assert_eq!(peek, 42);
        assert_counts!(
            metrics,
            expected_on_demand_cnt = 0,
            expected_background_load_cnt = 1,
        );

        // after resolution, will not increment
        assert_eq!(Arc::clone(&d).get().with_timeout_panic(TIMEOUT).await, 42);
        assert_counts!(
            metrics,
            expected_on_demand_cnt = 0,
            expected_background_load_cnt = 1,
        );
    }
}
