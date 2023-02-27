//! This module contains a dedicated thread pool for running "cpu
//! intensive" workloads such as DataFusion plans
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use std::{
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::oneshot::{error::RecvError, Receiver};
use tokio_util::sync::CancellationToken;

use futures::{
    future::{BoxFuture, Shared},
    ready, Future, FutureExt, TryFutureExt,
};

use observability_deps::tracing::warn;

/// Task that can be added to the executor-internal queue.
///
/// Every task within the executor is represented by a [`Job`] that can be polled by the API user.
struct Task {
    fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    cancel: CancellationToken,

    #[allow(dead_code)]
    task_ref: Arc<()>,
}

impl Task {
    /// Run task.
    ///
    /// This runs the payload or cancels if the linked [`Job`] is dropped.
    async fn run(self) {
        tokio::select! {
            _ = self.cancel.cancelled() => (),
            _ = self.fut => (),
        }
    }
}

/// The type of error that is returned from tasks in this module
pub type Error = String;

/// Job within the executor.
///
/// Dropping the job will cancel its linked task.
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct Job<T> {
    cancel: CancellationToken,
    detached: bool,
    #[pin]
    rx: Receiver<Result<T, String>>,
}

impl<T> Job<T> {
    /// Detached job so dropping it does not cancel it.
    ///
    /// You must ensure that this task eventually finishes, otherwise [`DedicatedExecutor::join`] may never return!
    pub fn detach(mut self) {
        // cannot destructure `Self` because we implement `Drop`, so we use a flag instead to prevent cancellation.
        self.detached = true;
    }
}

impl<T> Future for Job<T> {
    type Output = Result<T, Error>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match ready!(this.rx.poll(cx)) {
            Ok(res) => std::task::Poll::Ready(res),
            Err(_) => std::task::Poll::Ready(Err(String::from(
                "Worker thread gone, executor was likely shut down",
            ))),
        }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for Job<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.detached {
            self.cancel.cancel();
        }
    }
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor
#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<Mutex<State>>,

    /// Number of threads
    num_threads: NonZeroUsize,

    /// Used for testing.
    ///
    /// This will ignore explicit shutdown requests.
    testing: bool,
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor
struct State {
    /// Channel for requests -- the dedicated executor takes requests
    /// from here and runs them.
    ///
    /// This is `None` if we triggered shutdown.
    requests: Option<std::sync::mpsc::Sender<Task>>,

    /// Receiver side indicating that shutdown is complete.
    completed_shutdown: Shared<BoxFuture<'static, Result<(), Arc<RecvError>>>>,

    /// Task counter (uses Arc strong count).
    task_refs: Arc<()>,

    /// The inner thread that can be used to join during drop.
    thread: Option<std::thread::JoinHandle<()>>,
}

// IMPORTANT: Implement `Drop` for `State`, NOT for `DedicatedExecutor`, because the executor can be cloned and clones
// share their inner state.
impl Drop for State {
    fn drop(&mut self) {
        if self.requests.is_some() {
            warn!("DedicatedExecutor dropped without calling shutdown()");
            self.requests = None;
        }

        // do NOT poll the shared future if we are panicking due to https://github.com/rust-lang/futures-rs/issues/2575
        if !std::thread::panicking() && self.completed_shutdown.clone().now_or_never().is_none() {
            warn!("DedicatedExecutor dropped without waiting for worker termination",);
        }

        // join thread but don't care about the results
        self.thread.take().expect("not dropped yet").join().ok();
    }
}

/// The default worker priority (value passed to `libc::setpriority`);
const WORKER_PRIORITY: i32 = 10;

impl std::fmt::Debug for DedicatedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid taking the mutex in debug formatting
        write!(f, "DedicatedExecutor")
    }
}

/// [`DedicatedExecutor`] for testing purposes.
static TESTING_EXECUTOR: Lazy<DedicatedExecutor> =
    Lazy::new(|| DedicatedExecutor::new_inner("testing", NonZeroUsize::new(1).unwrap(), true));

impl DedicatedExecutor {
    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// executor that is separate from the threadpool created via
    /// `[tokio::main]` or similar.
    ///
    /// The worker thread priority is set to low so that such tasks do
    /// not starve other more important tasks (such as answering health checks)
    ///
    /// Follows the example from to stack overflow and spawns a new
    /// thread to install a Tokio runtime "context"
    /// <https://stackoverflow.com/questions/62536566>
    ///
    /// If you try to do this from a async context you see something like
    /// thread 'plan::stringset::tests::test_builder_plan' panicked at 'Cannot
    /// drop a runtime in a context where blocking is not allowed. This
    /// happens when a runtime is dropped from within an asynchronous
    /// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
    pub fn new(thread_name: &str, num_threads: NonZeroUsize) -> Self {
        Self::new_inner(thread_name, num_threads, false)
    }

    fn new_inner(thread_name: &str, num_threads: NonZeroUsize, testing: bool) -> Self {
        let thread_name = thread_name.to_string();
        let thread_counter = Arc::new(AtomicUsize::new(1));

        let (tx_tasks, rx_tasks) = std::sync::mpsc::channel::<Task>();
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();

        let thread = std::thread::Builder::new()
            .name(format!("{thread_name} driver"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name_fn(move || {
                        format!(
                            "{} {}",
                            thread_name,
                            thread_counter.fetch_add(1, Ordering::SeqCst)
                        )
                    })
                    .worker_threads(num_threads.get())
                    .on_thread_start(move || set_current_thread_priority(WORKER_PRIORITY))
                    .build()
                    .expect("Creating tokio runtime");

                runtime.block_on(async move {
                    // Dropping the tokio runtime only waits for tasks to yield not to complete
                    //
                    // We therefore use a RwLock to wait for tasks to complete
                    let join = Arc::new(tokio::sync::RwLock::new(()));

                    while let Ok(task) = rx_tasks.recv() {
                        let join = Arc::clone(&join);
                        let handle = join.read_owned().await;

                        tokio::task::spawn(async move {
                            task.run().await;
                            std::mem::drop(handle);
                        });
                    }

                    // Wait for all tasks to finish
                    let _guard = join.write().await;

                    // signal shutdown, but it's OK if the other side is gone
                    tx_shutdown.send(()).ok();
                })
            })
            .expect("executor setup");

        let state = State {
            requests: Some(tx_tasks),
            task_refs: Arc::new(()),
            completed_shutdown: rx_shutdown.map_err(Arc::new).boxed().shared(),
            thread: Some(thread),
        };

        Self {
            state: Arc::new(Mutex::new(state)),
            num_threads,
            testing,
        }
    }

    /// Create new executor for testing purposes.
    ///
    /// Internal state may be shared with other tests.
    pub fn new_testing() -> Self {
        TESTING_EXECUTOR.clone()
    }

    /// Number of threads that back this executor.
    pub fn num_threads(&self) -> NonZeroUsize {
        self.num_threads
    }

    /// Runs the specified Future (and any tasks it spawns) on the
    /// `DedicatedExecutor`.
    ///
    /// Currently all tasks are added to the tokio executor
    /// immediately and compete for the threadpool's resources.
    pub fn spawn<T>(&self, task: T) -> Job<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let fut = Box::pin(async move {
            let task_output = AssertUnwindSafe(task).catch_unwind().await.map_err(|e| {
                if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown internal error".to_string()
                }
            });

            if tx.send(task_output).is_err() {
                warn!("Spawned task output ignored: receiver dropped")
            }
        });
        let cancel = CancellationToken::new();
        let mut state = self.state.lock();
        let task = Task {
            fut,
            cancel: cancel.clone(),
            task_ref: Arc::clone(&state.task_refs),
        };

        if let Some(requests) = &mut state.requests {
            // would fail if someone has started shutdown
            requests.send(task).ok();
        } else {
            warn!("tried to schedule task on an executor that was shutdown");
        }

        Job {
            rx,
            cancel,
            detached: false,
        }
    }

    /// Number of currently active tasks.
    pub fn tasks(&self) -> usize {
        let state = self.state.lock();

        // the strong count is always `1 + jobs` because of the Arc we hold within Self
        Arc::strong_count(&state.task_refs).saturating_sub(1)
    }

    /// signals shutdown of this executor and any Clones
    pub fn shutdown(&self) {
        if self.testing {
            return;
        }

        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.lock();
        state.requests = None;
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    ///
    /// # Panic / Drop
    /// [`DedicatedExecutor`] implements shutdown on [`Drop`]. You should just use this behavior and NOT call
    /// [`join`](Self::join) manually during [`Drop`] or panics because this might lead to another panic, see
    /// <https://github.com/rust-lang/futures-rs/issues/2575>.
    pub async fn join(&self) {
        if self.testing {
            return;
        }

        self.shutdown();

        // get handle mutex is held
        let handle = {
            let state = self.state.lock();
            state.completed_shutdown.clone()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        handle.await.expect("Thread died?")
    }
}

#[cfg(unix)]
fn set_current_thread_priority(prio: i32) {
    // on linux setpriority sets the current thread's priority
    // (as opposed to the current process).
    unsafe { libc::setpriority(0, 0, prio) };
}

#[cfg(not(unix))]
fn set_current_thread_priority(prio: i32) {
    warn!("Setting worker thread priority not supported on this platform");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        panic::panic_any,
        sync::{Arc, Barrier},
        time::Duration,
    };
    use tokio::sync::Barrier as AsyncBarrier;

    #[cfg(unix)]
    fn get_current_thread_priority() -> i32 {
        // on linux setpriority sets the current thread's priority
        // (as opposed to the current process).
        unsafe { libc::getpriority(0, 0) }
    }

    #[cfg(not(unix))]
    fn get_current_thread_priority() -> i32 {
        WORKER_PRIORITY
    }

    #[tokio::test]
    async fn basic() {
        let barrier = Arc::new(Barrier::new(2));

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        let dedicated_task = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // Note the dedicated task will never complete if it runs on
        // the main tokio thread (as this test is not using the
        // 'multithreaded' version of the executor and the call to
        // barrier.wait actually blocks the tokio thread)
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn basic_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        // Run task on clone should work fine
        let dedicated_task = exec.clone().spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn drop_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());

        drop(exec.clone());

        let task = exec.spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn just_panic() {
        struct S(DedicatedExecutor);

        impl Drop for S {
            fn drop(&mut self) {
                self.0.join().now_or_never();
            }
        }

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        let _s = S(exec);

        // this must not lead to a double-panic and SIGILL
        panic!("foo")
    }

    #[tokio::test]
    async fn multi_task() {
        let barrier = Arc::new(Barrier::new(3));

        // make an executor with two threads
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(2).unwrap());
        let dedicated_task1 = exec.spawn(do_work(11, Arc::clone(&barrier)));
        let dedicated_task2 = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // block main thread until completion of other two tasks
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task1.await.unwrap(), 11);
        assert_eq!(dedicated_task2.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn worker_priority() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(2).unwrap());

        let dedicated_task = exec.spawn(async move { get_current_thread_priority() });

        assert_eq!(dedicated_task.await.unwrap(), WORKER_PRIORITY);

        exec.join().await;
    }

    #[tokio::test]
    async fn tokio_spawn() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(2).unwrap());

        // spawn a task that spawns to other tasks and ensure they run on the dedicated
        // executor
        let dedicated_task = exec.spawn(async move {
            // spawn separate tasks
            let t1 = tokio::task::spawn(async {
                let thread = std::thread::current();
                let tname = thread.name().expect("thread is named");

                assert!(
                    tname.starts_with("Test DedicatedExecutor"),
                    "Invalid thread name: {tname}",
                );

                25usize
            });
            t1.await.unwrap()
        });

        // Validate the inner task ran to completion (aka it did not panic)
        assert_eq!(dedicated_task.await.unwrap(), 25);

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_str() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("At the disco, on the dedicated task scheduler");
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "At the disco, on the dedicated task scheduler",
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_string() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("{} {}", 1, 2);
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(err.to_string(), "1 2",);

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_other() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        let dedicated_task = exec.spawn(async move {
            if true {
                panic_any(1)
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(err.to_string(), "unknown internal error",);

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_shutdown_while_task_running() {
        let barrier = Arc::new(Barrier::new(2));
        let captured = Arc::clone(&barrier);

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        let dedicated_task = exec.spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            do_work(42, captured).await
        });

        exec.shutdown();
        // block main thread until completion of the outstanding task
        barrier.wait();

        // task should complete successfully
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_submit_task_after_shutdown() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());

        // Simulate trying to submit tasks once executor has shutdown
        exec.shutdown();
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Worker thread gone, executor was likely shut down"
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_submit_task_after_clone_shutdown() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());

        // shutdown the clone (but not the exec)
        exec.clone().join().await;

        // Simulate trying to submit tasks once executor has shutdown
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Worker thread gone, executor was likely shut down"
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_join() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        // test it doesn't hang
        exec.join().await;
    }

    #[tokio::test]
    async fn executor_join2() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        // test it doesn't hang
        exec.join().await;
        exec.join().await;
    }

    #[tokio::test]
    #[allow(clippy::redundant_clone)]
    async fn executor_clone_join() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        // test it doesn't hang
        exec.clone().join().await;
        exec.clone().join().await;
        exec.join().await;
    }

    #[tokio::test]
    async fn drop_receiver() {
        // create empty executor
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        assert_eq!(exec.tasks(), 0);

        // create first blocked task
        let barrier1 = Arc::new(AsyncBarrier::new(2));
        let dedicated_task1 = exec.spawn(do_work_async(11, Arc::clone(&barrier1)));
        assert_eq!(exec.tasks(), 1);

        // create second blocked task
        let barrier2 = Arc::new(AsyncBarrier::new(2));
        let dedicated_task2 = exec.spawn(do_work_async(22, Arc::clone(&barrier2)));
        assert_eq!(exec.tasks(), 2);

        // cancel task
        drop(dedicated_task1);

        // cancelation might take a short while
        wait_for_tasks(&exec, 1).await;

        // unblock other task
        barrier2.wait().await;
        assert_eq!(dedicated_task2.await.unwrap(), 22);
        wait_for_tasks(&exec, 0).await;
        assert_eq!(exec.tasks(), 0);

        exec.join().await;
    }

    #[tokio::test]
    async fn detach_receiver() {
        // create empty executor
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", NonZeroUsize::new(1).unwrap());
        assert_eq!(exec.tasks(), 0);

        // create first task
        // `detach()` consumes the task but doesn't abort the task (in contrast to `drop`). We'll proof the that the
        // task is still running by linking it to a 2nd task using a barrier with size 3 (two tasks plus the main thread).
        let barrier = Arc::new(AsyncBarrier::new(3));
        let dedicated_task = exec.spawn(do_work_async(11, Arc::clone(&barrier)));
        dedicated_task.detach();
        assert_eq!(exec.tasks(), 1);

        // create second task
        let dedicated_task = exec.spawn(do_work_async(22, Arc::clone(&barrier)));
        assert_eq!(exec.tasks(), 2);

        // wait a bit just to make sure that our tasks doesn't get dropped
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(exec.tasks(), 2);

        // tasks should be unblocked because they both wait on the same barrier
        // unblock tasks
        barrier.wait().await;
        wait_for_tasks(&exec, 0).await;
        let result = dedicated_task.await.unwrap();
        assert_eq!(result, 22);

        exec.join().await;
    }

    /// Wait for the barrier and then return `result`
    async fn do_work(result: usize, barrier: Arc<Barrier>) -> usize {
        barrier.wait();
        result
    }

    /// Wait for the barrier and then return `result`
    async fn do_work_async(result: usize, barrier: Arc<AsyncBarrier>) -> usize {
        barrier.wait().await;
        result
    }

    // waits for up to 1 sec for the correct number of tasks
    async fn wait_for_tasks(exec: &DedicatedExecutor, num: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if exec.tasks() == num {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("Did not find expected num tasks within a second")
    }
}
