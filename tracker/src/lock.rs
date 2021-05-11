use lock_api::RawRwLock;
use metrics::{KeyValue, MetricObserver, MetricObserverBuilder};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// An instrumented Read-Write Lock
pub type RwLock<T> = lock_api::RwLock<InstrumentRawRwLock<parking_lot::RawRwLock>, T>;

/// A Lock tracker can be used to create instrumented read-write locks
/// that will record contention metrics
#[derive(Default, Debug, Clone)]
pub struct LockTracker {
    inner: Arc<LockTrackerShared>,
}

impl LockTracker {
    pub fn new_lock<T>(&self, t: T) -> RwLock<T> {
        RwLock::const_new(
            InstrumentRawRwLock {
                inner: parking_lot::RawRwLock::INIT,
                shared: Some(Arc::clone(&self.inner)),
            },
            t,
        )
    }

    pub fn exclusive_count(&self) -> u64 {
        self.inner.exclusive_count.load(Ordering::Relaxed)
    }

    pub fn shared_count(&self) -> u64 {
        self.inner.shared_count.load(Ordering::Relaxed)
    }

    pub fn exclusive_wait_nanos(&self) -> u64 {
        self.inner.exclusive_wait_nanos.load(Ordering::Relaxed)
    }

    pub fn shared_wait_nanos(&self) -> u64 {
        self.inner.shared_wait_nanos.load(Ordering::Relaxed)
    }
}

impl MetricObserver for &LockTracker {
    fn register(self, builder: MetricObserverBuilder<'_>) {
        let inner = Arc::clone(&self.inner);
        builder.register_counter_u64(
            "lock",
            None,
            "number of times the tracked locks have been obtained",
            move |observer| {
                observer.observe(
                    inner.exclusive_count.load(Ordering::Relaxed),
                    &[KeyValue::new("access", "exclusive")],
                );
                observer.observe(
                    inner.shared_count.load(Ordering::Relaxed),
                    &[KeyValue::new("access", "shared")],
                )
            },
        );

        let inner = Arc::clone(&self.inner);
        builder.register_counter_f64(
            "lock_wait",
            Some("seconds"),
            "time spent waiting to acquire any of the tracked locks",
            move |observer| {
                observer.observe(
                    Duration::from_nanos(inner.exclusive_wait_nanos.load(Ordering::Relaxed))
                        .as_secs_f64(),
                    &[KeyValue::new("access", "exclusive")],
                );
                observer.observe(
                    Duration::from_nanos(inner.shared_wait_nanos.load(Ordering::Relaxed))
                        .as_secs_f64(),
                    &[KeyValue::new("access", "shared")],
                );
            },
        );
    }
}

#[derive(Debug, Default)]
struct LockTrackerShared {
    exclusive_count: AtomicU64,
    shared_count: AtomicU64,
    exclusive_wait_nanos: AtomicU64,
    shared_wait_nanos: AtomicU64,
}

/// The RAII-goop for locks is provided by lock_api with individual crates
/// such as parking_lot providing raw lock implementations
///
/// This is a raw lock implementation that wraps another and instruments it
#[derive(Debug)]
pub struct InstrumentRawRwLock<R: Sized> {
    inner: R,

    /// Stores the tracking data if any
    ///
    /// RawRwLocks must be able to be constructed in a const context, for example,
    /// as the associated constant RawRwLock::INIT.
    ///
    /// Arc, however, does not have a const constructor.
    ///
    /// This field is therefore optional. There is no way to access
    /// this field from a RwLock anyway, so ultimately it makes no difference
    /// that tracking is effectively disabled for default constructed locks
    shared: Option<Arc<LockTrackerShared>>,
}

/// # Safety
///
/// Implementations of this trait must ensure that the `RwLock` is actually
/// exclusive: an exclusive lock can't be acquired while an exclusive or shared
/// lock exists, and a shared lock can't be acquire while an exclusive lock
/// exists.
///
/// This is done by delegating to the wrapped RawRwLock implementation
unsafe impl<R: RawRwLock + Sized> lock_api::RawRwLock for InstrumentRawRwLock<R> {
    const INIT: Self = Self {
        inner: R::INIT,
        shared: None,
    };
    type GuardMarker = R::GuardMarker;

    /// Acquires a shared lock, blocking the current thread until it is able to do so.
    fn lock_shared(&self) {
        match &self.shared {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_lock_shared() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.lock_shared();
                let elapsed = now.elapsed().as_nanos() as u64;
                shared.shared_count.fetch_add(1, Ordering::Relaxed);
                shared
                    .shared_wait_nanos
                    .fetch_add(elapsed, Ordering::Relaxed);
            }
            None => self.inner.lock_shared(),
        }
    }

    /// Attempts to acquire a shared lock without blocking.
    fn try_lock_shared(&self) -> bool {
        let ret = self.inner.try_lock_shared();
        if let Some(shared) = &self.shared {
            if ret {
                shared.shared_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        ret
    }

    /// Releases a shared lock.
    ///
    /// # Safety
    ///
    /// This method may only be called if a shared lock is held in the current context.
    #[inline]
    unsafe fn unlock_shared(&self) {
        self.inner.unlock_shared()
    }

    /// Acquires an exclusive lock, blocking the current thread until it is able to do so.
    fn lock_exclusive(&self) {
        match &self.shared {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_lock_exclusive() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.lock_exclusive();
                let elapsed = now.elapsed().as_nanos() as u64;
                shared.exclusive_count.fetch_add(1, Ordering::Relaxed);
                shared
                    .exclusive_wait_nanos
                    .fetch_add(elapsed, Ordering::Relaxed);
            }
            None => self.inner.lock_exclusive(),
        }
    }

    /// Attempts to acquire an exclusive lock without blocking.
    fn try_lock_exclusive(&self) -> bool {
        let ret = self.inner.try_lock_exclusive();
        if let Some(shared) = &self.shared {
            if ret {
                shared.exclusive_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        ret
    }

    /// Releases an exclusive lock.
    ///
    /// # Safety
    ///
    /// This method may only be called if an exclusive lock is held in the current context.
    #[inline]
    unsafe fn unlock_exclusive(&self) {
        self.inner.unlock_exclusive()
    }

    /// Checks if this `RwLock` is currently locked in any way.
    #[inline]
    fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_counts() {
        let tracker = LockTracker::default();

        let lock = tracker.new_lock(32);
        let _ = lock.read();
        let _ = lock.write();
        let _ = lock.read();

        assert_eq!(tracker.exclusive_count(), 1);
        assert_eq!(tracker.shared_count(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shared_wait_time() {
        let tracker = LockTracker::default();
        let l1 = Arc::new(tracker.new_lock(32));
        let l2 = Arc::clone(&l1);

        let write = l1.write();
        let join = tokio::spawn(async move {
            let _ = l2.read();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(write);

        join.await.unwrap();

        assert_eq!(tracker.exclusive_count(), 1);
        assert_eq!(tracker.shared_count(), 1);
        assert!(tracker.exclusive_wait_nanos() < 100_000);
        assert!(tracker.shared_wait_nanos() > Duration::from_millis(80).as_nanos() as u64);
        assert!(tracker.shared_wait_nanos() < Duration::from_millis(200).as_nanos() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exclusive_wait_time() {
        let tracker = LockTracker::default();
        let l1 = Arc::new(tracker.new_lock(32));
        let l2 = Arc::clone(&l1);

        let read = l1.read();
        let join = tokio::spawn(async move {
            let _ = l2.write();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(read);

        join.await.unwrap();

        assert_eq!(tracker.exclusive_count(), 1);
        assert_eq!(tracker.shared_count(), 1);
        assert!(tracker.shared_wait_nanos() < 100_000);
        assert!(tracker.exclusive_wait_nanos() > Duration::from_millis(80).as_nanos() as u64);
        assert!(tracker.exclusive_wait_nanos() < Duration::from_millis(200).as_nanos() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple() {
        let tracker = LockTracker::default();
        let l1 = Arc::new(tracker.new_lock(32));
        let l1_captured = Arc::clone(&l1);
        let l2 = Arc::new(tracker.new_lock(12));
        let l2_captured = Arc::clone(&l2);

        let r1 = l1.read();
        let w2 = l2.write();
        let join = tokio::spawn(async move {
            let _ = l1_captured.write();
            let _ = l2_captured.read();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(w2);
        std::mem::drop(r1);

        join.await.unwrap();

        assert_eq!(tracker.exclusive_count(), 2);
        assert_eq!(tracker.shared_count(), 2);
        assert!(tracker.shared_wait_nanos() < 100_000);
        assert!(tracker.exclusive_wait_nanos() > Duration::from_millis(80).as_nanos() as u64);
        assert!(tracker.exclusive_wait_nanos() < Duration::from_millis(200).as_nanos() as u64);
    }
}
