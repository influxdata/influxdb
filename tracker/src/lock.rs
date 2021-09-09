use std::sync::Arc;

use metric::{Attributes, DurationCounter, Metric, U64Counter};

type RawRwLock = InstrumentedRawRwLock<parking_lot::RawRwLock>;

/// An instrumented Read-Write Lock
pub type RwLock<T> = lock_api::RwLock<RawRwLock, T>;
pub type RwLockReadGuard<'a, T> = lock_api::RwLockReadGuard<'a, RawRwLock, T>;
pub type RwLockWriteGuard<'a, T> = lock_api::RwLockWriteGuard<'a, RawRwLock, T>;
pub type MappedRwLockReadGuard<'a, T> = lock_api::MappedRwLockReadGuard<'a, RawRwLock, T>;
pub type MappedRwLockWriteGuard<'a, T> = lock_api::MappedRwLockWriteGuard<'a, RawRwLock, T>;
pub type RwLockUpgradableReadGuard<'a, T> = lock_api::RwLockUpgradableReadGuard<'a, RawRwLock, T>;

#[derive(Debug)]
pub struct LockMetrics {
    exclusive_count: U64Counter,
    shared_count: U64Counter,
    upgradeable_count: U64Counter,
    upgrade_count: U64Counter,
    exclusive_wait: DurationCounter,
    shared_wait: DurationCounter,
    upgradeable_wait: DurationCounter,
    upgrade_wait: DurationCounter,
}

impl LockMetrics {
    pub fn new(registry: &metric::Registry, attributes: impl Into<Attributes>) -> Self {
        let mut attributes = attributes.into();

        let count: Metric<U64Counter> = registry.register_metric(
            "catalog_lock",
            "number of times the tracked locks have been obtained",
        );

        let wait: Metric<DurationCounter> = registry.register_metric(
            "catalog_lock_wait",
            "time spent waiting to acquire any of the tracked locks",
        );

        attributes.insert("access", "exclusive");
        let exclusive_count = count.recorder(attributes.clone());
        let exclusive_wait = wait.recorder(attributes.clone());

        attributes.insert("access", "shared");
        let shared_count = count.recorder(attributes.clone());
        let shared_wait = wait.recorder(attributes.clone());

        attributes.insert("access", "upgradeable");
        let upgradeable_count = count.recorder(attributes.clone());
        let upgradeable_wait = wait.recorder(attributes.clone());

        attributes.insert("access", "upgrade");
        let upgrade_count = count.recorder(attributes.clone());
        let upgrade_wait = wait.recorder(attributes);

        Self {
            exclusive_count,
            shared_count,
            upgradeable_count,
            upgrade_count,
            exclusive_wait,
            shared_wait,
            upgradeable_wait,
            upgrade_wait,
        }
    }

    pub fn new_unregistered() -> Self {
        Self {
            exclusive_count: Default::default(),
            shared_count: Default::default(),
            upgradeable_count: Default::default(),
            upgrade_count: Default::default(),
            exclusive_wait: Default::default(),
            shared_wait: Default::default(),
            upgradeable_wait: Default::default(),
            upgrade_wait: Default::default(),
        }
    }

    pub fn new_lock<T: Sized>(self: &Arc<Self>, t: T) -> RwLock<T> {
        self.new_lock_raw(t)
    }

    pub fn new_lock_raw<R: lock_api::RawRwLock, T: Sized>(
        self: &Arc<Self>,
        t: T,
    ) -> lock_api::RwLock<InstrumentedRawRwLock<R>, T> {
        lock_api::RwLock::const_new(
            InstrumentedRawRwLock {
                inner: R::INIT,
                metrics: Some(Arc::clone(self)),
            },
            t,
        )
    }
}

/// The RAII-goop for locks is provided by lock_api with individual crates
/// such as parking_lot providing raw lock implementations
///
/// This is a raw lock implementation that wraps another and instruments it
#[derive(Debug)]
pub struct InstrumentedRawRwLock<R: Sized> {
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
    metrics: Option<Arc<LockMetrics>>,
}

/// # Safety
///
/// Implementations of this trait must ensure that the `RwLock` is actually
/// exclusive: an exclusive lock can't be acquired while an exclusive or shared
/// lock exists, and a shared lock can't be acquire while an exclusive lock
/// exists.
///
/// This is done by delegating to the wrapped RawRwLock implementation
unsafe impl<R: lock_api::RawRwLock + Sized> lock_api::RawRwLock for InstrumentedRawRwLock<R> {
    const INIT: Self = Self {
        inner: R::INIT,
        metrics: None,
    };
    type GuardMarker = R::GuardMarker;

    /// Acquires a shared lock, blocking the current thread until it is able to do so.
    fn lock_shared(&self) {
        match &self.metrics {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_lock_shared() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.lock_shared();
                let elapsed = now.elapsed();
                shared.shared_count.inc(1);
                shared.shared_wait.inc(elapsed);
            }
            None => self.inner.lock_shared(),
        }
    }

    /// Attempts to acquire a shared lock without blocking.
    fn try_lock_shared(&self) -> bool {
        let ret = self.inner.try_lock_shared();
        if let Some(shared) = &self.metrics {
            if ret {
                shared.shared_count.inc(1);
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
        match &self.metrics {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_lock_exclusive() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.lock_exclusive();
                let elapsed = now.elapsed();
                shared.exclusive_count.inc(1);
                shared.exclusive_wait.inc(elapsed);
            }
            None => self.inner.lock_exclusive(),
        }
    }

    /// Attempts to acquire an exclusive lock without blocking.
    fn try_lock_exclusive(&self) -> bool {
        let ret = self.inner.try_lock_exclusive();
        if let Some(shared) = &self.metrics {
            if ret {
                shared.exclusive_count.inc(1);
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

/// # Safety
///
/// Implementations of this trait must ensure that the `RwLock` is actually
/// exclusive: an exclusive lock can't be acquired while an exclusive or shared
/// lock exists, and a shared lock can't be acquire while an exclusive lock
/// exists.
///
/// This is done by delegating to the wrapped RawRwLock implementation
unsafe impl<R: lock_api::RawRwLockUpgrade + Sized> lock_api::RawRwLockUpgrade
    for InstrumentedRawRwLock<R>
{
    fn lock_upgradable(&self) {
        match &self.metrics {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_lock_upgradable() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.lock_upgradable();
                let elapsed = now.elapsed();
                shared.upgradeable_count.inc(1);
                shared.upgradeable_wait.inc(elapsed);
            }
            None => self.inner.lock_upgradable(),
        }
    }

    fn try_lock_upgradable(&self) -> bool {
        let ret = self.inner.try_lock_upgradable();
        if let Some(shared) = &self.metrics {
            if ret {
                shared.upgradeable_count.inc(1);
            }
        }
        ret
    }

    unsafe fn unlock_upgradable(&self) {
        self.inner.unlock_upgradable()
    }

    unsafe fn upgrade(&self) {
        match &self.metrics {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_upgrade() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.upgrade();
                let elapsed = now.elapsed();
                shared.upgrade_count.inc(1);
                shared.upgrade_wait.inc(elapsed);
            }
            None => self.inner.upgrade(),
        }
    }

    unsafe fn try_upgrade(&self) -> bool {
        let ret = self.inner.try_upgrade();
        if let Some(shared) = &self.metrics {
            if ret {
                shared.upgrade_count.inc(1);
            }
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_counts() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let lock = metrics.new_lock(32);
        let _ = lock.read();
        let _ = lock.write();
        let _ = lock.read();

        assert_eq!(metrics.exclusive_count.fetch(), 1);
        assert_eq!(metrics.shared_count.fetch(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shared_wait_time() {
        let metrics = Arc::new(LockMetrics::new_unregistered());

        let l1 = Arc::new(metrics.new_lock(32));
        let l2 = Arc::clone(&l1);

        let write = l1.write();
        let join = tokio::spawn(async move {
            let _ = l2.read();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(write);

        join.await.unwrap();

        assert_eq!(metrics.exclusive_count.fetch(), 1);
        assert_eq!(metrics.shared_count.fetch(), 1);
        assert!(metrics.exclusive_wait.fetch() < Duration::from_micros(100));
        assert!(metrics.shared_wait.fetch() > Duration::from_millis(80));
        assert!(metrics.shared_wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exclusive_wait_time() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l2 = Arc::clone(&l1);

        let read = l1.read();
        let join = tokio::spawn(async move {
            let _ = l2.write();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(read);

        join.await.unwrap();

        assert_eq!(metrics.exclusive_count.fetch(), 1);
        assert_eq!(metrics.shared_count.fetch(), 1);
        assert!(metrics.shared_wait.fetch() < Duration::from_micros(100));
        assert!(metrics.exclusive_wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive_wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l1_captured = Arc::clone(&l1);
        let l2 = Arc::new(metrics.new_lock(12));
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

        assert_eq!(metrics.exclusive_count.fetch(), 2);
        assert_eq!(metrics.shared_count.fetch(), 2);
        assert!(metrics.shared_wait.fetch() < Duration::from_micros(100));
        assert!(metrics.exclusive_wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive_wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upgradeable() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l1_captured = Arc::clone(&l1);

        let r1 = l1.upgradable_read();
        let join = tokio::spawn(async move {
            let _ = l1_captured.write();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(r1);

        join.await.unwrap();

        assert_eq!(metrics.exclusive_count.fetch(), 1);
        assert_eq!(metrics.shared_count.fetch(), 0);
        assert_eq!(metrics.upgradeable_count.fetch(), 1);
        assert_eq!(metrics.upgrade_count.fetch(), 0);

        assert_eq!(metrics.upgrade_wait.fetch(), Duration::from_nanos(0));
        assert_eq!(metrics.shared_wait.fetch(), Duration::from_nanos(0));
        assert!(metrics.upgradeable_wait.fetch() < Duration::from_micros(100));
        assert!(metrics.exclusive_wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive_wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upgrade() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l1_captured = Arc::clone(&l1);

        let r1 = l1.read();
        let join = tokio::spawn(async move {
            let ur1 = l1_captured.upgradable_read();
            let _ = RwLockUpgradableReadGuard::upgrade(ur1);
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(r1);

        join.await.unwrap();

        assert_eq!(metrics.exclusive_count.fetch(), 0);
        assert_eq!(metrics.shared_count.fetch(), 1);
        assert_eq!(metrics.upgradeable_count.fetch(), 1);
        assert_eq!(metrics.upgrade_count.fetch(), 1);

        assert_eq!(metrics.exclusive_wait.fetch(), Duration::from_nanos(0));
        assert!(metrics.shared_wait.fetch() < Duration::from_micros(100));
        assert!(metrics.upgradeable_wait.fetch() < Duration::from_micros(100));
        assert!(metrics.upgrade_wait.fetch() > Duration::from_millis(80));
        assert!(metrics.upgrade_wait.fetch() < Duration::from_millis(200));
    }
}
