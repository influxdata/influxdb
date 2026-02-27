use std::sync::Arc;

use metric::{Attributes, DurationCounter, Metric, U64Counter};

type RawRwLock = InstrumentedRawLock<parking_lot::RawRwLock>;
type RawMutex = InstrumentedRawLock<parking_lot::RawMutex>;

/// An instrumented Read-Write Lock
pub type RwLock<T> = lock_api::RwLock<RawRwLock, T>;
pub type RwLockReadGuard<'a, T> = lock_api::RwLockReadGuard<'a, RawRwLock, T>;
pub type RwLockWriteGuard<'a, T> = lock_api::RwLockWriteGuard<'a, RawRwLock, T>;
pub type MappedRwLockReadGuard<'a, T> = lock_api::MappedRwLockReadGuard<'a, RawRwLock, T>;
pub type MappedRwLockWriteGuard<'a, T> = lock_api::MappedRwLockWriteGuard<'a, RawRwLock, T>;
pub type RwLockUpgradableReadGuard<'a, T> = lock_api::RwLockUpgradableReadGuard<'a, RawRwLock, T>;

/// An instrumented mutex
pub type Mutex<T> = lock_api::Mutex<RawMutex, T>;
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, RawMutex, T>;
pub type MappedMutexGuard<'a, T> = lock_api::MappedMutexGuard<'a, RawMutex, T>;

#[derive(Debug, Default)]
struct LockMetricsSection {
    count: U64Counter,
    wait: DurationCounter,
}

impl LockMetricsSection {
    fn new(registry: &metric::Registry, attributes: &Attributes, access: &'static str) -> Self {
        let count: Metric<U64Counter> = registry.register_metric(
            "catalog_lock",
            "number of times the tracked locks have been obtained",
        );

        let wait: Metric<DurationCounter> = registry.register_metric(
            "catalog_lock_wait",
            "time spent waiting to acquire any of the tracked locks",
        );

        let mut attributes = attributes.clone();
        attributes.insert("access", access);

        Self {
            count: count.recorder(attributes.clone()),
            wait: wait.recorder(attributes.clone()),
        }
    }
}

#[derive(Debug)]
pub struct LockMetrics {
    exclusive: LockMetricsSection,
    shared: LockMetricsSection,
    upgradeable: LockMetricsSection,
    upgrade: LockMetricsSection,
}

impl LockMetrics {
    pub fn new(registry: &metric::Registry, attributes: impl Into<Attributes>) -> Self {
        let attributes = attributes.into();

        Self {
            exclusive: LockMetricsSection::new(registry, &attributes, "exclusive"),
            shared: LockMetricsSection::new(registry, &attributes, "shared"),
            upgradeable: LockMetricsSection::new(registry, &attributes, "upgradeable"),
            upgrade: LockMetricsSection::new(registry, &attributes, "upgrade"),
        }
    }

    pub fn new_unregistered() -> Self {
        Self {
            exclusive: Default::default(),
            shared: Default::default(),
            upgradeable: Default::default(),
            upgrade: Default::default(),
        }
    }

    pub fn new_lock<T: Sized>(self: &Arc<Self>, t: T) -> RwLock<T> {
        self.new_lock_raw(t)
    }

    pub fn new_lock_raw<R: lock_api::RawRwLock, T: Sized>(
        self: &Arc<Self>,
        t: T,
    ) -> lock_api::RwLock<InstrumentedRawLock<R>, T> {
        lock_api::RwLock::const_new(
            InstrumentedRawLock {
                inner: R::INIT,
                metrics: Some(Arc::clone(self)),
            },
            t,
        )
    }

    pub fn new_mutex<T: Sized>(self: &Arc<Self>, t: T) -> Mutex<T> {
        self.new_mutex_raw(t)
    }

    pub fn new_mutex_raw<R: lock_api::RawMutex, T: Sized>(
        self: &Arc<Self>,
        t: T,
    ) -> lock_api::Mutex<InstrumentedRawLock<R>, T> {
        lock_api::Mutex::const_new(
            InstrumentedRawLock {
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
pub struct InstrumentedRawLock<R: Sized> {
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
unsafe impl<R: lock_api::RawRwLock + Sized> lock_api::RawRwLock for InstrumentedRawLock<R> {
    // A “non-constant” const item is a legacy way to supply an initialized value to downstream
    // static items. Can hopefully be replaced with `const fn new() -> Self` at some point.
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
                shared.shared.count.inc(1);
                shared.shared.wait.inc(elapsed);
            }
            None => self.inner.lock_shared(),
        }
    }

    /// Attempts to acquire a shared lock without blocking.
    fn try_lock_shared(&self) -> bool {
        let ret = self.inner.try_lock_shared();
        if let Some(shared) = &self.metrics
            && ret
        {
            shared.shared.count.inc(1);
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
        unsafe { self.inner.unlock_shared() }
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
                shared.exclusive.count.inc(1);
                shared.exclusive.wait.inc(elapsed);
            }
            None => self.inner.lock_exclusive(),
        }
    }

    /// Attempts to acquire an exclusive lock without blocking.
    fn try_lock_exclusive(&self) -> bool {
        let ret = self.inner.try_lock_exclusive();
        if let Some(shared) = &self.metrics
            && ret
        {
            shared.exclusive.count.inc(1);
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
        unsafe { self.inner.unlock_exclusive() }
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
    for InstrumentedRawLock<R>
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
                shared.upgradeable.count.inc(1);
                shared.upgradeable.wait.inc(elapsed);
            }
            None => self.inner.lock_upgradable(),
        }
    }

    fn try_lock_upgradable(&self) -> bool {
        let ret = self.inner.try_lock_upgradable();
        if let Some(shared) = &self.metrics
            && ret
        {
            shared.upgradeable.count.inc(1);
        }
        ret
    }

    unsafe fn unlock_upgradable(&self) {
        unsafe { self.inner.unlock_upgradable() }
    }

    unsafe fn upgrade(&self) {
        match &self.metrics {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if unsafe { self.try_upgrade() } {
                    return;
                }

                let now = std::time::Instant::now();
                unsafe { self.inner.upgrade() };
                let elapsed = now.elapsed();
                shared.upgrade.count.inc(1);
                shared.upgrade.wait.inc(elapsed);
            }
            None => unsafe { self.inner.upgrade() },
        }
    }

    unsafe fn try_upgrade(&self) -> bool {
        let ret = unsafe { self.inner.try_upgrade() };
        if let Some(shared) = &self.metrics
            && ret
        {
            shared.upgrade.count.inc(1);
        }
        ret
    }
}

/// # Safety
///
/// Implementations of this trait must ensure that the `Mutex` is actually
/// exclusive: an exclusive lock can't be acquired while another exclusive
/// lock exists.
///
/// This is done by delegating to the wrapped RawMutex implementation
unsafe impl<R: lock_api::RawMutex + Sized> lock_api::RawMutex for InstrumentedRawLock<R> {
    // A “non-constant” const item is a legacy way to supply an initialized value to downstream
    // static items. Can hopefully be replaced with `const fn new() -> Self` at some point.
    const INIT: Self = Self {
        inner: R::INIT,
        metrics: None,
    };

    type GuardMarker = R::GuardMarker;

    fn lock(&self) {
        match &self.metrics {
            Some(shared) => {
                // Early return if possible - Instant::now is not necessarily cheap
                if self.try_lock() {
                    return;
                }

                let now = std::time::Instant::now();
                self.inner.lock();
                let elapsed = now.elapsed();
                shared.exclusive.count.inc(1);
                shared.exclusive.wait.inc(elapsed);
            }
            None => self.inner.lock(),
        }
    }

    fn try_lock(&self) -> bool {
        let ret = self.inner.try_lock();
        if let Some(shared) = &self.metrics
            && ret
        {
            shared.exclusive.count.inc(1);
        }
        ret
    }

    unsafe fn unlock(&self) {
        unsafe { self.inner.unlock() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_rwlock_counts() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let lock = metrics.new_lock(32);

        let r = lock.read();
        drop(r);

        let w = lock.write();
        drop(w);

        let r = lock.read();
        drop(r);

        assert_eq!(metrics.exclusive.count.fetch(), 1);
        assert_eq!(metrics.shared.count.fetch(), 2);
    }

    #[test]
    fn test_mutex_counts() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let mutex = metrics.new_mutex(32);

        let g = mutex.lock();
        drop(g);

        let g = mutex.lock();
        drop(g);

        assert_eq!(metrics.exclusive.count.fetch(), 2);
        assert_eq!(metrics.shared.count.fetch(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shared_wait_time() {
        let metrics = Arc::new(LockMetrics::new_unregistered());

        let l1 = Arc::new(metrics.new_lock(32));
        let l2 = Arc::clone(&l1);

        let write = l1.write();
        let join = tokio::spawn(async move {
            let _w2 = l2.read();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(write);

        join.await.unwrap();

        assert_eq!(metrics.exclusive.count.fetch(), 1);
        assert_eq!(metrics.shared.count.fetch(), 1);
        assert!(metrics.exclusive.wait.fetch() < Duration::from_micros(100));
        assert!(metrics.shared.wait.fetch() > Duration::from_millis(80));
        assert!(metrics.shared.wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exclusive_wait_time() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l2 = Arc::clone(&l1);

        let read = l1.read();
        let join = tokio::spawn(async move {
            let _w2 = l2.write();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(read);

        join.await.unwrap();

        assert_eq!(metrics.exclusive.count.fetch(), 1);
        assert_eq!(metrics.shared.count.fetch(), 1);
        assert!(metrics.shared.wait.fetch() < Duration::from_micros(100));
        assert!(metrics.exclusive.wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive.wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mutex_wait_time() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_mutex(32));
        let l2 = Arc::clone(&l1);

        let g = l1.lock();
        let join = tokio::spawn(async move {
            let _g = l2.lock();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(g);

        join.await.unwrap();

        assert_eq!(metrics.exclusive.count.fetch(), 2);
        assert_eq!(metrics.shared.count.fetch(), 0);
        assert_eq!(metrics.shared.wait.fetch(), Duration::ZERO);
        assert!(metrics.exclusive.wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive.wait.fetch() < Duration::from_millis(200));
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
            let _w1 = l1_captured.write();
            let _r2 = l2_captured.read();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(w2);
        std::mem::drop(r1);

        join.await.unwrap();

        assert_eq!(metrics.exclusive.count.fetch(), 2);
        assert_eq!(metrics.shared.count.fetch(), 2);
        assert!(metrics.shared.wait.fetch() < Duration::from_micros(100));
        assert!(metrics.exclusive.wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive.wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upgradeable() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l1_captured = Arc::clone(&l1);

        let r1 = l1.upgradable_read();
        let join = tokio::spawn(async move {
            let _w1 = l1_captured.write();
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(r1);

        join.await.unwrap();

        assert_eq!(metrics.exclusive.count.fetch(), 1);
        assert_eq!(metrics.shared.count.fetch(), 0);
        assert_eq!(metrics.upgradeable.count.fetch(), 1);
        assert_eq!(metrics.upgrade.count.fetch(), 0);

        assert_eq!(metrics.upgrade.wait.fetch(), Duration::from_nanos(0));
        assert_eq!(metrics.shared.wait.fetch(), Duration::from_nanos(0));
        assert!(metrics.upgradeable.wait.fetch() < Duration::from_micros(100));
        assert!(metrics.exclusive.wait.fetch() > Duration::from_millis(80));
        assert!(metrics.exclusive.wait.fetch() < Duration::from_millis(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upgrade() {
        let metrics = Arc::new(LockMetrics::new_unregistered());
        let l1 = Arc::new(metrics.new_lock(32));
        let l1_captured = Arc::clone(&l1);

        let r1 = l1.read();
        let join = tokio::spawn(async move {
            let ur1 = l1_captured.upgradable_read();
            let _w1 = RwLockUpgradableReadGuard::upgrade(ur1);
        });

        std::thread::sleep(Duration::from_millis(100));
        std::mem::drop(r1);

        join.await.unwrap();

        assert_eq!(metrics.exclusive.count.fetch(), 0);
        assert_eq!(metrics.shared.count.fetch(), 1);
        assert_eq!(metrics.upgradeable.count.fetch(), 1);
        assert_eq!(metrics.upgrade.count.fetch(), 1);

        assert_eq!(metrics.exclusive.wait.fetch(), Duration::from_nanos(0));
        assert!(metrics.shared.wait.fetch() < Duration::from_micros(100));
        assert!(metrics.upgradeable.wait.fetch() < Duration::from_micros(100));
        assert!(metrics.upgrade.wait.fetch() > Duration::from_millis(80));
        assert!(metrics.upgrade.wait.fetch() < Duration::from_millis(200));
    }
}
