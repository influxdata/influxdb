use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// A wrapper around a type `T` that can be frozen with `Freezable::try_freeze`, preventing
/// modification of the contained `T` until the returned `FreezeHandle` is dropped.
///
/// A common usecase for Freezable is preventing other threads from
/// modifying some object for an extended period of time prior to
/// actually modifying it, while allowing other threads to read the
/// object during that time period.
///
/// ```
/// use internal_types::freezable::Freezable;
/// let mut data = Freezable::new(0);
/// assert!(data.get_mut().is_some());
///
/// *data.get_mut().unwrap() = 32;
/// assert_eq!(*data, 32);
///
/// let handle = data.try_freeze().unwrap();
/// assert!(data.get_mut().is_none());
///
/// // Cannot get another guard as one already exists
/// assert!(data.try_freeze().is_none());
///
/// *data.unfreeze(handle) = 34;
/// assert_eq!(*data, 34);
///
/// // Can get mutable access as guards dropped
/// assert!(data.get_mut().is_some());
///
/// // Can get another guard
/// assert!(data.try_freeze().is_some());
/// ```
///
/// This is useful for a common 3-stage transaction where nothing should be able to
/// start another transaction on the locked resource whilst this transaction is
/// in-progress, but locks are not held for the entirety of the transaction:
///
/// 1. Exclusive access is obtained to a resource and work is identified
/// 2. Locks are dropped and some async work performed
/// 3. Locks are re-acquired and the result of async work committed
///
/// ```
/// use internal_types::freezable::Freezable;
/// use std::sync::RwLock;
///
/// let lockable = RwLock::new(Freezable::new(23));
///
/// // Start transaction
/// let handle = {
///     let mut locked = lockable.read().unwrap();
///     locked.try_freeze().expect("other transaction in progress")
/// };
///
/// // The contained data cannot be modified
/// assert!(lockable.write().unwrap().get_mut().is_none());
/// // But it can still be read
/// assert_eq!(**lockable.read().unwrap(), 23);
///
/// // --------------
/// // Do async work
/// // --------------
///
/// // Finish transaction
/// {
///     let mut locked = lockable.write().unwrap();
///     *locked.unfreeze(handle) = 45;
/// }
/// ```
///
///
/// A freeze handle can also be acquired asynchronously
///
/// ```
/// use internal_types::freezable::Freezable;
/// use std::sync::RwLock;
///
/// let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
///
/// rt.block_on(async move {
///     let lockable = RwLock::new(Freezable::new(23));
///     let fut_handle = lockable.read().unwrap().freeze();
///
///     // NB: Only frozen once future resolved to FreezeHandle
///     *lockable.write().unwrap().get_mut().unwrap() = 56;
///
///     let handle = fut_handle.await;
///
///     // The contained data now cannot be modified
///     assert!(lockable.write().unwrap().get_mut().is_none());
///     // But it can still be read
///     assert_eq!(**lockable.read().unwrap(), 56);
///
///     // --------------
///     // Do async work
///     // --------------
///
///     // Finish transaction
///     *lockable.write().unwrap().unfreeze(handle) = 57;
/// });
/// ```
///
#[derive(Debug)]
pub struct Freezable<T> {
    lock: Arc<tokio::sync::Mutex<()>>,
    payload: T,
}

impl<T> Freezable<T> {
    pub fn new(payload: T) -> Self {
        Self {
            lock: Default::default(),
            payload,
        }
    }

    /// Returns a `FreezeHandle` that prevents modification
    /// of the contents of `Freezable` until it is dropped
    ///
    /// Returns None if the object is already frozen
    pub fn try_freeze(&self) -> Option<FreezeHandle> {
        let guard = Arc::clone(&self.lock).try_lock_owned().ok()?;
        Some(FreezeHandle(guard))
    }

    /// Returns a future that resolves to a FreezeHandle
    pub fn freeze(&self) -> impl std::future::Future<Output = FreezeHandle> {
        let captured = Arc::clone(&self.lock);
        async move { FreezeHandle(captured.lock_owned().await) }
    }

    /// Unfreezes this instance, returning a mutable reference to
    /// its contained data
    pub fn unfreeze(&mut self, handle: FreezeHandle) -> WriteGuard<'_, T> {
        assert!(
            Arc::ptr_eq(&self.lock, tokio::sync::OwnedMutexGuard::mutex(&handle.0)),
            "provided FreezeHandle is not for this instance"
        );

        WriteGuard {
            freezable: self,
            guard: handle.0,
        }
    }

    /// Try to get mutable access to the data
    ///
    /// Returns `None` if this instance is frozen
    pub fn get_mut(&mut self) -> Option<WriteGuard<'_, T>> {
        let guard = Arc::clone(&self.lock).try_lock_owned().ok()?;
        Some(WriteGuard {
            freezable: self,
            guard,
        })
    }
}

impl<T> Deref for Freezable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Freezable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.payload.fmt(f)
    }
}

/// The `WriteGuard` provides mutable access to the `Freezable` data whilst ensuring
/// that it remains unfrozen for the duration of the mutable access
#[derive(Debug)]
pub struct WriteGuard<'a, T> {
    freezable: &'a mut Freezable<T>,

    /// A locked guard from `Freezable`
    ///
    /// Ensures nothing can freeze the Freezable whilst this WriteGuard exists
    guard: tokio::sync::OwnedMutexGuard<()>,
}

impl<'a, T> WriteGuard<'a, T> {
    /// Converts this `WriteGuard` into a `FreezeHandle`
    pub fn freeze(self) -> FreezeHandle {
        FreezeHandle(self.guard)
    }
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.freezable.payload
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // This is valid as holding mutex guard
        &mut self.freezable.payload
    }
}

/// `FreezeHandle` is returned by `Freezable::try_freeze` and prevents modification
/// of the contents of `Freezable`. Importantly:
///
/// * `FreezeHandle` is `Send` and can be held across an await point
/// * `FreezeHandle` will unfreeze on `Drop` making it panic safe
/// * `FreezeHandle` is not coupled to the lifetime of the `Freezable`
///
/// The last of these is critical to allowing the `FreezeHandle`
/// to outlive the `&mut Freezable` from which it was created
///
#[derive(Debug)]
pub struct FreezeHandle(tokio::sync::OwnedMutexGuard<()>);

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{future::FutureExt, pin_mut};
    use std::sync::RwLock;

    #[tokio::test]
    async fn test_freeze() {
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let mut f = Freezable::new(1);

        let freeze_fut = f.freeze();
        pin_mut!(freeze_fut);

        let write_guard = f.get_mut().unwrap();

        // Shouldn't resolve whilst write guard active
        assert!(freeze_fut.poll_unpin(&mut cx).is_pending());

        std::mem::drop(write_guard);

        // Should resolve once write guard removed
        let handle = freeze_fut.now_or_never().unwrap();

        // Should prevent freezing
        assert!(f.try_freeze().is_none());
        assert!(f.get_mut().is_none());

        // But not acquiring a new future
        let freeze_fut = f.freeze();
        pin_mut!(freeze_fut);

        // Future shouldn't complete whilst handle active
        assert!(freeze_fut.poll_unpin(&mut cx).is_pending());

        std::mem::drop(handle);

        // Future should complete once handle dropped
        freeze_fut.now_or_never().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fuzz() {
        let count = 1000;
        let shared = Arc::new((
            RwLock::new(Freezable::new(0_usize)),
            tokio::sync::Barrier::new(count),
        ));

        let futures = (0..count).into_iter().map(|i| {
            let captured = Arc::clone(&shared);
            tokio::spawn(async move {
                let (lockable, barrier) = captured.as_ref();

                // Wait for all tasks to start
                barrier.wait().await;

                // Get handle
                let fut = lockable.read().unwrap().freeze();
                let handle = fut.await;

                // Start transaction
                let handle = {
                    let mut locked = lockable.write().unwrap();
                    let mut guard = locked.unfreeze(handle);

                    assert_eq!(*guard, 0);
                    *guard = i;

                    guard.freeze()
                };

                // Do async work
                tokio::time::sleep(tokio::time::Duration::from_nanos(10)).await;

                // Commit transaction
                {
                    let mut locked = lockable.write().unwrap();
                    let mut guard = locked.unfreeze(handle);

                    assert_eq!(*guard, i);
                    *guard = 0;
                }
            })
        });

        futures::future::try_join_all(futures).await.unwrap();
    }
}
