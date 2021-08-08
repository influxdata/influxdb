use std::ops::Deref;
use std::sync::{Arc, Weak};

/// A wrapper around a type `T` that can be frozen with `Freezable::try_freeze`, preventing
/// modification of the contained `T` until the returned `FreezeHandle` is dropped
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
///     let mut locked = lockable.write().unwrap();
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
#[derive(Debug)]
pub struct Freezable<T>(Arc<T>);

impl<T> Freezable<T> {
    pub fn new(payload: T) -> Self {
        Self(Arc::new(payload))
    }

    /// Returns a `FreezeHandle` that prevents modification
    /// of the contents of `Freezable` until it is dropped
    ///
    /// Returns None if the object is already frozen
    pub fn try_freeze(&mut self) -> Option<FreezeHandle<T>> {
        // Verify exclusive
        self.get_mut()?;
        Some(FreezeHandle(Arc::downgrade(&self.0)))
    }

    /// Unfreezes this instance, returning a mutable reference to
    /// its contained data
    pub fn unfreeze(&mut self, handle: FreezeHandle<T>) -> &mut T {
        assert!(
            std::ptr::eq(&*self.0, handle.0.as_ptr()),
            "provided FreezeHandle is not for this instance"
        );
        std::mem::drop(handle);
        // Just dropped `FreezeHandle` so should be valid
        self.get_mut().unwrap()
    }

    /// Try to get mutable access to the data
    ///
    /// Returns `None` if this instance is frozen
    pub fn get_mut(&mut self) -> Option<&mut T> {
        Arc::get_mut(&mut self.0)
    }
}

impl<T> Deref for Freezable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Freezable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
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
pub struct FreezeHandle<T>(Weak<T>);
