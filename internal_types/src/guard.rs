use std::ops::Deref;
use std::sync::{Arc, Weak};

/// A wrapper around a type T that allows acquiring `ReadGuard` that prevents
/// modification to the data within the `ReadLock` until the `ReadGuard` is dropped
///
/// ```
/// use internal_types::guard::ReadLock;
/// let mut data = ReadLock::new(0);
/// assert!(data.get_mut().is_some());
///
/// *data.get_mut().unwrap() = 32;
/// assert_eq!(*data, 32);
///
/// let guard = data.lock();
/// assert!(data.get_mut().is_none());
///
/// let guard2 = data.lock();
/// std::mem::drop(guard);
///
/// assert!(data.get_mut().is_none());
/// std::mem::drop(guard2);
///
/// // Can get mutable access as guards dropped
/// assert!(data.get_mut().is_some());
/// ```
///
/// The construction relies on the borrow checker to prevent creating
/// new `ReadGuard` whilst there is an active mutable borrow
///
/// ```compile_fail
/// use internal_types::guard::ReadLock;
/// let mut data = ReadLock::new(0);
/// let mut_borrow = data.get_mut();
///
/// data.lock(); // Shouldn't compile
///
/// std::mem::drop(mut_borrow);
/// ```
///
#[derive(Debug)]
pub struct ReadLock<T>(Arc<T>);

impl<T> ReadLock<T> {
    pub fn new(payload: T) -> Self {
        Self(Arc::new(payload))
    }

    /// Returns a new `ReadGuard` that prevents modification
    /// of the contents of `ReadLock` until dropped
    pub fn lock(&self) -> ReadGuard<T> {
        ReadGuard(Arc::downgrade(&self.0))
    }

    /// Try to get access to the mutable data
    ///
    /// Returns `None` if there are outstanding `ReadGuard`
    pub fn get_mut(&mut self) -> Option<&mut T> {
        Arc::get_mut(&mut self.0)
    }
}

impl<T> Deref for ReadLock<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct ReadGuard<T>(Weak<T>);
