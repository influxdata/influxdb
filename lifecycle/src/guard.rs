//! This module contains lock guards for use in the lifecycle traits
//!
//! Specifically they exist to work around a lack of support for generic associated
//! types within traits. https://github.com/rust-lang/rust/issues/44265
//!
//! ```ignore
//! trait MyTrait {
//!     type Guard;
//!
//!     fn read(&self) -> Self::Guard<'_> <-- this is not valid rust
//! }
//! ```
//!
//! The structs in this module therefore provide concrete types that can be
//! used in their place
//!
//! ```
//! use lifecycle::LifecycleReadGuard;
//! trait MyTrait {
//!     type AdditionalData;
//!     type LockedType;
//!
//!     fn read(&self) -> LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>;
//! }
//! ```
//!
//! One drawback of this approach is that the type returned from the read() method can't
//! be a user-provided type that implements a trait
//!
//! ```ignore
//! trait MyTrait {
//!     type Guard: GuardTrait; <-- this makes for a nice API
//!
//!     fn read(&self) -> Self::Guard<'_> <-- this is not valid rust
//! }
//! ```
//!
//! Instead we have to use associated functions, which are a bit more cumbersome
//!
//! ```
//! use lifecycle::LifecycleReadGuard;
//! use tracker::RwLock;
//! use std::sync::Arc;
//!
//! trait Lockable {
//!     type AdditionalData;
//!     type LockedType;
//!
//!     fn read(&self) -> LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>;
//!
//!     fn guard_func(s: LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>) -> u32;
//! }
//!
//! struct Locked {
//!     num: u32,
//! }
//!
//! #[derive(Clone)]
//! struct MyLockable {
//!     offset: u32,
//!     data: Arc<RwLock<Locked>>
//! }
//!
//! impl Lockable for MyLockable {
//!     type AdditionalData = Self;
//!     type LockedType = Locked;
//!
//!     fn read(&self) -> LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData> {
//!         LifecycleReadGuard::new(self.clone(), &self.data)
//!     }
//!
//!     fn guard_func(s: LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>) -> u32 {
//!         s.num + s.data().offset
//!     }
//! }
//!
//! let lockable = MyLockable{ offset: 32, data: Arc::new(RwLock::new(Locked{ num: 1 }))};
//! assert_eq!(MyLockable::guard_func(lockable.read()), 33);
//! lockable.data.write().num = 21;
//! assert_eq!(MyLockable::guard_func(lockable.read()), 53);
//!
//! ```
//!

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use tracker::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

/// The `LifecycleReadGuard` combines a `RwLockUpgradableReadGuard` with an arbitrary
/// data payload of type `D`.
///
/// The data of `P` can be immutably accessed through `std::ops::Deref` akin
/// to a normal read guard or smart pointer
///
/// Note: The `LifecycleReadGuard` will not block other readers to `RwLock<P>` but
/// they will block other upgradeable readers, e.g. other `LifecycleReadGuard`
pub struct LifecycleReadGuard<'a, P, D> {
    data: D,
    guard: RwLockUpgradableReadGuard<'a, P>,
}

impl<'a, P, D> LifecycleReadGuard<'a, P, D> {
    /// Create a new `LifecycleReadGuard` from the provided lock
    pub fn new(data: D, lock: &'a RwLock<P>) -> Self {
        let guard = lock.upgradable_read();
        Self { data, guard }
    }

    /// Upgrades this to a `LifecycleWriteGuard`
    pub fn upgrade(self) -> LifecycleWriteGuard<'a, P, D> {
        let guard = RwLockUpgradableReadGuard::upgrade(self.guard);
        LifecycleWriteGuard {
            data: self.data,
            guard,
        }
    }

    /// Returns a reference to the contained data payload
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Drops the locks held by this guard and returns the data payload
    pub fn unwrap(self) -> D {
        self.data
    }
}

impl<'a, P, D> Debug for LifecycleReadGuard<'a, P, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LifecycleReadGuard{{..}}")
    }
}

impl<'a, P, D> Deref for LifecycleReadGuard<'a, P, D> {
    type Target = P;
    #[inline]
    fn deref(&self) -> &P {
        &self.guard
    }
}

/// A `LifecycleWriteGuard` combines a `RwLockWriteGuard` with an arbitrary
/// data payload of type `D`.
///
/// The data of `P` can be immutably accessed through `std::ops::Deref` akin to
/// a normal read guard or smart pointer, and also mutably through
/// `std::ops::DerefMut` akin to a normal write guard
///
pub struct LifecycleWriteGuard<'a, P, D> {
    data: D,
    guard: RwLockWriteGuard<'a, P>,
}

impl<'a, P, D> LifecycleWriteGuard<'a, P, D> {
    /// Create a new `LifecycleWriteGuard` from the provided lock
    pub fn new(data: D, lock: &'a RwLock<P>) -> Self {
        let guard = lock.write();
        Self { data, guard }
    }

    /// Returns a reference to the contained data payload
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Drops the locks held by this guard and returns the data payload
    pub fn unwrap(self) -> D {
        self.data
    }
}

impl<'a, P, D> Debug for LifecycleWriteGuard<'a, P, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LifecycleWriteGuard{{..}}")
    }
}

impl<'a, P, D> Deref for LifecycleWriteGuard<'a, P, D> {
    type Target = P;
    #[inline]
    fn deref(&self) -> &P {
        &self.guard
    }
}

impl<'a, P, D> DerefMut for LifecycleWriteGuard<'a, P, D> {
    #[inline]
    fn deref_mut(&mut self) -> &mut P {
        &mut self.guard
    }
}
