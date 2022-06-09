//! LRU (Least Recently Used) cache system.
//!
//! # Usage
//!
//! ```
//! use std::{
//!     collections::HashMap,
//!     ops::{Add, Sub},
//!     sync::Arc,
//! };
//! use iox_time::SystemProvider;
//! use cache_system::backend::{
//!     CacheBackend,
//!     lru::{LruBackend, ResourcePool},
//!     resource_consumption::{Resource, ResourceEstimator},
//! };
//!
//! // first we implement a strongly-typed RAM size measurement
//! #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
//! struct RamSize(usize);
//!
//! impl Resource for RamSize {
//!     fn zero() -> Self {
//!         Self(0)
//!     }
//!
//!     fn unit() -> &'static str {
//!         "bytes"
//!     }
//! }
//!
//! impl From<RamSize> for u64 {
//!     fn from(s: RamSize) -> Self {
//!         s.0 as Self
//!     }
//! }
//!
//! impl Add for RamSize {
//!     type Output = Self;
//!
//!     fn add(self, rhs: Self) -> Self::Output {
//!         Self(self.0.checked_add(rhs.0).expect("overflow"))
//!     }
//! }
//!
//! impl Sub for RamSize {
//!     type Output = Self;
//!
//!     fn sub(self, rhs: Self) -> Self::Output {
//!         Self(self.0.checked_sub(rhs.0).expect("underflow"))
//!     }
//! }
//!
//! // a time provider is required to determine the age of entries
//! let time_provider = Arc::new(SystemProvider::new());
//!
//! // registry to capture metrics emitted by the LRU cache
//! let metric_registry = Arc::new(metric::Registry::new());
//!
//! // set up a memory pool
//! let limit = RamSize(50);
//! let pool = Arc::new(ResourcePool::new(
//!     "my_pool",
//!     limit,
//!     time_provider,
//!     metric_registry,
//! ));
//!
//! // set up first pool user: a u64->String map
//! #[derive(Debug)]
//! struct Estimator1 {}
//!
//! impl ResourceEstimator for Estimator1 {
//!     type K = u64;
//!     type V = String;
//!     type S = RamSize;
//!
//!     fn consumption(&self, _k: &Self::K, v: &Self::V) -> Self::S {
//!         RamSize(8) + RamSize(v.capacity())
//!     }
//! }
//! let mut backend1 = LruBackend::new(
//!     Box::new(HashMap::new()),
//!     Arc::clone(&pool),
//!     "id1",
//!     Arc::new(Estimator1{}),
//! );
//!
//! // add some data
//! backend1.set(1, String::from("some_entry"));
//! backend1.set(2, String::from("another_entry"));
//! assert_eq!(pool.current(), RamSize(39));
//!
//! // only test first one
//! assert!(backend1.get(&1).is_some());
//!
//! // fill up pool
//! backend1.set(3, String::from("this_will_evict_data"));
//! assert!(backend1.get(&1).is_some());
//! assert!(backend1.get(&2).is_none());
//! assert!(backend1.get(&3).is_some());
//! assert_eq!(pool.current(), RamSize(46));
//!
//! // set up second pool user with totally different types: a u8->Vec<u8> map
//! #[derive(Debug)]
//! struct Estimator2 {}
//!
//! impl ResourceEstimator for Estimator2 {
//!     type K = u8;
//!     type V = Vec<u8>;
//!     type S = RamSize;
//!
//!     fn consumption(&self, _k: &Self::K, v: &Self::V) -> Self::S {
//!         RamSize(1) + RamSize(v.capacity())
//!     }
//! }
//! let mut backend2 = LruBackend::new(
//!     Box::new(HashMap::new()),
//!     Arc::clone(&pool),
//!     "id2",
//!     Arc::new(Estimator2{}),
//! );
//!
//! // eviction works for all pool members
//! backend2.set(1, vec![1, 2, 3, 4]);
//! assert!(backend1.get(&1).is_none());
//! assert!(backend1.get(&2).is_none());
//! assert!(backend1.get(&3).is_some());
//! assert!(backend2.get(&1).is_some());
//! assert_eq!(pool.current(), RamSize(33));
//! ```
//!
//! # Internals
//! Here we describe the internals of the LRU cache system.
//!
//! ## Requirements
//! To understand the construction, we first must understand what the LRU system tries to achieve:
//!
//! - **Single Pool:** Have a single resource pool for multiple LRU backends.
//! - **Eviction Cascade:** Adding data to any of the backends (or modifying an existing entry) should check if there is
//!   enough space left in the LRU backend. If not, we must remove the least recently used entries over all backends
//!   (including the one that just got a new entry) until there is enough space.
//!
//! This has the following consequences:
//!
//! - **Cyclic Structure:** The LRU backends communicate with the pool, but the pool also neeeds to communicate with
//!   all the backends. This creates some form of cyclic data structure.
//! - **Type Erasure:** The pool is only specific to the resource type, not the key and value types of the
//!   participating backends. So at some place we need to perform type erasure.
//!
//! ## Data Structures
//!
//! ```text
//!               .~~~~~~~~~~~~.            .~~~~~~~~~~~~~~~~~.
//! ------------->: ResourcePool :--(mutex)-->: ResourcePoolInner :-----------------------------------+
//!               :    <S>     :            :       <S>       :                                   |
//!               .~~~~~~~~~~~~.            .~~~~~~~~~~~~~~~~~.                                   |
//!                   ^                                                                           |
//!                   |                                                                           |
//!                 (arc)                                                                         |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |  : LruBackendInner :<--: PoolMemberGuardImpl :<-(dyn)-: PoolMemberGuard : |
//!                   |  : <K1, V1, S>     :   :     <K1, V1, S>     :        :       <S>       : |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |        ^                           ^                           ^          |
//!                   |        |                           |                           |          |
//!                   |        |                           +-------------+-------------+          |
//!                   |        |                                    (call lock)                   |
//!                   |        |                           +-------------+-------------+          |
//!                   |     (mutex)                        |                           |          |
//!   .~~~~~~~~~~~~~. |        |                   .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.    |
//! ->: LruBackend  :-+      (arc)                 : PoolMemberImpl :           : PoolMember :<---+
//!   : <K1, V1, S> : |        |                   :   <K1, V1, S>  :           :    <S>     :    |
//!   :             :----------+-------------------:                :<--(dyn)---:            :    |
//!   .~~~~~~~~~~~~~. |                            .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.    |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |  : LruBackendInner :<--: PoolMemberGuardImpl :<-(dyn)-: PoolMemberGuard : |
//!                   |  : <K2, V2, S>     :   :     <K2, V2, S>     :        :       <S>       : |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |        ^                           ^                           ^          |
//!                   |        |                           |                           |          |
//!                   |        |                           +-------------+-------------+          |
//!                   |        |                                    (call lock)                   |
//!                   |        |                           +-------------+-------------+          |
//!                   |     (mutex)                        |                           |          |
//!   .~~~~~~~~~~~~~. |        |                   .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.    |
//! ->: LruBackend  :-+      (arc)                 : PoolMemberImpl :           : PoolMember :<---+
//!   : <K2, V2, S> :          |                   :   <K2, V2, S>  :           :    <S>     :
//!   :             :----------+-------------------:                :<--(dyn)---:            :
//!   .~~~~~~~~~~~~~.                              .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.
//! ```
//!
//! ## State
//! State is held in the following structures:
//!
//! - `LruBackendInner`: Holds the actual user-provided backend ([`CacheBackend`]) as well as an [`AddressableHeap`] to
//!   memorize when entries were used for the last time.
//! - `ResourcePoolInner`: Holds a reference to all pool members as well as the current consumption.
//!
//! All other structures and traits "only" act as glue.
//!
//! ## Locking
//! What and how we lock depends on the operation.
//!
//! Note that all locks are bare mutexes, there are no read-write-locks. "Only read" is not really an important use
//! case since even `get` requires updating the "last used" timestamp of the corresponding entry.
//!
//! ### Get
//! For [`get`](CacheBackend::get) we only need to update the "last used" timestamp for the affected entry. No
//! pool-wide operations are required. We just [`LruBackendInner`] and perform the read operation of the inner backend
//! and the modification of the "last used" timestamp.
//!
//! ### Remove
//! For [`remove`](CacheBackend::remove) the pool usage can only decrease, so other backends are never affected. We
//! first lock [`ResourcePoolInner`], then [`LruBackendInner`] and then perform the modification on both.
//!
//! ### Set
//! [`set`](CacheBackend::set) is the most complex operation and requires a bit of a lock dance:
//!
//! 1. Lock [`ResourcePoolInner`]
//! 2. Lock [`LruBackendInner`]
//! 3. Check if the entry already exists and remove it.
//! 4. Drop lock of [`LruBackendInner`] so that the pool can use it to free up space.
//! 5. Request to add more data to the pool:
//!    1. Check if we need to free up space, otherwise we can already procede to step 6.
//!    2. Lock all pool members ([`PoolMember::lock`] which ultimately locks [`LruBackendInner`])
//!    3. Loop:
//!       1. Ask pool members if they have anything to free.
//!       2. Pick least recently used result and free it
//!    4. Drop locks of [`LruBackendInner`]
//! 6. Lock [`LruBackendInner`]
//! 7. Drop lock of [`LruBackendInner`] and [`ResourcePoolInner`]
//!
//! The global locks in step 5.2 are required so that the reads in step 5.3.1 and the resulting actions in step 5.3.2
//! are consistent. Otherwise an interleaved `get` request might invalidate the results.
use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::Arc,
};

use iox_time::{Time, TimeProvider};
use metric::{U64Counter, U64Gauge};
use parking_lot::{Mutex, MutexGuard};

use super::{
    addressable_heap::AddressableHeap,
    resource_consumption::{Resource, ResourceEstimator},
    CacheBackend,
};

#[derive(Debug)]
/// Wrapper around something that can be converted into `u64`
/// to enable emitting metrics.
struct MeasuredT<T> {
    v: T,
    metric: U64Gauge,
}

impl<T> MeasuredT<T> {
    fn new(v: T, metric: U64Gauge) -> Self
    where
        T: Copy + Into<u64>,
    {
        metric.set(v.into());

        Self { v, metric }
    }

    fn inc(&mut self, delta: &T)
    where
        T: std::ops::Add<Output = T> + Copy + Into<u64>,
    {
        self.v = self.v + *delta;
        self.metric.inc((*delta).into());
    }

    fn dec(&mut self, delta: &T)
    where
        T: std::ops::Sub<Output = T> + Copy + Into<u64>,
    {
        self.v = self.v - *delta;
        self.metric.dec((*delta).into());
    }
}

impl<T> PartialEq for MeasuredT<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.v == other.v
    }
}

impl<T> PartialOrd for MeasuredT<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.v.partial_cmp(&other.v)
    }
}

/// Inner state of [`ResourcePool`] which is always behind a mutex.
#[derive(Debug)]
struct ResourcePoolInner<S>
where
    S: Resource,
{
    /// Resource limit.
    limit: MeasuredT<S>,

    /// Current resource usage.
    current: MeasuredT<S>,

    /// Members (= backends) that use this pool.
    members: BTreeMap<&'static str, Box<dyn PoolMember<S = S>>>,
}

impl<S> ResourcePoolInner<S>
where
    S: Resource,
{
    /// Create new, empty pool.
    fn new(limit: S, pool_name: &'static str, metric_registry: &metric::Registry) -> Self {
        let current = S::zero();

        let metric_limit = metric_registry
            .register_metric::<U64Gauge>("cache_lru_pool_limit", "Limit of the LRU resource pool")
            .recorder(&[("unit", S::unit()), ("pool", pool_name)]);
        let limit = MeasuredT::new(limit, metric_limit);

        let metric_current = metric_registry
            .register_metric::<U64Gauge>(
                "cache_lru_pool_usage",
                "Current consumption of the LRU resource pool",
            )
            .recorder(&[("unit", S::unit()), ("pool", pool_name)]);
        let current = MeasuredT::new(current, metric_current);

        Self {
            limit,
            current,
            members: BTreeMap::new(),
        }
    }

    /// Register new pool member.
    ///
    /// # Panic
    /// Panics when a member with the specific ID is already registered.
    fn register_member(&mut self, id: &'static str, member: Box<dyn PoolMember<S = S>>) {
        match self.members.entry(id) {
            Entry::Vacant(v) => {
                v.insert(member);
            }
            Entry::Occupied(o) => {
                panic!("Member '{}' already registered", o.key());
            }
        }
    }

    /// Unregister pool member.
    ///
    /// # Panic
    /// Panics when the member with the specified ID is unknown (or was already unregistered).
    fn unregister_member(&mut self, id: &str) {
        assert!(self.members.remove(id).is_some(), "Member '{}' unknown", id);
    }

    /// Add used resource too pool.
    fn add(&mut self, s: S) {
        self.current.inc(&s);

        if self.current > self.limit {
            // lock all members
            let mut members: Vec<_> = self.members.values().map(|member| member.lock()).collect();

            // evict data until we are below the limit
            while self.current > self.limit {
                let mut options: Vec<_> = members
                    .iter_mut()
                    .filter_map(|member| member.could_remove().map(|t| (t, member)))
                    .collect();
                options.sort_by_key(|(t, _member)| *t);

                let (_t, member) = options.first_mut().expect("accounting out of sync");
                let s = member.remove_oldest();
                self.current.dec(&s);
            }
        }
    }

    /// Remove used resource from pool.
    fn remove(&mut self, s: S) {
        self.current.dec(&s);
    }
}

/// Resource pool.
///
/// This can be used with [`LruBackend`].
#[derive(Debug)]
pub struct ResourcePool<S>
where
    S: Resource,
{
    inner: Mutex<ResourcePoolInner<S>>,
    name: &'static str,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: Arc<metric::Registry>,
}

impl<S> ResourcePool<S>
where
    S: Resource,
{
    /// Creates new empty resource pool with given limit.
    pub fn new(
        name: &'static str,
        limit: S,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
    ) -> Self {
        Self {
            inner: Mutex::new(ResourcePoolInner::new(limit, name, &metric_registry)),
            name,
            metric_registry,
            time_provider,
        }
    }

    /// Get current pool usage.
    pub fn current(&self) -> S {
        self.inner.lock().current.v
    }
}

/// Inner state of [`LruBackend`].
///
/// This is used by [`LruBackend`] directly but also by [`PoolMemberImpl`] to add it to a [`ResourcePool`]/[`ResourcePoolInner`].
#[derive(Debug)]
struct LruBackendInner<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    inner_backend: Box<dyn CacheBackend<K = K, V = V>>,
    last_used: AddressableHeap<K, S, Time>,
    metric_count: U64Gauge,
    metric_usage: U64Gauge,
    metric_evicted: U64Counter,
}

/// [Cache backend](CacheBackend) that wraps another backend and limits its resource usage.
#[derive(Debug)]
pub struct LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    id: &'static str,
    inner: Arc<Mutex<LruBackendInner<K, V, S>>>,
    pool: Arc<ResourcePool<S>>,
    resource_estimator: Arc<dyn ResourceEstimator<K = K, V = V, S = S>>,
}

impl<K, V, S> LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    /// Create new backend w/o any known keys.
    ///
    /// The inner backend MUST NOT contain any data at this point, otherwise we will not track any resource consumption
    /// for these entries.
    ///
    /// # Panic
    /// - Panics if the given ID is already used within the given pool.
    /// - If the inner backend is not empty.
    pub fn new(
        inner_backend: Box<dyn CacheBackend<K = K, V = V>>,
        pool: Arc<ResourcePool<S>>,
        id: &'static str,
        resource_estimator: Arc<dyn ResourceEstimator<K = K, V = V, S = S>>,
    ) -> Self {
        assert!(inner_backend.is_empty(), "inner backend is not empty");

        let metric_count = pool
            .metric_registry
            .register_metric::<U64Gauge>(
                "cache_lru_member_count",
                "Number of entries for a given LRU cache pool member",
            )
            .recorder(&[("pool", pool.name), ("member", id)]);
        let metric_usage = pool
            .metric_registry
            .register_metric::<U64Gauge>(
                "cache_lru_member_usage",
                "Resource usage of a given LRU cache pool member",
            )
            .recorder(&[("pool", pool.name), ("member", id), ("unit", S::unit())]);
        let metric_evicted = pool
            .metric_registry
            .register_metric::<U64Counter>(
                "cache_lru_member_evicted",
                "Number of entries that were evicted from a given LRU cache pool member",
            )
            .recorder(&[("pool", pool.name), ("member", id)]);
        let inner = Arc::new(Mutex::new(LruBackendInner {
            inner_backend,
            last_used: AddressableHeap::new(),
            metric_count,
            metric_usage,
            metric_evicted,
        }));

        pool.inner.lock().register_member(
            id,
            Box::new(PoolMemberImpl {
                inner: Arc::clone(&inner),
            }),
        );

        Self {
            id,
            inner,
            pool,
            resource_estimator,
        }
    }

    /// Get underlying / inner backend.
    pub fn inner_backend(&self) -> LruBackendInnerBackendHandle<'_, K, V, S> {
        LruBackendInnerBackendHandle {
            inner: self.inner.lock(),
        }
    }
}

impl<K, V, S> Drop for LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    fn drop(&mut self) {
        self.pool.inner.lock().unregister_member(self.id);
    }
}

impl<K, V, S> CacheBackend for LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        let mut inner = self.inner.lock();

        match inner.inner_backend.get(k) {
            Some(v) => {
                // update "last used"
                let now = self.pool.time_provider.now();
                let (consumption, _last_used) = inner
                    .last_used
                    .remove(k)
                    .expect("backend and last-used table out of sync");
                inner.last_used.insert(k.clone(), consumption, now);

                Some(v)
            }
            None => None,
        }
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        // determine all attributes before getting any locks
        let consumption = self.resource_estimator.consumption(&k, &v);
        let now = self.pool.time_provider.now();

        // get locks
        let mut pool = self.pool.inner.lock();

        // check for oversized entries
        if consumption > pool.limit.v {
            return;
        }

        // maybe clean from pool
        {
            let mut inner = self.inner.lock();
            if let Some((consumption, _last_used)) = inner.last_used.remove(&k) {
                pool.remove(consumption);
                inner.metric_count.dec(1);
                inner.metric_usage.dec(consumption.into());
            }
        }

        // pool-wide operation
        // Since this may call back to this very backend to remove entries, we MUST NOT hold an inner lock at this point.
        pool.add(consumption);

        // add new entry to inner backend AFTER adding it to the pool, so we are never overcommitting resources.
        let mut inner = self.inner.lock();
        inner.inner_backend.set(k.clone(), v);
        inner.last_used.insert(k, consumption, now);
        inner.metric_count.inc(1);
        inner.metric_usage.inc(consumption.into());
    }

    fn remove(&mut self, k: &Self::K) {
        let mut pool = self.pool.inner.lock();
        let mut inner = self.inner.lock();

        inner.inner_backend.remove(k);
        if let Some((consumption, _last_used)) = inner.last_used.remove(k) {
            pool.remove(consumption);
            inner.metric_count.dec(1);
            inner.metric_usage.dec(consumption.into());
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.lock().last_used.is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

/// A member of a [`ResourcePool`]/[`ResourcePoolInner`].
///
/// Must be [locked](Self::lock) to gain access.
///
/// The only implementation of this is [`PoolMemberImpl`]. This indirection is required to erase `K` and `V` from specific
/// backend so we can stick it into the generic pool.
trait PoolMember: Debug + Send + 'static {
    /// Resource type.
    type S;

    /// Lock pool member.
    fn lock(&self) -> Box<dyn PoolMemberGuard<S = Self::S> + '_>;
}

/// The only implementation of [`PoolMember`].
///
/// In constast to the trait, this still contains `K` and `V`.
#[derive(Debug)]
pub struct PoolMemberImpl<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    inner: Arc<Mutex<LruBackendInner<K, V, S>>>,
}

impl<K, V, S> PoolMember for PoolMemberImpl<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    type S = S;

    fn lock(&self) -> Box<dyn PoolMemberGuard<S = Self::S> + '_> {
        Box::new(PoolMemberGuardImpl {
            inner: self.inner.lock(),
        })
    }
}

/// Locked [`ResourcePool`]/[`ResourcePoolInner`] member.
///
/// The only implementation of this is [`PoolMemberGuardImpl`]. This indirection is required to erase `K` and `V` from
/// specific backend so we can stick it into the generic pool.
trait PoolMemberGuard: Debug {
    /// Resource type.
    type S;

    /// Check if this member has anything that could be removed. If so, return the "last used" timestamp of the oldest
    /// entry.
    fn could_remove(&self) -> Option<Time>;

    /// Remove oldest entry and return consumption of the removed entry.
    ///
    /// # Panic
    /// This must only be used if [`could_remove`](Self::could_remove) was used to check if there is anything to check
    /// if there is an entry that could be removed. Panics if this is not the case.
    fn remove_oldest(&mut self) -> Self::S;
}

/// The only implementation of [`PoolMemberGuard`].
///
/// In constast to the trait, this still contains `K` and `V`.
#[derive(Debug)]
pub struct PoolMemberGuardImpl<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    inner: MutexGuard<'a, LruBackendInner<K, V, S>>,
}

impl<'a, K, V, S> PoolMemberGuard for PoolMemberGuardImpl<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    type S = S;

    fn could_remove(&self) -> Option<Time> {
        self.inner.last_used.peek().map(|(_k, _s, t)| *t)
    }

    fn remove_oldest(&mut self) -> Self::S {
        let (k, s, _t) = self.inner.last_used.pop().expect("nothing to remove");
        self.inner.inner_backend.remove(&k);
        self.inner.metric_count.dec(1);
        self.inner.metric_usage.dec(s.into());
        self.inner.metric_evicted.inc(1);
        s
    }
}

/// Helper for [`LruBackend::inner_backend`].
#[derive(Debug)]
pub struct LruBackendInnerBackendHandle<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    inner: MutexGuard<'a, LruBackendInner<K, V, S>>,
}

impl<'a, K, V, S> Deref for LruBackendInnerBackendHandle<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    type Target = dyn CacheBackend<K = K, V = V>;

    fn deref(&self) -> &Self::Target {
        self.inner.inner_backend.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        ops::{Add, Sub},
        time::Duration,
    };

    use iox_time::MockProvider;
    use metric::{Observation, RawReporter};

    use super::*;

    #[test]
    #[should_panic(expected = "inner backend is not empty")]
    fn test_panic_inner_not_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        LruBackend::new(
            Box::new(HashMap::from([(String::from("foo"), 1usize)])),
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );
    }

    #[test]
    #[should_panic(expected = "Member 'id' already registered")]
    fn test_panic_id_collission() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let _backend1 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );
        let _backend2 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );
    }

    #[test]
    fn test_reregister_member() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let backend1 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );

        // drop the backend so re-registering the same ID ("id") MUST NOT panic
        drop(backend1);
        let _backend2 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );
    }

    #[test]
    fn test_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        assert_eq!(pool.current().0, 0);

        let _backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );

        assert_eq!(pool.current().0, 0);
    }

    #[test]
    fn test_override() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );

        backend.set(String::from("a"), 5usize);
        assert_eq!(pool.current().0, 5);

        backend.set(String::from("b"), 3usize);
        assert_eq!(pool.current().0, 8);

        backend.set(String::from("a"), 4usize);
        assert_eq!(pool.current().0, 7);
    }

    #[test]
    fn test_remove() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );

        backend.set(String::from("a"), 5usize);
        assert_eq!(pool.current().0, 5);

        backend.set(String::from("b"), 3usize);
        assert_eq!(pool.current().0, 8);

        backend.remove(&String::from("a"));
        assert_eq!(pool.current().0, 3);

        assert_eq!(backend.get(&String::from("a")), None);
        assert_inner_backend(&backend, [(String::from("b"), 3)]);

        // removing it again should just work
        backend.remove(&String::from("a"));
        assert_eq!(pool.current().0, 3);
    }

    #[test]
    fn test_eviction_order() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(21),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend1 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );
        let mut backend2 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id2",
            Arc::clone(&resource_estimator) as _,
        );

        backend1.set(String::from("b"), 1usize);
        backend2.set(String::from("a"), 2usize);
        backend1.set(String::from("a"), 3usize);
        backend1.set(String::from("c"), 4usize);
        assert_eq!(pool.current().0, 10);

        time_provider.inc(Duration::from_millis(1));

        backend1.set(String::from("d"), 5usize);
        assert_eq!(pool.current().0, 15);

        time_provider.inc(Duration::from_millis(1));
        backend2.set(String::from("b"), 6usize);
        assert_eq!(pool.current().0, 21);

        time_provider.inc(Duration::from_millis(1));

        // now are exactly at capacity
        assert_inner_backend(
            &backend1,
            [
                (String::from("a"), 3),
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // adding a single element will drop the smallest key from the first backend (by ID)
        backend1.set(String::from("foo1"), 1usize);
        assert_eq!(pool.current().0, 19);
        assert_inner_backend(
            &backend1,
            [
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
                (String::from("foo1"), 1),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // now we can fill up data up to the capacity again
        backend1.set(String::from("foo2"), 2usize);
        assert_eq!(pool.current().0, 21);
        assert_inner_backend(
            &backend1,
            [
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // can evict two keys at the same time
        backend1.set(String::from("foo3"), 2usize);
        assert_eq!(pool.current().0, 18);
        assert_inner_backend(
            &backend1,
            [
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // can evict from another backend
        backend1.set(String::from("foo4"), 4usize);
        assert_eq!(pool.current().0, 20);
        assert_inner_backend(
            &backend1,
            [
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
                (String::from("foo4"), 4),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("b"), 6)]);

        // can evict multiple timestamps
        backend1.set(String::from("foo5"), 7usize);
        assert_eq!(pool.current().0, 16);
        assert_inner_backend(
            &backend1,
            [
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
                (String::from("foo4"), 4),
                (String::from("foo5"), 7),
            ],
        );
        assert_inner_backend(&backend2, []);
    }

    #[test]
    fn test_get_updates_last_used() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(6),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );

        backend.set(String::from("a"), 1usize);
        backend.set(String::from("b"), 2usize);

        time_provider.inc(Duration::from_millis(1));

        backend.set(String::from("c"), 3usize);

        time_provider.inc(Duration::from_millis(1));

        assert_eq!(backend.get(&String::from("a")), Some(1usize));

        assert_eq!(pool.current().0, 6);
        assert_inner_backend(
            &backend,
            [
                (String::from("a"), 1),
                (String::from("b"), 2),
                (String::from("c"), 3),
            ],
        );

        backend.set(String::from("foo"), 3usize);
        assert_eq!(pool.current().0, 4);
        assert_inner_backend(&backend, [(String::from("a"), 1), (String::from("foo"), 3)]);
    }

    #[test]
    fn test_oversized_entries_are_never_added() {
        #[derive(Debug)]
        struct PanicAllBackend {}

        impl CacheBackend for PanicAllBackend {
            type K = String;
            type V = usize;

            fn get(&mut self, _k: &Self::K) -> Option<Self::V> {
                panic!("should never be called")
            }

            fn set(&mut self, _k: Self::K, _v: Self::V) {
                panic!("should never be called")
            }

            fn remove(&mut self, _k: &Self::K) {
                panic!("should never be called")
            }

            fn is_empty(&self) -> bool {
                true
            }

            fn as_any(&self) -> &dyn Any {
                self as &dyn Any
            }
        }

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(1),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = LruBackend::new(
            Box::new(PanicAllBackend {}),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );

        backend.set(String::from("a"), 2usize);
        assert_eq!(pool.current().0, 0);
    }

    #[test]
    fn test_values_are_dropped() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(3),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));

        #[derive(Debug)]
        struct Provider {}

        impl ResourceEstimator for Provider {
            type K = Arc<String>;
            type V = Arc<usize>;
            type S = TestSize;

            fn consumption(&self, _k: &Self::K, v: &Self::V) -> Self::S {
                TestSize(*v.as_ref())
            }
        }

        let resource_estimator = Arc::new(Provider {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );

        let k1 = Arc::new(String::from("a"));
        let v1 = Arc::new(2usize);
        let k2 = Arc::new(String::from("b"));
        let v2 = Arc::new(2usize);
        let k1_weak = Arc::downgrade(&k1);
        let v1_weak = Arc::downgrade(&v1);

        backend.set(k1, v1);

        time_provider.inc(Duration::from_millis(1));

        backend.set(k2, v2);

        assert_eq!(k1_weak.strong_count(), 0);
        assert_eq!(v1_weak.strong_count(), 0);
    }

    #[test]
    fn test_backends_are_dropped() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(3),
            Arc::clone(&time_provider) as _,
            Arc::new(metric::Registry::new()),
        ));

        let resource_estimator = Arc::new(TestResourceEstimator {});

        #[derive(Debug)]
        struct Backend {
            #[allow(dead_code)]
            marker: Arc<()>,
            inner: HashMap<String, usize>,
        }

        impl CacheBackend for Backend {
            type K = String;
            type V = usize;

            fn get(&mut self, k: &Self::K) -> Option<Self::V> {
                self.inner.get(k).copied()
            }

            fn set(&mut self, k: Self::K, v: Self::V) {
                self.inner.set(k, v)
            }

            fn remove(&mut self, k: &Self::K) {
                self.inner.remove(k);
            }

            fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            fn as_any(&self) -> &dyn Any {
                self as &dyn Any
            }
        }

        let marker = Arc::new(());
        let marker_weak = Arc::downgrade(&marker);

        let mut backend = LruBackend::new(
            Box::new(Backend {
                marker,
                inner: HashMap::new(),
            }),
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        );
        backend.set(String::from("a"), 2usize);

        drop(backend);
        assert_eq!(marker_weak.strong_count(), 0);
    }

    #[test]
    fn test_metrics() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(metric::Registry::new());
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&time_provider) as _,
            Arc::clone(&metric_registry),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_lru_pool_limit")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes")])
                .unwrap(),
            &Observation::U64Gauge(10)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_pool_usage")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes")])
                .unwrap(),
            &Observation::U64Gauge(0)
        );

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_lru_pool_limit")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes")])
                .unwrap(),
            &Observation::U64Gauge(10)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_pool_usage")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes")])
                .unwrap(),
            &Observation::U64Gauge(0)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_member_count")
                .unwrap()
                .observation(&[("pool", "pool"), ("member", "id")])
                .unwrap(),
            &Observation::U64Gauge(0)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_member_usage")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes"), ("member", "id")])
                .unwrap(),
            &Observation::U64Gauge(0)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_member_evicted")
                .unwrap()
                .observation(&[("pool", "pool"), ("member", "id")])
                .unwrap(),
            &Observation::U64Counter(0)
        );

        backend.set(String::from("a"), 1usize); // usage = 1
        backend.set(String::from("b"), 2usize); // usage = 3
        backend.set(String::from("b"), 3usize); // usage = 4
        backend.set(String::from("c"), 4usize); // usage = 8
        backend.set(String::from("d"), 3usize); // usage = 10 (evicted "a")
        backend.remove(&String::from("c")); // usage = 6

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_lru_pool_limit")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes")])
                .unwrap(),
            &Observation::U64Gauge(10)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_pool_usage")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes")])
                .unwrap(),
            &Observation::U64Gauge(6)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_member_count")
                .unwrap()
                .observation(&[("pool", "pool"), ("member", "id")])
                .unwrap(),
            &Observation::U64Gauge(2), // b and d
        );
        assert_eq!(
            reporter
                .metric("cache_lru_member_usage")
                .unwrap()
                .observation(&[("pool", "pool"), ("unit", "bytes"), ("member", "id")])
                .unwrap(),
            &Observation::U64Gauge(6)
        );
        assert_eq!(
            reporter
                .metric("cache_lru_member_evicted")
                .unwrap()
                .observation(&[("pool", "pool"), ("member", "id")])
                .unwrap(),
            &Observation::U64Counter(1)
        );
    }

    #[test]
    fn test_generic() {
        use crate::backend::test_util::test_generic;

        #[derive(Debug)]
        struct ZeroSizeProvider {}

        impl ResourceEstimator for ZeroSizeProvider {
            type K = u8;
            type V = String;
            type S = TestSize;

            fn consumption(&self, _k: &Self::K, _v: &Self::V) -> Self::S {
                TestSize(0)
            }
        }

        test_generic(|| {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let pool = Arc::new(ResourcePool::new(
                "pool",
                TestSize(10),
                Arc::clone(&time_provider) as _,
                Arc::new(metric::Registry::new()),
            ));
            let resource_estimator = Arc::new(ZeroSizeProvider {});

            LruBackend::new(
                Box::new(HashMap::new()),
                Arc::clone(&pool),
                "id",
                Arc::clone(&resource_estimator) as _,
            )
        });
    }

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    struct TestSize(usize);

    impl Resource for TestSize {
        fn zero() -> Self {
            Self(0)
        }

        fn unit() -> &'static str {
            "bytes"
        }
    }

    impl From<TestSize> for u64 {
        fn from(s: TestSize) -> Self {
            s.0 as Self
        }
    }

    impl Add for TestSize {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            Self(self.0.checked_add(rhs.0).expect("overflow"))
        }
    }

    impl Sub for TestSize {
        type Output = Self;

        fn sub(self, rhs: Self) -> Self::Output {
            Self(self.0.checked_sub(rhs.0).expect("underflow"))
        }
    }

    #[derive(Debug)]
    struct TestResourceEstimator {}

    impl ResourceEstimator for TestResourceEstimator {
        type K = String;
        type V = usize;
        type S = TestSize;

        fn consumption(&self, _k: &Self::K, v: &Self::V) -> Self::S {
            TestSize(*v)
        }
    }

    fn assert_inner_backend<const N: usize>(
        backend: &LruBackend<String, usize, TestSize>,
        data: [(String, usize); N],
    ) {
        let inner_backend = backend.inner_backend();
        let inner_backend = inner_backend
            .as_any()
            .downcast_ref::<HashMap<String, usize>>()
            .unwrap();
        let expected = HashMap::from(data);
        assert_eq!(inner_backend, &expected);
    }
}
