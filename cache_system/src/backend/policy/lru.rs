//! LRU (Least Recently Used) cache system.
//!
//! # Usage
//!
//! ```
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use std::{
//!     collections::HashMap,
//!     ops::{Add, Sub},
//!     sync::Arc,
//! };
//! use iox_time::SystemProvider;
//! use cache_system::{
//!     backend::{
//!         CacheBackend,
//!         policy::{
//!             lru::{LruPolicy, ResourcePool},
//!             PolicyBackend,
//!         },
//!     },
//!     resource_consumption::{Resource, ResourceEstimator},
//! };
//! use tokio::runtime::Handle;
//!
//! // first we implement a strongly-typed RAM size measurement
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
//!     metric_registry,
//!     &Handle::current(),
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
//!
//! let mut backend1 = PolicyBackend::new(
//!     Box::new(HashMap::new()),
//!     Arc::clone(&time_provider) as _,
//! );
//! backend1.add_policy(
//!     LruPolicy::new(
//!         Arc::clone(&pool),
//!         "id1",
//!         Arc::new(Estimator1{}),
//!     )
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
//!
//! // the policy will eventually evict the data, in tests we can use a help
//! // method to wait for that
//! pool.wait_converged().await;
//!
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
//!
//! let mut backend2 = PolicyBackend::new(
//!     Box::new(HashMap::new()),
//!     time_provider,
//! );
//! backend2.add_policy(
//!     LruPolicy::new(
//!         Arc::clone(&pool),
//!         "id2",
//!         Arc::new(Estimator2{}),
//!     )
//! );
//!
//! // eviction works for all pool members
//! backend2.set(1, vec![1, 2, 3, 4]);
//! pool.wait_converged().await;
//! assert!(backend1.get(&1).is_none());
//! assert!(backend1.get(&2).is_none());
//! assert!(backend1.get(&3).is_some());
//! assert!(backend2.get(&1).is_some());
//! assert_eq!(pool.current(), RamSize(33));
//! # });
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
//!   enough space left in the LRU backend. If not, we must EVENTUALLY remove the least recently used entries over all
//!   backends (including the one that just got a new entry) until there is enough space.
//!
//! This has the following consequences:
//!
//! - **Cyclic Structure:** The LRU backends communicate with the pool, but the pool also needs to communicate with
//!   all the backends. This creates some form of cyclic data structure.
//! - **Type Erasure:** The pool is only specific to the resource type, not the key and value types of the
//!   participating backends. So at some place we need to perform type erasure.
//!
//! ## Data Structures
//!
//! ```text
//!                                                   .~~~~~~~~~~~~~~~~.
//!           +---------------------------------------: CallbackHandle :
//!           |                                       :  <K, V>        :
//!           |                                       .~~~~~~~~~~~~~~~~.
//!           |                                                 ^
//!           |                        .~~~~~~~~~~~~~~~~~.      |
//!           |                        : AddressableHeap :      |
//!           |                        : <K, S, Time>    :   (mutex)
//!           |                        .~~~~~~~~~~~~~~~~~.      |
//!           |                                ^                |
//!           |                                |                |
//!           V                             (mutex)             |
//!    .~~~~~~~~~~~~~~~.    .~~~~~~~~~~~.      |      .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.
//! -->: PolicyBackend :--->: LruPolicy :      |      : PoolMemberImpl :           : PoolMember :
//!    :  <K, V>       :    : <K, V, S> :      |      :   <K, V, S>    :           :    <S>     :
//!    :               :    :           :      +------:                :<--(dyn)---:            :
//!    .~~~~~~~~~~~~~~~.    .~~~~~~~~~~~.             .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.
//!                               |   |                                                ^   ^
//!                               |   |                                                |   |
//!                               |   +--------------------------------------(arc)-----+   |
//!                             (arc)                                                      |
//!                               |                                                      (weak)
//!                               V                                                        |
//!                        .~~~~~~~~~~~~~~.                                        .~~~~~~~~~~~~~.
//! ---------------------->: ResourcePool :-----+-------(arc)--------------------->: SharedState :
//!                        :     <S>      :     |                                  :   <S>       :
//!                        .~~~~~~~~~~~~~~.     |                                  .~~~~~~~~~~~~~.
//!                               |             |
//!                            (handle)         |
//!                               |             |
//!                               V             |
//!                        .~~~~~~~~~~~~~~~.    |
//!                        : clean_up_loop :----+
//!                        :     <S>       :
//!                        .~~~~~~~~~~~~~~~.
//! ```
//!
//! ## State
//! State is held in the following structures:
//!
//! - `LruPolicyInner`: Holds [`CallbackHandle`] as well as an [`AddressableHeap`] to
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
//! For [`GET`] we only need to update the "last used" timestamp for the affected entry. No
//! pool-wide operations are required. We update [`AddressableHeap`] and then perform the read operation of the inner
//! backend.
//!
//! ### Remove
//! For [`REMOVE`] the pool usage can only decrease, so other backends are never affected. We
//! first lock [`AddressableHeap`] and check if the entry is present. If it is, we also the "current" counter in
//! [`SharedState`] and then perform the modification on both.
//!
//! ### Set
//! [`SET`] locks [`AddressableHeap`] to figure out if th item exists. If it does, it locks the "current" counter in
//! [`SharedState`] and removes the old value. Then it updates [`AddressableHeap`] with the new value and locks&updates
//! the "current" counter in [`SharedState`] again. It then notifies the clean-up loop that there was an up.
//!
//! Note that in case of an override, the existing "last used" time will be used instead of "now", because just
//! replacing an existing value (e.g. via a [refresh]) should not count as a use.
//!
//! ### Clean-up Loop
//! This is the beefy bit. First it locks and reads the "current" counter in [`SharedState`]. It instantly unlocks the
//! value to not block all pool members adding new values while it we figure out what to evict. Then it selects victims
//! one by one by asking the individual pool members what they could remove. This shortly locks their
//! [`AddressableHeap`]s (one member at the time). After enough victims where selected for eviction, it will delete in
//! them one pool member at the time. Each pool member will lock their [`CallbackHandle`] and when the deletion happens
//! also their [`AddressableHeap`] and the "current" counter in [`SharedState`]. However the lock order is identical to
//! a normal "remove" operation.
//!
//! Note that the clean up loop does not directly update the "current" counter in [`SharedState`] since the "remove"
//! routine already does that.
//!
//! ## Consistency
//! This system is eventually consistent and we are a bit loose at a few places to make it more efficient and easier to
//! implement. This subsection explains cases where this could be visible to an observer.
//!
//! ### Overcommit
//! Since we add new data to the cache pool and the clean-up loop will eventually evict data, we overcommit the pool for
//! a short time. In practice however we already allocated the memory before adding it to the pool.
//!
//! There is a another risk that the cached users will add data so fast that the clean-up loop cannot keep up. This
//! however is highly unlikely, since the loop selects enough victims to get the resource usage below the limit and
//! deletes these victims in batches. The more it runs behind, the large the batch will be.
//!
//! ### Overdelete
//! Similar to "overcommit", it is possible that the clean-up loop deletes more items than necessary. This can happen
//! when between victim selection and actual deletion, entries are removed from the cache (e.g. via [TTL]). However the
//! timing for that is very tight and we would have deleted the data anyways if the delete would have happened a tiny
//! bit later, so in reality this is not a concern. On the other hand, the effect might also be a cache miss that was
//! not strictly necessary and in turn worse performance than we could have had.
//!
//! ### Victim-Use-Delete
//! It is possible that a key is used between victim selection and its removal. In theory we should not remove the key
//! in this case because its no longer "least recently used". However if the key usage would have occurred only a bit
//! later, we would have removed the key anyways so this tight race has no practical meaning. No user can rely on such
//! tight timings and the fullness of a cache pool.
//!
//! ### Victim-Downsize-Delete
//! A selected victim might be replaced with a smaller one between victim selection and its deletion. In this case, the
//! clean-up loop does not delete enough data in its current try but needs an additional iteration.  In reality this is
//! very unlikely since most cached entries rarely shrink and even if they do, the clean-up loop will eventually catch
//! up again.
//!
//!
//! [`GET`]: Subscriber::get
//! [`PolicyBackend`]: super::PolicyBackend
//! [refresh]: super::refresh
//! [`REMOVE`]: Subscriber::remove
//! [`SET`]: Subscriber::set
//! [TTL]: super::ttl
use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap, BinaryHeap},
    fmt::Debug,
    hash::Hash,
    sync::{Arc, Weak},
};

use iox_time::Time;
use metric::{U64Counter, U64Gauge};
use observability_deps::tracing::trace;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::Notify, task::JoinSet};

use crate::{
    addressable_heap::{AddressableHeap, AddressableHeapIter},
    backend::CacheBackend,
    resource_consumption::{Resource, ResourceEstimator},
};

use super::{CallbackHandle, ChangeRequest, Subscriber};

/// Wrapper around something that can be converted into `u64`
/// to enable emitting metrics.
#[derive(Debug)]
struct MeasuredT<S>
where
    S: Resource,
{
    v: S,
    metric: U64Gauge,
}

impl<S> MeasuredT<S>
where
    S: Resource,
{
    fn new(v: S, metric: U64Gauge) -> Self {
        metric.set(v.into());

        Self { v, metric }
    }

    fn inc(&mut self, delta: &S) {
        self.v = self.v + *delta;
        self.metric.inc((*delta).into());
    }

    fn dec(&mut self, delta: &S) {
        self.v = self.v - *delta;
        self.metric.dec((*delta).into());
    }
}

/// Shared state between [`ResourcePool`] and [`clean_up_loop`].
#[derive(Debug)]
struct SharedState<S>
where
    S: Resource,
{
    /// Resource limit.
    limit: MeasuredT<S>,

    /// Current resource usage.
    current: Mutex<MeasuredT<S>>,

    /// Members (= backends) that use this pool.
    members: Mutex<BTreeMap<&'static str, Weak<dyn PoolMember<S = S>>>>,

    /// Notification when [`current`](Self::current) as changed.
    change_notify: Notify,
}

impl<S> SharedState<S>
where
    S: Resource,
{
    /// Get current members.
    ///
    /// This also performs a clean-up.
    fn members(&self) -> BTreeMap<&'static str, Arc<dyn PoolMember<S = S>>> {
        let mut members = self.members.lock();
        let mut out = BTreeMap::new();

        members.retain(|id, member| match member.upgrade() {
            Some(member) => {
                out.insert(*id, member);
                true
            }
            None => false,
        });

        out
    }
}

/// Resource pool.
///
/// This can be used with [`LruPolicy`].
#[derive(Debug)]
pub struct ResourcePool<S>
where
    S: Resource,
{
    /// Name of the pool.
    name: &'static str,

    /// Shared state.
    shared: Arc<SharedState<S>>,

    /// Metric registry associated with the pool.
    ///
    /// This is used to generate member-specific metrics as well.
    metric_registry: Arc<metric::Registry>,

    /// Background task.
    _background_task: JoinSet<()>,

    /// Notification when the background worker is idle, so tests know that the state has converged and that they can
    /// continue working.
    #[allow(dead_code)]
    notify_idle_test_side: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
}

impl<S> ResourcePool<S>
where
    S: Resource,
{
    /// Creates new empty resource pool with given limit.
    pub fn new(
        name: &'static str,
        limit: S,
        metric_registry: Arc<metric::Registry>,
        runtime_handle: &Handle,
    ) -> Self {
        let metric_limit = metric_registry
            .register_metric::<U64Gauge>("cache_lru_pool_limit", "Limit of the LRU resource pool")
            .recorder(&[("unit", S::unit()), ("pool", name)]);
        let limit = MeasuredT::new(limit, metric_limit);

        let metric_current = metric_registry
            .register_metric::<U64Gauge>(
                "cache_lru_pool_usage",
                "Current consumption of the LRU resource pool",
            )
            .recorder(&[("unit", S::unit()), ("pool", name)]);
        let current = Mutex::new(MeasuredT::new(S::zero(), metric_current));

        let shared = Arc::new(SharedState {
            limit,
            current,
            members: Default::default(),
            change_notify: Default::default(),
        });

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut background_task = JoinSet::new();
        background_task.spawn_on(clean_up_loop(Arc::clone(&shared), rx), runtime_handle);

        Self {
            name,
            shared,
            metric_registry,
            _background_task: background_task,
            notify_idle_test_side: tx,
        }
    }

    /// Get pool limit.
    pub fn limit(&self) -> S {
        self.shared.limit.v
    }

    /// Get current pool usage.
    pub fn current(&self) -> S {
        self.shared.current.lock().v
    }

    /// Register new pool member.
    ///
    /// # Panic
    /// Panics when a member with the specific ID is already registered.
    fn register_member(&self, id: &'static str, member: Weak<dyn PoolMember<S = S>>) {
        let mut members = self.shared.members.lock();

        match members.entry(id) {
            Entry::Vacant(v) => {
                v.insert(member);
            }
            Entry::Occupied(mut o) => {
                if o.get().strong_count() > 0 {
                    panic!("Member '{}' already registered", o.key());
                } else {
                    *o.get_mut() = member;
                }
            }
        }
    }

    /// Add used resource from pool.
    fn add(&self, s: S) {
        let mut current = self.shared.current.lock();
        current.inc(&s);
        if current.v > self.shared.limit.v {
            self.shared.change_notify.notify_one();
        }
    }

    /// Remove used resource from pool.
    fn remove(&self, s: S) {
        self.shared.current.lock().dec(&s);
    }

    /// Wait for the pool to converge to a steady state.
    ///
    /// This usually means that the background worker that runs the eviction loop is idle.
    ///
    /// # Panic
    /// Panics if the background worker is not idle within 5s or if the worker died.
    pub async fn wait_converged(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.notify_idle_test_side
            .send(tx)
            .expect("background worker alive");
        tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .unwrap()
            .unwrap();
    }
}

/// Cache policy that wraps another backend and limits its resource usage.
#[derive(Debug)]
pub struct LruPolicy<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    /// Link to central resource pool.
    pool: Arc<ResourcePool<S>>,

    /// Pool member
    member: Arc<PoolMemberImpl<K, V, S>>,

    /// Resource estimator that is used for new (via [`SET`](Subscriber::set)) entries.
    resource_estimator: Arc<dyn ResourceEstimator<K = K, V = V, S = S>>,

    /// Count number of elements within this specific pool member.
    metric_count: U64Gauge,

    /// Count resource usage of this specific pool member.
    metric_usage: U64Gauge,
}

impl<K, V, S> LruPolicy<K, V, S>
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
        pool: Arc<ResourcePool<S>>,
        id: &'static str,
        resource_estimator: Arc<dyn ResourceEstimator<K = K, V = V, S = S>>,
    ) -> impl FnOnce(CallbackHandle<K, V>) -> Self {
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

        move |mut callback_handle| {
            callback_handle.execute_requests(vec![ChangeRequest::ensure_empty()]);

            let member = Arc::new(PoolMemberImpl {
                id,
                last_used: Arc::new(Mutex::new(AddressableHeap::new())),
                metric_evicted,
                callback_handle: Mutex::new(callback_handle),
            });

            pool.register_member(id, Arc::downgrade(&member) as _);

            Self {
                pool,
                member,
                resource_estimator,
                metric_count,
                metric_usage,
            }
        }
    }
}

impl<K, V, S> Drop for LruPolicy<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    fn drop(&mut self) {
        let size_total = {
            let mut guard = self.member.last_used.lock();
            let mut accu = S::zero();
            while let Some((_k, s, _t)) = guard.pop() {
                accu = accu + s;
            }
            accu
        };
        self.pool.remove(size_total);
    }
}

impl<K, V, S> Subscriber for LruPolicy<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K, now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        trace!(?k, now = now.timestamp_nanos(), "LRU get",);
        let mut last_used = self.member.last_used.lock();

        // update "last used"
        last_used.update_order(k, now);

        vec![]
    }

    fn set(
        &mut self,
        k: &Self::K,
        v: &Self::V,
        now: Time,
    ) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        trace!(?k, now = now.timestamp_nanos(), "LRU set",);

        // determine all attributes before getting any locks
        let consumption = self.resource_estimator.consumption(k, v);

        // "last used" time for new entry
        // Note: this might be updated if the entry already exists
        let mut last_used_t = now;

        // check for oversized entries
        if consumption > self.pool.shared.limit.v {
            return vec![ChangeRequest::remove(k.clone())];
        }

        {
            let mut last_used = self.member.last_used.lock();

            // maybe clean from pool
            if let Some((consumption, last_used_t_previously)) = last_used.remove(k) {
                self.pool.remove(consumption);
                self.metric_count.dec(1);
                self.metric_usage.dec(consumption.into());
                last_used_t = last_used_t_previously;
            }

            // add new entry to inner backend BEFORE adding it to the pool, because the we can overcommit for a short
            // time and we want to give the pool a chance to also evict the new resource
            last_used.insert(k.clone(), consumption, last_used_t);
            self.metric_count.inc(1);
            self.metric_usage.inc(consumption.into());
        }

        // pool-wide operation
        // Since this may wake-up the background worker and cause evictions, drop the `last_used` lock before doing this (see
        // block above) to avoid lock contention.
        self.pool.add(consumption);

        vec![]
    }

    fn remove(&mut self, k: &Self::K, now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        trace!(?k, now = now.timestamp_nanos(), "LRU remove",);
        let mut last_used = self.member.last_used.lock();

        if let Some((consumption, _last_used)) = last_used.remove(k) {
            self.pool.remove(consumption);
            self.metric_count.dec(1);
            self.metric_usage.dec(consumption.into());
        }

        vec![]
    }
}

/// Iterator for enumerating removal candidates of a [`PoolMember`].
///
/// This is type-erased to make [`PoolMember`] object-safe.
type PoolMemberCouldRemove<S> = Box<dyn Iterator<Item = (Time, S, Box<dyn Any>)>>;

/// A member of a [`ResourcePool`]/[`SharedState`].
///
/// The only implementation of this is [`PoolMemberImpl`]. This indirection is required to erase `K` and `V` from specific
/// backend so we can stick it into the generic pool.
trait PoolMember: Debug + Send + Sync + 'static {
    /// Resource type.
    type S;

    /// Check if this member has anything that could be removed.
    ///
    /// If so, return:
    /// - "last used" timestamp
    /// - resource consumption of that entry
    /// - type-erased key
    ///
    /// Elements are returned in order of the "last used" timestamp, in increasing order.
    fn could_remove(&self) -> PoolMemberCouldRemove<Self::S>;

    /// Remove given set of keys.
    ///
    /// The keys MUST be a result of [`could_remove`](Self::could_remove), otherwise the downcasting may not work and panic.
    fn remove_keys(&self, keys: Vec<Box<dyn Any>>);
}

/// The only implementation of [`PoolMember`].
///
/// In contrast to the trait, this still contains `K` and `V`.
#[derive(Debug)]
pub struct PoolMemberImpl<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    /// Pool member ID.
    id: &'static str,

    /// Count number of evicted items.
    metric_evicted: U64Counter,

    /// Tracks usage of the last used elements.
    ///
    /// See documentation of [`callback_handle`](Self::callback_handle) for a reasoning about locking.
    last_used: Arc<Mutex<AddressableHeap<K, S, Time>>>,

    /// Handle to call back into the [`PolicyBackend`] to evict data.
    ///
    /// # Locking
    /// This MUST NOT share a lock with [`last_used`](Self::last_used) because otherwise we would deadlock during
    /// eviction:
    ///
    /// 1. [`remove_keys`](PoolMember::remove_keys)
    /// 2. lock both [`callback_handle`](Self::callback_handle) and [`last_used`](Self::last_used)
    /// 3. [`CallbackHandle::execute_requests`]
    /// 4. [`Subscriber::remove`]
    /// 5. need to lock [`last_used`](Self::last_used) again
    ///
    ///
    /// [`PolicyBackend`]: super::PolicyBackend
    callback_handle: Mutex<CallbackHandle<K, V>>,
}

impl<K, V, S> PoolMember for PoolMemberImpl<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: Resource,
{
    type S = S;

    fn could_remove(&self) -> Box<dyn Iterator<Item = (Time, Self::S, Box<dyn Any>)>> {
        it::build_it(self.last_used.lock_arc())
    }

    fn remove_keys(&self, keys: Vec<Box<dyn Any>>) {
        let keys = keys
            .into_iter()
            .map(|k| *k.downcast::<K>().expect("wrong type"))
            .collect::<Vec<K>>();

        trace!(
            id = self.id,
            ?keys,
            "evicting cache entries due to LRU pressure",
        );
        self.metric_evicted.inc(keys.len() as u64);

        let combined = ChangeRequest::from_fn(move |backend| {
            for k in keys {
                backend.remove(&k);
            }
        });

        self.callback_handle.lock().execute_requests(vec![combined]);
    }
}

/// Helper module that wraps the iterator handling for [`PoolMember`]/[`PoolMemberImpl`].
///
/// This is required because [`ouroboros`] generates a bunch of code that we do not want to leak all over the place.
mod it {
    // ignore some lints for the ouroboros codegen
    #![allow(clippy::future_not_send)]

    use super::*;

    /// The lock that we need to generate a candidate iterator.
    pub type Lock<K, S> =
        parking_lot::lock_api::ArcMutexGuard<parking_lot::RawMutex, AddressableHeap<K, S, Time>>;

    #[self_referencing]
    struct PoolMemberIter<K, S>
    where
        K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
        S: Resource,
    {
        lock: Lock<K, S>,

        #[borrows(lock)]
        #[covariant]
        it: AddressableHeapIter<'this, K, S, Time>,
    }

    impl<K, S> Iterator for PoolMemberIter<K, S>
    where
        K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
        S: Resource,
    {
        type Item = (Time, S, Box<dyn Any>);

        fn next(&mut self) -> Option<Self::Item> {
            self.with_it_mut(|it| {
                it.next()
                    .map(|(k, s, t)| (*t, *s, Box::new(k.clone()) as _))
            })
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.borrow_it().size_hint()
        }
    }

    /// Build iterator.
    pub fn build_it<K, S>(lock: Lock<K, S>) -> PoolMemberCouldRemove<S>
    where
        K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
        S: Resource,
    {
        Box::new(
            PoolMemberIterBuilder {
                lock,
                it_builder: |lock| lock.iter(),
            }
            .build(),
        )
    }
}

/// Background worker that eventually cleans up data if the pool reaches capacity.
///
/// This method NEVER returns.
async fn clean_up_loop<S>(
    shared: Arc<SharedState<S>>,
    mut notify_idle_worker_side: tokio::sync::mpsc::UnboundedReceiver<
        tokio::sync::oneshot::Sender<()>,
    >,
) where
    S: Resource,
{
    'outer: loop {
        // yield to tokio so that the runtime has a chance to abort this function during shutdown
        tokio::task::yield_now().await;

        // get current value but drop the lock immediately
        // Especially we must NOT hold the lock when we later execute the change requests, otherwise there will be two
        // lock direction:
        // - someone adding new resource: member -> pool
        // - clean up loop: pool -> memeber
        let mut current = {
            let guard = shared.current.lock();
            guard.v
        };

        if current <= shared.limit.v {
            // nothing to do, sleep and then continue w/ next round
            loop {
                tokio::select! {
                    // biased sleep so we can notify test hooks if we're idle
                    biased;

                    _ = shared.change_notify.notified() => {continue 'outer;},

                    idle_notify = notify_idle_worker_side.recv() => {
                        if let Some(n) = idle_notify {
                            n.send(()).ok();
                        }
                    },
                }
            }
        }

        // receive members
        // Do NOT hold the member lock during the deletion later because this can lead to deadlocks during shutdown.
        let members = shared.members();
        if members.is_empty() {
            // early retry, there's nothing we can do
            continue;
        }

        // select victims
        let mut victims: BTreeMap<&'static str, Vec<Box<dyn Any>>> = Default::default();
        {
            trace!(
                current = current.into(),
                limit = shared.limit.v.into(),
                "select eviction victims"
            );

            // limit scope of member iterators, because they contain locks and we MUST drop them before proceeding to
            // the actual deletion
            let mut heap: BinaryHeap<EvictionCandidateIter<S>> = members
                .iter()
                .map(|(id, member)| EvictionCandidateIter::new(id, member.could_remove()))
                .collect();

            while current > shared.limit.v {
                let candidate = heap.pop().expect("checked that we have at least 1 member");
                let (candidate, victim) = candidate.next();

                match victim {
                    Some((t, s, k)) => {
                        trace!(
                            id = candidate.id,
                            s = s.into(),
                            t_ns = t.timestamp_nanos(),
                            "found victim"
                        );
                        current = current - s;
                        victims.entry(candidate.id).or_default().push(k);
                    }
                    None => {
                        // The custom `Ord` implementation ensures that we prefer iterators with data over iterators
                        // without any candidates. So if the "best" iterators has NO candidates, this means that ALL
                        // iterators are empty.
                        //
                        // Or in other words: some data was deleted between retrieving the "current" value and locking
                        // the iterators. This is fine, just stop looping and remove the victims that we have selected
                        // so far.
                        trace!("no more data");
                        break;
                    }
                }

                heap.push(candidate);
            }

            trace!("done selecting eviction victims");
        }

        for (id, keys) in victims {
            let member = members.get(id).expect("did get this ID from this map");
            member.remove_keys(keys);
        }
    }
}

/// Current element presented by the [`EvictionCandidateIter`].
type EvictionCandidate<S> = Option<(Time, S, Box<dyn Any>)>;

/// Wraps a [`PoolMember`] so we can compare it in a "tournament" to find out what data to evict.
struct EvictionCandidateIter<S>
where
    S: Resource,
{
    id: &'static str,
    it: PoolMemberCouldRemove<S>,
    current: EvictionCandidate<S>,
}

impl<S> EvictionCandidateIter<S>
where
    S: Resource,
{
    fn new(id: &'static str, mut it: PoolMemberCouldRemove<S>) -> Self {
        let current = it.next();
        Self { id, it, current }
    }

    /// Get next eviction candidate.
    ///
    /// This advances the internal state so that this iterator compares correctly afterwards.
    fn next(mut self) -> (Self, EvictionCandidate<S>) {
        let mut tmp = self.it.next();
        std::mem::swap(&mut tmp, &mut self.current);
        (self, tmp)
    }
}

impl<S> PartialEq for EvictionCandidateIter<S>
where
    S: Resource,
{
    fn eq(&self, other: &Self) -> bool {
        match (self.current.as_ref(), other.current.as_ref()) {
            (None, None) | (Some(_), None) | (None, Some(_)) => false,
            (Some((t1, s1, _k1)), Some((t2, s2, _k2))) => (t1, s1) == (t2, s2),
        }
    }
}

impl<S> Eq for EvictionCandidateIter<S> where S: Resource {}

impl<S> PartialOrd for EvictionCandidateIter<S>
where
    S: Resource,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S> Ord for EvictionCandidateIter<S>
where
    S: Resource,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: reverse order because iterators are kept in a MAX heap
        match (self.current.as_ref(), other.current.as_ref()) {
            (None, None) => {
                // break tie
                self.id.cmp(other.id).reverse()
            }

            // prefer iterators with candidates over empty iterators
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,

            (Some((t1, _s1, _k1)), Some((t2, _s2, _k2))) => {
                // compare by time, break tie using member ID
                (t1, self.id).cmp(&(t2, other.id)).reverse()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use iox_time::{MockProvider, SystemProvider};
    use metric::{Observation, RawReporter};
    use test_helpers::maybe_start_logging;

    use crate::{
        backend::{policy::PolicyBackend, CacheBackend},
        resource_consumption::test_util::TestSize,
    };

    use super::*;

    #[tokio::test]
    #[should_panic(expected = "inner backend is not empty")]
    async fn test_panic_inner_not_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        let policy_constructor = LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        );
        backend.add_policy(|mut callback_handle| {
            callback_handle.execute_requests(vec![ChangeRequest::set(String::from("foo"), 1usize)]);
            policy_constructor(callback_handle)
        })
    }

    #[tokio::test]
    #[should_panic(expected = "Member 'id' already registered")]
    async fn test_panic_id_collision() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend1 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend1.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        ));

        let mut backend2 = PolicyBackend::hashmap_backed(time_provider);
        backend2.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        ));
    }

    #[tokio::test]
    async fn test_reregister_member() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend1 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend1.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        ));
        backend1.set(String::from("a"), 1usize);
        assert_eq!(pool.current(), TestSize(1));

        // drop the backend so re-registering the same ID ("id") MUST NOT panic
        drop(backend1);
        assert_eq!(pool.current(), TestSize(0));

        let mut backend2 = PolicyBackend::hashmap_backed(time_provider);
        backend2.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        ));
        backend2.set(String::from("a"), 2usize);
        assert_eq!(pool.current(), TestSize(2));
    }

    #[tokio::test]
    async fn test_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        assert_eq!(pool.current().0, 0);

        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        assert_eq!(pool.current().0, 0);
    }

    #[tokio::test]
    async fn test_double_set() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(2),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        backend.set(String::from("a"), 1usize);
        time_provider.inc(Duration::from_millis(1));

        backend.set(String::from("b"), 1usize);
        time_provider.inc(Duration::from_millis(1));

        // does NOT count as "used"
        backend.set(String::from("a"), 1usize);
        time_provider.inc(Duration::from_millis(1));

        backend.set(String::from("c"), 1usize);
        pool.wait_converged().await;

        assert_eq!(backend.get(&String::from("a")), None);
    }

    #[tokio::test]
    async fn test_override() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        backend.set(String::from("a"), 5usize);
        assert_eq!(pool.current().0, 5);

        backend.set(String::from("b"), 3usize);
        assert_eq!(pool.current().0, 8);

        backend.set(String::from("a"), 4usize);
        assert_eq!(pool.current().0, 7);
    }

    #[tokio::test]
    async fn test_remove() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        backend.set(String::from("a"), 5usize);
        assert_eq!(pool.current().0, 5);

        backend.set(String::from("b"), 3usize);
        assert_eq!(pool.current().0, 8);

        backend.remove(&String::from("a"));
        assert_eq!(pool.current().0, 3);

        assert_eq!(backend.get(&String::from("a")), None);
        assert_inner_backend(&mut backend, [(String::from("b"), 3)]);

        // removing it again should just work
        backend.remove(&String::from("a"));
        assert_eq!(pool.current().0, 3);
    }

    #[tokio::test]
    async fn test_eviction_order() {
        maybe_start_logging();

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(21),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend1 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend1.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        let mut backend2 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend2.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id2",
            Arc::clone(&resource_estimator) as _,
        ));

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
        pool.wait_converged().await;
        assert_inner_backend(
            &mut backend1,
            [
                (String::from("a"), 3),
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
            ],
        );
        assert_inner_backend(
            &mut backend2,
            [(String::from("a"), 2), (String::from("b"), 6)],
        );

        // adding a single element will drop the smallest key from the first backend (by ID)
        backend1.set(String::from("foo1"), 1usize);
        pool.wait_converged().await;
        assert_eq!(pool.current().0, 19);
        assert_inner_backend(
            &mut backend1,
            [
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
                (String::from("foo1"), 1),
            ],
        );
        assert_inner_backend(
            &mut backend2,
            [(String::from("a"), 2), (String::from("b"), 6)],
        );

        // now we can fill up data up to the capacity again
        backend1.set(String::from("foo2"), 2usize);
        pool.wait_converged().await;
        assert_eq!(pool.current().0, 21);
        assert_inner_backend(
            &mut backend1,
            [
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
            ],
        );
        assert_inner_backend(
            &mut backend2,
            [(String::from("a"), 2), (String::from("b"), 6)],
        );

        // can evict two keys at the same time
        backend1.set(String::from("foo3"), 2usize);
        pool.wait_converged().await;
        assert_eq!(pool.current().0, 18);
        assert_inner_backend(
            &mut backend1,
            [
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
            ],
        );
        assert_inner_backend(
            &mut backend2,
            [(String::from("a"), 2), (String::from("b"), 6)],
        );

        // can evict from another backend
        backend1.set(String::from("foo4"), 4usize);
        pool.wait_converged().await;
        assert_eq!(pool.current().0, 20);
        assert_inner_backend(
            &mut backend1,
            [
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
                (String::from("foo4"), 4),
            ],
        );
        assert_inner_backend(&mut backend2, [(String::from("b"), 6)]);

        // can evict multiple timestamps
        backend1.set(String::from("foo5"), 7usize);
        pool.wait_converged().await;
        assert_eq!(pool.current().0, 16);
        assert_inner_backend(
            &mut backend1,
            [
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
                (String::from("foo4"), 4),
                (String::from("foo5"), 7),
            ],
        );
        assert_inner_backend(&mut backend2, []);
    }

    #[tokio::test]
    async fn test_get_updates_last_used() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(6),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        backend.set(String::from("a"), 1usize);
        backend.set(String::from("b"), 2usize);

        time_provider.inc(Duration::from_millis(1));

        backend.set(String::from("c"), 3usize);
        pool.wait_converged().await;

        time_provider.inc(Duration::from_millis(1));

        assert_eq!(backend.get(&String::from("a")), Some(1usize));

        assert_eq!(pool.current().0, 6);
        assert_inner_backend(
            &mut backend,
            [
                (String::from("a"), 1),
                (String::from("b"), 2),
                (String::from("c"), 3),
            ],
        );

        backend.set(String::from("foo"), 3usize);
        pool.wait_converged().await;
        assert_eq!(pool.current().0, 4);
        assert_inner_backend(
            &mut backend,
            [(String::from("a"), 1), (String::from("foo"), 3)],
        );
    }

    #[tokio::test]
    async fn test_oversized_entries() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        backend.set(String::from("a"), 1usize);
        pool.wait_converged().await;
        backend.set(String::from("b"), 11usize);
        pool.wait_converged().await;

        // "a" did NOT get evicted. Instead we removed the oversized entry straight away.
        assert_eq!(pool.current().0, 1);
        assert_inner_backend(&mut backend, [(String::from("a"), 1)]);
    }

    #[tokio::test]
    async fn test_values_are_dropped() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(3),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
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

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        let k1 = Arc::new(String::from("a"));
        let v1 = Arc::new(2usize);
        let k2 = Arc::new(String::from("b"));
        let v2 = Arc::new(2usize);
        let k1_weak = Arc::downgrade(&k1);
        let v1_weak = Arc::downgrade(&v1);

        backend.set(k1, v1);
        pool.wait_converged().await;

        time_provider.inc(Duration::from_millis(1));

        backend.set(k2, v2);
        pool.wait_converged().await;

        assert_eq!(k1_weak.strong_count(), 0);
        assert_eq!(v1_weak.strong_count(), 0);
    }

    #[tokio::test]
    async fn test_backends_are_dropped() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(3),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
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

        let mut backend = PolicyBackend::new(
            Box::new(Backend {
                marker,
                inner: HashMap::new(),
            }),
            Arc::clone(&time_provider) as _,
        );
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));
        backend.set(String::from("a"), 2usize);

        drop(backend);
        assert_eq!(marker_weak.strong_count(), 0);
    }

    #[tokio::test]
    async fn test_metrics() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(metric::Registry::new());
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&metric_registry),
            &Handle::current(),
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

        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        ));

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
        pool.wait_converged().await;
        backend.set(String::from("b"), 2usize); // usage = 3
        pool.wait_converged().await;
        backend.set(String::from("b"), 3usize); // usage = 4
        pool.wait_converged().await;
        backend.set(String::from("c"), 4usize); // usage = 8
        pool.wait_converged().await;
        backend.set(String::from("d"), 3usize); // usage = 10 (evicted "a")
        pool.wait_converged().await;
        backend.remove(&String::from("c")); // usage = 6
        pool.wait_converged().await;

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

    /// A note regarding the test flavor:
    ///
    /// The main generic test function is not async, so the background clean-up would never fire because we don't
    /// yield to tokio. The test will pass in both cases (w/ a single worker and w/ multiple), however if the
    /// background worker is a actually doing anything it might be a more realistic test case.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_generic_backend() {
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
                Arc::new(metric::Registry::new()),
                &Handle::current(),
            ));
            let resource_estimator = Arc::new(ZeroSizeProvider {});

            let mut backend = PolicyBackend::hashmap_backed(time_provider);
            backend.add_policy(LruPolicy::new(
                Arc::clone(&pool),
                "id",
                Arc::clone(&resource_estimator) as _,
            ));
            backend
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_deadlock() {
        // Regression test for <https://github.com/influxdata/influxdb_iox/issues/8334>.
        test_deadlock_inner(Duration::from_secs(1)).await;

        // Regression test for <https://github.com/influxdata/influxdb_iox/issues/8378>
        for _ in 0..100 {
            test_deadlock_inner(Duration::from_millis(1)).await;
        }
    }

    async fn test_deadlock_inner(test_duration: Duration) {
        #[derive(Debug)]
        struct OneSizeProvider {}

        impl ResourceEstimator for OneSizeProvider {
            type K = u128;
            type V = ();
            type S = TestSize;

            fn consumption(&self, _k: &Self::K, _v: &Self::V) -> Self::S {
                TestSize(1)
            }
        }

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(100),
            Arc::new(metric::Registry::new()),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(OneSizeProvider {});

        let mut backend1 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider));
        backend1.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        let mut backend2 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider));
        backend2.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id2",
            Arc::clone(&resource_estimator) as _,
        ));

        let worker1 = tokio::spawn(async move {
            let mut counter = 0u128;
            loop {
                backend1.set(counter, ());
                counter += 2;
                tokio::task::yield_now().await;
            }
        });
        let worker2 = tokio::spawn(async move {
            let mut counter = 1u128;
            loop {
                backend2.set(counter, ());
                counter += 2;
                tokio::task::yield_now().await;
            }
        });

        tokio::time::sleep(test_duration).await;

        worker1.abort();
        worker2.abort();
    }

    #[tokio::test]
    async fn test_efficient_eviction() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(metric::Registry::new());
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(10),
            Arc::clone(&metric_registry),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id",
            Arc::clone(&resource_estimator) as _,
        ));

        // fill up pool
        for i in 0..10 {
            backend.set(i.to_string(), 1usize);
        }
        assert_eq!(pool.current(), TestSize(10));

        // evict all members using a single large one
        time_provider.inc(Duration::from_millis(1));
        backend.set(String::from("big"), 10usize);
        pool.wait_converged().await;
        assert_eq!(pool.current(), TestSize(10));

        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("cache_lru_member_evicted")
                .unwrap()
                .observation(&[("pool", "pool"), ("member", "id")])
                .unwrap(),
            // it is important that all 10 items are evicted with a single eviction
            &Observation::U64Counter(10)
        );
    }

    #[tokio::test]
    async fn test_eviction_half_half() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(metric::Registry::new());
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(20),
            Arc::clone(&metric_registry),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend1 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend1.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        let mut backend2 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend2.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id2",
            Arc::clone(&resource_estimator) as _,
        ));

        // fill up pool
        for i in 0..10 {
            backend1.set(i.to_string(), 1usize);
            backend2.set(i.to_string(), 1usize);
            time_provider.inc(Duration::from_millis(1));
        }
        assert_eq!(pool.current(), TestSize(20));

        // evict members using a single large one
        time_provider.inc(Duration::from_millis(1));
        backend1.set(String::from("big"), 10usize);
        pool.wait_converged().await;
        assert_eq!(pool.current(), TestSize(20));

        // every member lost 5 entries
        // Note: backend1 has 5+1 items because it own the "big" key
        assert_inner_len(&mut backend1, 6);
        assert_inner_len(&mut backend2, 5);
    }

    #[tokio::test]
    async fn test_eviction_one_member_all_other_member_some() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(metric::Registry::new());
        let pool = Arc::new(ResourcePool::new(
            "pool",
            TestSize(3),
            Arc::clone(&metric_registry),
            &Handle::current(),
        ));
        let resource_estimator = Arc::new(TestResourceEstimator {});

        let mut backend1 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend1.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id1",
            Arc::clone(&resource_estimator) as _,
        ));

        let mut backend2 = PolicyBackend::hashmap_backed(Arc::clone(&time_provider) as _);
        backend2.add_policy(LruPolicy::new(
            Arc::clone(&pool),
            "id2",
            Arc::clone(&resource_estimator) as _,
        ));

        // fill up pool
        backend1.set(String::from("a"), 1usize);
        time_provider.inc(Duration::from_millis(1));
        backend2.set(String::from("a"), 1usize);
        time_provider.inc(Duration::from_millis(1));
        backend2.set(String::from("b"), 1usize);
        assert_eq!(pool.current(), TestSize(3));

        // evict members using a single large one
        time_provider.inc(Duration::from_millis(1));
        backend2.set(String::from("big"), 2usize);
        pool.wait_converged().await;
        assert_eq!(pool.current(), TestSize(3));

        assert_inner_backend(&mut backend1, []);
        assert_inner_backend(
            &mut backend2,
            [(String::from("b"), 1usize), (String::from("big"), 2usize)],
        );
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

    #[track_caller]
    fn assert_inner_backend<const N: usize>(
        backend: &mut PolicyBackend<String, usize>,
        data: [(String, usize); N],
    ) {
        let inner_backend = backend.inner_ref();
        let inner_backend = inner_backend
            .as_any()
            .downcast_ref::<HashMap<String, usize>>()
            .unwrap();
        let expected = HashMap::from(data);
        assert_eq!(inner_backend, &expected);
    }

    #[track_caller]
    fn assert_inner_len(backend: &mut PolicyBackend<String, usize>, len: usize) {
        let inner_backend = backend.inner_ref();
        let inner_backend = inner_backend
            .as_any()
            .downcast_ref::<HashMap<String, usize>>()
            .unwrap();
        assert_eq!(inner_backend.len(), len);
    }
}
