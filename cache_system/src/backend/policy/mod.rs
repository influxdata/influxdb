//! Policy framework for [backends](crate::backend::CacheBackend).

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Weak},
};

use iox_time::{Time, TimeProvider};
use parking_lot::{lock_api::ArcMutexGuard, Mutex, RawMutex, ReentrantMutex};

use super::CacheBackend;

pub mod lru;
pub mod refresh;
pub mod remove_if;
pub mod ttl;

#[cfg(test)]
mod integration_tests;

/// Convenience macro to easily follow the borrow/lock chain of [`StrongSharedInner`].
///
/// This cannot just be a method because we cannot return references to local variables.
macro_rules! lock_inner {
    ($guard:ident = $inner:expr) => {
        let $guard = $inner.lock();
        let $guard = $guard.try_borrow_mut().expect("illegal recursive access");
    };
    (mut $guard:ident = $inner:expr) => {
        let $guard = $inner.lock();
        let mut $guard = $guard.try_borrow_mut().expect("illegal recursive access");
    };
}

/// Backend that is controlled by different policies.
///
/// # Policies & Recursion
///
/// Policies have two tasks:
///
/// - initiate changes (e.g. based on timers)
/// - react to changes
///
/// Getting data from a [`PolicyBackend`] and feeding data back into it in a somewhat synchronous
/// manner sounds really close to recursion. Uncontrolled recursion however is bad for the
/// following reasons:
///
/// 1. **Stack space:** We may easily run out of stack space.
/// 2. **Ownership:** Looping back into the same data structure can easily lead to deadlocks (data
///    corruption is luckily prevented by Rust's ownership model).
///
/// However sometimes we need to have interactions of policies in a "recursive" manner. E.g.:
///
/// 1. A refresh policies updates a value based on a timer. The value gets bigger.
/// 2. Some resource-pool policy decides that this is now too much data and wants to evict data.
/// 3. The refresh policy gets informed about the values that are removed so it can stop refreshing
///    them.
///
/// The solution that [`PolicyBackend`] uses is the following:
///
/// All interaction of the policy with a [`PolicyBackend`] happens through a proxy object called
/// [`ChangeRequest`]. The [`ChangeRequest`] encapsulates a single atomic "transaction" on the
/// underlying store. This can be a simple operation as [`REMOVE`](CacheBackend::remove) but also
/// compound operations like "get+remove" (e.g. to check if a value needs to be pruned from the
/// cache). The policy has two ways of issuing [`ChangeRequest`]s:
///
/// 1. **Initial / self-driven:** Upon creation the policy receives a [`CallbackHandle`] that it
///    can use initiate requests. This handle must only be used to create requests "out of thin
///    air" (e.g. based on a timer). It MUST NOT be used to react to changes (see next point) to
///    avoid deadlocks.
/// 2. **Reactions:** Each policy implements a [`Subscriber`] that receives notifications for each
///    changes. These notification return [`ChangeRequest`]s that the policy wishes to be
///    performed. This construct is designed to avoid recursion.
///
/// Also note that a policy that uses the subscriber interface MUST NOT hold locks on their
/// internal data structure while performing _initial requests_ to avoid deadlocks (since the
/// subscriber will be informed about the changes).
///
/// We cannot guarantee that policies fulfill this interface, but [`PolicyBackend`] performs some
/// sanity checks (e.g. it will catch if the same thread that started an initial requests recurses
/// into another initial request).
///
/// # Change Propagation
///
/// Each [`ChangeRequest`] is processed atomically, so "get + set" / "compare + exchange" patterns
/// work as expected.
///
/// Changes will be propagated "breadth first". This means that the initial changes will form a
/// task list. For every task in this list (front to back), we will execute the [`ChangeRequest`].
/// Every change that is performed within this request (usually only one) we propagate the change
/// as follows:
///
/// 1. underlying backend
/// 2. policies (in the order they where added)
///
/// From step 2 we collect new change requests that will be added to the back of the task list.
///
/// The original requests will return to the caller once all tasks are completed.
///
/// When a [`ChangeRequest`] performs multiple operations -- e.g. [`GET`](CacheBackend::get) and
/// [`SET`](CacheBackend::set) -- we first inform all subscribers about the first operation (in
/// this case: [`GET`](CacheBackend::get)) and collect the resulting [`ChangeRequest`]s. Then we
/// process the second operation (in this case: [`SET`](CacheBackend::set)).
///
/// # `GET`
///
/// The return value for [`CacheBackend::get`] is fetched from the inner backend AFTER all changes
/// are applied.
///
/// Note [`ChangeRequest::get`] has no way of returning a result to the [`Subscriber`] that created
/// it. The "changes" solely act as some kind of "keep alive" / "this was used" signal.
///
/// # Example
///
/// **The policies in these examples are deliberately silly but simple!**
///
/// Let's start with a purely reactive policy that will round up all integer values to the next
/// even number:
///
/// ```
/// use std::{
///     collections::HashMap,
///     sync::Arc,
/// };
/// use cache_system::backend::{
///     CacheBackend,
///     policy::{
///         ChangeRequest,
///         PolicyBackend,
///         Subscriber,
///     },
/// };
/// use iox_time::{
///     SystemProvider,
///     Time,
/// };
///
/// #[derive(Debug)]
/// struct EvenNumberPolicy;
///
/// type CR = ChangeRequest<'static, &'static str, u64>;
///
/// impl Subscriber for EvenNumberPolicy {
///     type K = &'static str;
///     type V = u64;
///
///     fn set(&mut self, k: &&'static str, v: &u64, _now: Time) -> Vec<CR> {
///       // When new key `k` is set to value `v` if `v` is odd,
///       // request a change to set `k` to `v+1`
///         if v % 2 == 1 {
///             vec![CR::set(k, v + 1)]
///         } else {
///             vec![]
///         }
///     }
/// }
///
/// let mut backend = PolicyBackend::new(
///     Box::new(HashMap::new()),
///     Arc::new(SystemProvider::new()),
/// );
/// backend.add_policy(|_callback_backend| EvenNumberPolicy);
///
/// backend.set("foo", 8);
/// backend.set("bar", 9);
///
/// assert_eq!(backend.get(&"foo"), Some(8));
/// assert_eq!(backend.get(&"bar"), Some(10));
/// ```
///
/// And here is a more active backend that regularly writes the current system time to a key:
///
/// ```
/// use std::{
///     collections::HashMap,
///     sync::{
///         Arc,
///         atomic::{AtomicBool, Ordering},
///     },
///     thread::{JoinHandle, sleep, spawn},
///     time::{Duration, Instant},
/// };
/// use cache_system::backend::{
///     CacheBackend,
///     policy::{
///         ChangeRequest,
///         PolicyBackend,
///         Subscriber,
///     },
/// };
/// use iox_time::SystemProvider;
///
/// #[derive(Debug)]
/// struct NowPolicy {
///     cancel: Arc<AtomicBool>,
///     join_handle: Option<JoinHandle<()>>,
/// };
///
/// impl Drop for NowPolicy {
///     fn drop(&mut self) {
///         self.cancel.store(true, Ordering::SeqCst);
///         self.join_handle
///             .take()
///             .expect("worker thread present")
///             .join()
///             .expect("worker thread finished");
///     }
/// }
///
/// type CR = ChangeRequest<'static, &'static str, Instant>;
///
/// impl Subscriber for NowPolicy {
///     type K = &'static str;
///     type V = Instant;
/// }
///
/// let mut backend = PolicyBackend::new(
///     Box::new(HashMap::new()),
///     Arc::new(SystemProvider::new()),
/// );
/// backend.add_policy(|mut callback_handle| {
///     let cancel = Arc::new(AtomicBool::new(false));
///     let cancel_captured = Arc::clone(&cancel);
///     let join_handle = spawn(move || {
///         loop {
///             if cancel_captured.load(Ordering::SeqCst) {
///                 break;
///             }
///             callback_handle.execute_requests(vec![
///                 CR::set("now", Instant::now()),
///             ]);
///             sleep(Duration::from_millis(1));
///         }
///     });
///     NowPolicy{cancel, join_handle: Some(join_handle)}
/// });
///
///
/// // eventually we should see a key
/// let t_start = Instant::now();
/// loop {
///     if let Some(t) = backend.get(&"now") {
///         // value should be fresh
///         assert!(t.elapsed() < Duration::from_millis(100));
///         break;
///     }
///
///     assert!(t_start.elapsed() < Duration::from_secs(1));
///     sleep(Duration::from_millis(10));
/// }
/// ```
#[derive(Debug)]
pub struct PolicyBackend<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner: StrongSharedInner<K, V>,
}

impl<K, V> PolicyBackend<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Create new backend w/o any policies.
    ///
    /// # Panic
    ///
    /// Panics if `inner` is not empty.
    pub fn new(
        inner: Box<dyn CacheBackend<K = K, V = V>>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        assert!(inner.is_empty(), "inner backend is not empty");

        Self {
            inner: Arc::new(ReentrantMutex::new(RefCell::new(PolicyBackendInner {
                inner: Arc::new(Mutex::new(inner)),
                subscribers: Vec::new(),
                time_provider,
            }))),
        }
    }

    /// Create a new backend with a HashMap as the [`CacheBackend`].
    pub fn hashmap_backed(time_provider: Arc<dyn TimeProvider>) -> Self {
        // See <https://github.com/rust-lang/rust-clippy/issues/9621>. This clippy lint suggests
        // replacing `Box::new(HashMap::new())` with `Box::default()`, which in most cases would be
        // shorter, but because this type is actually a `Box<dyn Trait>`, the replacement would
        // need to be `Box::<HashMap<_, _>>::default()`, which doesn't seem like an improvement.
        #[allow(clippy::box_default)]
        Self::new(Box::new(HashMap::new()), Arc::clone(&time_provider))
    }

    /// Adds new policy.
    ///
    /// See documentation of [`PolicyBackend`] for more information.
    ///
    /// This is called with a function that receives the "callback backend" to this backend and
    /// should return a [`Subscriber`]. This loopy construct was chosen to discourage the leakage
    /// of the "callback backend" to any other object.
    pub fn add_policy<C, S>(&mut self, policy_constructor: C)
    where
        C: FnOnce(CallbackHandle<K, V>) -> S,
        S: Subscriber<K = K, V = V>,
    {
        let callback_handle = CallbackHandle {
            inner: Arc::downgrade(&self.inner),
        };
        let subscriber = policy_constructor(callback_handle);
        lock_inner!(mut guard = self.inner);
        guard.subscribers.push(Box::new(subscriber));
    }

    /// Provide temporary read-only access to the underlying backend.
    ///
    /// This is mostly useful for debugging and testing.
    pub fn inner_ref(&mut self) -> InnerBackendRef<'_, K, V> {
        // NOTE: We deliberately use a mutable reference here to prevent users from using `<Self as
        // CacheBackend>` while we hold a lock to the underlying backend.
        lock_inner!(guard = self.inner);
        InnerBackendRef {
            inner: guard.inner.lock_arc(),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> CacheBackend for PolicyBackend<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        lock_inner!(mut guard = self.inner);
        perform_changes(&mut guard, vec![ChangeRequest::get(k.clone())]);

        // poll inner backend AFTER everything has settled
        let mut inner = guard.inner.lock();
        inner.get(k)
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        lock_inner!(mut guard = self.inner);
        perform_changes(&mut guard, vec![ChangeRequest::set(k, v)]);
    }

    fn remove(&mut self, k: &Self::K) {
        lock_inner!(mut guard = self.inner);
        perform_changes(&mut guard, vec![ChangeRequest::remove(k.clone())]);
    }

    fn is_empty(&self) -> bool {
        lock_inner!(guard = self.inner);
        let inner = guard.inner.lock();
        inner.is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Handle that allows a [`Subscriber`] to send [`ChangeRequest`]s back to the [`PolicyBackend`]
/// that owns that very [`Subscriber`].
#[derive(Debug)]
pub struct CallbackHandle<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner: WeakSharedInner<K, V>,
}

impl<K, V> CallbackHandle<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Start a series of requests to the [`PolicyBackend`] that is referenced by this handle.
    ///
    /// This method returns AFTER the requests and all the follow-up changes requested by all
    /// policies are played out. You should NOT hold a lock on your policies internal data
    /// structures while calling this function if you plan to also [subscribe](Subscriber) to
    /// changes because this would easily lead to deadlocks.
    pub fn execute_requests(&mut self, change_requests: Vec<ChangeRequest<'_, K, V>>) {
        let Some(inner) = self.inner.upgrade() else {
            // backend gone, can happen during shutdowns, try not to panic
            return;
        };

        lock_inner!(mut guard = inner);
        perform_changes(&mut guard, change_requests);
    }
}

#[derive(Debug)]
struct PolicyBackendInner<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Underlying cache backend.
    ///
    /// This is wrapped into another `Arc<Mutex<...>>` construct even though [`PolicyBackendInner`]
    /// is already guarded by a lock because we need to reference the underlying backend from
    /// [`Recorder`], and [`Recorder`] implements [`CacheBackend`] which is `'static`.
    inner: Arc<Mutex<Box<dyn CacheBackend<K = K, V = V>>>>,

    /// List of subscribers.
    subscribers: Vec<Box<dyn Subscriber<K = K, V = V>>>,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,
}

type WeakSharedInner<K, V> = Weak<ReentrantMutex<RefCell<PolicyBackendInner<K, V>>>>;
type StrongSharedInner<K, V> = Arc<ReentrantMutex<RefCell<PolicyBackendInner<K, V>>>>;

/// Perform changes breadth first.
fn perform_changes<K, V>(
    inner: &mut PolicyBackendInner<K, V>,
    change_requests: Vec<ChangeRequest<'_, K, V>>,
) where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    let mut tasks = VecDeque::from(change_requests);
    let now = inner.time_provider.now();

    while let Some(change_request) = tasks.pop_front() {
        let mut recorder = Recorder {
            inner: Arc::clone(&inner.inner),
            records: vec![],
        };

        change_request.eval(&mut recorder);

        for record in recorder.records {
            for subscriber in &mut inner.subscribers {
                let requests = match &record {
                    Record::Get { k } => subscriber.get(k, now),
                    Record::Set { k, v } => subscriber.set(k, v, now),
                    Record::Remove { k } => subscriber.remove(k, now),
                };

                tasks.extend(requests.into_iter());
            }
        }
    }
}

/// Subscriber to change events.
pub trait Subscriber: Debug + Send + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Ord + Debug + Send + 'static;

    /// Cached value.
    type V: Clone + Debug + Send + 'static;

    /// Get value for given key if it exists.
    ///
    /// The current time `now` is provided as a parameter so that all policies and backends use a
    /// unified timestamp rather than their own provider, which is more consistent and performant.
    fn get(&mut self, _k: &Self::K, _now: Time) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        // do nothing by default
        vec![]
    }

    /// Set value for given key.
    ///
    /// It is OK to set and override a key that already exists.
    ///
    /// The current time `now` is provided as a parameter so that all policies and backends use a
    /// unified timestamp rather than their own provider, which is more consistent and performant.
    fn set(
        &mut self,
        _k: &Self::K,
        _v: &Self::V,
        _now: Time,
    ) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        // do nothing by default
        vec![]
    }

    /// Remove value for given key.
    ///
    /// It is OK to remove a key even when it does not exist.
    ///
    /// The current time `now` is provided as a parameter so that all policies and backends use a
    /// unified timestamp rather than their own provider, which is more consistent and performant.
    fn remove(
        &mut self,
        _k: &Self::K,
        _now: Time,
    ) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
        // do nothing by default
        vec![]
    }
}

/// A change request to a backend.
pub struct ChangeRequest<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fun: ChangeRequestFn<'a, K, V>,
}

impl<'a, K, V> Debug for ChangeRequest<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheRequest").finish_non_exhaustive()
    }
}

impl<'a, K, V> ChangeRequest<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Custom way of constructing a change request.
    ///
    /// This is considered a rather low-level function and you should prefer the higher-level
    /// constructs like [`get`](Self::get), [`set`](Self::set), and [`remove`](Self::remove).
    ///
    /// Takes a "callback backend" and can freely act on it. The underlying backend of
    /// [`PolicyBackend`] is guaranteed to be locked during a single request, so "get + modify"
    /// patterns work out of the box without the need to fear interleaving modifications.
    pub fn from_fn<F>(f: F) -> Self
    where
        F: for<'b> FnOnce(&'b mut Recorder<K, V>) + 'a,
    {
        Self { fun: Box::new(f) }
    }

    /// [`GET`](CacheBackend::get)
    pub fn get(k: K) -> Self {
        Self::from_fn(move |backend| {
            backend.get(&k);
        })
    }

    /// [`SET`](CacheBackend::set)
    pub fn set(k: K, v: V) -> Self {
        Self::from_fn(move |backend| {
            backend.set(k, v);
        })
    }

    /// [`REMOVE`](CacheBackend::remove).
    pub fn remove(k: K) -> Self {
        Self::from_fn(move |backend| {
            backend.remove(&k);
        })
    }

    /// Ensure that backend is empty and panic otherwise.
    ///
    /// This is mostly useful during initialization.
    pub fn ensure_empty() -> Self {
        Self::from_fn(|backend| {
            assert!(backend.is_empty(), "inner backend is not empty");
        })
    }

    /// Execute this change request.
    pub fn eval(self, backend: &mut Recorder<K, V>) {
        (self.fun)(backend)
    }
}

/// Function captured within [`ChangeRequest`].
type ChangeRequestFn<'a, K, V> = Box<dyn for<'b> FnOnce(&'b mut Recorder<K, V>) + 'a>;

/// Records of interactions with the callback [`CacheBackend`].
#[derive(Debug, PartialEq)]
enum Record<K, V> {
    /// [`GET`](CacheBackend::get)
    Get {
        /// Key.
        k: K,
    },

    /// [`SET`](CacheBackend::set)
    Set {
        /// Key.
        k: K,

        /// Value.
        v: V,
    },

    /// [`REMOVE`](CacheBackend::remove).
    Remove {
        /// Key.
        k: K,
    },
}

/// Specialized [`CacheBackend`] that forwards changes and requests to the underlying backend of
/// [`PolicyBackend`] but also records all changes into [`Record`]s.
#[derive(Debug)]
pub struct Recorder<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner: Arc<Mutex<Box<dyn CacheBackend<K = K, V = V>>>>,
    records: Vec<Record<K, V>>,
}

impl<K, V> Recorder<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    /// Perform a [`GET`](CacheBackend::get) request that is NOT seen by other policies.
    ///
    /// This is helpful if you just want to check the underlying data of a key without treating it
    /// as "used".
    ///
    /// Note that this functionality only exists for [`GET`](CacheBackend::get) requests, not for
    /// modifying requests like [`SET`](CacheBackend::set) or [`REMOVE`](CacheBackend::remove)
    /// since they always require policies to be in-sync.
    pub fn get_untracked(&mut self, k: &K) -> Option<V> {
        self.inner.lock().get(k)
    }
}

impl<K, V> CacheBackend for Recorder<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        self.records.push(Record::Get { k: k.clone() });
        self.inner.lock().get(k)
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        self.records.push(Record::Set {
            k: k.clone(),
            v: v.clone(),
        });
        self.inner.lock().set(k, v);
    }

    fn remove(&mut self, k: &Self::K) {
        self.records.push(Record::Remove { k: k.clone() });
        self.inner.lock().remove(k);
    }

    fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Read-only ref to the inner backend of [`PolicyBackend`] for debugging.
pub struct InnerBackendRef<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner: ArcMutexGuard<RawMutex, Box<dyn CacheBackend<K = K, V = V>>>,
    _phantom: PhantomData<&'a mut ()>,
}

// Workaround for <https://github.com/rust-lang/rust/issues/100573>.
impl<'a, K, V> Drop for InnerBackendRef<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn drop(&mut self) {}
}

impl<'a, K, V> Debug for InnerBackendRef<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerBackendRef").finish_non_exhaustive()
    }
}

impl<'a, K, V> Deref for InnerBackendRef<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type Target = dyn CacheBackend<K = K, V = V>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Barrier, thread::JoinHandle};

    use iox_time::MockProvider;

    use super::*;

    #[allow(dead_code)]
    const fn assert_send<T: Send>() {}
    const _: () = assert_send::<CallbackHandle<String, usize>>();

    #[test]
    #[should_panic(expected = "inner backend is not empty")]
    fn test_panic_inner_not_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        PolicyBackend::new(
            Box::new(HashMap::from([(String::from("foo"), 1usize)])),
            time_provider,
        );
    }

    #[test]
    fn test_generic() {
        crate::backend::test_util::test_generic(|| {
            let time_provider = Arc::new(MockProvider::new(Time::MIN));
            PolicyBackend::hashmap_backed(time_provider)
        })
    }

    #[test]
    #[should_panic(expected = "test steps left")]
    fn test_meta_panic_steps_left() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Set {
                k: String::from("a"),
                v: 1,
            },
            action: TestAction::ChangeRequests(vec![]),
        }]));
    }

    #[test]
    #[should_panic(expected = "step left for get operation")]
    fn test_meta_panic_requires_condition_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![]));

        backend.get(&String::from("a"));
    }

    #[test]
    #[should_panic(expected = "step left for set operation")]
    fn test_meta_panic_requires_condition_set() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![]));

        backend.set(String::from("a"), 2);
    }

    #[test]
    #[should_panic(expected = "step left for remove operation")]
    fn test_meta_panic_requires_condition_remove() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![]));

        backend.remove(&String::from("a"));
    }

    #[test]
    #[should_panic(expected = "Condition mismatch")]
    fn test_meta_panic_checks_condition_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Get {
                k: String::from("a"),
            },
            action: TestAction::ChangeRequests(vec![]),
        }]));

        backend.get(&String::from("b"));
    }

    #[test]
    #[should_panic(expected = "Condition mismatch")]
    fn test_meta_panic_checks_condition_set() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Set {
                k: String::from("a"),
                v: 1,
            },
            action: TestAction::ChangeRequests(vec![]),
        }]));

        backend.set(String::from("a"), 2);
    }

    #[test]
    #[should_panic(expected = "Condition mismatch")]
    fn test_meta_panic_checks_condition_remove() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Remove {
                k: String::from("a"),
            },
            action: TestAction::ChangeRequests(vec![]),
        }]));

        backend.remove(&String::from("b"));
    }

    #[test]
    fn test_basic_propagation() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("b"),
                    v: 2,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("b"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("b"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 1);
        backend.set(String::from("b"), 2);
        backend.remove(&String::from("b"));

        assert_eq!(backend.get(&String::from("a")), Some(1));
        assert_eq!(backend.get(&String::from("b")), None);
    }

    #[test]
    #[should_panic(expected = "illegal recursive access")]
    fn test_panic_recursion_detection_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Remove {
                k: String::from("a"),
            },
            action: TestAction::CallBackendDirectly(TestBackendInteraction::Get {
                k: String::from("b"),
            }),
        }]));

        backend.remove(&String::from("a"));
    }

    #[test]
    #[should_panic(expected = "illegal recursive access")]
    fn test_panic_recursion_detection_set() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Remove {
                k: String::from("a"),
            },
            action: TestAction::CallBackendDirectly(TestBackendInteraction::Set {
                k: String::from("b"),
                v: 1,
            }),
        }]));

        backend.remove(&String::from("a"));
    }

    #[test]
    #[should_panic(expected = "illegal recursive access")]
    fn test_panic_recursion_detection_remove() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Remove {
                k: String::from("a"),
            },
            action: TestAction::CallBackendDirectly(TestBackendInteraction::Remove {
                k: String::from("b"),
            }),
        }]));

        backend.remove(&String::from("a"));
    }

    #[test]
    fn test_get_untracked() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::from_fn(
                    |backend| {
                        assert_eq!(backend.get_untracked(&String::from("a")), Some(1));
                    },
                )]),
            },
            // NO `GET` interaction triggered here!
        ]));

        backend.set(String::from("a"), 1);
    }

    #[test]
    fn test_basic_get_set() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    1,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        assert_eq!(backend.get(&String::from("a")), Some(1));
    }

    #[test]
    fn test_basic_get_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::get(String::from(
                    "a",
                ))]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        assert_eq!(backend.get(&String::from("a")), None);
    }

    #[test]
    fn test_basic_set_set_get_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("b"),
                    2,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("b"),
                    v: 2,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("b"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 1);

        assert_eq!(backend.get(&String::from("a")), Some(1));
        assert_eq!(backend.get(&String::from("b")), Some(2));
    }

    #[test]
    fn test_basic_set_remove_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::remove(
                    String::from("a"),
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 1);

        assert_eq!(backend.get(&String::from("a")), None);
    }

    #[test]
    fn test_basic_remove_set_get_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("b"),
                    1,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("b"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("b"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.remove(&String::from("a"));

        assert_eq!(backend.get(&String::from("a")), None);
        assert_eq!(backend.get(&String::from("b")), Some(1));
    }

    #[test]
    fn test_basic_remove_remove_get_get() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::remove(
                    String::from("b"),
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("b"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("b"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.remove(&String::from("a"));

        assert_eq!(backend.get(&String::from("a")), None);
        assert_eq!(backend.get(&String::from("b")), None);
    }

    #[test]
    fn test_ordering_within_requests_vector() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![
                    SendableChangeRequest::set(String::from("a"), 12),
                    SendableChangeRequest::set(String::from("a"), 13),
                ]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 11);

        assert_eq!(backend.get(&String::from("a")), Some(13));
    }

    #[test]
    fn test_ordering_across_policies() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    12,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    13,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 11);

        assert_eq!(backend.get(&String::from("a")), Some(13));
    }

    #[test]
    fn test_ping_pong() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    12,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    14,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 14,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    13,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 14,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 11);

        assert_eq!(backend.get(&String::from("a")), Some(14));
    }

    #[test]
    #[should_panic(expected = "this is a test")]
    fn test_meta_multithread_panics_are_propagated() {
        let barrier_pre = Arc::new(Barrier::new(2));
        let barrier_post = Arc::new(Barrier::new(1));

        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![TestStep {
            condition: TestBackendInteraction::Set {
                k: String::from("a"),
                v: 1,
            },
            action: TestAction::CallBackendDelayed(
                Arc::clone(&barrier_pre),
                TestBackendInteraction::Panic,
                Arc::clone(&barrier_post),
            ),
        }]));

        backend.set(String::from("a"), 1);
        barrier_pre.wait();

        // panic on drop
    }

    /// Checks that a policy background task can access the "callback backend" without triggering
    /// the "illegal recursion" detection.
    #[test]
    fn test_multithread() {
        let barrier_pre = Arc::new(Barrier::new(2));
        let barrier_post = Arc::new(Barrier::new(2));

        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::CallBackendDelayed(
                    Arc::clone(&barrier_pre),
                    TestBackendInteraction::Set {
                        k: String::from("a"),
                        v: 4,
                    },
                    Arc::clone(&barrier_post),
                ),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 2,
                },
                action: TestAction::BlockAndChangeRequest(
                    Arc::clone(&barrier_pre),
                    vec![SendableChangeRequest::set(String::from("a"), 3)],
                ),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 3,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 4,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 1);
        assert_eq!(backend.get(&String::from("a")), Some(1));

        backend.set(String::from("a"), 2);

        barrier_post.wait();
        assert_eq!(backend.get(&String::from("a")), Some(4));
    }

    #[test]
    fn test_get_from_policies_are_propagated() {
        let barrier_pre = Arc::new(Barrier::new(2));
        let barrier_post = Arc::new(Barrier::new(2));

        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::CallBackendDelayed(
                    Arc::clone(&barrier_pre),
                    TestBackendInteraction::Get {
                        k: String::from("a"),
                    },
                    Arc::clone(&barrier_post),
                ),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 1);
        barrier_pre.wait();
        barrier_post.wait();
    }

    /// Checks that dropping [`PolicyBackend`] drop the policies as well as the inner backend.
    #[test]
    fn test_drop() {
        let marker_backend = Arc::new(());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::new(
            Box::new(DropTester(Arc::clone(&marker_backend), ())),
            time_provider,
        );

        let marker_policy = Arc::new(());
        backend.add_policy(|callback| DropTester(Arc::clone(&marker_policy), callback));

        assert_eq!(Arc::strong_count(&marker_backend), 2);
        assert_eq!(Arc::strong_count(&marker_policy), 2);

        drop(backend);

        assert_eq!(Arc::strong_count(&marker_backend), 1);
        assert_eq!(Arc::strong_count(&marker_policy), 1);
    }

    /// We have to ways of handling "compound" [`ChangeRequest`]s, i.e. requests that perform
    /// multiple operations:
    ///
    /// 1. We could loop over the operations and inner-loop over the policies to collect reactions
    /// 2. We could loop over all the policies and present each polices all operations in one go
    ///
    /// We've decided to chose option 1. This test ensures that by setting up a compound request
    /// (reacting to `set("a", 11)`) with a compound of two operations (`set("a", 12)`, `set("a",
    /// 13)`) which we call `C1` and `C2` (for "compound 1 and 2"). The two policies react to
    /// these two compound operations as follows:
    ///
    /// |    | Policy 1       | Policy 2       |
    /// | -- | -------------- | -------------- |
    /// | C1 | `set("a", 14)` | `set("a", 15)` |
    /// | C2 | `set("a", 16)` | --             |
    ///
    /// For option (1) the outcome will be `"a" -> 16`, for option (2) the outcome would be `"a" ->
    /// 15`.
    #[test]
    fn test_ordering_within_compound_requests() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut backend = PolicyBackend::hashmap_backed(time_provider);
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::from_fn(
                    |backend| {
                        backend.set(String::from("a"), 12);
                        backend.set(String::from("a"), 13);
                    },
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    14,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    16,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 14,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 15,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 16,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 12,
                },
                action: TestAction::ChangeRequests(vec![SendableChangeRequest::set(
                    String::from("a"),
                    15,
                )]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 13,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 14,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 15,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 16,
                },
                action: TestAction::ChangeRequests(vec![]),
            },
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![]),
            },
        ]));

        backend.set(String::from("a"), 11);

        assert_eq!(backend.get(&String::from("a")), Some(16));
    }

    #[derive(Debug)]
    struct DropTester<T>(Arc<()>, T)
    where
        T: Debug + Send + 'static;

    impl<T> CacheBackend for DropTester<T>
    where
        T: Debug + Send + 'static,
    {
        type K = ();
        type V = ();

        fn get(&mut self, _k: &Self::K) -> Option<Self::V> {
            unreachable!()
        }

        fn set(&mut self, _k: Self::K, _v: Self::V) {
            unreachable!()
        }

        fn remove(&mut self, _k: &Self::K) {
            unreachable!()
        }

        fn is_empty(&self) -> bool {
            true
        }

        fn as_any(&self) -> &dyn std::any::Any {
            unreachable!()
        }
    }

    impl<T> Subscriber for DropTester<T>
    where
        T: Debug + Send + 'static,
    {
        type K = ();
        type V = ();
    }

    fn create_test_policy(
        steps: Vec<TestStep>,
    ) -> impl FnOnce(CallbackHandle<String, usize>) -> TestSubscriber {
        |handle| TestSubscriber {
            background_task: TestSubscriberBackgroundTask::NotStarted(handle),
            steps: VecDeque::from(steps),
        }
    }

    #[derive(Debug, PartialEq)]
    enum TestBackendInteraction {
        Get { k: String },

        Set { k: String, v: usize },

        Remove { k: String },

        Panic,
    }

    impl TestBackendInteraction {
        fn perform(self, handle: &mut CallbackHandle<String, usize>) {
            match self {
                Self::Get { k } => {
                    handle.execute_requests(vec![ChangeRequest::get(k)]);
                }
                Self::Set { k, v } => handle.execute_requests(vec![ChangeRequest::set(k, v)]),
                Self::Remove { k } => handle.execute_requests(vec![ChangeRequest::remove(k)]),
                Self::Panic => panic!("this is a test"),
            }
        }
    }

    #[derive(Debug)]
    enum TestAction {
        /// Perform an illegal direct, recursive call to the backend.
        CallBackendDirectly(TestBackendInteraction),

        /// Return change requests
        ChangeRequests(Vec<SendableChangeRequest>),

        /// Use callback backend but wait for a barrier in a background thread.
        ///
        /// This will return immediately.
        CallBackendDelayed(Arc<Barrier>, TestBackendInteraction, Arc<Barrier>),

        /// Block on barrier and return afterwards.
        BlockAndChangeRequest(Arc<Barrier>, Vec<SendableChangeRequest>),
    }

    impl TestAction {
        fn perform(
            self,
            background_task: &mut TestSubscriberBackgroundTask,
        ) -> Vec<ChangeRequest<'static, String, usize>> {
            match self {
                Self::CallBackendDirectly(interaction) => {
                    let handle = match background_task {
                        TestSubscriberBackgroundTask::NotStarted(handle) => handle,
                        TestSubscriberBackgroundTask::Started(_) => {
                            panic!("background task already started")
                        }
                        TestSubscriberBackgroundTask::Invalid => panic!("Invalid state"),
                    };

                    interaction.perform(handle);
                    unreachable!("illegal call should have failed")
                }
                Self::ChangeRequests(change_requests) => {
                    change_requests.into_iter().map(|r| r.into()).collect()
                }
                Self::CallBackendDelayed(barrier_pre, interaction, barrier_post) => {
                    let mut tmp = TestSubscriberBackgroundTask::Invalid;
                    std::mem::swap(&mut tmp, background_task);
                    let mut handle = match tmp {
                        TestSubscriberBackgroundTask::NotStarted(handle) => handle,
                        TestSubscriberBackgroundTask::Started(_) => {
                            panic!("background task already started")
                        }
                        TestSubscriberBackgroundTask::Invalid => panic!("Invalid state"),
                    };

                    let join_handle = std::thread::spawn(move || {
                        barrier_pre.wait();
                        interaction.perform(&mut handle);
                        barrier_post.wait();
                    });
                    *background_task = TestSubscriberBackgroundTask::Started(join_handle);

                    vec![]
                }
                Self::BlockAndChangeRequest(barrier, change_requests) => {
                    barrier.wait();
                    change_requests.into_iter().map(|r| r.into()).collect()
                }
            }
        }
    }

    #[derive(Debug)]
    struct TestStep {
        condition: TestBackendInteraction,
        action: TestAction,
    }

    #[derive(Debug)]
    enum TestSubscriberBackgroundTask {
        NotStarted(CallbackHandle<String, usize>),
        Started(JoinHandle<()>),

        /// Temporary variant for swapping.
        Invalid,
    }

    #[derive(Debug)]
    struct TestSubscriber {
        background_task: TestSubscriberBackgroundTask,
        steps: VecDeque<TestStep>,
    }

    impl Drop for TestSubscriber {
        fn drop(&mut self) {
            // prevent SIGABRT due to double-panic
            if !std::thread::panicking() {
                assert!(self.steps.is_empty(), "test steps left");
                let mut tmp = TestSubscriberBackgroundTask::Invalid;
                std::mem::swap(&mut tmp, &mut self.background_task);

                match tmp {
                    TestSubscriberBackgroundTask::NotStarted(_) => {
                        // nothing to check
                    }
                    TestSubscriberBackgroundTask::Started(handle) => {
                        // propagate panics
                        if let Err(e) = handle.join() {
                            if let Some(err) = e.downcast_ref::<&str>() {
                                panic!("Error in background task: {err}")
                            } else if let Some(err) = e.downcast_ref::<String>() {
                                panic!("Error in background task: {err}")
                            } else {
                                panic!("Error in background task: <unknown>")
                            }
                        }
                    }
                    TestSubscriberBackgroundTask::Invalid => {
                        // that's OK during drop
                    }
                }
            }
        }
    }

    impl Subscriber for TestSubscriber {
        type K = String;
        type V = usize;

        fn get(
            &mut self,
            k: &Self::K,
            _now: Time,
        ) -> Vec<ChangeRequest<'static, Self::K, Self::V>> {
            let step = self.steps.pop_front().expect("step left for get operation");

            let expected_condition = TestBackendInteraction::Get { k: k.clone() };
            assert_eq!(
                step.condition, expected_condition,
                "Condition mismatch\n\nActual:\n{:#?}\n\nExpected:\n{:#?}",
                step.condition, expected_condition,
            );

            step.action.perform(&mut self.background_task)
        }

        fn set(
            &mut self,
            k: &Self::K,
            v: &Self::V,
            _now: Time,
        ) -> Vec<ChangeRequest<'static, String, usize>> {
            let step = self.steps.pop_front().expect("step left for set operation");

            let expected_condition = TestBackendInteraction::Set {
                k: k.clone(),
                v: *v,
            };
            assert_eq!(
                step.condition, expected_condition,
                "Condition mismatch\n\nActual:\n{:#?}\n\nExpected:\n{:#?}",
                step.condition, expected_condition,
            );

            step.action.perform(&mut self.background_task)
        }

        fn remove(
            &mut self,
            k: &Self::K,
            _now: Time,
        ) -> Vec<ChangeRequest<'static, String, usize>> {
            let step = self
                .steps
                .pop_front()
                .expect("step left for remove operation");

            let expected_condition = TestBackendInteraction::Remove { k: k.clone() };
            assert_eq!(
                step.condition, expected_condition,
                "Condition mismatch\n\nActual:\n{:#?}\n\nExpected:\n{:#?}",
                step.condition, expected_condition,
            );

            step.action.perform(&mut self.background_task)
        }
    }

    /// Same as [`ChangeRequestFn`] but implements `Send`.
    type SendableChangeRequestFn =
        Box<dyn for<'a> FnOnce(&'a mut Recorder<String, usize>) + Send + 'static>;

    /// Same as [`ChangeRequest`] but implements `Send`.
    struct SendableChangeRequest {
        fun: SendableChangeRequestFn,
    }

    impl Debug for SendableChangeRequest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SendableCacheRequest")
                .finish_non_exhaustive()
        }
    }

    impl SendableChangeRequest {
        fn from_fn<F>(f: F) -> Self
        where
            F: for<'b> FnOnce(&'b mut Recorder<String, usize>) + Send + 'static,
        {
            Self { fun: Box::new(f) }
        }

        fn get(k: String) -> Self {
            Self::from_fn(move |backend| {
                backend.get(&k);
            })
        }

        fn set(k: String, v: usize) -> Self {
            Self::from_fn(move |backend| {
                backend.set(k, v);
            })
        }

        fn remove(k: String) -> Self {
            Self::from_fn(move |backend| {
                backend.remove(&k);
            })
        }
    }

    impl From<SendableChangeRequest> for ChangeRequest<'static, String, usize> {
        fn from(request: SendableChangeRequest) -> Self {
            Self::from_fn(request.fun)
        }
    }
}
