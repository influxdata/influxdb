//! Policy framework for [backends](crate::backend::CacheBackend).

use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::Debug,
    hash::Hash,
    sync::{Arc, Weak},
};

use parking_lot::ReentrantMutex;

use super::CacheBackend;

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
///
/// # Policies & Recursion
/// Policies have two tasks:
///
/// - initiate changes (e.g. based on timers)
/// - react to changes
///
/// Getting data from a [`PolicyBackend`] and feeding data back into it in a somewhat synchronous manner sounds really
/// close to recursion. Uncontrolled recursion however is bad for the following reasons:
///
/// 1. **Stack space:** We may easily run out of stack space.
/// 2. **Ownership:** Looping back into the same data structure can easily lead to deadlocks (data corruption is luckily
///    prevented by Rust's ownership model).
///
/// However sometimes we need to have interactions of policies in a "recursive" manner. E.g.:
///
/// 1. A refresh policies updates a value based on a timer. The value gets bigger.
/// 2. Some resource-pool policy decides that this is now too much data and wants to evict data.
/// 3. The refresh policy gets informed about the values that are removed so it can stop refreshing them.
///
/// The solution that [`PolicyBackend`] uses is the following:
///
/// - Initial requests (either internal ones or the ones created by policies) can use a normal [`CacheBackend`]
///   interface but shall not hold any locks on internal data structures of the policies. The virtual backend that is
///   used between [`PolicyBackend`] and the policies is called "callback backend".
/// - Policies react to changes NOT by using the "callback store" but by returning the changes they want to perform,
///   dropping all the locks on their internal data structures.
///
/// We cannot guarantee that policies fulfill this interface, but [`PolicyBackend`] performs some sanity checks (e.g. it
/// will catch if the same thread that started an initial requests recurses into another initial request).
///
///
/// # Change Propagation
/// Changes will be propated "breadth first". This means that the initial change will form a 1-element task list. For
/// every task in this list (front to back), we propagate the change as following:
///
/// 1. underlying backend
/// 2. policies (in the order they where added)
///
/// From step 2 we collect new change requests that will added to the back of the task list.
///
/// The original requests will return to the caller once all tasks are completed.
///
///
/// # `GET`
/// The return value for [`CacheBackend::get`] is fetched from the inner backend AFTER all changes are applied.
///
/// Note [`ChangeRequest::Get`] has no way of returning a result to the [`Subscriber`] that created it. The "changes"
/// solely act as some kind of "keep alive" / "this was used" signal.
///
///
/// # Example
/// **The policies in these examples are deliberately silly but simple!**
///
/// Let's start with a purely reactive policy that will round up all integer values to the next even number:
///
/// ```
/// use std::collections::HashMap;
/// use cache_system::backend::{
///     CacheBackend,
///     policy::{
///         ChangeRequest,
///         PolicyBackend,
///         Subscriber,
///     },
/// };
///
/// #[derive(Debug)]
/// struct EvenNumberPolicy;
///
/// type CR = ChangeRequest<&'static str, u64>;
///
/// impl Subscriber for EvenNumberPolicy {
///     type K = &'static str;
///     type V = u64;
///
///     fn set(&mut self, k: &'static str, v: u64) -> Vec<CR> {
///       // When new key `k` is set to value `v` if `v` is odd,
///       // request a change to set `k` to `v+1`
///         if v % 2 == 1 {
///             vec![CR::Set{k, v: v + 1}]
///         } else {
///             vec![]
///         }
///     }
/// }
///
/// let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
///
/// #[derive(Debug)]
/// struct NowPolicy {
///     cancel: Arc<AtomicBool>,
///     handle: JoinHandle<()>,
/// };
///
/// impl Drop for NowPolicy {
///     fn drop(&mut self) {
///         self.cancel.store(true, Ordering::SeqCst);
///     }
/// }
///
/// type CR = ChangeRequest<&'static str, Instant>;
///
/// impl Subscriber for NowPolicy {
///     type K = &'static str;
///     type V = Instant;
/// }
///
/// let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
/// backend.add_policy(|mut callback_backend| {
///     let cancel = Arc::new(AtomicBool::new(false));
///     let cancel_captured = Arc::clone(&cancel);
///     let handle = spawn(move || {
///         loop {
///             if cancel_captured.load(Ordering::SeqCst) {
///                 break;
///             }
///             callback_backend.set("now", Instant::now());
///             sleep(Duration::from_millis(1));
///         }
///     });
///     NowPolicy{cancel, handle}
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
    /// Panics if `inner` is not empty.
    pub fn new(inner: Box<dyn CacheBackend<K = K, V = V>>) -> Self {
        assert!(inner.is_empty(), "inner backend is not empty");

        Self {
            inner: Arc::new(ReentrantMutex::new(RefCell::new(PolicyBackendInner {
                inner,
                subscribers: Vec::new(),
            }))),
        }
    }

    /// Adds new policy.
    ///
    /// See documentation of [`PolicyBackend`] for more information.
    ///
    /// This is called with a function that receives the "callback backend" to this backend and should return a
    /// [`Subscriber`]. This loopy construct was chosen to discourage the leakage of the "callback backend" to any other object.
    pub fn add_policy<C, S>(&mut self, constructor: C)
    where
        C: FnOnce(Box<dyn CacheBackend<K = K, V = V>>) -> S,
        S: Subscriber<K = K, V = V>,
    {
        let backend_for_policy = BackendForPolicy {
            inner: Arc::downgrade(&self.inner),
        };
        let backend_for_policy = Box::new(backend_for_policy);
        let subscriber = constructor(backend_for_policy);
        lock_inner!(mut guard = self.inner);
        guard.subscribers.push(Box::new(subscriber));
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
        perform_changes(&mut guard, vec![ChangeRequest::Get { k: k.clone() }]);

        // poll inner backend AFTER everything has settled
        guard.inner.get(k)
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        lock_inner!(mut guard = self.inner);
        perform_changes(&mut guard, vec![ChangeRequest::Set { k, v }]);
    }

    fn remove(&mut self, k: &Self::K) {
        lock_inner!(mut guard = self.inner);
        perform_changes(&mut guard, vec![ChangeRequest::Remove { k: k.clone() }]);
    }

    fn is_empty(&self) -> bool {
        lock_inner!(guard = self.inner);
        guard.inner.is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Same as [`PolicyBackend`] but with a weak pointer to inner to avoid cyclic references.
#[derive(Debug)]
struct BackendForPolicy<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner: WeakSharedInner<K, V>,
}

impl<K, V> CacheBackend for BackendForPolicy<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        let inner = self.inner.upgrade().expect("backend gone");
        lock_inner!(mut guard = inner);
        perform_changes(&mut guard, vec![ChangeRequest::Get { k: k.clone() }]);

        // poll inner backend AFTER everything has settled
        guard.inner.get(k)
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        let inner = self.inner.upgrade().expect("backend gone");
        lock_inner!(mut guard = inner);
        perform_changes(&mut guard, vec![ChangeRequest::Set { k, v }]);
    }

    fn remove(&mut self, k: &Self::K) {
        let inner = self.inner.upgrade().expect("backend gone");
        lock_inner!(mut guard = inner);
        perform_changes(&mut guard, vec![ChangeRequest::Remove { k: k.clone() }]);
    }

    fn is_empty(&self) -> bool {
        let inner = self.inner.upgrade().expect("backend gone");
        lock_inner!(guard = inner);
        guard.inner.is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
struct PolicyBackendInner<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    inner: Box<dyn CacheBackend<K = K, V = V>>,
    subscribers: Vec<Box<dyn Subscriber<K = K, V = V>>>,
}

type WeakSharedInner<K, V> = Weak<ReentrantMutex<RefCell<PolicyBackendInner<K, V>>>>;
type StrongSharedInner<K, V> = Arc<ReentrantMutex<RefCell<PolicyBackendInner<K, V>>>>;

/// Perform changes breadth first.
fn perform_changes<K, V>(
    inner: &mut PolicyBackendInner<K, V>,
    change_requests: Vec<ChangeRequest<K, V>>,
) where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    let mut tasks = VecDeque::from(change_requests);

    while let Some(change_request) = tasks.pop_front() {
        match change_request {
            ChangeRequest::Get { k } => {
                // publish to underlying backend first
                // For most backends this shouldn't make a difference because their "inner life" should be ported to
                // this policy framework. However some may still react to the "keep alive" signal.
                inner.inner.get(&k);

                // publish to subscribers
                for subscriber in &mut inner.subscribers {
                    let requests = subscriber.get(&k);
                    tasks.extend(requests.into_iter());
                }
            }
            ChangeRequest::Set { k, v } => {
                // publish to underlying backend first
                inner.inner.set(k.clone(), v.clone());

                // publish to subscribers
                for subscriber in &mut inner.subscribers {
                    let requests = subscriber.set(k.clone(), v.clone());
                    tasks.extend(requests.into_iter());
                }
            }
            ChangeRequest::Remove { k } => {
                // publish to underlying backend first
                inner.inner.remove(&k);

                // publish to subscribers
                for subscriber in &mut inner.subscribers {
                    let requests = subscriber.remove(&k);
                    tasks.extend(requests.into_iter());
                }
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
    fn get(&mut self, _k: &Self::K) -> Vec<ChangeRequest<Self::K, Self::V>> {
        // do nothing by default
        vec![]
    }

    /// Set value for given key.
    ///
    /// It is OK to set and override a key that already exists.
    fn set(&mut self, _k: Self::K, _v: Self::V) -> Vec<ChangeRequest<Self::K, Self::V>> {
        // do nothing by default
        vec![]
    }

    /// Remove value for given key.
    ///
    /// It is OK to remove a key even when it does not exist.
    fn remove(&mut self, _k: &Self::K) -> Vec<ChangeRequest<Self::K, Self::V>> {
        // do nothing by default
        vec![]
    }
}

/// A change request to a backend.
#[derive(Debug, PartialEq)]
pub enum ChangeRequest<K, V> {
    /// Request to `GET` a value.
    ///
    /// Since the result is NOT propagated back the the [`Subscriber`], this is mostly useful to send "keep alive" requests.
    Get {
        /// Key.
        k: K,
    },

    /// Set value for given key.
    ///
    /// It is OK to set and override a key that already exists.
    Set {
        /// Key.
        k: K,

        /// Value.
        v: V,
    },

    /// Remove value for given key.
    ///
    /// It is OK to remove a key even when it does not exist.
    Remove {
        /// Key.
        k: K,
    },
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Barrier, thread::JoinHandle};

    use super::*;

    #[test]
    #[should_panic(expected = "inner backend is not empty")]
    fn test_panic_inner_not_empty() {
        PolicyBackend::new(Box::new(HashMap::from([(String::from("foo"), 1usize)])));
    }

    #[test]
    fn test_generic() {
        crate::backend::test_util::test_generic(|| PolicyBackend::new(Box::new(HashMap::new())))
    }

    #[test]
    #[should_panic(expected = "test steps left")]
    fn test_meta_panic_steps_left() {
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![]));

        backend.get(&String::from("a"));
    }

    #[test]
    #[should_panic(expected = "step left for set operation")]
    fn test_meta_panic_requires_condition_set() {
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![]));

        backend.set(String::from("a"), 2);
    }

    #[test]
    #[should_panic(expected = "step left for remove operation")]
    fn test_meta_panic_requires_condition_remove() {
        let mut backend = PolicyBackend::<String, usize>::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![]));

        backend.remove(&String::from("a"));
    }

    #[test]
    #[should_panic(expected = "Condition mismatch")]
    fn test_meta_panic_checks_condition_get() {
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
    fn test_basic_get_set() {
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("a"),
                    v: 1,
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Get {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Get {
                    k: String::from("a"),
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("b"),
                    v: 2,
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 1,
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Remove {
                    k: String::from("a"),
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("b"),
                    v: 1,
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Remove {
                    k: String::from("a"),
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Remove {
                    k: String::from("b"),
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![
                    ChangeRequest::Set {
                        k: String::from("a"),
                        v: 12,
                    },
                    ChangeRequest::Set {
                        k: String::from("a"),
                        v: 13,
                    },
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("a"),
                    v: 12,
                }]),
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
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("a"),
                    v: 13,
                }]),
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
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
        backend.add_policy(create_test_policy(vec![
            TestStep {
                condition: TestBackendInteraction::Set {
                    k: String::from("a"),
                    v: 11,
                },
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("a"),
                    v: 12,
                }]),
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
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("a"),
                    v: 14,
                }]),
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
                action: TestAction::ChangeRequests(vec![ChangeRequest::Set {
                    k: String::from("a"),
                    v: 13,
                }]),
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

        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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

    /// Checks that a policy background task can access the "callback backend" without triggering the "illegal
    /// recursion" detection.
    #[test]
    fn test_multithread() {
        let barrier_pre = Arc::new(Barrier::new(2));
        let barrier_post = Arc::new(Barrier::new(2));

        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
                    vec![ChangeRequest::Set {
                        k: String::from("a"),
                        v: 3,
                    }],
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

        let mut backend = PolicyBackend::new(Box::new(HashMap::new()));
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
        let mut backend = PolicyBackend::new(Box::new(DropTester(Arc::clone(&marker_backend), ())));

        let marker_policy = Arc::new(());
        backend.add_policy(|callback| DropTester(Arc::clone(&marker_policy), callback));

        assert_eq!(Arc::strong_count(&marker_backend), 2);
        assert_eq!(Arc::strong_count(&marker_policy), 2);

        drop(backend);

        assert_eq!(Arc::strong_count(&marker_backend), 1);
        assert_eq!(Arc::strong_count(&marker_policy), 1);
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
    ) -> impl FnOnce(Box<dyn CacheBackend<K = String, V = usize>>) -> TestSubscriber {
        |backend| TestSubscriber {
            background_task: TestSubscriberBackgroundTask::NotStarted(backend),
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

    #[derive(Debug)]
    enum TestAction {
        /// Perform an illegal direct, recursive call to the backend.
        CallBackendDirectly(TestBackendInteraction),

        /// Return change requests
        ChangeRequests(Vec<ChangeRequest<String, usize>>),

        /// Use callback backend but wait for a barrier in a background thread.
        ///
        /// This will return immediately.
        CallBackendDelayed(Arc<Barrier>, TestBackendInteraction, Arc<Barrier>),

        /// Block on barrier and return afterwards.
        BlockAndChangeRequest(Arc<Barrier>, Vec<ChangeRequest<String, usize>>),
    }

    impl TestAction {
        fn perform(
            self,
            background_task: &mut TestSubscriberBackgroundTask,
        ) -> Vec<ChangeRequest<String, usize>> {
            match self {
                Self::CallBackendDirectly(interaction) => {
                    let backend = match background_task {
                        TestSubscriberBackgroundTask::NotStarted(backend) => backend,
                        TestSubscriberBackgroundTask::Started(_) => {
                            panic!("background task already started")
                        }
                        TestSubscriberBackgroundTask::Invalid => panic!("Invalid state"),
                    };

                    match interaction {
                        TestBackendInteraction::Get { k } => {
                            backend.get(&k);
                        }
                        TestBackendInteraction::Set { k, v } => backend.set(k, v),
                        TestBackendInteraction::Remove { k } => backend.remove(&k),
                        TestBackendInteraction::Panic => panic!("this is a test"),
                    }

                    unreachable!("illegal call should have failed")
                }
                Self::ChangeRequests(change_requests) => change_requests,
                Self::CallBackendDelayed(barrier_pre, interaction, barrier_post) => {
                    let mut tmp = TestSubscriberBackgroundTask::Invalid;
                    std::mem::swap(&mut tmp, background_task);
                    let mut backend = match tmp {
                        TestSubscriberBackgroundTask::NotStarted(backend) => backend,
                        TestSubscriberBackgroundTask::Started(_) => {
                            panic!("background task already started")
                        }
                        TestSubscriberBackgroundTask::Invalid => panic!("Invalid state"),
                    };

                    let handle = std::thread::spawn(move || {
                        barrier_pre.wait();

                        match interaction {
                            TestBackendInteraction::Get { k } => {
                                backend.get(&k);
                            }
                            TestBackendInteraction::Set { k, v } => {
                                backend.set(k, v);
                            }
                            TestBackendInteraction::Remove { k } => {
                                backend.remove(&k);
                            }
                            TestBackendInteraction::Panic => panic!("this is a test"),
                        }

                        barrier_post.wait();
                    });
                    *background_task = TestSubscriberBackgroundTask::Started(handle);

                    vec![]
                }
                Self::BlockAndChangeRequest(barrier, change_requests) => {
                    barrier.wait();
                    change_requests
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
        NotStarted(Box<dyn CacheBackend<K = String, V = usize>>),
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

        fn get(&mut self, k: &Self::K) -> Vec<ChangeRequest<Self::K, Self::V>> {
            let step = self.steps.pop_front().expect("step left for get operation");

            let expected_condition = TestBackendInteraction::Get { k: k.clone() };
            assert_eq!(
                step.condition, expected_condition,
                "Condition mismatch\n\nActual:\n{:#?}\n\nExpected:\n{:#?}",
                step.condition, expected_condition,
            );

            step.action.perform(&mut self.background_task)
        }

        fn set(&mut self, k: Self::K, v: Self::V) -> Vec<ChangeRequest<String, usize>> {
            let step = self.steps.pop_front().expect("step left for set operation");

            let expected_condition = TestBackendInteraction::Set { k, v };
            assert_eq!(
                step.condition, expected_condition,
                "Condition mismatch\n\nActual:\n{:#?}\n\nExpected:\n{:#?}",
                step.condition, expected_condition,
            );

            step.action.perform(&mut self.background_task)
        }

        fn remove(&mut self, k: &Self::K) -> Vec<ChangeRequest<String, usize>> {
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
}
