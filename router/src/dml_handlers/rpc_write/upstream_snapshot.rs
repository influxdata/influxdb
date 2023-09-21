use std::{ops::Deref, sync::Arc};

use parking_lot::Mutex;
use smallvec::SmallVec;

/// Possible states of a single client `C` in an [`UpstreamSnapshot`].
///
/// ```text
///                         ┌────────────────┐
///                      ┌─▶│   Available    │
///                      │  └────────────────┘
///                      │           │
///                    drop       next()
///                      │           │
///                      │           ▼
///                      │  ┌────────────────┐
///                      └──│    Yielded     │
///                         └────────────────┘
///                                  │
///                               remove
///                                  │
///                                  ▼
///                         ┌────────────────┐
///                         │      Used      │
///                         └────────────────┘
/// ```
///
/// When the [`UpstreamSnapshot`] is initialised, all `C` are in the
/// [`UpstreamState::Available`] state. Once a given `C` is lent out, its slot
/// in the [`UpstreamSnapshot`] is replaced with [`UpstreamState::Yielded`],
/// indicating it is lent out, and dropping the reference to `C` (making it
/// impossible to lend out again!).
///
/// Once the caller drops the `C` they were yielded, the state returns to
/// [`UpstreamState::Available`] to be lent out again.
///
/// Once a `C` is removed from the snapshot by calling
/// [`UpstreamSnapshot::remove()`], the slot is transitioned to
/// [`UpstreamState::Used`] to indicate it cannot be reused.
///
#[derive(Debug)]
enum UpstreamState<C> {
    /// The given instance of `C` has not been returned to the caller yet, or
    /// has been dropped by the caller without calling
    /// [`UpstreamSnapshot::remove()`] first.
    Available(C),

    /// The instance of `C` is currently lent to the caller.
    ///
    /// The yielded `C` has yet not been dropped, or removed from the
    /// [`UpstreamSnapshot`].
    Yielded,

    /// The given `C` has been "used" and removed by a call to
    /// [`UpstreamSnapshot::remove()`].
    ///
    /// It cannot be returned to the caller again.
    Used,
}

impl<C> UpstreamState<C> {
    fn unwrap(self) -> C {
        match self {
            UpstreamState::Available(v) => v,
            UpstreamState::Used | UpstreamState::Yielded => {
                panic!("unwrap an unavailable upstream state")
            }
        }
    }
}

/// A smart-pointer dereferencing to a reference to `C`.
///
/// The [`UpstreamSnapshot`] ensures that only one [`Upstream`]-wrapped
/// reference to `C` is ever available at any one time. Dropping an instance of
/// [`Upstream`] returns the `C` it contains back to the [`UpstreamSnapshot`] it
/// came from, allowing it to be lent out to another caller.
///
/// To permanently remove this `C` from the [`UpstreamSnapshot`], pass it to
/// [`UpstreamSnapshot::remove()`].
#[derive(Debug)]
pub(super) struct Upstream<C> {
    /// The instance of `C` lent out from the snapshot.
    ///
    /// This option is always [`Some`] until dropped or removed from the
    /// snapshot, at which point it is set to [`None`].
    ///
    /// As an optimisation, do not attempt to acquire the set mutex to check if
    /// this [`Upstream`] has been marked as [`UpstreamState::Used`] before
    /// setting [`UpstreamState::Available`] if this option is [`None`] to
    /// reduce lock contention.
    inner: Option<C>,

    /// The set of clients from which this `C` has been borrowed.
    state: Arc<Mutex<SharedState<C>>>,

    /// The index into `set` at which this `C` can be found.
    idx: usize,
}

impl<C> Deref for Upstream<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl<C> Drop for Upstream<C> {
    fn drop(&mut self) {
        let inner = match self.inner.take() {
            Some(v) => v,
            None => return,
        };
        *self.state.lock().clients.get_mut(self.idx).unwrap() = UpstreamState::Available(inner);
    }
}

/// Mutable state shared between clones of a single [`UpstreamSnapshot`].
#[derive(Debug)]
struct SharedState<C> {
    /// The set of `C` for this [`UpstreamSnapshot`].
    clients: SmallVec<[UpstreamState<C>; 3]>,

    /// The current cursor index for this snapshot instance.
    idx: usize,
}

impl<C> SharedState<C> {
    #[inline(always)]
    fn current_idx(&self) -> usize {
        self.idx % self.clients.len()
    }
}

/// An infinite cycling iterator, yielding the 0-indexed `i`-th element first
/// (modulo wrapping).
///
/// The [`UpstreamSnapshot`] contains a set of `C`, maintaining an invariant
/// that writes to `C` do not happen concurrently, by yielding each `C` wrapped
/// in an [`Upstream`] to exactly one caller at a time.
///
/// Combined with the ability to remove a `C` from the set returned by the
/// [`UpstreamSnapshot`], the caller can ensure that once a write has been
/// successfully accepted by `C`, no further write attempts are made to it.
///
/// Cloning this [`UpstreamSnapshot`] allows it to be shared across thread /
/// task boundaries, while internally referencing the same set of `C` and
/// co-ordinating the state of each across each cloned [`UpstreamSnapshot`].
///
/// This allows concurrent replication of writes to N ingesters synchronise
/// using clones of this [`UpstreamSnapshot`], causing each write to land on a
/// distinct ingester.
///
/// If all `C` are currently lent out, this iterator yields [`None`]. If a `C`
/// is then returned, then the iterator will return [`Some`] at the next poll.
#[derive(Debug, Clone)]
pub(super) struct UpstreamSnapshot<C> {
    /// The mutable state shared between each cloned copy of this
    /// [`UpstreamSnapshot`] instance.
    state: Arc<Mutex<SharedState<C>>>,

    /// The length of `state.clients` to avoid locking to read this static
    /// value.
    len: usize,

    contains_probe: bool,
}

impl<C> UpstreamSnapshot<C> {
    /// Initialise a new snapshot, yielding the 0-indexed `i`-th element of
    /// `clients` next (or wrapping around if `i` is out-of-bounds).
    ///
    /// If one or more clients are included to perform a health probe,
    /// `contains_probe` should be true.
    ///
    /// Holds up to 3 elements on the stack; more than 3 elements will cause an
    /// allocation during construction.
    ///
    /// If `clients` is empty, this method returns [`None`].
    pub(super) fn new(
        clients: impl Iterator<Item = C>,
        i: usize,
        contains_probe: bool,
    ) -> Option<Self> {
        let clients: SmallVec<[UpstreamState<C>; 3]> =
            clients.map(UpstreamState::Available).collect();
        if clients.is_empty() {
            return None;
        }
        Some(Self {
            len: clients.len(),
            contains_probe,
            state: Arc::new(Mutex::new(SharedState {
                // So first call is the ith element even after the inc in next().
                idx: i.wrapping_sub(1),
                clients,
            })),
        })
    }

    /// Consume the given `upstream` from the [`UpstreamSnapshot`], taking
    /// ownership of it from the caller, and preventing it from being yielded
    /// again.
    ///
    /// # Panics
    ///
    /// Panics if `upstream` was not obtained from this [`UpstreamSnapshot`].
    pub(super) fn remove(&self, mut upstream: Upstream<C>) {
        // Ensure the `upstream` was yielded from this set.
        assert!(
            Arc::ptr_eq(&self.state, &upstream.state),
            "remove from disjoint sets"
        );

        let old = std::mem::replace(
            &mut self.state.lock().clients[upstream.idx],
            UpstreamState::Used,
        );

        // Invariant: any upstream being removed must have been yielded by the
        // iterator - the type system should enforce this as the Upstream does
        // not implement Clone, and this fn took ownership.
        assert!(matches!(old, UpstreamState::Yielded));

        // Prevent the drop impl from setting the state to "available" again.
        upstream.inner = None;
        drop(upstream); // explicit drop for clarity w.r.t the above
    }

    /// Returns the number of clients in this [`UpstreamSnapshot`] when
    /// constructed.
    ///
    /// If [`UpstreamSnapshot::remove()`] has been called since construction,
    /// this iterator will yield fewer distinct `C` than this returned number.
    pub(super) fn initial_len(&self) -> usize {
        self.len
    }

    /// Returns `true` if this [`UpstreamSnapshot`] was initialised with a
    /// client selected for a health probe request.
    pub(super) fn contains_probe(&self) -> bool {
        self.contains_probe
    }
}

impl<C> Iterator for UpstreamSnapshot<C> {
    type Item = Upstream<C>;

    fn next(&mut self) -> Option<Self::Item> {
        // Obtain the client set mutex outside the loop as the overhead of
        // acquiring the contended mutex is likely to outweigh the actual loop
        // critical section cost - it's better to not yield control until an
        // element has been found in the (fast) linear search to avoid
        // contention.
        let mut guard = self.state.lock();

        // Remember where in the client array this first attempt was.
        let start_idx = guard.current_idx();

        loop {
            // Move along the client array.
            guard.idx = guard.idx.wrapping_add(1);

            // Find the array index of this next client.
            let current_idx = guard.current_idx();

            // If this C is available, mark it as lent out and yield it to the
            // caller.
            let v = guard.clients.get_mut(current_idx).unwrap();
            if matches!(v, UpstreamState::Available(_)) {
                let got = std::mem::replace(v, UpstreamState::Yielded).unwrap();
                return Some(Upstream {
                    inner: Some(got),
                    idx: current_idx,
                    state: Arc::clone(&self.state),
                });
            }

            // Otherwise ensure the loop doesn't continue forever.
            //
            // Once all the elements have been visited once, the loop should
            // end. If there's no available upstream now, then this request will
            // never be satisfiable; another thread holds another upstream may
            // return it to the pool for this thread to acquire, but the other
            // thread would then fail to find a free upstream.
            if current_idx == start_idx {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        iter,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use proptest::proptest;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn test_len() {
        let elements = [
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ];

        let snap = UpstreamSnapshot::new(elements.iter(), 0, false)
            .expect("non-empty element set should yield snapshot");

        assert_eq!(snap.initial_len(), 3);

        let (min, max) = snap.size_hint();
        assert_eq!(min, 0);
        assert_eq!(max, None);
    }

    #[test]
    fn test_start_index() {
        let elements = [1, 2, 3];

        assert_eq!(
            **UpstreamSnapshot::new(elements.iter(), 0, false)
                .expect("non-empty element set should yield snapshot")
                .next()
                .expect("should yield value"),
            1
        );
        assert_eq!(
            **UpstreamSnapshot::new(elements.iter(), 1, false)
                .expect("non-empty element set should yield snapshot")
                .next()
                .expect("should yield value"),
            2
        );
        assert_eq!(
            **UpstreamSnapshot::new(elements.iter(), 2, false)
                .expect("non-empty element set should yield snapshot")
                .next()
                .expect("should yield value"),
            3
        );

        // Wraparound
        assert_eq!(
            **UpstreamSnapshot::new(elements.iter(), 3, false)
                .expect("non-empty element set should yield snapshot")
                .next()
                .expect("should yield value"),
            1
        );
    }

    #[test]
    fn test_cycles() {
        let elements = [
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ];

        // Create a snapshot and iterate over it twice.
        {
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0, false)
                .expect("non-empty element set should yield snapshot");
            for _ in 0..(elements.len() * 2) {
                snap.next()
                    .expect("should cycle forever")
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Assert all elements were visited twice.
        elements
            .into_iter()
            .for_each(|v| assert_eq!(v.load(Ordering::Relaxed), 2));
    }

    #[test]
    fn test_remove_element() {
        let elements = [1, 2, 3];

        // First element removed
        {
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0, false)
                .expect("non-empty element set should yield snapshot");

            assert_eq!(snap.initial_len(), 3);

            let item = snap.next().unwrap();
            assert_eq!(**item, 1);
            assert_eq!(item.idx, 0);
            snap.remove(item);

            // Removing is stable - it does not permute the item order.
            assert_eq!(snap.next().as_deref(), Some(&&2));
            assert_eq!(snap.next().as_deref(), Some(&&3));
            assert_eq!(snap.next().as_deref(), Some(&&2));
            assert_eq!(snap.next().as_deref(), Some(&&3));
            assert_eq!(snap.initial_len(), 3);
        }

        // Second element removed
        {
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0, false)
                .expect("non-empty element set should yield snapshot");
            assert_eq!(snap.next().as_deref(), Some(&&1));

            assert_eq!(snap.initial_len(), 3);

            let item = snap.next().unwrap();
            assert_eq!(**item, 2);
            assert_eq!(item.idx, 1);
            snap.remove(item);

            assert_eq!(snap.next().as_deref(), Some(&&3));
            assert_eq!(snap.next().as_deref(), Some(&&1));
            assert_eq!(snap.next().as_deref(), Some(&&3));
            assert_eq!(snap.initial_len(), 3);
        }

        // Last element removed
        {
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0, false)
                .expect("non-empty element set should yield snapshot");
            assert_eq!(snap.next().as_deref(), Some(&&1));
            assert_eq!(snap.next().as_deref(), Some(&&2));

            assert_eq!(snap.initial_len(), 3);

            let item = snap.next().unwrap();
            assert_eq!(**item, 3);
            assert_eq!(item.idx, 2);
            snap.remove(item);

            assert_eq!(snap.next().as_deref(), Some(&&1));
            assert_eq!(snap.next().as_deref(), Some(&&2));
            assert_eq!(snap.next().as_deref(), Some(&&1));
            assert_eq!(snap.initial_len(), 3);
        }
    }

    #[test]
    fn test_remove_all_elements() {
        let elements = [42];
        let mut snap = UpstreamSnapshot::new(elements.iter(), 0, false)
            .expect("non-empty element set should yield snapshot");

        assert_eq!(snap.initial_len(), 1);

        let item = snap.next().unwrap();
        snap.remove(item);
        assert!(snap.next().is_none());
        assert!(snap.next().is_none());

        assert_eq!(snap.initial_len(), 1);
    }

    #[test]
    fn test_empty_snap() {
        assert!(UpstreamSnapshot::<usize>::new(iter::empty(), 0, false).is_none());
        assert!(UpstreamSnapshot::<usize>::new(iter::empty(), 1, false).is_none());
    }

    #[test]
    #[should_panic(expected = "remove from disjoint sets")]
    fn test_upstream_from_disjoint_sets() {
        let mut set_a = UpstreamSnapshot::new([1].iter(), 0, false).unwrap();
        let set_b = UpstreamSnapshot::new([1].iter(), 0, false).unwrap();

        let item = set_a.next().unwrap();
        set_b.remove(item); // Oops - removing a from b!
    }

    #[test]
    fn test_contains_probe() {
        assert!(UpstreamSnapshot::new([1].iter(), 0, true)
            .unwrap()
            .contains_probe());
        assert!(!UpstreamSnapshot::new([1].iter(), 0, false)
            .unwrap()
            .contains_probe());
    }

    proptest! {
        /// Assert the set always cycles indefinitely, visiting all elements
        /// equally often (when the number of visits is a multiple of the set
        /// size).
        ///
        /// Ensure the starting offset does not affect this property.
        #[test]
        fn prop_upstream_set_cycles(
            complete_iters in (1_usize..5),
            set_size in (1_usize..5),
            offset in (1_usize..10),
        ) {
            let elements = (0..set_size).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();

            // Create a snapshot and iterate over it the specified number of
            // times.
            {
                let mut snap = UpstreamSnapshot::new(elements.iter(), offset, false)
                    .expect("non-empty element set should yield snapshot");

                for _ in 0..(elements.len() * complete_iters) {
                    snap.next()
                        .expect("should cycle forever")
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            // Assert all elements were visited exactly complete_iters number of
            // times.
            elements
                .into_iter()
                .for_each(|v| assert_eq!(v.load(Ordering::Relaxed), complete_iters));
        }

        /// Assert the set yields any item exactly once at any one time.
        #[test]
        fn prop_upstream_yield_exactly_once(
            complete_iters in (2_usize..5),
            set_size in (1_usize..5),
            offset in (1_usize..10),
            hold_idx in (0_usize..100),
        ) {
            let elements = (0..set_size).map(|_| Arc::new(AtomicUsize::new(0))).collect::<Vec<_>>();

            // Create a snapshot and iterate over it the specified number of
            // times.
            {
                let mut snap = UpstreamSnapshot::new(elements.iter(), offset, false)
                    .expect("non-empty element set should yield snapshot");

                // Take the specified index out of the set and hold onto it.
                let hold = snap.clone().nth(hold_idx).unwrap();

                // Now iterate over the snapshot and increment the counter of
                // each yielded upstream.
                //
                // Iterate exactly N times over M-1 elements remaining in the
                // set.
                let count = (elements.len() - 1) * complete_iters;
                for _ in 0..count {
                    snap.next()
                        .expect("should cycle forever")
                        .fetch_add(1, Ordering::Relaxed);
                }

                // Nothing incremented the element we were holding onto.
                assert_eq!(hold.load(Ordering::Relaxed), 0);

                // Store the expected count so there's a simple check below that
                // all the non-0 elements have the same expected count value.
                hold.store(complete_iters, Ordering::Relaxed);
            }

            // Assert all elements were visited exactly complete_iters number of
            // times.
            elements
                .into_iter()
                .for_each(|v| assert_eq!(v.load(Ordering::Relaxed), complete_iters));
        }
    }

    /// Ensure two concurrent callers obtain two different elements.
    #[tokio::test]
    async fn test_concurrent_callers_disjoint_elements() {
        let elements = (0..2).collect::<Vec<_>>();

        // Create a snapshot and iterate over it the specified number of
        // times.
        let snap = UpstreamSnapshot::<_>::new(elements.clone().into_iter(), 0, false)
            .expect("non-empty element set should yield snapshot");

        let (tx, mut rx) = mpsc::channel(2);

        tokio::spawn({
            let mut snap = snap.clone();
            let tx = tx.clone();
            async move {
                let got = snap.next().unwrap();
                tx.send(*got).await.unwrap();
            }
        });

        tokio::spawn({
            let mut snap = snap.clone();
            let tx = tx.clone();
            async move {
                let got = snap.next().unwrap();
                tx.send(*got).await.unwrap();
            }
        });

        let a = rx.recv().await.unwrap();
        let b = rx.recv().await.unwrap();

        assert!((a == 0) ^ (b == 0));
        assert!((a == 1) ^ (b == 1));
    }

    /// When N concurrent callers attempt to obtain one of N-1 elements, exactly
    /// one thread must observe [`None`].
    #[tokio::test]
    async fn test_all_yielded() {
        const N: usize = 3;

        let elements = (0..(N - 1)).collect::<Vec<_>>();

        // Create a snapshot and iterate over it the specified number of
        // times.
        let snap = UpstreamSnapshot::<_>::new(elements.clone().into_iter(), 0, false)
            .expect("non-empty element set should yield snapshot");

        let (tx, mut rx) = mpsc::channel(N);

        // One more thread than elements
        for _ in 0..N {
            tokio::spawn({
                let mut snap = snap.clone();
                let tx = tx.clone();
                async move {
                    let got = snap.next();
                    tx.send(got.as_ref().map(|v| **v)).await.unwrap();

                    // Do not "drop" the Upstream wrapper, in effect holding
                    // onto the item forever, ensuring this thread doesn't
                    // return the item to the snapshot for another thread to
                    // grab.
                    std::mem::forget(got);
                }
            });
        }

        let mut saw_nones = 0;
        for _ in 0..N {
            let v = rx
                .recv()
                .with_timeout_panic(Duration::from_secs(5))
                .await
                .expect("exactly N channel writes should occur");
            if v.is_none() {
                saw_nones += 1;
            }
        }

        assert_eq!(saw_nones, 1);
    }
}
