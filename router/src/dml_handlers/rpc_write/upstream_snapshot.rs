use smallvec::SmallVec;

/// An infinite cycling iterator, yielding the 0-indexed `i`-th element first
/// (modulo wrapping).
///
/// The last yielded element can be removed from the iterator by calling
/// [`UpstreamSnapshot::remove_last_unstable()`].
#[derive(Debug)]
pub(super) struct UpstreamSnapshot<'a, C> {
    clients: SmallVec<[&'a C; 3]>,
    idx: usize,
}

impl<'a, C> UpstreamSnapshot<'a, C> {
    /// Initialise a new snapshot, yielding the 0-indexed `i`-th element of
    /// `clients` next (or wrapping around if `i` is out-of-bounds).
    ///
    /// Holds up to 3 elements on the stack; ore than 3 elements will cause an
    /// allocation during construction.
    pub(super) fn new(clients: impl Iterator<Item = &'a C>, i: usize) -> Self {
        Self {
            clients: clients.collect(),
            // So first call is the ith element even after the inc in next().
            idx: i.wrapping_sub(1),
        }
    }

    /// Remove the last yielded upstream from this snapshot.
    ///
    /// # Ordering
    ///
    /// Calling this method MAY change the order of the yielded elements but
    /// MUST maintain equal visit counts across all elements.
    ///
    /// # Correctness
    ///
    /// If called before [`UpstreamSnapshot`] has yielded any elements, this MAY
    /// remove an arbitrary element from the snapshot.
    #[allow(unused)]
    pub(super) fn remove_last_unstable(&mut self) {
        self.clients.swap_remove(self.idx());
        // Try the element now in the idx position next.
        self.idx = self.idx.wrapping_sub(1);
    }

    #[inline(always)]
    fn idx(&self) -> usize {
        self.idx % self.clients.len()
    }
}

impl<'a, C> Iterator for UpstreamSnapshot<'a, C> {
    type Item = &'a C;

    fn next(&mut self) -> Option<Self::Item> {
        if self.clients.is_empty() {
            return None;
        }
        self.idx = self.idx.wrapping_add(1);
        Some(self.clients[self.idx()])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.clients.len()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn test_size_hint() {
        let elements = [
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ];

        let mut snap = UpstreamSnapshot::new(elements.iter(), 0);

        let (min, max) = snap.size_hint();
        assert_eq!(min, 0);
        assert_eq!(max, Some(3));

        snap.remove_last_unstable(); // Arbitrary element removed

        let (min, max) = snap.size_hint();
        assert_eq!(min, 0);
        assert_eq!(max, Some(2));
    }

    #[test]
    fn test_start_index() {
        let elements = [1, 2, 3];

        assert_eq!(
            *UpstreamSnapshot::new(elements.iter(), 0)
                .next()
                .expect("should yield value"),
            1
        );
        assert_eq!(
            *UpstreamSnapshot::new(elements.iter(), 1)
                .next()
                .expect("should yield value"),
            2
        );
        assert_eq!(
            *UpstreamSnapshot::new(elements.iter(), 2)
                .next()
                .expect("should yield value"),
            3
        );

        // Wraparound
        assert_eq!(
            *UpstreamSnapshot::new(elements.iter(), 3)
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
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0);
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
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0);
            assert_eq!(snap.next(), Some(&1));
            snap.remove_last_unstable();
            assert_eq!(snap.next(), Some(&3)); // Not 2 - unstable remove!
            assert_eq!(snap.next(), Some(&2));
            assert_eq!(snap.next(), Some(&3));
        }

        // Second element removed
        {
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0);
            assert_eq!(snap.next(), Some(&1));
            assert_eq!(snap.next(), Some(&2));
            snap.remove_last_unstable();
            assert_eq!(snap.next(), Some(&3));
            assert_eq!(snap.next(), Some(&1));
            assert_eq!(snap.next(), Some(&3));
        }

        // Last element removed
        {
            let mut snap = UpstreamSnapshot::new(elements.iter(), 0);
            assert_eq!(snap.next(), Some(&1));
            assert_eq!(snap.next(), Some(&2));
            assert_eq!(snap.next(), Some(&3));
            snap.remove_last_unstable();
            assert_eq!(snap.next(), Some(&1));
            assert_eq!(snap.next(), Some(&2));
            assert_eq!(snap.next(), Some(&1));
        }
    }

    #[test]
    fn test_remove_last_element() {
        let elements = [42];
        let mut snap = UpstreamSnapshot::new(elements.iter(), 0);
        assert_eq!(snap.next(), Some(&42));
        assert_eq!(snap.next(), Some(&42));
        snap.remove_last_unstable();
        assert_eq!(snap.next(), None);
        assert_eq!(snap.next(), None);
    }
}
