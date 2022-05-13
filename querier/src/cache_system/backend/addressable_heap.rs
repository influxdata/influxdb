use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

/// Addressable heap.
///
/// Stores a value `V` together with a key `K` and an order `O`. Elements are sorted by `O` and the smallest element can
/// be peeked/popped. At the same time elements can be addressed via `K`.
///
/// Note that `K` requires [`Ord`] to implement the inner data structure as a tie breaker.
/// structure.
#[derive(Debug, Clone)]
pub struct AddressableHeap<K, V, O>
where
    K: Clone + Eq + Hash + Ord,
    O: Clone + Ord,
{
    /// Key to order and value.
    ///
    /// The order is required to lookup data within the queue.
    ///
    /// The value is stored here instead of the queue since HashMap entries are copied around less often than queue elements.
    key_to_order_and_value: HashMap<K, (V, O)>,

    /// Queue that handles the priorities.
    ///
    /// The order goes first, the key goes second.
    ///
    /// Note: This is not really a heap, but it fullfills the interface that we need.
    queue: VecDeque<(O, K)>,
}

impl<K, V, O> AddressableHeap<K, V, O>
where
    K: Clone + Eq + Hash + Ord,
    O: Clone + Ord,
{
    /// Create new, empty heap.
    pub fn new() -> Self {
        Self {
            key_to_order_and_value: HashMap::new(),
            queue: VecDeque::new(),
        }
    }

    /// Check if the heap is empty.
    pub fn is_empty(&self) -> bool {
        let res1 = self.key_to_order_and_value.is_empty();
        let res2 = self.queue.is_empty();
        assert_eq!(res1, res2, "data structures out of sync");
        res1
    }

    /// Insert element.
    ///
    /// If the element (compared by `K`) already exists, it will be returned.
    pub fn insert(&mut self, k: K, v: V, o: O) -> Option<(V, O)> {
        // always remove the entry first so we have a clean queue
        let result = self.remove(&k);

        assert!(
            self.key_to_order_and_value
                .insert(k.clone(), (v, o.clone()))
                .is_none(),
            "entry should have been removed by now"
        );

        match self.queue.binary_search_by_key(&(&o, &k), project_tuple) {
            Ok(_) => unreachable!("entry should have been removed by now"),
            Err(index) => {
                self.queue.insert(index, (o, k));
            }
        }

        result
    }

    /// Peek first element (by smallest `O`).
    pub fn peek(&self) -> Option<(&K, &V, &O)> {
        if let Some((o, k)) = self.queue.front() {
            let (v, o2) = self
                .key_to_order_and_value
                .get(k)
                .expect("value is in queue");
            assert!(o == o2);
            Some((k, v, o))
        } else {
            None
        }
    }

    /// Pop first element (by smallest `O`) from heap.
    pub fn pop(&mut self) -> Option<(K, V, O)> {
        if let Some((o, k)) = self.queue.pop_front() {
            let (v, o2) = self
                .key_to_order_and_value
                .remove(&k)
                .expect("value is in queue");
            assert!(o == o2);
            Some((k, v, o))
        } else {
            None
        }
    }

    /// Get element by key.
    pub fn get(&self, k: &K) -> Option<(&V, &O)> {
        self.key_to_order_and_value.get(k).map(project_tuple)
    }

    /// Remove element by key.
    ///
    /// If the element exists within the heap (addressed via `K`), the value and order will be returned.
    pub fn remove(&mut self, k: &K) -> Option<(V, O)> {
        if let Some((v, o)) = self.key_to_order_and_value.remove(k) {
            let index = self
                .queue
                .binary_search_by_key(&(&o, k), project_tuple)
                .expect("key was in key_to_order");
            self.queue.remove(index);
            Some((v, o))
        } else {
            None
        }
    }
}

impl<K, V, O> Default for AddressableHeap<K, V, O>
where
    K: Clone + Eq + Hash + Ord,
    O: Clone + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Project tuple references.
fn project_tuple<A, B>(t: &(A, B)) -> (&A, &B) {
    (&t.0, &t.1)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_peek_empty() {
        let heap = AddressableHeap::<i32, &str, i32>::new();

        assert_eq!(heap.peek(), None);
    }

    #[test]
    fn test_peek_some() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(3, "c", 5);

        assert_eq!(heap.peek(), Some((&2, &"b", &3)));
    }

    #[test]
    fn test_peek_tie() {
        let mut heap = AddressableHeap::new();

        heap.insert(3, "a", 1);
        heap.insert(1, "b", 1);
        heap.insert(2, "c", 1);

        assert_eq!(heap.peek(), Some((&1, &"b", &1)));
    }

    #[test]
    fn test_peek_after_remove() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(3, "c", 5);

        assert_eq!(heap.peek(), Some((&2, &"b", &3)));
        heap.remove(&3);
        assert_eq!(heap.peek(), Some((&2, &"b", &3)));
        heap.remove(&2);
        assert_eq!(heap.peek(), Some((&1, &"a", &4)));
        heap.remove(&1);
        assert_eq!(heap.peek(), None);
    }

    #[test]
    fn test_peek_after_override() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(1, "c", 2);

        assert_eq!(heap.peek(), Some((&1, &"c", &2)));
    }

    #[test]
    fn test_pop_empty() {
        let mut heap = AddressableHeap::<i32, &str, i32>::new();

        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_pop_all() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(3, "c", 5);

        assert_eq!(heap.pop(), Some((2, "b", 3)));
        assert_eq!(heap.pop(), Some((1, "a", 4)));
        assert_eq!(heap.pop(), Some((3, "c", 5)));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_pop_tie() {
        let mut heap = AddressableHeap::new();

        heap.insert(3, "a", 1);
        heap.insert(1, "b", 1);
        heap.insert(2, "c", 1);

        assert_eq!(heap.pop(), Some((1, "b", 1)));
        assert_eq!(heap.pop(), Some((2, "c", 1)));
        assert_eq!(heap.pop(), Some((3, "a", 1)));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_pop_after_insert() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(3, "c", 5);

        assert_eq!(heap.pop(), Some((2, "b", 3)));

        heap.insert(4, "d", 2);
        assert_eq!(heap.pop(), Some((4, "d", 2)));
        assert_eq!(heap.pop(), Some((1, "a", 4)));
    }

    #[test]
    fn test_pop_after_remove() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(3, "c", 5);

        heap.remove(&2);
        assert_eq!(heap.pop(), Some((1, "a", 4)));
    }

    #[test]
    fn test_pop_after_override() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);
        heap.insert(1, "c", 2);

        assert_eq!(heap.pop(), Some((1, "c", 2)));
        assert_eq!(heap.pop(), Some((2, "b", 3)));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_get_empty() {
        let heap = AddressableHeap::<i32, &str, i32>::new();

        assert_eq!(heap.get(&1), None);
    }

    #[test]
    fn test_get_multiple() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);

        assert_eq!(heap.get(&1), Some((&"a", &4)));
        assert_eq!(heap.get(&2), Some((&"b", &3)));
    }

    #[test]
    fn test_get_after_remove() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);

        heap.remove(&1);

        assert_eq!(heap.get(&1), None);
        assert_eq!(heap.get(&2), Some((&"b", &3)));
    }

    #[test]
    fn test_get_after_pop() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);

        heap.pop();

        assert_eq!(heap.get(&1), Some((&"a", &4)));
        assert_eq!(heap.get(&2), None);
    }

    #[test]
    fn test_get_after_override() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(1, "b", 3);

        assert_eq!(heap.get(&1), Some((&"b", &3)));
    }

    #[test]
    fn test_remove_empty() {
        let mut heap = AddressableHeap::<i32, &str, i32>::new();

        assert_eq!(heap.remove(&1), None);
    }

    #[test]
    fn test_remove_some() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);

        assert_eq!(heap.remove(&1), Some(("a", 4)));
        assert_eq!(heap.remove(&2), Some(("b", 3)));
    }

    #[test]
    fn test_remove_twice() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);

        assert_eq!(heap.remove(&1), Some(("a", 4)));
        assert_eq!(heap.remove(&1), None);
    }

    #[test]
    fn test_remove_after_pop() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(2, "b", 3);

        heap.pop();

        assert_eq!(heap.remove(&1), Some(("a", 4)));
        assert_eq!(heap.remove(&2), None);
    }

    #[test]
    fn test_remove_after_override() {
        let mut heap = AddressableHeap::new();

        heap.insert(1, "a", 4);
        heap.insert(1, "b", 3);

        assert_eq!(heap.remove(&1), Some(("b", 3)));
        assert_eq!(heap.remove(&1), None);
    }

    #[test]
    fn test_override() {
        let mut heap = AddressableHeap::new();

        assert_eq!(heap.insert(1, "a", 4), None);
        assert_eq!(heap.insert(2, "b", 3), None);
        assert_eq!(heap.insert(1, "c", 5), Some(("a", 4)));
    }

    /// Simple version of [`AddressableHeap`] for testing.
    struct SimpleAddressableHeap {
        inner: Vec<(u8, String, i8)>,
    }

    impl SimpleAddressableHeap {
        fn new() -> Self {
            Self { inner: Vec::new() }
        }

        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        fn insert(&mut self, k: u8, v: String, o: i8) -> Option<(String, i8)> {
            let res = self.remove(&k);
            self.inner.push((k, v, o));

            res
        }

        fn peek(&self) -> Option<(&u8, &String, &i8)> {
            self.inner
                .iter()
                .min_by_key(|(k, _v, o)| (o, k))
                .map(|(k, v, o)| (k, v, o))
        }

        fn pop(&mut self) -> Option<(u8, String, i8)> {
            self.inner
                .iter()
                .enumerate()
                .min_by_key(|(_idx, (k, _v, o))| (o, k))
                .map(|(idx, _)| idx)
                .map(|idx| self.inner.remove(idx))
        }

        fn get(&self, k: &u8) -> Option<(&String, &i8)> {
            self.inner
                .iter()
                .find(|(k2, _v, _o)| k2 == k)
                .map(|(_k, v, o)| (v, o))
        }

        fn remove(&mut self, k: &u8) -> Option<(String, i8)> {
            self.inner
                .iter()
                .enumerate()
                .find(|(_idx, (k2, _v, _o))| k2 == k)
                .map(|(idx, _)| idx)
                .map(|idx| {
                    let (_k, v, o) = self.inner.remove(idx);
                    (v, o)
                })
        }
    }

    #[derive(Debug, Clone)]
    enum Action {
        IsEmpty,
        Insert { k: u8, v: String, o: i8 },
        Peek,
        Pop,
        Get { k: u8 },
        Remove { k: u8 },
    }

    // Use a hand-rolled strategy instead of `proptest-derive`, because the latter one is quite a heavy dependency.
    fn action() -> impl Strategy<Value = Action> {
        prop_oneof![
            Just(Action::IsEmpty),
            (any::<u8>(), ".*", any::<i8>()).prop_map(|(k, v, o)| Action::Insert { k, v, o }),
            Just(Action::Peek),
            Just(Action::Pop),
            any::<u8>().prop_map(|k| Action::Get { k }),
            any::<u8>().prop_map(|k| Action::Remove { k }),
        ]
    }

    proptest! {
        #[test]
        fn test_proptest(actions in prop::collection::vec(action(), 0..100)) {
            let mut heap = AddressableHeap::new();
            let mut sim = SimpleAddressableHeap::new();

            for action in actions {
                match action {
                    Action::IsEmpty => {
                        let res1 = heap.is_empty();
                        let res2 = sim.is_empty();
                        assert_eq!(res1, res2);
                    }
                    Action::Insert{k, v, o} => {
                        let res1 = heap.insert(k, v.clone(), o);
                        let res2 = sim.insert(k, v, o);
                        assert_eq!(res1, res2);
                    }
                    Action::Peek => {
                        let res1 = heap.peek();
                        let res2 = sim.peek();
                        assert_eq!(res1, res2);
                    }
                    Action::Pop => {
                        let res1 = heap.pop();
                        let res2 = sim.pop();
                        assert_eq!(res1, res2);
                    }
                    Action::Get{k} => {
                        let res1 = heap.get(&k);
                        let res2 = sim.get(&k);
                        assert_eq!(res1, res2);
                    }
                    Action::Remove{k} => {
                        let res1 = heap.remove(&k);
                        let res2 = sim.remove(&k);
                        assert_eq!(res1, res2);
                    }
                }
            }
        }
    }
}
