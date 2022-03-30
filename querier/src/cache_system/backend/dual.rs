use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

use parking_lot::Mutex;

use super::CacheBackend;

type SharedBackends<K1, K2, V> = Arc<
    Mutex<(
        Box<dyn CacheBackend<K = K1, V = V>>,
        Box<dyn CacheBackend<K = K2, V = V>>,
    )>,
>;

/// Boxed mapper function.
type BoxedMapper<K1, K2, V> =
    Box<dyn for<'k, 'v> Fn(&'k K1, &'v V) -> Option<K2> + Send + Sync + 'static>;

/// Create two backends that cross-populate [`set`](CacheBackend::set) options each other.
///
/// This can be helpful if data is requested via two keys, e.g. via ID and via name.
///
/// Note that other operations like [`remove`](CacheBackend::remove) are NOT cross-populated.
///
/// Takes two backend and a mapper that can extract the keys for the other backends. If these mappers return `None`, the
/// data will NOT cross-populated. This can be helpful in cases where the other key cannot be extracted, e.g. for
/// "missing" values. Be careful however since this can lead to inconsistent states in both caches.
///
/// # Panic
/// If the two backends are not empty.
pub fn dual_backends<K1, K2, V, F1, F2>(
    backend1: Box<dyn CacheBackend<K = K1, V = V>>,
    mapper1: F1,
    backend2: Box<dyn CacheBackend<K = K2, V = V>>,
    mapper2: F2,
) -> (DualBackend1<K1, K2, V>, DualBackend2<K1, K2, V>)
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
    F1: for<'k, 'v> Fn(&'k K1, &'v V) -> Option<K2> + Send + Sync + 'static,
    F2: for<'k, 'v> Fn(&'k K2, &'v V) -> Option<K1> + Send + Sync + 'static,
{
    assert!(backend1.is_empty(), "backend1 is not empty");
    assert!(backend2.is_empty(), "backend2 is not empty");

    let shared = Arc::new(Mutex::new((backend1, backend2)));
    let dual1 = DualBackend1 {
        shared: Arc::clone(&shared),
        mapper: Box::new(mapper1),
    };
    let dual2 = DualBackend2 {
        shared,
        mapper: Box::new(mapper2),
    };
    (dual1, dual2)
}

/// First backend created by [`dual_backends`].
pub struct DualBackend1<K1, K2, V>
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    shared: SharedBackends<K1, K2, V>,

    /// Mapper that can extract K2 from K1 and V and decides if the other cache should be populated.
    mapper: BoxedMapper<K1, K2, V>,
}

impl<K1, K2, V> Debug for DualBackend1<K1, K2, V>
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualBackend1")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<K1, K2, V> CacheBackend for DualBackend1<K1, K2, V>
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    type K = K1;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        self.shared.lock().0.get(k)
    }

    fn set(&mut self, k1: Self::K, v: Self::V) {
        let mut shared = self.shared.lock();
        if let Some(k2) = (self.mapper)(&k1, &v) {
            shared.1.set(k2, v.clone());
        }
        shared.0.set(k1, v)
    }

    fn remove(&mut self, k: &Self::K) {
        self.shared.lock().0.remove(k)
    }

    fn is_empty(&self) -> bool {
        self.shared.lock().0.is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

/// Second backend created by [`dual_backends`].
pub struct DualBackend2<K1, K2, V>
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    shared: SharedBackends<K1, K2, V>,

    /// Mapper that can extract K1 from K2 and V and decides if the other cache should be populated.
    mapper: BoxedMapper<K2, K1, V>,
}

impl<K1, K2, V> Debug for DualBackend2<K1, K2, V>
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualBackend2")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<K1, K2, V> CacheBackend for DualBackend2<K1, K2, V>
where
    K1: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    K2: Clone + Eq + Hash + std::fmt::Debug + Ord + Send + 'static,
    V: Clone + std::fmt::Debug + Send + 'static,
{
    type K = K2;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        self.shared.lock().1.get(k)
    }

    fn set(&mut self, k2: Self::K, v: Self::V) {
        let mut shared = self.shared.lock();
        if let Some(k1) = (self.mapper)(&k2, &v) {
            shared.0.set(k1, v.clone());
        }
        shared.1.set(k2, v)
    }

    fn remove(&mut self, k: &Self::K) {
        self.shared.lock().1.remove(k)
    }

    fn is_empty(&self) -> bool {
        self.shared.lock().1.is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_crossing() {
        let backend1 = Box::new(HashMap::<u8, String>::new());
        let backend2 = Box::new(HashMap::<i8, String>::new());

        let (mut backend1, mut backend2) = dual_backends(
            backend1,
            |k1, _v| (*k1 < 10).then(|| *k1 as i8),
            backend2,
            |k2, _v| (*k2 < 10).then(|| *k2 as u8),
        );

        // both start empty
        assert_eq!(backend1.get(&1), None);
        assert_eq!(backend2.get(&1), None);
        assert!(backend1.is_empty());
        assert!(backend2.is_empty());

        // populate via first backend, test first backend first
        backend1.set(1, String::from("a"));
        assert_eq!(backend1.get(&1), Some(String::from("a")));
        assert_eq!(backend2.get(&1), Some(String::from("a")));
        assert!(!backend1.is_empty());
        assert!(!backend2.is_empty());

        // populate via second backend, test first backend first
        backend2.set(2, String::from("b"));
        assert_eq!(backend1.get(&2), Some(String::from("b")));
        assert_eq!(backend2.get(&2), Some(String::from("b")));

        // override via second backend, test first backend first
        backend2.set(1, String::from("c"));
        assert_eq!(backend1.get(&1), Some(String::from("c")));
        assert_eq!(backend2.get(&1), Some(String::from("c")));

        // delete from second backend
        backend2.remove(&1);
        assert_eq!(backend1.get(&1), Some(String::from("c")));
        assert_eq!(backend2.get(&1), None);
        assert_eq!(backend1.get(&2), Some(String::from("b")));
        assert_eq!(backend2.get(&2), Some(String::from("b")));

        // some things are NOT cross-populated
        backend1.set(10, String::from("d"));
        backend2.set(11, String::from("e"));
        assert_eq!(backend1.get(&10), Some(String::from("d")));
        assert_eq!(backend2.get(&10), None);
        assert_eq!(backend1.get(&11), None);
        assert_eq!(backend2.get(&11), Some(String::from("e")));

        // Mappers CAN lead to inconsistent states. This is not adviced, but just demonstrates of what would happen in
        // that case.
        backend1.set(11, String::from("f"));
        assert_eq!(backend1.get(&11), Some(String::from("f")));
        assert_eq!(backend2.get(&11), Some(String::from("e")));
    }

    #[test]
    fn test_is_empty_partial1() {
        let backend1 = Box::new(HashMap::<u8, String>::new());
        let backend2 = Box::new(HashMap::<i8, String>::new());

        let (mut backend1, backend2) = dual_backends(
            backend1,
            |k1, _v| (*k1 < 10).then(|| *k1 as i8),
            backend2,
            |k2, _v| (*k2 < 10).then(|| *k2 as u8),
        );

        assert!(backend1.is_empty());
        assert!(backend2.is_empty());

        // populate via first backend but not 2nd, test first backend first
        backend1.set(10, String::from("a"));
        assert!(!backend1.is_empty());
        assert!(backend2.is_empty());
    }

    #[test]
    fn test_is_empty_partial2() {
        let backend1 = Box::new(HashMap::<u8, String>::new());
        let backend2 = Box::new(HashMap::<i8, String>::new());

        let (backend1, mut backend2) = dual_backends(
            backend1,
            |k1, _v| (*k1 < 10).then(|| *k1 as i8),
            backend2,
            |k2, _v| (*k2 < 10).then(|| *k2 as u8),
        );

        assert!(backend1.is_empty());
        assert!(backend2.is_empty());

        // populate via first backend but not 2nd, test first backend first
        backend2.set(10, String::from("a"));
        assert!(backend1.is_empty());
        assert!(!backend2.is_empty());
    }

    #[test]
    #[should_panic(expected = "backend1 is not empty")]
    fn test_panic_inner1_not_empty() {
        let backend1 = Box::new(HashMap::<u8, String>::from([(1, String::from("a"))]));
        let backend2 = Box::new(HashMap::<i8, String>::new());

        dual_backends(
            backend1,
            |k1, _v| Some(*k1 as i8),
            backend2,
            |k2, _v| Some(*k2 as u8),
        );
    }

    #[test]
    #[should_panic(expected = "backend2 is not empty")]
    fn test_panic_inner2_not_empty() {
        let backend1 = Box::new(HashMap::<u8, String>::new());
        let backend2 = Box::new(HashMap::<i8, String>::from([(1, String::from("a"))]));

        dual_backends(
            backend1,
            |k1, _v| Some(*k1 as i8),
            backend2,
            |k2, _v| Some(*k2 as u8),
        );
    }

    #[test]
    fn test_generic1() {
        use crate::cache_system::backend::test_util::test_generic;

        test_generic(|| {
            let backend1 = Box::new(HashMap::<u8, String>::new());
            let backend2 = Box::new(HashMap::<i8, String>::new());

            let (backend1, _backend2) = dual_backends(
                backend1,
                |k1, _v| Some(*k1 as i8),
                backend2,
                |k2, _v| Some(*k2 as u8),
            );
            backend1
        });
    }

    #[test]
    fn test_generic2() {
        use crate::cache_system::backend::test_util::test_generic;

        test_generic(|| {
            let backend1 = Box::new(HashMap::<i8, String>::new());
            let backend2 = Box::new(HashMap::<u8, String>::new());

            let (_backend1, backend2) = dual_backends(
                backend1,
                |k1, _v| Some(*k1 as u8),
                backend2,
                |k2, _v| Some(*k2 as i8),
            );
            backend2
        });
    }
}
