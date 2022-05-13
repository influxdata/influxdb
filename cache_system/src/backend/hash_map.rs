//! Implements [`CacheBackend`] for [`HashMap`].
use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    hash::{BuildHasher, Hash},
};

use super::CacheBackend;

impl<K, V, S> CacheBackend for HashMap<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: BuildHasher + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        Self::get(self, k).cloned()
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        self.insert(k, v);
    }

    fn remove(&mut self, k: &Self::K) {
        self.remove(k);
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generic() {
        use crate::backend::test_util::test_generic;

        test_generic(HashMap::new);
    }
}
