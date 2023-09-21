//! Storage backends to keep and manage cached entries.
use std::{any::Any, fmt::Debug, hash::Hash};

pub mod hash_map;
pub mod policy;

#[cfg(test)]
mod test_util;

/// Backend to keep and manage stored entries.
///
/// A backend might remove entries at any point, e.g. due to memory pressure or expiration.
pub trait CacheBackend: Debug + Send + 'static {
    /// Cache key.
    type K: Clone + Eq + Hash + Ord + Debug + Send + 'static;

    /// Cached value.
    type V: Clone + Debug + Send + 'static;

    /// Get value for given key if it exists.
    fn get(&mut self, k: &Self::K) -> Option<Self::V>;

    /// Set value for given key.
    ///
    /// It is OK to set and override a key that already exists.
    fn set(&mut self, k: Self::K, v: Self::V);

    /// Remove value for given key.
    ///
    /// It is OK to remove a key even when it does not exist.
    fn remove(&mut self, k: &Self::K);

    /// Check if backend is empty.
    fn is_empty(&self) -> bool;

    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

impl<K, V> CacheBackend for Box<dyn CacheBackend<K = K, V = V>>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        self.as_mut().get(k)
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        self.as_mut().set(k, v)
    }

    fn remove(&mut self, k: &Self::K) {
        self.as_mut().remove(k)
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}
