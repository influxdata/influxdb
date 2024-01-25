//! A local in-memory cache

mod limit;

use crate::local::limit::MemoryLimiter;
use crate::{CacheEntry, CacheKey, CacheValue};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use snafu::Snafu;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Error for [`CatalogCache`]
#[derive(Debug, Snafu)]
#[allow(missing_docs, missing_copy_implementations)]
pub enum Error {
    #[snafu(display("Cannot reserve additional {size} bytes for cache containing {current} bytes as would exceed limit of {limit} bytes"))]
    OutOfMemory {
        size: usize,
        current: usize,
        limit: usize,
    },

    #[snafu(display("Cannot reserve additional {size} bytes for cache as request exceeds total memory limit of {limit} bytes"))]
    TooLarge { size: usize, limit: usize },
}

/// Result for [`CatalogCache`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A trait for observing updated to [`CatalogCache`]
///
/// This can be used for injecting metrics, maintaining secondary indices or otherwise
///
/// Note: members are invoked under locks in [`CatalogCache`] and should therefore
/// be short-running and not call back into [`CatalogCache`]
pub trait CatalogCacheObserver: std::fmt::Debug + Send + Sync {
    /// Called before a value is potentially inserted into [`CatalogCache`]
    ///
    /// This is called regardless of it [`CatalogCache`] already contains the value
    fn insert(&self, key: CacheKey, new: &CacheValue, old: Option<&CacheValue>);

    /// A key removed from the [`CatalogCache`]
    fn evict(&self, key: CacheKey, value: &CacheValue);
}

/// A concurrent Not-Recently-Used cache mapping [`CacheKey`] to [`CacheValue`]
#[derive(Debug, Default)]
pub struct CatalogCache {
    map: DashMap<CacheKey, CacheEntry>,
    observer: Option<Arc<dyn CatalogCacheObserver>>,
    limit: Option<MemoryLimiter>,
}

impl CatalogCache {
    /// Create a new `CatalogCache` with an optional memory limit
    pub fn new(limit: Option<usize>) -> Self {
        Self {
            limit: limit.map(MemoryLimiter::new),
            ..Default::default()
        }
    }

    /// Sets a [`CatalogCacheObserver`] for this [`CatalogCache`]
    pub fn with_observer(self, observer: Arc<dyn CatalogCacheObserver>) -> Self {
        Self {
            observer: Some(observer),
            ..self
        }
    }

    /// Returns the value for `key` if it exists
    pub fn get(&self, key: CacheKey) -> Option<CacheValue> {
        let entry = self.map.get(&key)?;
        entry.used.store(true, Ordering::Relaxed);
        Some(entry.value.clone())
    }

    /// Insert the given `value` into the cache
    ///
    /// Skips insertion and returns false iff an entry already exists with the
    /// same or greater generation
    pub fn insert(&self, key: CacheKey, value: CacheValue) -> Result<bool> {
        match self.map.entry(key) {
            Entry::Occupied(mut o) => {
                let old = &o.get().value;
                if value.generation <= old.generation {
                    return Ok(false);
                }
                if let Some(l) = &self.limit {
                    let new_len = value.data.len();
                    let cur_len = old.data.len();
                    match new_len > cur_len {
                        true => l.reserve(new_len - cur_len)?,
                        false => l.free(cur_len - new_len),
                    }
                }
                if let Some(v) = &self.observer {
                    v.insert(key, &value, Some(old));
                }
                o.insert(value.into());
            }
            Entry::Vacant(v) => {
                if let Some(l) = &self.limit {
                    l.reserve(value.data.len())?;
                }
                if let Some(v) = &self.observer {
                    v.insert(key, &value, None);
                }
                v.insert(value.into());
            }
        }
        Ok(true)
    }

    /// Removes the [`CacheValue`] for the given `key` if any
    pub fn delete(&self, key: CacheKey) -> Option<CacheValue> {
        match self.map.entry(key) {
            Entry::Occupied(o) => {
                let old = &o.get().value;
                if let Some(v) = &self.observer {
                    v.evict(key, old)
                }
                if let Some(l) = &self.limit {
                    l.free(old.data.len())
                }
                Some(o.remove().value)
            }
            _ => None,
        }
    }

    /// Returns an iterator over the items in this cache
    pub fn list(&self) -> CacheIterator<'_> {
        CacheIterator(self.map.iter())
    }

    /// Evict all entries not accessed with [`CatalogCache::get`] or updated since
    /// the last call to this function
    ///
    /// Periodically calling this provides a Not-Recently-Used eviction policy
    pub fn evict_unused(&self) {
        self.map.retain(|key, entry| {
            let retain = entry.used.swap(false, Ordering::Relaxed);
            if !retain {
                if let Some(v) = &self.observer {
                    v.evict(*key, &entry.value);
                }
                if let Some(l) = &self.limit {
                    l.free(entry.value.data.len());
                }
            }
            retain
        });
    }
}

/// Iterator for [`CatalogCache`]
#[allow(missing_debug_implementations)]
pub struct CacheIterator<'a>(dashmap::iter::Iter<'a, CacheKey, CacheEntry>);

impl<'a> Iterator for CacheIterator<'a> {
    type Item = (CacheKey, CacheValue);

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.0.next()?;
        Some((*value.key(), value.value().value.clone()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dashmap::DashSet;

    #[derive(Debug, Default)]
    struct KeyObserver {
        keys: DashSet<CacheKey>,
    }

    impl KeyObserver {
        fn keys(&self) -> Vec<CacheKey> {
            let mut keys: Vec<_> = self.keys.iter().map(|k| *k).collect();
            keys.sort_unstable();
            keys
        }
    }

    impl CatalogCacheObserver for KeyObserver {
        fn insert(&self, key: CacheKey, _new: &CacheValue, _old: Option<&CacheValue>) {
            self.keys.insert(key);
        }

        fn evict(&self, key: CacheKey, _value: &CacheValue) {
            self.keys.remove(&key);
        }
    }

    #[test]
    fn test_basic() {
        let observer = Arc::new(KeyObserver::default());
        let cache = CatalogCache::default().with_observer(Arc::clone(&observer) as _);

        let v1 = CacheValue::new("1".into(), 5);
        assert!(cache.insert(CacheKey::Table(0), v1.clone()).unwrap());
        assert_eq!(cache.get(CacheKey::Table(0)).unwrap(), v1);

        // Older generation rejected
        assert!(!cache
            .insert(CacheKey::Table(0), CacheValue::new("2".into(), 3))
            .unwrap());

        // Value unchanged
        assert_eq!(cache.get(CacheKey::Table(0)).unwrap(), v1);

        // Different key accepted
        let v2 = CacheValue::new("2".into(), 5);
        assert!(cache.insert(CacheKey::Table(1), v2.clone()).unwrap());
        assert_eq!(cache.get(CacheKey::Table(1)).unwrap(), v2);

        let v3 = CacheValue::new("3".into(), 0);
        assert!(cache.insert(CacheKey::Partition(0), v3.clone()).unwrap());

        // Newer generation updates
        let v4 = CacheValue::new("4".into(), 6);
        assert!(cache.insert(CacheKey::Table(0), v4.clone()).unwrap());

        let mut values: Vec<_> = cache.list().collect();
        values.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        assert_eq!(
            values,
            vec![
                (CacheKey::Table(0), v4.clone()),
                (CacheKey::Table(1), v2),
                (CacheKey::Partition(0), v3),
            ]
        );
        assert_eq!(
            observer.keys(),
            vec![
                CacheKey::Table(0),
                CacheKey::Table(1),
                CacheKey::Partition(0)
            ]
        );

        assert_eq!(cache.get(CacheKey::Namespace(0)), None);
        assert_eq!(cache.delete(CacheKey::Namespace(0)), None);

        assert_eq!(cache.get(CacheKey::Table(0)).unwrap(), v4);
        assert_eq!(cache.delete(CacheKey::Table(0)).unwrap(), v4);
        assert_eq!(cache.get(CacheKey::Table(0)), None);

        assert_eq!(cache.list().count(), 2);
        assert_eq!(observer.keys.len(), 2);
    }

    #[test]
    fn test_nru() {
        let observer = Arc::new(KeyObserver::default());
        let cache = CatalogCache::default().with_observer(Arc::clone(&observer) as _);

        let value = CacheValue::new("1".into(), 0);
        cache.insert(CacheKey::Namespace(0), value.clone()).unwrap();
        cache.insert(CacheKey::Partition(0), value.clone()).unwrap();
        cache.insert(CacheKey::Table(0), value.clone()).unwrap();

        cache.evict_unused();
        // Inserted records should only be evicted on the next pass
        assert_eq!(cache.list().count(), 3);
        assert_eq!(observer.keys.len(), 3);

        // Updating a record marks it used
        cache
            .insert(CacheKey::Table(0), CacheValue::new("2".into(), 1))
            .unwrap();

        // Fetching a record marks it used
        cache.get(CacheKey::Partition(0)).unwrap();

        // Insert a new record is used
        cache.insert(CacheKey::Partition(1), value.clone()).unwrap();

        cache.evict_unused();

        // Namespace(0) evicted
        let mut values: Vec<_> = cache.list().map(|(k, _)| k).collect();
        values.sort_unstable();
        let expected = vec![
            CacheKey::Table(0),
            CacheKey::Partition(0),
            CacheKey::Partition(1),
        ];
        assert_eq!(values, expected);
        assert_eq!(observer.keys(), expected);

        // Stale updates don't count as usage
        assert!(!cache.insert(CacheKey::Partition(0), value).unwrap());

        // Listing does not preserve recently used
        assert_eq!(cache.list().count(), 3);

        cache.evict_unused();
        assert_eq!(cache.list().count(), 0);
        assert_eq!(observer.keys.len(), 0)
    }

    #[test]
    fn test_limit() {
        let cache = CatalogCache::new(Some(200));

        let k1 = CacheKey::Table(1);
        let k2 = CacheKey::Table(2);
        let k3 = CacheKey::Table(3);

        let v_100 = Bytes::from(vec![0; 100]);
        let v_20 = Bytes::from(vec![0; 20]);

        cache.insert(k1, CacheValue::new(v_100.clone(), 0)).unwrap();
        cache.insert(k2, CacheValue::new(v_100.clone(), 0)).unwrap();

        let r = cache.insert(k3, CacheValue::new(v_20.clone(), 0));
        assert_eq!(r.unwrap_err().to_string(), "Cannot reserve additional 20 bytes for cache containing 200 bytes as would exceed limit of 200 bytes");

        // Upsert k1 to 20 bytes
        cache.insert(k1, CacheValue::new(v_20.clone(), 1)).unwrap();

        // Can now insert k3
        cache.insert(k3, CacheValue::new(v_20.clone(), 0)).unwrap();

        // Should evict nothing
        cache.evict_unused();

        // Cannot increase size of k3 to 100
        let r = cache.insert(k3, CacheValue::new(v_100.clone(), 1));
        assert_eq!(r.unwrap_err().to_string(), "Cannot reserve additional 80 bytes for cache containing 140 bytes as would exceed limit of 200 bytes");

        cache.delete(k2).unwrap();
        cache.insert(k3, CacheValue::new(v_100.clone(), 1)).unwrap();

        let r = cache.insert(k2, CacheValue::new(v_100.clone(), 1));
        assert_eq!(r.unwrap_err().to_string(), "Cannot reserve additional 100 bytes for cache containing 120 bytes as would exceed limit of 200 bytes");

        // Should evict everything apart from k3
        cache.evict_unused();

        cache.insert(k2, CacheValue::new(v_100.clone(), 1)).unwrap();
    }
}
