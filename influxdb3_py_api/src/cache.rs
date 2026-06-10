use std::{collections::BTreeMap, sync::Arc, time::Duration};

use hashbrown::{HashMap, HashSet};
use influxdb3_id::{DbId, TriggerId};
use iox_time::{Time, TimeProvider};
use parking_lot::Mutex;
use pyo3::{Py, PyAny, PyErr, PyResult, Python, exceptions::PyValueError, pyclass, pymethods};

#[derive(Debug)]
pub enum CacheType {
    TestCache(String),
    Trigger {
        database: String,
        trigger_name: String,
    },
}

// Cache entry with optional expiration
#[derive(Debug)]
pub struct CacheEntry {
    value: Py<PyAny>,         // Python object reference with proper reference counting
    expires_at: Option<Time>, // Expiration time if any
}

impl CacheEntry {
    fn try_new(
        value: Py<PyAny>,
        ttl: Option<f64>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> PyResult<Self> {
        let expires_at = ttl
            .map(|seconds| {
                PyResult::Ok(
                    time_provider.now()
                        + Duration::try_from_secs_f64(seconds)
                            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?,
                )
            })
            .transpose()?;
        Ok(Self { value, expires_at })
    }
}

// Cache store that manages namespaced caches
#[derive(Debug)]
pub struct CacheStore {
    // Map of namespace -> (key -> cache entry)
    namespaces: HashMap<CacheId, ExpiringCache>,
    time_provider: Arc<dyn TimeProvider>,
    last_cleanup: Time,
    cleanup_interval: Duration,
}

#[derive(Debug)]
pub struct ExpiringCache {
    entries: HashMap<String, CacheEntry>,
    expirations: BTreeMap<Time, HashSet<String>>,
    default_ttl: Option<Duration>,
    time_provider: Arc<dyn TimeProvider>,
}

impl ExpiringCache {
    fn new(time_provider: Arc<dyn TimeProvider>, default_ttl: Option<Duration>) -> Self {
        Self {
            entries: HashMap::default(),
            expirations: BTreeMap::default(),
            default_ttl,
            time_provider,
        }
    }

    fn insert(&mut self, key: String, mut entry: CacheEntry) {
        // this is for test call caches, which should expire.
        if let Some(default_ttl) = self.default_ttl
            && entry.expires_at.is_none()
        {
            entry.expires_at = Some(self.time_provider.now() + default_ttl);
        }
        // if key has a ttl, record its expiration.
        let expiration = entry.expires_at;
        // if this was an overwrite, remove from expirations
        if let Some(old_entry) = self.entries.insert(key.clone(), entry)
            && let Some(old_expiration) = old_entry.expires_at
        {
            self.expirations
                .get_mut(&old_expiration)
                .unwrap()
                .remove(&key);
        }
        if let Some(expiration) = expiration {
            self.expirations.entry(expiration).or_default().insert(key);
        }
    }

    fn get(&mut self, key: &str, py: Python<'_>) -> Option<Py<PyAny>> {
        let entry = self.entries.get(key)?;
        if let Some(expiration) = entry.expires_at
            && expiration <= self.time_provider.now()
        {
            self.remove(key);
            return None;
        }
        Some(entry.value.clone_ref(py))
    }

    fn remove(&mut self, key: &str) -> bool {
        if let Some(entry) = self.entries.remove(key) {
            if let Some(expiration) = entry.expires_at {
                self.expirations.get_mut(&expiration).unwrap().remove(key);
            }
            return true;
        }
        false
    }

    fn cleanup(&mut self) {
        let now = self.time_provider.now();
        if self.expirations.is_empty() || *self.expirations.first_key_value().unwrap().0 > now {
            return;
        }
        let mut unexpired_expirations = self.expirations.split_off(&now);
        for keys in self.expirations.values() {
            for key in keys {
                self.entries.remove(key);
            }
        }
        std::mem::swap(&mut self.expirations, &mut unexpired_expirations);
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
enum CacheId {
    Global(),
    GlobalTest(String),
    Trigger { db_id: DbId, trigger_id: TriggerId },
    TriggerTest(String),
}

impl CacheId {
    fn default_expiration(&self) -> Option<Duration> {
        match self {
            Self::Global() | Self::Trigger { .. } => None,
            // for tests, keep values around for 30 minutes.
            Self::GlobalTest(_) | Self::TriggerTest(_) => Some(Duration::from_secs(30 * 60)),
        }
    }
}

impl CacheStore {
    pub fn new(time_provider: Arc<dyn TimeProvider>, cleanup_interval: Duration) -> Self {
        let last_cleanup = time_provider.now();
        Self {
            namespaces: HashMap::new(),
            time_provider,
            cleanup_interval,
            last_cleanup,
        }
    }

    fn should_run_cleanup(&self) -> bool {
        self.time_provider
            .now()
            .checked_duration_since(self.last_cleanup)
            .unwrap_or_default()
            >= self.cleanup_interval
    }

    fn cleanup(&mut self) {
        if !self.should_run_cleanup() {
            return;
        }
        for cache in self.namespaces.values_mut() {
            cache.cleanup();
        }

        self.namespaces.retain(|_, cache| !cache.is_empty());

        self.last_cleanup = self.time_provider.now();
    }

    fn put(
        &mut self,
        cache_id: &CacheId,
        key: &str,
        value: Py<PyAny>,
        ttl: Option<f64>,
    ) -> PyResult<()> {
        let entry = CacheEntry::try_new(value, ttl, Arc::clone(&self.time_provider))?;
        if let Some(cache) = self.namespaces.get_mut(cache_id) {
            cache.insert(key.to_string(), entry);
            return Ok(());
        }
        self.namespaces
            .entry(cache_id.clone())
            .or_insert_with(|| {
                ExpiringCache::new(
                    Arc::clone(&self.time_provider),
                    cache_id.default_expiration(),
                )
            })
            .insert(key.to_string(), entry);
        Ok(())
    }

    fn get(&mut self, cache_id: &CacheId, py: Python<'_>, key: &str) -> Option<Py<PyAny>> {
        let cache = self.namespaces.get_mut(cache_id)?;

        cache.get(key, py)
    }

    fn delete(&mut self, cache_id: &CacheId, key: &str) -> bool {
        if let Some(cache) = self.namespaces.get_mut(cache_id) {
            cache.remove(key)
        } else {
            false
        }
    }
    pub fn drop_trigger_cache(&mut self, db_id: DbId, trigger_id: TriggerId) -> bool {
        self.namespaces
            .remove(&CacheId::Trigger { db_id, trigger_id })
            .is_some()
    }

    pub fn drop_all_trigger_caches_for_db(&mut self, db_id: DbId) {
        self.namespaces
            .retain(|id, _| !matches!(id, CacheId::Trigger { db_id: d, .. } if *d == db_id));
    }
}

// Python class for Cache
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCache {
    global_cache_id: CacheId,
    local_cache_id: CacheId,
    cache_store: Arc<Mutex<CacheStore>>,
}

impl PyCache {
    fn cache_id(&self, use_global: Option<bool>) -> &CacheId {
        if use_global.unwrap_or_default() {
            &self.global_cache_id
        } else {
            &self.local_cache_id
        }
    }
    pub fn new_test_cache(cache_store: Arc<Mutex<CacheStore>>, test_name: String) -> Self {
        Self {
            global_cache_id: CacheId::GlobalTest(test_name.clone()),
            local_cache_id: CacheId::TriggerTest(test_name),
            cache_store,
        }
    }

    pub fn new_trigger_cache(
        cache_store: Arc<Mutex<CacheStore>>,
        db_id: DbId,
        trigger_id: TriggerId,
    ) -> Self {
        Self {
            global_cache_id: CacheId::Global(),
            local_cache_id: CacheId::Trigger { db_id, trigger_id },
            cache_store,
        }
    }

    pub(crate) fn cleanup(&self, py: Python<'_>) {
        let cache_store = Arc::clone(&self.cache_store);
        py.detach(|| {
            cache_store.lock().cleanup();
        });
    }
}

#[pymethods]
impl PyCache {
    #[pyo3(signature = (key, value, ttl=None, use_global=None))]
    pub fn put(
        &self,
        key: String,
        value: Py<PyAny>,
        ttl: Option<f64>,
        use_global: Option<bool>,
    ) -> PyResult<()> {
        let cache_id = self.cache_id(use_global);
        self.cache_store.lock().put(cache_id, &key, value, ttl)?;

        Ok(())
    }

    #[pyo3(signature = (key, default=None, use_global=None))]
    fn get(
        &self,
        key: String,
        default: Option<Py<PyAny>>,
        use_global: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        Python::attach(|py| {
            let cache_id = self.cache_id(use_global);
            let result = self.cache_store.lock().get(cache_id, py, &key);

            match result {
                Some(value) => {
                    // Return the retrieved Python object
                    Ok(value)
                }
                None => {
                    // Return the default value if provided
                    if let Some(default_value) = default {
                        Ok(default_value)
                    } else {
                        // Return None if no default was provided
                        let x = py.None();
                        Ok(x)
                    }
                }
            }
        })
    }

    #[pyo3(signature = (key, use_global=None))]
    fn delete(&self, key: String, use_global: Option<bool>) -> PyResult<bool> {
        let cache_id = self.cache_id(use_global);
        let result = self.cache_store.lock().delete(cache_id, &key);

        Ok(result)
    }
}

#[cfg(test)]
mod tests;
