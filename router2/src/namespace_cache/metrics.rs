//! Metric instrumentation for a [`NamespaceCache`] implementation.

use std::sync::Arc;

use data_types::DatabaseName;
use iox_catalog::interface::NamespaceSchema;
use metric::{Metric, U64Counter};

use super::NamespaceCache;

/// An [`InstrumentedCache`] decorates a [`NamespaceCache`] with cache read
/// hit/miss and cache put insert/update metrics.
#[derive(Debug)]
pub struct InstrumentedCache<T> {
    inner: T,

    /// A cache read hit
    get_hit_counter: U64Counter,
    /// A cache read miss
    get_miss_counter: U64Counter,

    /// A cache put for a namespace that did not previously exist.
    put_insert_counter: U64Counter,
    /// A cache put replacing a namespace that previously had a cache entry.
    put_update_counter: U64Counter,
}

impl<T> InstrumentedCache<T> {
    /// Instrument `T`, recording cache operations to `registry`.
    pub fn new(inner: T, registry: Arc<metric::Registry>) -> Self {
        let get_counter: Metric<U64Counter> =
            registry.register_metric("namespace_cache_get_count", "cache read requests");
        let get_hit_counter = get_counter.recorder(&[("result", "hit")]);
        let get_miss_counter = get_counter.recorder(&[("result", "miss")]);

        let put_counter: Metric<U64Counter> =
            registry.register_metric("namespace_cache_put_count", "cache put requests");
        let put_insert_counter = put_counter.recorder(&[("op", "insert")]);
        let put_update_counter = put_counter.recorder(&[("op", "update")]);

        Self {
            inner,
            get_hit_counter,
            get_miss_counter,
            put_insert_counter,
            put_update_counter,
        }
    }
}

impl<T> NamespaceCache for Arc<InstrumentedCache<T>>
where
    T: NamespaceCache,
{
    fn get_schema(&self, namespace: &DatabaseName<'_>) -> Option<Arc<NamespaceSchema>> {
        match self.inner.get_schema(namespace) {
            Some(v) => {
                self.get_hit_counter.inc(1);
                Some(v)
            }
            None => {
                self.get_miss_counter.inc(1);
                None
            }
        }
    }

    fn put_schema(
        &self,
        namespace: DatabaseName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> Option<Arc<NamespaceSchema>> {
        match self.inner.put_schema(namespace, schema) {
            Some(v) => {
                self.put_update_counter.inc(1);
                Some(v)
            }
            None => {
                self.put_insert_counter.inc(1);
                None
            }
        }
    }
}
