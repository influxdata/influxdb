//! Attributes for [`object_store`] response to signal cache state.

use futures::future::BoxFuture;
use std::{borrow::Cow, fmt, sync::Arc};

use object_store::{Attribute, AttributeValue};

/// Attribute that encodes [`CacheStateKind`].
pub const ATTR_CACHE_STATE: Attribute = Attribute::Metadata(Cow::Borrowed("cache_state"));

/// Result type for cache requests.
pub type CacheRequestResult<V> = Result<V, Arc<dyn std::error::Error + Send + Sync>>;

/// Simple cache state kind for attributes and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheStateKind {
    /// Entry was already part of the cache and fully fetched.
    WasCached,

    /// Entry was already part of the cache but did not finish loading.
    AlreadyLoading,

    /// A new entry was created.
    NewEntry,
}

/// Cache state in response to `get_or_fetch` operations on value `V`.
///
/// The data may be available earlier using the optional `D`.
pub enum CacheState<V, D>
where
    V: Clone + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Entry was already part of the cache and fully fetched.
    WasCached(CacheRequestResult<V>),

    /// Entry was already part of the cache but did not finish loading.
    AlreadyLoading(BoxFuture<'static, CacheRequestResult<V>>, D),

    /// A new entry was created.
    NewEntry(BoxFuture<'static, CacheRequestResult<V>>, D),
}

impl<V, D> CacheState<V, D>
where
    V: Clone + Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
{
    /// Returns the kind of cache state for use in attributes and metrics.
    pub fn kind(&self) -> CacheStateKind {
        match self {
            Self::WasCached(_) => CacheStateKind::WasCached,
            Self::AlreadyLoading(_, _) => CacheStateKind::AlreadyLoading,
            Self::NewEntry(_, _) => CacheStateKind::NewEntry,
        }
    }

    /// Await the inner result, regardless of cache state.
    ///
    /// This method extracts the result from WasCached or awaits the future
    /// from AlreadyLoading/NewEntry states.
    pub async fn await_inner(self) -> CacheRequestResult<V> {
        match self {
            Self::WasCached(result) => result,
            Self::AlreadyLoading(fut, _) => fut.await,
            Self::NewEntry(fut, _) => fut.await,
        }
    }

    /// Extract the inner future if present.
    ///
    /// Returns None for WasCached states, Some(future) for AlreadyLoading/NewEntry states.
    /// This allows tests to access the future separately from early access data.
    pub fn inner_fut(self) -> Option<BoxFuture<'static, CacheRequestResult<V>>> {
        match self {
            Self::WasCached(_) => None,
            Self::AlreadyLoading(fut, _) => Some(fut),
            Self::NewEntry(fut, _) => Some(fut),
        }
    }

    /// Extract the early access data if present.
    ///
    /// Returns None for WasCached states or when no early access data was provided.
    /// Returns Some(data) for AlreadyLoading/NewEntry states with early access data.
    pub fn early_access_data(&self) -> Option<&D> {
        match self {
            Self::WasCached(_) => None,
            Self::AlreadyLoading(_, data) => Some(data),
            Self::NewEntry(_, data) => Some(data),
        }
    }
}

impl From<CacheStateKind> for AttributeValue {
    fn from(state: CacheStateKind) -> Self {
        let s = match state {
            CacheStateKind::AlreadyLoading => "already_loading",
            CacheStateKind::NewEntry => "new_entry",
            CacheStateKind::WasCached => "was_cached",
        };
        Self::from(s)
    }
}

impl TryFrom<&AttributeValue> for CacheStateKind {
    type Error = String;

    fn try_from(value: &AttributeValue) -> Result<Self, Self::Error> {
        match value.as_ref() {
            "already_loading" => Ok(Self::AlreadyLoading),
            "new_entry" => Ok(Self::NewEntry),
            "was_cached" => Ok(Self::WasCached),
            other => Err(format!("unknown cache state: {other}")),
        }
    }
}

impl<V, D> fmt::Debug for CacheState<V, D>
where
    V: Clone + Send + Sync + 'static + fmt::Debug,
    D: Clone + Send + Sync + 'static + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WasCached(result) => f.debug_tuple("WasCached").field(result).finish(),
            Self::AlreadyLoading(_, data) => f
                .debug_tuple("AlreadyLoading")
                .field(&"<future>")
                .field(data)
                .finish(),
            Self::NewEntry(_, data) => f
                .debug_tuple("NewEntry")
                .field(&"<future>")
                .field(data)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_state_kind_roundtrip() {
        for cache_state in [
            CacheStateKind::AlreadyLoading,
            CacheStateKind::NewEntry,
            CacheStateKind::WasCached,
        ] {
            let val = AttributeValue::from(cache_state);
            let cache_state_2 = CacheStateKind::try_from(&val).unwrap();
            assert_eq!(cache_state, cache_state_2);
        }

        assert_eq!(
            CacheStateKind::try_from(&AttributeValue::from("foo")).unwrap_err(),
            "unknown cache state: foo",
        );
    }
}
