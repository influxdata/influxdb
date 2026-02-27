pub mod chain;
pub mod level_trigger;
pub mod limit;
mod notify;
pub mod observer;
pub mod test_utils;

use std::sync::Arc;

use crate::cache_system::DynError;

/// A trait for hooking into cache updates.
///
/// This can be used for:
/// - injecting metrics
/// - maintaining secondary indices
/// - limiting memory usage
/// - ...
///
/// Note: members are invoked under locks and should therefore
/// be short-running and not call back into the cache.
///
/// # Eventual Consistency
/// To simplify accounting and prevent large-scale locking, [`evict`](Self::evict) will only be called after the
/// fetching future is finished. This means that a key may be observed concurrently, one version that is dropped from
/// the cache but that still has a polling future and a new version. Use the generation number to distinguish them.
pub trait Hook<K: ?Sized>: std::fmt::Debug + Send + Sync {
    /// Called before a value is potentially inserted.
    fn insert(&self, _gen: u64, _k: &Arc<K>) {}

    /// A value was fetched.
    ///
    /// The hook can reject a value using an error.
    fn fetched(&self, _gen: u64, _k: &Arc<K>, _res: Result<usize, &DynError>) -> HookDecision {
        HookDecision::default()
    }

    /// A key removed.
    fn evict(&self, _gen: u64, _k: &Arc<K>, _res: EvictResult) {}
}

/// Decision made by [`Hook::fetched`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookDecision {
    /// The the entry.
    #[default]
    Keep,

    /// Evict the entry immediately.
    Evict,
}

impl HookDecision {
    /// Combine two decisions but favor [eviction](Self::Evict).
    pub fn favor_evict(self, other: Self) -> Self {
        match (self, other) {
            (Self::Keep, Self::Keep) => Self::Keep,
            (Self::Keep, Self::Evict) => Self::Evict,
            (Self::Evict, Self::Keep) => Self::Evict,
            (Self::Evict, Self::Evict) => Self::Evict,
        }
    }
}

/// Status that is report to [`Hook::evict`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvictResult {
    /// Evict element that was never fetched.
    Unfetched,

    /// Evict element that was fetched.
    Fetched {
        /// Size in bytes.
        size: usize,
    },

    /// Evict element that could not be fetched due to error.
    Failed,
}
