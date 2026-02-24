use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::cache_system::{DynError, hook::Hook};

use super::{EvictResult, HookDecision};

/// Record created by [`TestHook`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestHookRecord<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    Insert(u64, K),
    Fetched(u64, K, Result<usize, String>),
    Evict(u64, K, EvictResult),
}

/// A [`Hook`] that records its interactions.
///
/// Generations recorded by this mechanisms are normalized for easier testing.
#[derive(Debug, Default)]
pub struct TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    state: Mutex<TestHookState<K>>,
}

#[derive(Debug, Default)]
struct TestHookState<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    records: Vec<TestHookRecord<K>>,
    fetch_results: VecDeque<HookDecision>,
    next_gen: u64,
    gen_mapping: HashMap<u64, u64>,
}

impl<K> TestHookState<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    /// Fetch normalized generation.
    fn normalize_gen(&mut self, generation: u64) -> u64 {
        *self.gen_mapping.entry(generation).or_insert_with(|| {
            let g = self.next_gen;
            self.next_gen += 1;
            g
        })
    }

    /// Forget generation.
    fn forget_gen(&mut self, generation: u64) {
        self.gen_mapping.remove(&generation);
    }
}

impl<K> TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    /// Return recorded interactions.
    pub fn records(&self) -> Vec<TestHookRecord<K>> {
        self.state.lock().unwrap().records.clone()
    }

    /// Mock next result of [`Hook::fetched`].
    ///
    /// If there are already unused mocks, then we attach this to the end.
    ///
    /// If no result is mocked, [`HookDecision::default`] is used.
    pub fn mock_next_fetch(&self, res: HookDecision) {
        self.state.lock().unwrap().fetch_results.push_back(res);
    }
}

impl<K> Drop for TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    fn drop(&mut self) {
        if !std::thread::panicking() && !self.state.lock().unwrap().fetch_results.is_empty() {
            panic!("mocked fetch results left");
        }
    }
}

impl<K> Hook<K> for TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    fn insert(&self, generation: u64, k: &Arc<K>) {
        let mut state = self.state.lock().unwrap();

        let gen_norm = state.normalize_gen(generation);
        state
            .records
            .push(TestHookRecord::Insert(gen_norm, k.as_ref().clone()))
    }

    fn fetched(&self, generation: u64, k: &Arc<K>, res: Result<usize, &DynError>) -> HookDecision {
        let mut state = self.state.lock().unwrap();
        let gen_norm = state.normalize_gen(generation);
        state.records.push(TestHookRecord::Fetched(
            gen_norm,
            k.as_ref().clone(),
            res.map_err(|e| e.to_string()),
        ));
        state.fetch_results.pop_front().unwrap_or_default()
    }

    fn evict(&self, generation: u64, k: &Arc<K>, res: EvictResult) {
        let mut state = self.state.lock().unwrap();

        let gen_norm = state.normalize_gen(generation);
        state
            .records
            .push(TestHookRecord::Evict(gen_norm, k.as_ref().clone(), res));
        state.forget_gen(generation);
    }
}

/// A [`Hook`] that does nothing.
#[derive(Debug)]
pub struct NoOpHook<K>(PhantomData<K>)
where
    K: std::fmt::Debug + Send + Sync + ?Sized;

impl<K> Default for NoOpHook<K>
where
    K: std::fmt::Debug + Send + Sync + ?Sized,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K> Hook<K> for NoOpHook<K>
where
    K: std::fmt::Debug + Send + Sync + ?Sized,
{
    fn insert(&self, _gen: u64, _k: &Arc<K>) {}

    fn fetched(&self, _gen: u64, _k: &Arc<K>, _res: Result<usize, &DynError>) -> HookDecision {
        HookDecision::default()
    }

    fn evict(&self, _gen: u64, _k: &Arc<K>, _res: EvictResult) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "mocked fetch results left")]
    fn test_panic_for_remaining_mocks() {
        let hook = TestHook::<&'static str>::default();
        hook.mock_next_fetch(HookDecision::Keep);
    }

    #[test]
    #[should_panic(expected = "foo")]
    fn test_no_double_panic() {
        let hook = TestHook::<&'static str>::default();
        hook.mock_next_fetch(HookDecision::Keep);
        panic!("foo");
    }

    #[test]
    fn test_gen_normalization() {
        let hook = TestHook::<&'static str>::default();
        hook.insert(10, &Arc::new("foo"));
        hook.insert(8, &Arc::new("bar"));
        hook.fetched(10, &Arc::new("foo"), Ok(10));
        hook.evict(10, &Arc::new("foo"), EvictResult::Fetched { size: 10 });
        hook.fetched(8, &Arc::new("bar"), Ok(8));
        hook.insert(10, &Arc::new("foo"));

        assert_eq!(
            hook.records(),
            vec![
                TestHookRecord::Insert(0, "foo"),
                TestHookRecord::Insert(1, "bar"),
                TestHookRecord::Fetched(0, "foo", Ok(10)),
                TestHookRecord::Evict(0, "foo", EvictResult::Fetched { size: 10 }),
                TestHookRecord::Fetched(1, "bar", Ok(8)),
                TestHookRecord::Insert(2, "foo"),
            ],
        );
    }
}
