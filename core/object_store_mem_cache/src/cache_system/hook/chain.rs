use std::sync::Arc;

use crate::cache_system::{DynError, hook::Hook};

use super::{EvictResult, HookDecision};

/// Chains multiple [hooks](Hook).
///
/// For [fetched](Hook::fetched) this will combine errors.
pub struct HookChain<K> {
    hooks: Box<[Arc<dyn Hook<K>>]>,
}

impl<K> HookChain<K> {
    pub fn new(hooks: impl IntoIterator<Item = Arc<dyn Hook<K>>>) -> Self {
        Self {
            hooks: hooks.into_iter().collect(),
        }
    }
}

impl<K> std::fmt::Debug for HookChain<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookChain")
            .field("hooks", &self.hooks)
            .finish()
    }
}

impl<K> Hook<K> for HookChain<K> {
    fn insert(&self, generation: u64, k: &Arc<K>) {
        for hook in &self.hooks {
            hook.insert(generation, k);
        }
    }

    fn fetched(&self, generation: u64, k: &Arc<K>, res: Result<usize, &DynError>) -> HookDecision {
        self.hooks
            .iter()
            .map(|hook| hook.fetched(generation, k, res))
            .fold::<Option<HookDecision>, _>(None, |a, b| {
                Some(a.map(|a| a.favor_evict(b)).unwrap_or(b))
            })
            .unwrap_or_default()
    }

    fn evict(&self, generation: u64, k: &Arc<K>, res: EvictResult) {
        for hook in &self.hooks {
            hook.evict(generation, k, res);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache_system::{
        hook::test_utils::{TestHook, TestHookRecord},
        utils::str_err,
    };

    use super::*;

    #[test]
    fn test_empty_hook_chain() {
        let chain = HookChain::<()>::new([]);

        chain.insert(1, &Arc::new(()));
        assert_eq!(
            chain.fetched(2, &Arc::new(()), Ok(1)),
            HookDecision::default(),
        );
        assert_eq!(
            chain.fetched(3, &Arc::new(()), Err(&str_err("foo"))),
            HookDecision::default(),
        );
        chain.evict(4, &Arc::new(()), EvictResult::Unfetched);
        chain.evict(5, &Arc::new(()), EvictResult::Fetched { size: 1 });
        chain.evict(6, &Arc::new(()), EvictResult::Failed);
    }

    #[test]
    fn test_hook_chain() {
        let h1 = Arc::new(TestHook::<u8>::default());
        let h2 = Arc::new(TestHook::<u8>::default());
        let chain = HookChain::<u8>::new([Arc::clone(&h1) as _, Arc::clone(&h2) as _]);

        chain.insert(0, &Arc::new(0));

        h1.mock_next_fetch(HookDecision::Keep);
        h2.mock_next_fetch(HookDecision::Keep);
        assert_eq!(chain.fetched(1, &Arc::new(1), Ok(1000)), HookDecision::Keep);

        h1.mock_next_fetch(HookDecision::Keep);
        h2.mock_next_fetch(HookDecision::Keep);
        assert_eq!(
            chain.fetched(2, &Arc::new(2), Err(&str_err("e1"))),
            HookDecision::Keep
        );

        h1.mock_next_fetch(HookDecision::Evict);
        h2.mock_next_fetch(HookDecision::Keep);
        assert_eq!(
            chain.fetched(3, &Arc::new(3), Ok(2000)),
            HookDecision::Evict,
        );

        h1.mock_next_fetch(HookDecision::Evict);
        h2.mock_next_fetch(HookDecision::Keep);
        assert_eq!(
            chain.fetched(4, &Arc::new(4), Err(&str_err("e2"))),
            HookDecision::Evict,
        );

        h1.mock_next_fetch(HookDecision::Keep);
        h2.mock_next_fetch(HookDecision::Evict);
        assert_eq!(
            chain.fetched(5, &Arc::new(5), Ok(3000)),
            HookDecision::Evict,
        );

        h1.mock_next_fetch(HookDecision::Keep);
        h2.mock_next_fetch(HookDecision::Evict);
        assert_eq!(
            chain.fetched(6, &Arc::new(6), Err(&str_err("e3"))),
            HookDecision::Evict,
        );

        h1.mock_next_fetch(HookDecision::Evict);
        h2.mock_next_fetch(HookDecision::Evict);
        assert_eq!(
            chain.fetched(7, &Arc::new(7), Ok(4000)),
            HookDecision::Evict,
        );

        h1.mock_next_fetch(HookDecision::Evict);
        h2.mock_next_fetch(HookDecision::Evict);
        assert_eq!(
            chain.fetched(8, &Arc::new(8), Err(&str_err("e4"))),
            HookDecision::Evict,
        );

        chain.evict(9, &Arc::new(9), EvictResult::Unfetched);
        chain.evict(10, &Arc::new(10), EvictResult::Fetched { size: 5000 });
        chain.evict(11, &Arc::new(11), EvictResult::Failed);

        let records = vec![
            TestHookRecord::Insert(0, 0),
            TestHookRecord::Fetched(1, 1, Ok(1000)),
            TestHookRecord::Fetched(2, 2, Err("e1".to_owned())),
            TestHookRecord::Fetched(3, 3, Ok(2000)),
            TestHookRecord::Fetched(4, 4, Err("e2".to_owned())),
            TestHookRecord::Fetched(5, 5, Ok(3000)),
            TestHookRecord::Fetched(6, 6, Err("e3".to_owned())),
            TestHookRecord::Fetched(7, 7, Ok(4000)),
            TestHookRecord::Fetched(8, 8, Err("e4".to_owned())),
            TestHookRecord::Evict(9, 9, EvictResult::Unfetched),
            TestHookRecord::Evict(10, 10, EvictResult::Fetched { size: 5000 }),
            TestHookRecord::Evict(11, 11, EvictResult::Failed),
        ];
        assert_eq!(h1.records(), records,);
        assert_eq!(h2.records(), records,);
    }
}
