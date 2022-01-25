use std::{collections::VecDeque, fmt::Debug, sync::Arc};

use parking_lot::Mutex;

use super::Sharder;

#[derive(Debug, Clone)]
pub struct MockSharderCall<P> {
    pub table_name: String,
    pub namespace: String,
    pub payload: P,
}

#[derive(Debug, Default)]
struct Inner<T, P> {
    calls: Vec<MockSharderCall<P>>,
    shard_return: VecDeque<T>,
}

impl<T, P> Inner<T, P> {
    fn record_call(&mut self, call: MockSharderCall<P>) {
        self.calls.push(call);
    }
}

#[derive(Debug)]
pub struct MockSharder<T, P>(Mutex<Inner<T, P>>);

impl<T, P> Default for MockSharder<T, P> {
    fn default() -> Self {
        Self(Mutex::new(Inner {
            calls: Default::default(),
            shard_return: VecDeque::new(),
        }))
    }
}

impl<T, P> MockSharder<T, P>
where
    P: Clone,
{
    /// Return the values specified in `ret` in sequence for calls to `shard`,
    /// starting from the front.
    ///
    /// # Memory Leak
    ///
    /// Each call to `shard` leaks the memory of the `T` it returns.
    pub fn with_return(self, ret: impl Into<VecDeque<T>>) -> Self {
        self.0.lock().shard_return = ret.into();
        self
    }

    pub fn calls(&self) -> Vec<MockSharderCall<P>> {
        self.0.lock().calls.clone()
    }
}

impl<T, P> Sharder<P> for Arc<MockSharder<T, P>>
where
    P: Debug + Send + Sync + Clone,
    T: Debug + Send + Sync,
{
    type Item = T;

    fn shard(
        &self,
        table: &str,
        namespace: &data_types::DatabaseName<'_>,
        payload: &P,
    ) -> &Self::Item {
        let mut guard = self.0.lock();
        guard.record_call(MockSharderCall {
            table_name: table.to_string(),
            namespace: namespace.to_string(),
            payload: payload.clone(),
        });
        Box::leak(Box::new(
            guard
                .shard_return
                .pop_front()
                .expect("no shard mock value to return"),
        ))
    }
}
