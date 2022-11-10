use super::Sharder;
use data_types::{DeletePredicate, NamespaceName};
use mutable_batch::MutableBatch;
use parking_lot::Mutex;
use std::{collections::VecDeque, fmt::Debug, sync::Arc};

#[derive(Debug, Clone)]
pub enum MockSharderPayload {
    MutableBatch(MutableBatch),
    DeletePredicate(DeletePredicate),
}

impl MockSharderPayload {
    pub fn mutable_batch(&self) -> &MutableBatch {
        match self {
            Self::MutableBatch(v) => v,
            _ => panic!("payload is not a mutable batch"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockSharderCall {
    pub table_name: String,
    pub namespace: String,
    pub payload: MockSharderPayload,
}

#[derive(Debug, Default)]
struct Inner<T> {
    calls: Vec<MockSharderCall>,
    shard_return: VecDeque<T>,
}

impl<T> Inner<T> {
    fn record_call(&mut self, call: MockSharderCall) {
        self.calls.push(call);
    }
}

#[derive(Debug)]
pub struct MockSharder<T>(Mutex<Inner<T>>);

impl<T> Default for MockSharder<T> {
    fn default() -> Self {
        Self(Mutex::new(Inner {
            calls: Default::default(),
            shard_return: VecDeque::new(),
        }))
    }
}

impl<T> MockSharder<T> {
    /// Return the values specified in `ret` in sequence for calls to `shard`,
    /// starting from the front.
    pub fn with_return(self, ret: impl Into<VecDeque<T>>) -> Self {
        self.0.lock().shard_return = ret.into();
        self
    }

    pub fn calls(&self) -> Vec<MockSharderCall> {
        self.0.lock().calls.clone()
    }
}

impl<T> Sharder<MutableBatch> for Arc<MockSharder<T>>
where
    T: Debug + Send + Sync,
{
    type Item = T;

    fn shard(
        &self,
        table: &str,
        namespace: &NamespaceName<'_>,
        payload: &MutableBatch,
    ) -> Self::Item {
        let mut guard = self.0.lock();
        guard.record_call(MockSharderCall {
            table_name: table.to_string(),
            namespace: namespace.to_string(),
            payload: MockSharderPayload::MutableBatch(payload.clone()),
        });
        guard
            .shard_return
            .pop_front()
            .expect("no shard mock value to return")
    }
}

impl<T> Sharder<DeletePredicate> for Arc<MockSharder<T>>
where
    T: Debug + Send + Sync,
{
    type Item = Vec<T>;

    fn shard(
        &self,
        table: &str,
        namespace: &NamespaceName<'_>,
        payload: &DeletePredicate,
    ) -> Self::Item {
        let mut guard = self.0.lock();
        guard.record_call(MockSharderCall {
            table_name: table.to_string(),
            namespace: namespace.to_string(),
            payload: MockSharderPayload::DeletePredicate(payload.clone()),
        });
        vec![guard
            .shard_return
            .pop_front()
            .expect("no shard mock value to return")]
    }
}
