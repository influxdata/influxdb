use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use dml::DmlOperation;
use mutable_batch_lp::PayloadStatistics;
use parking_lot::Mutex;

use super::{DmlError, DmlHandler};

#[derive(Debug, Clone)]
pub enum MockDmlHandlerCall {
    Dispatch {
        db_name: String,
        op: DmlOperation,
        payload_stats: PayloadStatistics,
        body_len: usize,
    },
}

#[derive(Debug, Default)]
struct Inner {
    calls: Vec<MockDmlHandlerCall>,
    dispatch_return: VecDeque<Result<(), DmlError>>,
}

impl Inner {
    fn record_call(&mut self, call: MockDmlHandlerCall) {
        self.calls.push(call);
    }
}

#[derive(Debug, Default)]
pub struct MockDmlHandler(Mutex<Inner>);

impl MockDmlHandler {
    pub fn with_dispatch_return(self, ret: impl Into<VecDeque<Result<(), DmlError>>>) -> Self {
        self.0.lock().dispatch_return = ret.into();
        self
    }

    pub fn calls(&self) -> Vec<MockDmlHandlerCall> {
        self.0.lock().calls.clone()
    }
}

/// Mock helper to record a call and return the pre-configured value.
///
/// Pushes `$call` to call record, popping `self.state.$return` and returning it
/// to the caller. If no value exists, the pop attempt causes a panic.
macro_rules! record_and_return {
    ($self:ident, $call:expr, $return:ident) => {{
        let mut guard = $self.0.lock();
        guard.record_call($call);
        guard.$return.pop_front().expect("no mock value to return")
    }};
}

#[async_trait]
impl DmlHandler for Arc<MockDmlHandler> {
    async fn dispatch<'a>(
        &'a self,
        db_name: impl Into<String> + Send + Sync + 'a,
        op: impl Into<DmlOperation> + Send + Sync + 'a,
        payload_stats: PayloadStatistics,
        body_len: usize,
    ) -> Result<(), DmlError> {
        record_and_return!(
            self,
            MockDmlHandlerCall::Dispatch {
                db_name: db_name.into(),
                op: op.into().clone(),
                payload_stats,
                body_len,
            },
            dispatch_return
        )
    }
}
