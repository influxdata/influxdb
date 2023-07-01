use std::collections::VecDeque;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::dml_payload::IngestOp;

use super::{DmlError, DmlSink};

#[derive(Debug, Default)]
struct MockDmlSinkState {
    calls: Vec<IngestOp>,
    ret: VecDeque<Result<(), DmlError>>,
}

#[derive(Debug, Default)]
pub struct MockDmlSink {
    state: Mutex<MockDmlSinkState>,
}

impl MockDmlSink {
    pub(crate) fn with_apply_return(self, ret: impl Into<VecDeque<Result<(), DmlError>>>) -> Self {
        self.state.lock().ret = ret.into();
        self
    }

    pub(crate) fn get_calls(&self) -> Vec<IngestOp> {
        self.state.lock().calls.clone()
    }
}

#[async_trait]
impl DmlSink for MockDmlSink {
    type Error = DmlError;
    async fn apply(&self, op: IngestOp) -> Result<(), DmlError> {
        let mut state = self.state.lock();
        state.calls.push(op);
        state.ret.pop_front().expect("no mock sink value to return")
    }
}
