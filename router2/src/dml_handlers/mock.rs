use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};

use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use parking_lot::Mutex;
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

#[derive(Debug, Clone)]
pub enum MockDmlHandlerCall {
    Write {
        namespace: String,
        batches: HashMap<String, MutableBatch>,
    },
    Delete {
        namespace: String,
        table: String,
        predicate: DeletePredicate,
    },
}

#[derive(Debug, Default)]
struct Inner {
    calls: Vec<MockDmlHandlerCall>,
    write_return: VecDeque<Result<(), DmlError>>,
    delete_return: VecDeque<Result<(), DmlError>>,
}

impl Inner {
    fn record_call(&mut self, call: MockDmlHandlerCall) {
        self.calls.push(call);
    }
}

#[derive(Debug, Default)]
pub struct MockDmlHandler(Mutex<Inner>);

impl MockDmlHandler {
    pub fn with_write_return(self, ret: impl Into<VecDeque<Result<(), DmlError>>>) -> Self {
        self.0.lock().write_return = ret.into();
        self
    }

    pub fn with_delete_return(self, ret: impl Into<VecDeque<Result<(), DmlError>>>) -> Self {
        self.0.lock().delete_return = ret.into();
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
    type WriteError = DmlError;
    type DeleteError = DmlError;
    type WriteInput = HashMap<String, MutableBatch>;

    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError> {
        record_and_return!(
            self,
            MockDmlHandlerCall::Write {
                namespace: namespace.into(),
                batches,
            },
            write_return
        )
    }

    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        record_and_return!(
            self,
            MockDmlHandlerCall::Delete {
                namespace: namespace.into(),
                table: table.into(),
                predicate,
            },
            delete_return
        )
    }
}
