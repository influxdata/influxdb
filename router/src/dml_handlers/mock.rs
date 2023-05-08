use std::{collections::VecDeque, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use parking_lot::Mutex;
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// A captured call to a [`MockDmlHandler`], generic over `W`, the captured
/// [`DmlHandler::WriteInput`] type.
#[derive(Debug, Clone)]
pub enum MockDmlHandlerCall<W> {
    Write {
        namespace: String,
        namespace_schema: Arc<NamespaceSchema>,
        write_input: W,
    },
}

#[derive(Debug)]
struct Inner<W> {
    calls: Vec<MockDmlHandlerCall<W>>,
    write_return: VecDeque<Result<(), DmlError>>,
}

impl<W> Default for Inner<W> {
    fn default() -> Self {
        Self {
            calls: Default::default(),
            write_return: Default::default(),
        }
    }
}

impl<W> Inner<W> {
    fn record_call(&mut self, call: MockDmlHandlerCall<W>) {
        self.calls.push(call);
    }
}

#[derive(Debug)]
pub struct MockDmlHandler<W>(Mutex<Inner<W>>);

impl<W> Default for MockDmlHandler<W> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<W> MockDmlHandler<W>
where
    W: Clone,
{
    pub fn with_write_return(self, ret: impl Into<VecDeque<Result<(), DmlError>>>) -> Self {
        self.0.lock().write_return = ret.into();
        self
    }

    pub fn calls(&self) -> Vec<MockDmlHandlerCall<W>> {
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
impl<W> DmlHandler for MockDmlHandler<W>
where
    W: Debug + Send + Sync,
{
    type WriteError = DmlError;
    type WriteInput = W;
    type WriteOutput = ();

    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        write_input: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        record_and_return!(
            self,
            MockDmlHandlerCall::Write {
                namespace: namespace.into(),
                namespace_schema,
                write_input,
            },
            write_return
        )
    }
}
