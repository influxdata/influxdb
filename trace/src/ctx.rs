use std::borrow::Cow;
use std::num::{NonZeroU128, NonZeroU64};
use std::sync::Arc;

use rand::Rng;

use crate::{
    span::{Span, SpanStatus},
    TraceCollector,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceId(pub NonZeroU128);

impl TraceId {
    pub fn new(val: u128) -> Option<Self> {
        Some(Self(NonZeroU128::new(val)?))
    }

    pub fn get(self) -> u128 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanId(pub NonZeroU64);

impl SpanId {
    pub fn new(val: u64) -> Option<Self> {
        Some(Self(NonZeroU64::new(val)?))
    }

    pub fn gen() -> Self {
        // Should this be a UUID?
        Self(rand::thread_rng().gen())
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// The immutable context of a `Span`
///
/// Importantly this contains all the information necessary to create a child `Span`
#[derive(Debug, Clone)]
pub struct SpanContext {
    pub trace_id: TraceId,

    pub parent_span_id: Option<SpanId>,

    pub span_id: SpanId,

    pub collector: Option<Arc<dyn TraceCollector>>,
}

impl SpanContext {
    /// Creates a new child of the Span described by this TraceContext
    pub fn child(&self, name: impl Into<Cow<'static, str>>) -> Span {
        Span {
            name: name.into(),
            ctx: Self {
                trace_id: self.trace_id,
                span_id: SpanId::gen(),
                collector: self.collector.clone(),
                parent_span_id: Some(self.span_id),
            },
            start: None,
            end: None,
            status: SpanStatus::Unknown,
            metadata: Default::default(),
            events: Default::default(),
        }
    }
}
