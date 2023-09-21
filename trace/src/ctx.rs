use std::borrow::Cow;
use std::num::{NonZeroU128, NonZeroU64};
use std::sync::Arc;

use rand::Rng;

use crate::{span::Span, TraceCollector};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(pub NonZeroU128);

impl TraceId {
    pub fn new(val: u128) -> Option<Self> {
        Some(Self(NonZeroU128::new(val)?))
    }

    pub fn get(self) -> u128 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    /// Link to other spans, can be cross-trace if this span aggregates multiple spans.
    ///
    /// See <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/overview.md#links-between-spans>.
    pub links: Vec<(TraceId, SpanId)>,

    pub collector: Option<Arc<dyn TraceCollector>>,

    /// If we should also sample based on this context (i.e. emit child spans).
    pub sampled: bool,
}

impl SpanContext {
    /// Create a new root span context, sent to `collector`. The
    /// new span context has a random trace_id and span_id, and thus
    /// is not connected to any existing span or trace.
    pub fn new(collector: Arc<dyn TraceCollector>) -> Self {
        Self::new_with_optional_collector(Some(collector))
    }

    /// Same as [`new`](Self::new), but with an optional collector.
    pub fn new_with_optional_collector(collector: Option<Arc<dyn TraceCollector>>) -> Self {
        let mut rng = rand::thread_rng();
        let trace_id: u128 = rng.gen_range(1..u128::MAX);
        let span_id: u64 = rng.gen_range(1..u64::MAX);

        Self {
            trace_id: TraceId(NonZeroU128::new(trace_id).unwrap()),
            parent_span_id: None,
            span_id: SpanId(NonZeroU64::new(span_id).unwrap()),
            links: vec![],
            collector,
            sampled: true,
        }
    }

    /// Creates a new child of the Span described by this TraceContext
    pub fn child(&self, name: impl Into<Cow<'static, str>>) -> Span {
        let ctx = Self {
            trace_id: self.trace_id,
            span_id: SpanId::gen(),
            collector: self.collector.clone(),
            links: Vec::with_capacity(0),
            parent_span_id: Some(self.span_id),
            sampled: self.sampled,
        };
        Span::new(name, ctx)
    }

    /// Return the approximate memory size of the span, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .links
                .iter()
                .map(|(t_id, s_id)| std::mem::size_of_val(t_id) + std::mem::size_of_val(s_id))
                .sum::<usize>()
    }
}

impl PartialEq for SpanContext {
    fn eq(&self, other: &Self) -> bool {
        self.trace_id == other.trace_id
            && self.parent_span_id == other.parent_span_id
            && self.span_id == other.span_id
            && self.links == other.links
            && self.collector.is_some() == other.collector.is_some()
            && self.sampled == other.sampled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::RingBufferTraceCollector;

    #[test]
    fn test_new() {
        // two newly created spans should not have duplicated trace or span ids
        let collector = Arc::new(RingBufferTraceCollector::new(5)) as _;

        let ctx1 = SpanContext::new(Arc::clone(&collector));
        let ctx2 = SpanContext::new(collector);

        assert_ne!(ctx1.trace_id, ctx2.trace_id);
        assert_ne!(ctx1.span_id, ctx2.span_id);
    }

    #[test]
    fn test_partial_eq() {
        let collector_1 = Arc::new(RingBufferTraceCollector::new(5)) as _;
        let collector_2 = Arc::new(RingBufferTraceCollector::new(5)) as _;

        let ctx_ref = SpanContext {
            trace_id: TraceId::new(1).unwrap(),
            parent_span_id: Some(SpanId::new(2).unwrap()),
            span_id: SpanId::new(3).unwrap(),
            links: vec![
                (TraceId::new(4).unwrap(), SpanId::new(5).unwrap()),
                (TraceId::new(6).unwrap(), SpanId::new(7).unwrap()),
            ],
            collector: Some(collector_1),
            sampled: true,
        };

        let ctx = SpanContext { ..ctx_ref.clone() };
        assert_eq!(ctx_ref, ctx);

        let ctx = SpanContext {
            trace_id: TraceId::new(10).unwrap(),
            ..ctx_ref.clone()
        };
        assert_ne!(ctx_ref, ctx);

        let ctx = SpanContext {
            parent_span_id: Some(SpanId::new(10).unwrap()),
            ..ctx_ref.clone()
        };
        assert_ne!(ctx_ref, ctx);

        let ctx = SpanContext {
            span_id: SpanId::new(10).unwrap(),
            ..ctx_ref.clone()
        };
        assert_ne!(ctx_ref, ctx);

        let ctx = SpanContext {
            links: vec![(TraceId::new(4).unwrap(), SpanId::new(5).unwrap())],
            ..ctx_ref.clone()
        };
        assert_ne!(ctx_ref, ctx);

        let ctx = SpanContext {
            collector: None,
            ..ctx_ref.clone()
        };
        assert_ne!(ctx_ref, ctx);

        let ctx = SpanContext {
            collector: Some(collector_2),
            ..ctx_ref.clone()
        };
        assert_eq!(ctx_ref, ctx);

        let ctx = SpanContext {
            sampled: false,
            ..ctx_ref.clone()
        };
        assert_ne!(ctx_ref, ctx);
    }
}
