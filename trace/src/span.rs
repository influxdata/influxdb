use std::borrow::Cow;
use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::ctx::SpanContext;

#[derive(Debug, Copy, Clone)]
pub enum SpanStatus {
    Unknown,
    Ok,
    Err,
}

/// A `Span` is a representation of a an interval of time spent performing some operation
///
/// A `Span` has a name, metadata, a start and end time and a unique ID. Additionally they
/// have relationships with other Spans that together comprise a Trace
///
///
#[derive(Debug, Clone)]
pub struct Span {
    pub name: Cow<'static, str>,

    pub ctx: SpanContext,

    pub start: Option<DateTime<Utc>>,

    pub end: Option<DateTime<Utc>>,

    pub status: SpanStatus,

    pub metadata: HashMap<Cow<'static, str>, MetaValue>,

    pub events: Vec<SpanEvent>,
}

impl Span {
    /// Record an event on this `Span`
    pub fn event(&mut self, meta: impl Into<Cow<'static, str>>) {
        let event = SpanEvent {
            time: Utc::now(),
            msg: meta.into(),
        };
        self.events.push(event)
    }

    /// Record an error on this `Span`
    pub fn error(&mut self, meta: impl Into<Cow<'static, str>>) {
        self.event(meta);
        self.status = SpanStatus::Err;
    }

    /// Exports this `Span` to its registered collector if any
    pub fn export(mut self) {
        if let Some(collector) = self.ctx.collector.take() {
            collector.export(self)
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub time: DateTime<Utc>,

    pub msg: Cow<'static, str>,
}

/// Values that can be stored in a Span's metadata and events
#[derive(Debug, Clone)]
pub enum MetaValue {
    String(Cow<'static, str>),
    Float(f64),
    Int(i64),
}

impl From<&'static str> for MetaValue {
    fn from(v: &'static str) -> Self {
        Self::String(Cow::Borrowed(v))
    }
}

impl From<String> for MetaValue {
    fn from(v: String) -> Self {
        Self::String(Cow::Owned(v))
    }
}

impl From<f64> for MetaValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<i64> for MetaValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

/// `SpanRecorder` is a utility for instrumenting code that produces `Span`
///
/// If a `SpanRecorder` is created from a `Span` it will update the start timestamp
/// of the span on creation, and on Drop will set the finish time and call `Span::export`
///
/// If not created with a `Span`, e.g. this request is not being sampled, all operations
/// called on this `SpanRecorder` will be a no-op
#[derive(Debug, Default)]
pub struct SpanRecorder {
    span: Option<Span>,
}

impl<'a> SpanRecorder {
    pub fn new(mut span: Option<Span>) -> Self {
        if let Some(span) = span.as_mut() {
            span.start = Some(Utc::now());
        }

        Self { span }
    }

    /// Record an event on the contained `Span` if any
    pub fn event(&mut self, meta: impl Into<Cow<'static, str>>) {
        if let Some(span) = self.span.as_mut() {
            span.event(meta)
        }
    }

    /// Record an error on the contained `Span` if any
    pub fn error(&mut self, meta: impl Into<Cow<'static, str>>) {
        if let Some(span) = self.span.as_mut() {
            span.error(meta)
        }
    }

    /// Take the contents of this recorder returning a new recorder
    ///
    /// From this point on `self` will behave as if it were created with no span
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// If this `SpanRecorder` has a `Span`, creates a new child of that `Span` and
    /// returns a `SpanRecorder` for it. Otherwise returns an empty `SpanRecorder`
    pub fn child(&self, name: &'static str) -> Self {
        match &self.span {
            Some(span) => Self::new(Some(span.ctx.child(name))),
            None => Self::new(None),
        }
    }
}

impl<'a> Drop for SpanRecorder {
    fn drop(&mut self) {
        if let Some(mut span) = self.span.take() {
            let now = Utc::now();

            // SystemTime is not monotonic so must also check min
            span.start = Some(match span.start {
                Some(a) => a.min(now),
                None => now,
            });

            span.end = Some(match span.end {
                Some(a) => a.max(now),
                None => now,
            });

            span.export()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU128, NonZeroU64};
    use std::sync::Arc;

    use crate::ctx::{SpanId, TraceId};
    use crate::{RingBufferTraceCollector, TraceCollector};

    use super::*;

    fn make_span(collector: Arc<dyn TraceCollector>) -> Span {
        Span {
            name: "foo".into(),
            ctx: SpanContext {
                trace_id: TraceId(NonZeroU128::new(23948923).unwrap()),
                parent_span_id: None,
                span_id: SpanId(NonZeroU64::new(3498394).unwrap()),
                collector: Some(collector),
            },
            start: None,
            end: None,
            status: SpanStatus::Unknown,
            metadata: Default::default(),
            events: vec![],
        }
    }

    #[test]
    fn test_span() {
        let collector = Arc::new(RingBufferTraceCollector::new(5));

        let span = make_span(Arc::<RingBufferTraceCollector>::clone(&collector));

        assert_eq!(collector.spans().len(), 0);

        span.export();

        // Should publish span
        let spans = collector.spans();
        assert_eq!(spans.len(), 1);
    }

    #[test]
    fn test_entered_span() {
        let collector = Arc::new(RingBufferTraceCollector::new(5));

        let span = make_span(Arc::<RingBufferTraceCollector>::clone(&collector));

        let recorder = SpanRecorder::new(Some(span));

        std::thread::sleep(std::time::Duration::from_millis(100));

        std::mem::drop(recorder);

        // Span should have been published on drop with set spans
        let spans = collector.spans();
        assert_eq!(spans.len(), 1);

        let span = &spans[0];

        assert!(span.start.is_some());
        assert!(span.end.is_some());
        assert!(span.start.unwrap() < span.end.unwrap());
    }
}
