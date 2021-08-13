use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use observability_deps::tracing::error;

use crate::ctx::SpanContext;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
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
/// On Drop a `Span` is published to the registered collector
///
#[derive(Debug, Serialize, Deserialize)]
pub struct Span<'a> {
    pub name: &'a str,

    //#[serde(flatten)] - https://github.com/serde-rs/json/issues/505
    pub ctx: SpanContext,

    pub start: Option<DateTime<Utc>>,

    pub end: Option<DateTime<Utc>>,

    pub status: SpanStatus,

    #[serde(borrow)]
    pub metadata: HashMap<&'a str, MetaValue<'a>>,

    #[serde(borrow)]
    pub events: Vec<SpanEvent<'a>>,
}

impl<'a> Span<'a> {
    pub fn event(&mut self, meta: impl Into<MetaValue<'a>>) {
        let event = SpanEvent {
            time: Utc::now(),
            msg: meta.into(),
        };
        self.events.push(event)
    }

    pub fn error(&mut self, meta: impl Into<MetaValue<'a>>) {
        self.event(meta);
        self.status = SpanStatus::Err;
    }

    pub fn json(&self) -> String {
        match serde_json::to_string(self) {
            Ok(serialized) => serialized,
            Err(e) => {
                error!(%e, "error serializing span to JSON");
                format!("\"Invalid span: {}\"", e)
            }
        }
    }
}

impl<'a> Drop for Span<'a> {
    fn drop(&mut self) {
        if let Some(collector) = &self.ctx.collector {
            collector.export(self)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent<'a> {
    pub time: DateTime<Utc>,

    #[serde(borrow)]
    pub msg: MetaValue<'a>,
}

/// Values that can be stored in a Span's metadata and events
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetaValue<'a> {
    String(&'a str),
    Float(f64),
    Int(i64),
}

impl<'a> From<&'a str> for MetaValue<'a> {
    fn from(v: &'a str) -> Self {
        Self::String(v)
    }
}

impl<'a> From<f64> for MetaValue<'a> {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl<'a> From<i64> for MetaValue<'a> {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

/// Updates the start and end times on the provided Span
#[derive(Debug)]
pub struct EnteredSpan<'a> {
    span: Span<'a>,
}

impl<'a> Deref for EnteredSpan<'a> {
    type Target = Span<'a>;

    fn deref(&self) -> &Self::Target {
        &self.span
    }
}

impl<'a> DerefMut for EnteredSpan<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.span
    }
}

impl<'a> EnteredSpan<'a> {
    pub fn new(mut span: Span<'a>) -> Self {
        span.start = Some(Utc::now());
        Self { span }
    }
}

impl<'a> Drop for EnteredSpan<'a> {
    fn drop(&mut self) {
        let now = Utc::now();

        // SystemTime is not monotonic so must also check min

        self.span.start = Some(match self.span.start {
            Some(a) => a.min(now),
            None => now,
        });

        self.span.end = Some(match self.span.end {
            Some(a) => a.max(now),
            None => now,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU128, NonZeroU64};
    use std::sync::Arc;

    use chrono::TimeZone;

    use crate::ctx::{SpanId, TraceId};
    use crate::{RingBufferTraceCollector, TraceCollector};

    use super::*;

    fn make_span(collector: Arc<dyn TraceCollector>) -> Span<'static> {
        Span {
            name: "foo",
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

        let mut span = make_span(Arc::<RingBufferTraceCollector>::clone(&collector));

        assert_eq!(
            span.json(),
            r#"{"name":"foo","ctx":{"trace_id":23948923,"parent_span_id":null,"span_id":3498394},"start":null,"end":null,"status":"Unknown","metadata":{},"events":[]}"#
        );

        span.events.push(SpanEvent {
            time: Utc.timestamp_nanos(1000),
            msg: MetaValue::String("this is a test event"),
        });

        assert_eq!(
            span.json(),
            r#"{"name":"foo","ctx":{"trace_id":23948923,"parent_span_id":null,"span_id":3498394},"start":null,"end":null,"status":"Unknown","metadata":{},"events":[{"time":"1970-01-01T00:00:00.000001Z","msg":"this is a test event"}]}"#
        );

        span.metadata.insert("foo", MetaValue::String("bar"));
        span.start = Some(Utc.timestamp_nanos(100));

        assert_eq!(
            span.json(),
            r#"{"name":"foo","ctx":{"trace_id":23948923,"parent_span_id":null,"span_id":3498394},"start":"1970-01-01T00:00:00.000000100Z","end":null,"status":"Unknown","metadata":{"foo":"bar"},"events":[{"time":"1970-01-01T00:00:00.000001Z","msg":"this is a test event"}]}"#
        );

        span.status = SpanStatus::Ok;
        span.ctx.parent_span_id = Some(SpanId(NonZeroU64::new(23493).unwrap()));

        let expected = r#"{"name":"foo","ctx":{"trace_id":23948923,"parent_span_id":23493,"span_id":3498394},"start":"1970-01-01T00:00:00.000000100Z","end":null,"status":"Ok","metadata":{"foo":"bar"},"events":[{"time":"1970-01-01T00:00:00.000001Z","msg":"this is a test event"}]}"#;
        assert_eq!(span.json(), expected);

        std::mem::drop(span);

        // Should publish span
        let spans = collector.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0], expected)
    }

    #[test]
    fn test_entered_span() {
        let collector = Arc::new(RingBufferTraceCollector::new(5));

        let span = make_span(Arc::<RingBufferTraceCollector>::clone(&collector));

        let entered = EnteredSpan::new(span);

        std::thread::sleep(std::time::Duration::from_millis(100));

        std::mem::drop(entered);

        // Span should have been published on drop with set spans
        let spans = collector.spans();
        assert_eq!(spans.len(), 1);

        let span: Span<'_> = serde_json::from_str(spans[0].as_str()).unwrap();

        assert!(span.start.is_some());
        assert!(span.end.is_some());
        assert!(span.start.unwrap() < span.end.unwrap());
    }
}
