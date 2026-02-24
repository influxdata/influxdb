use std::{borrow::Cow, sync::Arc};
use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};

use crate::{TraceCollector, ctx::SpanContext};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
/// The `Span` is the primary interface for instrumenting code that isn't tracing aware.
/// You might accumulate some statistics about library calls, then afterwards create a
/// [`Span`] to record the results.  For example, at this time Data Fusion doesn't support
/// tracing, so we generate a [`Span`] after calling DF to trace the details of the DF call.
///
/// Example:
///  ```compile_fail
///   # use trace::span::Span;
///   # use trace::span::SpanRecorder;
///
///   async fn my_function(mut recorder: SpanRecorder) {
///       // We're about to call a library that doesn't support tracing, but we want to record
///       // details about the library's work. So we create a span from the recorder.
///       let span = recorder.child_span("data_fusion");
///
///       // Call library that doesn't support tracing, but returns detailed data about its work.
///       let plan = components
///            .df_planner
///            .plan(&plan_ir, Arc::clone(partition_info))
///            .await?;
///        .....
///
///        // Now populate the span with details of the library's work
///        if let Some(span) = &span {
///            // see iox_query::exec::query_tracing::send_metrics_to_tracing for example implementation.
///            send_metrics_to_tracing(Utc::now(), span, plan.as_ref(), true);
///        };
///   }
///  ```
///
/// See Also: If you are implementing code that is tracing aware, you may prefer to use
/// [`SpanRecorder`] as your primary interface.
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
    /// Create new span with given context and name.
    pub(crate) fn new(name: impl Into<Cow<'static, str>>, ctx: SpanContext) -> Self {
        Self {
            name: name.into(),
            ctx,
            start: None,
            end: None,
            status: SpanStatus::Unknown,
            // assume no metadata by default
            metadata: HashMap::with_capacity(0),
            // assume no events by default
            events: Vec::with_capacity(0),
        }
    }

    /// Create new root span.
    pub fn root(name: impl Into<Cow<'static, str>>, collector: Arc<dyn TraceCollector>) -> Self {
        let ctx = SpanContext::new(collector);
        Self::new(name, ctx)
    }

    /// Record an event on this `Span`
    pub fn event(&mut self, event: SpanEvent) {
        self.events.push(event);
    }

    /// Record success on this `Span` setting the status if it isn't already set
    pub fn ok(&mut self, msg: impl Into<Cow<'static, str>>) {
        self.event(SpanEvent::new(msg));
        self.status(SpanStatus::Ok);
    }

    /// Record an error on this `Span` setting the status if it isn't already set
    pub fn error(&mut self, msg: impl Into<Cow<'static, str>>) {
        self.event(SpanEvent::new(msg));
        self.status(SpanStatus::Err);
    }

    /// Set status of `Span`
    pub fn status(&mut self, status: SpanStatus) {
        if self.status == SpanStatus::Unknown {
            self.status = status;
        }
    }

    /// Exports this `Span` to its registered collector if any
    pub fn export(mut self) {
        if let Some(collector) = self.ctx.collector.take() {
            collector.export(self)
        }
    }

    /// Create a new child span with the specified name
    ///
    /// Note that the created Span will not be emitted
    /// automatically. The caller must explicitly call [`Self::export`].
    ///
    /// See [`SpanRecorder`] for a helper that automatically emits span data.
    pub fn child(&self, name: impl Into<Cow<'static, str>>) -> Self {
        self.ctx.child(name)
    }

    /// Link this span to another context.
    pub fn link(&mut self, other: &SpanContext) {
        self.ctx.links.push((other.trace_id, other.span_id));
    }
}

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub time: DateTime<Utc>,

    pub msg: Cow<'static, str>,

    pub metadata: HashMap<Cow<'static, str>, MetaValue>,
}

impl SpanEvent {
    /// Create new event.
    pub fn new(msg: impl Into<Cow<'static, str>>) -> Self {
        Self {
            time: Utc::now(),
            msg: msg.into(),
            // assume no metadata by default
            metadata: HashMap::with_capacity(0),
        }
    }

    /// Set meta data.
    pub fn set_metadata(&mut self, key: impl Into<Cow<'static, str>>, value: impl Into<MetaValue>) {
        self.metadata.insert(key.into(), value.into());
    }
}

/// Values that can be stored in a Span's metadata and events
#[derive(Debug, Clone, PartialEq)]
pub enum MetaValue {
    String(Cow<'static, str>),
    Float(f64),
    Int(i64),
    Bool(bool),
}

impl MetaValue {
    pub fn string(&self) -> Option<&str> {
        match &self {
            Self::String(s) => Some(s.as_ref()),
            _ => None,
        }
    }
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

impl From<bool> for MetaValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

/// Record a [`Duration`] using a millisecond string representation.
impl From<Duration> for MetaValue {
    fn from(v: Duration) -> Self {
        Self::String(format!("{}ms", v.as_millis()).into())
    }
}

/// Utility for instrumenting code that produces [`Span`].
///
/// If a [`SpanRecorder`] is created from a [`Span`] it will update the start timestamp
/// of the span on creation, and on Drop will set the finish time and call [`Span::export`]
///
/// If not created with a [`Span`], e.g. this request is not being sampled, all operations
/// called on this `SpanRecorder` will be a no-op
///
/// For code we control and can instrument with tracing, [`SpanRecorder`]` is the primary interface
/// to add the tracing.  If you're instrumenting code that is not tracing aware, you may need
/// to use [`Span`] to create traces after the fact.
///
/// Example:
///  ```compile_fail
///   # use trace::span::SpanRecorder;
///
///   pub async fn start_my_operation() {
///       let root_span: Option<Span> = trace_collector
///           .as_ref()
///           .map(|collector| Span::root("compaction", Arc::clone(collector)));
///       let recorder = SpanRecorder::new(root_span);
///
///       // add metadata to the recorder
///       recorder.set_metadata("partition_id", partition_id.get().to_string());
///
///       // Create a child recorder to track a sub-operation
///       do_interesting_work(recorder.child("interesting_work")).await;
///
///       // can create more child recorders for various sub-operations
///       ...
///   }
///
///   async fn do_interesting_work(mut recorder: SpanRecorder) {
///       ...
///       // can add metadata to the recorder to report details of the operation
///       recorder.set_metadata("input_bytes", plan_ir.input_bytes().to_string());
///
///       // can create (gran)child recorders as desired
///       result = do_more_interesting_work(recorder.child("more_interesting_work"));
///
///       recorder.set_metadata("output_bytes", result.bytes().to_string());
///       ...
///   }
///  ```
///
/// Example: Testing
///  ```compile_fail
///     // Create a no-op SpanRecorder to allow testing of a function that uses a SpanRecorder
///     start_my_operation(SpanRecorder::new(None)).await;
///  ```
///
///   You generally shouldn't add timing data to a SpanRecorder, as the start/stop times are
///   automatically recorded for each SpanRecorder.  Instead, consider adding child SpanRecorders
///   to track the timing of various activities.
///
#[derive(Debug, Default)]
pub struct SpanRecorder {
    span: Option<Span>,
}

impl SpanRecorder {
    pub fn new(mut span: Option<Span>) -> Self {
        if let Some(span) = span.as_mut() {
            span.start = Some(Utc::now());
        }

        Self { span }
    }

    /// Set meta data on the [`Span`], if any.
    pub fn set_metadata(&mut self, key: impl Into<Cow<'static, str>>, value: impl Into<MetaValue>) {
        if let Some(span) = self.span.as_mut() {
            span.metadata.insert(key.into(), value.into());
        }
    }

    /// Returns mutable access to the inner [`Span`] metadata.
    pub fn metadata_mut(&mut self) -> Option<&mut HashMap<Cow<'static, str>, MetaValue>> {
        self.span.as_mut().map(|v| &mut v.metadata)
    }

    /// Record an event on the contained `Span` if any
    pub fn event(&mut self, event: SpanEvent) {
        if let Some(span) = self.span.as_mut() {
            span.event(event);
        }
    }

    /// Record an event on the inner `Span` with the specified `msg`.
    pub fn event_msg(&mut self, msg: impl Into<Cow<'static, str>>) {
        if let Some(s) = &mut self.span {
            s.event(SpanEvent::new(msg));
        };
    }

    /// Record success on the contained `Span` if any
    pub fn ok(&mut self, meta: impl Into<Cow<'static, str>>) {
        if let Some(span) = self.span.as_mut() {
            span.ok(meta)
        }
    }

    /// Record an error on the contained `Span` if any
    pub fn error(&mut self, meta: impl Into<Cow<'static, str>>) {
        if let Some(span) = self.span.as_mut() {
            span.error(meta)
        }
    }

    /// Set status of contained `Span` if any
    pub fn status(&mut self, status: SpanStatus) {
        if let Some(span) = self.span.as_mut() {
            span.status(status);
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
    pub fn child(&self, name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(self.child_span(name))
    }

    /// Return a reference to the span contained in this SpanRecorder,
    /// or None if there is no active span
    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    /// Return a child span of the specified name, if this SpanRecorder
    /// has an active span, `None` otherwise.
    pub fn child_span(&self, name: impl Into<Cow<'static, str>>) -> Option<Span> {
        self.span.as_ref().map(|span| span.child(name))
    }

    /// Link this span to another context.
    pub fn link(&mut self, other: &SpanContext) {
        if let Some(span) = self.span.as_mut() {
            span.link(other);
        }
    }
}

/// Helper trait to make spans easier to work with
pub trait SpanExt {
    /// Return a child_span, if that makes sense
    fn child_span(&self, name: &'static str) -> Option<Span>;
}

impl SpanExt for Option<SpanContext> {
    fn child_span(&self, name: &'static str) -> Option<Span> {
        self.as_ref().child_span(name)
    }
}
impl SpanExt for Option<&SpanContext> {
    fn child_span(&self, name: &'static str) -> Option<Span> {
        self.map(|span| span.child(name))
    }
}

impl Drop for SpanRecorder {
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
    use std::sync::Arc;

    use crate::{RingBufferTraceCollector, TraceCollector};

    use super::*;

    fn make_span(collector: Arc<dyn TraceCollector>) -> Span {
        SpanContext::new(collector).child("foo")
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
