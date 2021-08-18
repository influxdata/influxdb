use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

use observability_deps::tracing::{error, info, warn};
use opentelemetry::sdk::export::trace::{ExportResult, SpanData, SpanExporter};

use crate::ctx::{SpanContext, SpanId, TraceId};
use crate::span::{MetaValue, SpanEvent, SpanStatus};
use crate::{span::Span, TraceCollector};

/// Size of the exporter buffer
const CHANNEL_SIZE: usize = 1000;

/// Maximum number of events that can be associated with a span
const MAX_EVENTS: u32 = 10;

/// Maximum number of attributes that can be associated with a span
const MAX_ATTRIBUTES: u32 = 100;

/// `OtelExporter` wraps a opentelemetry SpanExporter and sinks spans to it
///
/// In order to do this it spawns a background worker that pulls messages
/// of a queue and writes them to opentelemetry. If this worker cannot keep
/// up, and this queue fills up, spans will be dropped and warnings logged
#[derive(Debug)]
pub struct OtelExporter {
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    sender: tokio::sync::mpsc::Sender<SpanData>,

    shutdown: CancellationToken,
}

impl OtelExporter {
    /// Creates a new `OtelExporter`
    pub fn new<T: SpanExporter + 'static>(exporter: T) -> Self {
        let shutdown = CancellationToken::new();
        let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);

        let handle = tokio::spawn(background_worker(shutdown.clone(), exporter, receiver));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self {
            join,
            shutdown,
            sender,
        }
    }

    /// Triggers shutdown of this `OtelExporter`
    pub fn shutdown(&self) {
        info!("otel exporter shutting down");
        self.shutdown.cancel()
    }

    /// Waits for the background worker of OtelExporter to finish
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        self.join.clone()
    }
}

impl TraceCollector for OtelExporter {
    fn export(&self, span: Span) {
        use mpsc::error::TrySendError;

        match self.sender.try_send(span.into()) {
            Ok(_) => {
                //TODO: Increment some metric
            }
            Err(TrySendError::Full(_)) => {
                warn!("exporter cannot keep up, dropping spans")
            }
            Err(TrySendError::Closed(_)) => {
                warn!("background worker shutdown")
            }
        }
    }
}

async fn background_worker<T: SpanExporter + 'static>(
    shutdown: CancellationToken,
    exporter: T,
    receiver: mpsc::Receiver<SpanData>,
) {
    tokio::select! {
        _ = exporter_loop(exporter, receiver) => {
            // Don't expect this future to complete
            error!("otel exporter loop completed")
        }
        _ = shutdown.cancelled() => {}
    }
    info!("otel exporter shut down")
}

/// An opentelemetry::SpanExporter that sinks writes to a tokio mpsc channel.
///
/// Intended for testing ONLY
///
/// Note: There is a similar construct in opentelemetry behind the testing feature
/// flag, but enabling this brings in a large number of additional dependencies and
/// so we just implement our own version
#[derive(Debug)]
pub struct TestOtelExporter {
    channel: mpsc::Sender<SpanData>,
}

impl TestOtelExporter {
    pub fn new(channel: mpsc::Sender<SpanData>) -> Self {
        Self { channel }
    }
}

#[async_trait]
impl SpanExporter for TestOtelExporter {
    async fn export(&mut self, batch: Vec<SpanData>) -> ExportResult {
        for span in batch {
            self.channel.send(span).await.expect("channel closed")
        }
        Ok(())
    }
}

async fn exporter_loop<T: SpanExporter + 'static>(
    mut exporter: T,
    mut receiver: tokio::sync::mpsc::Receiver<SpanData>,
) {
    while let Some(span) = receiver.recv().await {
        // TODO: Batch export spans
        if let Err(e) = exporter.export(vec![span]).await {
            error!(%e, "error exporting span")
        }
    }

    warn!("sender-side of jaeger exporter dropped without waiting for shut down")
}

impl From<Span> for SpanData {
    fn from(span: Span) -> Self {
        use opentelemetry::sdk::trace::{EvictedHashMap, EvictedQueue};
        use opentelemetry::sdk::InstrumentationLibrary;
        use opentelemetry::trace::{SpanId, SpanKind};
        use opentelemetry::{Key, KeyValue};

        let parent_span_id = match span.ctx.parent_span_id {
            Some(id) => id.into(),
            None => SpanId::invalid(),
        };

        let mut ret = Self {
            span_context: (&span.ctx).into(),
            parent_span_id,
            span_kind: SpanKind::Server,
            name: span.name,
            start_time: span.start.map(Into::into).unwrap_or(std::time::UNIX_EPOCH),
            end_time: span.end.map(Into::into).unwrap_or(std::time::UNIX_EPOCH),
            attributes: EvictedHashMap::new(MAX_ATTRIBUTES, 0),
            events: EvictedQueue::new(MAX_EVENTS),
            links: EvictedQueue::new(0),
            status_code: span.status.into(),
            status_message: Default::default(),
            resource: None,
            instrumentation_lib: InstrumentationLibrary::new("iox-trace", None),
        };

        ret.events.extend(span.events.into_iter().map(Into::into));
        for (key, value) in span.metadata {
            let key = match key {
                Cow::Owned(key) => Key::new(key),
                Cow::Borrowed(key) => Key::new(key),
            };

            ret.attributes.insert(KeyValue::new(key, value))
        }
        ret
    }
}

impl<'a> From<&'a SpanContext> for opentelemetry::trace::SpanContext {
    fn from(ctx: &'a SpanContext) -> Self {
        Self::new(
            ctx.trace_id.into(),
            ctx.span_id.into(),
            Default::default(),
            false,
            Default::default(),
        )
    }
}

impl From<SpanEvent> for opentelemetry::trace::Event {
    fn from(event: SpanEvent) -> Self {
        Self {
            name: event.msg,
            timestamp: event.time.into(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }
    }
}

impl From<SpanStatus> for opentelemetry::trace::StatusCode {
    fn from(status: SpanStatus) -> Self {
        match status {
            SpanStatus::Unknown => Self::Unset,
            SpanStatus::Ok => Self::Ok,
            SpanStatus::Err => Self::Error,
        }
    }
}

impl From<SpanId> for opentelemetry::trace::SpanId {
    fn from(id: SpanId) -> Self {
        Self::from_u64(id.0.get())
    }
}

impl From<TraceId> for opentelemetry::trace::TraceId {
    fn from(id: TraceId) -> Self {
        Self::from_u128(id.0.get())
    }
}

impl From<MetaValue> for opentelemetry::Value {
    fn from(v: MetaValue) -> Self {
        match v {
            MetaValue::String(v) => Self::String(v),
            MetaValue::Float(v) => Self::F64(v),
            MetaValue::Int(v) => Self::I64(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use opentelemetry::{Key, Value};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn test_conversion() {
        let root = SpanContext {
            trace_id: TraceId::new(232345).unwrap(),
            parent_span_id: Some(SpanId::new(2484).unwrap()),
            span_id: SpanId::new(2343).unwrap(),
            collector: None,
        };

        let mut span = root.child("foo");
        span.metadata.insert("string".into(), "bar".into());
        span.metadata.insert("float".into(), 3.32.into());
        span.metadata.insert("int".into(), 5.into());

        span.events.push(SpanEvent {
            time: Utc.timestamp_nanos(1230),
            msg: "event".into(),
        });
        span.status = SpanStatus::Ok;

        span.start = Some(Utc.timestamp_nanos(1000));
        span.end = Some(Utc.timestamp_nanos(2000));

        let span_data: SpanData = span.clone().into();

        assert_eq!(
            span_data.span_context.span_id().to_u64(),
            span.ctx.span_id.get()
        );
        assert_eq!(
            span_data.span_context.trace_id().to_u128(),
            span.ctx.trace_id.get()
        );
        assert_eq!(
            span_data.parent_span_id.to_u64(),
            span.ctx.parent_span_id.unwrap().get()
        );
        assert_eq!(
            span_data.start_time,
            UNIX_EPOCH + Duration::from_nanos(1000)
        );
        assert_eq!(span_data.end_time, UNIX_EPOCH + Duration::from_nanos(2000));

        let events: Vec<_> = span_data.events.iter().collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name.as_ref(), "event");
        assert_eq!(events[0].timestamp, UNIX_EPOCH + Duration::from_nanos(1230));
        assert_eq!(events[0].attributes.len(), 0);

        assert_eq!(
            span_data
                .attributes
                .get(&Key::from_static_str("string"))
                .unwrap()
                .clone(),
            Value::String("bar".into())
        );
        assert_eq!(
            span_data
                .attributes
                .get(&Key::from_static_str("float"))
                .unwrap()
                .clone(),
            Value::F64(3.32)
        );
        assert_eq!(
            span_data
                .attributes
                .get(&Key::from_static_str("int"))
                .unwrap()
                .clone(),
            Value::I64(5)
        );
    }

    #[tokio::test]
    async fn test_exporter() {
        let (sender, mut receiver) = mpsc::channel(10);
        let exporter = OtelExporter::new(TestOtelExporter::new(sender));

        assert!(exporter.join().now_or_never().is_none());

        let root = SpanContext {
            trace_id: TraceId::new(232345).unwrap(),
            parent_span_id: None,
            span_id: SpanId::new(2343).unwrap(),
            collector: None,
        };
        let s1 = root.child("foo");
        let s2 = root.child("bar");

        exporter.export(s1.clone());
        exporter.export(s2.clone());
        exporter.export(s2.clone());

        let r1 = receiver.recv().await.unwrap();
        let r2 = receiver.recv().await.unwrap();
        let r3 = receiver.recv().await.unwrap();

        exporter.shutdown();
        exporter.join().await.unwrap();

        // Should not be fatal despite exporter having been shutdown
        exporter.export(s2.clone());

        assert_eq!(root.span_id.get(), r1.parent_span_id.to_u64());
        assert_eq!(s1.ctx.span_id.get(), r1.span_context.span_id().to_u64());
        assert_eq!(s1.ctx.trace_id.get(), r1.span_context.trace_id().to_u128());

        assert_eq!(root.span_id.get(), r2.parent_span_id.to_u64());
        assert_eq!(s2.ctx.span_id.get(), r2.span_context.span_id().to_u64());
        assert_eq!(s2.ctx.trace_id.get(), r2.span_context.trace_id().to_u128());

        assert_eq!(root.span_id.get(), r3.parent_span_id.to_u64());
        assert_eq!(s2.ctx.span_id.get(), r3.span_context.span_id().to_u64());
        assert_eq!(s2.ctx.trace_id.get(), r3.span_context.trace_id().to_u128());
    }
}
