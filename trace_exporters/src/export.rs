use std::sync::Arc;

use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use tokio::sync::mpsc;
use tokio::task::JoinError;

use observability_deps::tracing::{error, info, warn};
use trace::{span::Span, TraceCollector};

/// Size of the exporter buffer
const CHANNEL_SIZE: usize = 1000;

/// An `AsyncExport` is a batched async version of `trace::TraceCollector`
#[async_trait]
pub trait AsyncExport: Send + 'static {
    async fn export(&mut self, span: Vec<Span>);
}

/// `AsyncExporter` wraps a `AsyncExport` and sinks spans to it
///
/// In order to do this it spawns a background worker that pulls messages
/// off a queue and writes them to the `AsyncExport`.
///
/// If this worker cannot keep up, and this queue fills up, spans will
/// be dropped and warnings logged
///
/// Note: Currently this does not batch spans (#2392)
#[derive(Debug)]
pub struct AsyncExporter {
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    /// Communication queue with the background worker
    ///
    /// Sending None triggers termination
    sender: tokio::sync::mpsc::Sender<Option<Span>>,
}

impl AsyncExporter {
    /// Creates a new `AsyncExporter`
    pub fn new<T: AsyncExport>(collector: T) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);

        let handle = tokio::spawn(background_worker(collector, receiver));
        let join = handle.map_err(Arc::new).boxed().shared();

        Self { join, sender }
    }

    /// Triggers shutdown of this `AsyncExporter` and waits until all in-flight
    /// spans have been published to the `AsyncExport`
    pub async fn drain(&self) -> Result<(), Arc<JoinError>> {
        info!("batched exporter shutting down");
        let _ = self.sender.send(None).await;
        self.join.clone().await
    }
}

impl TraceCollector for AsyncExporter {
    fn export(&self, span: Span) {
        use mpsc::error::TrySendError;
        match self.sender.try_send(Some(span)) {
            Ok(_) => {
                //TODO: Increment some metric (#2613)
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

async fn background_worker<T: AsyncExport>(
    mut exporter: T,
    mut receiver: mpsc::Receiver<Option<Span>>,
) {
    loop {
        match receiver.recv().await {
            Some(Some(span)) => exporter.export(vec![span]).await,
            Some(None) => {
                info!("async exporter shut down");
                break;
            }
            None => {
                error!("sender-side of async exporter dropped without waiting for shut down");
                break;
            }
        }
    }
}

/// An `AsyncExporter` that sinks writes to a tokio mpsc channel.
///
/// Intended for testing ONLY
///
#[derive(Debug)]
pub struct TestAsyncExporter {
    channel: mpsc::Sender<Span>,
}

impl TestAsyncExporter {
    pub fn new(channel: mpsc::Sender<Span>) -> Self {
        Self { channel }
    }
}

#[async_trait]
impl AsyncExport for TestAsyncExporter {
    async fn export(&mut self, batch: Vec<Span>) {
        for span in batch {
            self.channel.send(span).await.expect("channel closed")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trace::ctx::SpanContext;

    #[tokio::test]
    async fn test_exporter() {
        let (sender, mut receiver) = mpsc::channel(10);
        let exporter = AsyncExporter::new(TestAsyncExporter::new(sender));

        let root = SpanContext::new(Arc::new(trace::LogTraceCollector::new()));
        let s1 = root.child("foo");
        let s2 = root.child("bar");

        exporter.export(s1.clone());
        exporter.export(s2.clone());
        exporter.export(s2.clone());

        // Drain should wait for all published spans to be flushed
        exporter.drain().await.unwrap();

        let r1 = receiver.recv().await.unwrap();
        let r2 = receiver.recv().await.unwrap();
        let r3 = receiver.recv().await.unwrap();

        // Should not be fatal despite exporter having been shutdown
        exporter.export(s2.clone());

        assert_eq!(root.span_id.get(), r1.ctx.parent_span_id.unwrap().get());
        assert_eq!(s1.ctx.span_id.get(), r1.ctx.span_id.get());
        assert_eq!(s1.ctx.trace_id.get(), r1.ctx.trace_id.get());

        assert_eq!(root.span_id.get(), r2.ctx.parent_span_id.unwrap().get());
        assert_eq!(s2.ctx.span_id.get(), r2.ctx.span_id.get());
        assert_eq!(s2.ctx.trace_id.get(), r2.ctx.trace_id.get());

        assert_eq!(root.span_id.get(), r3.ctx.parent_span_id.unwrap().get());
        assert_eq!(s2.ctx.span_id.get(), r3.ctx.span_id.get());
        assert_eq!(s2.ctx.trace_id.get(), r3.ctx.trace_id.get());
    }
}
