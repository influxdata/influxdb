#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{any::Any, collections::VecDeque, sync::Arc};

use parking_lot::Mutex;

use observability_deps::tracing::info;

use crate::span::Span;

pub mod ctx;
pub mod span;

/// A TraceCollector is a sink for completed `Span`
pub trait TraceCollector: std::fmt::Debug + Send + Sync {
    /// Exports the specified `Span` for collection by the sink
    fn export(&self, span: Span);

    /// Cast client to [`Any`], useful for downcasting.
    fn as_any(&self) -> &dyn Any;
}

/// A basic trace collector that prints to stdout
#[derive(Debug)]
pub struct LogTraceCollector {}

impl LogTraceCollector {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for LogTraceCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceCollector for LogTraceCollector {
    fn export(&self, span: Span) {
        info!("completed span {:?}", span)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A trace collector that maintains a ring buffer of spans
#[derive(Debug)]
pub struct RingBufferTraceCollector {
    buffer: Mutex<VecDeque<Span>>,
}

impl RingBufferTraceCollector {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn spans(&self) -> Vec<Span> {
        self.buffer.lock().iter().cloned().collect()
    }
}

impl TraceCollector for RingBufferTraceCollector {
    fn export(&self, span: Span) {
        let mut buffer = self.buffer.lock();
        if buffer.len() == buffer.capacity() {
            buffer.pop_front();
        }
        buffer.push_back(span);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<T> TraceCollector for Arc<T>
where
    T: TraceCollector,
{
    fn export(&self, span: Span) {
        (**self).export(span)
    }

    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
}
