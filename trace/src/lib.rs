#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use std::collections::VecDeque;

use parking_lot::Mutex;

use observability_deps::tracing::info;

use crate::span::Span;

pub mod ctx;
pub mod span;

/// A TraceCollector is a sink for completed `Span`
pub trait TraceCollector: std::fmt::Debug + Send + Sync {
    fn export(&self, span: Span);
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
}
