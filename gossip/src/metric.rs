//! Metric newtype wrappers for type safety.
//!
//! The metrics are easily confused (they're all counters) so have the compiler
//! check the right ones are being used in the right places.

use metric::U64Counter;

#[derive(Debug, Clone)]
pub(crate) struct SentFrames(metric::U64Counter);

impl SentFrames {
    pub(crate) fn inc(&self, v: usize) {
        self.0.inc(v as u64)
    }
}

#[derive(Debug)]
pub(crate) struct ReceivedFrames(metric::U64Counter);

impl ReceivedFrames {
    pub(crate) fn inc(&self, v: usize) {
        self.0.inc(v as u64)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SentBytes(metric::U64Counter);

impl SentBytes {
    pub(crate) fn inc(&self, v: usize) {
        self.0.inc(v as u64)
    }
}

#[derive(Debug)]
pub(crate) struct ReceivedBytes(metric::U64Counter);

impl ReceivedBytes {
    pub(crate) fn inc(&self, v: usize) {
        self.0.inc(v as u64)
    }
}

pub(crate) fn new_metrics(
    metrics: &metric::Registry,
) -> (SentFrames, ReceivedFrames, SentBytes, ReceivedBytes) {
    let metric_frames = metrics.register_metric::<U64Counter>(
        "gossip_frames",
        "number of frames sent/received by this node",
    );
    let metric_bytes = metrics
        .register_metric::<U64Counter>("gossip_bytes", "sum of bytes sent/received by this node");

    (
        SentFrames(metric_frames.recorder(&[("direction", "sent")])),
        ReceivedFrames(metric_frames.recorder(&[("direction", "received")])),
        SentBytes(metric_bytes.recorder(&[("direction", "sent")])),
        ReceivedBytes(metric_bytes.recorder(&[("direction", "received")])),
    )
}
