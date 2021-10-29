use data_types::sequence::Sequence;
use time::Time;
use trace::ctx::SpanContext;

/// Metadata information about a write
#[derive(Debug, Default, Clone)]
pub struct WriteMeta {
    /// The sequence number associated with this write
    sequence: Option<Sequence>,

    /// When this write was ingested into the write buffer
    producer_ts: Option<Time>,

    // Optional span context associated w/ this write
    span_ctx: Option<SpanContext>,
}

impl WriteMeta {
    pub fn new(
        sequence: Option<Sequence>,
        producer_ts: Option<Time>,
        span_ctx: Option<SpanContext>,
    ) -> Self {
        Self {
            sequence,
            producer_ts,
            span_ctx,
        }
    }

    pub fn sequence(&self) -> Option<&Sequence> {
        self.sequence.as_ref()
    }
}
