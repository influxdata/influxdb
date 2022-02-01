use std::sync::Arc;

use data_types::sequence::Sequence;
use dml::{DmlMeta, DmlOperation, DmlWrite};
use hashbrown::{hash_map::Entry, HashMap};
use mutable_batch::MutableBatch;
use observability_deps::tracing::debug;
use rskafka::{
    client::producer::aggregator::{self, Aggregator, StatusDeaggregator, TryPush},
    record::Record,
};
use schema::selection::Selection;
use time::{Time, TimeProvider};
use trace::{ctx::SpanContext, span::SpanRecorder, TraceCollector};

use crate::codec::{ContentType, IoxHeaders};

/// Newtype wrapper for tags given back to the aggregator framework.
///
/// We cannot just use a simple `usize` to get the offsets from the produced records because we can have writes for
/// different namespaces open at the same time, so the actual record offset will only be known after the tag has been
/// produced. Internally we use simple lookup table "tag -> record" to solve this issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tag(usize);

/// Aggregate writes of a single namespace.
#[derive(Debug)]
struct WriteAggregator {
    /// Namespace.
    namespace: String,

    /// Data for every table.
    tables: HashMap<String, MutableBatch>,

    /// Span recorder to link spans from incoming writes to aggregated write.
    span_recorder: SpanRecorder,

    /// Tag, so we can later find the offset of the produced record.
    tag: Tag,
}

impl WriteAggregator {
    fn new(write: DmlWrite, collector: Option<Arc<dyn TraceCollector>>, tag: Tag) -> Self {
        // TODO: `.into_tables()` could be helpful
        let tables = write
            .tables()
            .map(|(name, batch)| (name.to_owned(), batch.clone()))
            .collect();
        let mut span_recorder = SpanRecorder::new(
            collector.map(|collector| SpanContext::new(collector).child("write buffer aggregator")),
        );

        if let Some(ctx) = write.meta().span_context() {
            span_recorder.link(ctx);
        }

        Self {
            namespace: write.namespace().to_owned(),
            tables,
            span_recorder,
            tag,
        }
    }

    /// Check if we can push the given write to this aggregator (mostly if the schemas match).
    fn can_push(&self, write: &DmlWrite) -> bool {
        assert_eq!(write.namespace(), self.namespace);

        for (table, batch) in write.tables() {
            if let Some(existing) = self.tables.get(table) {
                match (
                    existing.schema(Selection::All),
                    batch.schema(Selection::All),
                ) {
                    (Ok(schema_a), Ok(schema_b)) if schema_a == schema_b => {}
                    _ => {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Push write to this aggregator.
    ///
    /// The caller MUST call [`can_push`](Self::can_push) beforehand to check if the schemas match.
    fn push(&mut self, write: DmlWrite) {
        assert_eq!(write.namespace(), self.namespace);

        // TODO: `.into_tables()` could be helpful
        for (table, batch) in write.tables() {
            self.tables
                .entry(table.to_owned())
                .and_modify(|existing| {
                    existing
                        .extend_from(batch)
                        .expect("Caller should have checked if schemas are compatible")
                })
                .or_insert_with(|| batch.clone());
        }

        if let Some(ctx) = write.meta().span_context() {
            self.span_recorder.link(ctx);
        }
    }

    /// Flush aggregator to a ready-to-use DML write.
    fn flush(mut self) -> DmlWrite {
        self.span_recorder.ok("aggregated");

        // attach a span if there is at least 1 active link
        let meta = DmlMeta::unsequenced(
            self.span_recorder
                .span()
                .map(|span| (!span.ctx.links.is_empty()).then(|| span.ctx.clone()))
                .flatten(),
        );
        DmlWrite::new(self.namespace, self.tables, meta)
    }

    /// Current estimated size of the aggregated data.
    fn size(&self) -> usize {
        self.namespace.len()
            + self
                .tables
                .iter()
                .map(|(k, v)| std::mem::size_of_val(k) + k.capacity() + v.size())
                .sum::<usize>()
    }
}

/// Inner state of [`DmlAggregator`].
// TODO: in theory we could also fold the deletes into the writes already at this point.
#[derive(Debug, Default)]
struct DmlAggregatorState {
    /// Completed (i.e. aggregated or flushed) operations in correct order.
    completed_ops: Vec<DmlOperation>,

    /// Sum of all sizes of `completed_ops` for fast access.
    completed_size: usize,

    /// Current writes per namespace.
    current_writes: HashMap<String, WriteAggregator>,

    /// Maps tags to record.
    tag_to_record: Vec<usize>,
}

impl DmlAggregatorState {
    /// Current estimated size of all aggregated data.
    fn size(&self) -> usize {
        self.completed_size
            + self
                .current_writes
                .values()
                .map(|agg| agg.size())
                .sum::<usize>()
    }

    /// Reserve so it can be used with [`push_op`](Self::push_op).
    ///
    /// This method takes `tag_to_offset` instead of `self` so we can use it while holding a reference to other struct
    /// members.
    fn reserve_tag(tag_to_offset: &mut Vec<usize>) -> Tag {
        let tag = Tag(tag_to_offset.len());
        tag_to_offset.push(usize::MAX);
        tag
    }

    /// Push given DML operation to completed operations.
    ///
    /// Takes an optional pre-calculated size.
    fn push_op(&mut self, op: DmlOperation, size: Option<usize>, tag: Tag) {
        self.completed_size += size.unwrap_or_else(|| op.size());
        let offset = self.completed_ops.len();
        self.tag_to_record[tag.0] = offset;
        self.completed_ops.push(op);
    }

    /// Flushes write for given namespace to completed operations.
    ///
    /// This is a no-op if no active write exists.
    fn flush_write(&mut self, namespace: &str) {
        if let Some(agg) = self.current_writes.remove(namespace) {
            let tag = agg.tag;
            self.push_op(DmlOperation::Write(agg.flush()), None, tag);
        }
    }

    /// Flushes writes for all namespaces to completed operations in sorted order (by namespace).
    fn flush_writes(&mut self) {
        let mut writes: Vec<_> = self.current_writes.drain().collect();
        writes.sort_by_key(|(k, _v)| k.clone());

        for (_k, agg) in writes {
            let tag = agg.tag;
            self.push_op(DmlOperation::Write(agg.flush()), None, tag);
        }
    }
}

/// Aggregator of [`DmlOperation`].
#[derive(Debug)]
pub struct DmlAggregator {
    /// Optional trace collector.
    collector: Option<Arc<dyn TraceCollector>>,

    /// Database name.
    database_name: String,

    /// Maximum batch size in bytes.
    max_size: usize,

    /// Sequencer ID.
    sequencer_id: u32,

    /// Inner state that will be modified via `try_push` and reset via `flush`.
    state: DmlAggregatorState,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,
}

impl DmlAggregator {
    pub fn new(
        collector: Option<Arc<dyn TraceCollector>>,
        database_name: String,
        max_size: usize,
        sequencer_id: u32,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            collector,
            database_name,
            max_size,
            sequencer_id,
            state: DmlAggregatorState::default(),
            time_provider,
        }
    }
}

impl Aggregator for DmlAggregator {
    type Input = DmlOperation;

    type Tag = Tag;

    type StatusDeaggregator = Deaggregator;

    fn try_push(
        &mut self,
        op: Self::Input,
    ) -> Result<TryPush<Self::Input, Self::Tag>, aggregator::Error> {
        let op_size = op.size();
        if self.state.size() + op_size > self.max_size {
            return Ok(TryPush::NoCapacity(op));
        }

        match op {
            DmlOperation::Write(write) => {
                let tag = match self
                    .state
                    .current_writes
                    .entry(write.namespace().to_string())
                {
                    Entry::Occupied(mut o) => {
                        // Open write aggregator => check if we can push to it.
                        let agg = o.get_mut();

                        if agg.can_push(&write) {
                            // Schemas match => use this aggregator.
                            agg.push(write);
                            agg.tag
                        } else {
                            // Schemas don't match => use new aggregator (the write will likely fail on the ingester
                            // side though).
                            let new_tag =
                                DmlAggregatorState::reserve_tag(&mut self.state.tag_to_record);
                            let mut agg2 = WriteAggregator::new(
                                write,
                                self.collector.as_ref().map(Arc::clone),
                                new_tag,
                            );
                            std::mem::swap(agg, &mut agg2);

                            let flushed_tag = agg2.tag;
                            self.state.push_op(
                                DmlOperation::Write(agg2.flush()),
                                None,
                                flushed_tag,
                            );

                            new_tag
                        }
                    }
                    Entry::Vacant(v) => {
                        // No open write aggregator yet => create one.
                        let tag = DmlAggregatorState::reserve_tag(&mut self.state.tag_to_record);
                        v.insert(WriteAggregator::new(
                            write,
                            self.collector.as_ref().map(Arc::clone),
                            tag,
                        ));
                        tag
                    }
                };

                Ok(TryPush::Aggregated(tag))
            }
            DmlOperation::Delete(_) => {
                // must flush write aggregate to prevent deletes from "bypassing" deletes
                self.state.flush_write(op.namespace());

                let tag = DmlAggregatorState::reserve_tag(&mut self.state.tag_to_record);
                self.state.push_op(op, Some(op_size), tag);
                Ok(TryPush::Aggregated(tag))
            }
        }
    }

    fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), aggregator::Error> {
        let mut state = std::mem::take(&mut self.state);
        state.flush_writes();

        let mut records = Vec::with_capacity(state.completed_ops.len());
        let mut metadata = Vec::with_capacity(state.completed_ops.len());

        for op in state.completed_ops {
            // truncate milliseconds from timestamps because that's what Kafka supports
            let now = op
                .meta()
                .producer_ts()
                .unwrap_or_else(|| self.time_provider.now());

            let timestamp_millis = now.date_time().timestamp_millis();
            let timestamp = Time::from_timestamp_millis(timestamp_millis);

            let headers = IoxHeaders::new(
                ContentType::Protobuf,
                op.meta().span_context().cloned(),
                op.namespace().to_owned(),
            );

            let mut buf = Vec::new();
            crate::codec::encode_operation(&self.database_name, &op, &mut buf)?;
            let buf_len = buf.len();

            let record = Record {
                key: Default::default(),
                value: buf,
                headers: headers
                    .headers()
                    .map(|(k, v)| (k.to_owned(), v.as_bytes().to_vec()))
                    .collect(),
                timestamp: rskafka::time::OffsetDateTime::from_unix_timestamp_nanos(
                    timestamp_millis as i128 * 1_000_000,
                )?,
            };

            debug!(db_name=%self.database_name, partition=self.sequencer_id, size=buf_len, "writing to kafka");

            let kafka_write_size = record.approximate_size();

            metadata.push(Metadata {
                timestamp,
                span_ctx: op.meta().span_context().cloned(),
                kafka_write_size,
            });
            records.push(record);
        }

        Ok((
            records,
            Deaggregator {
                sequencer_id: self.sequencer_id,
                metadata,
                tag_to_record: state.tag_to_record,
            },
        ))
    }
}

/// Metadata that we carry over for each pushed [`DmlOperation`] so we can return a proper [`DmlMeta`].
#[derive(Debug)]
struct Metadata {
    timestamp: Time,
    span_ctx: Option<SpanContext>,
    kafka_write_size: usize,
}

#[derive(Debug)]
pub struct Deaggregator {
    /// Sequencer ID.
    sequencer_id: u32,

    /// Metadata for every record.
    ///
    /// This is NOT per-tag, use `tag_to_record` to map tags to records first.
    metadata: Vec<Metadata>,

    /// Maps tags to records.
    tag_to_record: Vec<usize>,
}

impl StatusDeaggregator for Deaggregator {
    type Status = DmlMeta;

    type Tag = Tag;

    fn deaggregate(
        &self,
        input: &[i64],
        tag: Self::Tag,
    ) -> Result<Self::Status, aggregator::Error> {
        let record = self.tag_to_record[tag.0];

        let offset = input[record];
        let md = &self.metadata[record];

        Ok(DmlMeta::sequenced(
            Sequence::new(self.sequencer_id, offset.try_into()?),
            md.timestamp,
            md.span_ctx.clone(),
            md.kafka_write_size,
        ))
    }
}
