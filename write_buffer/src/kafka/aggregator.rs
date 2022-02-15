use std::sync::Arc;

use data_types::sequence::Sequence;
use dml::{DmlMeta, DmlOperation, DmlWrite};
use hashbrown::{hash_map::Entry, HashMap};
use mutable_batch::MutableBatch;
use observability_deps::tracing::warn;
use rskafka::{
    client::producer::aggregator::{self, Aggregator, StatusDeaggregator, TryPush},
    record::Record,
};
use schema::selection::Selection;
use time::{Time, TimeProvider};
use trace::{
    ctx::SpanContext,
    span::{Span, SpanRecorder},
    TraceCollector,
};

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
    span_recorder: Option<SpanRecorder>,

    /// Tag, so we can later find the offset of the produced record.
    tag: Tag,

    /// Trace collector
    collector: Option<Arc<dyn TraceCollector>>,
}

impl WriteAggregator {
    fn new(write: DmlWrite, collector: Option<Arc<dyn TraceCollector>>, tag: Tag) -> Self {
        let mut span_recorder = None;
        Self::record_span(&mut span_recorder, write.meta().span_context(), &collector);

        Self {
            namespace: write.namespace().to_owned(),
            tables: write.into_tables().collect(),
            span_recorder,
            tag,
            collector,
        }
    }

    /// Fold new trace into existing one
    fn record_span(
        recorder: &mut Option<SpanRecorder>,
        ctx: Option<&SpanContext>,
        collector: &Option<Arc<dyn TraceCollector>>,
    ) {
        match (recorder.as_mut(), ctx) {
            (None, None) => {
                // no existing recorder and no context => nothing to trace
            }
            (Some(_recorder), None) => {
                // existing recorder but no context => just keep the existing recorder
            }
            (None, Some(ctx)) => {
                // got a context but don't have a recorder yet => create a recorder and record span
                let mut recorder_inner =
                    SpanRecorder::new(collector.as_ref().map(|collector| {
                        Span::root("write buffer aggregator", Arc::clone(collector))
                    }));
                recorder_inner.link(ctx);
                *recorder = Some(recorder_inner);
            }
            (Some(recorder), Some(ctx)) => {
                // got context and already has a recorder => just record it
                recorder.link(ctx);
            }
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

        Self::record_span(
            &mut self.span_recorder,
            write.meta().span_context(),
            &self.collector,
        );

        for (table, batch) in write.into_tables() {
            self.tables
                .entry(table)
                .and_modify(|existing| {
                    existing
                        .extend_from(&batch)
                        .expect("Caller should have checked if schemas are compatible")
                })
                .or_insert(batch);
        }
    }

    /// Finalizes span recording.
    fn finalize_span(mut self) {
        if let Some(span_recorder) = self.span_recorder.as_mut() {
            span_recorder.ok("aggregated");
        }
    }

    /// Encode into DML write.
    ///
    /// This copies the inner data.
    fn encode(&self) -> DmlWrite {
        // attach a span if there is at least 1 active link
        let ctx = if let Some(span_recorder) = self.span_recorder.as_ref() {
            span_recorder.span().map(|span| span.ctx.clone())
        } else {
            None
        };

        let meta = DmlMeta::unsequenced(ctx);
        DmlWrite::new(self.namespace.clone(), self.tables.clone(), meta)
    }
}

/// Inner state of [`DmlAggregator`].
// TODO: in theory we could also fold the deletes into the writes already at this point.
#[derive(Debug, Default)]
struct DmlAggregatorState {
    /// Completed (i.e. aggregated or flushed) operations in correct order.
    completed_ops: Vec<(Record, Metadata)>,

    /// Current writes per namespace.
    current_writes: HashMap<String, (WriteAggregator, Record, Metadata)>,

    /// Maps tags to record.
    tag_to_record: Vec<usize>,
}

impl DmlAggregatorState {
    /// Current estimated size of all aggregated data.
    fn size(&self) -> usize {
        self.completed_ops
            .iter()
            .map(|(op, _md)| op.approximate_size())
            .sum::<usize>()
            + self
                .current_writes
                .values()
                .map(|(_agg, record, _md)| record.approximate_size())
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

    /// Push given encoded DML operation to completed operations.
    fn push_op(&mut self, record: Record, md: Metadata, tag: Tag) {
        let offset = self.completed_ops.len();
        self.tag_to_record[tag.0] = offset;
        self.completed_ops.push((record, md));
    }

    /// Flushes write for given namespace to completed operations.
    ///
    /// This is a no-op if no active write exists.
    fn flush_write(&mut self, namespace: &str) {
        if let Some((agg, record, md)) = self.current_writes.remove(namespace) {
            let tag = agg.tag;
            agg.finalize_span();
            self.push_op(record, md, tag);
        }
    }

    /// Flushes writes for all namespaces to completed operations in sorted order (by namespace).
    fn flush_writes(&mut self) {
        let mut writes: Vec<_> = self.current_writes.drain().collect();
        writes.sort_by_key(|(k, _v)| k.clone());

        for (_k, (agg, record, md)) in writes {
            let tag = agg.tag;
            agg.finalize_span();
            self.push_op(record, md, tag);
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
        let (op_record, op_md) =
            encode_operation(&op, &self.database_name, self.time_provider.as_ref())?;
        let op_size = op_record.approximate_size();

        if self.state.size() + op_size > self.max_size {
            if op_size > self.max_size {
                warn!(
                    max_size = self.max_size,
                    op_size, "Got operation that is too large for operator",
                );
            }
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
                        let (agg, record, md) = o.get_mut();

                        if agg.can_push(&write) {
                            // Schemas match => use this aggregator.
                            agg.push(write);

                            // update cached record
                            let (record_new, md_new) = encode_operation(
                                &DmlOperation::Write(agg.encode()),
                                &self.database_name,
                                self.time_provider.as_ref(),
                            )?;
                            *record = record_new;
                            *md = md_new;

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
                            let mut record2 = op_record;
                            let mut md2 = op_md;
                            std::mem::swap(agg, &mut agg2);
                            std::mem::swap(record, &mut record2);
                            std::mem::swap(md, &mut md2);

                            let flushed_tag = agg2.tag;
                            agg2.finalize_span();
                            self.state.push_op(record2, md2, flushed_tag);

                            new_tag
                        }
                    }
                    Entry::Vacant(v) => {
                        // No open write aggregator yet => create one.
                        let tag = DmlAggregatorState::reserve_tag(&mut self.state.tag_to_record);
                        v.insert((
                            WriteAggregator::new(
                                write,
                                self.collector.as_ref().map(Arc::clone),
                                tag,
                            ),
                            op_record,
                            op_md,
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
                self.state.push_op(op_record, op_md, tag);
                Ok(TryPush::Aggregated(tag))
            }
        }
    }

    fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), aggregator::Error> {
        let mut state = std::mem::take(&mut self.state);
        state.flush_writes();

        let (records, metadata) = state.completed_ops.into_iter().unzip();

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

fn encode_operation(
    op: &DmlOperation,
    db_name: &str,
    time_provider: &dyn TimeProvider,
) -> Result<(Record, Metadata), aggregator::Error> {
    // truncate milliseconds from timestamps because that's what Kafka supports
    let now = op
        .meta()
        .producer_ts()
        .unwrap_or_else(|| time_provider.now());

    let timestamp_millis = now.date_time().timestamp_millis();
    let timestamp = Time::from_timestamp_millis(timestamp_millis);

    let headers = IoxHeaders::new(
        ContentType::Protobuf,
        op.meta().span_context().cloned(),
        op.namespace().to_owned(),
    );

    let mut buf = Vec::new();
    crate::codec::encode_operation(db_name, op, &mut buf)?;

    let record = Record {
        key: None,
        value: Some(buf),
        headers: headers
            .headers()
            .map(|(k, v)| (k.to_owned(), v.as_bytes().to_vec()))
            .collect(),
        timestamp: rskafka::time::OffsetDateTime::from_unix_timestamp_nanos(
            timestamp_millis as i128 * 1_000_000,
        )?,
    };

    let kafka_write_size = record.approximate_size();

    let md = Metadata {
        timestamp,
        span_ctx: op.meta().span_context().cloned(),
        kafka_write_size,
    };

    Ok((record, md))
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
