use std::sync::Arc;

use data_types::{Sequence, SequenceNumber, ShardIndex};
use dml::{DmlMeta, DmlOperation};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::warn;
use rskafka::{
    client::producer::aggregator::{
        Aggregator, Error, RecordAggregator as RecordAggregatorDelegate,
        RecordAggregatorStatusDeaggregator, StatusDeaggregator, TryPush,
    },
    record::Record,
};
use trace::ctx::SpanContext;

use crate::codec::{ContentType, IoxHeaders};

/// The [`Tag`] is a data-carrying token identifier used to de-aggregate
/// responses from a batch aggregated of requests using the
/// [`DmlMetaDeaggregator`].
#[derive(Debug)]
pub struct Tag {
    /// The tag into the batch returned by the
    /// [`RecordAggregatorDelegate::try_push()`] call.
    idx: usize,

    /// The timestamp assigned to the resulting Kafka [`Record`].
    timestamp: Time,
    /// A span extracted from the original [`DmlOperation`].
    span_ctx: Option<SpanContext>,
    /// The approximate byte size of the serialised [`Record`], as calculated by
    /// [`Record::approximate_size()`].
    approx_kafka_write_size: usize,
}

/// A [`RecordAggregator`] implements [rskafka]'s abstract [`Aggregator`]
/// behaviour to provide batching of requests for a single Kafka partition.
///
/// Specifically the [`RecordAggregator`] maps [`DmlOperation`] instances to
/// Kafka [`Record`] instances, and delegates the batching to the
/// [`RecordAggregatorDelegate`] implementation maintained within [rskafka]
/// itself.
///
/// [rskafka]: https://github.com/influxdata/rskafka
#[derive(Debug)]
pub struct RecordAggregator {
    time_provider: Arc<dyn TimeProvider>,

    /// The shard index (Kafka partition number) this aggregator batches ops for (from Kafka,
    /// not the catalog).
    shard_index: ShardIndex,

    /// The underlying record aggregator the non-IOx-specific batching is
    /// delegated to.
    aggregator: RecordAggregatorDelegate,
}

impl RecordAggregator {
    /// Initialise a new [`RecordAggregator`] to aggregate up to
    /// `max_batch_size` number of bytes per message.
    pub fn new(
        shard_index: ShardIndex,
        max_batch_size: usize,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            shard_index,
            aggregator: RecordAggregatorDelegate::new(max_batch_size),
            time_provider,
        }
    }
}

impl RecordAggregator {
    /// Serialise the [`DmlOperation`] destined for the specified `db_name` into a
    /// [`Record`], returning the producer timestamp assigned to it.
    fn to_record(&self, op: &DmlOperation) -> Result<(Record, Time), Error> {
        let now = op
            .meta()
            .producer_ts()
            .unwrap_or_else(|| self.time_provider.now());

        let headers = IoxHeaders::new(ContentType::Protobuf, op.meta().span_context().cloned());

        let mut buf = Vec::new();
        crate::codec::encode_operation(op, &mut buf)?;
        buf.shrink_to_fit();

        let record = Record {
            key: None,
            value: Some(buf),
            headers: headers
                .headers()
                .map(|(k, v)| (k.to_owned(), v.as_bytes().to_vec()))
                .collect(),
            timestamp: now.date_time(),
        };

        Ok((record, now))
    }
}

impl Aggregator for RecordAggregator {
    type Input = DmlOperation;
    type Tag = <DmlMetaDeaggregator as StatusDeaggregator>::Tag;
    type StatusDeaggregator = DmlMetaDeaggregator;

    /// Callers should retain the returned [`Tag`] in order to de-aggregate the
    /// [`DmlMeta`] from the request response.
    fn try_push(&mut self, op: Self::Input) -> Result<TryPush<Self::Input, Self::Tag>, Error> {
        // Encode the DML op to a Record
        let (record, timestamp) = self.to_record(&op)?;

        // Capture various metadata necessary to construct the Tag/DmlMeta for
        // the caller once a batch has been flushed.
        let span_ctx = op.meta().span_context().cloned();
        let approx_kafka_write_size = record.approximate_size();

        // And delegate batching to rskafka's RecordAggregator implementation
        Ok(match self.aggregator.try_push(record)? {
            // NoCapacity returns the original input to the caller when the
            // batching fails.
            //
            // The RecordBatcher delegate is returning the Record encoded from
            // op above, but the caller of this fn is expecting the original op.
            //
            // Map to the original input op this fn was called with, discarding
            // the encoded Record.
            TryPush::NoCapacity(_) => {
                // Log a warning if this occurs - this allows an operator to
                // increase the maximum Kafka message size, or lower the linger
                // time to minimise latency while still producing large enough
                // batches for it to be worth while.
                warn!("aggregated batch reached maximum capacity");
                TryPush::NoCapacity(op)
            }

            // A successful delegate aggregation returns the tag for offset
            // de-aggregation later. For simplicity, the tag this layer returns
            // also carries the various (small) metadata elements needed to
            // construct the DmlMeta at the point of de-aggregation.
            TryPush::Aggregated(idx) => TryPush::Aggregated(Tag {
                idx,
                timestamp,
                span_ctx,
                approx_kafka_write_size,
            }),
        })
    }

    fn flush(&mut self) -> Result<(Vec<Record>, Self::StatusDeaggregator), Error> {
        let records = self.aggregator.flush()?.0;
        Ok((records, DmlMetaDeaggregator::new(self.shard_index)))
    }
}

/// The de-aggregation half of the [`RecordAggregator`], this type consumes the
/// caller's [`Tag`] obtained from the aggregator to return the corresponding
/// [`DmlMeta`] from the batched response.
///
/// The [`DmlMetaDeaggregator`] is a stateless wrapper over the (also stateless)
/// [`RecordAggregatorStatusDeaggregator`] delegate, with most of the metadata
/// elements carried in the [`Tag`] itself.
#[derive(Debug)]
pub struct DmlMetaDeaggregator {
    shard_index: ShardIndex,
}

impl DmlMetaDeaggregator {
    pub fn new(shard_index: ShardIndex) -> Self {
        Self { shard_index }
    }
}

impl StatusDeaggregator for DmlMetaDeaggregator {
    type Status = DmlMeta;
    type Tag = Tag;

    fn deaggregate(&self, input: &[i64], tag: Self::Tag) -> Result<Self::Status, Error> {
        // Delegate de-aggregation to the (stateless) record batch
        // de-aggregator for forwards compatibility.
        let offset = RecordAggregatorStatusDeaggregator::default()
            .deaggregate(input, tag.idx)
            .expect("invalid de-aggregation index");

        Ok(DmlMeta::sequenced(
            Sequence::new(self.shard_index, SequenceNumber::new(offset)),
            tag.timestamp,
            tag.span_ctx,
            tag.approx_kafka_write_size,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::{NamespaceId, TableId};
    use dml::DmlWrite;
    use hashbrown::HashMap;
    use iox_time::MockProvider;
    use mutable_batch::{writer::Writer, MutableBatch};
    use trace::LogTraceCollector;

    use crate::codec::{CONTENT_TYPE_PROTOBUF, HEADER_CONTENT_TYPE, HEADER_TRACE_CONTEXT};

    use super::*;

    const SHARD_INDEX: ShardIndex = ShardIndex::new(42);
    const TIMESTAMP_MILLIS: i64 = 1659990497000;

    fn test_op() -> DmlOperation {
        let mut batch = MutableBatch::new();
        let mut writer = Writer::new(&mut batch, 1);
        writer
            // Date: "1970-01-01"
            .write_time("time", [42].into_iter())
            .unwrap();
        writer
            .write_i64("A", Some(&[0b00000001]), [1].into_iter())
            .unwrap();
        writer.commit();

        let mut m = HashMap::default();
        m.insert(TableId::new(24), batch);

        let span = SpanContext::new(Arc::new(LogTraceCollector::new()));

        DmlOperation::Write(DmlWrite::new(
            NamespaceId::new(42),
            m,
            "1970-01-01".into(),
            DmlMeta::unsequenced(Some(span)),
        ))
    }

    #[test]
    fn test_record_aggregate() {
        let clock = Arc::new(MockProvider::new(
            Time::from_timestamp_millis(TIMESTAMP_MILLIS).unwrap(),
        ));
        let mut agg = RecordAggregator::new(SHARD_INDEX, usize::MAX, clock);
        let write = test_op();

        let res = agg.try_push(write).expect("aggregate call should succeed");
        let tag = match res {
            TryPush::NoCapacity(_) => panic!("unexpected no capacity"),
            TryPush::Aggregated(tag) => tag,
        };

        // Flush the aggregator to acquire the records
        let (records, deagg) = agg.flush().expect("should flush");
        assert_eq!(records.len(), 1);

        // Another flush should not yield the same records
        let (records2, _) = agg.flush().expect("should flush");
        assert!(records2.is_empty());

        // Assert properties of the resulting record
        let record = records[0].clone();
        assert_eq!(record.key, None);
        assert!(record.value.is_some());
        assert_eq!(
            *record
                .headers
                .get(HEADER_CONTENT_TYPE)
                .expect("no content type"),
            Vec::<u8>::from(CONTENT_TYPE_PROTOBUF),
        );
        assert!(record.headers.get(HEADER_TRACE_CONTEXT).is_some());
        assert_eq!(record.timestamp.timestamp(), 1659990497);

        // Extract the DmlMeta from the de-aggregator
        let got = deagg
            .deaggregate(&[4242], tag)
            .expect("de-aggregate should succeed");

        // Assert the metadata properties
        assert!(got.span_context().is_some());
        assert_eq!(
            *got.sequence().expect("should be sequenced"),
            Sequence::new(SHARD_INDEX, SequenceNumber::new(4242))
        );
        assert_eq!(
            got.producer_ts().expect("no producer timestamp"),
            Time::from_timestamp_millis(TIMESTAMP_MILLIS).unwrap(),
        );
        assert_eq!(
            got.bytes_read().expect("no approx size"),
            record.approximate_size()
        );
    }

    #[test]
    fn test_record_aggregate_no_capacity() {
        let clock = Arc::new(MockProvider::new(
            Time::from_timestamp_millis(TIMESTAMP_MILLIS).unwrap(),
        ));
        let mut agg = RecordAggregator::new(SHARD_INDEX, usize::MIN, clock);
        let write = test_op();

        let res = agg
            .try_push(write.clone())
            .expect("aggregate call should succeed");
        match res {
            TryPush::NoCapacity(res) => assert_eq!(res.namespace_id(), write.namespace_id()),
            TryPush::Aggregated(_) => panic!("expected no capacity"),
        };
    }
}
