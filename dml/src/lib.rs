//! DML data types

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::time::Duration;

use data_types::{
    DeletePredicate, NamespaceId, NonEmptyString, PartitionKey, SequenceNumber, StatValues,
    Statistics, TableId,
};
use hashbrown::HashMap;
use iox_time::{Time, TimeProvider};
use mutable_batch::MutableBatch;
use trace::ctx::SpanContext;

/// Metadata information about a DML operation
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DmlMeta {
    /// The sequence number associated with this write
    sequence_number: Option<SequenceNumber>,

    /// When this write was ingested into the write buffer
    producer_ts: Option<Time>,

    /// Optional span context associated w/ this write
    span_ctx: Option<SpanContext>,

    /// Bytes read from the wire
    bytes_read: Option<usize>,
}

impl DmlMeta {
    /// Create a new [`DmlMeta`] for a sequenced operation
    pub fn sequenced(
        sequence_number: SequenceNumber,
        producer_ts: Time,
        span_ctx: Option<SpanContext>,
        bytes_read: usize,
    ) -> Self {
        Self {
            sequence_number: Some(sequence_number),
            producer_ts: Some(producer_ts),
            span_ctx,
            bytes_read: Some(bytes_read),
        }
    }

    /// Create a new [`DmlMeta`] for an unsequenced operation
    pub fn unsequenced(span_ctx: Option<SpanContext>) -> Self {
        Self {
            sequence_number: None,
            producer_ts: None,
            span_ctx,
            bytes_read: None,
        }
    }

    /// Gets the sequence number associated with the write if any
    pub fn sequence(&self) -> Option<SequenceNumber> {
        self.sequence_number
    }

    /// Gets the producer timestamp associated with the write if any
    pub fn producer_ts(&self) -> Option<Time> {
        self.producer_ts
    }

    /// returns the Duration since this DmlMeta was produced, if any
    pub fn duration_since_production(&self, time_provider: &dyn TimeProvider) -> Option<Duration> {
        self.producer_ts
            .and_then(|ts| time_provider.now().checked_duration_since(ts))
    }

    /// Gets the span context if any
    pub fn span_context(&self) -> Option<&SpanContext> {
        self.span_ctx.as_ref()
    }

    /// Returns the number of bytes read from the wire if relevant
    pub fn bytes_read(&self) -> Option<usize> {
        self.bytes_read
    }

    /// Return the approximate memory size of the metadata, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .span_ctx
                .as_ref()
                .map(|ctx| ctx.size())
                .unwrap_or_default()
            - self
                .span_ctx
                .as_ref()
                .map(|_| std::mem::size_of::<SpanContext>())
                .unwrap_or_default()
    }
}

/// A DML operation
#[derive(Debug, Clone)]
pub enum DmlOperation {
    /// A write operation
    Write(DmlWrite),

    /// A delete operation
    Delete(DmlDelete),
}

impl DmlOperation {
    /// Gets the metadata associated with this operation
    pub fn meta(&self) -> &DmlMeta {
        match &self {
            Self::Write(w) => w.meta(),
            Self::Delete(d) => d.meta(),
        }
    }

    /// Sets the metadata for this operation
    pub fn set_meta(&mut self, meta: DmlMeta) {
        match self {
            Self::Write(w) => w.set_meta(meta),
            Self::Delete(d) => d.set_meta(meta),
        }
    }

    /// Return the approximate memory size of the operation, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        match self {
            Self::Write(w) => {
                std::mem::size_of::<Self>() - std::mem::size_of::<DmlWrite>() + w.size()
            }
            Self::Delete(d) => {
                std::mem::size_of::<Self>() - std::mem::size_of::<DmlDelete>() + d.size()
            }
        }
    }

    /// Namespace catalog ID associated with this operation
    pub fn namespace_id(&self) -> NamespaceId {
        match self {
            Self::Write(w) => w.namespace_id(),
            Self::Delete(d) => d.namespace_id(),
        }
    }
}

impl From<DmlWrite> for DmlOperation {
    fn from(v: DmlWrite) -> Self {
        Self::Write(v)
    }
}

impl From<DmlDelete> for DmlOperation {
    fn from(v: DmlDelete) -> Self {
        Self::Delete(v)
    }
}

/// A collection of writes to potentially multiple tables within the same namespace
#[derive(Debug, Clone)]
pub struct DmlWrite {
    /// The namespace being written to
    namespace_id: NamespaceId,
    /// Writes to individual tables keyed by table ID
    table_ids: HashMap<TableId, MutableBatch>,
    /// Write metadata
    meta: DmlMeta,
    min_timestamp: i64,
    max_timestamp: i64,
    /// The partition key derived for this write.
    partition_key: PartitionKey,
}

impl DmlWrite {
    /// Create a new [`DmlWrite`]
    ///
    /// # Panic
    ///
    /// Panics if
    ///
    /// - `table_ids` is empty
    /// - a MutableBatch is empty
    /// - a MutableBatch lacks an i64 "time" column
    pub fn new(
        namespace_id: NamespaceId,
        table_ids: HashMap<TableId, MutableBatch>,
        partition_key: PartitionKey,
        meta: DmlMeta,
    ) -> Self {
        assert_ne!(table_ids.len(), 0);

        let mut stats = StatValues::new_empty();
        for (table_id, table) in &table_ids {
            match table
                .column(schema::TIME_COLUMN_NAME)
                .expect("time")
                .stats()
            {
                Statistics::I64(col_stats) => stats.update_from(&col_stats),
                s => unreachable!(
                    "table \"{}\" has unexpected type for time column: {}",
                    table_id,
                    s.type_name()
                ),
            };
        }

        Self {
            table_ids,
            partition_key,
            meta,
            min_timestamp: stats.min.unwrap(),
            max_timestamp: stats.max.unwrap(),
            namespace_id,
        }
    }

    /// Metadata associated with this write
    pub fn meta(&self) -> &DmlMeta {
        &self.meta
    }

    /// Set the metadata
    pub fn set_meta(&mut self, meta: DmlMeta) {
        self.meta = meta
    }

    /// Returns an iterator over the per-table writes within this [`DmlWrite`]
    /// in no particular order
    pub fn tables(&self) -> impl Iterator<Item = (&TableId, &MutableBatch)> + '_ {
        self.table_ids.iter()
    }

    /// Consumes `self`, returning an iterator of the table ID and data contained within it.
    pub fn into_tables(self) -> impl Iterator<Item = (TableId, MutableBatch)> {
        self.table_ids.into_iter()
    }

    /// Gets the write for a given table
    pub fn table(&self, id: &TableId) -> Option<&MutableBatch> {
        self.table_ids.get(id)
    }

    /// Returns the number of tables within this write
    pub fn table_count(&self) -> usize {
        self.table_ids.len()
    }

    /// Returns the minimum timestamp in the write
    pub fn min_timestamp(&self) -> i64 {
        self.min_timestamp
    }

    /// Returns the maximum timestamp in the write
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Return the approximate memory size of the write, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .table_ids
                .values()
                .map(|v| std::mem::size_of::<TableId>() + v.size())
                .sum::<usize>()
            + self.meta.size()
            + std::mem::size_of::<NamespaceId>()
            + std::mem::size_of::<PartitionKey>()
            - std::mem::size_of::<DmlMeta>()
    }

    /// Return the partition key derived for this op.
    pub fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Return the [`NamespaceId`] to which this [`DmlWrite`] should be applied.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

/// A delete operation
#[derive(Debug, Clone, PartialEq)]
pub struct DmlDelete {
    namespace_id: NamespaceId,
    predicate: DeletePredicate,
    table_name: Option<NonEmptyString>,
    meta: DmlMeta,
}

impl DmlDelete {
    /// Create a new [`DmlDelete`]
    pub fn new(
        namespace_id: NamespaceId,
        predicate: DeletePredicate,
        table_name: Option<NonEmptyString>,
        meta: DmlMeta,
    ) -> Self {
        Self {
            namespace_id,
            predicate,
            table_name,
            meta,
        }
    }

    /// Returns the table_name for this delete
    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    /// Returns the [`DeletePredicate`]
    pub fn predicate(&self) -> &DeletePredicate {
        &self.predicate
    }

    /// Returns the [`DmlMeta`]
    pub fn meta(&self) -> &DmlMeta {
        &self.meta
    }

    /// Sets the [`DmlMeta`] for this [`DmlDelete`]
    pub fn set_meta(&mut self, meta: DmlMeta) {
        self.meta = meta
    }

    /// Return the approximate memory size of the delete, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.predicate.size() - std::mem::size_of::<DeletePredicate>()
            + self
                .table_name
                .as_ref()
                .map(|s| s.len())
                .unwrap_or_default()
            + self.meta.size()
            - std::mem::size_of::<DmlMeta>()
    }

    /// Return the [`NamespaceId`] to which this operation should be applied.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

/// Test utilities
pub mod test_util {
    use arrow_util::display::pretty_format_batches;
    use schema::Projection;

    use super::*;

    /// Asserts two operations are equal
    pub fn assert_op_eq(a: &DmlOperation, b: &DmlOperation) {
        match (a, b) {
            (DmlOperation::Write(a), DmlOperation::Write(b)) => assert_writes_eq(a, b),
            (DmlOperation::Delete(a), DmlOperation::Delete(b)) => assert_eq!(a, b),
            (a, b) => panic!("a != b, {a:?} vs {b:?}"),
        }
    }

    /// Asserts `a` contains a [`DmlWrite`] equal to `b`
    pub fn assert_write_op_eq(a: &DmlOperation, b: &DmlWrite) {
        match a {
            DmlOperation::Write(a) => assert_writes_eq(a, b),
            _ => panic!("unexpected operation: {a:?}"),
        }
    }

    /// Asserts two writes are equal
    pub fn assert_writes_eq(a: &DmlWrite, b: &DmlWrite) {
        assert_eq!(a.namespace_id, b.namespace_id);
        assert_eq!(a.partition_key(), b.partition_key());

        // Depending on what implementation is under test ( :( ) different
        // timestamp precisions may be used.
        //
        // Truncate them all to milliseconds (the lowest common denominator) so
        // they are comparable.
        assert_eq!(
            truncate_timestamp_to_millis(a.meta()),
            truncate_timestamp_to_millis(b.meta())
        );

        assert_eq!(a.table_count(), b.table_count());

        for (table_id, a_batch) in a.tables() {
            let b_batch = b.table(table_id).expect("table not found");

            assert_eq!(
                pretty_format_batches(&[a_batch.to_arrow(Projection::All).unwrap()]).unwrap(),
                pretty_format_batches(&[b_batch.to_arrow(Projection::All).unwrap()]).unwrap(),
                "batches for table \"{table_id}\" differ"
            );
        }
    }

    /// Asserts `a` contains a [`DmlDelete`] equal to `b`
    pub fn assert_delete_op_eq(a: &DmlOperation, b: &DmlDelete) {
        match a {
            DmlOperation::Delete(a) => assert_eq!(a, b),
            _ => panic!("unexpected operation: {a:?}"),
        }
    }

    fn truncate_timestamp_to_millis(m: &DmlMeta) -> DmlMeta {
        // Kafka supports millisecond precision in timestamps, so drop some
        // precision from this producer timestamp in the metadata (which has
        // nanosecond precision) to ensure the returned write is directly
        // comparable to a write that has come through the write buffer.
        //
        // This mangling is to support testing comparisons only.
        let timestamp = m
            .producer_ts()
            .expect("no producer timestamp in de-aggregated metadata");
        let timestamp =
            Time::from_timestamp_millis(timestamp.timestamp_millis()).expect("ts in range");

        DmlMeta::sequenced(
            m.sequence().unwrap(),
            timestamp,
            m.span_context().cloned(),
            m.bytes_read().unwrap(),
        )
    }
}
