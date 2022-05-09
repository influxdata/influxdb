//! DML data types

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use data_types::{DeletePredicate, NonEmptyString, Sequence, StatValues, Statistics};
use hashbrown::HashMap;
use iox_time::Time;
use mutable_batch::MutableBatch;
use trace::ctx::SpanContext;

/// Metadata information about a DML operation
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DmlMeta {
    /// The sequence number associated with this write
    sequence: Option<Sequence>,

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
        sequence: Sequence,
        producer_ts: Time,
        span_ctx: Option<SpanContext>,
        bytes_read: usize,
    ) -> Self {
        Self {
            sequence: Some(sequence),
            producer_ts: Some(producer_ts),
            span_ctx,
            bytes_read: Some(bytes_read),
        }
    }

    /// Create a new [`DmlMeta`] for an unsequenced operation
    pub fn unsequenced(span_ctx: Option<SpanContext>) -> Self {
        Self {
            sequence: None,
            producer_ts: None,
            span_ctx,
            bytes_read: None,
        }
    }

    /// Gets the sequence number associated with the write if any
    pub fn sequence(&self) -> Option<&Sequence> {
        self.sequence.as_ref()
    }

    /// Gets the producer timestamp associated with the write if any
    pub fn producer_ts(&self) -> Option<Time> {
        self.producer_ts
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
            DmlOperation::Write(w) => w.meta(),
            DmlOperation::Delete(d) => d.meta(),
        }
    }

    /// Sets the metadata for this operation
    pub fn set_meta(&mut self, meta: DmlMeta) {
        match self {
            DmlOperation::Write(w) => w.set_meta(meta),
            DmlOperation::Delete(d) => d.set_meta(meta),
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

    /// Namespace associated with this operation
    pub fn namespace(&self) -> &str {
        match self {
            DmlOperation::Write(w) => w.namespace(),
            DmlOperation::Delete(d) => d.namespace(),
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

/// A collection of writes to potentially multiple tables within the same database
#[derive(Debug, Clone)]
pub struct DmlWrite {
    /// The namespace being written to
    namespace: String,
    /// Writes to individual tables keyed by table name
    tables: HashMap<String, MutableBatch>,
    /// Write metadata
    meta: DmlMeta,
    min_timestamp: i64,
    max_timestamp: i64,
}

impl DmlWrite {
    /// Create a new [`DmlWrite`]
    ///
    /// # Panic
    ///
    /// Panics if
    ///
    /// - `tables` is empty
    /// - a MutableBatch is empty
    /// - a MutableBatch lacks an i64 "time" column
    pub fn new(
        namespace: impl Into<String>,
        tables: HashMap<String, MutableBatch>,
        meta: DmlMeta,
    ) -> Self {
        assert_ne!(tables.len(), 0);

        let mut stats = StatValues::new_empty();
        for (table_name, table) in &tables {
            match table
                .column(schema::TIME_COLUMN_NAME)
                .expect("time")
                .stats()
            {
                Statistics::I64(col_stats) => stats.update_from(&col_stats),
                s => unreachable!(
                    "table \"{}\" has unexpected type for time column: {}",
                    table_name,
                    s.type_name()
                ),
            };
        }

        Self {
            namespace: namespace.into(),
            tables,
            meta,
            min_timestamp: stats.min.unwrap(),
            max_timestamp: stats.max.unwrap(),
        }
    }

    /// Namespace associated with this write
    pub fn namespace(&self) -> &str {
        &self.namespace
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
    pub fn tables(&self) -> impl Iterator<Item = (&str, &MutableBatch)> + '_ {
        self.tables.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Moves the table names and mutable batches out of the map
    pub fn into_tables(self) -> impl Iterator<Item = (String, MutableBatch)> {
        self.tables.into_iter()
    }

    /// Gets the write for a given table
    pub fn table(&self, name: &str) -> Option<&MutableBatch> {
        self.tables.get(name)
    }

    /// Returns the number of tables within this write
    pub fn table_count(&self) -> usize {
        self.tables.len()
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
                .tables
                .iter()
                .map(|(k, v)| std::mem::size_of_val(k) + k.capacity() + v.size())
                .sum::<usize>()
            + self.meta.size()
            - std::mem::size_of::<DmlMeta>()
    }
}

/// A delete operation
#[derive(Debug, Clone, PartialEq)]
pub struct DmlDelete {
    namespace: String,
    predicate: DeletePredicate,
    table_name: Option<NonEmptyString>,
    meta: DmlMeta,
}

impl DmlDelete {
    /// Create a new [`DmlDelete`]
    pub fn new(
        namespace: impl Into<String>,
        predicate: DeletePredicate,
        table_name: Option<NonEmptyString>,
        meta: DmlMeta,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            predicate,
            table_name,
            meta,
        }
    }

    /// Namespace associated with this delete
    pub fn namespace(&self) -> &str {
        &self.namespace
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
}

/// Test utilities
pub mod test_util {
    use arrow_util::display::pretty_format_batches;
    use schema::selection::Selection;

    use super::*;

    /// Asserts two operations are equal
    pub fn assert_op_eq(a: &DmlOperation, b: &DmlOperation) {
        match (a, b) {
            (DmlOperation::Write(a), DmlOperation::Write(b)) => assert_writes_eq(a, b),
            (DmlOperation::Delete(a), DmlOperation::Delete(b)) => assert_eq!(a, b),
            (a, b) => panic!("a != b, {:?} vs {:?}", a, b),
        }
    }

    /// Asserts `a` contains a [`DmlWrite`] equal to `b`
    pub fn assert_write_op_eq(a: &DmlOperation, b: &DmlWrite) {
        match a {
            DmlOperation::Write(a) => assert_writes_eq(a, b),
            _ => panic!("unexpected operation: {:?}", a),
        }
    }

    /// Asserts two writes are equal
    pub fn assert_writes_eq(a: &DmlWrite, b: &DmlWrite) {
        assert_eq!(a.namespace, b.namespace);
        assert_eq!(a.meta(), b.meta());

        assert_eq!(a.table_count(), b.table_count());

        for (table_name, a_batch) in a.tables() {
            let b_batch = b.table(table_name).expect("table not found");

            assert_eq!(
                pretty_format_batches(&[a_batch.to_arrow(Selection::All).unwrap()]).unwrap(),
                pretty_format_batches(&[b_batch.to_arrow(Selection::All).unwrap()]).unwrap(),
                "batches for table \"{}\" differ",
                table_name
            );
        }
    }

    /// Asserts `a` contains a [`DmlDelete`] equal to `b`
    pub fn assert_delete_op_eq(a: &DmlOperation, b: &DmlDelete) {
        match a {
            DmlOperation::Delete(a) => assert_eq!(a, b),
            _ => panic!("unexpected operation: {:?}", a),
        }
    }
}
