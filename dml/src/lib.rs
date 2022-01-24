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

use std::collections::{BTreeMap, HashSet};

use data_types::router::{ShardConfig, ShardId};
use hashbrown::HashMap;

use data_types::delete_predicate::DeletePredicate;
use data_types::non_empty::NonEmptyString;
use data_types::partition_metadata::{StatValues, Statistics};
use data_types::sequence::Sequence;
use mutable_batch::MutableBatch;
use time::Time;
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

    /// Shards this [`DmlOperation`]
    pub fn shard(self, config: &ShardConfig) -> BTreeMap<ShardId, Self> {
        match self {
            DmlOperation::Write(write) => write
                .shard(config)
                .into_iter()
                .map(|(shard, write)| (shard, Self::Write(write)))
                .collect(),
            DmlOperation::Delete(delete) => delete
                .shard(config)
                .into_iter()
                .map(|(shard, delete)| (shard, Self::Delete(delete)))
                .collect(),
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
    pub fn new(tables: HashMap<String, MutableBatch>, meta: DmlMeta) -> Self {
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
            tables,
            meta,
            min_timestamp: stats.min.unwrap(),
            max_timestamp: stats.max.unwrap(),
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
    pub fn tables(&self) -> impl Iterator<Item = (&str, &MutableBatch)> + '_ {
        self.tables.iter().map(|(k, v)| (k.as_str(), v))
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

    /// Shards this [`DmlWrite`]
    pub fn shard(self, config: &ShardConfig) -> BTreeMap<ShardId, Self> {
        let mut batches: HashMap<ShardId, HashMap<String, MutableBatch>> = HashMap::new();

        for (table, batch) in self.tables {
            if let Some(shard_id) = shard_table(&table, config) {
                assert!(batches
                    .entry(shard_id)
                    .or_default()
                    .insert(table, batch.clone())
                    .is_none());
            }
        }

        batches
            .into_iter()
            .map(|(shard_id, tables)| (shard_id, Self::new(tables, self.meta.clone())))
            .collect()
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
    predicate: DeletePredicate,
    table_name: Option<NonEmptyString>,
    meta: DmlMeta,
}

impl DmlDelete {
    /// Create a new [`DmlDelete`]
    pub fn new(
        predicate: DeletePredicate,
        table_name: Option<NonEmptyString>,
        meta: DmlMeta,
    ) -> Self {
        Self {
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

    /// Shards this [`DmlDelete`]
    pub fn shard(self, config: &ShardConfig) -> BTreeMap<ShardId, Self> {
        if let Some(table) = self.table_name() {
            if let Some(shard_id) = shard_table(table, config) {
                BTreeMap::from([(shard_id, self)])
            } else {
                BTreeMap::default()
            }
        } else {
            let shards: HashSet<ShardId> = config
                .specific_targets
                .iter()
                .map(|matcher2shard| matcher2shard.shard)
                .chain(
                    config
                        .hash_ring
                        .iter()
                        .map(|hashring| Vec::<ShardId>::from(hashring.shards.clone()).into_iter())
                        .flatten(),
                )
                .collect();

            shards
                .into_iter()
                .map(|shard| (shard, self.clone()))
                .collect()
        }
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

/// Shard only based on table name
fn shard_table(table: &str, config: &ShardConfig) -> Option<ShardId> {
    for matcher2shard in &config.specific_targets {
        if let Some(regex) = &matcher2shard.matcher.table_name_regex {
            if regex.is_match(table) {
                return Some(matcher2shard.shard);
            }
        }
    }

    if let Some(hash_ring) = &config.hash_ring {
        if let Some(id) = hash_ring.shards.find(table) {
            return Some(id);
        }
    }

    None
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

#[cfg(test)]
mod tests {
    use data_types::{
        consistent_hasher::ConsistentHasher,
        delete_predicate::DeletePredicate,
        non_empty::NonEmptyString,
        router::{HashRing, Matcher, MatcherToShard},
        timestamp::TimestampRange,
    };
    use mutable_batch_lp::lines_to_batches;
    use regex::Regex;

    use crate::test_util::assert_writes_eq;

    use super::*;

    #[test]
    fn test_write_sharding() {
        let config = ShardConfig {
            specific_targets: vec![
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: None,
                    },
                    shard: ShardId::new(1),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_foo").unwrap()),
                    },
                    shard: ShardId::new(2),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("other").unwrap()),
                    },
                    shard: ShardId::new(3),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_.*").unwrap()),
                    },
                    shard: ShardId::new(4),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("baz").unwrap()),
                    },
                    shard: ShardId::new(2),
                },
            ],
            hash_ring: Some(HashRing {
                shards: ConsistentHasher::new(&[
                    ShardId::new(11),
                    ShardId::new(12),
                    ShardId::new(13),
                ]),
            }),
        };

        let meta = DmlMeta::unsequenced(None);
        let write = db_write(
            &[
                "some_foo x=1 10",
                "some_foo x=2 20",
                "some_bar y=3 30",
                "other z=4 40",
                "rnd1 r=5 50",
                "rnd2 r=6 60",
                "rnd3 r=7 70",
                "baz b=8 80",
            ],
            &meta,
        );

        let actual = write.shard(&config);
        let expected = BTreeMap::from([
            (
                ShardId::new(2),
                db_write(&["some_foo x=1 10", "some_foo x=2 20", "baz b=8 80"], &meta),
            ),
            (ShardId::new(3), db_write(&["other z=4 40"], &meta)),
            (ShardId::new(4), db_write(&["some_bar y=3 30"], &meta)),
            (ShardId::new(11), db_write(&["rnd1 r=5 50"], &meta)),
            (ShardId::new(12), db_write(&["rnd3 r=7 70"], &meta)),
            (ShardId::new(13), db_write(&["rnd2 r=6 60"], &meta)),
        ]);

        let actual_shard_ids: Vec<_> = actual.keys().cloned().collect();
        let expected_shard_ids: Vec<_> = expected.keys().cloned().collect();
        assert_eq!(actual_shard_ids, expected_shard_ids);

        for (actual_write, expected_write) in actual.values().zip(expected.values()) {
            assert_writes_eq(actual_write, expected_write);
        }
    }

    #[test]
    fn test_write_no_match() {
        let config = ShardConfig::default();

        let meta = DmlMeta::default();
        let write = db_write(&["foo x=1 10"], &meta);

        let actual = write.shard(&config);
        assert!(actual.is_empty());
    }

    #[test]
    fn test_delete_sharding() {
        let config = ShardConfig {
            specific_targets: vec![
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: None,
                    },
                    shard: ShardId::new(1),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_foo").unwrap()),
                    },
                    shard: ShardId::new(2),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_.*").unwrap()),
                    },
                    shard: ShardId::new(3),
                },
            ],
            hash_ring: Some(HashRing {
                shards: ConsistentHasher::new(&[
                    ShardId::new(11),
                    ShardId::new(12),
                    ShardId::new(13),
                ]),
            }),
        };

        // Deletes w/o table name go to all shards
        let meta = DmlMeta::unsequenced(None);
        let delete = DmlDelete::new(
            DeletePredicate {
                range: TimestampRange::new(1, 2),
                exprs: vec![],
            },
            None,
            meta,
        );

        let actual = delete.clone().shard(&config);
        let expected = BTreeMap::from([
            (ShardId::new(1), delete.clone()),
            (ShardId::new(2), delete.clone()),
            (ShardId::new(3), delete.clone()),
            (ShardId::new(11), delete.clone()),
            (ShardId::new(12), delete.clone()),
            (ShardId::new(13), delete),
        ]);
        assert_sharded_deletes_eq(&actual, &expected);

        // Deletes are matched by table name regex
        let meta = DmlMeta::unsequenced(None);
        let delete = DmlDelete::new(
            DeletePredicate {
                range: TimestampRange::new(3, 4),
                exprs: vec![],
            },
            Some(NonEmptyString::new("some_foo").unwrap()),
            meta,
        );

        let actual = delete.clone().shard(&config);
        let expected = BTreeMap::from([(ShardId::new(2), delete)]);
        assert_sharded_deletes_eq(&actual, &expected);

        // Deletes can be matched by hash-ring
        let meta = DmlMeta::unsequenced(None);
        let delete = DmlDelete::new(
            DeletePredicate {
                range: TimestampRange::new(5, 6),
                exprs: vec![],
            },
            Some(NonEmptyString::new("bar").unwrap()),
            meta,
        );

        let actual = delete.clone().shard(&config);
        let expected = BTreeMap::from([(ShardId::new(13), delete)]);
        assert_sharded_deletes_eq(&actual, &expected);
    }

    fn db_write(lines: &[&str], meta: &DmlMeta) -> DmlWrite {
        DmlWrite::new(
            lines_to_batches(&lines.join("\n"), 0).unwrap(),
            meta.clone(),
        )
    }

    fn assert_sharded_deletes_eq(
        actual: &BTreeMap<ShardId, DmlDelete>,
        expected: &BTreeMap<ShardId, DmlDelete>,
    ) {
        let actual_shard_ids: Vec<_> = actual.keys().cloned().collect();
        let expected_shard_ids: Vec<_> = expected.keys().cloned().collect();
        assert_eq!(actual_shard_ids, expected_shard_ids);

        for (actual_delete, expected_delete) in actual.values().zip(expected.values()) {
            assert_eq!(actual_delete, expected_delete);
        }
    }
}
