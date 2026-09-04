//! The in memory buffer of a table that can be quickly added to and queried

use arrow::array::builder::BooleanBufferBuilder;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, BooleanBuilder, Float64Builder,
    GenericByteBuilder, Int64Builder, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
    StringDictionaryBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{GenericStringType, Int32Type};
use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::{TableDefinition, legacy};
use influxdb3_id::ColumnId;
use influxdb3_wal::{FieldData, Row};
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;
use thiserror::Error;

use crate::ChunkFilter;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Field not found in table buffer: {0}")]
    FieldNotFound(String),

    #[error("Error creating record batch: {0}")]
    RecordBatchError(#[from] arrow::error::ArrowError),

    #[error("row range {start}..{end} is out of bounds for a chunk of {row_count} rows")]
    RowRangeOutOfBounds {
        start: usize,
        end: usize,
        row_count: usize,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Default)]
pub struct TableBuffer {
    chunk_time_to_chunks: BTreeMap<i64, ChunkTimeBuffer>,
    snapshotting_chunks: Vec<SnapshotChunk>,
}

/// All buffered chunks for one chunk_time (gen1 window) of a table, plus the
/// overwrite index spanning them. A chunk_time usually holds one chunk; a
/// var-column overflow splits it into several, and hoisting the index here
/// keeps overwrite collapsing working across the split — including on WAL
/// replay, where batch boundaries (and therefore split points) can differ
/// from the original ingest.
#[derive(Default)]
struct ChunkTimeBuffer {
    chunks: Vec<MutableTableChunk>,
    /// Overwrite identity of each live row -> (chunk index, row index).
    /// See [`row_key_for`].
    row_key_to_index: HashMap<u128, (usize, usize)>,
}

impl ChunkTimeBuffer {
    fn add_rows(&mut self, rows: &[Row]) {
        let last = self.chunks.len() - 1;

        // Reused across the batch's rows so overwrite-identity hashing does
        // no per-row allocation.
        let mut tag_scratch: Vec<(u16, &str)> = Vec::new();
        let mut key_scratch: Vec<u8> = Vec::new();

        for r in rows {
            let row_key = row_key_for(r, &mut tag_scratch, &mut key_scratch);
            let replaces = self.row_key_to_index.get(&row_key).copied();

            let (earlier, target) = self.chunks.split_at_mut(last);
            let target = &mut target[0];
            let this_row = match replaces {
                None => target.append_row(r, CarrySource::None),
                Some((ci, ri)) if ci == last => {
                    let idx = target.append_row(r, CarrySource::Local(ri));
                    target.mark_superseded(ri);
                    idx
                }
                Some((ci, ri)) => {
                    let idx = target.append_row(r, CarrySource::Foreign(&earlier[ci], ri));
                    earlier[ci].mark_superseded(ri);
                    idx
                }
            };
            self.row_key_to_index.insert(row_key, (last, this_row));
        }
    }
}

/// Where an appended row's missing fields are carried forward from
enum CarrySource<'a> {
    /// A fresh row
    None,
    /// An earlier row in the same chunk
    Local(usize),
    /// A row in an earlier split chunk of the same chunk_time
    Foreign(&'a MutableTableChunk, usize),
}

impl TableBuffer {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn buffer_chunk(&mut self, chunk_time: i64, rows: &[Row]) {
        let ctb = self.chunk_time_to_chunks.entry(chunk_time).or_default();

        let mut incoming_per_column = HashMap::new();
        for r in rows {
            for f in &r.fields {
                match &f.value {
                    FieldData::String(s) | FieldData::Tag(s) => {
                        *incoming_per_column.entry(f.id).or_default() += s.len();
                    }
                    _ => {}
                }
            }
        }

        let needs_new_chunk = ctb.chunks.is_empty()
            || ctb
                .chunks
                .last()
                .is_some_and(|c| c.would_exceed_limit_with(&incoming_per_column));

        if needs_new_chunk {
            ctb.chunks.push(MutableTableChunk::new());
        }

        ctb.add_rows(rows);
    }

    /// Produce a partitioned set of record batches along with their min/max timestamp
    ///
    /// The partitions are stored and returned in a `HashMap`, keyed on the generation time.
    ///
    /// This uses the provided `filter` to prune out chunks from the buffer that do not fall in
    /// the filter's time boundaries. If the filter contains literal guarantees on tag columns
    /// that are in the buffer index, this will also leverage those to prune rows in the resulting
    /// chunks that do not satisfy the guarantees specified in the filter.
    pub fn partitioned_record_batches(
        &self,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Result<HashMap<i64, BufferedBatches>> {
        let mut batches: HashMap<i64, BufferedBatches> = HashMap::new();
        let schema = table_def.schema.as_arrow();
        for sc in self.snapshotting_chunks.iter().filter(|sc| {
            filter.test_time_stamp_min_max(sc.timestamp_min_max.min, sc.timestamp_min_max.max)
        }) {
            let cols: std::result::Result<Vec<_>, _> = schema
                .fields()
                .iter()
                .map(|f| {
                    let col = sc
                        .record_batch
                        .column_by_name(f.name())
                        .ok_or(Error::FieldNotFound(f.name().to_string()));
                    col.cloned()
                })
                .collect();
            let cols = cols?;
            let rb = RecordBatch::try_new(Arc::clone(&schema), cols)?;
            batches
                .entry(sc.chunk_time)
                .or_default()
                .push_snapshotting(rb, sc.timestamp_min_max);
        }
        for (t, ctb) in self.chunk_time_to_chunks.iter() {
            for c in ctb
                .chunks
                .iter()
                .filter(|c| filter.test_time_stamp_min_max(c.timestamp_min, c.timestamp_max))
            {
                let ts_min_max = TimestampMinMax::new(c.timestamp_min, c.timestamp_max);
                let rb = c.record_batch(Arc::clone(&table_def))?;
                batches.entry(*t).or_default().push_live(rb, ts_min_max);
            }
        }
        Ok(batches)
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        let (min, max) = if self.chunk_time_to_chunks.is_empty() {
            (0, 0)
        } else {
            self.chunk_time_to_chunks
                .values()
                .flat_map(|ctb| ctb.chunks.iter())
                .map(|c| (c.timestamp_min, c.timestamp_max))
                .fold((i64::MAX, i64::MIN), |(a_min, b_min), (a_max, b_max)| {
                    (a_min.min(b_min), a_max.max(b_max))
                })
        };
        let mut timestamp_min_max = TimestampMinMax::new(min, max);

        for sc in &self.snapshotting_chunks {
            timestamp_min_max = timestamp_min_max.union(&sc.timestamp_min_max);
        }

        timestamp_min_max
    }

    /// Returns an estimate of the size of this table buffer based on the data and index sizes.
    #[allow(dead_code)]
    pub fn computed_size(&self) -> usize {
        let mut size = size_of::<Self>();

        for ctb in self.chunk_time_to_chunks.values() {
            for c in &ctb.chunks {
                for builder in c.data.values() {
                    size += size_of::<ColumnId>() + size_of::<String>() + builder.size();
                }
                size += c.live.capacity() / 8;
            }
            size += ctb.row_key_to_index.len() * (size_of::<u128>() + 2 * size_of::<usize>());
        }

        size
    }

    pub fn snapshot(
        &mut self,
        table_def: Arc<TableDefinition>,
        older_than_chunk_time: i64,
    ) -> Vec<SnapshotChunk> {
        let keys_to_remove = self
            .chunk_time_to_chunks
            .keys()
            .filter(|k| **k < older_than_chunk_time)
            .copied()
            .collect::<Vec<_>>();

        let mut snapshot_chunks = Vec::new();
        for chunk_time in keys_to_remove {
            let chunks = self
                .chunk_time_to_chunks
                .remove(&chunk_time)
                .unwrap()
                .chunks;
            // A chunk_time can yield several chunks (a string/tag column crossing
            // the Arrow varchar limit splits one). The ordinal makes each chunk
            // individually addressable, both for its parquet path and for
            // `remove_snapshotting_chunk` once that parquet file exists.
            for (chunk_ordinal, chunk) in chunks.into_iter().enumerate() {
                // Every row of an earlier split chunk can be superseded by
                // overwrites landing in a later split; such a chunk has
                // nothing to persist and would otherwise become an empty
                // parquet file.
                if chunk.superseded_count == chunk.row_count {
                    continue;
                }
                let timestamp_min_max = chunk.timestamp_min_max();
                let (schema, record_batch) = chunk.into_schema_record_batch(Arc::clone(&table_def));

                snapshot_chunks.push(SnapshotChunk {
                    chunk_time,
                    chunk_ordinal: chunk_ordinal as u32,
                    timestamp_min_max,
                    record_batch,
                    schema,
                });
            }
        }
        self.snapshotting_chunks = snapshot_chunks;

        self.snapshotting_chunks.clone()
    }

    /// Drop a single snapshotting chunk, identified by the `chunk_time` /
    /// `chunk_ordinal` pair [`TableBuffer::snapshot`] assigned it.
    ///
    /// Callers persist the chunks of one snapshot concurrently and must drop
    /// each chunk only once its own parquet file is queryable — a chunk is
    /// served from `snapshotting_chunks` until then, and from the parquet file
    /// after, so dropping it early makes those rows briefly invisible to
    /// queries.
    pub fn remove_snapshotting_chunk(&mut self, chunk_time: i64, chunk_ordinal: u32) {
        if let Some(index) = self.snapshotting_chunks.iter().position(|chunk| {
            chunk.chunk_time == chunk_time && chunk.chunk_ordinal == chunk_ordinal
        }) {
            self.snapshotting_chunks.swap_remove(index);
        }
    }

    /// Drop every snapshotting chunk at once.
    ///
    /// Only correct for a caller that has already made the whole table's
    /// snapshot queryable elsewhere — one that registers every parquet file of
    /// the snapshot before clearing. A caller that persists chunk by chunk
    /// wants [`TableBuffer::remove_snapshotting_chunk`].
    pub fn clear_snapshots(&mut self) {
        self.snapshotting_chunks.clear();
    }
}

/// The queryable batches of one chunk_time, split by provenance so the
/// consumer can rank them: `snapshotting` batches are frozen pre-persist
/// state that must lose dedup against `live` batches, which can contain
/// newer overwrites of the same points (arriving while the snapshot's
/// parquet files are being written). Mixing them under one chunk order
/// would make that dedup order-undefined.
///
/// Each provenance set carries its own time range so the chunks built from
/// them get stats no looser than their actual batches.
#[derive(Debug, Default)]
pub struct BufferedBatches {
    /// Frozen snapshot batches, already collapsed; rank below `live`.
    pub snapshotting: Vec<RecordBatch>,
    /// Time range covered by `snapshotting`; `None` when it is empty.
    pub snapshotting_min_max: Option<TimestampMinMax>,
    /// Batches from mutable chunks, collapsed as of this call.
    pub live: Vec<RecordBatch>,
    /// Time range covered by `live`; `None` when it is empty.
    pub live_min_max: Option<TimestampMinMax>,
}

impl BufferedBatches {
    fn push_snapshotting(&mut self, batch: RecordBatch, min_max: TimestampMinMax) {
        self.snapshotting_min_max = Some(union_opt(self.snapshotting_min_max, min_max));
        self.snapshotting.push(batch);
    }

    fn push_live(&mut self, batch: RecordBatch, min_max: TimestampMinMax) {
        self.live_min_max = Some(union_opt(self.live_min_max, min_max));
        self.live.push(batch);
    }

    /// Time range across both provenance sets; `None` when both are empty.
    pub fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        match (self.snapshotting_min_max, self.live_min_max) {
            (Some(s), Some(l)) => Some(s.union(&l)),
            (s, None) => s,
            (None, l) => l,
        }
    }

    /// All batches, snapshotting first. For consumers (and tests) that do
    /// not rank by provenance.
    pub fn combined(&self) -> Vec<RecordBatch> {
        self.snapshotting
            .iter()
            .chain(self.live.iter())
            .cloned()
            .collect()
    }
}

fn union_opt(cur: Option<TimestampMinMax>, next: TimestampMinMax) -> TimestampMinMax {
    cur.map_or(next, |cur| cur.union(&next))
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub(crate) chunk_time: i64,
    /// Position of this chunk among the chunks sharing its `chunk_time`,
    /// assigned by [`TableBuffer::snapshot`]. Identifies the chunk for its
    /// parquet path and for [`TableBuffer::remove_snapshotting_chunk`].
    pub(crate) chunk_ordinal: u32,
    pub(crate) timestamp_min_max: TimestampMinMax,
    pub(crate) record_batch: RecordBatch,
    pub(crate) schema: Schema,
}

impl SnapshotChunk {
    pub fn into_batch(self) -> RecordBatch {
        self.record_batch
    }
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for TableBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (min_time, max_time, row_count) = self
            .chunk_time_to_chunks
            .values()
            .flat_map(|ctb| ctb.chunks.iter())
            .map(|c| (c.timestamp_min, c.timestamp_max, c.row_count))
            .fold(
                (i64::MAX, i64::MIN, 0),
                |(a_min, a_max, a_count), (b_min, b_max, b_count)| {
                    (a_min.min(b_min), a_max.max(b_max), a_count + b_count)
                },
            );
        let chunk_count: usize = self
            .chunk_time_to_chunks
            .values()
            .map(|ctb| ctb.chunks.len())
            .sum();
        f.debug_struct("TableBuffer")
            .field("chunk_count", &chunk_count)
            .field("timestamp_min", &min_time)
            .field("timestamp_max", &max_time)
            .field("row_count", &row_count)
            .finish()
    }
}

/// An append-only set of column builders holding buffered rows, with
/// overwrite collapsing: a row that repeats an earlier row's (tag set,
/// timestamp) supersedes it. The new row unions the field sets — fields the
/// new row does not provide are carried forward from the replaced row — and
/// the replaced row is masked out of every batch this chunk produces, so
/// duplicate keys never reach query or persist dedup with an undefined
/// order. Builders are never mutated in place; superseded rows stay in the
/// builders and are skipped when a batch is produced ([`Self::materialize`]),
/// in the same single pass that copies the builders into immutable arrays.
struct MutableTableChunk {
    timestamp_min: i64,
    timestamp_max: i64,
    data: BTreeMap<ColumnId, Builder>,
    row_count: usize,
    string_bytes_per_column: HashMap<ColumnId, usize>,
    /// One bit per row: set while the row is live, cleared once a later
    /// duplicate supersedes it. Read directly by the materialization kernels.
    live: BooleanBufferBuilder,
    superseded_count: usize,
    /// Lowest row index superseded since [`Self::reset_superseded_watermark`]
    /// was last called (`None` if none). Lets a materialization cache tell
    /// whether a cached prefix of this chunk is still valid without scanning
    /// the mask; nothing consumes it yet.
    superseded_since_watermark: Option<usize>,
}

/// Per-process random keys for the overwrite-identity hash below. Tag values
/// are user-controlled, so the hash is keyed (SipHash is a PRF): without the
/// keys, colliding tag sets cannot be constructed — the same reasoning that
/// gives `PartitionHashId` a cryptographic hash for user-set partition keys.
/// Fresh keys per process are fine because the index never leaves the process.
static ROW_KEY_HASH_KEYS: std::sync::LazyLock<(u64, u64)> = std::sync::LazyLock::new(|| {
    use std::hash::{BuildHasher, Hasher};
    let entropy = std::collections::hash_map::RandomState::new();
    let k0 = entropy.build_hasher().finish();
    let mut h = entropy.build_hasher();
    h.write_u64(!k0);
    (k0, h.finish())
});

/// The overwrite identity of a row — its tag (column id, value) pairs sorted
/// by column id, then its timestamp — reduced to a 128-bit keyed hash. Two
/// rows with equal keys are treated as versions of the same point.
///
/// Hashing instead of storing the identity bytes keeps the per-row cost
/// allocation-free (`tag_scratch` and `key_scratch` are reused across the
/// rows of a batch). The trade-off is a hash-collision chance that two
/// distinct points merge: at 128 bits the accidental probability is
/// ~n²/2¹²⁸ for n buffered rows (~10⁻²⁵ even at 10M rows), and the keyed
/// hash ([`ROW_KEY_HASH_KEYS`]) rules out constructed collisions.
fn row_key_for<'a>(
    row: &'a Row,
    tag_scratch: &mut Vec<(u16, &'a str)>,
    key_scratch: &mut Vec<u8>,
) -> u128 {
    use siphasher::sip128::Hasher128;
    use std::hash::Hasher;

    tag_scratch.clear();
    tag_scratch.extend(row.fields.iter().filter_map(|f| match &f.value {
        FieldData::Tag(v) => Some((f.id.get(), v.as_ref())),
        _ => None,
    }));
    tag_scratch.sort_unstable_by_key(|(id, _)| *id);
    key_scratch.clear();
    for (id, v) in tag_scratch.iter() {
        key_scratch.extend_from_slice(&id.to_be_bytes());
        key_scratch.extend_from_slice(&(v.len() as u32).to_be_bytes());
        key_scratch.extend_from_slice(v.as_bytes());
    }
    key_scratch.extend_from_slice(&row.time.to_be_bytes());

    let (k0, k1) = *ROW_KEY_HASH_KEYS;
    let mut hasher = siphasher::sip128::SipHasher13::new_with_keys(k0, k1);
    hasher.write(key_scratch);
    hasher.finish128().as_u128()
}

// Test infrastructure for configurable string size limit - thread-local for test isolation.
#[cfg(test)]
thread_local! {
    static TEST_VAR_COL_MAX_BYTES: std::cell::Cell<usize> = const {
        std::cell::Cell::new(influxdb3_types::arrow_limits::ARROW_VAR_COL_MAX_BYTES)
    };
}

/// Returns the variable-column byte capacity limit.
fn var_col_max_bytes() -> usize {
    #[cfg(test)]
    {
        TEST_VAR_COL_MAX_BYTES.with(|c| c.get())
    }
    #[cfg(not(test))]
    {
        influxdb3_types::arrow_limits::ARROW_VAR_COL_MAX_BYTES
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct VarColMaxGuard(usize);

#[cfg(test)]
impl VarColMaxGuard {
    pub(crate) fn new(cap: usize) -> Self {
        let prev = TEST_VAR_COL_MAX_BYTES.with(|c| {
            let prev = c.get();
            c.set(cap);
            prev
        });
        Self(prev)
    }
}

#[cfg(test)]
impl Drop for VarColMaxGuard {
    fn drop(&mut self) {
        TEST_VAR_COL_MAX_BYTES.with(|c| c.set(self.0));
    }
}

impl MutableTableChunk {
    fn new() -> Self {
        Self {
            timestamp_min: i64::MAX,
            timestamp_max: i64::MIN,
            data: Default::default(),
            row_count: 0,
            string_bytes_per_column: HashMap::new(),
            live: BooleanBufferBuilder::new(0),
            superseded_count: 0,
            superseded_since_watermark: None,
        }
    }

    /// Mark `row` as replaced by a later duplicate.
    fn mark_superseded(&mut self, row: usize) {
        debug_assert!(self.live.get_bit(row), "row {row} superseded twice");
        self.live.set_bit(row, false);
        self.superseded_count += 1;
        self.superseded_since_watermark =
            Some(self.superseded_since_watermark.map_or(row, |w| w.min(row)));
    }

    /// See the `superseded_since_watermark` field.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "read by a materialization cache that does not exist yet"
        )
    )]
    pub(crate) fn superseded_watermark(&self) -> Option<usize> {
        self.superseded_since_watermark
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "reset by a materialization cache that does not exist yet"
        )
    )]
    pub(crate) fn reset_superseded_watermark(&mut self) {
        self.superseded_since_watermark = None;
    }

    fn would_exceed_limit_with(&self, incoming_per_column: &HashMap<ColumnId, usize>) -> bool {
        let limit = var_col_max_bytes();
        incoming_per_column.iter().any(|(col_id, additional)| {
            let existing = self
                .string_bytes_per_column
                .get(col_id)
                .copied()
                .unwrap_or(0);
            existing.saturating_add(*additional) > limit
        })
    }
}

impl MutableTableChunk {
    /// Append one row, returning its row index. `carry` names the row this
    /// one replaces (if any) so fields the new row does not provide are
    /// carried forward — from this chunk or from an earlier split chunk of
    /// the same chunk_time. The caller (`ChunkTimeBuffer`) owns duplicate
    /// detection and superseded marking.
    fn append_row(&mut self, r: &Row, carry: CarrySource<'_>) -> usize {
        let this_row = self.row_count;
        // Exact capacity for builders created by this row avoids the default
        // 1024-element allocation, which adds up across many small chunks in
        // sparse time-series data.
        let builder_capacity = this_row + 1;

        {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in &r.fields {
                value_added.insert(f.id);

                match &f.value {
                    FieldData::Timestamp(v) => {
                        self.timestamp_min = self.timestamp_min.min(*v);
                        self.timestamp_max = self.timestamp_max.max(*v);

                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut time_builder =
                                TimestampNanosecondBuilder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            time_builder.append_nulls(this_row);
                            Builder::Time(time_builder)
                        });
                        if let Builder::Time(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Tag(v) => {
                        *self.string_bytes_per_column.entry(f.id).or_default() += v.len();

                        if let Entry::Vacant(e) = self.data.entry(f.id) {
                            let mut tag_builder = StringDictionaryBuilder::with_capacity(
                                builder_capacity,
                                builder_capacity.min(1024),
                                (builder_capacity * 64).min(1024),
                            );
                            // append nulls for all previous rows
                            tag_builder.append_nulls(this_row);
                            e.insert(Builder::Tag(tag_builder));
                        }
                        let b = self.data.get_mut(&f.id).expect("tag builder should exist");
                        if let Builder::Tag(b) = b {
                            b.append(v)
                                .expect("shouldn't be able to overflow 32 bit dictionary");
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::String(v) => {
                        *self.string_bytes_per_column.entry(f.id).or_default() += v.len();

                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut string_builder = StringBuilder::with_capacity(
                                builder_capacity,
                                (builder_capacity * 64).min(1024),
                            );
                            // append nulls for all previous rows
                            string_builder.append_nulls(this_row);
                            Builder::String(string_builder)
                        });
                        if let Builder::String(b) = b {
                            b.append_value(v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Integer(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut int_builder = Int64Builder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            int_builder.append_nulls(this_row);
                            Builder::I64(int_builder)
                        });
                        if let Builder::I64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::UInteger(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut uint_builder = UInt64Builder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            uint_builder.append_nulls(this_row);
                            Builder::U64(uint_builder)
                        });
                        if let Builder::U64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Float(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut float_builder = Float64Builder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            float_builder.append_nulls(this_row);
                            Builder::F64(float_builder)
                        });
                        if let Builder::F64(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Boolean(v) => {
                        let b = self.data.entry(f.id).or_insert_with(|| {
                            let mut bool_builder = BooleanBuilder::with_capacity(builder_capacity);
                            // append nulls for all previous rows
                            bool_builder.append_nulls(this_row);
                            Builder::Bool(bool_builder)
                        });
                        if let Builder::Bool(b) = b {
                            b.append_value(*v);
                        } else {
                            panic!("unexpected field type");
                        }
                    }
                    FieldData::Key(_) => unreachable!("key type should never be constructed"),
                }
            }

            // Columns the row doesn't provide: an overwrite carries the
            // replaced row's value forward (line protocol unions the field
            // sets, conflicts favoring the new write); a fresh row gets null.
            for (column_id, builder) in &mut self.data {
                if !value_added.contains(column_id) {
                    let copied_bytes = match &carry {
                        CarrySource::None => {
                            builder.append_null();
                            0
                        }
                        CarrySource::Local(old) => {
                            builder.append_copied(builder_read(builder, *old))
                        }
                        CarrySource::Foreign(src_chunk, old) => match src_chunk.data.get(column_id)
                        {
                            Some(src) => builder.append_copied(builder_read(src, *old)),
                            None => {
                                builder.append_null();
                                0
                            }
                        },
                    };
                    if copied_bytes > 0 {
                        *self.string_bytes_per_column.entry(*column_id).or_default() +=
                            copied_bytes;
                    }
                }
            }

            // A foreign carry can reference columns this chunk has never
            // seen (they were only ever written before the split). Create
            // them so the union does not drop fields across the split.
            if let CarrySource::Foreign(src_chunk, old) = &carry {
                for (column_id, src) in &src_chunk.data {
                    if !value_added.contains(column_id) && !self.data.contains_key(column_id) {
                        let mut builder = src.new_null_prefilled(this_row);
                        let copied_bytes = builder.append_copied(builder_read(src, *old));
                        if copied_bytes > 0 {
                            *self.string_bytes_per_column.entry(*column_id).or_default() +=
                                copied_bytes;
                        }
                        self.data.insert(*column_id, builder);
                    }
                }
            }
        }

        self.live.append(true);
        self.row_count += 1;
        this_row
    }

    fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax::new(self.timestamp_min, self.timestamp_max)
    }

    fn record_batch(&self, table_def: Arc<TableDefinition>) -> Result<RecordBatch> {
        self.materialize(table_def, 0..self.row_count)
    }

    /// Produce the live rows of `rows` as an immutable batch in table-schema
    /// order. Rows superseded by a later duplicate are skipped in the same
    /// pass that copies the builders, so no consumer (query or persist) sees
    /// two versions of one point from this chunk and no second copy is made
    /// to hide them. A chunk with no superseded rows takes the plain
    /// `finish_cloned` path.
    pub(crate) fn materialize(
        &self,
        table_def: Arc<TableDefinition>,
        rows: Range<usize>,
    ) -> Result<RecordBatch> {
        if rows.start > rows.end || rows.end > self.row_count {
            return Err(Error::RowRangeOutOfBounds {
                start: rows.start,
                end: rows.end,
                row_count: self.row_count,
            });
        }
        let schema = table_def.schema.as_arrow();
        let table_def = legacy::TableDefinition::new(table_def);
        // The mask view and its runs are built once and shared by every column.
        let live = self.live_rows(&rows);

        let mut cols = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let column_def = table_def
                .column_definition(f.name())
                .expect("a valid column name");
            let col = match self.data.get(&column_def.id) {
                Some(b) => self.column_for(b, &live)?,
                None => array_ref_nulls_for_type(column_def.data_type, live.kept),
            };
            cols.push(col);
        }
        Ok(RecordBatch::try_new(schema, cols)?)
    }

    /// The live rows of `rows`: `None` runs when nothing in the chunk is
    /// superseded (every row of the range is live).
    fn live_rows(&self, rows: &Range<usize>) -> LiveRows {
        if self.superseded_count == 0 {
            LiveRows {
                rows: rows.clone(),
                kept: rows.len(),
                runs: None,
            }
        } else {
            LiveRows::masked(&self.live, rows)
        }
    }

    /// The live rows of one builder as an immutable array.
    fn column_for(&self, b: &Builder, live: &LiveRows) -> Result<ArrayRef> {
        match &live.runs {
            None => {
                let arr = b.as_arrow();
                Ok(if live.rows.len() == self.row_count {
                    arr
                } else {
                    arr.slice(live.rows.start, live.rows.len())
                })
            }
            Some(_) => b.as_arrow_kept(live),
        }
    }

    fn into_schema_record_batch(self, table_def: Arc<TableDefinition>) -> (Schema, RecordBatch) {
        let live = self.live_rows(&(0..self.row_count));
        let kept = live.kept;
        let Self { data, .. } = self;
        let table_def = legacy::TableDefinition::new(table_def);
        let mut cols = Vec::with_capacity(data.len());
        let mut schema_builder = SchemaBuilder::new();
        let mut cols_in_batch = HashSet::new();
        for (col_id, mut builder) in data.into_iter() {
            cols_in_batch.insert(col_id);
            let col_type = builder.influx_column_type();
            // Superseded rows are skipped in the copy itself; with none, the
            // builders are drained into the arrays without a copy.
            let col = if live.runs.is_none() {
                builder.finish()
            } else {
                builder
                    .as_arrow_kept(&live)
                    .expect("filtering superseded rows should never fail")
            };
            schema_builder.influx_column(
                table_def
                    .column_id_to_name(&col_id)
                    .expect("valid column id")
                    .as_ref(),
                col_type,
            );
            cols.push(col);
            schema_builder.with_series_key(&table_def.inner().series_key_names);
        }

        // ensure that every field column is present in the batch
        for (col_id, col_def) in table_def.columns.iter() {
            if !cols_in_batch.contains(col_id) {
                schema_builder.influx_column(col_def.name.as_ref(), col_def.data_type);
                let col = array_ref_nulls_for_type(col_def.data_type, kept);

                cols.push(col);
            }
        }
        let schema = schema_builder
            .build()
            .expect("should always be able to build schema");
        let arrow_schema = schema.as_arrow();

        let batch = RecordBatch::try_new(arrow_schema, cols)
            .expect("should always be able to build record batch");

        (schema, batch)
    }
}

/// The live rows of one materialization, computed once and shared by every
/// column of the batch.
struct LiveRows {
    rows: Range<usize>,
    /// Number of live rows in `rows`.
    kept: usize,
    /// `None` when every row of `rows` is live. Otherwise the mask bits of
    /// `rows` and its maximal runs of live rows as absolute row ranges:
    /// copying run by run is what keeps the kernels at memcpy speed when
    /// superseded rows are sparse or clustered, rather than a branch per row.
    runs: Option<(BooleanBuffer, Vec<Range<usize>>)>,
}

impl LiveRows {
    /// Read the `live` bits of `rows` (one copy of `rows.len() / 8` bytes).
    fn masked(live: &BooleanBufferBuilder, rows: &Range<usize>) -> Self {
        let bits = BooleanBuffer::new(Buffer::from(live.as_slice()), rows.start, rows.len());
        let runs: Vec<Range<usize>> = bits
            .set_slices()
            .map(|(s, e)| rows.start + s..rows.start + e)
            .collect();
        Self {
            rows: rows.clone(),
            kept: bits.count_set_bits(),
            runs: Some((bits, runs)),
        }
    }
}

/// Copy the validity bits of `runs` out of a builder's bitmap, or `None`
/// when the builder has never seen a null.
fn kept_nulls(validity: Option<&[u8]>, runs: &[Range<usize>], kept: usize) -> Option<NullBuffer> {
    let validity = validity?;
    let mut out = BooleanBufferBuilder::new(kept);
    for r in runs {
        out.append_packed_range(r.clone(), validity);
    }
    Some(NullBuffer::new(out.finish()))
}

/// Copy the live rows out of a primitive builder, one memcpy per run.
fn kept_primitive<T: ArrowPrimitiveType>(
    b: &PrimitiveBuilder<T>,
    runs: &[Range<usize>],
    kept: usize,
) -> PrimitiveArray<T> {
    let values = b.values_slice();
    let mut out = MutableBuffer::with_capacity(kept * size_of::<T::Native>());
    for r in runs {
        out.extend_from_slice(&values[r.clone()]);
    }
    let values = ScalarBuffer::<T::Native>::new(out.into(), 0, kept);
    PrimitiveArray::<T>::new(values, kept_nulls(b.validity_slice(), runs, kept))
}

/// Copy the live rows out of a boolean builder, one packed bit range per run.
fn kept_bool(b: &BooleanBuilder, runs: &[Range<usize>], kept: usize) -> BooleanArray {
    let values = b.values_slice();
    let mut out = BooleanBufferBuilder::new(kept);
    for r in runs {
        out.append_packed_range(r.clone(), values);
    }
    BooleanArray::new(out.finish(), kept_nulls(b.validity_slice(), runs, kept))
}

/// Copy the live rows out of a string builder: one memcpy of bytes per run,
/// offsets rebased as they are copied.
fn kept_string(
    b: &GenericByteBuilder<GenericStringType<i32>>,
    runs: &[Range<usize>],
    kept: usize,
) -> StringArray {
    let values = b.values_slice();
    let offsets = b.offsets_slice();
    let bytes: usize = runs
        .iter()
        .map(|r| (offsets[r.end] - offsets[r.start]) as usize)
        .sum();
    let mut out_values = MutableBuffer::with_capacity(bytes);
    let mut out_offsets = Vec::<i32>::with_capacity(kept + 1);
    out_offsets.push(0);
    for r in runs {
        let (first, last) = (offsets[r.start], offsets[r.end]);
        out_values.extend_from_slice(&values[first as usize..last as usize]);
        let rebase = out_offsets[out_offsets.len() - 1] - first;
        out_offsets.extend(offsets[r.start + 1..=r.end].iter().map(|o| o + rebase));
    }
    // The bytes were written by a `StringBuilder`, so they are valid UTF-8 and
    // the offsets are monotonic; `new` only re-checks that.
    StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(out_offsets)),
        out_values.into(),
        kept_nulls(b.validity_slice(), runs, kept),
    )
}

fn array_ref_nulls_for_type(data_type: InfluxColumnType, len: usize) -> ArrayRef {
    match data_type {
        InfluxColumnType::Field(InfluxFieldType::Boolean) => {
            let mut builder = BooleanBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Timestamp => {
            let mut builder = TimestampNanosecondBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Tag => {
            let mut builder: StringDictionaryBuilder<Int32Type> = StringDictionaryBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::Integer) => {
            let mut builder = Int64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::Float) => {
            let mut builder = Float64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::String) => {
            let mut builder = StringBuilder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
        InfluxColumnType::Field(InfluxFieldType::UInteger) => {
            let mut builder = UInt64Builder::new();
            builder.append_nulls(len);
            Arc::new(builder.finish())
        }
    }
}

// Debug implementation for TableBuffer
impl std::fmt::Debug for MutableTableChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutableTableChunk")
            .field("timestamp_min", &self.timestamp_min)
            .field("timestamp_max", &self.timestamp_max)
            .field("row_count", &self.row_count)
            .finish()
    }
}

/// A value read out of a builder slot for carry-forward. Owned, so the
/// source borrow ends before the (possibly same) destination is appended.
enum CopiedValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    U64(u64),
    Time(i64),
    String(String),
}

/// Read the value at `idx` from a builder. Tag columns read as null: a
/// duplicate row carries the same tag set as the row it replaces, so a tag
/// column missing from the new row was missing from the old one too.
fn builder_read(b: &Builder, idx: usize) -> CopiedValue {
    fn is_valid(validity: Option<&[u8]>, idx: usize) -> bool {
        validity.is_none_or(|v| v[idx / 8] & (1 << (idx % 8)) != 0)
    }
    match b {
        Builder::Bool(b) => {
            if is_valid(b.validity_slice(), idx) {
                CopiedValue::Bool(b.values_slice()[idx / 8] & (1 << (idx % 8)) != 0)
            } else {
                CopiedValue::Null
            }
        }
        Builder::I64(b) => {
            if is_valid(b.validity_slice(), idx) {
                CopiedValue::I64(b.values_slice()[idx])
            } else {
                CopiedValue::Null
            }
        }
        Builder::F64(b) => {
            if is_valid(b.validity_slice(), idx) {
                CopiedValue::F64(b.values_slice()[idx])
            } else {
                CopiedValue::Null
            }
        }
        Builder::U64(b) => {
            if is_valid(b.validity_slice(), idx) {
                CopiedValue::U64(b.values_slice()[idx])
            } else {
                CopiedValue::Null
            }
        }
        Builder::Time(b) => {
            if is_valid(b.validity_slice(), idx) {
                CopiedValue::Time(b.values_slice()[idx])
            } else {
                CopiedValue::Null
            }
        }
        Builder::String(b) => {
            if is_valid(b.validity_slice(), idx) {
                let offsets = b.offsets_slice();
                let start = offsets[idx] as usize;
                let end = offsets[idx + 1] as usize;
                CopiedValue::String(
                    std::str::from_utf8(&b.values_slice()[start..end])
                        .expect("buffered string values are valid utf8")
                        .to_string(),
                )
            } else {
                CopiedValue::Null
            }
        }
        Builder::Tag(_) => CopiedValue::Null,
    }
}

pub(super) enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

impl Builder {
    fn as_arrow(&self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish_cloned()),
            Self::I64(b) => Arc::new(b.finish_cloned()),
            Self::F64(b) => Arc::new(b.finish_cloned()),
            Self::U64(b) => Arc::new(b.finish_cloned()),
            Self::String(b) => Arc::new(b.finish_cloned()),
            Self::Tag(b) => Arc::new(b.finish_cloned()),
            Self::Time(b) => Arc::new(b.finish_cloned()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Builder::Bool(b) => b.append_null(),
            Builder::I64(b) => b.append_null(),
            Builder::F64(b) => b.append_null(),
            Builder::U64(b) => b.append_null(),
            Builder::String(b) => b.append_null(),
            Builder::Tag(b) => b.append_null(),
            Builder::Time(b) => b.append_null(),
        }
    }

    /// Append a previously read value (see [`builder_read`]), or null.
    /// Returns the number of variable-length bytes appended, for the
    /// caller's string-size accounting.
    fn append_copied(&mut self, value: CopiedValue) -> usize {
        match (self, value) {
            (b, CopiedValue::Null) => {
                b.append_null();
                0
            }
            (Builder::Bool(b), CopiedValue::Bool(v)) => {
                b.append_value(v);
                0
            }
            (Builder::I64(b), CopiedValue::I64(v)) => {
                b.append_value(v);
                0
            }
            (Builder::F64(b), CopiedValue::F64(v)) => {
                b.append_value(v);
                0
            }
            (Builder::U64(b), CopiedValue::U64(v)) => {
                b.append_value(v);
                0
            }
            (Builder::Time(b), CopiedValue::Time(v)) => {
                b.append_value(v);
                0
            }
            (Builder::String(b), CopiedValue::String(v)) => {
                let len = v.len();
                b.append_value(v);
                len
            }
            _ => panic!("unexpected field type"),
        }
    }

    /// An empty builder of the same variant as `self`, prefilled with
    /// `rows` nulls. Used when a carry-forward references a column this
    /// chunk has not seen yet.
    fn new_null_prefilled(&self, rows: usize) -> Builder {
        let mut b = match self {
            Builder::Bool(_) => Builder::Bool(BooleanBuilder::with_capacity(rows + 1)),
            Builder::I64(_) => Builder::I64(Int64Builder::with_capacity(rows + 1)),
            Builder::F64(_) => Builder::F64(Float64Builder::with_capacity(rows + 1)),
            Builder::U64(_) => Builder::U64(UInt64Builder::with_capacity(rows + 1)),
            Builder::String(_) => Builder::String(StringBuilder::with_capacity(rows + 1, 1024)),
            Builder::Tag(_) => Builder::Tag(StringDictionaryBuilder::new()),
            Builder::Time(_) => Builder::Time(TimestampNanosecondBuilder::with_capacity(rows + 1)),
        };
        for _ in 0..rows {
            b.append_null();
        }
        b
    }

    fn influx_column_type(&self) -> InfluxColumnType {
        match self {
            Self::Bool(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
            Self::I64(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
            Self::F64(_) => InfluxColumnType::Field(InfluxFieldType::Float),
            Self::U64(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
            Self::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
            Self::Tag(_) => InfluxColumnType::Tag,
            Self::Time(_) => InfluxColumnType::Timestamp,
        }
    }

    /// Drain the builder into an immutable array without copying; the
    /// builder is left empty.
    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish()),
            Self::I64(b) => Arc::new(b.finish()),
            Self::F64(b) => Arc::new(b.finish()),
            Self::U64(b) => Arc::new(b.finish()),
            Self::String(b) => Arc::new(b.finish()),
            Self::Tag(b) => Arc::new(b.finish()),
            Self::Time(b) => Arc::new(b.finish()),
        }
    }

    /// The live rows of `live.rows` as an immutable array, copied out of the
    /// builder in a single pass that skips rows whose `live` bit is clear —
    /// one bulk copy per run of live rows. Dictionary (tag) columns clone
    /// their keys and dictionary and filter the keys, which is cheap relative
    /// to a value column.
    fn as_arrow_kept(&self, live: &LiveRows) -> Result<ArrayRef> {
        let (bits, runs) = live
            .runs
            .as_ref()
            .expect("as_arrow_kept is only called with a superseded-row mask");
        let (rows, kept) = (&live.rows, live.kept);
        Ok(match self {
            Self::Tag(b) => {
                let arr = b.finish_cloned().slice(rows.start, rows.len());
                let mask = BooleanArray::new(bits.clone(), None);
                arrow::compute::filter(&arr, &mask)?
            }
            Self::Bool(b) => Arc::new(kept_bool(b, runs, kept)),
            Self::I64(b) => Arc::new(kept_primitive(b, runs, kept)),
            Self::F64(b) => Arc::new(kept_primitive(b, runs, kept)),
            Self::U64(b) => Arc::new(kept_primitive(b, runs, kept)),
            Self::Time(b) => Arc::new(kept_primitive(b, runs, kept)),
            Self::String(b) => Arc::new(kept_string(b, runs, kept)),
        })
    }

    fn size(&self) -> usize {
        let data_size = match self {
            Self::Bool(b) => b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0),
            Self::I64(b) => {
                size_of::<i64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::F64(b) => {
                size_of::<f64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::U64(b) => {
                size_of::<u64>() * b.capacity() + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::String(b) => {
                b.values_slice().len()
                    + b.offsets_slice().len()
                    + b.validity_slice().map(|s| s.len()).unwrap_or(0)
            }
            Self::Tag(b) => {
                let b = b.finish_cloned();
                b.keys().len() * size_of::<i32>() + b.values().get_array_memory_size()
            }
            Self::Time(b) => size_of::<i64>() * b.capacity(),
        };
        size_of::<Self>() + data_size
    }
}

#[cfg(test)]
mod tests;
