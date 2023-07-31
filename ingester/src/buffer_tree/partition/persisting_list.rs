use std::collections::VecDeque;

use arrow::record_batch::RecordBatch;
use data_types::TimestampMinMax;
use schema::{merge::SchemaMerger, Schema};

use crate::query::projection::OwnedProjection;

use super::{
    buffer::{traits::Queryable, BufferState, Persisting},
    persisting::BatchIdent,
};

/// An ordered list of buffered, persisting data as [`BufferState<Persisting>`]
/// FSM instances.
///
/// This type maintains a cache of row count & timestamp min/max statistics
/// across all persisting batches, and performs incremental computation at
/// persist time, moving it out of the query execution path.
#[derive(Debug)]
pub(crate) struct PersistingList {
    /// The currently persisting [`DataBuffer`] instances, if any.
    ///
    /// This queue is ordered from newest at the head, to oldest at the tail -
    /// forward iteration order matches write order.
    ///
    /// The [`BatchIdent`] is a generational counter that is used to tag each
    /// persisting with a unique, opaque, monotonic identifier.
    ///
    /// [`DataBuffer`]: super::buffer::DataBuffer
    persisting: VecDeque<(BatchIdent, BufferState<Persisting>)>,

    cached: Option<CachedStats>,
}

impl Default for PersistingList {
    fn default() -> Self {
        Self {
            persisting: VecDeque::with_capacity(1),
            cached: None,
        }
    }
}

impl PersistingList {
    /// Add this `buffer` which was assigned `ident` when marked as persisting
    /// to the list.
    ///
    /// This call incrementally recomputes the cached data statistics.
    ///
    /// # Panics
    ///
    /// Panics if a batch with a later `ident` has already been added to this
    /// list - calls MUST push ordered buffers/idents to maintain correct
    /// ordering of row updates across batches.
    ///
    /// The provided buffer MUST be non-empty (containing a timestamp column,
    /// and a schema)
    pub(crate) fn push(&mut self, ident: BatchIdent, buffer: BufferState<Persisting>) {
        // Recompute the statistics.
        match &mut self.cached {
            Some(v) => v.push(&buffer),
            None => {
                // Set the cached stats, as there's no other stats to merge
                // with, so skip merging schemas.
                self.cached = Some(CachedStats {
                    rows: buffer.rows(),
                    timestamps: buffer
                        .timestamp_stats()
                        .expect("persisting batch must contain timestamps"),
                    schema: buffer.schema().expect("persisting batch must have schema"),
                });
            }
        }

        // Invariant: the batch being added MUST be ordered strictly after
        // existing batches.
        //
        // The BatchIdent provides this ordering assurance, as it is a monotonic
        // (opaque) identifier.
        assert!(self
            .persisting
            .back()
            .map(|(last, _)| ident > *last)
            .unwrap_or(true));

        self.persisting.push_back((ident, buffer));
    }

    /// Remove the buffer identified by `ident` from the list.
    ///
    /// There is no ordering requirement for this call, but is more efficient
    /// when removals match the order of calls to [`PersistingList::push()`].
    ///
    /// # Panics
    ///
    /// This method panics if there is currently no batch identified by `ident`
    /// in the list.
    pub(crate) fn remove(&mut self, ident: BatchIdent) -> BufferState<Persisting> {
        let idx = self
            .persisting
            .iter()
            .position(|(old, _)| *old == ident)
            .expect("no currently persisting batch");

        let (old_ident, fsm) = self.persisting.remove(idx).unwrap();
        assert_eq!(old_ident, ident);

        // Recompute the cache of all remaining persisting batch stats (if any)
        self.cached = CachedStats::new(self.persisting.iter().map(|(_, v)| v));

        fsm
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.persisting.is_empty()
    }

    /// Returns the row count sum across all batches in this list.
    ///
    /// This is an `O(1)` operation.
    pub(crate) fn rows(&self) -> usize {
        self.cached.as_ref().map(|v| v.rows).unwrap_or_default()
    }

    /// Returns the timestamp min/max values across all batches in this list.
    ///
    /// This is an `O(1)` operation.
    pub(crate) fn timestamp_stats(&self) -> Option<TimestampMinMax> {
        self.cached.as_ref().map(|v| v.timestamps)
    }

    /// Returns the merged schema of all batches in this list.
    ///
    /// This is an `O(1)` operation.
    pub(crate) fn schema(&self) -> Option<&Schema> {
        self.cached.as_ref().map(|v| &v.schema)
    }

    /// Returns the [`RecordBatch`] in this list, optionally applying the given
    /// projection.
    ///
    /// This is an `O(n)` operation.
    pub(crate) fn get_query_data<'a, 'b: 'a>(
        &'a self,
        projection: &'b OwnedProjection,
    ) -> impl Iterator<Item = RecordBatch> + 'a {
        self.persisting
            .iter()
            .flat_map(move |(_, b)| b.get_query_data(projection))
    }
}

/// The set of cached statistics describing the batches of data within the
/// [`PersistingList`].
#[derive(Debug)]
struct CachedStats {
    rows: usize,
    timestamps: TimestampMinMax,

    /// The merged schema of all the persisting batches.
    schema: Schema,
}

impl CachedStats {
    /// Generate a new [`CachedStats`] from an iterator of batches, if any.
    ///
    /// # Panics
    ///
    /// If any batches are empty (containing no schema or timestamp column), or
    /// the batches do not contain compatible schemas, this call panics.
    fn new<'a, T>(mut iter: T) -> Option<Self>
    where
        T: Iterator<Item = &'a BufferState<Persisting>> + 'a,
    {
        let v = iter.next()?;

        let mut schema = SchemaMerger::new();
        schema = schema
            .merge(&v.schema().expect("persisting batch must be non-empty"))
            .unwrap();

        let mut rows = v.rows();
        debug_assert!(rows > 0);

        let mut timestamps = v
            .timestamp_stats()
            .expect("unprojected batch should have timestamp");

        for buf in iter {
            rows += buf.rows();
            if let Some(v) = buf.schema() {
                debug_assert!(buf.rows() > 0);

                schema = schema
                    .merge(&v)
                    .expect("persit list contains incompatible schemas");

                let ts = buf
                    .timestamp_stats()
                    .expect("no timestamp for bach containing rows");

                timestamps.min = timestamps.min.min(ts.min);
                timestamps.max = timestamps.max.max(ts.max);
            }
        }

        Some(Self {
            rows,
            timestamps,
            schema: schema.build(),
        })
    }

    // Incrementally recompute the cached stats by adding `buffer` to the
    // statistics.
    fn push(&mut self, buffer: &BufferState<Persisting>) {
        // This re-computation below MUST complete - no early exit is allowed or
        // the stats will be left in an inconsistent state.

        self.rows += buffer.rows();

        let ts = buffer
            .timestamp_stats()
            .expect("persisting batch must contain timestamps");

        self.timestamps.min = self.timestamps.min.min(ts.min);
        self.timestamps.max = self.timestamps.max.max(ts.max);

        let mut schema = SchemaMerger::new();
        schema = schema.merge(&self.schema).unwrap();
        schema = schema
            .merge(&buffer.schema().expect("persisting batch must have schema"))
            .expect("incompatible schema");
        self.schema = schema.build()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow_util::assert_batches_eq;
    use assert_matches::assert_matches;
    use data_types::SequenceNumber;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;

    use crate::buffer_tree::partition::buffer::Transition;

    use super::*;

    /// Ensure the ordering of yielded batches matches that of the calls to
    /// push(), preserving batch ordering, and in turn, causal row ordering.
    #[test]
    fn test_batch_ordering() {
        let mut list = PersistingList::default();
        let mut ident_oracle = BatchIdent::default();

        assert!(list.is_empty());

        // Generate a buffer with a single row.
        let buffer = buffer_with_lp(r#"bananas,tag=platanos great="yes" 42"#);

        // Add it to the list.
        list.push(ident_oracle.next(), buffer);

        // The statistics must now match the expected values.
        assert!(!list.is_empty());
        assert_eq!(list.rows(), 1);
        assert_matches!(
            list.timestamp_stats(),
            Some(TimestampMinMax { min: 42, max: 42 })
        );
        assert_schema_matches(list.schema().unwrap(), &["time", "great", "tag"]);

        // Assert the row content
        let data = list
            .get_query_data(&OwnedProjection::default())
            .collect::<Vec<_>>();
        let expected = vec![
            "+-------+----------+--------------------------------+",
            "| great | tag      | time                           |",
            "+-------+----------+--------------------------------+",
            "| yes   | platanos | 1970-01-01T00:00:00.000000042Z |",
            "+-------+----------+--------------------------------+",
        ];
        assert_eq!(data.len(), 1);
        assert_batches_eq!(&expected, &data);

        // Push a new buffer updating the last row to check yielded row ordering.
        let buffer = buffer_with_lp(r#"bananas,tag=platanos great="definitely" 42"#);
        list.push(ident_oracle.next(), buffer);

        // The statistics must now match the expected values.
        assert!(!list.is_empty());
        assert_eq!(list.rows(), 2);
        assert_matches!(
            list.timestamp_stats(),
            Some(TimestampMinMax { min: 42, max: 42 })
        );
        assert_schema_matches(list.schema().unwrap(), &["time", "great", "tag"]);

        // Assert the row content
        let data = list
            .get_query_data(&OwnedProjection::default())
            .collect::<Vec<_>>();
        let expected = vec![
            "+------------+----------+--------------------------------+",
            "| great      | tag      | time                           |",
            "+------------+----------+--------------------------------+",
            "| yes        | platanos | 1970-01-01T00:00:00.000000042Z |",
            "| definitely | platanos | 1970-01-01T00:00:00.000000042Z |",
            "+------------+----------+--------------------------------+",
        ];
        assert_eq!(data.len(), 2);
        assert_batches_eq!(&expected, &data);
    }

    /// Assert projection across batches works, and does not panic when given a
    /// missing column.
    #[test]
    fn test_projection() {
        let mut list = PersistingList::default();
        let mut ident_oracle = BatchIdent::default();

        assert!(list.is_empty());

        // Populate the list.
        list.push(
            ident_oracle.next(),
            buffer_with_lp(
                "\
                bananas,tag=platanos v=1 42\n\
                bananas,tag=platanos v=2,bananas=100 4242\n\
            ",
            ),
        );

        list.push(
            ident_oracle.next(),
            buffer_with_lp(
                "\
                bananas,tag=platanos v=3 424242\n\
                bananas v=4,bananas=200 42424242\n\
            ",
            ),
        );

        // Assert the row content
        let data = list
            .get_query_data(&OwnedProjection::from(vec!["time", "tag", "missing"]))
            .collect::<Vec<_>>();
        let expected = vec![
            "+--------------------------------+----------+",
            "| time                           | tag      |",
            "+--------------------------------+----------+",
            "| 1970-01-01T00:00:00.000000042Z | platanos |",
            "| 1970-01-01T00:00:00.000004242Z | platanos |",
            "| 1970-01-01T00:00:00.000424242Z | platanos |",
            "| 1970-01-01T00:00:00.042424242Z |          |",
            "+--------------------------------+----------+",
        ];
        assert_batches_eq!(&expected, &data);
    }

    /// Validate the cached statistics as batches are added and removed.
    #[test]
    fn test_cached_statistics() {
        let mut list = PersistingList::default();
        let mut ident_oracle = BatchIdent::default();

        assert!(list.is_empty());

        // Generate a buffer with a single row.
        let first_batch = ident_oracle.next();
        list.push(
            first_batch,
            buffer_with_lp(r#"bananas,tag=platanos great="yes" 42"#),
        );

        // The statistics must now match the expected values.
        assert!(!list.is_empty());
        assert_eq!(list.rows(), 1);
        assert_matches!(
            list.timestamp_stats(),
            Some(TimestampMinMax { min: 42, max: 42 })
        );
        assert_schema_matches(list.schema().unwrap(), &["time", "great", "tag"]);

        // Push another row.
        let second_batch = ident_oracle.next();
        list.push(
            second_batch,
            buffer_with_lp(r#"bananas,another=yes great="definitely",incremental=true 4242"#),
        );

        // The statistics must now match the expected values.
        assert!(!list.is_empty());
        assert_eq!(list.rows(), 2);
        assert_matches!(
            list.timestamp_stats(),
            Some(TimestampMinMax { min: 42, max: 4242 })
        );
        assert_schema_matches(
            list.schema().unwrap(),
            &["time", "great", "tag", "another", "incremental"],
        );

        // Remove the first batch.
        list.remove(first_batch);

        // The statistics must now match the second batch values.
        assert!(!list.is_empty());
        assert_eq!(list.rows(), 1);
        assert_matches!(
            list.timestamp_stats(),
            Some(TimestampMinMax {
                min: 4242,
                max: 4242
            })
        );
        assert_schema_matches(
            list.schema().unwrap(),
            &["time", "great", "another", "incremental"],
        );

        // Remove the second/final batch.
        list.remove(second_batch);

        assert!(list.is_empty());
        assert_eq!(list.rows(), 0);
        assert_matches!(list.timestamp_stats(), None);
        assert_matches!(list.schema(), None);
    }

    /// Assert the schema columns match the given names.
    fn assert_schema_matches(schema: &Schema, cols: &[&str]) {
        let schema = schema.as_arrow();
        let got = schema
            .all_fields()
            .into_iter()
            .map(|v| v.name().to_owned())
            .collect::<BTreeSet<_>>();

        let want = cols
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();

        assert_eq!(got, want);
    }

    /// Return a persisting buffer containing the given LP content.
    fn buffer_with_lp(lp: &str) -> BufferState<Persisting> {
        let mut buffer = BufferState::new();
        // Write some data to a buffer.
        buffer
            .write(lp_to_mutable_batch(lp).1, SequenceNumber::new(0))
            .expect("write to empty buffer should succeed");

        // Convert the buffer into a persisting snapshot.
        match buffer.snapshot() {
            Transition::Ok(v) => v.into_persisting(),
            Transition::Unchanged(_) => panic!("did not transition to snapshot state"),
        }
    }
}
