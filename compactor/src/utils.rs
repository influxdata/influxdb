//! Helpers of the Compactor

use crate::query::QueryableParquetChunk;
use data_types::{ParquetFile, ParquetFileId, TableSchema, Timestamp, Tombstone, TombstoneId};
use observability_deps::tracing::*;
use parquet_file::{chunk::ParquetChunk, storage::ParquetStorage};
use schema::{sort::SortKey, Schema};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Wrapper of group of parquet files with their min time and total size
#[derive(Debug, Clone, PartialEq)]
pub struct GroupWithMinTimeAndSize {
    /// Parquet files and their metadata
    pub(crate) parquet_files: Vec<ParquetFile>,

    /// min time of all parquet_files
    pub(crate) min_time: Timestamp,

    /// total size of all file
    pub(crate) total_file_size_bytes: i64,

    /// true if this group was split from a group of many overlapped files
    pub(crate) overlapped_with_other_groups: bool,
}

impl GroupWithMinTimeAndSize {
    /// Make GroupWithMinTimeAndSize for a given set of parquet files
    pub fn new(files: Vec<ParquetFile>, overlaped: bool) -> Self {
        let mut group = Self {
            parquet_files: files,
            min_time: Timestamp::new(i64::MAX),
            total_file_size_bytes: 0,
            overlapped_with_other_groups: overlaped,
        };

        assert!(
            !group.parquet_files.is_empty(),
            "invalid empty group for computing min time and total size"
        );

        for file in &group.parquet_files {
            group.min_time = group.min_time.min(file.min_time);
            group.total_file_size_bytes += file.file_size_bytes;
        }

        group
    }
}

/// Wrapper of a parquet file and its tombstones
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ParquetFileWithTombstone {
    data: Arc<ParquetFile>,
    tombstones: Vec<Tombstone>,
}

impl std::ops::Deref for ParquetFileWithTombstone {
    type Target = Arc<ParquetFile>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl ParquetFileWithTombstone {
    /// Pair a [`ParquetFile`] with the specified [`Tombstone`] instances.
    pub fn new(data: Arc<ParquetFile>, tombstones: Vec<Tombstone>) -> Self {
        Self { data, tombstones }
    }

    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }

    /// Return true if there is no tombstone
    pub fn no_tombstones(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// Return ID of this parquet file
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.data.id
    }

    /// Return the tombstones as a slice of [`Tombstone`].
    pub fn tombstones(&self) -> &[Tombstone] {
        &self.tombstones
    }

    /// Return all tombstones as a map keyed by tombstone ID.
    pub fn tombstone_map(&self) -> BTreeMap<TombstoneId, Tombstone> {
        self.tombstones
            .iter()
            .map(|ts| (ts.id, ts.clone()))
            .collect()
    }

    /// Add more tombstones
    pub fn add_tombstones(&mut self, tombstones: Vec<Tombstone>) {
        self.tombstones.extend(tombstones);
    }

    /// Convert to a QueryableParquetChunk
    pub fn to_queryable_parquet_chunk(
        &self,
        store: ParquetStorage,
        table_name: String,
        table_schema: &TableSchema,
        partition_sort_key: Option<SortKey>,
    ) -> QueryableParquetChunk {
        let column_id_lookup = table_schema.column_id_map();
        let selection: Vec<_> = self
            .column_set
            .iter()
            .flat_map(|id| column_id_lookup.get(id).copied())
            .collect();
        let table_schema: Schema = table_schema
            .clone()
            .try_into()
            .expect("table schema is broken");
        let schema = table_schema
            .select_by_names(&selection)
            .expect("schema in-sync");
        let pk = schema.primary_key();
        let sort_key = partition_sort_key.as_ref().map(|sk| sk.filter_to(&pk));

        let parquet_chunk = ParquetChunk::new(Arc::clone(&self.data), Arc::new(schema), store);

        trace!(
            parquet_file_id=?self.id,
            parquet_file_sequencer_id=?self.sequencer_id,
            parquet_file_namespace_id=?self.namespace_id,
            parquet_file_table_id=?self.table_id,
            parquet_file_partition_id=?self.partition_id,
            parquet_file_object_store_id=?self.object_store_id,
            "built parquet chunk from metadata"
        );

        QueryableParquetChunk::new(
            table_name,
            self.data.partition_id,
            Arc::new(parquet_chunk),
            &self.tombstones,
            self.data.max_sequence_number,
            self.data.min_time,
            self.data.max_time,
            sort_key,
            partition_sort_key,
            self.data.compaction_level,
        )
    }
}

/// Compute time to split data
/// Return a list of times at which we want data to be split. The times are computed
/// based on the max_desired_file_size each file should not exceed and the total_size this input
/// time range [min_time, max_time] contains.
/// The split times assume that the data is evenly distributed in the time range and if
/// that is not the case the resulting files are not guaranteed to be below max_desired_file_size
/// Hence, the range between two contiguous returned time is percentage of
/// max_desired_file_size/total_size of the time range
/// Example:
///  . Input
///      min_time = 1
///      max_time = 21
///      total_size = 100
///      max_desired_file_size = 30
///
///  . Pecentage = 70/100 = 0.3
///  . Time range between 2 times = (21 - 1) * 0.3 = 6
///
///  . Output = [7, 13, 19] in which
///     7 = 1 (min_time) + 6 (time range)
///     13 = 7 (previous time) + 6 (time range)
///     19 = 13 (previous time) + 6 (time range)
pub(crate) fn compute_split_time(
    min_time: i64,
    max_time: i64,
    total_size: u64,
    max_desired_file_size: u64,
) -> Vec<i64> {
    // Too small to split
    if total_size <= max_desired_file_size {
        return vec![max_time];
    }

    // Same min and max time, nothing to split
    if min_time == max_time {
        return vec![max_time];
    }

    let mut split_times = vec![];
    let percentage = max_desired_file_size as f64 / total_size as f64;
    let mut min = min_time;
    loop {
        let split_time = min + ((max_time - min_time) as f64 * percentage).ceil() as i64;
        if split_time < max_time {
            split_times.push(split_time);
            min = split_time;
        } else {
            break;
        }
    }

    split_times
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_split_time() {
        let min_time = 1;
        let max_time = 11;
        let total_size = 100;
        let max_desired_file_size = 100;

        // no split
        let result = compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], max_time);

        // split 70% and 30%
        let max_desired_file_size = 70;
        let result = compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        // only need to store the last split time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 8); // = 1 (min_time) + 7

        // split 40%, 40%, 20%
        let max_desired_file_size = 40;
        let result = compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        // store first and second split time
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 5); // = 1 (min_time) + 4
        assert_eq!(result[1], 9); // = 5 (previous split_time) + 4
    }

    #[test]
    fn compute_split_time_when_min_time_equals_max() {
        // Imagine a customer is backfilling a large amount of data and for some reason, all the
        // times on the data are exactly the same. That means the min_time and max_time will be the
        // same, but the total_size will be greater than the desired size.
        // We will not split it becasue the split has to stick to non-overlapped time range

        let min_time = 1;
        let max_time = 1;

        let total_size = 200;
        let max_desired_file_size = 100;

        let result = compute_split_time(min_time, max_time, total_size, max_desired_file_size);

        // must return vector of one containing max_time
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 1);
    }

    #[test]
    fn compute_split_time_please_dont_explode() {
        // degenerated case where the step size is so small that it is < 1 (but > 0). In this case we shall still
        // not loop forever.
        let min_time = 10;
        let max_time = 20;

        let total_size = 600000;
        let max_desired_file_size = 10000;

        let result = compute_split_time(min_time, max_time, total_size, max_desired_file_size);
        assert_eq!(result.len(), 9);
    }
}
