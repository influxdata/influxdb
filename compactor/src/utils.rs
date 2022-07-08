//! Helpers of the Compactor

use crate::query::QueryableParquetChunk;
use data_types::{
    ParquetFile, ParquetFileId, ParquetFileParams, PartitionId, TableSchema, Timestamp, Tombstone,
    TombstoneId,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use observability_deps::tracing::*;
use parquet_file::{
    chunk::ParquetChunk,
    metadata::{IoxMetadata, IoxParquetMetaData},
    storage::ParquetStorage,
};
use schema::{sort::SortKey, Schema};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Wrapper of a group of parquet files and their tombstones that overlap in time and should be
/// considered during compaction.
#[derive(Debug)]
pub struct GroupWithTombstones {
    /// Each file with the set of tombstones relevant to it
    pub(crate) parquet_files: Vec<ParquetFileWithTombstone>,
    /// All tombstones relevant to any of the files in the group
    pub(crate) tombstones: Vec<Tombstone>,
}

impl GroupWithTombstones {
    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }
}

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
            self.data.min_sequence_number,
            self.data.max_sequence_number,
            self.data.min_time,
            self.data.max_time,
            sort_key,
            partition_sort_key,
        )
    }
}

/// Struct holding output of a compacted stream
pub struct CompactedData {
    pub(crate) data: SendableRecordBatchStream,
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
}

impl CompactedData {
    /// Initialize compacted data
    pub fn new(
        data: SendableRecordBatchStream,
        meta: IoxMetadata,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
    ) -> Self {
        Self {
            data,
            meta,
            tombstones,
        }
    }
}

/// Information needed to update the catalog after compacting a group of files
#[derive(Debug)]
pub struct CatalogUpdate {
    #[allow(dead_code)]
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
    pub(crate) parquet_file: ParquetFileParams,
}

impl CatalogUpdate {
    /// Initialize with data received from a persist to object storage
    pub fn new(
        partition_id: PartitionId,
        meta: IoxMetadata,
        file_size: usize,
        md: IoxParquetMetaData,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
        table_schema: &TableSchema,
    ) -> Self {
        let parquet_file = meta.to_parquet_file(partition_id, file_size, &md, |name| {
            table_schema.columns.get(name).expect("unknown column").id
        });
        Self {
            meta,
            tombstones,
            parquet_file,
        }
    }
}

/// Compute time to split data
/// Return a list of times at which we want data to be split. The times are computed
/// based on the max_desired_file_size each file should not exceed and the total_size this input
/// time range [min_time, max_time] contains.
/// The split times assume that the data is evenly distributed in the time range and if
/// that is not the case the resulting files are not guaranteed to be below max_desired_file_size
/// Hence, the range between two contiguous returned time is pecentage of
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
#[allow(dead_code)] // This is temporarily not being used anywhere
fn compute_split_time(
    min_time: i64,
    max_time: i64,
    total_size: i64,
    max_desired_file_size: i64,
) -> Vec<i64> {
    // Too small to split
    if total_size <= max_desired_file_size {
        return vec![max_time];
    }

    let mut split_times = vec![];
    let percentage = max_desired_file_size as f64 / total_size as f64;
    let mut min = min_time;
    loop {
        let split_time = min + ((max_time - min_time) as f64 * percentage).floor() as i64;
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

    #[tokio::test]
    async fn test_compute_split_time() {
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
}
