//! Contains the algorithm to determine which chunks may contain "duplicate" primary keys (that is
//! where data with the same combination of "tag" columns and timestamp in the InfluxDB DataModel
//! have been written in via multiple distinct line protocol writes (and thus are stored in
//! separate rows)

use crate::QueryChunkMeta;
use data_types::{
    DeletePredicate, ParquetFile, PartitionId, Statistics, TableSummary, TimestampMinMax,
};
use observability_deps::tracing::debug;
use schema::{sort::SortKey, Schema};
use snafu::Snafu;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Mismatched type when comparing statistics for column '{}'",
        column_name
    ))]
    MismatchedStatsTypes { column_name: String },

    #[snafu(display(
        "Internal error. Partial statistics found for column '{}' looking for duplicates. s1: '{:?}' s2: '{:?}'",
        column_name, s1, s2
    ))]
    InternalPartialStatistics {
        column_name: String,
        s1: Statistics,
        s2: Statistics,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// Implement QueryChunkMeta for ParquetFileWithMetadata to have group_potential_duplicates
// work on ParquetFileWithMetadata. Since group_potential_duplicates only needs 2 functions
// partition_id and timestamp_min_max, other functions are left `umimplemneted` on purpose
impl QueryChunkMeta for ParquetFile {
    fn summary(&self) -> Option<&TableSummary> {
        unimplemented!()
    }

    fn schema(&self) -> Arc<Schema> {
        unimplemented!()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.partition_id)
    }

    fn sort_key(&self) -> Option<&SortKey> {
        unimplemented!()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        unimplemented!()
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        Some(TimestampMinMax {
            min: self.min_time.get(),
            max: self.max_time.get(),
        })
    }

    /// return a reference to delete predicates of the chunk
    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        unimplemented!()
    }
}

/// Groups [`QueryChunkMeta`] objects into disjoint sets of overlapped time range.
/// Does not preserve or guarantee any ordering.
pub fn group_potential_duplicates<T>(chunks: Vec<T>) -> Result<Vec<Vec<T>>>
where
    T: QueryChunkMeta,
{
    // If at least one of the chunks has no time range,
    // all chunks are considered to overlap with each other.
    if chunks.iter().any(|c| c.timestamp_min_max().is_none()) {
        debug!("At least one chunk has not timestamp mim max");
        return Ok(vec![chunks]);
    }

    // Use this algorithm to group them
    // https://towardsdatascience.com/overlapping-time-period-problem-b7f1719347db

    let num_chunks = chunks.len();
    let mut grouper = Vec::with_capacity(num_chunks * 2);

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum StartEnd {
        Start,
        End,
    }
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct StartEndChunk<I> {
        start_end: StartEnd,
        chunk: Option<I>,
    }
    struct GrouperRecord<I, V: PartialOrd> {
        value: V,
        start_end_chunk: StartEndChunk<I>,
    }

    for chunk in chunks {
        let time_range = chunk
            .timestamp_min_max()
            .expect("Time range should have value");

        grouper.push(GrouperRecord {
            value: time_range.min,
            start_end_chunk: StartEndChunk {
                start_end: StartEnd::Start,
                chunk: None,
            },
        });
        grouper.push(GrouperRecord {
            value: time_range.max,
            start_end_chunk: StartEndChunk {
                start_end: StartEnd::End,
                chunk: Some(chunk),
            },
        });
    }

    grouper.sort_by_key(|gr| (gr.value, gr.start_end_chunk.start_end));

    let mut cumulative_sum = 0;
    let mut groups = Vec::with_capacity(num_chunks);

    for gr in grouper {
        cumulative_sum += match gr.start_end_chunk.start_end {
            StartEnd::Start => 1,
            StartEnd::End => -1,
        };

        if matches!(gr.start_end_chunk.start_end, StartEnd::Start) && cumulative_sum == 1 {
            groups.push(Vec::with_capacity(num_chunks));
        }
        if let StartEnd::End = gr.start_end_chunk.start_end {
            groups
                .last_mut()
                .expect("a start should have pushed at least one empty group")
                .push(gr.start_end_chunk.chunk.expect("Must have chunk value"));
        }
    }
    Ok(groups)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{test::TestChunk, QueryChunk};

    #[macro_export]
    macro_rules! assert_groups_eq {
        ($EXPECTED_LINES: expr, $GROUPS: expr) => {
            let expected_lines: Vec<String> =
                $EXPECTED_LINES.into_iter().map(|s| s.to_string()).collect();

            let actual_lines = to_string($GROUPS);

            assert_eq!(
                expected_lines, actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    // Test cases:

    #[test]
    fn one_time_column_overlap_same_min_max() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 1));
        let c2 = Arc::new(TestChunk::new("chunk2").with_timestamp_min_max(1, 1));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_time_column_overlap_bad_case() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 10));
        let c2 = Arc::new(TestChunk::new("chunk2").with_timestamp_min_max(15, 30));
        let c3 = Arc::new(TestChunk::new("chunk3").with_timestamp_min_max(7, 20));
        let c4 = Arc::new(TestChunk::new("chunk4").with_timestamp_min_max(25, 35));

        let groups = group_potential_duplicates(vec![c1, c2, c3, c4]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1, chunk3, chunk2, chunk4]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_time_column_overlap_contiguous() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 10));
        let c2 = Arc::new(TestChunk::new("chunk2").with_timestamp_min_max(7, 20));
        let c3 = Arc::new(TestChunk::new("chunk3").with_timestamp_min_max(15, 30));
        let c4 = Arc::new(TestChunk::new("chunk4").with_timestamp_min_max(25, 35));

        let groups = group_potential_duplicates(vec![c1, c2, c3, c4]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1, chunk2, chunk3, chunk4]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_time_column_overlap_2_groups() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 10));
        let c2 = Arc::new(TestChunk::new("chunk2").with_timestamp_min_max(7, 20));
        let c3 = Arc::new(TestChunk::new("chunk3").with_timestamp_min_max(21, 30));
        let c4 = Arc::new(TestChunk::new("chunk4").with_timestamp_min_max(25, 35));

        let groups = group_potential_duplicates(vec![c1, c2, c3, c4]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1, chunk2]", "Group 1: [chunk3, chunk4]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_time_column_overlap_3_groups() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 10));
        let c2 = Arc::new(TestChunk::new("chunk2").with_timestamp_min_max(7, 20));
        let c3 = Arc::new(TestChunk::new("chunk3").with_timestamp_min_max(21, 24));
        let c4 = Arc::new(TestChunk::new("chunk4").with_timestamp_min_max(25, 35));

        let groups = group_potential_duplicates(vec![c1, c4, c3, c2]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec![
            "Group 0: [chunk1, chunk2]",
            "Group 1: [chunk3]",
            "Group 2: [chunk4]",
        ];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_time_column_overlap_1_chunk() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 10));

        let groups = group_potential_duplicates(vec![c1]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn overlap_no_groups() {
        let groups: Vec<Vec<TestChunk>> =
            group_potential_duplicates(vec![]).expect("grouping succeeded");

        assert!(groups.is_empty());
    }

    #[test]
    fn multi_columns_overlap_bad_case() {
        let c1 = Arc::new(TestChunk::new("chunk1").with_timestamp_min_max(1, 10));
        let c2 = Arc::new(
            TestChunk::new("chunk2")
                .with_timestamp_min_max(15, 30)
                .with_i64_field_column("field1"),
        );
        let c3 = Arc::new(
            TestChunk::new("chunk3")
                .with_timestamp_min_max(7, 20)
                .with_tag_column("tag1"),
        );
        let c4 = Arc::new(TestChunk::new("chunk4").with_timestamp_min_max(25, 35));

        let groups = group_potential_duplicates(vec![c1, c2, c3, c4]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1, chunk3, chunk2, chunk4]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn multi_columns_overlap_1_chunk() {
        let c1 = Arc::new(
            TestChunk::new("chunk1")
                .with_timestamp_min_max(1, 10)
                .with_tag_column("tag1"),
        );

        let groups = group_potential_duplicates(vec![c1]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec!["Group 0: [chunk1]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn multi_columns_overlap_3_groups() {
        let c1 = Arc::new(
            TestChunk::new("chunk1")
                .with_timestamp_min_max(1, 10)
                .with_tag_column("tag1"),
        );
        let c2 = Arc::new(TestChunk::new("chunk2").with_timestamp_min_max(7, 20));
        let c3 = Arc::new(
            TestChunk::new("chunk3")
                .with_timestamp_min_max(21, 24)
                .with_tag_column("tag2"),
        );
        let c4 = Arc::new(TestChunk::new("chunk4").with_timestamp_min_max(25, 35));

        let groups = group_potential_duplicates(vec![c1, c4, c3, c2]).expect("grouping succeeded");
        let groups = to_group_query_chunks(groups);

        let expected = vec![
            "Group 0: [chunk1, chunk2]",
            "Group 1: [chunk3]",
            "Group 2: [chunk4]",
        ];
        assert_groups_eq!(expected, groups);
    }

    // --- Test infrastructure --
    fn to_string(groups: Vec<Vec<Arc<dyn QueryChunk>>>) -> Vec<String> {
        let mut s = vec![];
        for (idx, group) in groups.iter().enumerate() {
            let names = group.iter().map(|c| c.table_name()).collect::<Vec<_>>();
            s.push(format!("Group {}: [{}]", idx, names.join(", ")));
        }
        s
    }

    // convert from Vec<Vec<Arc<TestChunk>>> to Vec<Vec<Arc<dyn QueryChunk>>>
    fn to_group_query_chunks(groups: Vec<Vec<Arc<TestChunk>>>) -> Vec<Vec<Arc<dyn QueryChunk>>> {
        groups
            .into_iter()
            .map(|chunks| chunks.into_iter().map(|c| c as _).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }
}
