//! Contains the algorithm to determine which chunks may contain
//! "duplicate" primary keys (that is where data with the same
//! combination of "tag" columns and timestamp in the InfluxDB
//! DataModel have been written in via multiple distinct line protocol
//! writes (and thus are stored in separate rows)

use data_types::partition_metadata::{ColumnSummary, StatOverlap, Statistics};
use schema::TIME_COLUMN_NAME;
use snafu::Snafu;

use crate::QueryChunkMeta;

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

/// Groups [`QueryChunkMeta`] objects into disjoint sets using values of
/// min/max statistics. The groups are formed such that each group
/// *may* contain InfluxDB data model primary key duplicates with
/// others in that set.
///
/// The *may* overlap calculation is conservative -- that is it may
/// flag two chunks as having overlapping data when in reality they do
/// not. If chunks are split into different groups, then they are
/// guaranteed not to contain any rows with the same primary key.
///
/// Note 1: since this algorithm is based on statistics, it may have
/// false positives (flag that two objects may have overlap when in
/// reality they do not)
///
/// Note 2: this algorithm is O(n^2) worst case (when no chunks have
/// any overlap)
pub fn group_potential_duplicates<C>(chunks: Vec<C>) -> Result<Vec<Vec<C>>>
where
    C: QueryChunkMeta,
{
    let mut groups: Vec<Vec<KeyStats<'_, _>>> = vec![];

    // Step 1: find the up groups using references to `chunks` stored
    // in KeyStats views
    for (idx, chunk) in chunks.iter().enumerate() {
        // try to find a place to put this chunk
        let mut key_stats = Some(KeyStats::new(idx, chunk));

        'outer: for group in &mut groups {
            // If this chunk overlaps any existing chunk in group add
            // it to group
            for ks in group.iter() {
                if ks.potential_overlap(key_stats.as_ref().unwrap())? {
                    group.push(key_stats.take().unwrap());
                    break 'outer;
                }
            }
        }

        if let Some(key_stats) = key_stats {
            // couldn't place key_stats in any existing group, needs a
            // new group
            groups.push(vec![key_stats])
        }
    }

    // Now some shenanigans to rearrange the actual input chunks into
    // the final resulting groups corresponding to the groups of
    // KeyStats

    // drop all references to chunks, and only keep indicides
    #[allow(clippy::needless_collect)] // required for the borrow checker
    let groups: Vec<Vec<usize>> = groups
        .into_iter()
        .map(|group| group.into_iter().map(|key_stats| key_stats.index).collect())
        .collect();

    let mut chunks: Vec<Option<C>> = chunks.into_iter().map(Some).collect();

    let groups = groups
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .map(|index| {
                    chunks[index]
                        .take()
                        .expect("Internal mismatch while gathering into groups")
                })
                .collect::<Vec<C>>()
        })
        .collect::<Vec<Vec<C>>>();

    Ok(groups)
}

/// Holds a view to a chunk along with information about its columns
/// in an easy to compare form
#[derive(Debug)]
struct KeyStats<'a, C>
where
    C: QueryChunkMeta,
{
    /// The index of the chunk
    index: usize,

    /// The underlying chunk
    chunk: &'a C,

    /// the ColumnSummaries for the chunk's 'primary_key' columns, in
    /// "lexographical" order (aka sorted by name)
    key_summaries: Vec<&'a ColumnSummary>,
}

impl<'a, C> KeyStats<'a, C>
where
    C: QueryChunkMeta,
{
    /// Create a new view for the specified chunk at index `index`,
    /// computing the columns to be used in the primary key comparison
    pub fn new(index: usize, chunk: &'a C) -> Self {
        // find summaries for each primary key column:
        let key_summaries = chunk
            .schema()
            .primary_key()
            .into_iter()
            .map(|key_name| {
                chunk
                    .summary()
                    .column(key_name)
                    .expect("can not find column in chunk summary")
            })
            .collect();

        Self {
            index,
            chunk,
            key_summaries,
        }
    }

    /// Returns true if the chunk has a potential primary key overlap
    /// with the other chunk.
    ///
    /// This this algorithm is O(2^N) in the worst case. However, the
    /// pathological case is where two chunks each have a large
    /// numbers of tags that have no overlap, which seems unlikely in
    /// the real world.
    ///
    /// Note this algoritm is quite conservative (in that it will
    /// assume that any column can contain nulls) and thus can match
    /// with chunks that do not have that column.   for example
    ///
    /// Chunk 1: tag_a
    /// Chunk 2: tag_a, tag_b
    ///
    /// In this case Chunk 2 has values for tag_b but Chunk 1
    /// doesn't have any values in tag_b (its values are implicitly
    /// null)
    ///
    /// If Chunk 2 has any null values in the tag_b column, it could
    /// overlap with Chunk 1 (as logically there can be rows with
    /// (tag_a = NULL, tag_b = NULL) in both chunks
    ///
    /// We could make this algorithm significantly less conservative
    /// if we stored the Null count in the ColumnSummary (and thus
    /// could rule out matches with columns that were not present) if
    /// there were no NULLs
    fn potential_overlap(&self, other: &Self) -> Result<bool> {
        // This algorithm assumes that the keys are sorted by name (so
        // they can't appear in different orders on the two sides) except
        // the "time" column which is always the last column
        debug_assert!(self
            .key_summaries
            .windows(2)
            .all(|s| s[1].name == TIME_COLUMN_NAME || s[0].name <= s[1].name));
        debug_assert!(other
            .key_summaries
            .windows(2)
            .all(|s| s[1].name == TIME_COLUMN_NAME || s[0].name <= s[1].name));
        self.potential_overlap_impl(0, other, 0)
    }

    // Checks the remainder of self.columns[self_idx..] and
    // other.columns[..other_idx] if they are compatible
    fn potential_overlap_impl(
        &self,
        mut self_idx: usize,
        other: &Self,
        mut other_idx: usize,
    ) -> Result<bool> {
        loop {
            let s1 = self.key_summaries.get(self_idx);
            let s2 = other.key_summaries.get(other_idx);

            if let (Some(s1), Some(s2)) = (s1, s2) {
                if s1.name == s2.name {
                    // pk matched in this position, so check values. If we
                    // find no overlap, know this is false, otherwise need to keep checking
                    if Self::columns_might_overlap(s1, s2)? {
                        self_idx += 1;
                        other_idx += 1;
                    } else {
                        return Ok(false);
                    }
                } else {
                    // name didn't match, so try and find the next
                    // place it does.  Since there may be missing keys
                    // in each side, need to check each in turn
                    //
                    // Note this will result in O(num_tags) stack
                    // frames in the worst case, but we expect the
                    // number of tags to be relatively small (~20 at
                    // the time of this writing)
                    return Ok(self.potential_overlap_impl(self_idx + 1, other, other_idx)?
                        || self.potential_overlap_impl(self_idx, other, other_idx + 1)?);
                }
            } else {
                // ran out of columns to check on one side, assume the
                // other could have nulls all the way down (due to null
                // assumption)
                return Ok(true);
            }
        }
    }

    /// Returns true if the two columns MAY overlap other, based on
    /// statistics
    pub fn columns_might_overlap(s1: &ColumnSummary, s2: &ColumnSummary) -> Result<bool> {
        use Statistics::*;

        let overlap = match (&s1.stats, &s2.stats) {
            (I64(s1), I64(s2)) => s1.overlaps(s2),
            (U64(s1), U64(s2)) => s1.overlaps(s2),
            (F64(s1), F64(s2)) => s1.overlaps(s2),
            (Bool(s1), Bool(s2)) => s1.overlaps(s2),
            (String(s1), String(s2)) => s1.overlaps(s2),
            _ => {
                return MismatchedStatsTypes {
                    column_name: s1.name.clone(),
                }
                .fail()
            }
        };

        // If either column has no min/max, treat the column as being
        // entirely null, meaning that it could overlap the other
        // stats if it had nulls.
        let is_none = s1.stats.is_none() || s2.stats.is_none();

        match overlap {
            StatOverlap::NonZero => Ok(true),
            StatOverlap::Zero => Ok(false),
            StatOverlap::Unknown if is_none => Ok(true),
            // This case means there some stats, but not all.
            // Unclear how this could happen, so throw an error for now
            StatOverlap::Unknown => InternalPartialStatistics {
                column_name: s1.name.clone(),
                s1: s1.stats.clone(),
                s2: s2.stats.clone(),
            }
            .fail(),
        }
    }
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
    fn one_column_no_overlap() {
        let c1 = TestChunk::new("chunk1").with_tag_column_with_stats(
            "tag1",
            Some("boston"),
            Some("mumbai"),
        );

        let c2 = TestChunk::new("chunk2").with_tag_column_with_stats(
            "tag1",
            Some("new york"),
            Some("zoo york"),
        );

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_column_overlap() {
        let c1 = TestChunk::new("chunk1").with_tag_column_with_stats(
            "tag1",
            Some("boston"),
            Some("new york"),
        );

        let c2 = TestChunk::new("chunk2").with_tag_column_with_stats(
            "tag1",
            Some("denver"),
            Some("zoo york"),
        );

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_time_column_overlap() {
        let c1 = TestChunk::new("chunk1").with_time_column_with_stats(Some(100), Some(1000));

        let c2 = TestChunk::new("chunk2").with_time_column_with_stats(Some(200), Some(500));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn multi_columns() {
        let c1 = TestChunk::new("chunk1")
            .with_time_column_with_stats(Some(0), Some(1000))
            .with_tag_column_with_stats("tag1", Some("boston"), Some("new york"));

        // Overlaps in tag1, but not in time
        let c2 = TestChunk::new("chunk2")
            .with_tag_column_with_stats("tag1", Some("denver"), Some("zoo york"))
            .with_time_column_with_stats(Some(2000), Some(3000));

        // Overlaps in time, but not in tag1
        let c3 = TestChunk::new("chunk3")
            .with_tag_column_with_stats("tag1", Some("zzx"), Some("zzy"))
            .with_time_column_with_stats(Some(500), Some(1500));

        // Overlaps in time, and in tag1
        let c4 = TestChunk::new("chunk4")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("zzz"))
            .with_time_column_with_stats(Some(500), Some(1500));

        let groups = group_potential_duplicates(vec![c1, c2, c3, c4]).expect("grouping succeeded");

        let expected = vec![
            "Group 0: [chunk1, chunk4]",
            "Group 1: [chunk2]",
            "Group 2: [chunk3]",
        ];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn multi_columns_time_last() {
        // test for wrong assumption of assert_debug reported in
        // https://github.com/influxdata/influxdb_iox/issues/2408
        // This test makes sure the debug_assert in potential_overlap works correctly
        // even if the column before the last column "time" has a name larger than it, e.g "url"

        // Even "time" column is stored in front of "url", the primary_key function
        // invoked inside potential_overlap invoked by group_potential_duplicates
        //  will return "url", "time"
        let c1 = TestChunk::new("chunk1")
            .with_time_column_with_stats(Some(0), Some(1000))
            .with_tag_column_with_stats("url", Some("boston"), Some("new york")); // "url" > "time"

        // Overlaps in tag1, but not in time
        let c2 = TestChunk::new("chunk2")
            .with_tag_column_with_stats("url", Some("denver"), Some("zoo york"))
            .with_time_column_with_stats(Some(2000), Some(3000));

        // Overlaps in time, but not in tag1
        let c3 = TestChunk::new("chunk3")
            .with_tag_column_with_stats("url", Some("zzx"), Some("zzy"))
            .with_time_column_with_stats(Some(500), Some(1500));

        // Overlaps in time, and in tag1
        let c4 = TestChunk::new("chunk4")
            .with_tag_column_with_stats("url", Some("aaa"), Some("zzz"))
            .with_time_column_with_stats(Some(500), Some(1500));

        let groups = group_potential_duplicates(vec![c1, c2, c3, c4]).expect("grouping succeeded");

        let expected = vec![
            "Group 0: [chunk1, chunk4]",
            "Group 1: [chunk2]",
            "Group 2: [chunk3]",
        ];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn boundary() {
        // check that overlap calculations include the bound
        let c1 =
            TestChunk::new("chunk1").with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"));
        let c2 =
            TestChunk::new("chunk2").with_tag_column_with_stats("tag1", Some("bbb"), Some("ccc"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn same() {
        // check that if chunks overlap exactly on the boundaries they are still grouped
        let c1 =
            TestChunk::new("chunk1").with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"));
        let c2 =
            TestChunk::new("chunk2").with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn different_tag_names() {
        // check that if chunks overlap but in different tag names
        let c1 =
            TestChunk::new("chunk1").with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"));
        let c2 =
            TestChunk::new("chunk2").with_tag_column_with_stats("tag2", Some("aaa"), Some("bbb"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        // the overlap could come when (tag1 = NULL, tag2=NULL) which
        // could exist in either chunk
        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn different_tag_names_multi_tags() {
        // check that if chunks overlap but in different tag names
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("aaa"), Some("bbb"));

        let c2 = TestChunk::new("chunk2")
            .with_tag_column_with_stats("tag2", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag3", Some("aaa"), Some("bbb"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        // the overlap could come when  (tag1 = NULL, tag2, tag3=NULL)
        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn three_column() {
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("xxx"), Some("yyy"))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("xxx"), Some("yyy"))
            // Timestamp doesn't overlap, but the two tags do
            .with_time_column_with_stats(Some(2001), Some(3000));

        let c3 = TestChunk::new("chunk3")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("aaa"), Some("zzz"))
            // all three overlap
            .with_time_column_with_stats(Some(1000), Some(2000));

        let groups = group_potential_duplicates(vec![c1, c2, c3]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk3]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order() {
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("xxx"), Some("yyy"))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            .with_tag_column_with_stats("tag2", Some("aaa"), Some("zzz"))
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            // all three overlap, but tags in different order
            .with_time_column_with_stats(Some(500), Some(1000));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order_no_tags() {
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("xxx"), Some("yyy"))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            // tag1 and timestamp overlap, but no tag2 (aka it is all null)
            // so it could overlap if there was a null tag2 value in chunk1
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_time_column_with_stats(Some(500), Some(1000));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order_null_stats() {
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", Some("xxx"), Some("yyy"))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            // tag1 and timestamp overlap, tag2 has no stats (is all null)
            // so they might overlap if chunk1 had a null in tag 2
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_tag_column_with_stats("tag2", None, None)
            .with_time_column_with_stats(Some(500), Some(1000));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order_partial_stats() {
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            // tag1 has a min but not a max. Should result in error
            .with_tag_column_with_stats("tag1", Some("aaa"), None)
            .with_time_column_with_stats(Some(500), Some(1000));

        let result = group_potential_duplicates(vec![c1, c2]).unwrap_err();

        let result = result.to_string();
        let expected =
            "Internal error. Partial statistics found for column 'tag1' looking for duplicates";
        assert!(
            result.contains(expected),
            "can not find {} in {}",
            expected,
            result
        );
    }

    #[test]
    fn tag_fields_not_counted() {
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_i64_field_column_with_stats("field", Some(0), Some(2))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            // tag1 and timestamp overlap, but field value does not
            // should still overlap
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_i64_field_column_with_stats("field", Some(100), Some(200))
            .with_time_column_with_stats(Some(500), Some(1000));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn mismatched_types() {
        // When the same column has different types in different
        // chunks; this will likely cause errors elsewhere in practice
        // as the schemas are incompatible (and can't be merged)
        let c1 = TestChunk::new("chunk1")
            .with_tag_column_with_stats("tag1", Some("aaa"), Some("bbb"))
            .with_time_column_with_stats(Some(0), Some(1000));

        let c2 = TestChunk::new("chunk2")
            // tag1 column is actually a field is different in chunk
            // 2, so since the timestamps overlap these chunks
            // might also have duplicates (if tag1 was null in c1)
            .with_i64_field_column_with_stats("tag1", Some(100), Some(200))
            .with_time_column_with_stats(Some(0), Some(1000));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    // --- Test infrastructure --

    fn to_string(groups: Vec<Vec<TestChunk>>) -> Vec<String> {
        let mut s = vec![];
        for (idx, group) in groups.iter().enumerate() {
            let names = group.iter().map(|c| c.table_name()).collect::<Vec<_>>();
            s.push(format!("Group {}: [{}]", idx, names.join(", ")));
        }
        s
    }
}
