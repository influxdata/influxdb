//! Contains the algorithm to determine which chunks may contain
//! "duplicate" primary keys (that is where data with the same
//! combination of "tag" columns and timestamp in the InfluxDB
//! DataModel have been written in via multiple distinct line protocol
//! writes (and thus are stored in separate rows)

use std::cmp::Ordering;

use crate::pruning::Prunable;
use data_types::partition_metadata::{ColumnSummary, InfluxDbType, StatOverlap, Statistics};
use snafu::Snafu;

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

/// Groups [`Prunable`] objects into disjoint sets using values of
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
    C: Prunable,
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
    C: Prunable,
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
    C: Prunable,
{
    /// Create a new view for the specified chunk at index `index`,
    /// computing the columns to be used in the primary key comparison
    pub fn new(index: usize, chunk: &'a C) -> Self {
        use InfluxDbType::*;
        let mut key_summaries: Vec<&'a ColumnSummary> = chunk
            .summary()
            .columns
            .iter()
            .filter(|s| match s.influxdb_type {
                Some(Tag) => true,
                Some(Field) => false,
                Some(Timestamp) => true,
                None => false,
            })
            .collect();

        // Now, sort lexographically (but put timestamp last)
        key_summaries.sort_by(
            |a, b| match (a.influxdb_type.as_ref(), b.influxdb_type.as_ref()) {
                (Some(Tag), Some(Tag)) => a.name.cmp(&b.name),
                (Some(Timestamp), Some(Tag)) => Ordering::Greater,
                (Some(Tag), Some(Timestamp)) => Ordering::Less,
                (Some(Timestamp), Some(Timestamp)) => panic!("multiple timestamps in summary"),
                _ => panic!("Unexpected types in key summary"),
            },
        );

        Self {
            index,
            chunk,
            key_summaries,
        }
    }

    /// Returns true if the chunk has a potential primary key overlap with the other chunk
    fn potential_overlap(&self, other: &Self) -> Result<bool> {
        // in order to have overlap, *all* the columns in the sort order
        // need to be the same. Note gaps in the sort order mean they
        // are for different parts of the keyspace
        if self.key_summaries.len() != other.key_summaries.len() {
            // Short circuit on different lengths
            return Ok(false);
        }

        let iter = self.key_summaries.iter().zip(other.key_summaries.iter());
        for (s1, s2) in iter {
            if s1.name != s2.name || !Self::columns_might_overlap(s1, s2)? {
                return Ok(false);
            }
        }

        Ok(true)
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

        // If either column has no min/max, treat the column as
        // being entirely null
        let is_none = s1.stats.is_none() || s2.stats.is_none();

        match overlap {
            StatOverlap::NonZero => Ok(true),
            StatOverlap::Zero => Ok(false),
            StatOverlap::Unknown if is_none => Ok(false), // no stats means no values
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
    use arrow::datatypes::SchemaRef;
    use data_types::partition_metadata::{
        ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary,
    };
    use internal_types::schema::{builder::SchemaBuilder, TIME_COLUMN_NAME};

    use super::*;

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
        let c1 = TestChunk::new("chunk1").with_tag("tag1", Some("boston"), Some("mumbai"));

        let c2 = TestChunk::new("chunk2").with_tag("tag1", Some("new york"), Some("zoo york"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn one_column_overlap() {
        let c1 = TestChunk::new("chunk1").with_tag("tag1", Some("boston"), Some("new york"));

        let c2 = TestChunk::new("chunk2").with_tag("tag1", Some("denver"), Some("zoo york"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn multi_columns() {
        let c1 = TestChunk::new("chunk1").with_timestamp(0, 1000).with_tag(
            "tag1",
            Some("boston"),
            Some("new york"),
        );

        // Overlaps in tag1, but not in time
        let c2 = TestChunk::new("chunk2")
            .with_tag("tag1", Some("denver"), Some("zoo york"))
            .with_timestamp(2000, 3000);

        // Overlaps in time, but not in tag1
        let c3 = TestChunk::new("chunk3")
            .with_tag("tag1", Some("zzx"), Some("zzy"))
            .with_timestamp(500, 1500);

        // Overlaps in time, and in tag1
        let c4 = TestChunk::new("chunk4")
            .with_tag("tag1", Some("aaa"), Some("zzz"))
            .with_timestamp(500, 1500);

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
        let c1 = TestChunk::new("chunk1").with_tag("tag1", Some("aaa"), Some("bbb"));
        let c2 = TestChunk::new("chunk2").with_tag("tag1", Some("bbb"), Some("ccc"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn same() {
        // check that if chunks overlap exactly on the boundaries they are still grouped
        let c1 = TestChunk::new("chunk1").with_tag("tag1", Some("aaa"), Some("bbb"));
        let c2 = TestChunk::new("chunk2").with_tag("tag1", Some("aaa"), Some("bbb"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn different_tag_names() {
        // check that if chunks overlap but in different tag names
        let c1 = TestChunk::new("chunk1").with_tag("tag1", Some("aaa"), Some("bbb"));
        let c2 = TestChunk::new("chunk2").with_tag("tag2", Some("aaa"), Some("bbb"));

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn three_column() {
        let c1 = TestChunk::new("chunk1")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", Some("xxx"), Some("yyy"))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", Some("xxx"), Some("yyy"))
            // Timestamp doesn't overlap, but the two tags do
            .with_timestamp(2001, 3000);

        let c3 = TestChunk::new("chunk3")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", Some("aaa"), Some("zzz"))
            // all three overlap
            .with_timestamp(1000, 2000);

        let groups = group_potential_duplicates(vec![c1, c2, c3]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk3]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order() {
        let c1 = TestChunk::new("chunk1")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", Some("xxx"), Some("yyy"))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            .with_tag("tag2", Some("aaa"), Some("zzz"))
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            // all three overlap, but tags in different order
            .with_timestamp(500, 1000);

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order_no_tags() {
        let c1 = TestChunk::new("chunk1")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", Some("xxx"), Some("yyy"))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            // tag1 and timestamp overlap, but no tag2 (aka it is all null)
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_timestamp(500, 1000);

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order_null_stats() {
        let c1 = TestChunk::new("chunk1")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", Some("xxx"), Some("yyy"))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            // tag1 and timestamp overlap, tag2 has no stats (null)
            // so we say they can't overlap
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_tag("tag2", None, None)
            .with_timestamp(500, 1000);

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn tag_order_partial_stats() {
        let c1 = TestChunk::new("chunk1")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            // tag1 has a min but not a max. Should result in error
            .with_tag("tag1", Some("aaa"), None)
            .with_timestamp(500, 1000);

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
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_int_field("field", Some(0), Some(2))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            // tag1 and timestamp overlap, but field value does not
            // should still overlap
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_int_field("field", Some(100), Some(200))
            .with_timestamp(500, 1000);

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1, chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    #[test]
    fn mismatched_types() {
        // Test if same column has different types in different
        // chunks; this will likely cause errors elsewhere in practice
        // as the schemas are incompatible (and can't be merged)
        let c1 = TestChunk::new("chunk1")
            .with_tag("tag1", Some("aaa"), Some("bbb"))
            .with_timestamp(0, 1000);

        let c2 = TestChunk::new("chunk2")
            // tag1 column is actually a field is different in chunk
            // 2, so even though the timestamps overlap these chunks
            // don't have duplicates
            .with_int_field("tag1", Some(100), Some(200))
            .with_timestamp(0, 1000);

        let groups = group_potential_duplicates(vec![c1, c2]).expect("grouping succeeded");

        let expected = vec!["Group 0: [chunk1]", "Group 1: [chunk2]"];
        assert_groups_eq!(expected, groups);
    }

    // --- Test infrastructure --

    fn to_string(groups: Vec<Vec<TestChunk>>) -> Vec<String> {
        let mut s = vec![];
        for (idx, group) in groups.iter().enumerate() {
            let names = group.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();
            s.push(format!("Group {}: [{}]", idx, names.join(", ")));
        }
        s
    }

    /// Mocked out prunable provider to use testing overlaps
    #[derive(Debug)]
    struct TestChunk {
        // The name of this chunk
        name: String,
        summary: TableSummary,
        builder: SchemaBuilder,
    }

    /// Implementation of creating a new column with statitics for TestPrunable
    macro_rules! make_stats {
        ($MIN:expr, $MAX:expr, $STAT_TYPE:ident) => {{
            Statistics::$STAT_TYPE(StatValues {
                distinct_count: None,
                min: $MIN,
                max: $MAX,
                count: 42,
            })
        }};
    }

    impl TestChunk {
        /// Create a new TestChunk with a specified name
        fn new(name: impl Into<String>) -> Self {
            let name = name.into();
            let summary = TableSummary::new(name.clone());
            let builder = SchemaBuilder::new();
            Self {
                name,
                summary,
                builder,
            }
        }

        /// Adds a tag column with the specified min/max values
        fn with_tag(
            mut self,
            name: impl Into<String>,
            min: Option<&str>,
            max: Option<&str>,
        ) -> Self {
            let min = min.map(|v| v.to_string());
            let max = max.map(|v| v.to_string());

            let tag_name = name.into();
            self.builder.tag(&tag_name);

            self.summary.columns.push(ColumnSummary {
                name: tag_name,
                influxdb_type: Some(InfluxDbType::Tag),
                stats: make_stats!(min, max, String),
            });
            self
        }

        /// Adds a timestamp column with the specified min/max values
        fn with_timestamp(mut self, min: i64, max: i64) -> Self {
            self.builder.timestamp();

            let min = Some(min);
            let max = Some(max);

            self.summary.columns.push(ColumnSummary {
                name: TIME_COLUMN_NAME.into(),
                influxdb_type: Some(InfluxDbType::Timestamp),
                stats: make_stats!(min, max, I64),
            });
            self
        }

        /// Adds an I64 field column with the specified min/max values
        fn with_int_field(
            mut self,
            name: impl Into<String>,
            min: Option<i64>,
            max: Option<i64>,
        ) -> Self {
            let field_name = name.into();
            self.builder
                .field(&field_name, arrow::datatypes::DataType::Int64);

            self.summary.columns.push(ColumnSummary {
                name: field_name,
                influxdb_type: Some(InfluxDbType::Field),
                stats: make_stats!(min, max, I64),
            });
            self
        }
    }

    impl Prunable for TestChunk {
        fn summary(&self) -> &TableSummary {
            &self.summary
        }

        fn schema(&self) -> SchemaRef {
            self.builder
                // need to clone because `build` resets builder state
                .clone()
                .build()
                .expect("created schema")
                .as_arrow()
        }
    }
}
