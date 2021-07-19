//! Implement iterator and comparator to split data into distinct ranges

use arrow::array::{ArrayData, DynComparator, build_compare};
use arrow::compute::{SortColumn, SortOptions};

use snafu::Snafu;
use std::cmp::Ordering;
use std::iter::Iterator;
use std::ops::Range;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Sort requires at least one column"
    ))]
    EmptyColumns {},

    #[snafu(display(
        "Sort columns have different row counts"
    ))]
    DifferentRowCounts{},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


/// Given a list of key columns, find partition ranges that would partition
/// equal values across columns
///
/// The returned vec would be of size k where k is cardinality of the values; Consecutive
/// values will be connected: (a, b) and (b, c), where start = 0 and end = n for the first and last
/// range.
///
/// The algorithms here work with any set of columns but it is implemented to optimize the input columns
/// Example Input columns:
/// Invisible Index |  Highest_Cardinality | Time | Second_Highest_Cardinality | Lowest_Cardinality
/// --------------- | -------------------- | ---- | -------------------------- | --------------------
///         0       |          1           |  1   |             1              |            1
///         1       |          1           |  10  |             1              |            1
///         2       |          3           |  8   |             1              |            1
///         3       |          4           |  9   |             1              |            1
///         4       |          4           |  9   |             1              |            1
///         5       |          5           |  1   |             1              |            1
///         6       |          5           |  15  |             1              |            1
///         7       |          5           |  15  |             2              |            1
///         8       |          5           |  15  |             2              |            1
///         9       |          5           |  15  |             2              |            2
///  The columns are sorted (and RLE) on this sort order:
///    (Lowest_Cardinality, Second_Highest_Cardinality, Highest_Cardinality, Time)
/// Out put ranges: 8 ranges on their invisible indices
///   [0, 1], 
///   [1, 2], 
///   [2, 3], 
///   [3, 5],  -- 2 rows with same values (4, 9, 1, 1)
///   [5, 6],
///   [6, 7],
///   [7, 9],  -- 2 rows with same values (5, 15, 2, 1)
///   [9, 10],

pub fn key_ranges(
    columns: &[SortColumn],
) -> Result<impl Iterator<Item = Range<usize>> + '_> {
    KeyRangeIterator::try_new(columns)
}

struct KeyRangeIterator<'a> {
    // function to compare values of columns
    // Todo: this is the same as LexicographicalComparator. 
    // Either use it or make it like https://github.com/apache/arrow-rs/issues/563
    comparator: KeyRangeComparator<'a>,  
    // Number of rows of the columns
    num_rows: usize,
    // end index of previous range which will be used as starting index of the next computing range
    start_range_idx: usize,
    //  
    // current_range_idx: usize,
    //value_indices: Vec<usize>,
}

impl<'a> KeyRangeIterator<'a> {
    fn try_new(columns: &'a [SortColumn]) -> Result<KeyRangeIterator<'a>> {
        if columns.is_empty() {
            panic!("Compare requires at least one column");
        }

        let num_rows = columns[0].values.len();
        if columns.iter().any(|item| item.values.len() != num_rows) {
            panic!("Sort columns have different row counts");
        };

        let comparator = KeyRangeComparator::try_new(columns)?;
        Ok(KeyRangeIterator {
            comparator,
            num_rows,
            start_range_idx: 0,
        })
    }
}

impl<'a> Iterator for KeyRangeIterator<'a> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        // End of the row
        if self.start_range_idx >= self.num_rows {
            return None;
        }

        let idx = self.start_range_idx + 1;
        while idx < self.num_rows {
            if self.comparator.compare(&self.start_range_idx, &idx) == Ordering::Equal {
                idx += 1;
            } else {
                break;
            }
        }
        let start = self.start_range_idx;
        self.start_range_idx = idx;
        Some(Range {start, end: idx})
    }
}

type KeyRangeCompareItem<'a> = (
    &'a ArrayData, // data
    DynComparator, // comparator
    SortOptions,   // sort_option
);

/// A comparator that wraps given array data (columns) and can compare data
/// at given two indices. The lifetime is the same at the data wrapped.
pub(super) struct KeyRangeComparator<'a> {
    compare_items: Vec<KeyRangeCompareItem<'a>>,
}

impl KeyRangeComparator<'_> {
    /// compare values at the wrapped columns with given indices.
    pub(super) fn compare<'a, 'b>(
        &'a self,
        a_idx: &'b usize,
        b_idx: &'b usize,
    ) -> Ordering {
        for (data, comparator, sort_option) in &self.compare_items {
            match (data.is_valid(*a_idx), data.is_valid(*b_idx)) {
                (true, true) => {
                    match (comparator)(*a_idx, *b_idx) {
                        // equal, move on to next column
                        Ordering::Equal => continue,
                        order => {
                            if sort_option.descending {
                                return order.reverse();
                            } else {
                                return order;
                            }
                        }
                    }
                }
                (false, true) => {
                    return if sort_option.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (true, false) => {
                    return if sort_option.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                // equal, move on to next column
                (false, false) => continue,
            }
        }

        Ordering::Equal
    }

    /// Create a new comparator that will wrap the given columns and give comparison
    /// results with two indices.
    pub(super) fn try_new(
        columns: &[SortColumn],
    ) -> Result<KeyRangeComparator<'_>> {
        let compare_items = columns
            .iter()
            .map(|column| {
                // flatten and convert build comparators
                // use ArrayData for is_valid checks later to avoid dynamic call
                let values = column.values.as_ref();
                let data = values.data_ref();
                Ok((
                    data,
                    build_compare(values, values)?,
                    column.options.unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(KeyRangeComparator { compare_items })
    }
}