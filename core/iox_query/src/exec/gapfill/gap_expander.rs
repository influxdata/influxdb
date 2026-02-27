use arrow::array::Array;
use datafusion::{common::Result, scalar::ScalarValue};
use std::ops::{Bound, Range};

/// A type alias for the expanded value from an array.
/// If the value was present in the array the second
/// element contains it's row index.
pub type ExpandedValue = (ScalarValue, Option<usize>);

/// Trait that is used in the gap-filling algorithm to find missing
/// rows in the input data.
pub trait GapExpander: std::fmt::Display + std::fmt::Debug {
    /// Expand missing rows in the input data. The input array needs to be
    /// sorted in ascending order, and must not contain any NULL values.
    ///
    /// If `start` is `Unbounded`, the first value in the array is used
    /// as the starting value, otherwise the start is calculated from
    /// the bound value. Although, the start value is ignored if it is
    /// larger than the first value in the array. Required rows are
    /// inserted between the start and the first value in the array.
    ///
    /// If `end` is `Unbounded`, the last value in the array is used
    /// as the ending value, otherwise the end is calculated from the
    /// bound value. Although, the end value is ignored if it is smaller
    /// than the last value in the array. Required rows are inserted
    /// between the last value in the array and the end.
    ///
    /// The input array may contain no values, but the output will only
    /// contain data if both start and end are unbound.
    ///
    /// The output is a tuple of two vectors. The first vector contains
    /// the array of values with the missing rows inserted. The second
    /// vector contains the original indexes of the rows that are present
    /// in the input array.
    fn expand_gaps(
        &self,
        range: Range<Bound<ScalarValue>>,
        array: &dyn Array,
        max_output_rows: usize,
    ) -> Result<(Vec<ExpandedValue>, usize)>;

    /// Count the number of output rows that would be produced by `expand_gaps`
    /// with the provided parameters.
    fn count_rows(&self, range: Range<Bound<ScalarValue>>, array: &dyn Array) -> Result<usize>;
}
