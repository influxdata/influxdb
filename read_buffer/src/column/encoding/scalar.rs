pub mod fixed;
pub mod fixed_null;
pub mod rle;
pub mod transcoders;

use either::Either;
use std::{fmt::Debug, fmt::Display};

use crate::column::{cmp, RowIDs};

pub use fixed::Fixed;
pub use fixed_null::FixedNull;
pub use rle::RLE;

/// `ScalarEncoding` describes the behaviour of a columnar encoding for scalar
/// values.
pub trait ScalarEncoding<L>: Debug + Display + Send + Sync {
    /// A useful name for the encoding, likely used in instrumentation.
    fn name(&self) -> &'static str;

    /// The total size in bytes to store encoded data in memory.
    fn size(&self) -> usize;

    /// The estimated total size in bytes of the underlying encoded values if
    /// they were stored contiguously as a vector of `L`. `include_null` should
    /// decide whether to consider NULL values to take up `size_of::<L>()`
    /// bytes.
    fn size_raw(&self, include_nulls: bool) -> usize;

    /// The total number of rows in the encoding.
    fn num_rows(&self) -> u32;

    /// The total number of NULL values in the encoding.
    fn null_count(&self) -> u32;

    /// Determines if the encoding contains a non-null value.
    fn has_any_non_null_value(&self) -> bool;

    /// Determines if the encoding contains a non-null value at one of the
    /// provided ordinal offsets.
    fn has_non_null_value(&self, row_ids: &[u32]) -> bool;

    /// Returns the logical value `L` located at the provided ordinal offset.
    fn value(&self, row_id: u32) -> Option<L>;

    /// Returns the logical values found at the provided ordinal offsets.
    /// Implementations that can't contain NULL values have the option to return
    /// `Either::Left`.
    fn values(&self, row_ids: &[u32]) -> Either<Vec<L>, Vec<Option<L>>>;

    /// Returns all logical values in the column. Implementations that can't
    /// contain NULL values have the option to return `Either::Left`.
    fn all_values(&self) -> Either<Vec<L>, Vec<Option<L>>>;

    /// Returns the ordinal offsets that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// logical value will fit within the physical type of the encoded column.
    fn row_ids_filter(&self, value: L, op: &cmp::Operator, dst: RowIDs) -> RowIDs;

    /// Returns the ordinal offsets that satisfy both the provided predicates.
    ///
    /// This method is a special case optimisation for common cases where one
    /// wishes to do the equivalent of WHERE x > y AND x <= y` for example.
    ///
    /// Essentially, this supports:
    ///     `x {>, >=, <, <=} value1 AND x {>, >=, <, <=} value2`.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Logical` value is comparable to the encoding's physical type.
    fn row_ids_filter_range(
        &self,
        low: (L, &cmp::Operator),
        high: (L, &cmp::Operator),
        dst: RowIDs,
    ) -> RowIDs;

    /// The minimum non-null value located at any of the provided ordinal
    /// offsets.
    fn min(&self, row_ids: &[u32]) -> Option<L>;

    /// The maximum non-null value located at any of the provided ordinal
    /// offsets.
    fn max(&self, row_ids: &[u32]) -> Option<L>;

    /// The sum of all non-null values located at the provided ordinal offsets.
    fn sum(&self, row_ids: &[u32]) -> Option<L>;

    /// The count of non-null values located at the provided ordinal offsets.
    fn count(&self, row_ids: &[u32]) -> u32;
}
