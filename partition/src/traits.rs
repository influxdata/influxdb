mod mutable_batch;
mod record_batch;

use thiserror::Error;

/// An error accessing the time column of a batch.
#[allow(missing_copy_implementations)]
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum TimeColumnError {
    /// The batch did not have a time column.
    #[error("No time column found")]
    NotFound,
}

/// The behavior a column in a batch needs to have to be partitioned
pub trait PartitioningColumn: std::fmt::Debug {
    /// The type of a thing that can be used to identify whether a tag has changed or not; may or
    /// may not be the actual tag
    type TagIdentityKey: ?Sized + PartialEq;

    /// Whether the value at the given row index is valid or NULL
    fn is_valid(&self, idx: usize) -> bool;

    /// The raw packed validity bytes.
    ///
    /// The validity mask MUST follow the Arrow specification for validity masks
    /// (<https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps>).
    fn valid_bytes(&self) -> &[u8];

    /// Get the identity of the tag at the given row index.
    ///
    /// The return value is only valid if `is_valid(idx)` for the same `idx`
    /// returns true.
    fn get_tag_identity_key(&self, idx: usize) -> Option<&Self::TagIdentityKey>;

    /// Get the value of the tag that has the given identity
    fn get_tag_value<'a>(&'a self, tag_identity_key: &'a Self::TagIdentityKey) -> Option<&'a str>;

    /// A string describing this column's data type; used in error messages
    fn type_description(&self) -> String;
}

/// Behavior of a batch of data used by partitioning code
pub trait Batch {
    /// The type of this batch's columns
    type Column: PartitioningColumn;

    /// How many rows are in this batch
    fn num_rows(&self) -> usize;

    /// The column in the batch with the given name, if any
    fn column(&self, column: &str) -> Option<&Self::Column>;

    /// Return the values in the time column in this batch. Return an error if the batch has no
    /// time column.
    ///
    /// # Panics
    ///
    /// If a time column exists but its data isn't the expected type, this function will panic.
    fn time_column(&self) -> Result<&[i64], TimeColumnError>;
}
