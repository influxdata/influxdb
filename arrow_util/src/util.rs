//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    error::ArrowError,
    record_batch::RecordBatch,
};

/// Returns a single column record batch of type Utf8 from the
/// contents of something that can be turned into an iterator over
/// `Option<&str>`
pub fn str_iter_to_batch<Ptr, I>(field_name: &str, iter: I) -> Result<RecordBatch, ArrowError>
where
    I: IntoIterator<Item = Option<Ptr>>,
    Ptr: AsRef<str>,
{
    let array = StringArray::from_iter(iter);

    RecordBatch::try_from_iter(vec![(field_name, Arc::new(array) as ArrayRef)])
}
