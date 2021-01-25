//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
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
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
        DataType::Utf8,
        false,
    )]));
    let array = StringArray::from_iter(iter);
    let columns: Vec<ArrayRef> = vec![Arc::new(array)];

    RecordBatch::try_new(schema, columns)
}
