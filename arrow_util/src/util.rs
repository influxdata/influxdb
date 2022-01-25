//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{new_null_array, ArrayRef, StringArray},
    datatypes::SchemaRef,
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

/// Merge the record bacthes into one record batch
/// and padd null values to columns that are not available in certain bacthes
pub fn merge_record_batches(
    output_schema: SchemaRef,
    batches: Vec<Arc<RecordBatch>>,
) -> Result<Option<RecordBatch>, arrow::error::ArrowError> {
    // Add null values for non-existing columns
    let batches = batches
        .iter()
        .map(|batch| {
            let batch_schema = batch.schema();

            // Go over all columns of output_schema
            let batch_output_columns = output_schema
                .fields()
                .iter()
                .map(|output_field| {
                    // See if the output_field available in the batch
                    let batch_field_index = batch_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .find(|(_, batch_field)| output_field.name() == batch_field.name())
                        .map(|(idx, _)| idx);

                    if let Some(batch_field_index) = batch_field_index {
                        // The column available, use it
                        Arc::clone(batch.column(batch_field_index))
                    } else {
                        // the column not avaialble, add it with all null values
                        new_null_array(output_field.data_type(), batch.num_rows())
                    }
                })
                .collect::<Vec<_>>();

            RecordBatch::try_new(Arc::clone(&output_schema), batch_output_columns)
                .expect("A record batch should have been created")
        })
        .collect::<Vec<_>>();

    // Combine batches
    if batches.is_empty() {
        return Ok(None);
    }

    Ok(Some(RecordBatch::concat(&output_schema, &batches)?))
}
