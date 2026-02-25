//! Utility functions for working with arrow

use std::sync::Arc;

use arrow::{
    array::new_null_array, datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch,
};

/// Create a new [`RecordBatch`] that has the specified schema, adding null values for columns that
/// don't appear in the batch.
pub fn ensure_schema(
    output_schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    // Go over all columns of output_schema
    let batch_output_columns = output_schema
        .fields()
        .iter()
        .map(|output_field| {
            // If the field is available in the batch, use it. Otherwise, add a column with all
            // null values.
            batch
                .column_by_name(output_field.name())
                .cloned()
                .unwrap_or_else(|| new_null_array(output_field.data_type(), batch.num_rows()))
        })
        .collect();

    RecordBatch::try_new(Arc::clone(output_schema), batch_output_columns)
}
