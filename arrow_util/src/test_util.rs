//! A collection of testing functions for arrow based code
use std::sync::Arc;

use arrow::{
    array::{new_null_array, ArrayRef, StringArray},
    compute::kernels::sort::{lexsort, SortColumn, SortOptions},
    datatypes::Schema,
    error::ArrowError,
    record_batch::RecordBatch,
};

/// Compares the formatted output with the pretty formatted results of
/// record batches. This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
/// assert_batches_eq(expected_lines: &[&str], chunks: &[RecordBatch])
#[macro_export]
macro_rules! assert_batches_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> =
            $EXPECTED_LINES.into_iter().map(|s| s.to_string()).collect();

        let formatted = arrow_util::display::pretty_format_batches($CHUNKS).unwrap();

        let actual_lines = formatted.trim().split('\n').collect::<Vec<_>>();

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

/// Compares formatted output of a record batch with an expected
/// vector of strings in a way that order does not matter.
/// This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
///
/// `assert_batch_sorted_eq!(expected_lines: &[&str], batches: &[RecordBatch])`
#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = arrow_util::display::pretty_format_batches($CHUNKS).unwrap();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

// sort a record batch by all columns (to provide a stable output order for test
// comparison)
pub fn sort_record_batch(batch: RecordBatch) -> RecordBatch {
    let sort_input: Vec<SortColumn> = batch
        .columns()
        .iter()
        .map(|col| SortColumn {
            values: Arc::clone(col),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        })
        .collect();

    let sort_output = lexsort(&sort_input, None).expect("Sorting to complete");

    RecordBatch::try_new(batch.schema(), sort_output).unwrap()
}

/// Return a new `StringArray` where each element had a normalization
/// function `norm` applied.
pub fn normalize_string_array<N>(arr: &StringArray, norm: N) -> ArrayRef
where
    N: Fn(&str) -> String,
{
    let normalized: StringArray = arr.iter().map(|s| s.map(&norm)).collect();
    Arc::new(normalized)
}

/// Return a new set of `RecordBatch`es where the function `norm` has
/// applied to all `StringArray` rows.
pub fn normalize_batches<N>(batches: Vec<RecordBatch>, norm: N) -> Vec<RecordBatch>
where
    N: Fn(&str) -> String,
{
    // The idea here is is to get a function that normalizes strings
    // and apply it to each StringArray element by element
    batches
        .into_iter()
        .map(|batch| {
            let new_columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|array| {
                    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                        normalize_string_array(array, &norm)
                    } else {
                        Arc::clone(array)
                    }
                })
                .collect();

            RecordBatch::try_new(batch.schema(), new_columns)
                .expect("error occurred during normalization")
        })
        .collect()
}

/// Equalize batch schemas by creating NULL columns.
pub fn equalize_batch_schemas(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, ArrowError> {
    let common_schema = Arc::new(Schema::try_merge(
        batches.iter().map(|batch| batch.schema().as_ref().clone()),
    )?);

    Ok(batches
        .into_iter()
        .map(|batch| {
            let batch_schema = batch.schema();
            let columns = common_schema
                .fields()
                .iter()
                .map(|field| match batch_schema.index_of(field.name()) {
                    Ok(idx) => Arc::clone(batch.column(idx)),
                    Err(_) => new_null_array(field.data_type(), batch.num_rows()),
                })
                .collect();
            RecordBatch::try_new(Arc::clone(&common_schema), columns).unwrap()
        })
        .collect())
}
