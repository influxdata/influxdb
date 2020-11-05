//! This module contains the definition of a "StringSet" a set of
//! logical strings and the code to create them from record batches.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use arrow_deps::{
    arrow,
    arrow::array::{Array, StringArray},
    arrow::datatypes::DataType,
};
use snafu::{ensure, OptionExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error extracting results from Record Batches: schema not a single Utf8: {:?}",
        schema
    ))]
    InternalSchemaWasNotString { schema: SchemaRef },

    #[snafu(display("Internal error, failed to downcast field to Utf8"))]
    InternalFailedToDowncast {},

    #[snafu(display("Internal error, unexpected null value"))]
    InternalUnexpectedNull {},

    #[snafu(display(
        "Error reading record batch while converting to StringSet: {:?}",
        source
    ))]
    ReadingRecordBatch { source: arrow::error::ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type StringSet = BTreeSet<String>;
pub type StringSetRef = Arc<StringSet>;

/// Trait to convert RecordBatch'y things into
/// `StringSetRef`s. Assumes that the input record batches each have a
/// single string column. Can return errors, so don't use
/// `std::convert::From`
pub trait IntoStringSet {
    /// Convert this thing into a stringset
    fn into_stringset(self) -> Result<StringSetRef>;
}

/// Converts record batches into StringSets.
impl IntoStringSet for Vec<RecordBatch> {
    fn into_stringset(self) -> Result<StringSetRef> {
        let mut strings = StringSet::new();

        // process the record batches one by one
        for record_batch in self.into_iter() {
            let num_rows = record_batch.num_rows();
            let schema = record_batch.schema();
            let fields = schema.fields();
            ensure!(
                fields.len() == 1,
                InternalSchemaWasNotString {
                    schema: schema.clone(),
                }
            );

            let field = &fields[0];

            ensure!(
                field.data_type() == &DataType::Utf8,
                InternalSchemaWasNotString {
                    schema: schema.clone(),
                }
            );

            let array = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context(InternalFailedToDowncast)?;

            add_utf8_array_to_stringset(&mut strings, array, num_rows)?;
        }
        Ok(StringSetRef::new(strings))
    }
}

fn add_utf8_array_to_stringset(
    dest: &mut StringSet,
    src: &StringArray,
    num_rows: usize,
) -> Result<()> {
    for i in 0..num_rows {
        // Not sure how to handle a NULL -- StringSet contains
        // Strings, not Option<String>
        if src.is_null(i) {
            return InternalUnexpectedNull {}.fail();
        } else {
            let src_value = src.value(i);
            if !dest.contains(src_value) {
                dest.insert(src_value.into());
            }
        }
    }
    Ok(())
}
