//! This module contains the definition of a "StringSet" a set of
//! logical strings and the code to create them from record batches.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{Array, DictionaryArray, StringArray},
    datatypes::{DataType, Int32Type, SchemaRef},
    record_batch::RecordBatch,
};

use snafu::{ensure, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error extracting results from Record Batches: schema not a single Utf8 or string dictionary: {:?}",
        schema
    ))]
    InternalSchemaWasNotString { schema: SchemaRef },

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

impl IntoStringSet for &[&str] {
    fn into_stringset(self) -> Result<StringSetRef> {
        let set: StringSet = self.iter().map(|s| s.to_string()).collect();
        Ok(Arc::new(set))
    }
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
                    schema: Arc::clone(&schema),
                }
            );

            let field = &fields[0];

            match field.data_type() {
                DataType::Utf8 => {
                    let array = record_batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();

                    add_utf8_array_to_stringset(&mut strings, array, num_rows)?;
                }
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let array = record_batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .unwrap();

                    add_utf8_dictionary_to_stringset(&mut strings, array, num_rows)?;
                }
                _ => InternalSchemaWasNotString {
                    schema: Arc::clone(&schema),
                }
                .fail()?,
            }
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

fn add_utf8_dictionary_to_stringset(
    dest: &mut StringSet,
    dictionary: &DictionaryArray<Int32Type>,
    num_rows: usize,
) -> Result<()> {
    let keys = dictionary.keys();
    let values = dictionary.values();
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();

    // It might be quicker to construct an intermediate collection
    // of unique indexes and then hydrate them

    for i in 0..num_rows {
        // Not sure how to handle a NULL -- StringSet contains
        // Strings, not Option<String>
        if keys.is_null(i) {
            return InternalUnexpectedNull {}.fail();
        } else {
            let idx = keys.value(i);
            let src_value = values.value(idx as _);
            if !dest.contains(src_value) {
                dest.insert(src_value.into());
            }
        }
    }
    Ok(())
}
