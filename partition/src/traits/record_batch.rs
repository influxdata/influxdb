use super::{Batch, PartitioningColumn, TimeColumnError};
use arrow::{
    array::{Array, DictionaryArray, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use schema::TIME_COLUMN_NAME;
use std::sync::Arc;

impl PartitioningColumn for Arc<dyn Array> {
    type TagIdentityKey = str;

    fn is_valid(&self, idx: usize) -> bool {
        Array::is_valid(&self, idx)
    }

    fn valid_bytes(&self) -> &[u8] {
        self.nulls()
            .expect("this RecordBatch's Array should be nullable")
            .validity()
    }

    fn get_tag_identity_key(&self, idx: usize) -> Option<&Self::TagIdentityKey> {
        debug_assert!(PartitioningColumn::is_valid(self, idx));
        match self.data_type() {
            DataType::Utf8 => self
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|col_data| col_data.value(idx)),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
            {
                let dict = self
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("should have gotten a DictionaryArray");

                let values = dict
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("should have gotten a StringArray");
                Some(values.value(dict.key(idx)?))
            }
            _ => None,
        }
    }

    fn get_tag_value<'a>(&'a self, tag_identity_key: &'a Self::TagIdentityKey) -> Option<&'a str> {
        Some(tag_identity_key)
    }

    fn type_description(&self) -> String {
        self.data_type().to_string()
    }
}

impl Batch for RecordBatch {
    type Column = Arc<dyn Array>;

    fn num_rows(&self) -> usize {
        self.num_rows()
    }

    fn column(&self, column: &str) -> Option<&Self::Column> {
        self.column_by_name(column)
    }

    fn time_column(&self) -> Result<&[i64], TimeColumnError> {
        let time_column = self
            .column_by_name(TIME_COLUMN_NAME)
            .ok_or(TimeColumnError::NotFound)?;

        Ok(time_column
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("time column was an unexpected type")
            .values()
            .inner()
            .typed_data())
    }
}
