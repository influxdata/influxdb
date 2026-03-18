mod mutable_batch;
mod record_batch;

use bitvec::{prelude::Lsb0, view::BitView};
use data_types::TableBatchWithoutId;

use generated_types::influxdata::pbdata::v1::{
    self as proto,
    column::{SemanticType, Values},
};
use schema::InfluxColumnType;
use table_batch::ValueCollection;
use thiserror::Error;

/// An error accessing the time column of a batch.
#[expect(missing_copy_implementations)]
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

impl PartitioningColumn for proto::Column {
    type TagIdentityKey = u32;

    fn is_valid(&self, idx: usize) -> bool {
        let null_bits = self.null_mask.view_bits::<Lsb0>();

        // If we have the null bit set, then we know it's invalid, since all trailing bits that
        // aren't actually a part of the null mask must be set to 0.
        if null_bits.get(idx).is_some_and(|f| *f) {
            return false;
        }

        let num_nulls = null_bits.get(..idx).unwrap_or(null_bits).count_ones();
        let real_idx = idx - num_nulls;

        self.values.as_ref().is_some_and(|v| {
            let Values {
                i64_values,
                f64_values,
                u64_values,
                string_values,
                bool_values,
                bytes_values,
                packed_string_values,
                interned_string_values,
            } = v;

            if !i64_values.is_empty() {
                i64_values.len() > real_idx
            } else if !f64_values.is_empty() {
                f64_values.len() > real_idx
            } else if !u64_values.is_empty() {
                u64_values.len() > real_idx
            } else if !string_values.is_empty() {
                string_values.len() > real_idx
            } else if !bool_values.is_empty() {
                bool_values.len() > real_idx
            } else if !bytes_values.is_empty() {
                bytes_values.len() > real_idx
            } else if let Some(strs) = packed_string_values.as_ref().filter(|p| !p.is_empty()) {
                strs.len() > real_idx
            } else if let Some(strs) = interned_string_values.as_ref().filter(|i| !i.is_empty()) {
                strs.len() > real_idx
            } else {
                false
            }
        })
    }

    fn get_tag_identity_key(&self, idx: usize) -> Option<&Self::TagIdentityKey> {
        const TAG_I32: i32 = SemanticType::Tag as i32;
        match self.semantic_type {
            TAG_I32 => {
                let null_bits = self.null_mask.view_bits::<Lsb0>();
                let is_valid = null_bits.get(idx).is_none_or(|b| b == false);
                if !is_valid {
                    return None;
                }

                // To get the real index, we just need to count how many nulls exist in the map up
                // to this index, then subtract that from the index we're trying to get. For
                // example, if the null mask is [0, 1, 0, 1, 0], and we want to get idx 4, we'll see
                // that 2 nulls exist before. So we do 4 - 2 to get the real idx of 2, then use that
                // as an index for the valid values.
                // If, however, the null mask is empty or doesn't cover this index, then we know
                // that everything past the end of the null mask is valid, so this strategy still
                // works. Just count the nulls before and subtract.
                let nulls_before = null_bits.get(..idx).unwrap_or(null_bits).count_ones();
                self.values
                    .as_ref()
                    .and_then(|v| v.interned_string_values.as_ref())
                    .and_then(|v| v.values.get(idx - nulls_before))
            }
            _ => None,
        }
    }

    fn get_tag_value<'a>(&'a self, tag_identity_key: &'a Self::TagIdentityKey) -> Option<&'a str> {
        match self.semantic_type() {
            SemanticType::Tag => self
                .values
                .as_ref()
                .and_then(|v| v.interned_string_values.as_ref())
                .and_then(|s| s.dictionary.as_ref())
                .map(|dict| {
                    let start =
                        usize::try_from(dict.offsets[usize::try_from(*tag_identity_key).unwrap()])
                            .unwrap();
                    let end = usize::try_from(
                        dict.offsets[usize::try_from(tag_identity_key + 1).unwrap()],
                    )
                    .unwrap();
                    dict.values.split_at(start).1.split_at(end - start).0
                }),
            _ => None,
        }
    }

    fn type_description(&self) -> String {
        match self.semantic_type() {
            SemanticType::Unspecified => {
                unreachable!("We should never receive an unspecified type in a TableBatch")
            }
            SemanticType::Tag => InfluxColumnType::Tag.to_string(),
            SemanticType::Time => InfluxColumnType::Timestamp.to_string(),
            SemanticType::Field => {
                let Values {
                    i64_values,
                    f64_values,
                    u64_values,
                    string_values,
                    bool_values,
                    bytes_values: _,
                    packed_string_values,
                    interned_string_values,
                } = self.values.as_ref().unwrap();

                if !i64_values.is_empty() {
                    InfluxColumnType::Field(schema::InfluxFieldType::Integer).to_string()
                } else if !f64_values.is_empty() {
                    InfluxColumnType::Field(schema::InfluxFieldType::Float).to_string()
                } else if !u64_values.is_empty() {
                    InfluxColumnType::Field(schema::InfluxFieldType::UInteger).to_string()
                } else if !bool_values.is_empty() {
                    InfluxColumnType::Field(schema::InfluxFieldType::Boolean).to_string()
                } else if !string_values.is_empty()
                    || packed_string_values.is_some()
                    || interned_string_values.is_some()
                {
                    InfluxColumnType::Field(schema::InfluxFieldType::String).to_string()
                } else {
                    unreachable!(
                        "By the point that we're doing partitioning, we should've already verified that we have values for each column that we want to partition"
                    )
                }
            }
        }
    }
}

impl Batch for TableBatchWithoutId {
    type Column = proto::Column;

    fn num_rows(&self) -> usize {
        self.row_count.try_into().unwrap()
    }

    fn column(&self, column: &str) -> Option<&Self::Column> {
        self.columns.iter().find(|c| c.column_name == column)
    }

    fn time_column(&self) -> Result<&[i64], TimeColumnError> {
        let t = self
            .columns
            .iter()
            .find(|c| c.semantic_type() == SemanticType::Time)
            .ok_or(TimeColumnError::NotFound)?;

        Ok(&t
            .values
            .as_ref()
            .ok_or(TimeColumnError::NotFound)?
            .i64_values)
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::lines_to_table_batch;

    use super::*;
    use influxdb_line_protocol::{ParsedLine, test_helpers::arbitrary_parsed_lines_same_table};
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use table_batch::column::ColumnExt;

    fn verify_column_transformations(lines: Vec<ParsedLine<'static>>) {
        let (tb, mutable_batch) = lines_to_table_batch(lines);

        let mb_columns = mutable_batch.columns();
        assert_eq!(mb_columns.len(), tb.columns.len());

        for (idx, name, mb_column) in mb_columns {
            let tb_column = &tb.columns[idx];
            assert_eq!(&tb_column.column_name, name);

            let row_count = tb_column.num_rows();

            for i in 0..row_count {
                let tb_is_valid = tb_column.is_valid(i);
                let mb_is_valid = mb_column.is_valid(i);

                assert_eq!(
                    tb_is_valid, mb_is_valid,
                    "valid mismatch at idx {i}. tb_column: {tb_column:#?}, mb_column: {mb_column:#?}"
                );

                // the `get_tag_identity_key` impl for MutableBatch's column `debug_assert`s that
                // it's valid, so we need to only try calling that when we've verified that it's
                // valid.
                if tb_is_valid {
                    let tb_id_key = tb_column.get_tag_identity_key(i);
                    let mb_id_key = mb_column.get_tag_identity_key(i);

                    assert_eq!(
                        tb_id_key.map(|u| i32::try_from(*u).unwrap()),
                        mb_id_key.copied()
                    );

                    if let Some(tb_id_key) = tb_id_key {
                        let tb_tag_value = tb_column.get_tag_value(tb_id_key);
                        let mb_tag_value = mb_column.get_tag_value(mb_id_key.unwrap());

                        assert_eq!(tb_tag_value, mb_tag_value);
                    }
                }

                assert_eq!(tb_column.type_description(), mb_column.type_description());
            }
        }
    }

    proptest! {
        #[test]
        fn column_and_batch_parameters_work_equivalently(
            lines in arbitrary_parsed_lines_same_table(false)
        ) {
            verify_column_transformations(lines);
        }
    }
}
