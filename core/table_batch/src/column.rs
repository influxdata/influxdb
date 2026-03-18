use generated_types::influxdata::pbdata::v1::{self as proto, column::Values};

use crate::ValueCollection;

/// A helper trait for [`proto::Column`]s. At time of writing, it only contains a function to get
/// the number of rows out of a column, but can be expanded for when we need to add more
/// functionality to [`proto::Column`]s.
pub trait ColumnExt {
    /// Get the number of values, null and nonnull, within this column.
    fn num_rows(&self) -> usize;
}

impl ColumnExt for proto::Column {
    fn num_rows(&self) -> usize {
        let Some(values) = self.values.as_ref() else {
            return 0;
        };

        let Values {
            i64_values,
            f64_values,
            u64_values,
            string_values,
            bool_values,
            bytes_values,
            packed_string_values,
            interned_string_values,
        } = values;

        // First, we want to get the total number of nonnull values
        let mut nonnull_values_left = if !i64_values.is_empty() {
            i64_values.len()
        } else if !f64_values.is_empty() {
            f64_values.len()
        } else if !u64_values.is_empty() {
            u64_values.len()
        } else if !string_values.is_empty() {
            string_values.len()
        } else if !bool_values.is_empty() {
            bool_values.len()
        } else if !bytes_values.is_empty() {
            bytes_values.len()
        } else if let Some(strs) = packed_string_values.as_ref().filter(|p| !p.is_empty()) {
            strs.offsets.len().saturating_sub(1)
        } else if let Some(strs) = interned_string_values.as_ref().filter(|p| !p.is_empty()) {
            strs.values.len()
        } else {
            0
        };

        // this is to count how many values are represented by the null mask
        let mut values_in_mask = 0;

        // so we go through each byte in the null mask
        'outer: for byte in &self.null_mask {
            // check how many nonnulls this specific byte represents
            let nonnulls = byte.count_zeros() as usize;

            // if this represents more nonnnulls than values we have left, we need to go bit-by-bit
            // to actually check how many nonnulls exist in this byte.
            if nonnulls >= nonnull_values_left {
                // so for each bit, we check if it seems to be null/nonnull and how many nonnull
                // values we have left
                for bit_idx in 0..8 {
                    match (byte & (1 << bit_idx), nonnull_values_left) {
                        // If the bit mask says it should be valid, but we have none left, then
                        // we've reached the end of the bitmask. Ignore the rest.
                        (0, 0) => break 'outer,
                        // If it's null, regardless of how many nonnull values we have left, that's
                        // just another counted value
                        (1.., _) => values_in_mask += 1,
                        // If it's not null and we have more nonnulls, then decrement one from the
                        // remaining nonnulls and increment one value
                        (0, 1..) => {
                            nonnull_values_left -= 1;
                            values_in_mask += 1;
                        }
                    }
                }
            } else {
                // If we have more nonnull values left than this byte represents, just do some
                // simple arithmetic to account for it and continue.
                nonnull_values_left -= nonnulls;
                values_in_mask += 8;
            }
        }

        values_in_mask + nonnull_values_left
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{
        Column,
        column::{SemanticType, Values},
    };

    #[test]
    fn count_column_rows() {
        let num = Column {
            column_name: "a".to_string(),
            semantic_type: SemanticType::Tag.into(),
            values: None,
            null_mask: vec![0b01010101],
        }
        .num_rows();
        assert_eq!(num, 0);

        let num = Column {
            column_name: "a".to_string(),
            semantic_type: SemanticType::Time.into(),
            values: Some(Values {
                i64_values: vec![1, 2],
                ..Values::default()
            }),
            null_mask: vec![0b00000101],
        }
        .num_rows();
        assert_eq!(num, 4);

        // This one should have two values because we should stop counting once we hit the first
        // non-null value in the null mask which doesn't have a corresponding value in self.values
        let num = Column {
            column_name: "a".to_string(),
            semantic_type: SemanticType::Field.into(),
            values: Some(Values {
                f64_values: vec![1.0],
                ..Values::default()
            }),
            null_mask: vec![0b10000001, 0b00000001],
        }
        .num_rows();
        assert_eq!(num, 2);

        let num = Column {
            column_name: "a".to_string(),
            semantic_type: SemanticType::Field.into(),
            values: Some(Values {
                f64_values: vec![1.0, 6.0],
                ..Values::default()
            }),
            null_mask: vec![0b10000001],
        }
        .num_rows();
        assert_eq!(num, 3);

        let num = Column {
            column_name: "a".to_string(),
            semantic_type: SemanticType::Field.into(),
            values: Some(Values {
                u64_values: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                ..Values::default()
            }),
            null_mask: vec![],
        }
        .num_rows();
        assert_eq!(num, 10);

        let num = Column {
            column_name: "a".to_string(),
            semantic_type: SemanticType::Field.into(),
            values: Some(Values {
                u64_values: vec![0],
                ..Values::default()
            }),
            null_mask: vec![0b01111111],
        }
        .num_rows();
        assert_eq!(num, 8);
    }
}
