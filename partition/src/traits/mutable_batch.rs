use super::{Batch, PartitioningColumn, TimeColumnError};
use mutable_batch::{
    column::{Column as MutableBatchColumn, ColumnData},
    MutableBatch,
};
use schema::TIME_COLUMN_NAME;

impl PartitioningColumn for MutableBatchColumn {
    type TagIdentityKey = i32;

    fn is_valid(&self, idx: usize) -> bool {
        self.valid_mask().get(idx)
    }

    fn valid_bytes(&self) -> &[u8] {
        self.valid_mask().bytes()
    }

    fn get_tag_identity_key(&self, idx: usize) -> Option<&Self::TagIdentityKey> {
        debug_assert!(PartitioningColumn::is_valid(self, idx));
        match self.data() {
            ColumnData::Tag(col_data, _, _) => Some(&col_data[idx]),
            _ => None,
        }
    }

    fn get_tag_value<'a>(&'a self, tag_identity_key: &'a Self::TagIdentityKey) -> Option<&'a str> {
        match self.data() {
            ColumnData::Tag(_, dictionary, _) => dictionary.lookup_id(*tag_identity_key),
            _ => None,
        }
    }

    fn type_description(&self) -> String {
        self.influx_type().to_string()
    }
}

impl Batch for MutableBatch {
    type Column = MutableBatchColumn;

    fn num_rows(&self) -> usize {
        self.rows()
    }

    fn column(&self, column: &str) -> Option<&Self::Column> {
        self.column(column).ok()
    }

    fn time_column(&self) -> Result<&[i64], TimeColumnError> {
        let time_column = self
            .column(TIME_COLUMN_NAME)
            .map_err(|_| TimeColumnError::NotFound)?;

        match &time_column.data() {
            ColumnData::I64(col_data, _) => Ok(col_data),
            x => unreachable!("expected i64 got {}", x),
        }
    }
}
