use schema::InfluxColumnType;

use crate::writer::{ColumnInsertValidator, InvalidInsertionError};

/// A no-op [`ColumnInsertValidator`], which always returns `Ok`.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopValidator;

impl ColumnInsertValidator for NoopValidator {
    fn validate_insertion(
        &self,
        _col_name: &str,
        _col_type: InfluxColumnType,
    ) -> Result<(), InvalidInsertionError> {
        Ok(())
    }
}
