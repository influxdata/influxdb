use std::sync::Arc;

use crate::error::*;
use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use once_cell::sync::Lazy;

/// Returns the list of catalogs in the DataFusion catalog
pub(crate) fn get_catalogs(ctx: &SessionContext) -> Result<RecordBatch> {
    let mut catalog_names = ctx.catalog_names();
    catalog_names.sort_unstable();

    let batch = RecordBatch::try_new(
        Arc::clone(&GET_CATALOG_SCHEMA),
        vec![Arc::new(StringArray::from_iter_values(catalog_names)) as _],
    )?;
    Ok(batch)
}

/// Returns the schema that will result from [`get_catalogs`]
pub(crate) fn get_catalogs_schema() -> &'static Schema {
    &GET_CATALOG_SCHEMA
}

/// The schema for GetCatalogs
static GET_CATALOG_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "catalog_name",
        DataType::Utf8,
        false,
    )]))
});
