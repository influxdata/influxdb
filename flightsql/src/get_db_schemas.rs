use std::sync::Arc;

use crate::error::*;
use arrow::{
    array::{ArrayRef, StringBuilder},
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use once_cell::sync::Lazy;

/// Implementation of FlightSQL GetDbSchemas
///
/// TODO: use upstream implementation when
/// <https://github.com/apache/arrow-rs/pull/4296> is available
///
/// Return a RecordBatch for the GetDbSchemas
///
/// # Parameters
///
/// Definition from <https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1156-L1173>
///
/// catalog: Specifies the Catalog to search for the tables.
/// An empty string retrieves those without a catalog.
/// If omitted the catalog name should not be used to narrow the search.
///
/// db_schema_filter_pattern: Specifies a filter pattern for schemas to search for.
/// When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search.
/// In the pattern string, two special characters can be used to denote matching rules:
///    - "%" means to match any substring with 0 or more characters.
///    - "_" means to match any one character.
///
pub(crate) fn get_db_schemas(
    ctx: &SessionContext,
    catalog_filter: Option<String>,
    db_schema_filter_pattern: Option<String>,
) -> Result<RecordBatch> {
    // data needs to be ordered by
    // ORDER BY table_catalog, table_schema

    let mut builder = GetSchemasBuilder::new(db_schema_filter_pattern);

    let catalog_list = ctx.state().catalog_list();

    for catalog_name in sorted(catalog_list.catalog_names()) {
        // we just got the catalog name from the catalog_list, so it
        // should always be Some, but avoid unwrap to be safe
        let Some(catalog) = catalog_list.catalog(&catalog_name) else {
            continue
        };

        // if the catalog doesn't match request, skip
        if let Some(catalog_filter) = catalog_filter.as_ref() {
            if catalog_filter != &catalog_name {
                continue;
            }
        }

        let mut schema_names: Vec<_> = catalog.schema_names();
        schema_names.push(String::from("information_schema"));

        for schema_name in sorted(schema_names) {
            builder.append(&catalog_name, &schema_name)?;
        }
    }

    builder.build()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort_unstable();
    v
}

/// Return the schema of the RecordBatch that will be returned from
/// [`get_db_schemas`].
pub(crate) fn get_db_schemas_schema() -> SchemaRef {
    Arc::clone(&GET_DB_SCHEMAS_SCHEMA)
}

/// The schema for GetDbSchemas
static GET_DB_SCHEMAS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
    ]))
});

/// Builds rows like this:
///
/// * catalog_name: utf8,
/// * db_schema_name: utf8,
///
/// Applies filters for as described on [`get_db_schemas`]
struct GetSchemasBuilder {
    // Optional filters to apply
    db_schema_filter_pattern: Option<String>,

    catalog_name: StringBuilder,
    db_schema_name: StringBuilder,
}

impl GetSchemasBuilder {
    fn new(db_schema_filter_pattern: Option<String>) -> Self {
        let catalog_name = StringBuilder::new();
        let db_schema_name = StringBuilder::new();

        Self {
            db_schema_filter_pattern,

            catalog_name,
            db_schema_name,
        }
    }

    /// Append a row
    fn append(&mut self, catalog_name: &str, schema_name: &str) -> Result<()> {
        self.catalog_name.append_value(catalog_name);
        self.db_schema_name.append_value(schema_name);
        Ok(())
    }

    /// builds the correct schema
    fn build(self) -> Result<RecordBatch> {
        let Self {
            db_schema_filter_pattern,

            mut catalog_name,
            mut db_schema_name,
        } = self;

        // Make the arrays
        let catalog_name = catalog_name.finish();
        let db_schema_name = db_schema_name.finish();

        // the filter, if requested, getting a BooleanArray that represents
        // the rows that passed the filter
        let filter = db_schema_filter_pattern
            .map(|db_schema_filter_pattern| {
                // use like kernel to get wildcard matching
                arrow::compute::like_utf8_scalar(&db_schema_name, &db_schema_filter_pattern)
            })
            .transpose()?;

        let batch = RecordBatch::try_new(
            get_db_schemas_schema(),
            vec![
                Arc::new(catalog_name) as ArrayRef,
                Arc::new(db_schema_name) as ArrayRef,
            ],
        )?;

        // Apply the filters if needed
        if let Some(filter) = filter {
            Ok(filter_record_batch(&batch, &filter)?)
        } else {
            Ok(batch)
        }
    }
}
