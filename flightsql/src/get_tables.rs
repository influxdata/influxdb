use std::sync::Arc;

use crate::error::*;
use arrow::{
    array::{ArrayRef, BinaryBuilder, StringBuilder},
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{IpcMessage, SchemaAsIpc};
use datafusion::{logical_expr::TableType, prelude::SessionContext, sql::TableReference};
use once_cell::sync::Lazy;

/// Implementation of FlightSQL GetTables in terms of a DataFusion SessionContext

/// Return a RecordBatch for the GetTables
///
/// Return a `LogicalPlan` for GetTables
///
/// # Parameters
///
/// Definition from <https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1176-L1241>
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
/// table_name_filter_pattern: Specifies a filter pattern for tables to search for.
/// When no table_name_filter_pattern is provided, all tables matching other filters are searched.
/// In the pattern string, two special characters can be used to denote matching rules:
///    - "%" means to match any substring with 0 or more characters.
///    - "_" means to match any one character.
///
/// table_types: Specifies a filter of table types which must match.
/// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
/// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
///
/// include_schema: Specifies if the Arrow schema should be returned for found tables.
///
pub(crate) async fn get_tables(
    ctx: &SessionContext,
    catalog_filter: Option<String>,
    db_schema_filter_pattern: Option<String>,
    table_name_filter_pattern: Option<String>,
    table_types_filter: Vec<String>,
    include_schema: bool,
) -> Result<RecordBatch> {
    // data needs to be ordered by
    // ORDER BY table_catalog, table_schema, table_name, table_type",

    let mut builder = GetTablesBuilder::new(
        db_schema_filter_pattern,
        table_name_filter_pattern,
        include_schema,
    );

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
            if schema_name == "information_schema" {
                // special case the "public"."information_schema" as it is a
                // "virtual" catalog in DataFusion and thus is not reported
                // directly via the table providers
                // We ensure this list is kept in sync with tests
                let table_names = vec!["columns", "df_settings", "tables", "views"];
                for table_name in table_names {
                    let table_ref = TableReference::full(&catalog_name, &schema_name, table_name);

                    let Some(table) = ctx.table(table_ref).await.ok() else {
                        continue;
                    };

                    let table_type = "VIEW";
                    if skip_table(table_type, &table_types_filter) {
                        continue;
                    }

                    let arrow_schema: Schema = table.schema().into();

                    builder.append(
                        &catalog_name,
                        "information_schema",
                        table_name,
                        table_type,
                        &arrow_schema,
                    )?;
                }
            } else {
                // schemas other than "information_schema"
                let Some(schema) = catalog.schema(&schema_name) else {
                    continue
                };

                for table_name in sorted(schema.table_names()) {
                    let Some(table) = schema.table(&table_name).await else {
                        continue
                    };

                    // If the table type doesn't match the types specified in the query, skip
                    let table_type = table_type_name(table.table_type());
                    if skip_table(table_type, &table_types_filter) {
                        continue;
                    }

                    builder.append(
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        table_type,
                        table.schema().as_ref(),
                    )?;
                }
            }
        }
    }

    builder.build()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort_unstable();
    v
}

// returns true if table_type does not appear in `table_types_filter`
// (aka should be skipped given the table_types_filter)
fn skip_table(table_type: &str, table_types_filter: &[String]) -> bool {
    if table_types_filter.is_empty() {
        false
    } else {
        !table_types_filter
            .iter()
            .any(|search_type| table_type == search_type)
    }
}

/// Return the schema of the RecordBatch that will be returned from
/// [`get_tables`].
///
/// Note the schema differs based on the values of `include_schema`
pub(crate) fn get_tables_schema(include_schema: bool) -> SchemaRef {
    if include_schema {
        Arc::clone(&GET_TABLES_SCHEMA_WITH_TABLE_SCHEMA)
    } else {
        Arc::clone(&GET_TABLES_SCHEMA_WITHOUT_TABLE_SCHEMA)
    }
}

/// Builds rows like this:
///
/// * catalog_name: utf8,
/// * db_schema_name: utf8,
/// * table_name: utf8 not null,
/// * table_type: utf8 not null,
/// * (optional) table_schema: bytes not null (schema of the table as described
///   in Schema.fbs::Schema it is serialized as an IPC message.)
///
/// Applies filters for as described on [`get_tables`]
struct GetTablesBuilder {
    // Optional filters to apply
    db_schema_filter_pattern: Option<String>,
    table_name_filter_pattern: Option<String>,

    catalog_name: StringBuilder,
    db_schema_name: StringBuilder,
    table_name: StringBuilder,
    table_type: StringBuilder,
    table_schema: Option<BinaryBuilder>,
}

impl GetTablesBuilder {
    fn new(
        db_schema_filter_pattern: Option<String>,
        table_name_filter_pattern: Option<String>,
        include_schema: bool,
    ) -> Self {
        let catalog_name = StringBuilder::new();
        let db_schema_name = StringBuilder::new();
        let table_name = StringBuilder::new();
        let table_type = StringBuilder::new();

        let table_schema = if include_schema {
            Some(BinaryBuilder::new())
        } else {
            None
        };

        Self {
            db_schema_filter_pattern,
            table_name_filter_pattern,

            catalog_name,
            db_schema_name,
            table_name,
            table_type,
            table_schema,
        }
    }

    /// Append a row
    fn append(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_type: &str,
        table_schema: &Schema,
    ) -> Result<()> {
        self.catalog_name.append_value(catalog_name);
        self.db_schema_name.append_value(schema_name);
        self.table_name.append_value(table_name);
        self.table_type.append_value(table_type);
        if let Some(self_table_schema) = self.table_schema.as_mut() {
            let options = IpcWriteOptions::default();
            // encode the schema into the correct form
            let message: Result<IpcMessage, ArrowError> =
                SchemaAsIpc::new(table_schema, &options).try_into();
            let IpcMessage(schema) = message?;
            self_table_schema.append_value(schema);
        }

        Ok(())
    }

    /// builds the correct schema
    fn build(self) -> Result<RecordBatch> {
        let Self {
            db_schema_filter_pattern,
            table_name_filter_pattern,

            mut catalog_name,
            mut db_schema_name,
            mut table_name,
            mut table_type,
            table_schema,
        } = self;

        // Make the arrays
        let catalog_name = catalog_name.finish();
        let db_schema_name = db_schema_name.finish();
        let table_name = table_name.finish();
        let table_type = table_type.finish();
        let table_schema = table_schema.map(|mut table_schema| table_schema.finish());

        // apply any filters, getting a BooleanArray that represents
        // the rows that passed the filter
        let mut filters = vec![];

        if let Some(db_schema_filter_pattern) = db_schema_filter_pattern {
            // use like kernel to get wildcard matching
            filters.push(arrow::compute::like_utf8_scalar(
                &db_schema_name,
                &db_schema_filter_pattern,
            )?)
        }

        if let Some(table_name_filter_pattern) = table_name_filter_pattern {
            // use like kernel to get wildcard matching
            filters.push(arrow::compute::like_utf8_scalar(
                &table_name,
                &table_name_filter_pattern,
            )?)
        }

        let batch = if let Some(table_schema) = table_schema {
            let include_schema = true;
            RecordBatch::try_new(
                get_tables_schema(include_schema),
                vec![
                    Arc::new(catalog_name) as ArrayRef,
                    Arc::new(db_schema_name) as ArrayRef,
                    Arc::new(table_name) as ArrayRef,
                    Arc::new(table_type) as ArrayRef,
                    Arc::new(table_schema) as ArrayRef,
                ],
            )
        } else {
            let include_schema = false;
            RecordBatch::try_new(
                get_tables_schema(include_schema),
                vec![
                    Arc::new(catalog_name) as ArrayRef,
                    Arc::new(db_schema_name) as ArrayRef,
                    Arc::new(table_name) as ArrayRef,
                    Arc::new(table_type) as ArrayRef,
                ],
            )
        }?;

        // `AND` any filters together
        let mut total_filter = None;
        while let Some(filter) = filters.pop() {
            let new_filter = match total_filter {
                Some(total_filter) => arrow::compute::and(&total_filter, &filter)?,
                None => filter,
            };
            total_filter = Some(new_filter);
        }

        // Apply the filters if needed
        if let Some(total_filter) = total_filter {
            Ok(filter_record_batch(&batch, &total_filter)?)
        } else {
            Ok(batch)
        }
    }
}

/// The schema for GetTables without `table_schema` column
static GET_TABLES_SCHEMA_WITHOUT_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]))
});

/// The schema for GetTables with `table_schema` column
static GET_TABLES_SCHEMA_WITH_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new("table_schema", DataType::Binary, false),
    ]))
});

fn table_type_name(table_type: TableType) -> &'static str {
    match table_type {
        // from https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L284-L288
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}
