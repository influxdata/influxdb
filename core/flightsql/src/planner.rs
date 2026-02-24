//! FlightSQL handling
use std::sync::{Arc, LazyLock};

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{
    FlightData, IpcMessage, SchemaAsIpc,
    decode::FlightRecordBatchStream,
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
        CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandGetXdbcTypeInfo, CommandStatementQuery,
        DoPutPreparedStatementResult,
    },
};
use arrow_util::flight::prepare_schema_for_flight;
use bytes::Bytes;
use datafusion::{
    logical_expr::{LogicalPlan, TableType},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use futures::{TryStreamExt, stream::Peekable};
use generated_types::Streaming;
use iox_query::{QueryNamespace, exec::IOxSessionContext};
use prost::Message;
use snafu::OptionExt;
use tracing::debug;

use crate::{
    BaseTableType, Error, error::*, sql_info::iox_sql_info_data,
    xdbc_type_info::xdbc_type_info_data,
};
use crate::{FlightSQLCommand, PreparedStatementHandle};

/// Logic for creating plans for various Flight messages against a query database
#[derive(Debug, Default, Copy, Clone)]
pub struct FlightSQLPlanner {}

impl FlightSQLPlanner {
    pub fn new() -> Self {
        Self {}
    }

    /// Returns the schema for the request in msg.
    pub async fn get_schema(
        namespace_name: impl Into<String> + Send,
        // We can take this by-ref cause it (mostly) doesn't actually need to consume any of the data in it.
        // There are some `clone`s in the body of this function, but the function 2/3 end up
        // resolving to and calling doesn't actually use the `self` reference, so a Sufficiently
        // Smart Compiler (tm) should optimize away those clones. Ideally. So no consuming except
        // for one instance, and imo cloning preemptively is not worth it.
        cmd: &FlightSQLCommand,
        ctx: &IOxSessionContext,
    ) -> Result<SchemaRef> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, %cmd, "Handling flightsql get_flight_info (get schema)");

        match cmd {
            FlightSQLCommand::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                get_schema_for_query(query, ctx).await
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                get_schema_for_query(handle.query(), ctx).await
            }
            FlightSQLCommand::CommandGetSqlInfo(CommandGetSqlInfo { .. }) => {
                Ok(iox_sql_info_data().schema())
            }
            FlightSQLCommand::CommandGetCatalogs(req) => Ok(req.into_builder().schema()),
            FlightSQLCommand::CommandGetCrossReference(CommandGetCrossReference { .. }) => {
                Ok(Arc::clone(&GET_CROSS_REFERENCE_SCHEMA))
            }
            FlightSQLCommand::CommandGetDbSchemas(req) => Ok(req.clone().into_builder().schema()),
            FlightSQLCommand::CommandGetExportedKeys(CommandGetExportedKeys { .. }) => {
                Ok(Arc::clone(&GET_EXPORTED_KEYS_SCHEMA))
            }
            FlightSQLCommand::CommandGetImportedKeys(CommandGetImportedKeys { .. }) => {
                Ok(Arc::clone(&GET_IMPORTED_KEYS_SCHEMA))
            }
            FlightSQLCommand::CommandGetPrimaryKeys(CommandGetPrimaryKeys { .. }) => {
                Ok(Arc::clone(&GET_PRIMARY_KEYS_SCHEMA))
            }
            FlightSQLCommand::CommandGetTables(req) => Ok(req.clone().into_builder().schema()),
            FlightSQLCommand::CommandGetTableTypes(CommandGetTableTypes { .. }) => {
                Ok(Arc::clone(&GET_TABLE_TYPE_SCHEMA))
            }
            FlightSQLCommand::CommandGetXdbcTypeInfo(CommandGetXdbcTypeInfo { .. }) => {
                Ok(Arc::clone(&GET_XDBC_TYPE_INFO_SCHEMA))
            }
            FlightSQLCommand::ActionCreatePreparedStatementRequest(_)
            | FlightSQLCommand::ActionClosePreparedStatementRequest(_) => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "GetFlightInfo",
            }
            .fail(),
        }
    }

    /// Returns a plan that computes results requested in msg
    pub async fn do_get(
        namespace_name: impl AsRef<str> + Send,
        _database: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        ctx: &IOxSessionContext,
        base_table_type: BaseTableType,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let namespace_name = namespace_name.as_ref();
        debug!(%namespace_name, %cmd, "Handling flightsql do_get");

        match cmd {
            FlightSQLCommand::CommandStatementQuery(CommandStatementQuery { query, .. }) => {
                debug!(%query, "Planning FlightSQL query");
                Ok(ctx.sql_to_physical_plan(&query).await?)
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                debug!(%handle, "Planning FlightSQL prepared query");
                Ok(ctx
                    .sql_to_physical_plan_with_params(handle.query(), handle.to_df_param_values()?)
                    .await?)
            }
            FlightSQLCommand::CommandGetSqlInfo(cmd) => {
                debug!(?cmd, "Planning GetSqlInfo query");
                let plan = plan_get_sql_info(ctx, cmd).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetCatalogs(cmd) => {
                debug!("Planning GetCatalogs query");
                let plan = plan_get_catalogs(ctx, cmd).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetCrossReference(CommandGetCrossReference {
                pk_catalog,
                pk_db_schema,
                pk_table,
                fk_catalog,
                fk_db_schema,
                fk_table,
            }) => {
                debug!(
                    ?pk_catalog,
                    ?pk_db_schema,
                    ?pk_table,
                    ?fk_catalog,
                    ?fk_db_schema,
                    ?fk_table,
                    "Planning CommandGetCrossReference query"
                );
                let plan = plan_get_cross_reference(
                    ctx,
                    pk_catalog,
                    pk_db_schema,
                    pk_table,
                    fk_catalog,
                    fk_db_schema,
                    fk_table,
                )
                .await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetDbSchemas(cmd) => {
                debug!(
                    catalog=?cmd.catalog,
                    db_schema_filter_pattern=?cmd.db_schema_filter_pattern,
                    "Planning GetDbSchemas query"
                );
                let plan = plan_get_db_schemas(ctx, cmd).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetExportedKeys(CommandGetExportedKeys {
                catalog,
                db_schema,
                table,
            }) => {
                debug!(
                    ?catalog,
                    ?db_schema,
                    ?table,
                    "Planning GetExportedKeys query"
                );
                let plan = plan_get_exported_keys(ctx, catalog, db_schema, table).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetImportedKeys(CommandGetImportedKeys {
                catalog,
                db_schema,
                table,
            }) => {
                debug!(
                    ?catalog,
                    ?db_schema,
                    ?table,
                    "Planning CommandGetImportedKeys query"
                );
                let plan = plan_get_imported_keys(ctx, catalog, db_schema, table).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetPrimaryKeys(CommandGetPrimaryKeys {
                catalog,
                db_schema,
                table,
            }) => {
                debug!(
                    ?catalog,
                    ?db_schema,
                    ?table,
                    "Planning GetPrimaryKeys query"
                );
                let plan = plan_get_primary_keys(ctx, catalog, db_schema, table).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetTables(cmd) => {
                debug!(
                    catalog=?cmd.catalog,
                    db_schema_filter_pattern=?cmd.db_schema_filter_pattern,
                    table_name_filter_pattern=?cmd.table_name_filter_pattern,
                    table_types=?cmd.table_types,
                    include_schema=?cmd.include_schema,
                    "Planning GetTables query"
                );
                let plan = plan_get_tables(ctx, cmd, base_table_type).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetTableTypes(CommandGetTableTypes {}) => {
                debug!("Planning GetTableTypes query");
                let plan = plan_get_table_types(ctx, base_table_type).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetXdbcTypeInfo(cmd) => {
                debug!(?cmd, "Planning GetXdbcTypeInfo query");
                let plan = plan_get_xdbc_type_info(ctx, cmd).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::ActionClosePreparedStatementRequest(_)
            | FlightSQLCommand::ActionCreatePreparedStatementRequest(_) => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "DoGet",
            }
            .fail(),
        }
    }

    /// Handles the action specified in `msg` and returns bytes for
    /// the [`arrow_flight::Result`] (not the same as a rust
    /// [`Result`]!)
    pub async fn do_action(
        namespace_name: impl Into<String> + Send,
        _database: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        ctx: &IOxSessionContext,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, %cmd, "Handling flightsql do_action");

        match cmd {
            FlightSQLCommand::ActionCreatePreparedStatementRequest(
                ActionCreatePreparedStatementRequest { query, .. },
            ) => {
                debug!(%query, "Creating prepared statement");

                let (dataset_schema, parameter_schema) =
                    get_encoded_schemas_for_prepared_statement(&query, ctx).await?;
                let handle = PreparedStatementHandle::new(query);

                let result = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: Bytes::try_from(handle)?,
                    dataset_schema,
                    parameter_schema,
                };

                let msg = Any::pack(&result)?;
                Ok(msg.encode_to_vec().into())
            }
            FlightSQLCommand::ActionClosePreparedStatementRequest(handle) => {
                let query = handle.query();
                debug!(%query, "Closing prepared statement");

                // Nothing really to do
                Ok(Bytes::new())
            }
            _ => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "DoAction",
            }
            .fail(),
        }
    }

    /// Handles receiving the given `data` from the client based on the action
    /// specified by `cmd` and returns bytes for the [`arrow_flight::Result`]
    /// (not the same as a rust [`Result`]!)
    pub async fn do_put(
        namespace_name: impl Into<String> + Send,
        _database: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        data: Peekable<Streaming<FlightData>>,
        _ctx: &IOxSessionContext,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, %cmd, "Handling flightsql do_put");
        match cmd {
            FlightSQLCommand::CommandPreparedStatementQuery(mut prepared_statement_handle) => {
                debug!(
                    %prepared_statement_handle,
                    "Adding parameters to prepared statement handle"
                );
                // convert FlightData stream into RecordBatches
                let params_batch =
                    FlightRecordBatchStream::new_from_flight_data(data.map_err(|e| e.into()))
                        .try_next()
                        .await?
                        .with_context(|| NoFlightDataSnafu {
                            cmd: format!(
                                "{}",
                                FlightSQLCommand::CommandPreparedStatementQuery(
                                    prepared_statement_handle.clone()
                                )
                            ),
                            method: "DoPut",
                        })?;
                prepared_statement_handle.params = Some(params_batch);
                let result = DoPutPreparedStatementResult {
                    prepared_statement_handle: Some(prepared_statement_handle.try_into()?),
                };
                Ok(result.encode_to_vec().into())
            }
            _ => ProtocolSnafu {
                cmd: format!("{cmd:?}"),
                method: "DoPut",
            }
            .fail(),
        }
    }

    /// Return the schema for the specified logical plan
    pub fn get_schema_for_plan(logical_plan: &LogicalPlan) -> SchemaRef {
        // gather real schema, but only
        let schema = logical_plan.schema().as_ref();
        prepare_schema_for_flight(schema.as_arrow())
    }
}

/// Return the IPC encoded dataset and parameter schema for the specified query
async fn get_encoded_schemas_for_prepared_statement(
    query: &str,
    ctx: &IOxSessionContext,
) -> Result<(Bytes, Bytes)> {
    let plan = ctx.sql_to_logical_plan(query).await?;
    let dataset_schema = FlightSQLPlanner::get_schema_for_plan(&plan);
    let parameter_schema = get_parameter_schema_for_plan(&plan)?;
    Ok((
        encode_schema(&dataset_schema)?,
        encode_schema(&parameter_schema)?,
    ))
}

/// Return the schema for the specified query
async fn get_schema_for_query(query: &str, ctx: &IOxSessionContext) -> Result<SchemaRef> {
    let plan = ctx.sql_to_logical_plan(query).await?;
    Ok(FlightSQLPlanner::get_schema_for_plan(&plan))
}

/// Return the schema for any bind parameters within the specified logical plan
fn get_parameter_schema_for_plan(logical_plan: &LogicalPlan) -> Result<SchemaRef> {
    let parameter_schema = parameter_types_to_schema(logical_plan.get_parameter_types()?)?;
    Ok(prepare_schema_for_flight(&parameter_schema))
}

/// Return the parameter schema for the specified map of parameters.
/// Map can be produced from [LogicalPlan::get_parameter_types]
fn parameter_types_to_schema(
    parameter_types: impl IntoIterator<Item = (String, Option<DataType>)>,
) -> Result<Schema> {
    Ok(Schema::new(
        parameter_types
            .into_iter()
            .map(|(id, data_type)| {
                let data_type = data_type.ok_or_else(|| Error::UnknownParameterType {
                    parameter_name: id.clone(),
                })?;
                Ok(Field::new(id, data_type, true))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

/// Encodes the schema IPC encoded (schema_bytes)
fn encode_schema(schema: &Schema) -> Result<Bytes> {
    let options = IpcWriteOptions::default();

    // encode the schema into the correct form
    let message: Result<IpcMessage, ArrowError> = SchemaAsIpc::new(schema, &options).try_into();

    let IpcMessage(schema) = message?;

    Ok(schema)
}

/// Return a `LogicalPlan` for GetSqlInfo
async fn plan_get_sql_info(ctx: &IOxSessionContext, cmd: CommandGetSqlInfo) -> Result<LogicalPlan> {
    let batch = cmd.into_builder(iox_sql_info_data()).build()?;
    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// Return a list of "catalogs" from the DataFusion catalog
async fn plan_get_catalogs(
    ctx: &IOxSessionContext,
    cmd: CommandGetCatalogs,
) -> Result<LogicalPlan> {
    let mut builder = cmd.into_builder();
    for catalog_name in ctx.inner().catalog_names() {
        builder.append(catalog_name);
    }
    let batch = builder.build()?;
    Ok(ctx.batch_to_logical_plan(batch)?)
}

async fn plan_get_cross_reference(
    ctx: &IOxSessionContext,
    _pk_catalog: Option<String>,
    _pk_db_schema: Option<String>,
    _pk_table: String,
    _fk_catalog: Option<String>,
    _fk_db_schema: Option<String>,
    _fk_table: String,
) -> Result<LogicalPlan> {
    let batch = RecordBatch::new_empty(Arc::clone(&GET_CROSS_REFERENCE_SCHEMA));
    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// Return a list of schema from the DataFusion catalog
async fn plan_get_db_schemas(
    ctx: &IOxSessionContext,
    cmd: CommandGetDbSchemas,
) -> Result<LogicalPlan> {
    let mut builder = cmd.into_builder();
    let catalog_list = Arc::clone(ctx.inner().state().catalog_list());

    for catalog_name in catalog_list.catalog_names() {
        // we just got the catalog name from the catalog_list, so it
        // should always be Some, but avoid unwrap to be safe
        let Some(catalog) = catalog_list.catalog(&catalog_name) else {
            continue;
        };

        builder.append(&catalog_name, "information_schema");
        for schema_name in catalog.schema_names() {
            builder.append(&catalog_name, &schema_name);
        }
    }

    let batch = builder.build()?;
    Ok(ctx.batch_to_logical_plan(batch)?)
}

async fn plan_get_exported_keys(
    ctx: &IOxSessionContext,
    _catalog: Option<String>,
    _db_schema: Option<String>,
    _table: String,
) -> Result<LogicalPlan> {
    let batch = RecordBatch::new_empty(Arc::clone(&GET_EXPORTED_KEYS_SCHEMA));
    Ok(ctx.batch_to_logical_plan(batch)?)
}

async fn plan_get_imported_keys(
    ctx: &IOxSessionContext,
    _catalog: Option<String>,
    _db_schema: Option<String>,
    _table: String,
) -> Result<LogicalPlan> {
    let batch = RecordBatch::new_empty(Arc::clone(&GET_IMPORTED_KEYS_SCHEMA));
    Ok(ctx.batch_to_logical_plan(batch)?)
}

async fn plan_get_primary_keys(
    ctx: &IOxSessionContext,
    _catalog: Option<String>,
    _db_schema: Option<String>,
    _table: String,
) -> Result<LogicalPlan> {
    let batch = RecordBatch::new_empty(Arc::clone(&GET_PRIMARY_KEYS_SCHEMA));
    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// Return a list of tables from the DataFusion catalog
async fn plan_get_tables(
    ctx: &IOxSessionContext,
    cmd: CommandGetTables,
    base_table_type: BaseTableType,
) -> Result<LogicalPlan> {
    let mut builder = cmd.into_builder();
    let catalog_list = Arc::clone(ctx.inner().state().catalog_list());

    for catalog_name in catalog_list.catalog_names() {
        // we just got the catalog name from the catalog_list, so it
        // should always be Some, but avoid unwrap to be safe
        let Some(catalog) = catalog_list.catalog(&catalog_name) else {
            continue;
        };

        // special case the "public"."information_schema" as it is a
        // "virtual" catalog in DataFusion and thus is not reported
        // directly via the table providers
        // We ensure this list is kept in sync with tests
        let table_names = vec![
            "columns",
            "df_settings",
            "parameters",
            "routines",
            "tables",
            "views",
            "schemata",
        ];
        for table_name in table_names {
            let schema_name = "information_schema";
            let table_ref = TableReference::full(catalog_name.clone(), schema_name, table_name);

            let Some(table) = ctx.inner().table(table_ref).await.ok() else {
                continue;
            };

            let table_type = "VIEW";
            let schema = Schema::from(table.schema());
            builder.append(&catalog_name, schema_name, table_name, table_type, &schema)?;
        }

        for schema_name in catalog.schema_names() {
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };

            for table_name in schema.table_names() {
                let Some(table) = schema.table(&table_name).await? else {
                    continue;
                };

                let table_type = table_type_name(table.table_type(), base_table_type);

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
    let batch = builder.build()?;

    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// Return the correct FlightSQL name for the DataFusion TableType
fn table_type_name(table_type: TableType, base_table_type: BaseTableType) -> &'static str {
    match table_type {
        // from https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L284-L288
        TableType::Base => base_table_type.as_str(),
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

/// Return a `LogicalPlan` for GetTableTypes
async fn plan_get_table_types(
    ctx: &IOxSessionContext,
    base_table_type: BaseTableType,
) -> Result<LogicalPlan> {
    // https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L285-L287
    // IOx doesn't support LOCAL TEMPORARY yet
    let table_types = match base_table_type {
        BaseTableType::BaseTable => vec!["BASE TABLE", "VIEW"],
        BaseTableType::Table => vec!["TABLE", "VIEW"],
    };

    let table_type = Arc::new(StringArray::from_iter_values(table_types)) as ArrayRef;
    let batch = RecordBatch::try_new(Arc::clone(&GET_TABLE_TYPE_SCHEMA), vec![table_type])
        .map_err(|e| Error::Arrow { source: e })?;

    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// Return a `LogicalPlan` for GetXdbcTypeInfo
async fn plan_get_xdbc_type_info(
    ctx: &IOxSessionContext,
    cmd: CommandGetXdbcTypeInfo,
) -> Result<LogicalPlan> {
    let batch = cmd.into_builder(xdbc_type_info_data()).build()?;
    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// The schema for GetTableTypes
static GET_TABLE_TYPE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

/// The returned data should be ordered by pk_catalog_name, pk_db_schema_name,
/// pk_table_name, pk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions:
///    - 0 = CASCADE
///    - 1 = RESTRICT
///    - 2 = SET NULL
///    - 3 = NO ACTION
///    - 4 = SET DEFAULT
static GET_CROSS_REFERENCE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("pk_catalog_name", DataType::Utf8, false),
        Field::new("pk_db_schema_name", DataType::Utf8, false),
        Field::new("pk_table_name", DataType::Utf8, false),
        Field::new("pk_column_name", DataType::Utf8, false),
        Field::new("fk_catalog_name", DataType::Utf8, false),
        Field::new("fk_db_schema_name", DataType::Utf8, false),
        Field::new("fk_table_name", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
        Field::new("fk_key_name", DataType::Utf8, false),
        Field::new("pk_key_name", DataType::Utf8, false),
        Field::new("update_rule", DataType::UInt8, false),
        Field::new("delete_rule", DataType::UInt8, false),
    ]))
});

static GET_EXPORTED_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("pk_catalog_name", DataType::Utf8, false),
        Field::new("pk_db_schema_name", DataType::Utf8, false),
        Field::new("pk_table_name", DataType::Utf8, false),
        Field::new("pk_column_name", DataType::Utf8, false),
        Field::new("fk_catalog_name", DataType::Utf8, false),
        Field::new("fk_db_schema_name", DataType::Utf8, false),
        Field::new("fk_table_name", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
        Field::new("fk_key_name", DataType::Utf8, false),
        Field::new("pk_key_name", DataType::Utf8, false),
        Field::new("update_rule", DataType::UInt8, false),
        Field::new("delete_rule", DataType::UInt8, false),
    ]))
});

static GET_IMPORTED_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("pk_catalog_name", DataType::Utf8, false),
        Field::new("pk_db_schema_name", DataType::Utf8, false),
        Field::new("pk_table_name", DataType::Utf8, false),
        Field::new("pk_column_name", DataType::Utf8, false),
        Field::new("fk_catalog_name", DataType::Utf8, false),
        Field::new("fk_db_schema_name", DataType::Utf8, false),
        Field::new("fk_table_name", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
        Field::new("fk_key_name", DataType::Utf8, false),
        Field::new("pk_key_name", DataType::Utf8, false),
        Field::new("update_rule", DataType::UInt8, false),
        Field::new("delete_rule", DataType::UInt8, false),
    ]))
});

static GET_PRIMARY_KEYS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
    ]))
});

/// The schema for GetXdbcTypeInfo
// From https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1064-L1113
static GET_XDBC_TYPE_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("type_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Int32, false),
        Field::new("column_size", DataType::Int32, true),
        Field::new("literal_prefix", DataType::Utf8, true),
        Field::new("literal_suffix", DataType::Utf8, true),
        Field::new(
            "create_params",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
        Field::new("nullable", DataType::Int32, false), // Nullable enum: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1014-L1029
        Field::new("case_sensitive", DataType::Boolean, false),
        Field::new("searchable", DataType::Int32, false), // Searchable enum: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1031-L1056
        Field::new("unsigned_attribute", DataType::Boolean, true),
        Field::new("fixed_prec_scale", DataType::Boolean, false),
        Field::new("auto_increment", DataType::Boolean, true),
        Field::new("local_type_name", DataType::Utf8, true),
        Field::new("minimum_scale", DataType::Int32, true),
        Field::new("maximum_scale", DataType::Int32, true),
        Field::new("sql_data_type", DataType::Int32, false),
        Field::new("datetime_subcode", DataType::Int32, true),
        Field::new("num_prec_radix", DataType::Int32, true),
        Field::new("interval_precision", DataType::Int32, true),
    ]))
});

#[cfg(test)]
mod test {

    use arrow::datatypes::{DataType, Field, Schema};

    use super::parameter_types_to_schema;

    #[test]
    fn test_prepared_statement_parameter_schema() {
        let types = [
            ("param1".to_owned(), Some(DataType::Null)),
            ("param2".to_owned(), Some(DataType::Utf8)),
            (
                "param3".to_owned(),
                Some(DataType::Dictionary(
                    Box::new(DataType::UInt64),
                    Box::new(DataType::Utf8),
                )),
            ),
        ];
        let schema = parameter_types_to_schema(types).unwrap();
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("param1", DataType::Null, true),
                Field::new("param2", DataType::Utf8, true),
                Field::new(
                    "param3".to_owned(),
                    DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
                    true
                ),
            ])
        );
    }
}
