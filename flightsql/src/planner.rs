//! FlightSQL handling
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetPrimaryKeys,
        CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandStatementQuery,
    },
    IpcMessage, SchemaAsIpc,
};
use arrow_util::flight::prepare_schema_for_flight;
use bytes::Bytes;
use datafusion::{logical_expr::LogicalPlan, physical_plan::ExecutionPlan};
use iox_query::{exec::IOxSessionContext, QueryNamespace};
use observability_deps::tracing::debug;
use once_cell::sync::Lazy;
use prost::Message;

use crate::{
    error::*,
    get_catalogs::{get_catalogs, get_catalogs_schema},
    get_db_schemas::{get_db_schemas, get_db_schemas_schema},
    get_tables::{get_tables, get_tables_schema},
    sql_info::iox_sql_info_list,
};
use crate::{FlightSQLCommand, PreparedStatementHandle};

/// Logic for creating plans for various Flight messages against a query database
#[derive(Debug, Default)]
pub struct FlightSQLPlanner {}

impl FlightSQLPlanner {
    pub fn new() -> Self {
        Self {}
    }

    /// Returns the schema, in Arrow IPC encoded form, for the request in msg.
    pub async fn get_flight_info(
        namespace_name: impl Into<String>,
        cmd: FlightSQLCommand,
        ctx: &IOxSessionContext,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, %cmd, "Handling flightsql get_flight_info");

        match cmd {
            FlightSQLCommand::CommandStatementQuery(CommandStatementQuery { query }) => {
                get_schema_for_query(&query, ctx).await
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                get_schema_for_query(handle.query(), ctx).await
            }
            FlightSQLCommand::CommandGetSqlInfo(CommandGetSqlInfo { .. }) => {
                encode_schema(iox_sql_info_list().schema())
            }
            FlightSQLCommand::CommandGetCatalogs(CommandGetCatalogs {}) => {
                encode_schema(get_catalogs_schema())
            }
            FlightSQLCommand::CommandGetDbSchemas(CommandGetDbSchemas { .. }) => {
                encode_schema(get_db_schemas_schema().as_ref())
            }
            FlightSQLCommand::CommandGetExportedKeys(CommandGetExportedKeys { .. }) => {
                encode_schema(&GET_EXPORTED_KEYS_SCHEMA)
            }
            FlightSQLCommand::CommandGetPrimaryKeys(CommandGetPrimaryKeys { .. }) => {
                encode_schema(&GET_PRIMARY_KEYS_SCHEMA)
            }
            FlightSQLCommand::CommandGetTables(CommandGetTables { include_schema, .. }) => {
                encode_schema(get_tables_schema(include_schema).as_ref())
            }
            FlightSQLCommand::CommandGetTableTypes(CommandGetTableTypes { .. }) => {
                encode_schema(&GET_TABLE_TYPE_SCHEMA)
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
        namespace_name: impl Into<String>,
        _database: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, %cmd, "Handling flightsql do_get");

        match cmd {
            FlightSQLCommand::CommandStatementQuery(CommandStatementQuery { query }) => {
                debug!(%query, "Planning FlightSQL query");
                Ok(ctx.sql_to_physical_plan(&query).await?)
            }
            FlightSQLCommand::CommandPreparedStatementQuery(handle) => {
                let query = handle.query();
                debug!(%query, "Planning FlightSQL prepared query");
                Ok(ctx.sql_to_physical_plan(query).await?)
            }
            FlightSQLCommand::CommandGetSqlInfo(CommandGetSqlInfo { info }) => {
                debug!("Planning GetSqlInfo query");
                let plan = plan_get_sql_info(ctx, info).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetCatalogs(CommandGetCatalogs {}) => {
                debug!("Planning GetCatalogs query");
                let plan = plan_get_catalogs(ctx).await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetDbSchemas(CommandGetDbSchemas {
                catalog,
                db_schema_filter_pattern,
            }) => {
                debug!(
                    ?catalog,
                    ?db_schema_filter_pattern,
                    "Planning GetDbSchemas query"
                );
                let plan = plan_get_db_schemas(ctx, catalog, db_schema_filter_pattern).await?;
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
            FlightSQLCommand::CommandGetTables(CommandGetTables {
                catalog,
                db_schema_filter_pattern,
                table_name_filter_pattern,
                table_types,
                include_schema,
            }) => {
                debug!(
                    ?catalog,
                    ?db_schema_filter_pattern,
                    ?table_name_filter_pattern,
                    ?table_types,
                    ?include_schema,
                    "Planning GetTables query"
                );
                let plan = plan_get_tables(
                    ctx,
                    catalog,
                    db_schema_filter_pattern,
                    table_name_filter_pattern,
                    table_types,
                    include_schema,
                )
                .await?;
                Ok(ctx.create_physical_plan(&plan).await?)
            }
            FlightSQLCommand::CommandGetTableTypes(CommandGetTableTypes {}) => {
                debug!("Planning GetTableTypes query");
                let plan = plan_get_table_types(ctx).await?;
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
        namespace_name: impl Into<String>,
        _database: Arc<dyn QueryNamespace>,
        cmd: FlightSQLCommand,
        ctx: &IOxSessionContext,
    ) -> Result<Bytes> {
        let namespace_name = namespace_name.into();
        debug!(%namespace_name, %cmd, "Handling flightsql do_action");

        match cmd {
            FlightSQLCommand::ActionCreatePreparedStatementRequest(
                ActionCreatePreparedStatementRequest { query },
            ) => {
                debug!(%query, "Creating prepared statement");

                // todo run the planner here and actually figure out parameter schemas
                // see https://github.com/apache/arrow-datafusion/pull/4701
                let parameter_schema = vec![];

                let dataset_schema = get_schema_for_query(&query, ctx).await?;
                let handle = PreparedStatementHandle::new(query);

                let result = ActionCreatePreparedStatementResult {
                    prepared_statement_handle: Bytes::from(handle),
                    dataset_schema,
                    parameter_schema: Bytes::from(parameter_schema),
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
}

/// Return the schema for the specified query
///
/// returns: IPC encoded (schema_bytes) for this query
async fn get_schema_for_query(query: &str, ctx: &IOxSessionContext) -> Result<Bytes> {
    get_schema_for_plan(ctx.sql_to_logical_plan(query).await?)
}

/// Return the schema for the specified logical plan
///
/// returns: IPC encoded (schema_bytes) for this query
fn get_schema_for_plan(logical_plan: LogicalPlan) -> Result<Bytes> {
    // gather real schema, but only
    let schema = Arc::new(Schema::from(logical_plan.schema().as_ref())) as _;
    let schema = prepare_schema_for_flight(schema);
    encode_schema(&schema)
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
///
/// The infos are passed directly from the [`CommandGetSqlInfo::info`]
async fn plan_get_sql_info(ctx: &IOxSessionContext, info: Vec<u32>) -> Result<LogicalPlan> {
    let batch = iox_sql_info_list().filter(&info).encode()?;
    Ok(ctx.batch_to_logical_plan(batch)?)
}

async fn plan_get_catalogs(ctx: &IOxSessionContext) -> Result<LogicalPlan> {
    Ok(ctx.batch_to_logical_plan(get_catalogs(ctx.inner())?)?)
}

async fn plan_get_db_schemas(
    ctx: &IOxSessionContext,
    catalog: Option<String>,
    db_schema_filter_pattern: Option<String>,
) -> Result<LogicalPlan> {
    let batch = get_db_schemas(ctx.inner(), catalog, db_schema_filter_pattern)?;
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

async fn plan_get_primary_keys(
    ctx: &IOxSessionContext,
    _catalog: Option<String>,
    _db_schema: Option<String>,
    _table: String,
) -> Result<LogicalPlan> {
    let batch = RecordBatch::new_empty(Arc::clone(&GET_PRIMARY_KEYS_SCHEMA));
    Ok(ctx.batch_to_logical_plan(batch)?)
}

async fn plan_get_tables(
    ctx: &IOxSessionContext,
    catalog: Option<String>,
    db_schema_filter_pattern: Option<String>,
    table_name_filter_pattern: Option<String>,
    table_types: Vec<String>,
    include_schema: bool,
) -> Result<LogicalPlan> {
    let batch = get_tables(
        ctx.inner(),
        catalog,
        db_schema_filter_pattern,
        table_name_filter_pattern,
        table_types,
        include_schema,
    )
    .await?;

    Ok(ctx.batch_to_logical_plan(batch)?)
}

/// Return a `LogicalPlan` for GetTableTypes
async fn plan_get_table_types(ctx: &IOxSessionContext) -> Result<LogicalPlan> {
    Ok(ctx.batch_to_logical_plan(TABLE_TYPES_RECORD_BATCH.clone())?)
}

/// The schema for GetTableTypes
static GET_TABLE_TYPE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

static TABLE_TYPES_RECORD_BATCH: Lazy<RecordBatch> = Lazy::new(|| {
    // https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L285-L287
    // IOx doesn't support LOCAL TEMPORARY yet
    let table_type = Arc::new(StringArray::from_iter_values(["BASE TABLE", "VIEW"])) as ArrayRef;
    RecordBatch::try_new(Arc::clone(&GET_TABLE_TYPE_SCHEMA), vec![table_type]).unwrap()
});

static GET_EXPORTED_KEYS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
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
        // According to the definition in https://github.com/apache/arrow/blob/0434ab65075ecd1d2ab9245bcd7ec6038934ed29/format/FlightSql.proto#L1327-L1328
        // update_rule and delete_rule are in type uint1
        // However, Rust DataType does not have this type,
        // the closet is DataType::UInt8
        Field::new("update_rule", DataType::UInt8, false),
        Field::new("delete_rule", DataType::UInt8, false),
    ]))
});

static GET_PRIMARY_KEYS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
    ]))
});
