//! FlightSQL handling
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ipc::writer::IpcWriteOptions,
};
use arrow_flight::{
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandStatementQuery,
    },
    IpcMessage, SchemaAsIpc,
};
use bytes::Bytes;
use datafusion::{logical_expr::LogicalPlan, physical_plan::ExecutionPlan, scalar::ScalarValue};
use iox_query::{exec::IOxSessionContext, QueryNamespace};
use observability_deps::tracing::debug;
use once_cell::sync::Lazy;
use prost::Message;

use crate::{error::*, sql_info::iox_sql_info_list};
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
                encode_schema(&GET_CATALOG_SCHEMA)
            }
            FlightSQLCommand::CommandGetDbSchemas(CommandGetDbSchemas { .. }) => {
                encode_schema(&GET_DB_SCHEMAS_SCHEMA)
            }
            FlightSQLCommand::CommandGetTables(CommandGetTables { .. }) => {
                encode_schema(&GET_TABLES_SCHEMA)
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
            FlightSQLCommand::CommandGetTables(CommandGetTables {
                catalog,
                db_schema_filter_pattern,
                table_name_filter_pattern,
                table_types,
                include_schema,
            }) => {
                debug!("Planning GetTables query");
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
    let schema = Schema::from(logical_plan.schema().as_ref());
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

/// Return a `LogicalPlan` for GetCatalogs
///
/// In the future this could be made more efficient by building the
/// response directly from the IOx catalog rather than running an
/// entire DataFusion plan.
async fn plan_get_catalogs(ctx: &IOxSessionContext) -> Result<LogicalPlan> {
    let query = "SELECT DISTINCT table_catalog AS catalog_name FROM information_schema.tables ORDER BY table_catalog";
    Ok(ctx.sql_to_logical_plan(query).await?)
}

/// The schema for GetCatalogs
static GET_CATALOG_SCHEMA: Lazy<Schema> =
    Lazy::new(|| Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]));

/// Return a `LogicalPlan` for GetDbSchemas
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
async fn plan_get_db_schemas(
    ctx: &IOxSessionContext,
    catalog: Option<String>,
    db_schema_filter_pattern: Option<String>,
) -> Result<LogicalPlan> {
    // use '%' to match anything if filters are not specified
    let catalog = catalog.unwrap_or_else(|| String::from("%"));
    let db_schema_filter_pattern = db_schema_filter_pattern.unwrap_or_else(|| String::from("%"));

    let query = "PREPARE my_plan(VARCHAR, VARCHAR) AS \
                 SELECT DISTINCT table_catalog AS catalog_name, table_schema AS db_schema_name \
                 FROM information_schema.tables \
                 WHERE table_catalog like $1 AND table_schema like $2  \
                 ORDER BY table_catalog, table_schema";

    let params = vec![
        ScalarValue::Utf8(Some(catalog)),
        ScalarValue::Utf8(Some(db_schema_filter_pattern)),
    ];

    let plan = ctx.sql_to_logical_plan(query).await?;
    debug!(?plan, "Prepared plan is");
    Ok(plan.with_param_values(params)?)
}

/// The schema for GetDbSchemas
static GET_DB_SCHEMAS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
    ])
});

/// Return a `LogicalPlan` for GetTables
async fn plan_get_tables(
    ctx: &IOxSessionContext,
    _catalog: Option<String>,
    _db_schema_filter_pattern: Option<String>,
    _table_name_filter_pattern: Option<String>,
    _table_types: Vec<String>,
    _include_schema: bool,
) -> Result<LogicalPlan> {
    let query = "SELECT table_catalog AS catalog_name, \
        table_schema AS db_schema_name, table_name, table_type \
        FROM information_schema.tables \
        ORDER BY table_catalog, table_schema, table_name";

    Ok(ctx.sql_to_logical_plan(query).await?)
}

/// The schema for GetTables
static GET_TABLES_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ])
});

/// Return a `LogicalPlan` for GetTableTypes
///
/// In the future this could be made more efficient by building the
/// response directly from the IOx catalog rather than running an
/// entire DataFusion plan.
async fn plan_get_table_types(ctx: &IOxSessionContext) -> Result<LogicalPlan> {
    let query = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";
    Ok(ctx.sql_to_logical_plan(query).await?)
}

/// The schema for GetTableTypes
static GET_TABLE_TYPE_SCHEMA: Lazy<Schema> =
    Lazy::new(|| Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]));
