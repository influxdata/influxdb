//! Represents the response to FlightSQL `GetSqlInfo` requests and
//! handles the conversion to/from the format specified in the
//! [Arrow FlightSQL Specification].
//!
//! <
//!   info_name: uint32 not null,
//!   value: dense_union<
//!               string_value: utf8,
//!               bool_value: bool,
//!               bigint_value: int64,
//!               int32_bitmask: int32,
//!               string_list: list<string_data: utf8>
//!               int32_to_int32_list_map: map<key: int32, value: list<$data$: int32>>
//!  >
//!
//! where there is one row per requested piece of metadata information.
//!
//!
//! [Arrow FlightSQL Specification]: https://github.com/apache/arrow/blob/f1eece9f276184063c9c35011e8243eb3b071233/format/FlightSql.proto#L33-L42

mod meta;

use arrow_flight::sql::{
    metadata::{SqlInfoData, SqlInfoDataBuilder},
    SqlInfo, SqlNullOrdering, SqlSupportedCaseSensitivity, SqlSupportedTransactions,
    SupportedSqlGrammar,
};
use once_cell::sync::Lazy;

use meta::{
    SQL_INFO_DATE_TIME_FUNCTIONS, SQL_INFO_NUMERIC_FUNCTIONS, SQL_INFO_SQL_KEYWORDS,
    SQL_INFO_STRING_FUNCTIONS, SQL_INFO_SYSTEM_FUNCTIONS,
};

#[allow(non_snake_case)]
static INSTANCE: Lazy<SqlInfoData> = Lazy::new(|| {
    // The following are not defined in the [`SqlInfo`], but are
    // documented at
    // https://arrow.apache.org/docs/format/FlightSql.html#protocol-buffer-definitions.

    let SqlInfoFlightSqlServerSql = 4;
    let SqlInfoFlightSqlServerSubstrait = 5;
    //let SqlInfoFlightSqlServerSubstraitMinVersion = 6;
    //let SqlInfoFlightSqlServerSubstraitMaxVersion = 7;
    let SqlInfoFlightSqlServerTransaction = 8;
    let SqlInfoFlightSqlServerCancel = 9;
    let SqlInfoFlightSqlServerStatementTimeout = 100;
    let SqlInfoFlightSqlServerTransactionTimeout = 101;

    // Copied from https://github.com/influxdata/idpe/blob/85aa7a52b40f173cc4d79ac02b3a4a13e82333c4/queryrouter/internal/server/flightsql_handler.go#L208-L275

    let mut builder = SqlInfoDataBuilder::new();

    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "InfluxDB IOx");
    builder.append(SqlInfo::FlightSqlServerVersion, "2");
    // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfoFlightSqlServerSql, true);
    builder.append(SqlInfoFlightSqlServerSubstrait, false);
    builder.append(
        SqlInfoFlightSqlServerTransaction,
        SqlSupportedTransactions::SqlTransactionUnspecified as i32,
    );
    // don't yetsupport `CancelQuery` action
    builder.append(SqlInfoFlightSqlServerCancel, false);
    builder.append(SqlInfoFlightSqlServerStatementTimeout, 0i32);
    builder.append(SqlInfoFlightSqlServerTransactionTimeout, 0i32);
    // SQL syntax information
    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, false);
    builder.append(SqlInfo::SqlDdlTable, false);
    builder.append(
        SqlInfo::SqlIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase as i32,
    );
    builder.append(SqlInfo::SqlIdentifierQuoteChar, r#"""#);
    builder.append(
        SqlInfo::SqlQuotedIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityCaseInsensitive as i32,
    );
    builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
    builder.append(
        SqlInfo::SqlNullOrdering,
        SqlNullOrdering::SqlNullsSortedHigh as i32,
    );
    builder.append(SqlInfo::SqlKeywords, SQL_INFO_SQL_KEYWORDS);
    builder.append(SqlInfo::SqlNumericFunctions, SQL_INFO_NUMERIC_FUNCTIONS);
    builder.append(SqlInfo::SqlStringFunctions, SQL_INFO_STRING_FUNCTIONS);
    builder.append(SqlInfo::SqlSystemFunctions, SQL_INFO_SYSTEM_FUNCTIONS);
    builder.append(SqlInfo::SqlDatetimeFunctions, SQL_INFO_DATE_TIME_FUNCTIONS);
    builder.append(SqlInfo::SqlSearchStringEscape, "\\");
    builder.append(SqlInfo::SqlExtraNameCharacters, "");
    builder.append(SqlInfo::SqlSupportsColumnAliasing, true);
    builder.append(SqlInfo::SqlNullPlusNullIsNull, true);
    // Skip SqlSupportsConvert (which is the map of the conversions that are supported);
    // .with_sql_info(SqlInfo::SqlSupportsConvert, TBD);
    // https://github.com/influxdata/influxdb_iox/issues/7253
    builder.append(SqlInfo::SqlSupportsTableCorrelationNames, false);
    builder.append(SqlInfo::SqlSupportsDifferentTableCorrelationNames, false);
    builder.append(SqlInfo::SqlSupportsExpressionsInOrderBy, true);
    builder.append(SqlInfo::SqlSupportsOrderByUnrelated, true);
    builder.append(SqlInfo::SqlSupportedGroupBy, 3i32);
    builder.append(SqlInfo::SqlSupportsLikeEscapeClause, true);
    builder.append(SqlInfo::SqlSupportsNonNullableColumns, true);
    builder.append(
        SqlInfo::SqlSupportedGrammar,
        SupportedSqlGrammar::SqlCoreGrammar as i32,
    );
    // report IOx supports all ansi 92
    builder.append(SqlInfo::SqlAnsi92SupportedLevel, 0b111_i32);
    builder.append(SqlInfo::SqlSupportsIntegrityEnhancementFacility, false);
    builder.append(SqlInfo::SqlOuterJoinsSupportLevel, 2i32);
    builder.append(SqlInfo::SqlSchemaTerm, "schema");
    builder.append(SqlInfo::SqlProcedureTerm, "procedure");
    builder.append(SqlInfo::SqlCatalogAtStart, false);
    builder.append(SqlInfo::SqlSchemasSupportedActions, 0i32);
    builder.append(SqlInfo::SqlCatalogsSupportedActions, 0i32);
    builder.append(SqlInfo::SqlSupportedPositionedCommands, 0i32);
    builder.append(SqlInfo::SqlSelectForUpdateSupported, false);
    builder.append(SqlInfo::SqlStoredProceduresSupported, false);
    builder.append(SqlInfo::SqlSupportedSubqueries, 15i32);
    builder.append(SqlInfo::SqlCorrelatedSubqueriesSupported, true);
    builder.append(SqlInfo::SqlSupportedUnions, 3i32);
    // For max lengths, report max arrow string length (IOx
    // doesn't enfore many of these limits yet
    builder.append(SqlInfo::SqlMaxBinaryLiteralLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxCharLiteralLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxColumnNameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxColumnsInGroupBy, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxColumnsInIndex, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxColumnsInOrderBy, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxColumnsInSelect, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxColumnsInTable, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxConnections, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxCursorNameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxIndexLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlDbSchemaNameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxProcedureNameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxCatalogNameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxRowSize, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxRowSizeIncludesBlobs, true);
    builder.append(SqlInfo::SqlMaxStatementLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxStatements, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxTableNameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxTablesInSelect, i32::MAX as i64);
    builder.append(SqlInfo::SqlMaxUsernameLength, i32::MAX as i64);
    builder.append(SqlInfo::SqlDefaultTransactionIsolation, 0i64);
    builder.append(SqlInfo::SqlTransactionsSupported, false);
    builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0i32);
    builder.append(SqlInfo::SqlDataDefinitionCausesTransactionCommit, false);
    builder.append(SqlInfo::SqlDataDefinitionsInTransactionsIgnored, true);
    builder.append(SqlInfo::SqlSupportedResultSetTypes, 0i32);
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetUnspecified,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetForwardOnly,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetScrollSensitive,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetScrollInsensitive,
        0i32,
    );
    builder.append(SqlInfo::SqlBatchUpdatesSupported, false);
    builder.append(SqlInfo::SqlSavepointsSupported, false);
    builder.append(SqlInfo::SqlNamedParametersSupported, false);
    builder.append(SqlInfo::SqlLocatorsUpdateCopy, false);
    builder.append(SqlInfo::SqlStoredFunctionsUsingCallSyntaxSupported, false);

    builder.build().expect("Successfully built metadata")
});

/// Return a [`SqlInfoData`] that describes IOx's capablities
pub fn iox_sql_info_data() -> &'static SqlInfoData {
    &INSTANCE
}
