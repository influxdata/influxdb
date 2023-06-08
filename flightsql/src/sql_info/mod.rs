//! TODO: use version in flight-sql when
//! <https://github.com/apache/arrow-rs/pull/4266> is available
//!
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
mod value;

use crate::error::Result;
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use arrow::{
    array::UInt32Builder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_flight::sql::{
    SqlInfo, SqlNullOrdering, SqlSupportedCaseSensitivity, SqlSupportedTransactions,
    SupportedSqlGrammar,
};
use once_cell::sync::Lazy;

use meta::{
    SQL_INFO_DATE_TIME_FUNCTIONS, SQL_INFO_NUMERIC_FUNCTIONS, SQL_INFO_SQL_KEYWORDS,
    SQL_INFO_STRING_FUNCTIONS, SQL_INFO_SYSTEM_FUNCTIONS,
};
use value::{SqlInfoName, SqlInfoUnionBuilder, SqlInfoValue};

/// A list of SQL info names and valies
#[derive(Debug, Clone, PartialEq)]
pub struct SqlInfoList {
    /// Use BTreeMap to ensure the values are sorted by value as
    /// to make output consistent
    ///
    /// Use u32 to support "custom" sql info values that are not
    /// part of the SqlInfo enum
    infos: BTreeMap<u32, SqlInfoValue>,
}

impl SqlInfoList {
    pub fn new() -> Self {
        Self {
            infos: BTreeMap::new(),
        }
    }

    /// register the specific sql metadata item
    fn with_sql_info(mut self, name: impl SqlInfoName, value: impl Into<SqlInfoValue>) -> Self {
        self.infos.insert(name.as_u32(), value.into());
        self
    }

    /// Filter this info list keeping only the info values specified
    /// in `infos`.
    ///
    /// Returns self if infos is empty (no filtering)
    pub fn filter(&self, info: &[u32]) -> Cow<'_, Self> {
        if info.is_empty() {
            Cow::Borrowed(self)
        } else {
            let infos: BTreeMap<_, _> = info
                .iter()
                .filter_map(|name| self.infos.get(name).map(|v| (*name, v.clone())))
                .collect();
            Cow::Owned(Self { infos })
        }
    }

    /// Encode the contents of this info list according to the FlightSQL spec
    pub fn encode(&self) -> Result<RecordBatch> {
        let mut name_builder = UInt32Builder::new();
        let mut value_builder = SqlInfoUnionBuilder::new();

        for (&name, value) in &self.infos {
            name_builder.append_value(name);
            value_builder.append_value(value)
        }

        let batch = RecordBatch::try_from_iter(vec![
            ("info_name", Arc::new(name_builder.finish()) as _),
            ("value", Arc::new(value_builder.finish()) as _),
        ])?;
        Ok(batch)
    }

    /// Return the schema for the record batches produced
    pub fn schema(&self) -> SchemaRef {
        // It is always the same
        Arc::clone(&SCHEMA)
    }
}

// The schema produced by [`SqlInfoList`]
static SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new("value", SqlInfoUnionBuilder::schema().clone(), false),
    ]))
});

#[allow(non_snake_case)]
static INSTANCE: Lazy<SqlInfoList> = Lazy::new(|| {
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

    SqlInfoList::new()
        // Server information
        .with_sql_info(SqlInfo::FlightSqlServerName, "InfluxDB IOx")
        .with_sql_info(SqlInfo::FlightSqlServerVersion, "2")
        // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
        .with_sql_info(SqlInfo::FlightSqlServerArrowVersion, "1.3")
        .with_sql_info(SqlInfo::FlightSqlServerReadOnly, true)
        .with_sql_info(SqlInfoFlightSqlServerSql, true)
        .with_sql_info(SqlInfoFlightSqlServerSubstrait, false)
        .with_sql_info(
            SqlInfoFlightSqlServerTransaction,
            SqlSupportedTransactions::SqlTransactionUnspecified as i32,
        )
        // don't yetsupport `CancelQuery` action
        .with_sql_info(SqlInfoFlightSqlServerCancel, false)
        .with_sql_info(SqlInfoFlightSqlServerStatementTimeout, 0i32)
        .with_sql_info(SqlInfoFlightSqlServerTransactionTimeout, 0i32)
        // SQL syntax information
        .with_sql_info(SqlInfo::SqlDdlCatalog, false)
        .with_sql_info(SqlInfo::SqlDdlSchema, false)
        .with_sql_info(SqlInfo::SqlDdlTable, false)
        .with_sql_info(
            SqlInfo::SqlIdentifierCase,
            SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase as i32,
        )
        .with_sql_info(SqlInfo::SqlIdentifierQuoteChar, r#"""#)
        .with_sql_info(
            SqlInfo::SqlQuotedIdentifierCase,
            SqlSupportedCaseSensitivity::SqlCaseSensitivityCaseInsensitive as i32,
        )
        .with_sql_info(SqlInfo::SqlAllTablesAreSelectable, true)
        .with_sql_info(
            SqlInfo::SqlNullOrdering,
            SqlNullOrdering::SqlNullsSortedHigh as i32,
        )
        .with_sql_info(SqlInfo::SqlKeywords, SQL_INFO_SQL_KEYWORDS)
        .with_sql_info(SqlInfo::SqlNumericFunctions, SQL_INFO_NUMERIC_FUNCTIONS)
        .with_sql_info(SqlInfo::SqlStringFunctions, SQL_INFO_STRING_FUNCTIONS)
        .with_sql_info(SqlInfo::SqlSystemFunctions, SQL_INFO_SYSTEM_FUNCTIONS)
        .with_sql_info(SqlInfo::SqlDatetimeFunctions, SQL_INFO_DATE_TIME_FUNCTIONS)
        .with_sql_info(SqlInfo::SqlSearchStringEscape, "\\")
        .with_sql_info(SqlInfo::SqlExtraNameCharacters, "")
        .with_sql_info(SqlInfo::SqlSupportsColumnAliasing, true)
        .with_sql_info(SqlInfo::SqlNullPlusNullIsNull, true)
        // Skip SqlSupportsConvert (which is the map of the conversions that are supported)
        // .with_sql_info(SqlInfo::SqlSupportsConvert, TBD)
        // https://github.com/influxdata/influxdb_iox/issues/7253
        .with_sql_info(SqlInfo::SqlSupportsTableCorrelationNames, false)
        .with_sql_info(SqlInfo::SqlSupportsDifferentTableCorrelationNames, false)
        .with_sql_info(SqlInfo::SqlSupportsExpressionsInOrderBy, true)
        .with_sql_info(SqlInfo::SqlSupportsOrderByUnrelated, true)
        .with_sql_info(SqlInfo::SqlSupportedGroupBy, 3i32)
        .with_sql_info(SqlInfo::SqlSupportsLikeEscapeClause, true)
        .with_sql_info(SqlInfo::SqlSupportsNonNullableColumns, true)
        .with_sql_info(
            SqlInfo::SqlSupportedGrammar,
            SupportedSqlGrammar::SqlCoreGrammar as i32,
        )
        // report IOx supports all ansi 92
        .with_sql_info(SqlInfo::SqlAnsi92SupportedLevel, 0b111_i32)
        .with_sql_info(SqlInfo::SqlSupportsIntegrityEnhancementFacility, false)
        .with_sql_info(SqlInfo::SqlOuterJoinsSupportLevel, 2i32)
        .with_sql_info(SqlInfo::SqlSchemaTerm, "schema")
        .with_sql_info(SqlInfo::SqlProcedureTerm, "procedure")
        .with_sql_info(SqlInfo::SqlCatalogAtStart, false)
        .with_sql_info(SqlInfo::SqlSchemasSupportedActions, 0i32)
        .with_sql_info(SqlInfo::SqlCatalogsSupportedActions, 0i32)
        .with_sql_info(SqlInfo::SqlSupportedPositionedCommands, 0i32)
        .with_sql_info(SqlInfo::SqlSelectForUpdateSupported, false)
        .with_sql_info(SqlInfo::SqlStoredProceduresSupported, false)
        .with_sql_info(SqlInfo::SqlSupportedSubqueries, 15i32)
        .with_sql_info(SqlInfo::SqlCorrelatedSubqueriesSupported, true)
        .with_sql_info(SqlInfo::SqlSupportedUnions, 3i32)
        // For max lengths, report max arrow string length (IOx
        // doesn't enfore many of these limits yet
        .with_sql_info(SqlInfo::SqlMaxBinaryLiteralLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxCharLiteralLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxColumnNameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxColumnsInGroupBy, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxColumnsInIndex, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxColumnsInOrderBy, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxColumnsInSelect, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxColumnsInTable, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxConnections, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxCursorNameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxIndexLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlDbSchemaNameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxProcedureNameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxCatalogNameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxRowSize, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxRowSizeIncludesBlobs, true)
        .with_sql_info(SqlInfo::SqlMaxStatementLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxStatements, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxTableNameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxTablesInSelect, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlMaxUsernameLength, i32::MAX as i64)
        .with_sql_info(SqlInfo::SqlDefaultTransactionIsolation, 0i64)
        .with_sql_info(SqlInfo::SqlTransactionsSupported, false)
        .with_sql_info(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0i32)
        .with_sql_info(SqlInfo::SqlDataDefinitionCausesTransactionCommit, false)
        .with_sql_info(SqlInfo::SqlDataDefinitionsInTransactionsIgnored, true)
        .with_sql_info(SqlInfo::SqlSupportedResultSetTypes, 0i32)
        .with_sql_info(
            SqlInfo::SqlSupportedConcurrenciesForResultSetUnspecified,
            0i32,
        )
        .with_sql_info(
            SqlInfo::SqlSupportedConcurrenciesForResultSetForwardOnly,
            0i32,
        )
        .with_sql_info(
            SqlInfo::SqlSupportedConcurrenciesForResultSetScrollSensitive,
            0i32,
        )
        .with_sql_info(
            SqlInfo::SqlSupportedConcurrenciesForResultSetScrollInsensitive,
            0i32,
        )
        .with_sql_info(SqlInfo::SqlBatchUpdatesSupported, false)
        .with_sql_info(SqlInfo::SqlSavepointsSupported, false)
        .with_sql_info(SqlInfo::SqlNamedParametersSupported, false)
        .with_sql_info(SqlInfo::SqlLocatorsUpdateCopy, false)
        .with_sql_info(SqlInfo::SqlStoredFunctionsUsingCallSyntaxSupported, false)
});

/// Return a [`SqlInfoList`] that describes IOx's capablities
pub fn iox_sql_info_list() -> &'static SqlInfoList {
    &INSTANCE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_empty() {
        let filter = &[];
        assert_eq!(
            iox_sql_info_list(),
            iox_sql_info_list().filter(filter).as_ref()
        )
    }

    #[test]
    fn filter_some() {
        let filter = &[
            SqlInfo::FlightSqlServerName as u32,
            SqlInfo::FlightSqlServerArrowVersion as u32,
            SqlInfo::SqlBatchUpdatesSupported as u32,
            999999, //  model some unknown info requested
        ];
        let result = iox_sql_info_list().filter(filter);

        let infos = &result.infos;
        assert_eq!(result.infos.len(), 3);
        assert!(infos.contains_key(&(SqlInfo::FlightSqlServerName as u32)));
        assert!(infos.contains_key(&(SqlInfo::FlightSqlServerArrowVersion as u32)));
        assert!(infos.contains_key(&(SqlInfo::SqlBatchUpdatesSupported as u32)));
        assert!(!infos.contains_key(&999999));
    }
}
