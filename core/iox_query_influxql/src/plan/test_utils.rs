//! APIs for testing.
#![cfg(test)]

use crate::error;
use crate::plan::SchemaProvider;
use chrono::{DateTime, NaiveDate, Utc};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::physical_expr::execution_props::ExecutionProps;
use influxdb_influxql_parser::parse_statements;
use influxdb_influxql_parser::select::SelectStatement;
use influxdb_influxql_parser::statement::Statement;
use itertools::Itertools;
use schema::{Schema, SchemaBuilder};
use std::collections::HashMap;
use std::sync::Arc;

/// Returns the InfluxQL [`SelectStatement`] for the specified SQL, `s`.
pub(crate) fn parse_select(s: &str) -> SelectStatement {
    let statements = parse_statements(s).unwrap();
    match statements.first() {
        Some(Statement::Select(sel)) => *sel.clone(),
        _ => panic!("expected SELECT statement"),
    }
}

/// Module which provides a test database and schema for InfluxQL tests.
pub(crate) mod database {
    use super::*;
    use schema::InfluxFieldType;

    /// Return a set of schemas that make up the test database.
    pub(crate) fn schemas() -> Vec<Schema> {
        vec![
            SchemaBuilder::new()
                .measurement("cpu")
                .timestamp()
                .tag("host")
                .tag("region")
                .tag("cpu")
                .influx_field("usage_user", InfluxFieldType::Float)
                .influx_field("usage_system", InfluxFieldType::Float)
                .influx_field("usage_idle", InfluxFieldType::Float)
                .build()
                .unwrap(),
            SchemaBuilder::new()
                .measurement("disk")
                .timestamp()
                .tag("host")
                .tag("region")
                .tag("device")
                .influx_field("bytes_used", InfluxFieldType::Integer)
                .influx_field("bytes_free", InfluxFieldType::Integer)
                .build()
                .unwrap(),
            SchemaBuilder::new()
                .measurement("diskio")
                .timestamp()
                .tag("host")
                .tag("region")
                .tag("status")
                .influx_field("bytes_read", InfluxFieldType::Integer)
                .influx_field("bytes_written", InfluxFieldType::Integer)
                .influx_field("read_utilization", InfluxFieldType::Float)
                .influx_field("write_utilization", InfluxFieldType::Float)
                .influx_field("is_local", InfluxFieldType::Boolean)
                .build()
                .unwrap(),
            // Schemas for testing merged schemas
            SchemaBuilder::new()
                .measurement("temp_01")
                .timestamp()
                .tag("shared_tag0")
                .tag("shared_tag1")
                .influx_field("shared_field0", InfluxFieldType::Float)
                .influx_field("field_f64", InfluxFieldType::Float)
                .influx_field("field_i64", InfluxFieldType::Integer)
                .influx_field("field_u64", InfluxFieldType::UInteger)
                .influx_field("field_str", InfluxFieldType::String)
                .build()
                .unwrap(),
            SchemaBuilder::new()
                .measurement("temp_02")
                .timestamp()
                .tag("shared_tag0")
                .tag("shared_tag1")
                .influx_field("shared_field0", InfluxFieldType::Integer)
                .build()
                .unwrap(),
            SchemaBuilder::new()
                .measurement("temp_03")
                .timestamp()
                .tag("shared_tag0")
                .tag("shared_tag1")
                .influx_field("shared_field0", InfluxFieldType::String)
                .build()
                .unwrap(),
            // Schemas for testing clashing column names when merging across measurements
            SchemaBuilder::new()
                .measurement("merge_00")
                .timestamp()
                .tag("col0")
                .influx_field("col1", InfluxFieldType::Float)
                .influx_field("col2", InfluxFieldType::Boolean)
                .influx_field("col3", InfluxFieldType::String)
                .build()
                .unwrap(),
            SchemaBuilder::new()
                .measurement("merge_01")
                .timestamp()
                .tag("col1")
                .influx_field("col0", InfluxFieldType::Float)
                .influx_field("col3", InfluxFieldType::Boolean)
                .influx_field("col2", InfluxFieldType::String)
                .build()
                .unwrap(),
            // Schema with all types
            SchemaBuilder::new()
                .measurement("all_types")
                .timestamp()
                .tag("tag0")
                .influx_field("field_f64", InfluxFieldType::Float)
                .influx_field("field_i64", InfluxFieldType::Integer)
                .influx_field("field_u64", InfluxFieldType::UInteger)
                .influx_field("field_str", InfluxFieldType::String)
                .influx_field("field_bool", InfluxFieldType::Boolean)
                .build()
                .unwrap(),
        ]
    }
}

pub(crate) struct MockSchemaProvider {
    execution_props: ExecutionProps,
    tables: HashMap<String, (Arc<dyn TableSource>, Schema)>,
}

impl Default for MockSchemaProvider {
    fn default() -> Self {
        // Choose a static start time so that tests are deteministic.
        let start_time = NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let start_time = DateTime::<Utc>::from_naive_utc_and_offset(start_time, Utc);
        let mut execution_props = ExecutionProps::new();
        execution_props.query_execution_start_time = start_time;
        let mut res = Self {
            execution_props,
            tables: HashMap::new(),
        };
        res.add_schemas(database::schemas());
        res
    }
}

impl MockSchemaProvider {
    pub(crate) fn add_schema(&mut self, schema: Schema) {
        let schema = schema.sort_fields_by_name();

        let table_name = schema.measurement().unwrap().clone();
        let s = Arc::new(EmptyTable::new(schema.as_arrow()));
        self.tables
            .insert(table_name, (provider_as_source(s), schema));
    }

    pub(crate) fn add_schemas(&mut self, schemas: impl IntoIterator<Item = Schema>) {
        schemas.into_iter().for_each(|s| self.add_schema(s));
    }
}

impl SchemaProvider for MockSchemaProvider {
    fn get_table_provider(&self, name: &str) -> DataFusionResult<Arc<dyn TableSource>> {
        self.tables
            .get(name)
            .map(|(t, _)| Arc::clone(t))
            .ok_or_else(|| error::map::query(format!("measurement does not exist: {name}")))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn table_names(&self) -> Vec<&'_ str> {
        self.tables
            .keys()
            .map(|k| k.as_str())
            .sorted()
            .collect::<Vec<_>>()
    }

    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(name).map(|(_, s)| s.clone())
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }
}
