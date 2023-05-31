//! APIs for testing.
#![cfg(test)]

use crate::plan::ir::DataSourceSchema;
use crate::plan::util::Schemas;
use chrono::{DateTime, NaiveDate, Utc};
use datafusion::common::{DFSchemaRef, ToDFSchema};
use datafusion::execution::context::ExecutionProps;
use schema::{InfluxFieldType, SchemaBuilder};
use std::sync::Arc;

pub(super) fn new_schemas() -> (Schemas, DataSourceSchema<'static>) {
    let iox_schema = SchemaBuilder::new()
        .measurement("m0")
        .timestamp()
        .tag("tag0")
        .tag("tag1")
        .influx_field("float_field", InfluxFieldType::Float)
        .influx_field("integer_field", InfluxFieldType::Integer)
        .influx_field("unsigned_field", InfluxFieldType::UInteger)
        .influx_field("string_field", InfluxFieldType::String)
        .influx_field("boolean_field", InfluxFieldType::Boolean)
        .build()
        .expect("schema failed");
    let df_schema: DFSchemaRef = Arc::clone(iox_schema.inner()).to_dfschema_ref().unwrap();
    (Schemas { df_schema }, DataSourceSchema::Table(iox_schema))
}

/// Return execution properties with a date of `2023-01-01T00:00:00Z`, which may be used to
/// evaluate the `now` function in data fusion logical expressions during simplification.
pub(super) fn execution_props() -> ExecutionProps {
    let start_time = NaiveDate::from_ymd_opt(2023, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let start_time = DateTime::<Utc>::from_utc(start_time, Utc);
    let mut props = ExecutionProps::new();
    props.query_execution_start_time = start_time;
    props
}
