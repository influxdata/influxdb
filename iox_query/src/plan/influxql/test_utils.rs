//! APIs for testing.
#![cfg(test)]

use crate::test::TestChunk;
use crate::QueryChunkMeta;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use influxdb_influxql_parser::parse_statements;
use influxdb_influxql_parser::select::{Field, SelectStatement};
use influxdb_influxql_parser::statement::Statement;
use itertools::Itertools;
use std::any::Any;
use std::sync::Arc;

struct EmptyTable {
    table_schema: SchemaRef,
}

impl EmptyTable {
    pub(crate) fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

/// Returns the first `Field` of the `SELECT` statement.
pub(crate) fn get_first_field(s: &str) -> Field {
    parse_select(s).fields.head().unwrap().clone()
}

/// Returns the InfluxQL [`SelectStatement`] for the specified SQL, `s`.
pub(crate) fn parse_select(s: &str) -> SelectStatement {
    let statements = parse_statements(s).unwrap();
    match statements.first() {
        Some(Statement::Select(sel)) => *sel.clone(),
        _ => panic!("expected SELECT statement"),
    }
}

#[async_trait]
impl TableProvider for EmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }
}

pub(crate) struct MockSchemaProvider {}

impl MockSchemaProvider {
    /// Convenience constructor to return a new instance of [`Self`] as a dynamic [`SchemaProvider`].
    pub(crate) fn new_schema_provider() -> Arc<dyn SchemaProvider> {
        Arc::new(Self {})
    }
}

impl SchemaProvider for MockSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            "cpu".into(),
            "disk".into(),
            "diskio".into(),
            "temp_01".into(),
            "temp_03".into(),
            "temp_03".into(),
        ]
        .into_iter()
        .sorted()
        .collect::<Vec<_>>()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let schema = match name {
            "cpu" => Some(
                TestChunk::new("cpu")
                    .with_tag_column("host")
                    .with_tag_column("region")
                    .with_f64_field_column("usage_user")
                    .with_f64_field_column("usage_system")
                    .with_f64_field_column("usage_idle")
                    .schema(),
            ),
            "disk" => Some(
                TestChunk::new("disk")
                    .with_tag_column("host")
                    .with_tag_column("region")
                    .with_i64_field_column("bytes_used")
                    .with_i64_field_column("bytes_free")
                    .schema(),
            ),
            "diskio" => Some(
                TestChunk::new("diskio")
                    .with_tag_column("host")
                    .with_tag_column("region")
                    .with_tag_column("status")
                    .with_i64_field_column("bytes_read")
                    .with_i64_field_column("bytes_written")
                    .with_f64_field_column("read_utilization")
                    .with_f64_field_column("write_utilization")
                    .with_bool_field_column("is_local")
                    .schema(),
            ),
            // Schemas for testing merged schemas
            "temp_01" => Some(
                TestChunk::new("temp_01")
                    .with_tag_column("shared_tag0")
                    .with_tag_column("shared_tag1")
                    .with_f64_field_column("shared_field0")
                    .with_f64_field_column("field_f64")
                    .with_i64_field_column("field_i64")
                    .with_string_field_column_with_stats("field_str", None, None)
                    .schema(),
            ),
            "temp_02" => Some(
                TestChunk::new("temp_02")
                    .with_tag_column("shared_tag0")
                    .with_tag_column("shared_tag1")
                    .with_i64_field_column("shared_field0")
                    .schema(),
            ),
            "temp_03" => Some(
                TestChunk::new("temp_03")
                    .with_tag_column("shared_tag0")
                    .with_tag_column("shared_tag1")
                    .with_string_field_column_with_stats("shared_field0", None, None)
                    .schema(),
            ),
            _ => None,
        };

        match schema {
            Some(s) => Some(Arc::new(EmptyTable::new(Arc::clone(s.inner())))),
            None => None,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
    }
}
