//! APIs for testing.
#![cfg(test)]

use crate::test::TestChunk;
use crate::QueryChunkMeta;
use influxdb_influxql_parser::parse_statements;
use influxdb_influxql_parser::select::{Field, SelectStatement};
use influxdb_influxql_parser::statement::Statement;
use predicate::rpc_predicate::QueryNamespaceMeta;
use schema::Schema;
use std::sync::Arc;

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

pub(crate) struct MockNamespace {
    chunks: Vec<TestChunk>,
}

impl Default for MockNamespace {
    fn default() -> Self {
        let chunks = vec![
            TestChunk::new("cpu")
                .with_quiet()
                .with_tag_column("host")
                .with_tag_column("region")
                .with_f64_field_column("usage_user")
                .with_f64_field_column("usage_system")
                .with_f64_field_column("usage_idle"),
            TestChunk::new("disk")
                .with_quiet()
                .with_tag_column("host")
                .with_tag_column("region")
                .with_i64_field_column("bytes_used")
                .with_i64_field_column("bytes_free"),
            TestChunk::new("diskio")
                .with_quiet()
                .with_tag_column("host")
                .with_tag_column("region")
                .with_tag_column("status")
                .with_i64_field_column("bytes_read")
                .with_i64_field_column("bytes_written")
                .with_f64_field_column("read_utilization")
                .with_f64_field_column("write_utilization")
                .with_bool_field_column("is_local"),
            // Schemas for testing merged schemas
            TestChunk::new("temp_01")
                .with_quiet()
                .with_tag_column("shared_tag0")
                .with_tag_column("shared_tag1")
                .with_f64_field_column("shared_field0")
                .with_f64_field_column("field_f64")
                .with_i64_field_column("field_i64")
                .with_string_field_column_with_stats("field_str", None, None),
            TestChunk::new("temp_02")
                .with_quiet()
                .with_tag_column("shared_tag0")
                .with_tag_column("shared_tag1")
                .with_i64_field_column("shared_field0"),
            TestChunk::new("temp_03")
                .with_quiet()
                .with_tag_column("shared_tag0")
                .with_tag_column("shared_tag1")
                .with_string_field_column_with_stats("shared_field0", None, None),
        ];
        Self { chunks }
    }
}

impl QueryNamespaceMeta for MockNamespace {
    fn table_names(&self) -> Vec<String> {
        self.chunks
            .iter()
            .map(|x| x.table_name().to_string())
            .collect()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        let c = self.chunks.iter().find(|x| x.table_name() == table_name)?;
        Some(c.schema())
    }
}
