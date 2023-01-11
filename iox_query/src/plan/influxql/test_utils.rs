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

/// Module which provides a test database and schema for InfluxQL tests.
pub(crate) mod database {
    use super::*;

    /// Return a set of chunks that make up the test database.
    ///
    /// ## NOTE
    /// The chunks returned by this function start numbering their
    /// IDs from 1,000,000. A caller may wish to merge additional chunks
    /// by using IDs less than 1,000,000.
    pub(crate) fn chunks() -> Vec<Arc<TestChunk>> {
        let mut chunk_id = 1_000_000;
        let mut next_chunk_id = || {
            chunk_id += 1;
            chunk_id
        };

        vec![
            Arc::new(
                TestChunk::new("cpu")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("host")
                    .with_tag_column("region")
                    .with_f64_field_column("usage_user")
                    .with_f64_field_column("usage_system")
                    .with_f64_field_column("usage_idle")
                    .with_one_row_of_data(),
            ),
            Arc::new(
                TestChunk::new("disk")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("host")
                    .with_tag_column("region")
                    .with_i64_field_column("bytes_used")
                    .with_i64_field_column("bytes_free")
                    .with_one_row_of_data(),
            ),
            Arc::new(
                TestChunk::new("diskio")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("host")
                    .with_tag_column("region")
                    .with_tag_column("status")
                    .with_i64_field_column("bytes_read")
                    .with_i64_field_column("bytes_written")
                    .with_f64_field_column("read_utilization")
                    .with_f64_field_column("write_utilization")
                    .with_bool_field_column("is_local")
                    .with_one_row_of_data(),
            ),
            // Schemas for testing merged schemas
            Arc::new(
                TestChunk::new("temp_01")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("shared_tag0")
                    .with_tag_column("shared_tag1")
                    .with_f64_field_column("shared_field0")
                    .with_f64_field_column("field_f64")
                    .with_i64_field_column("field_i64")
                    .with_string_field_column_with_stats("field_str", None, None)
                    .with_one_row_of_data(),
            ),
            Arc::new(
                TestChunk::new("temp_02")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("shared_tag0")
                    .with_tag_column("shared_tag1")
                    .with_i64_field_column("shared_field0")
                    .with_one_row_of_data(),
            ),
            Arc::new(
                TestChunk::new("temp_03")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("shared_tag0")
                    .with_tag_column("shared_tag1")
                    .with_string_field_column_with_stats("shared_field0", None, None)
                    .with_one_row_of_data(),
            ),
        ]
    }
}

pub(crate) struct MockNamespace {
    chunks: Vec<Arc<TestChunk>>,
}

impl Default for MockNamespace {
    fn default() -> Self {
        let chunks = database::chunks();
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

    fn table_schema(&self, table_name: &str) -> Option<Schema> {
        let c = self.chunks.iter().find(|x| x.table_name() == table_name)?;
        Some(c.schema().clone())
    }
}
