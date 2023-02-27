//! APIs for testing.
#![cfg(test)]

use crate::plan::influxql::SchemaProvider;
use crate::test::{TestChunk, TestDatabase};
use crate::QueryChunkMeta;
use datafusion::common::DataFusionError;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use influxdb_influxql_parser::parse_statements;
use influxdb_influxql_parser::select::{Field, SelectStatement};
use influxdb_influxql_parser::statement::Statement;
use predicate::rpc_predicate::QueryNamespaceMeta;
use schema::Schema;
use std::collections::HashMap;
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
                    .with_tag_column("cpu")
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
                    .with_tag_column("device")
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
                    .with_u64_field_column_no_stats("field_u64")
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
            // Schemas for testing clashing column names when merging across measurements
            Arc::new(
                TestChunk::new("merge_00")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("col0")
                    .with_f64_field_column("col1")
                    .with_bool_field_column("col2")
                    .with_string_field_column_with_stats("col3", None, None)
                    .with_one_row_of_data(),
            ),
            Arc::new(
                TestChunk::new("merge_01")
                    .with_id(next_chunk_id())
                    .with_quiet()
                    .with_time_column()
                    .with_tag_column("col1")
                    .with_f64_field_column("col0")
                    .with_bool_field_column("col3")
                    .with_string_field_column_with_stats("col2", None, None)
                    .with_one_row_of_data(),
            ),
        ]
    }
}

pub(crate) struct MockSchemaProvider {
    chunks: Vec<Arc<TestChunk>>,
}

impl Default for MockSchemaProvider {
    fn default() -> Self {
        let chunks = database::chunks();
        Self { chunks }
    }
}

impl SchemaProvider for MockSchemaProvider {
    fn get_table_provider(
        &self,
        _name: &str,
    ) -> crate::exec::context::Result<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn table_names(&self) -> Vec<&'_ str> {
        self.chunks.iter().map(|x| x.table_name()).collect()
    }

    fn table_schema(&self, name: &str) -> Option<Schema> {
        let c = self.chunks.iter().find(|x| x.table_name() == name)?;
        Some(c.schema().clone())
    }
}

pub(crate) struct TestDatabaseAdapter {
    tables: HashMap<String, (Arc<dyn TableSource>, Schema)>,
}

impl SchemaProvider for TestDatabaseAdapter {
    fn get_table_provider(&self, name: &str) -> crate::exec::context::Result<Arc<dyn TableSource>> {
        self.tables
            .get(name)
            .map(|(t, _)| Arc::clone(t))
            .ok_or_else(|| DataFusionError::Plan(format!("measurement does not exist: {name}")))
    }

    fn table_names(&self) -> Vec<&'_ str> {
        self.tables.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    }

    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(name).map(|(_, s)| s.clone())
    }
}

impl TestDatabaseAdapter {
    pub(crate) fn new(db: &TestDatabase) -> Self {
        let table_names = db.table_names();
        let mut res = Self {
            tables: HashMap::with_capacity(table_names.len()),
        };
        for table in table_names {
            let schema = db.table_schema(&table).unwrap();
            let s = Arc::new(EmptyTable::new(schema.as_arrow()));
            res.tables.insert(table, (provider_as_source(s), schema));
        }

        res
    }
}
