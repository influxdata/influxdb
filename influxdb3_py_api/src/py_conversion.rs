//! Conversion of WAL data to Python format for plugin execution.
//!
//! This module provides the `ToPythonTableBatches` trait for converting
//! parquet-based (`WriteBatch`) WAL flush data to Python table batches.

use anyhow::Context;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_id::TableId;
use influxdb3_wal::{FieldData, WriteBatch};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{PyDict, PyList};
use pyo3::{Bound, Py, PyAny, Python};

/// Trait for converting WAL data to Python table batches.
///
/// This allows WAL flush data to be converted to the Python format for plugin execution.
pub trait ToPythonTableBatches {
    /// Convert the data to Python table batches for the process_writes function.
    ///
    /// Returns a list of dicts, each with "table_name" and "rows" keys.
    fn to_python_table_batches<'py>(
        &self,
        py: Python<'py>,
        schema: &DatabaseSchema,
        table_filter: Option<TableId>,
    ) -> Result<Bound<'py, PyList>, anyhow::Error>;
}

impl ToPythonTableBatches for WriteBatch {
    fn to_python_table_batches<'py>(
        &self,
        py: Python<'py>,
        schema: &DatabaseSchema,
        table_filter: Option<TableId>,
    ) -> Result<Bound<'py, PyList>, anyhow::Error> {
        let mut table_batches = Vec::with_capacity(self.table_chunks.len());

        for (table_id, table_chunks) in &self.table_chunks {
            if let Some(filter) = table_filter
                && table_id != &filter
            {
                continue;
            }
            let table_def = schema
                .legacy_table_definition_by_id(table_id)
                .context("table not found")?;

            let dict = PyDict::new(py);
            dict.set_item("table_name", table_def.table_name.as_ref())
                .context("failed to set table_name")?;

            let mut rows: Vec<Py<PyAny>> = Vec::new();
            for chunk in table_chunks.chunk_time_to_chunk.values() {
                for row in &chunk.rows {
                    let py_row = PyDict::new(py);

                    for field in &row.fields {
                        let field_name = table_def
                            .column_id_to_name(&field.id)
                            .context("field not found")?;
                        match &field.value {
                            FieldData::String(s) => {
                                py_row
                                    .set_item(field_name.as_ref(), s.as_str())
                                    .context("failed to set string field")?;
                            }
                            FieldData::Integer(i) => {
                                py_row
                                    .set_item(field_name.as_ref(), i)
                                    .context("failed to set integer field")?;
                            }
                            FieldData::UInteger(u) => {
                                py_row
                                    .set_item(field_name.as_ref(), u)
                                    .context("failed to set unsigned integer field")?;
                            }
                            FieldData::Float(f) => {
                                py_row
                                    .set_item(field_name.as_ref(), f)
                                    .context("failed to set float field")?;
                            }
                            FieldData::Boolean(b) => {
                                py_row
                                    .set_item(field_name.as_ref(), b)
                                    .context("failed to set boolean field")?;
                            }
                            FieldData::Tag(t) => {
                                py_row
                                    .set_item(field_name.as_ref(), t.as_str())
                                    .context("failed to set tag field")?;
                            }
                            FieldData::Timestamp(t) => {
                                py_row
                                    .set_item(field_name.as_ref(), t)
                                    .context("failed to set timestamp")?;
                            }
                            FieldData::Key(_) => {
                                unreachable!("key type should never be constructed")
                            }
                        };
                    }

                    rows.push(py_row.into());
                }
            }

            let rows = PyList::new(py, rows).context("failed to create rows list")?;

            dict.set_item("rows", rows.unbind())
                .context("failed to set rows")?;
            table_batches.push(dict);
        }

        PyList::new(py, table_batches).context("failed to create table_batches list")
    }
}
