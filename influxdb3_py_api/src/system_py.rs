use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_wal::{FieldData, Row, WriteBatch};
use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyList};
use pyo3::{
    pyclass, pymethods, pymodule, Bound, IntoPy, IntoPyObject, PyErr, PyObject, PyResult, Python,
};
use schema::{InfluxColumnType, Schema};
use std::ffi::CString;
use std::sync::Arc;

#[pyclass]
#[derive(Debug)]
pub struct PyWriteBatchIterator {
    table_definition: Arc<TableDefinition>,
    rows: Vec<Row>,
    current_index: usize,
}

#[pymethods]
impl PyWriteBatchIterator {
    fn next_point(&mut self) -> PyResult<Option<PyObject>> {
        if self.current_index >= self.rows.len() {
            return Ok(None);
        }

        Python::with_gil(|py| {
            let row = &self.rows[self.current_index];
            self.current_index += 1;

            // Import Point class
            let point_class = py
                .import("influxdb_client_3.write_client.client.write.point")?
                .getattr("Point")
                .unwrap();

            // Create new Point instance with measurement name (table name)
            let point = point_class.call1((self.table_definition.table_name.as_ref(),))?;

            // Set timestamp
            point.call_method1("time", (row.time,))?;

            // Add fields based on column definitions and field data
            for field in &row.fields {
                if let Some(col_def) = self.table_definition.columns.get(&field.id) {
                    let field_name = col_def.name.as_ref();

                    match col_def.data_type {
                        InfluxColumnType::Tag => {
                            let FieldData::Tag(tag) = &field.value else {
                                // error out because we expect a tag
                                return Err(PyValueError::new_err(format!(
                                    "expect FieldData:Tag for tagged columns, not ${:?}",
                                    field
                                )));
                            };
                            point.call_method1("tag", (field_name, tag.as_str()))?;
                        }
                        InfluxColumnType::Timestamp => {}
                        InfluxColumnType::Field(_) => {
                            match &field.value {
                                FieldData::String(s) => {
                                    point.call_method1("field", (field_name, s.as_str()))?
                                }
                                FieldData::Integer(i) => {
                                    point.call_method1("field", (field_name, *i))?
                                }
                                FieldData::UInteger(u) => {
                                    point.call_method1("field", (field_name, *u))?
                                }
                                FieldData::Float(f) => {
                                    point.call_method1("field", (field_name, *f))?
                                }
                                FieldData::Boolean(b) => {
                                    point.call_method1("field", (field_name, *b))?
                                }
                                FieldData::Tag(t) => {
                                    point.call_method1("field", (field_name, t.as_str()))?
                                }
                                FieldData::Key(k) => {
                                    point.call_method1("field", (field_name, k.as_str()))?
                                }
                                FieldData::Timestamp(ts) => {
                                    point.call_method1("field", (field_name, *ts))?
                                }
                            };
                        }
                    }
                }
            }

            Ok(Some(point.into_pyobject(py)?.unbind()))
        })
    }
}

#[pyclass]
#[derive(Debug)]
pub struct PyWriteBatch {
    pub write_batch: WriteBatch,
    pub schema: Arc<DatabaseSchema>,
}

#[pymethods]
impl PyWriteBatch {
    fn get_iterator_for_table(&self, table_name: &str) -> PyResult<Option<PyWriteBatchIterator>> {
        // Find table ID from name
        let table_id = self
            .schema
            .table_map
            .get_by_right(&Arc::from(table_name))
            .ok_or_else(|| {
                PyErr::new::<PyValueError, _>(format!("Table '{}' not found", table_name))
            })?;

        // Get table chunks
        let Some(chunks) = self.write_batch.table_chunks.get(table_id) else {
            return Ok(None);
        };

        // Get table definition
        let table_def = self.schema.tables.get(table_id).ok_or_else(|| {
            PyErr::new::<PyValueError, _>(format!(
                "Table definition not found for '{}'",
                table_name
            ))
        })?;

        Ok(Some(PyWriteBatchIterator {
            table_definition: Arc::clone(table_def),
            // TODO: avoid copying all the data at once.
            rows: chunks
                .chunk_time_to_chunk
                .values()
                .flat_map(|chunk| chunk.rows.clone())
                .collect(),
            current_index: 0,
        }))
    }
}

#[derive(Debug)]
#[pyclass]
pub struct PyLineProtocolOutput {
    lines: Arc<Mutex<Vec<String>>>,
}

#[pymethods]
impl PyLineProtocolOutput {
    fn insert_line_protocol(&mut self, line: &str) -> PyResult<()> {
        let mut lines = self.lines.lock();
        lines.push(line.to_string());
        Ok(())
    }
}

impl PyWriteBatch {
    pub fn call_against_table(
        &self,
        table_name: &str,
        setup_code: &str,
        call_site: &str,
    ) -> PyResult<Vec<String>> {
        let Some(iterator) = self.get_iterator_for_table(table_name)? else {
            return Ok(Vec::new());
        };

        Python::with_gil(|py| {
            py.run(&CString::new(setup_code)?, None, None)?;
            let py_func = py.eval(&CString::new(call_site)?, None, None)?;

            // Create the output collector with shared state
            let lines = Arc::new(Mutex::new(Vec::new()));
            let output = PyLineProtocolOutput {
                lines: Arc::clone(&lines),
            };

            // Pass both iterator and output collector to the Python function
            py_func.call1((iterator, output.into_pyobject(py)?))?;

            let output_lines = lines.lock().clone();
            Ok(output_lines)
        })
    }
}

#[pyclass]
#[derive(Debug)]
struct PyPluginCallApi {
    schema: Arc<DatabaseSchema>,
    catalog: Arc<Catalog>,
    return_state: Arc<Mutex<ReturnState>>,
}

#[derive(Debug, Default)]
struct ReturnState {
    log_lines: Vec<LogLine>,
}

enum LogLine {
    Info(String),
    Warn(String),
    Error(String),
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLine::Info(s) => write!(f, "INFO: {}", s),
            LogLine::Warn(s) => write!(f, "WARN: {}", s),
            LogLine::Error(s) => write!(f, "ERROR: {}", s),
        }
    }
}

impl std::fmt::Debug for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[pymethods]
impl PyPluginCallApi {
    fn info(&self, line: &str) -> PyResult<()> {
        self.return_state
            .lock()
            .log_lines
            .push(LogLine::Info(line.to_string()));
        Ok(())
    }

    fn warn(&self, line: &str) -> PyResult<()> {
        self.return_state
            .lock()
            .log_lines
            .push(LogLine::Warn(line.to_string()));
        Ok(())
    }

    fn error(&self, line: &str) -> PyResult<()> {
        self.return_state
            .lock()
            .log_lines
            .push(LogLine::Error(line.to_string()));
        Ok(())
    }
}

pub fn execute_python_with_batch(
    code: &str,
    write_batch: &WriteBatch,
    schema: Arc<DatabaseSchema>,
    catalog: Arc<Catalog>,
) -> PyResult<Vec<String>> {
    Python::with_gil(|py| {
        let mut table_batches = Vec::with_capacity(write_batch.table_chunks.len());

        for (table_id, table_chunks) in &write_batch.table_chunks {
            let table_def = schema.tables.get(table_id).unwrap();

            let dict = PyDict::new(py);
            dict.set_item("table_name", table_def.table_name.as_ref())
                .unwrap();

            let mut rows: Vec<PyObject> = Vec::new();
            for chunk in table_chunks.chunk_time_to_chunk.values() {
                for row in &chunk.rows {
                    let py_row = PyDict::new(py);
                    py_row.set_item("time", row.time).unwrap();
                    let mut fields = Vec::with_capacity(row.fields.len());
                    for field in &row.fields {
                        let field_data = match &field.value {
                            FieldData::String(s) => s.into_py(py),
                            FieldData::Integer(i) => i.into_py(py),
                            FieldData::UInteger(u) => u.into_py(py),
                            FieldData::Float(f) => f.into_py(py),
                            FieldData::Boolean(b) => b.into_py(py),
                            FieldData::Tag(t) => t.into_py(py),
                            FieldData::Key(k) => k.into_py(py),
                            FieldData::Timestamp(ts) => ts.into_py(py),
                        };
                        let field_name = table_def.column_id_to_name(&field.id).unwrap();
                        if field_name.as_ref() == "time" {
                            continue;
                        }

                        let field = PyDict::new(py);
                        field.set_item("name", field_name.as_ref()).unwrap();
                        field.set_item("value", field_data).unwrap();
                        fields.push(field.unbind());
                    }
                    let fields = PyList::new(py, fields).unwrap();
                    py_row.set_item("fields", fields.unbind()).unwrap();

                    rows.push(py_row.into());
                }
            }

            let rows = PyList::new(py, rows).unwrap();

            dict.set_item("rows", rows.unbind()).unwrap();
            table_batches.push(dict);
        }

        let py_batches = PyList::new(py, table_batches).unwrap();

        let locals = PyDict::new(py);
        locals
            .set_item("table_batches", py_batches.unbind())
            .unwrap();

        let api = PyPluginCallApi {
            schema,
            catalog,
            return_state: Default::default(),
        };
        let return_state = Arc::clone(&api.return_state);
        locals.set_item("api", api.into_py(py)).unwrap();

        py.run(&CString::new(code).unwrap(), None, Some(&locals))
            .unwrap();

        let log_lines = return_state
            .lock()
            .log_lines
            .iter()
            .map(|line| line.to_string())
            .collect();

        Ok(log_lines)
    })
}

// Module initialization
#[pymodule]
fn influxdb3_py_api(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWriteBatch>()?;
    m.add_class::<PyWriteBatchIterator>()?;
    Ok(())
}
