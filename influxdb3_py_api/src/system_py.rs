use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_wal::{FieldData, Row, WriteBatch};
use log::info;
use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::types::{PyDict, PyList};
use pyo3::{
    pyclass, pymethods, pymodule, Bound, IntoPyObject, PyAny, PyErr, PyObject, PyResult, Python,
};
use schema::InfluxColumnType;
use std::collections::HashMap;
use std::env;
use std::ffi::CString;
use std::sync::Arc;
use std::sync::Once;

static PYTHON_INIT: Once = Once::new();

fn ensure_python_initialized() {
    PYTHON_INIT.call_once(|| {
        // Get the executable path and set up Python paths relative to it
        if let Ok(exe) = env::current_exe() {
            if let Some(exe_dir) = exe.parent() {
                let python_home = exe_dir.join("../../python_embedded/python");
                env::set_var("PYTHONHOME", &python_home);
                env::set_var("PYTHONPATH", python_home.join("lib/python3"));
            }
        }
        info!("successfully initialized python system");
        pyo3::prepare_freethreaded_python();
    });
}

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
        ensure_python_initialized();
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
    _schema: Arc<DatabaseSchema>,
    _catalog: Arc<Catalog>,
    return_state: Arc<Mutex<PluginReturnState>>,
}

#[derive(Debug, Default)]
pub struct PluginReturnState {
    pub log_lines: Vec<LogLine>,
    pub write_back_lines: Vec<String>,
    pub write_db_lines: HashMap<String, Vec<String>>,
}

impl PluginReturnState {
    pub fn log(&self) -> Vec<String> {
        self.log_lines.iter().map(|l| l.to_string()).collect()
    }
}

pub enum LogLine {
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

    fn write(&self, line_builder: &Bound<'_, PyAny>) -> PyResult<()> {
        // Get the built line from the LineBuilder object
        let line = line_builder.getattr("build")?.call0()?;
        let line_str = line.extract::<String>()?;

        // Add to write_back_lines
        self.return_state.lock().write_back_lines.push(line_str);

        Ok(())
    }

    fn write_to_db(&self, db_name: &str, line_builder: &Bound<'_, PyAny>) -> PyResult<()> {
        let line = line_builder.getattr("build")?.call0()?;
        let line_str = line.extract::<String>()?;

        self.return_state
            .lock()
            .write_db_lines
            .entry(db_name.to_string())
            .or_default()
            .push(line_str);

        Ok(())
    }
}

// constant for the process writes call site string
const PROCESS_WRITES_CALL_SITE: &str = "process_writes";

const LINE_BUILDER_CODE: &str = r#"
from typing import Optional
from collections import OrderedDict

class InfluxDBError(Exception):
    """Base exception for InfluxDB-related errors"""
    pass

class InvalidMeasurementError(InfluxDBError):
    """Raised when measurement name is invalid"""
    pass

class InvalidKeyError(InfluxDBError):
    """Raised when a tag or field key is invalid"""
    pass

class LineBuilder:
    def __init__(self, measurement: str):
        if ' ' in measurement:
            raise InvalidMeasurementError("Measurement name cannot contain spaces")
        self.measurement = measurement
        self.tags: OrderedDict[str, str] = OrderedDict()
        self.fields: OrderedDict[str, str] = OrderedDict()
        self._timestamp_ns: Optional[int] = None

    def _validate_key(self, key: str, key_type: str) -> None:
        """Validate that a key does not contain spaces, commas, or equals signs."""
        if not key:
            raise InvalidKeyError(f"{key_type} key cannot be empty")
        if ' ' in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain spaces")
        if ',' in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain commas")
        if '=' in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain equals signs")

    def tag(self, key: str, value: str) -> 'LineBuilder':
        """Add a tag to the line protocol."""
        self._validate_key(key, "tag")
        self.tags[key] = str(value)
        return self

    def int64_field(self, key: str, value: int) -> 'LineBuilder':
        """Add an integer field to the line protocol."""
        self._validate_key(key, "field")
        self.fields[key] = f"{value}i"
        return self

    def float64_field(self, key: str, value: float) -> 'LineBuilder':
        """Add a float field to the line protocol."""
        self._validate_key(key, "field")
        # Check if value has no decimal component
        self.fields[key] = f"{int(value)}.0" if value % 1 == 0 else str(value)
        return self

    def string_field(self, key: str, value: str) -> 'LineBuilder':
        """Add a string field to the line protocol."""
        self._validate_key(key, "field")
        # Escape quotes and backslashes in string values
        escaped_value = value.replace('"', '\\"').replace('\\', '\\\\')
        self.fields[key] = f'"{escaped_value}"'
        return self

    def time_ns(self, timestamp_ns: int) -> 'LineBuilder':
        """Set the timestamp in nanoseconds."""
        self._timestamp_ns = timestamp_ns
        return self

    def build(self) -> str:
        """Build the line protocol string."""
        # Start with measurement name (escape commas only)
        line = self.measurement.replace(',', '\\,')

        # Add tags if present
        if self.tags:
            tags_str = ','.join(
                f"{k}={v}" for k, v in self.tags.items()
            )
            line += f",{tags_str}"

        # Add fields (required)
        if not self.fields:
            raise ValueError("At least one field is required")

        fields_str = ','.join(
            f"{k}={v}" for k, v in self.fields.items()
        )
        line += f" {fields_str}"

        # Add timestamp if present
        if self._timestamp_ns is not None:
            line += f" {self._timestamp_ns}"

        return line"#;

pub fn execute_python_with_batch(
    code: &str,
    write_batch: &WriteBatch,
    schema: Arc<DatabaseSchema>,
    catalog: Arc<Catalog>,
    args: Option<HashMap<String, String>>,
) -> PyResult<PluginReturnState> {
    ensure_python_initialized();
    Python::with_gil(|py| {
        // import the LineBuilder for use in the python code
        let globals = PyDict::new(py);

        py.run(
            &CString::new(LINE_BUILDER_CODE).unwrap(),
            Some(&globals),
            None,
        )?;

        // convert the write batch into a python object
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
                        let field_name = table_def.column_id_to_name(&field.id).unwrap();
                        if field_name.as_ref() == "time" {
                            continue;
                        }
                        let py_field = PyDict::new(py);
                        py_field.set_item("name", field_name.as_ref()).unwrap();

                        match &field.value {
                            FieldData::String(s) => {
                                py_field.set_item("value", s.as_str()).unwrap();
                            }
                            FieldData::Integer(i) => {
                                py_field.set_item("value", i).unwrap();
                            }
                            FieldData::UInteger(u) => {
                                py_field.set_item("value", u).unwrap();
                            }
                            FieldData::Float(f) => {
                                py_field.set_item("value", f).unwrap();
                            }
                            FieldData::Boolean(b) => {
                                py_field.set_item("value", b).unwrap();
                            }
                            FieldData::Tag(t) => {
                                py_field.set_item("value", t.as_str()).unwrap();
                            }
                            FieldData::Key(k) => {
                                py_field.set_item("value", k.as_str()).unwrap();
                            }
                            FieldData::Timestamp(_) => {
                                // return an error, this shouldn't happen
                                return Err(PyValueError::new_err(
                                    "Timestamps should be in the time field",
                                ));
                            }
                        };

                        fields.push(py_field.unbind());
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

        let api = PyPluginCallApi {
            _schema: schema,
            _catalog: catalog,
            return_state: Default::default(),
        };
        let return_state = Arc::clone(&api.return_state);
        let local_api = api.into_pyobject(py)?;

        // turn args into an optional dict to pass into python
        let args = args.map(|args| {
            let dict = PyDict::new(py);
            for (key, value) in args {
                dict.set_item(key, value).unwrap();
            }
            dict
        });

        // run the code and get the python function to call
        py.run(&CString::new(code).unwrap(), Some(&globals), None)?;
        let py_func = py.eval(
            &CString::new(PROCESS_WRITES_CALL_SITE).unwrap(),
            Some(&globals),
            None,
        )?;
        py_func.call1((local_api, py_batches.unbind(), args))?;

        // swap with an empty return state to avoid cloning
        let empty_return_state = PluginReturnState::default();
        let ret = std::mem::replace(&mut *return_state.lock(), empty_return_state);

        Ok(ret)
    })
}

// Module initialization
#[pymodule]
fn influxdb3_py_api(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWriteBatch>()?;
    m.add_class::<PyWriteBatchIterator>()?;
    Ok(())
}
