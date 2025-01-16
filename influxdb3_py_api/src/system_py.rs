use crate::ExecutePluginError;
use anyhow::Context;
use arrow_array::types::Int32Type;
use arrow_array::{
    BooleanArray, DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::DataType;
use futures::TryStreamExt;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_id::TableId;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_wal::{FieldData, WriteBatch};
use iox_query_params::StatementParams;
use observability_deps::tracing::{error, info, warn};
use parking_lot::Mutex;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{
    create_exception, pyclass, pymethods, pymodule, Bound, IntoPyObject, Py, PyAny, PyObject,
    PyResult, Python,
};
use std::ffi::CString;
use std::sync::Arc;

create_exception!(influxdb3_py_api, QueryError, PyException);

#[pyclass]
#[derive(Debug)]
struct PyPluginCallApi {
    db_schema: Arc<DatabaseSchema>,
    query_executor: Arc<dyn QueryExecutor>,
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
    #[pyo3(signature = (*args))]
    fn info(&self, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let line = args
            .try_iter()?
            .map(|arg| arg?.str()?.extract::<String>())
            .collect::<Result<Vec<String>, _>>()?
            .join(" ");

        info!("processing engine: {}", line);
        self.return_state.lock().log_lines.push(LogLine::Info(line));
        Ok(())
    }

    #[pyo3(signature = (*args))]
    fn warn(&self, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let line = args
            .try_iter()?
            .map(|arg| arg?.str()?.extract::<String>())
            .collect::<Result<Vec<String>, _>>()?
            .join(" ");

        warn!("processing engine: {}", line);
        self.return_state
            .lock()
            .log_lines
            .push(LogLine::Warn(line.to_string()));
        Ok(())
    }

    #[pyo3(signature = (*args))]
    fn error(&self, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let line = args
            .try_iter()?
            .map(|arg| arg?.str()?.extract::<String>())
            .collect::<Result<Vec<String>, _>>()?
            .join(" ");

        error!("processing engine: {}", line);
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

    #[pyo3(signature = (query, args=None))]
    fn query(
        &self,
        query: String,
        args: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Py<PyList>> {
        let query_executor = Arc::clone(&self.query_executor);
        let db_schema_name = Arc::clone(&self.db_schema.name);

        let params = args.map(|args| {
            let mut params = StatementParams::new();
            for (key, value) in args {
                params.insert(key, value);
            }
            params
        });

        // Spawn the async task
        let handle = tokio::spawn(async move {
            let res = query_executor
                .query_sql(db_schema_name.as_ref(), &query, params, None, None)
                .await
                .map_err(|e| {
                    QueryError::new_err(format!("error: {} executing query: {}", e, query))
                })?;

            res.try_collect().await.map_err(|e| {
                QueryError::new_err(format!("error: {} executing query: {}", e, query))
            })
        });

        // Block the current thread until the async task completes
        let res =
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(handle));

        let res = res.map_err(|e| QueryError::new_err(format!("join error: {}", e)))?;

        let batches: Vec<RecordBatch> = res?;

        Python::with_gil(|py| {
            let mut rows: Vec<PyObject> = Vec::new();

            for batch in batches {
                let num_rows = batch.num_rows();
                let schema = batch.schema();

                for row_idx in 0..num_rows {
                    let row = PyDict::new(py);
                    for col_idx in 0..schema.fields().len() {
                        let field = schema.field(col_idx);
                        let field_name = field.name().as_str();

                        let array = batch.column(col_idx);

                        match array.data_type() {
                            DataType::Int64 => {
                                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                row.set_item(field_name, array.value(row_idx))?;
                            }
                            DataType::UInt64 => {
                                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                                row.set_item(field_name, array.value(row_idx))?;
                            }
                            DataType::Float64 => {
                                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                row.set_item(field_name, array.value(row_idx))?;
                            }
                            DataType::Utf8 => {
                                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                                row.set_item(field_name, array.value(row_idx))?;
                            }
                            DataType::Boolean => {
                                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                                row.set_item(field_name, array.value(row_idx))?;
                            }
                            DataType::Timestamp(_, _) => {
                                let array = array
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .unwrap();
                                row.set_item(field_name, array.value(row_idx))?;
                            }
                            DataType::Dictionary(_, _) => {
                                let col = array
                                    .as_any()
                                    .downcast_ref::<DictionaryArray<Int32Type>>()
                                    .expect("unexpected datatype");

                                let values = col.values();
                                let values = values
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .expect("unexpected datatype");

                                let val = values.value(row_idx).to_string();
                                row.set_item(field_name, val)?;
                            }
                            _ => {
                                return Err(PyValueError::new_err(format!(
                                    "Unsupported data type: {:?}",
                                    array.data_type()
                                )))
                            }
                        }
                    }
                    rows.push(row.into());
                }
            }

            let list = PyList::new(py, rows)?.unbind();
            Ok(list)
        })
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

class InvalidLineError(InfluxDBError):
    """Raised when a line protocol string is invalid"""
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

    def uint64_field(self, key: str, value: int) -> 'LineBuilder':
        """Add an unsigned integer field to the line protocol."""
        self._validate_key(key, "field")
        if value < 0:
            raise ValueError(f"uint64 field '{key}' cannot be negative")
        self.fields[key] = f"{value}u"
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

    def bool_field(self, key: str, value: bool) -> 'LineBuilder':
        """Add a boolean field to the line protocol."""
        self._validate_key(key, "field")
        self.fields[key] = 't' if value else 'f'
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
            raise InvalidLineError(f"At least one field is required: {line}")

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
    query_executor: Arc<dyn QueryExecutor>,
    table_filter: Option<TableId>,
    args: &Option<HashMap<String, String>>,
) -> Result<PluginReturnState, ExecutePluginError> {
    Python::with_gil(|py| {
        // import the LineBuilder for use in the python code
        let globals = PyDict::new(py);

        py.run(
            &CString::new(LINE_BUILDER_CODE).unwrap(),
            Some(&globals),
            None,
        )
        .map_err(|e| anyhow::Error::new(e).context("failed to eval the LineBuilder API code"))?;

        // convert the write batch into a python object
        let mut table_batches = Vec::with_capacity(write_batch.table_chunks.len());

        for (table_id, table_chunks) in &write_batch.table_chunks {
            if let Some(table_filter) = table_filter {
                if table_id != &table_filter {
                    continue;
                }
            }
            let table_def = schema.tables.get(table_id).context("table not found")?;

            let dict = PyDict::new(py);
            dict.set_item("table_name", table_def.table_name.as_ref())
                .context("failed to set table_name")?;

            let mut rows: Vec<PyObject> = Vec::new();
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
                            FieldData::Key(k) => {
                                py_row
                                    .set_item(field_name.as_ref(), k.as_str())
                                    .context("failed to set key field")?;
                            }
                            FieldData::Timestamp(t) => {
                                py_row
                                    .set_item(field_name.as_ref(), t)
                                    .context("failed to set timestamp")?;
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

        let py_batches =
            PyList::new(py, table_batches).context("failed to create table_batches list")?;

        let api = PyPluginCallApi {
            db_schema: schema,
            query_executor,
            return_state: Default::default(),
        };
        let return_state = Arc::clone(&api.return_state);
        let local_api = api.into_pyobject(py).map_err(anyhow::Error::from)?;

        // turn args into an optional dict to pass into python
        let args = args.as_ref().map(|args| {
            let dict = PyDict::new(py);
            for (key, value) in args {
                dict.set_item(key, value).unwrap();
            }
            dict
        });

        // run the code and get the python function to call
        py.run(&CString::new(code).unwrap(), Some(&globals), None)
            .map_err(anyhow::Error::from)?;
        let py_func = py
            .eval(
                &CString::new(PROCESS_WRITES_CALL_SITE).unwrap(),
                Some(&globals),
                None,
            )
            .map_err(|_| ExecutePluginError::MissingProcessWritesFunction)?;

        py_func
            .call1((local_api, py_batches.unbind(), args))
            .map_err(anyhow::Error::from)?;

        // swap with an empty return state to avoid cloning
        let empty_return_state = PluginReturnState::default();
        let ret = std::mem::replace(&mut *return_state.lock(), empty_return_state);

        Ok(ret)
    })
}

// Module initialization
#[pymodule]
fn influxdb3_py_api(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
