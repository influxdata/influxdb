use crate::logging::{LogLevel, ProcessingEngineLog};
use crate::ExecutePluginError;
use anyhow::Context;
use arrow_array::types::Int32Type;
use arrow_array::{
    BooleanArray, DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::DataType;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use hashbrown::HashMap;
use humantime::format_duration;
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_id::TableId;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_sys_events::SysEventStore;
use influxdb3_wal::{FieldData, WriteBatch};
use iox_query_params::StatementParams;
use iox_time::TimeProvider;
use observability_deps::tracing::{error, info, warn};
use parking_lot::Mutex;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::types::{PyBytes, PyDateTime, PyDict, PyList, PyTuple};
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
    logger: Option<ProcessingEngineLogger>,
}

#[derive(Debug, Clone)]
pub struct ProcessingEngineLogger {
    sys_event_store: Arc<SysEventStore>,
    trigger_name: Arc<str>,
}

impl ProcessingEngineLogger {
    pub fn new(sys_event_store: Arc<SysEventStore>, trigger_name: impl Into<Arc<str>>) -> Self {
        Self {
            sys_event_store,
            trigger_name: trigger_name.into(),
        }
    }

    pub fn log(&self, log_level: LogLevel, log_line: impl Into<String>) {
        self.sys_event_store.record(ProcessingEngineLog::new(
            self.sys_event_store.time_provider().now(),
            log_level,
            Arc::clone(&self.trigger_name),
            log_line.into(),
        ))
    }
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
        let line = self.log_args_to_string(args)?;

        info!("processing engine: {}", line);
        self.write_to_logger(LogLevel::Info, line.clone());
        self.return_state.lock().log_lines.push(LogLine::Info(line));
        Ok(())
    }

    #[pyo3(signature = (*args))]
    fn warn(&self, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let line = self.log_args_to_string(args)?;

        warn!("processing engine: {}", line);
        self.write_to_logger(LogLevel::Warn, line.clone());
        self.return_state
            .lock()
            .log_lines
            .push(LogLine::Warn(line.to_string()));
        Ok(())
    }

    #[pyo3(signature = (*args))]
    fn error(&self, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let line = self.log_args_to_string(args)?;

        error!("processing engine: {}", line);
        self.write_to_logger(LogLevel::Error, line.clone());
        self.return_state
            .lock()
            .log_lines
            .push(LogLine::Error(line.to_string()));
        Ok(())
    }

    fn log_args_to_string(&self, args: &Bound<'_, PyTuple>) -> PyResult<String> {
        let line = args
            .try_iter()?
            .map(|arg| arg?.str()?.extract::<String>())
            .collect::<Result<Vec<String>, _>>()?
            .join(" ");
        Ok(line)
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

impl PyPluginCallApi {
    fn write_to_logger(&self, level: LogLevel, log_line: String) {
        if let Some(logger) = &self.logger {
            logger.log(level, log_line);
        }
    }
}

// constant for the process writes call site string
const PROCESS_WRITES_CALL_SITE: &str = "process_writes";

const PROCESS_SCHEDULED_CALL_SITE: &str = "process_scheduled_call";

const PROCESS_REQUEST_CALL_SITE: &str = "process_request";

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

fn args_to_py_object<'py>(
    py: Python<'py>,
    args: &Option<HashMap<String, String>>,
) -> Option<Bound<'py, PyDict>> {
    args.as_ref().map(|args| map_to_py_object(py, args))
}

fn map_to_py_object<'py>(py: Python<'py>, map: &HashMap<String, String>) -> Bound<'py, PyDict> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        dict.set_item(key, value).unwrap();
    }
    dict
}

pub fn execute_python_with_batch(
    code: &str,
    write_batch: &WriteBatch,
    schema: Arc<DatabaseSchema>,
    query_executor: Arc<dyn QueryExecutor>,
    logger: Option<ProcessingEngineLogger>,
    table_filter: Option<TableId>,
    args: &Option<HashMap<String, String>>,
) -> Result<PluginReturnState, ExecutePluginError> {
    let start_time = if let Some(logger) = &logger {
        logger.log(
            LogLevel::Info,
            "starting execution of wal plugin.".to_string(),
        );
        Some(logger.sys_event_store.time_provider().now())
    } else {
        None
    };
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
            logger: logger.clone(),
            return_state: Default::default(),
        };
        let return_state = Arc::clone(&api.return_state);
        let local_api = api.into_pyobject(py).map_err(anyhow::Error::from)?;

        // turn args into an optional dict to pass into python
        let args = args_to_py_object(py, args);

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

        if let Some(logger) = &logger {
            let runtime = logger
                .sys_event_store
                .time_provider()
                .now()
                .checked_duration_since(start_time.unwrap());
            logger.log(
                LogLevel::Info,
                format!(
                    "finished execution in {}",
                    format_duration(runtime.unwrap_or_default())
                ),
            );
        }

        Ok(ret)
    })
}

pub fn execute_schedule_trigger(
    code: &str,
    schedule_time: DateTime<Utc>,
    schema: Arc<DatabaseSchema>,
    query_executor: Arc<dyn QueryExecutor>,
    logger: Option<ProcessingEngineLogger>,
    args: &Option<HashMap<String, String>>,
) -> Result<PluginReturnState, ExecutePluginError> {
    let start_time = if let Some(logger) = &logger {
        logger.log(
            LogLevel::Info,
            format!("starting execution with scheduled time {}", schedule_time),
        );
        Some(logger.sys_event_store.time_provider().now())
    } else {
        None
    };
    Python::with_gil(|py| {
        // import the LineBuilder for use in the python code
        let globals = PyDict::new(py);

        let py_datetime = PyDateTime::from_timestamp(py, schedule_time.timestamp() as f64, None)
            .map_err(|e| {
                anyhow::Error::new(e).context("error converting the schedule time to Python time")
            })?;

        py.run(
            &CString::new(LINE_BUILDER_CODE).unwrap(),
            Some(&globals),
            None,
        )
        .map_err(|e| anyhow::Error::new(e).context("failed to eval the LineBuilder API code"))?;

        let api = PyPluginCallApi {
            db_schema: schema,
            query_executor,
            logger: logger.clone(),
            return_state: Default::default(),
        };
        let return_state = Arc::clone(&api.return_state);
        let local_api = api.into_pyobject(py).map_err(anyhow::Error::from)?;

        // turn args into an optional dict to pass into python
        let args = args_to_py_object(py, args);

        // run the code and get the python function to call
        py.run(&CString::new(code).unwrap(), Some(&globals), None)
            .map_err(anyhow::Error::from)?;

        let py_func = py
            .eval(
                &CString::new(PROCESS_SCHEDULED_CALL_SITE).unwrap(),
                Some(&globals),
                None,
            )
            .map_err(|_| ExecutePluginError::MissingProcessScheduledCallFunction)?;

        py_func
            .call1((local_api, py_datetime, args))
            .map_err(anyhow::Error::from)?;

        // swap with an empty return state to avoid cloning
        let empty_return_state = PluginReturnState::default();
        let ret = std::mem::replace(&mut *return_state.lock(), empty_return_state);
        if let Some(logger) = &logger {
            let runtime = logger
                .sys_event_store
                .time_provider()
                .now()
                .checked_duration_since(start_time.unwrap());
            logger.log(
                LogLevel::Info,
                format!(
                    "finished execution in {}",
                    format_duration(runtime.unwrap_or_default())
                ),
            );
        }
        Ok(ret)
    })
}

#[allow(clippy::too_many_arguments)]
pub fn execute_request_trigger(
    code: &str,
    db_schema: Arc<DatabaseSchema>,
    query_executor: Arc<dyn QueryExecutor>,
    logger: Option<ProcessingEngineLogger>,
    args: &Option<HashMap<String, String>>,
    query_params: HashMap<String, String>,
    request_headers: HashMap<String, String>,
    request_body: Bytes,
) -> Result<(u16, HashMap<String, String>, String, PluginReturnState), ExecutePluginError> {
    let start_time = if let Some(logger) = &logger {
        logger.log(
            LogLevel::Info,
            "starting execution of request plugin.".to_string(),
        );
        Some(logger.sys_event_store.time_provider().now())
    } else {
        None
    };
    Python::with_gil(|py| {
        // import the LineBuilder for use in the python code
        let globals = PyDict::new(py);

        py.run(
            &CString::new(LINE_BUILDER_CODE).unwrap(),
            Some(&globals),
            None,
        )
        .map_err(|e| anyhow::Error::new(e).context("failed to eval the LineBuilder API code"))?;

        let api = PyPluginCallApi {
            db_schema,
            query_executor,
            logger: logger.clone(),
            return_state: Default::default(),
        };
        let return_state = Arc::clone(&api.return_state);
        let local_api = api.into_pyobject(py).map_err(anyhow::Error::from)?;

        // turn args into an optional dict to pass into python
        let args = args_to_py_object(py, args);

        let query_params = map_to_py_object(py, &query_params);
        let request_params = map_to_py_object(py, &request_headers);

        // run the code and get the python function to call
        py.run(&CString::new(code).unwrap(), Some(&globals), None)
            .map_err(anyhow::Error::from)?;

        let py_func = py
            .eval(
                &CString::new(PROCESS_REQUEST_CALL_SITE).unwrap(),
                Some(&globals),
                None,
            )
            .map_err(|_| ExecutePluginError::MissingProcessRequestFunction)?;

        // convert the body bytes into python bytes blob
        let request_body = PyBytes::new(py, &request_body[..]);

        // get the result from calling the python function
        let result = py_func
            .call1((local_api, query_params, request_params, request_body, args))
            .map_err(|e| anyhow::anyhow!("Python function call failed: {}", e))?;

        // the return from the process_request function should be a tuple of (status_code, response_headers, body)
        let response_code: i64 = result.get_item(0).context("Python request function didn't return a tuple of (status_code, response_headers, body)")?.extract().context("unable to convert first tuple element from Python request function return to integer")?;
        let headers_binding = result.get_item(1).context("Python request function didn't return a tuple of (status_code, response_headers, body)")?;
        let py_headers_dict = headers_binding
            .downcast::<PyDict>()
            .map_err(|e| anyhow::anyhow!("Failed to downcast to PyDict: {}", e))?;
        let body_binding = result.get_item(2).context("Python request function didn't return a tuple of (status_code, response_headers, body)")?;
        let response_body: &str = body_binding.extract().context(
            "unable to convert the third tuple element from Python request function to a string",
        )?;

        // Then convert the dict to HashMap
        let response_headers: std::collections::HashMap<String, String> = py_headers_dict
            .extract()
            .map_err(|e| anyhow::anyhow!("error converting response headers into hashmap {}", e))?;

        // convert the returned i64 to a u16 or it's a 500
        let response_code: u16 = response_code.try_into().unwrap_or_else(|_| {
            warn!(
                "Invalid response code from Python request trigger: {}",
                response_code
            );

            500
        });

        let response_headers: HashMap<String, String> = response_headers.into_iter().collect();

        // swap with an empty return state to avoid cloning
        let empty_return_state = PluginReturnState::default();
        let ret = std::mem::replace(&mut *return_state.lock(), empty_return_state);

        if let Some(logger) = &logger {
            let runtime = logger
                .sys_event_store
                .time_provider()
                .now()
                .checked_duration_since(start_time.unwrap());
            logger.log(
                LogLevel::Info,
                format!(
                    "finished execution in {}",
                    format_duration(runtime.unwrap_or_default())
                ),
            )
        }

        Ok((
            response_code,
            response_headers,
            response_body.to_string(),
            ret,
        ))
    })
}

// Module initialization
#[pymodule]
fn influxdb3_py_api(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
