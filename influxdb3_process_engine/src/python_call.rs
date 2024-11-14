use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::pyarrow::PyArrowType;
use influxdb3_id::TableId;
use pyo3::prelude::*;
use pyo3::{PyResult, Python};
use serde::Deserialize;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PythonCall {
    pub call_name: String,
    pub code: String,
    pub function_name: String,
}

impl PythonCall {
    pub fn new(call_name: String, code: String, function_name: String) -> Self {
        Self {
            call_name,
            code,
            function_name,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct ProcessEngineTrigger {
    pub source_table: TableId,
    pub trigger_table: TableId,
    pub trigger_name: String,
    pub trigger_type: TriggerType,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Deserialize)]
pub enum TriggerType {
    OnRead,
}

impl PythonCall {
    pub fn call(&self, input_batch: &RecordBatch) -> PyResult<RecordBatch> {
        Python::with_gil(|py| {
            py.run_bound(self.code.as_str(), None, None)?;
            let py_batch: PyArrowType<_> = PyArrowType(input_batch.clone());
            let py_func = py.eval_bound(self.function_name.as_str(), None, None)?;
            let result = py_func.call1((py_batch,))?;
            let updated_batch: PyArrowType<RecordBatch> = result.extract()?;
            Ok(updated_batch.0)
        })
    }
}
