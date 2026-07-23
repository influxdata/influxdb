//! Conversion of WAL data to Python format for plugin execution.
//!
//! This module provides the `ToPythonTableBatches` trait that unifies conversion of
//! both parquet-based (`WriteBatch`) WAL
//! flush data to Python table batches.

use anyhow::{Context, ensure};
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, DictionaryArray, RecordBatch};
use arrow_schema::DataType;
use hashbrown::HashMap;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{PyDict, PyList, PyString};
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyResult, Python};

/// Convert Arrow arrays from [`RecordBatch`]es to Python list of dicts.
///
/// The record batches are required to have the same schema.
pub(crate) fn record_batches_to_py_rows<'py>(
    py: Python<'py>,
    batches: &[RecordBatch],
) -> Result<Bound<'py, PyList>, anyhow::Error> {
    // Pre-create Python strings for field/tag names once for all batches;
    // schema must be the same across batches.
    let Some(first_batch) = batches.first() else {
        return Ok(PyList::empty(py));
    };
    let field_names: Vec<Bound<'_, PyString>> = first_batch
        .schema()
        .fields()
        .iter()
        .map(|f| PyString::new(py, f.name().as_str()))
        .collect();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut rows: Vec<Py<PyAny>> = Vec::with_capacity(total_rows);

    for batch in batches {
        ensure!(
            batch.num_columns() == field_names.len(),
            "unexpected batch schema mismatch: expected {} columns, got {}",
            field_names.len(),
            batch.num_columns()
        );
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let row = PyDict::new(py);
            for (col_idx, field_name) in field_names.iter().enumerate() {
                let array = batch.column(col_idx);
                let value = extract_arrow_value_to_py(py, array, row_idx)?;
                row.set_item(field_name, value).context("set dict item")?;
            }
            rows.push(row.into());
        }
    }

    let list = PyList::new(py, rows)?;
    Ok(list)
}

/// Extract a value from an Arrow array at the given index and convert to Python.
fn extract_arrow_value_to_py<'py>(
    py: Python<'py>,
    array: &ArrayRef,
    index: usize,
) -> Result<Py<PyAny>, anyhow::Error> {
    use arrow_array::cast::AsArray;

    // Handle null values
    if array.is_null(index) {
        return Ok(py.None());
    }

    let data_type = array.data_type();
    let value = match data_type {
        DataType::Int64 => {
            let arr = array.as_primitive::<arrow_array::types::Int64Type>();
            arr.value(index).into_pyobject(py)?.into_any().unbind()
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<arrow_array::types::UInt64Type>();
            arr.value(index).into_pyobject(py)?.into_any().unbind()
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<arrow_array::types::Float64Type>();
            arr.value(index).into_pyobject(py)?.into_any().unbind()
        }
        DataType::Boolean => {
            let arr = array.as_boolean();
            arr.value(index)
                .into_pyobject(py)?
                .to_owned()
                .into_any()
                .unbind()
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            arr.value(index).into_pyobject(py)?.into_any().unbind()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            arr.value(index).into_pyobject(py)?.into_any().unbind()
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            let arr = array.as_primitive::<arrow_array::types::TimestampNanosecondType>();
            arr.value(index).into_pyobject(py)?.into_any().unbind()
        }
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            // Dictionary-encoded strings (common for tags)
            let dict_arr = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .context("failed to downcast dictionary array")?;
            let values = dict_arr.values().as_string::<i32>();
            let key = dict_arr.keys().value(index) as usize;
            values.value(key).into_pyobject(py)?.into_any().unbind()
        }
        _ => {
            anyhow::bail!(
                "unsupported Arrow type for Python conversion: {:?}. \
                Supported types: Int64, UInt64, Float64, Boolean, Utf8, LargeUtf8, Timestamp\
                (Nanosecond), Dictionary(Int32, Utf8).",
                data_type
            );
        }
    };

    Ok(value)
}

pub(crate) fn args_to_py_object<'py>(
    py: Python<'py>,
    args: &Option<HashMap<String, String>>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    args.as_ref()
        .map(|args| map_to_py_object(py, args))
        .transpose()
}

pub(crate) fn map_to_py_object<'py>(
    py: Python<'py>,
    map: &HashMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        dict.set_item(key, value)?;
    }
    Ok(dict)
}
