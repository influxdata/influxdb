use std::sync::Arc;

use anyhow::Context;
use arrow_array::RecordBatch;
use influxdb3_id::TableId;
use pyo3::{
    Bound, Python,
    types::{PyDict, PyDictMethods, PyList},
};

use crate::py_conversion::record_batches_to_py_rows;

#[derive(Debug, Clone)]
pub struct WalFlushElement {
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub data: Vec<RecordBatch>,
}

pub(crate) fn wal_flush_to_py<'py>(
    flush: &[WalFlushElement],
    py: Python<'py>,
    table_filter: Option<TableId>,
) -> Result<Bound<'py, PyList>, anyhow::Error> {
    let mut table_batches = Vec::with_capacity(flush.len());

    for element in flush {
        if let Some(filter_id) = table_filter
            && element.table_id != filter_id
        {
            continue;
        }

        let dict = PyDict::new(py);
        dict.set_item("table_name", element.table_name.as_ref())
            .context("failed to set table_name")?;

        let rows = record_batches_to_py_rows(py, &element.data)?;
        dict.set_item("rows", rows.unbind())
            .context("failed to set rows")?;

        table_batches.push(dict);
    }

    PyList::new(py, table_batches).context("failed to create table_batches list")
}
