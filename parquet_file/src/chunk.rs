//! A metadata summary of a Parquet file in object storage, with the ability to
//! download & execute a scan.

use crate::{
    storage::{ParquetExecInput, ParquetStorage},
    ParquetFilePath,
};
use data_types::{ParquetFile, TimestampMinMax};
use schema::{Projection, Schema};
use std::{collections::BTreeSet, mem, sync::Arc};
use uuid::Uuid;

/// A abstract representation of a Parquet file in object storage, with
/// associated metadata.
#[derive(Debug)]
pub struct ParquetChunk {
    /// Parquet file.
    parquet_file: Arc<ParquetFile>,

    /// Schema that goes with this table's parquet file
    schema: Schema,

    /// Persists the parquet file within a namespace's relative path
    store: ParquetStorage,
}

impl ParquetChunk {
    /// Create parquet chunk.
    pub fn new(parquet_file: Arc<ParquetFile>, schema: Schema, store: ParquetStorage) -> Self {
        Self {
            parquet_file,
            schema,
            store,
        }
    }

    /// Store that contains this file.
    pub fn store(&self) -> &ParquetStorage {
        &self.store
    }

    /// Return raw parquet file metadata.
    pub fn parquet_file(&self) -> &Arc<ParquetFile> {
        &self.parquet_file
    }

    /// Return object store id
    pub fn object_store_id(&self) -> Uuid {
        self.parquet_file.object_store_id
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        mem::size_of_val(self) + self.parquet_file.size() - mem::size_of_val(&self.parquet_file)
    }

    /// Infallibly return the full schema (for all columns) for this chunk
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Return the columns names that belong to the given column selection
    pub fn column_names(&self, selection: Projection<'_>) -> Option<BTreeSet<String>> {
        let fields = self.schema.inner().fields().iter();

        Some(match selection {
            Projection::Some(cols) => fields
                .filter_map(|x| {
                    if cols.contains(&x.name().as_str()) {
                        Some(x.name().clone())
                    } else {
                        None
                    }
                })
                .collect(),
            Projection::All => fields.map(|x| x.name().clone()).collect(),
        })
    }

    /// Return stream of data read from parquet file
    /// Inputs for [`ParquetExec`].
    ///
    /// See [`ParquetExecInput`] for more information.
    ///
    /// [`ParquetExec`]: datafusion::physical_plan::file_format::ParquetExec
    pub fn parquet_exec_input(&self) -> ParquetExecInput {
        let path: ParquetFilePath = self.parquet_file.as_ref().into();
        self.store.parquet_exec_input(&path, self.file_size_bytes())
    }

    /// The total number of rows in all row groups in this chunk.
    pub fn rows(&self) -> usize {
        self.parquet_file.row_count as usize
    }

    /// Size of the parquet file in object store
    pub fn file_size_bytes(&self) -> usize {
        self.parquet_file.file_size_bytes as usize
    }

    /// return time range
    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.parquet_file.min_time.get(),
            max: self.parquet_file.max_time.get(),
        }
    }
}
