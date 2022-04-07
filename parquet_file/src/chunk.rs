use crate::{
    metadata::{DecodedIoxParquetMetaData, IoxMetadata, IoxParquetMetaData},
    storage::Storage,
};
use data_types::{
    partition_metadata::{Statistics, TableSummary},
    timestamp::{TimestampMinMax, TimestampRange},
};
use data_types2::ParquetFile;
use datafusion::physical_plan::SendableRecordBatchStream;
use iox_object_store::{IoxObjectStore, ParquetFilePath};
use observability_deps::tracing::*;
use predicate::Predicate;
use schema::selection::Selection;
use schema::{Schema, TIME_COLUMN_NAME};
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeSet, mem, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table '{}' not found in chunk", table_name))]
    NamedTableNotFoundInChunk { table_name: String },

    #[snafu(display("Failed to read parquet: {}", source))]
    ReadParquet { source: crate::storage::Error },

    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(
        display("Cannot decode parquet metadata from {:?}: {}", path, source),
        visibility(pub)
    )]
    MetadataDecodeFailed {
        source: crate::metadata::Error,
        path: ParquetFilePath,
    },

    #[snafu(
        display("Cannot read schema from {:?}: {}", path, source),
        visibility(pub)
    )]
    SchemaReadFailed {
        source: crate::metadata::Error,
        path: ParquetFilePath,
    },

    #[snafu(
        display("Cannot read statistics from {:?}: {}", path, source),
        visibility(pub)
    )]
    StatisticsReadFailed {
        source: crate::metadata::Error,
        path: ParquetFilePath,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct ChunkMetrics {
    // Placeholder
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metric registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {}
    }

    pub fn new(_metrics: &metric::Registry) -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct ParquetChunk {
    /// Meta data of the table
    table_summary: Arc<TableSummary>,

    /// Schema that goes with this table's parquet file
    schema: Arc<Schema>,

    /// min/max time range of this table's parquet file
    /// (extracted from TableSummary), if known
    timestamp_min_max: Option<TimestampMinMax>,

    /// Persists the parquet file within a database's relative path
    iox_object_store: Arc<IoxObjectStore>,

    /// Path in the database's object store.
    path: ParquetFilePath,

    /// Size of the data, in object store
    file_size_bytes: usize,

    /// Parquet metadata that can be used checkpoint the catalog state.
    parquet_metadata: Arc<IoxParquetMetaData>,

    /// Number of rows
    rows: usize,

    #[allow(dead_code)]
    metrics: ChunkMetrics,
}

impl ParquetChunk {
    /// Creates new chunk from given parquet metadata.
    pub fn new(
        path: &ParquetFilePath,
        iox_object_store: Arc<IoxObjectStore>,
        file_size_bytes: usize,
        parquet_metadata: Arc<IoxParquetMetaData>,
        metrics: ChunkMetrics,
    ) -> Result<Self> {
        let decoded = parquet_metadata
            .decode()
            .context(MetadataDecodeFailedSnafu { path })?;
        let schema = decoded
            .read_schema()
            .context(SchemaReadFailedSnafu { path })?;
        let columns = decoded
            .read_statistics(&schema)
            .context(StatisticsReadFailedSnafu { path })?;
        let table_summary = TableSummary { columns };
        let rows = decoded.row_count();

        Ok(Self::new_from_parts(
            Arc::new(table_summary),
            schema,
            path,
            iox_object_store,
            file_size_bytes,
            parquet_metadata,
            rows,
            metrics,
        ))
    }

    /// Creates a new chunk from given parts w/o parsing anything from the provided parquet
    /// metadata.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_from_parts(
        table_summary: Arc<TableSummary>,
        schema: Arc<Schema>,
        path: &ParquetFilePath,
        iox_object_store: Arc<IoxObjectStore>,
        file_size_bytes: usize,
        parquet_metadata: Arc<IoxParquetMetaData>,
        rows: usize,
        metrics: ChunkMetrics,
    ) -> Self {
        let timestamp_min_max = extract_range(&table_summary);

        Self {
            table_summary,
            schema,
            timestamp_min_max,
            iox_object_store,
            path: path.into(),
            file_size_bytes,
            parquet_metadata,
            rows,
            metrics,
        }
    }

    /// Return object store path for this chunk
    pub fn path(&self) -> &ParquetFilePath {
        &self.path
    }

    /// Returns the summary statistics for this chunk
    pub fn table_summary(&self) -> &Arc<TableSummary> {
        &self.table_summary
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.table_summary.size()
            + mem::size_of_val(&self.schema.as_ref())
            + mem::size_of_val(&self.path)
            + self.parquet_metadata.size()
    }

    /// Infallably return the full schema (for all columns) for this chunk
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    // Return true if this chunk contains values within the time
    // range, or if the range is `None`.
    pub fn has_timerange(&self, timestamp_range: Option<&TimestampRange>) -> bool {
        match (self.timestamp_min_max, timestamp_range) {
            (Some(timestamp_min_max), Some(timestamp_range)) => {
                timestamp_min_max.overlaps(*timestamp_range)
            }
            // If this chunk doesn't have a time column it can't match
            (None, Some(_)) => false,
            // If there no range specified,
            (_, None) => true,
        }
    }

    // Return the columns names that belong to the given column selection
    pub fn column_names(&self, selection: Selection<'_>) -> Option<BTreeSet<String>> {
        let fields = self.schema.inner().fields().iter();

        Some(match selection {
            Selection::Some(cols) => fields
                .filter_map(|x| {
                    if cols.contains(&x.name().as_str()) {
                        Some(x.name().clone())
                    } else {
                        None
                    }
                })
                .collect(),
            Selection::All => fields.map(|x| x.name().clone()).collect(),
        })
    }

    /// Return stream of data read from parquet file
    pub fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(path=?self.path, "fetching parquet data for filtered read");
        Storage::read_filter(
            predicate,
            selection,
            Arc::clone(&self.schema.as_arrow()),
            self.path.clone(),
            Arc::clone(&self.iox_object_store),
        )
        .context(ReadParquetSnafu)
    }

    /// The total number of rows in all row groups in this chunk.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Size of the parquet file in object store
    pub fn file_size_bytes(&self) -> usize {
        self.file_size_bytes
    }

    /// Parquet metadata from the underlying file.
    pub fn parquet_metadata(&self) -> Arc<IoxParquetMetaData> {
        Arc::clone(&self.parquet_metadata)
    }
}

/// Extracts min/max values of the timestamp column, from the TableSummary, if possible
fn extract_range(table_summary: &TableSummary) -> Option<TimestampMinMax> {
    table_summary.column(TIME_COLUMN_NAME).and_then(|c| {
        if let Statistics::I64(s) = &c.stats {
            if let (Some(min), Some(max)) = (s.min, s.max) {
                return Some(TimestampMinMax::new(min, max));
            }
        }
        None
    })
}
// Parquet file with decoded metadata.
#[derive(Debug)]
pub struct DecodedParquetFile {
    pub parquet_file: ParquetFile,
    pub parquet_metadata: Arc<IoxParquetMetaData>,
    pub decoded_metadata: DecodedIoxParquetMetaData,
    pub iox_metadata: IoxMetadata,
}

impl DecodedParquetFile {
    pub fn new(parquet_file: ParquetFile) -> Self {
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(
            parquet_file.parquet_metadata.clone(),
        ));
        let decoded_metadata = parquet_metadata.decode().expect("parquet metadata broken");
        let iox_metadata = decoded_metadata
            .read_iox_metadata_new()
            .expect("cannot read IOx metadata from parquet MD");

        Self {
            parquet_file,
            parquet_metadata,
            decoded_metadata,
            iox_metadata,
        }
    }
}

/// Create parquet chunk.
pub fn new_parquet_chunk(
    decoded_parquet_file: &DecodedParquetFile,
    metrics: ChunkMetrics,
    iox_object_store: Arc<IoxObjectStore>,
) -> ParquetChunk {
    let iox_metadata = &decoded_parquet_file.iox_metadata;
    let path = ParquetFilePath::new_new_gen(
        iox_metadata.namespace_id,
        iox_metadata.table_id,
        iox_metadata.sequencer_id,
        iox_metadata.partition_id,
        iox_metadata.object_store_id,
    );

    let parquet_file = &decoded_parquet_file.parquet_file;
    let file_size_bytes = parquet_file.file_size_bytes as usize;

    ParquetChunk::new(
        &path,
        iox_object_store,
        file_size_bytes,
        Arc::clone(&decoded_parquet_file.parquet_metadata),
        metrics,
    )
    .expect("cannot create chunk")
}
