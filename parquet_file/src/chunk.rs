//! A metadata summary of a Parquet file in object storage, with the ability to
//! download & execute a scan.

use crate::{
    metadata::{DecodedIoxParquetMetaData, IoxMetadata, IoxParquetMetaData},
    storage::ParquetStorage,
};
use data_types::{
    ParquetFile, ParquetFileId, ParquetFileWithMetadata, PartitionId, SequenceNumber, SequencerId,
    TableId, TableSummary, TimestampMinMax, TimestampRange,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use predicate::Predicate;
use schema::{selection::Selection, sort::SortKey, Schema};
use std::{collections::BTreeSet, mem, sync::Arc};

#[derive(Debug)]
#[allow(missing_copy_implementations, missing_docs)]
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

    /// This constructor builds nothing.
    pub fn new(_metrics: &metric::Registry) -> Self {
        Self {}
    }
}

/// A abstract representation of a Parquet file in object storage, with
/// associated metadata.
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
    store: ParquetStorage,

    /// Size of the data, in object store
    file_size_bytes: usize,

    /// Parquet metadata that can be used checkpoint the catalog state.
    parquet_metadata: Arc<IoxParquetMetaData>,

    /// The [`IoxMetadata`] data from the [`DecodedParquetFile`].
    iox_metadata: IoxMetadata,

    /// Number of rows
    rows: usize,

    #[allow(dead_code)]
    metrics: ChunkMetrics,
}

impl ParquetChunk {
    /// Create parquet chunk.
    pub fn new(
        decoded_parquet_file: &DecodedParquetFile,
        metrics: ChunkMetrics,
        store: ParquetStorage,
    ) -> Self {
        let decoded = decoded_parquet_file
            .parquet_metadata
            .as_ref()
            .decode()
            .unwrap();
        let schema = decoded.read_schema().unwrap();
        let columns = decoded.read_statistics(&schema).unwrap();
        let table_summary = TableSummary { columns };
        let rows = decoded.row_count();
        let timestamp_min_max = table_summary.time_range();
        let file_size_bytes = decoded_parquet_file.parquet_file.file_size_bytes as usize;

        Self {
            table_summary: Arc::new(table_summary),
            schema,
            timestamp_min_max,
            store,
            file_size_bytes,
            parquet_metadata: Arc::clone(&decoded_parquet_file.parquet_metadata),
            iox_metadata: decoded_parquet_file.iox_metadata.clone(),
            rows,
            metrics,
        }
    }

    /// Returns the summary statistics for this chunk
    pub fn table_summary(&self) -> &Arc<TableSummary> {
        &self.table_summary
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        mem::size_of_val(self)
            + self.table_summary.size()
            + mem::size_of_val(&self.schema.as_ref())
            + mem::size_of_val(&self.iox_metadata)
            + self.parquet_metadata.size()
    }

    /// Infallably return the full schema (for all columns) for this chunk
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Return true if this chunk contains values within the time range, or if
    /// the range is `None`.
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

    /// Return the columns names that belong to the given column selection
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
    ) -> Result<SendableRecordBatchStream, crate::storage::ReadError> {
        self.store.read_filter(
            predicate,
            selection,
            Arc::clone(&self.schema.as_arrow()),
            &self.iox_metadata,
        )
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

    /// return time range
    pub fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        self.timestamp_min_max
    }
}

/// Parquet file with decoded metadata.
#[derive(Debug)]
#[allow(missing_docs)]
pub struct DecodedParquetFile {
    pub parquet_file: ParquetFile,
    pub parquet_metadata: Arc<IoxParquetMetaData>,
    pub decoded_metadata: DecodedIoxParquetMetaData,
    pub iox_metadata: IoxMetadata,
}

impl DecodedParquetFile {
    /// initialise a [`DecodedParquetFile`] from the provided file & metadata.
    pub fn new(parquet_file_with_metadata: ParquetFileWithMetadata) -> Self {
        let (parquet_file, parquet_metadata) = parquet_file_with_metadata.split_off_metadata();
        let parquet_metadata = Arc::new(IoxParquetMetaData::from_thrift_bytes(parquet_metadata));
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

    /// The IOx schema from the decoded IOx parquet metadata
    pub fn schema(&self) -> Arc<Schema> {
        self.decoded_metadata.read_schema().unwrap()
    }

    /// The IOx parquet file ID
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.parquet_file.id
    }

    /// The IOx partition ID
    pub fn partition_id(&self) -> PartitionId {
        self.parquet_file.partition_id
    }

    /// The IOx sequencer ID
    pub fn sequencer_id(&self) -> SequencerId {
        self.iox_metadata.sequencer_id
    }

    /// The IOx table ID
    pub fn table_id(&self) -> TableId {
        self.parquet_file.table_id
    }

    /// The sort key from the IOx metadata
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.iox_metadata.sort_key.as_ref()
    }

    /// The minimum sequence number in this file
    pub fn min_sequence_number(&self) -> SequenceNumber {
        self.parquet_file.min_sequence_number
    }

    /// The maximum sequence number in this file
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.parquet_file.max_sequence_number
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        // note substract size of non Arc'd members as they are
        // already included in Type::size()
        mem::size_of_val(self) +
            self.parquet_file.size() -
            mem::size_of_val(&self.parquet_file) +
            self.parquet_metadata.size() +
            // parquet_metadata is wrapped in Arc so not included in size of self
            self.decoded_metadata.size()
            - mem::size_of_val(&self.decoded_metadata)
            + self.iox_metadata.size()
            - mem::size_of_val(&self.iox_metadata)
    }
}
