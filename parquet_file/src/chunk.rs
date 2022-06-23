//! A metadata summary of a Parquet file in object storage, with the ability to
//! download & execute a scan.

use crate::{
    metadata::{DecodedIoxParquetMetaData, IoxMetadata, IoxParquetMetaData},
    storage::ParquetStorage,
    ParquetFilePath,
};
use data_types::{
    ParquetFile, ParquetFileId, ParquetFileWithMetadata, PartitionId, SequenceNumber, SequencerId,
    TableId, TimestampMinMax, TimestampRange,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use predicate::Predicate;
use schema::{selection::Selection, sort::SortKey, Schema};
use std::{collections::BTreeSet, mem, sync::Arc};

/// A abstract representation of a Parquet file in object storage, with
/// associated metadata.
#[derive(Debug)]
pub struct ParquetChunk {
    /// Parquet file.
    parquet_file: Arc<ParquetFile>,

    /// Schema that goes with this table's parquet file
    schema: Arc<Schema>,

    /// Persists the parquet file within a database's relative path
    store: ParquetStorage,
}

impl ParquetChunk {
    /// Create parquet chunk.
    pub fn new(parquet_file: Arc<ParquetFile>, schema: Arc<Schema>, store: ParquetStorage) -> Self {
        Self {
            parquet_file,
            schema,
            store,
        }
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        mem::size_of_val(self) + self.parquet_file.size() - mem::size_of_val(&self.parquet_file)
    }

    /// Infallably return the full schema (for all columns) for this chunk
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Return true if this chunk contains values within the time range, or if
    /// the range is `None`.
    pub fn has_timerange(&self, timestamp_range: Option<&TimestampRange>) -> bool {
        if let Some(timestamp_range) = timestamp_range {
            self.timestamp_min_max().overlaps(*timestamp_range)
        } else {
            // no range specified
            true
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
        let path: ParquetFilePath = self.parquet_file.as_ref().into();
        self.store.read_filter(
            predicate,
            selection,
            Arc::clone(&self.schema.as_arrow()),
            &path,
        )
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
