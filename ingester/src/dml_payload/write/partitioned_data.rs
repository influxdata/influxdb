use data_types::SequenceNumber;
use mutable_batch::MutableBatch;

/// Partitioned data belonging to a write, sequenced individually from
/// other [`PartitionedData`]
#[derive(Debug, Clone)]
pub struct PartitionedData {
    sequence_number: SequenceNumber,
    data: MutableBatch,
}

impl PartitionedData {
    /// Creates a new set of partitioned data, assigning it a [`SequenceNumber`]
    pub fn new(sequence_number: SequenceNumber, data: MutableBatch) -> Self {
        Self {
            sequence_number,
            data,
        }
    }

    /// Returns the [`SequenceNumber`] assigned
    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }

    /// Returns a reference to the data
    pub fn data(&self) -> &MutableBatch {
        &self.data
    }

    /// Consumes `self`, returning the owned data
    pub fn into_data(self) -> MutableBatch {
        self.data
    }
}
