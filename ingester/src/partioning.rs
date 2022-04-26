//! Temporary partioning implementation until partitions are properly transmitted by the router.
use chrono::{format::StrftimeItems, TimeZone, Utc};
use mutable_batch::{column::ColumnData, MutableBatch};
use schema::TIME_COLUMN_NAME;

/// Error for [`Partitioner`]
pub type PartitionerError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Interface to partition data within the ingester.
///
/// This is a temporary solution until the partition keys/IDs are properly transmitted by the router.
pub trait Partitioner: std::fmt::Debug + Send + Sync + 'static {
    /// Calculate partition key for given mutable batch.
    fn partition_key(&self, batch: &MutableBatch) -> Result<String, PartitionerError>;
}

/// Default partitioner that matches the current router implementation.
#[derive(Debug, Default)]
#[allow(missing_copy_implementations)]
pub struct DefaultPartitioner {}

impl Partitioner for DefaultPartitioner {
    fn partition_key(&self, batch: &MutableBatch) -> Result<String, PartitionerError> {
        let (_, col) = batch
            .columns()
            .find(|(name, _)| *name == TIME_COLUMN_NAME)
            .unwrap();
        let timestamp = match col.data() {
            ColumnData::I64(_, s) => s.min.unwrap(),
            _ => return Err(String::from("Time column not present").into()),
        };

        Ok(format!(
            "{}",
            Utc.timestamp_nanos(timestamp)
                .format_with_items(StrftimeItems::new("%Y-%m-%d"))
        ))
    }
}
