use std::sync::Arc;

use arrow::array::{StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;

use data_types::error::ErrorLogger;
use data_types::partition_metadata::PartitionAddr;
use data_types::write_summary::WriteSummary;

use crate::db::catalog::Catalog;
use crate::db::system_tables::IoxSystemTable;

/// Implementation of system.persistence_windows table
#[derive(Debug)]
pub(super) struct PersistenceWindowsTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl PersistenceWindowsTable {
    pub(super) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            schema: persistence_windows_schema(),
            catalog,
        }
    }
}

impl IoxSystemTable for PersistenceWindowsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn batch(&self) -> Result<RecordBatch> {
        from_write_summaries(self.schema(), self.catalog.persistence_summaries())
            .log_if_error("system.persistence_windows table")
    }
}

fn persistence_windows_schema() -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Nanosecond, None);
    Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("time_of_first_write", ts.clone(), false),
        Field::new("time_of_last_write", ts.clone(), false),
        Field::new("min_timestamp", ts.clone(), false),
        Field::new("max_timestamp", ts, false),
    ]))
}

fn from_write_summaries(
    schema: SchemaRef,
    chunks: Vec<(PartitionAddr, WriteSummary)>,
) -> Result<RecordBatch> {
    let partition_key = chunks
        .iter()
        .map(|(addr, _)| Some(addr.partition_key.as_ref()))
        .collect::<StringArray>();
    let table_name = chunks
        .iter()
        .map(|(addr, _)| Some(addr.table_name.as_ref()))
        .collect::<StringArray>();
    let row_counts = chunks
        .iter()
        .map(|(_, w)| Some(w.row_count as u64))
        .collect::<UInt64Array>();
    let time_of_first_write = chunks
        .iter()
        .map(|(_, w)| Some(w.time_of_first_write.timestamp_nanos()))
        .collect::<TimestampNanosecondArray>();
    let time_of_last_write = chunks
        .iter()
        .map(|(_, w)| Some(w.time_of_last_write.timestamp_nanos()))
        .collect::<TimestampNanosecondArray>();
    let min_timestamp = chunks
        .iter()
        .map(|(_, w)| Some(w.min_timestamp.timestamp_nanos()))
        .collect::<TimestampNanosecondArray>();
    let max_timestamp = chunks
        .iter()
        .map(|(_, w)| Some(w.max_timestamp.timestamp_nanos()))
        .collect::<TimestampNanosecondArray>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(partition_key),
            Arc::new(table_name),
            Arc::new(row_counts),
            Arc::new(time_of_first_write),
            Arc::new(time_of_last_write),
            Arc::new(min_timestamp),
            Arc::new(max_timestamp),
        ],
    )
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use arrow_util::assert_batches_eq;

    use super::*;

    #[test]
    fn test_from_write_summaries() {
        let addr = PartitionAddr {
            db_name: Arc::from("db"),
            table_name: Arc::from("table"),
            partition_key: Arc::from("partition"),
        };

        let summaries = vec![
            (
                addr.clone(),
                WriteSummary {
                    time_of_first_write: Utc.timestamp_nanos(0),
                    time_of_last_write: Utc.timestamp_nanos(20),
                    min_timestamp: Utc.timestamp_nanos(50),
                    max_timestamp: Utc.timestamp_nanos(60),
                    row_count: 320,
                },
            ),
            (
                addr,
                WriteSummary {
                    time_of_first_write: Utc.timestamp_nanos(6),
                    time_of_last_write: Utc.timestamp_nanos(21),
                    min_timestamp: Utc.timestamp_nanos(1),
                    max_timestamp: Utc.timestamp_nanos(2),
                    row_count: 2,
                },
            ),
        ];

        let expected = vec![
            "+---------------+------------+-----------+-------------------------------+-------------------------------+-------------------------------+-------------------------------+",
            "| partition_key | table_name | row_count | time_of_first_write           | time_of_last_write            | min_timestamp                 | max_timestamp                 |",
            "+---------------+------------+-----------+-------------------------------+-------------------------------+-------------------------------+-------------------------------+",
            "| partition     | table      | 320       | 1970-01-01 00:00:00           | 1970-01-01 00:00:00.000000020 | 1970-01-01 00:00:00.000000050 | 1970-01-01 00:00:00.000000060 |",
            "| partition     | table      | 2         | 1970-01-01 00:00:00.000000006 | 1970-01-01 00:00:00.000000021 | 1970-01-01 00:00:00.000000001 | 1970-01-01 00:00:00.000000002 |",
            "+---------------+------------+-----------+-------------------------------+-------------------------------+-------------------------------+-------------------------------+",
        ];

        let schema = persistence_windows_schema();
        let batch = from_write_summaries(schema, summaries).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }
}
