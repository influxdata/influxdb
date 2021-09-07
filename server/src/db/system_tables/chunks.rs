use crate::db::{catalog::Catalog, system_tables::IoxSystemTable};
use arrow::{
    array::{StringArray, TimestampNanosecondArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::Result,
    record_batch::RecordBatch,
};
use chrono::{DateTime, Utc};
use data_types::{chunk_metadata::ChunkSummary, error::ErrorLogger};
use std::sync::Arc;

/// Implementation of system.chunks table
#[derive(Debug)]
pub(super) struct ChunksTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ChunksTable {
    pub(super) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            schema: chunk_summaries_schema(),
            catalog,
        }
    }
}

impl IoxSystemTable for ChunksTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn batch(&self) -> Result<RecordBatch> {
        from_chunk_summaries(self.schema(), self.catalog.chunk_summaries())
            .log_if_error("system.chunks table")
    }
}

fn chunk_summaries_schema() -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Nanosecond, None);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("storage", DataType::Utf8, false),
        Field::new("lifecycle_action", DataType::Utf8, true),
        Field::new("memory_bytes", DataType::UInt64, false),
        Field::new("object_store_bytes", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("time_of_last_access", ts.clone(), true),
        Field::new("time_of_first_write", ts.clone(), false),
        Field::new("time_of_last_write", ts.clone(), false),
        Field::new("time_closed", ts, true),
        Field::new("order", DataType::UInt32, false),
    ]))
}

// TODO: Use a custom proc macro or serde to reduce the boilerplate
fn optional_time_to_ts(time: Option<DateTime<Utc>>) -> Option<i64> {
    time.and_then(time_to_ts)
}

fn time_to_ts(ts: DateTime<Utc>) -> Option<i64> {
    Some(ts.timestamp_nanos())
}

fn from_chunk_summaries(schema: SchemaRef, chunks: Vec<ChunkSummary>) -> Result<RecordBatch> {
    let id = chunks.iter().map(|c| Some(c.id)).collect::<UInt32Array>();
    let partition_key = chunks
        .iter()
        .map(|c| Some(c.partition_key.as_ref()))
        .collect::<StringArray>();
    let table_name = chunks
        .iter()
        .map(|c| Some(c.table_name.as_ref()))
        .collect::<StringArray>();
    let storage = chunks
        .iter()
        .map(|c| Some(c.storage.as_str()))
        .collect::<StringArray>();
    let lifecycle_action = chunks
        .iter()
        .map(|c| c.lifecycle_action.map(|a| a.name()))
        .collect::<StringArray>();
    let memory_bytes = chunks
        .iter()
        .map(|c| Some(c.memory_bytes as u64))
        .collect::<UInt64Array>();
    let object_store_bytes = chunks
        .iter()
        .map(|c| Some(c.object_store_bytes as u64).filter(|&v| v > 0))
        .collect::<UInt64Array>();
    let row_counts = chunks
        .iter()
        .map(|c| Some(c.row_count as u64))
        .collect::<UInt64Array>();
    let time_of_last_access = chunks
        .iter()
        .map(|c| c.time_of_last_access)
        .map(optional_time_to_ts)
        .collect::<TimestampNanosecondArray>();
    let time_of_first_write = chunks
        .iter()
        .map(|c| c.time_of_first_write)
        .map(time_to_ts)
        .collect::<TimestampNanosecondArray>();
    let time_of_last_write = chunks
        .iter()
        .map(|c| c.time_of_last_write)
        .map(time_to_ts)
        .collect::<TimestampNanosecondArray>();
    let time_closed = chunks
        .iter()
        .map(|c| c.time_closed)
        .map(optional_time_to_ts)
        .collect::<TimestampNanosecondArray>();
    let order = chunks
        .iter()
        .map(|c| Some(c.order))
        .collect::<UInt32Array>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id),
            Arc::new(partition_key),
            Arc::new(table_name),
            Arc::new(storage),
            Arc::new(lifecycle_action),
            Arc::new(memory_bytes),
            Arc::new(object_store_bytes),
            Arc::new(row_counts),
            Arc::new(time_of_last_access),
            Arc::new(time_of_first_write),
            Arc::new(time_of_last_write),
            Arc::new(time_closed),
            Arc::new(order),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use chrono::{TimeZone, Utc};
    use data_types::chunk_metadata::{ChunkLifecycleAction, ChunkStorage};

    #[test]
    fn test_from_chunk_summaries() {
        let chunks = vec![
            ChunkSummary {
                partition_key: Arc::from("p1"),
                table_name: Arc::from("table1"),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                lifecycle_action: None,
                memory_bytes: 23754,
                object_store_bytes: 0,
                row_count: 11,
                time_of_last_access: None,
                time_of_first_write: Utc.timestamp_nanos(10_000_000_000),
                time_of_last_write: Utc.timestamp_nanos(10_000_000_000),
                time_closed: None,
                order: 5,
            },
            ChunkSummary {
                partition_key: Arc::from("p1"),
                table_name: Arc::from("table1"),
                id: 1,
                storage: ChunkStorage::OpenMutableBuffer,
                lifecycle_action: Some(ChunkLifecycleAction::Persisting),
                memory_bytes: 23455,
                object_store_bytes: 0,
                row_count: 22,
                time_of_last_access: Some(Utc.timestamp_nanos(754_000_000_000)),
                time_of_first_write: Utc.timestamp_nanos(80_000_000_000),
                time_of_last_write: Utc.timestamp_nanos(80_000_000_000),
                time_closed: None,
                order: 6,
            },
            ChunkSummary {
                partition_key: Arc::from("p1"),
                table_name: Arc::from("table1"),
                id: 2,
                storage: ChunkStorage::ObjectStoreOnly,
                lifecycle_action: None,
                memory_bytes: 1234,
                object_store_bytes: 5678,
                row_count: 33,
                time_of_last_access: Some(Utc.timestamp_nanos(5_000_000_000)),
                time_of_first_write: Utc.timestamp_nanos(100_000_000_000),
                time_of_last_write: Utc.timestamp_nanos(200_000_000_000),
                time_closed: None,
                order: 7,
            },
        ];

        let expected = vec![
            "+----+---------------+------------+-------------------+------------------------------+--------------+--------------------+-----------+----------------------+----------------------+----------------------+-------------+-------+",
            "| id | partition_key | table_name | storage           | lifecycle_action             | memory_bytes | object_store_bytes | row_count | time_of_last_access  | time_of_first_write  | time_of_last_write   | time_closed | order |",
            "+----+---------------+------------+-------------------+------------------------------+--------------+--------------------+-----------+----------------------+----------------------+----------------------+-------------+-------+",
            "| 0  | p1            | table1     | OpenMutableBuffer |                              | 23754        |                    | 11        |                      | 1970-01-01T00:00:10Z | 1970-01-01T00:00:10Z |             | 5     |",
            "| 1  | p1            | table1     | OpenMutableBuffer | Persisting to Object Storage | 23455        |                    | 22        | 1970-01-01T00:12:34Z | 1970-01-01T00:01:20Z | 1970-01-01T00:01:20Z |             | 6     |",
            "| 2  | p1            | table1     | ObjectStoreOnly   |                              | 1234         | 5678               | 33        | 1970-01-01T00:00:05Z | 1970-01-01T00:01:40Z | 1970-01-01T00:03:20Z |             | 7     |",
            "+----+---------------+------------+-------------------+------------------------------+--------------+--------------------+-----------+----------------------+----------------------+----------------------+-------------+-------+",
        ];

        let schema = chunk_summaries_schema();
        let batch = from_chunk_summaries(schema, chunks).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }
}
