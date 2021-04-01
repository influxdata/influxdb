use std::any::Any;
use std::convert::AsRef;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use arrow_deps::{
    arrow::{
        array::{Array, StringBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder},
        datatypes::{Field, Schema},
        error::Result,
        record_batch::RecordBatch,
    },
    datafusion::{
        catalog::schema::SchemaProvider,
        datasource::{MemTable, TableProvider},
    },
};
use data_types::{chunk::ChunkSummary, error::ErrorLogger, partition_metadata::PartitionSummary};

use super::catalog::Catalog;

// The IOx system schema
pub const SYSTEM_SCHEMA: &str = "system";

const CHUNKS: &str = "chunks";
const COLUMNS: &str = "columns";

#[derive(Debug)]
pub struct SystemSchemaProvider {
    catalog: Arc<Catalog>,
}

impl SystemSchemaProvider {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        vec![CHUNKS.to_string(), COLUMNS.to_string()]
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // TODO: Use of a MemTable potentially results in materializing redundant data
        let batch = match name {
            CHUNKS => from_chunk_summaries(self.catalog.chunk_summaries())
                .log_if_error("chunks table")
                .ok()?,
            COLUMNS => from_partition_summaries(self.catalog.partition_summaries())
                .log_if_error("chunks table")
                .ok()?,
            _ => return None,
        };

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .log_if_error("constructing chunks system table")
            .ok()?;

        Some(Arc::<MemTable>::new(table))
    }
}

fn append_time(
    builder: &mut TimestampNanosecondBuilder,
    time: Option<DateTime<Utc>>,
) -> Result<()> {
    match time {
        Some(time) => builder.append_value(time.timestamp_nanos()),
        None => builder.append_null(),
    }
}

// TODO: Use a custom proc macro or serde to reduce the boilerplate

fn from_chunk_summaries(chunks: Vec<ChunkSummary>) -> Result<RecordBatch> {
    let mut id = UInt32Builder::new(chunks.len());
    let mut partition_key = StringBuilder::new(chunks.len());
    let mut storage = StringBuilder::new(chunks.len());
    let mut estimated_bytes = UInt64Builder::new(chunks.len());
    let mut time_of_first_write = TimestampNanosecondBuilder::new(chunks.len());
    let mut time_of_last_write = TimestampNanosecondBuilder::new(chunks.len());
    let mut time_closing = TimestampNanosecondBuilder::new(chunks.len());

    for chunk in chunks {
        id.append_value(chunk.id)?;
        partition_key.append_value(chunk.partition_key.as_ref())?;
        storage.append_value(chunk.storage.as_str())?;
        estimated_bytes.append_value(chunk.estimated_bytes as u64)?;

        append_time(&mut time_of_first_write, chunk.time_of_first_write)?;
        append_time(&mut time_of_last_write, chunk.time_of_last_write)?;
        append_time(&mut time_closing, chunk.time_closing)?;
    }

    let id = id.finish();
    let partition_key = partition_key.finish();
    let storage = storage.finish();
    let estimated_bytes = estimated_bytes.finish();
    let time_of_first_write = time_of_first_write.finish();
    let time_of_last_write = time_of_last_write.finish();
    let time_closing = time_closing.finish();

    let schema = Schema::new(vec![
        Field::new("id", id.data_type().clone(), false),
        Field::new("partition_key", partition_key.data_type().clone(), false),
        Field::new("storage", storage.data_type().clone(), false),
        Field::new("estimated_bytes", estimated_bytes.data_type().clone(), true),
        Field::new(
            "time_of_first_write",
            time_of_first_write.data_type().clone(),
            true,
        ),
        Field::new(
            "time_of_last_write",
            time_of_last_write.data_type().clone(),
            true,
        ),
        Field::new("time_closing", time_closing.data_type().clone(), true),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id),
            Arc::new(partition_key),
            Arc::new(storage),
            Arc::new(estimated_bytes),
            Arc::new(time_of_first_write),
            Arc::new(time_of_last_write),
            Arc::new(time_closing),
        ],
    )
}

fn from_partition_summaries(partitions: Vec<PartitionSummary>) -> Result<RecordBatch> {
    // Assume each partition has roughly 5 tables with 5 columns
    let row_estimate = partitions.len() * 25;

    let mut partition_key = StringBuilder::new(row_estimate);
    let mut table_name = StringBuilder::new(row_estimate);
    let mut column_name = StringBuilder::new(row_estimate);
    let mut count = UInt64Builder::new(row_estimate);

    for partition in partitions {
        if partition.tables.is_empty() {
            partition_key.append_value(&partition.key)?;
            table_name.append_null()?;
            column_name.append_null()?;
            count.append_null()?;
            continue;
        }

        for table in partition.tables {
            if table.columns.is_empty() {
                partition_key.append_value(&partition.key)?;
                table_name.append_value(&table.name)?;
                column_name.append_null()?;
                count.append_null()?;
                continue;
            }

            for column in table.columns {
                partition_key.append_value(&partition.key)?;
                table_name.append_value(&table.name)?;
                column_name.append_value(&column.name)?;
                count.append_value(column.count())?;
            }
        }
    }

    let partition_key = partition_key.finish();
    let table_name = table_name.finish();
    let column_name = column_name.finish();
    let count = count.finish();

    let schema = Schema::new(vec![
        Field::new("partition_key", partition_key.data_type().clone(), false),
        Field::new("table_name", table_name.data_type().clone(), true),
        Field::new("column_name", column_name.data_type().clone(), true),
        Field::new("count", count.data_type().clone(), true),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(partition_key),
            Arc::new(table_name),
            Arc::new(column_name),
            Arc::new(count),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_deps::assert_table_eq;
    use chrono::NaiveDateTime;
    use data_types::chunk::ChunkStorage;
    use data_types::partition_metadata::{ColumnSummary, StatValues, Statistics, TableSummary};

    #[test]
    fn test_from_chunk_summaries() {
        let chunks = vec![
            ChunkSummary {
                partition_key: Arc::new("".to_string()),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 23754,
                time_of_first_write: Some(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(10, 0),
                    Utc,
                )),
                time_of_last_write: None,
                time_closing: None,
            },
            ChunkSummary {
                partition_key: Arc::new("".to_string()),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 23454,
                time_of_first_write: None,
                time_of_last_write: Some(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(80, 0),
                    Utc,
                )),
                time_closing: None,
            },
        ];

        let expected = vec![
            "+----+---------------+-------------------+-----------------+---------------------+---------------------+--------------+",
            "| id | partition_key | storage           | estimated_bytes | time_of_first_write | time_of_last_write  | time_closing |",
            "+----+---------------+-------------------+-----------------+---------------------+---------------------+--------------+",
            "| 0  |               | OpenMutableBuffer | 23754           | 1970-01-01 00:00:10 |                     |              |",
            "| 0  |               | OpenMutableBuffer | 23454           |                     | 1970-01-01 00:01:20 |              |",
            "+----+---------------+-------------------+-----------------+---------------------+---------------------+--------------+",
        ];

        let batch = from_chunk_summaries(chunks).unwrap();
        assert_table_eq!(&expected, &[batch]);
    }

    #[test]
    fn test_from_partition_summaries() {
        let partitions = vec![
            PartitionSummary {
                key: "p1".to_string(),
                tables: vec![TableSummary {
                    name: "t1".to_string(),
                    columns: vec![
                        ColumnSummary {
                            name: "c1".to_string(),
                            stats: Statistics::I64(StatValues::new(23)),
                        },
                        ColumnSummary {
                            name: "c2".to_string(),
                            stats: Statistics::I64(StatValues::new(43)),
                        },
                    ],
                }],
            },
            PartitionSummary {
                key: "p2".to_string(),
                tables: vec![],
            },
            PartitionSummary {
                key: "p3".to_string(),
                tables: vec![TableSummary {
                    name: "t1".to_string(),
                    columns: vec![],
                }],
            },
        ];

        let expected = vec![
            "+---------------+------------+-------------+-------+",
            "| partition_key | table_name | column_name | count |",
            "+---------------+------------+-------------+-------+",
            "| p1            | t1         | c1          | 1     |",
            "| p1            | t1         | c2          | 1     |",
            "| p2            |            |             |       |",
            "| p3            | t1         |             |       |",
            "+---------------+------------+-------------+-------+",
        ];

        let batch = from_partition_summaries(partitions).unwrap();
        assert_table_eq!(&expected, &[batch]);
    }
}
