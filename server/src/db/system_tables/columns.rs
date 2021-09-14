use crate::db::{catalog::Catalog, system_tables::IoxSystemTable};
use arrow::{
    array::{ArrayRef, StringBuilder, UInt32Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::Result,
    record_batch::RecordBatch,
};
use data_types::{
    chunk_metadata::DetailedChunkSummary,
    error::ErrorLogger,
    partition_metadata::{PartitionSummary, TableSummary},
};
use std::{collections::HashMap, sync::Arc};

/// Implementation of `system.columns` system table
#[derive(Debug)]
pub(super) struct ColumnsTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ColumnsTable {
    pub(super) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            schema: partition_summaries_schema(),
            catalog,
        }
    }
}

impl IoxSystemTable for ColumnsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn batch(&self) -> Result<RecordBatch> {
        from_partition_summaries(self.schema(), self.catalog.partition_summaries())
            .log_if_error("system.columns table")
    }
}

fn partition_summaries_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_type", DataType::Utf8, false),
        Field::new("influxdb_type", DataType::Utf8, true),
    ]))
}

fn from_partition_summaries(
    schema: SchemaRef,
    partitions: Vec<PartitionSummary>,
) -> Result<RecordBatch> {
    // Assume each partition has roughly 5 tables with 5 columns
    let row_estimate = partitions.len() * 25;

    let mut partition_key = StringBuilder::new(row_estimate);
    let mut table_name = StringBuilder::new(row_estimate);
    let mut column_name = StringBuilder::new(row_estimate);
    let mut column_type = StringBuilder::new(row_estimate);
    let mut influxdb_type = StringBuilder::new(row_estimate);

    // Note no rows are produced for partitions with no tabes, or
    // tables with no columns: There are other tables to list tables
    // and columns
    for partition in partitions {
        let table = partition.table;
        for column in table.columns {
            partition_key.append_value(&partition.key)?;
            table_name.append_value(&table.name)?;
            column_name.append_value(&column.name)?;
            column_type.append_value(column.type_name())?;
            if let Some(t) = &column.influxdb_type {
                influxdb_type.append_value(t.as_str())?;
            } else {
                influxdb_type.append_null()?;
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(partition_key.finish()) as ArrayRef,
            Arc::new(table_name.finish()),
            Arc::new(column_name.finish()),
            Arc::new(column_type.finish()),
            Arc::new(influxdb_type.finish()),
        ],
    )
}

/// Implementation of system.column_chunks table
#[derive(Debug)]
pub(super) struct ChunkColumnsTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ChunkColumnsTable {
    pub(super) fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            schema: chunk_columns_schema(),
            catalog,
        }
    }
}

impl IoxSystemTable for ChunkColumnsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn batch(&self) -> Result<RecordBatch> {
        assemble_chunk_columns(self.schema(), self.catalog.detailed_chunk_summaries())
            .log_if_error("system.column_chunks table")
    }
}

fn chunk_columns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("chunk_id", DataType::UInt32, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("storage", DataType::Utf8, false),
        Field::new("row_count", DataType::UInt64, true),
        Field::new("null_count", DataType::UInt64, true),
        Field::new("min_value", DataType::Utf8, true),
        Field::new("max_value", DataType::Utf8, true),
        Field::new("memory_bytes", DataType::UInt64, true),
    ]))
}

fn assemble_chunk_columns(
    schema: SchemaRef,
    chunk_summaries: Vec<(Arc<TableSummary>, DetailedChunkSummary)>,
) -> Result<RecordBatch> {
    /// Builds an index from column_name -> size
    fn make_column_index(summary: &DetailedChunkSummary) -> HashMap<&str, u64> {
        summary
            .columns
            .iter()
            .map(|column_summary| {
                (
                    column_summary.name.as_ref(),
                    column_summary.memory_bytes as u64,
                )
            })
            .collect()
    }

    // Assume each chunk has roughly 5 columns
    let row_estimate = chunk_summaries.len() * 5;

    let mut partition_key = StringBuilder::new(row_estimate);
    let mut chunk_id = UInt32Builder::new(row_estimate);
    let mut table_name = StringBuilder::new(row_estimate);
    let mut column_name = StringBuilder::new(row_estimate);
    let mut storage = StringBuilder::new(row_estimate);
    let mut row_count = UInt64Builder::new(row_estimate);
    let mut null_count = UInt64Builder::new(row_estimate);
    let mut min_values = StringBuilder::new(row_estimate);
    let mut max_values = StringBuilder::new(row_estimate);
    let mut memory_bytes = UInt64Builder::new(row_estimate);

    // Note no rows are produced for partitions with no chunks, or
    // tables with no partitions: There are other tables to list tables
    // and columns
    for (table_summary, chunk_summary) in chunk_summaries {
        let mut column_index = make_column_index(&chunk_summary);
        let storage_value = chunk_summary.inner.storage.as_str();

        for column in &table_summary.columns {
            partition_key.append_value(chunk_summary.inner.partition_key.as_ref())?;
            chunk_id.append_value(chunk_summary.inner.id)?;
            table_name.append_value(&chunk_summary.inner.table_name)?;
            column_name.append_value(&column.name)?;
            storage.append_value(storage_value)?;
            row_count.append_value(column.total_count())?;
            null_count.append_value(column.null_count())?;
            if let Some(v) = column.stats.min_as_str() {
                min_values.append_value(v)?;
            } else {
                min_values.append(false)?;
            }
            if let Some(v) = column.stats.max_as_str() {
                max_values.append_value(v)?;
            } else {
                max_values.append(false)?;
            }

            let size = column_index.remove(column.name.as_str());

            memory_bytes.append_option(size)?;
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(partition_key.finish()) as ArrayRef,
            Arc::new(chunk_id.finish()),
            Arc::new(table_name.finish()),
            Arc::new(column_name.finish()),
            Arc::new(storage.finish()),
            Arc::new(row_count.finish()),
            Arc::new(null_count.finish()),
            Arc::new(min_values.finish()),
            Arc::new(max_values.finish()),
            Arc::new(memory_bytes.finish()),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use chrono::{TimeZone, Utc};
    use data_types::{
        chunk_metadata::{ChunkColumnSummary, ChunkStorage, ChunkSummary},
        partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics},
    };

    #[test]
    fn test_from_partition_summaries() {
        let partitions = vec![
            PartitionSummary {
                key: "p1".to_string(),
                table: TableSummary {
                    name: "t1".to_string(),
                    columns: vec![
                        ColumnSummary {
                            name: "c1".to_string(),
                            influxdb_type: Some(InfluxDbType::Tag),
                            stats: Statistics::I64(StatValues::new_with_value(23)),
                        },
                        ColumnSummary {
                            name: "c2".to_string(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::I64(StatValues::new_with_value(43)),
                        },
                        ColumnSummary {
                            name: "c3".to_string(),
                            influxdb_type: None,
                            stats: Statistics::String(StatValues::new_with_value(
                                "foo".to_string(),
                            )),
                        },
                        ColumnSummary {
                            name: "time".to_string(),
                            influxdb_type: Some(InfluxDbType::Timestamp),
                            stats: Statistics::I64(StatValues::new_with_value(43)),
                        },
                    ],
                },
            },
            PartitionSummary {
                key: "p3".to_string(),
                table: TableSummary {
                    name: "t1".to_string(),
                    columns: vec![],
                },
            },
        ];

        let expected = vec![
            "+---------------+------------+-------------+-------------+---------------+",
            "| partition_key | table_name | column_name | column_type | influxdb_type |",
            "+---------------+------------+-------------+-------------+---------------+",
            "| p1            | t1         | c1          | I64         | Tag           |",
            "| p1            | t1         | c2          | I64         | Field         |",
            "| p1            | t1         | c3          | String      |               |",
            "| p1            | t1         | time        | I64         | Timestamp     |",
            "+---------------+------------+-------------+-------------+---------------+",
        ];

        let batch = from_partition_summaries(partition_summaries_schema(), partitions).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }

    #[test]
    fn test_assemble_chunk_columns() {
        let lifecycle_action = None;

        let summaries = vec![
            (
                Arc::new(TableSummary {
                    name: "t1".to_string(),
                    columns: vec![
                        ColumnSummary {
                            name: "c1".to_string(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::String(StatValues::new(
                                Some("bar".to_string()),
                                Some("foo".to_string()),
                                55,
                                0,
                            )),
                        },
                        ColumnSummary {
                            name: "c2".to_string(),
                            influxdb_type: Some(InfluxDbType::Field),
                            stats: Statistics::F64(StatValues::new(Some(11.0), Some(43.0), 66, 0)),
                        },
                    ],
                }),
                DetailedChunkSummary {
                    inner: ChunkSummary {
                        partition_key: "p1".into(),
                        table_name: "t1".into(),
                        id: 42,
                        storage: ChunkStorage::ReadBuffer,
                        lifecycle_action,
                        memory_bytes: 23754,
                        object_store_bytes: 0,
                        row_count: 11,
                        time_of_last_access: None,
                        time_of_first_write: Utc.timestamp_nanos(1),
                        time_of_last_write: Utc.timestamp_nanos(2),
                        time_closed: None,
                        order: 5,
                    },
                    columns: vec![
                        ChunkColumnSummary {
                            name: "c1".into(),
                            memory_bytes: 11,
                        },
                        ChunkColumnSummary {
                            name: "c2".into(),
                            memory_bytes: 12,
                        },
                    ],
                },
            ),
            (
                Arc::new(TableSummary {
                    name: "t1".to_string(),
                    columns: vec![ColumnSummary {
                        name: "c1".to_string(),
                        influxdb_type: Some(InfluxDbType::Field),
                        stats: Statistics::F64(StatValues::new(Some(110.0), Some(430.0), 667, 99)),
                    }],
                }),
                DetailedChunkSummary {
                    inner: ChunkSummary {
                        partition_key: "p2".into(),
                        table_name: "t1".into(),
                        id: 43,
                        storage: ChunkStorage::OpenMutableBuffer,
                        lifecycle_action,
                        memory_bytes: 23754,
                        object_store_bytes: 0,
                        row_count: 11,
                        time_of_last_access: None,
                        time_of_first_write: Utc.timestamp_nanos(1),
                        time_of_last_write: Utc.timestamp_nanos(2),
                        time_closed: None,
                        order: 6,
                    },
                    columns: vec![ChunkColumnSummary {
                        name: "c1".into(),
                        memory_bytes: 100,
                    }],
                },
            ),
            (
                Arc::new(TableSummary {
                    name: "t2".to_string(),
                    columns: vec![ColumnSummary {
                        name: "c3".to_string(),
                        influxdb_type: Some(InfluxDbType::Field),
                        stats: Statistics::F64(StatValues::new(Some(-1.0), Some(2.0), 4, 0)),
                    }],
                }),
                DetailedChunkSummary {
                    inner: ChunkSummary {
                        partition_key: "p2".into(),
                        table_name: "t2".into(),
                        id: 44,
                        storage: ChunkStorage::OpenMutableBuffer,
                        lifecycle_action,
                        memory_bytes: 23754,
                        object_store_bytes: 0,
                        row_count: 11,
                        time_of_last_access: None,
                        time_of_first_write: Utc.timestamp_nanos(1),
                        time_of_last_write: Utc.timestamp_nanos(2),
                        time_closed: None,
                        order: 5,
                    },
                    columns: vec![ChunkColumnSummary {
                        name: "c3".into(),
                        memory_bytes: 200,
                    }],
                },
            ),
        ];

        let expected = vec![
            "+---------------+----------+------------+-------------+-------------------+-----------+------------+-----------+-----------+--------------+",
            "| partition_key | chunk_id | table_name | column_name | storage           | row_count | null_count | min_value | max_value | memory_bytes |",
            "+---------------+----------+------------+-------------+-------------------+-----------+------------+-----------+-----------+--------------+",
            "| p1            | 42       | t1         | c1          | ReadBuffer        | 55        | 0          | bar       | foo       | 11           |",
            "| p1            | 42       | t1         | c2          | ReadBuffer        | 66        | 0          | 11        | 43        | 12           |",
            "| p2            | 43       | t1         | c1          | OpenMutableBuffer | 667       | 99         | 110       | 430       | 100          |",
            "| p2            | 44       | t2         | c3          | OpenMutableBuffer | 4         | 0          | -1        | 2         | 200          |",
            "+---------------+----------+------------+-------------+-------------------+-----------+------------+-----------+-----------+--------------+",
        ];

        let batch = assemble_chunk_columns(chunk_columns_schema(), summaries).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }
}
