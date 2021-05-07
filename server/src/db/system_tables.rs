use std::any::Any;
use std::convert::AsRef;
use std::iter::FromIterator;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use arrow::{
    array::{
        ArrayRef, StringArray, StringBuilder, Time64NanosecondArray, TimestampNanosecondArray,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::Result,
    record_batch::RecordBatch,
};
use data_types::{
    chunk::ChunkSummary, error::ErrorLogger, job::Job, partition_metadata::PartitionSummary,
};
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::{datasource::Statistics, MemTable, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use tracker::TaskTracker;

use super::catalog::Catalog;
use crate::JobRegistry;

// The IOx system schema
pub const SYSTEM_SCHEMA: &str = "system";

const CHUNKS: &str = "chunks";
const COLUMNS: &str = "columns";
const OPERATIONS: &str = "operations";

#[derive(Debug)]
pub struct SystemSchemaProvider {
    db_name: String,
    catalog: Arc<Catalog>,
    jobs: Arc<JobRegistry>,

    chunks: Arc<ChunksProvider>,
}

impl SystemSchemaProvider {
    pub fn new(db_name: impl Into<String>, catalog: Arc<Catalog>, jobs: Arc<JobRegistry>) -> Self {
        let db_name = db_name.into();
        let chunks = Arc::new(ChunksProvider::new(Arc::clone(&catalog)));
        Self {
            db_name,
            catalog,
            jobs,
            chunks,
        }
    }
}

impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            CHUNKS.to_string(),
            COLUMNS.to_string(),
            OPERATIONS.to_string(),
        ]
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // TODO: Use of a MemTable potentially results in materializing redundant data
        if name == CHUNKS {
            return Some(Arc::clone(&self.chunks) as Arc<dyn TableProvider>);
        }

        let batch = match name {
            COLUMNS => from_partition_summaries(self.catalog.partition_summaries())
                .log_if_error("chunks table")
                .ok()?,
            OPERATIONS => from_task_trackers(&self.db_name, self.jobs.tracked())
                .log_if_error("operations table")
                .ok()?,
            _ => return None,
        };

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .log_if_error("constructing chunks system table")
            .ok()?;

        Some(Arc::<MemTable>::new(table))
    }
}

// TODO: Use a custom proc macro or serde to reduce the boilerplate
fn time_to_ts(time: Option<DateTime<Utc>>) -> Option<i64> {
    time.map(|ts| ts.timestamp_nanos())
}

/// Implementation of system.chunks table
#[derive(Debug)]
struct ChunksProvider {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ChunksProvider {
    fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            schema: chunk_summaries_schema(),
            catalog,
        }
    }

    fn chunk_summaries(&self) -> Result<RecordBatch> {
        let chunks = self.catalog.chunk_summaries();
        from_chunk_summaries(self.schema(), chunks)
    }
}

fn chunk_summaries_schema() -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Nanosecond, None);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("storage", DataType::Utf8, false),
        Field::new("estimated_bytes", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("time_of_first_write", ts.clone(), true),
        Field::new("time_of_last_write", ts.clone(), true),
        Field::new("time_closed", ts, true),
    ]))
}

fn from_chunk_summaries(schema: SchemaRef, chunks: Vec<ChunkSummary>) -> Result<RecordBatch> {
    let id = UInt32Array::from_iter(chunks.iter().map(|c| Some(c.id)));
    let partition_key =
        StringArray::from_iter(chunks.iter().map(|c| Some(c.partition_key.as_ref())));
    let table_name = StringArray::from_iter(chunks.iter().map(|c| Some(c.table_name.as_ref())));
    let storage = StringArray::from_iter(chunks.iter().map(|c| Some(c.storage.as_str())));
    let estimated_bytes =
        UInt64Array::from_iter(chunks.iter().map(|c| Some(c.estimated_bytes as u64)));
    let row_counts = UInt64Array::from_iter(chunks.iter().map(|c| Some(c.row_count as u64)));
    let time_of_first_write = TimestampNanosecondArray::from_iter(
        chunks.iter().map(|c| c.time_of_first_write).map(time_to_ts),
    );
    let time_of_last_write = TimestampNanosecondArray::from_iter(
        chunks.iter().map(|c| c.time_of_last_write).map(time_to_ts),
    );
    let time_closed =
        TimestampNanosecondArray::from_iter(chunks.iter().map(|c| c.time_closed).map(time_to_ts));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id), // as ArrayRef,
            Arc::new(partition_key),
            Arc::new(table_name),
            Arc::new(storage),
            Arc::new(estimated_bytes),
            Arc::new(row_counts),
            Arc::new(time_of_first_write),
            Arc::new(time_of_last_write),
            Arc::new(time_closed),
        ],
    )
}

impl TableProvider for ChunksProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        // It would be cool to push projection and limit down
        _filters: &[datafusion::logical_plan::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let batch = self.chunk_summaries().log_if_error("chunks table")?;
        scan_batch(batch, self.schema(), projection.as_ref())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

fn from_partition_summaries(partitions: Vec<PartitionSummary>) -> Result<RecordBatch> {
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
        for table in partition.tables {
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
    }

    let partition_key = partition_key.finish();
    let table_name = table_name.finish();
    let column_name = column_name.finish();
    let column_type = column_type.finish();
    let influxdb_type = influxdb_type.finish();

    RecordBatch::try_from_iter_with_nullable(vec![
        ("partition_key", Arc::new(partition_key) as ArrayRef, false),
        ("table_name", Arc::new(table_name), false),
        ("column_name", Arc::new(column_name), false),
        ("column_type", Arc::new(column_type), false),
        ("influxdb_type", Arc::new(influxdb_type), true),
    ])
}

fn from_task_trackers(db_name: &str, jobs: Vec<TaskTracker<Job>>) -> Result<RecordBatch> {
    let jobs = jobs
        .into_iter()
        .filter(|job| job.metadata().db_name() == Some(db_name))
        .collect::<Vec<_>>();

    let ids = StringArray::from_iter(jobs.iter().map(|job| Some(job.id().to_string())));
    let statuses = StringArray::from_iter(jobs.iter().map(|job| Some(job.get_status().name())));
    let cpu_time_used = Time64NanosecondArray::from_iter(
        jobs.iter()
            .map(|job| job.get_status().cpu_nanos().map(|n| n as i64)),
    );
    let wall_time_used = Time64NanosecondArray::from_iter(
        jobs.iter()
            .map(|job| job.get_status().wall_nanos().map(|n| n as i64)),
    );
    let partition_keys =
        StringArray::from_iter(jobs.iter().map(|job| job.metadata().partition_key()));
    let chunk_ids = UInt32Array::from_iter(jobs.iter().map(|job| job.metadata().chunk_id()));
    let descriptions =
        StringArray::from_iter(jobs.iter().map(|job| Some(job.metadata().description())));

    RecordBatch::try_from_iter_with_nullable(vec![
        ("id", Arc::new(ids) as ArrayRef, false),
        ("status", Arc::new(statuses), true),
        ("cpu_time_used", Arc::new(cpu_time_used), true),
        ("wall_time_used", Arc::new(wall_time_used), true),
        ("partition_key", Arc::new(partition_keys), true),
        ("chunk_id", Arc::new(chunk_ids), true),
        ("description", Arc::new(descriptions), true),
    ])
}

/// Creates a DataFusion ExecutionPlan node that scans a single batch
/// of records.
fn scan_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // apply projection, if any
    let (schema, batch) = match projection {
        None => (schema, batch),
        Some(projection) => {
            let projected_columns: DataFusionResult<Vec<Field>> = projection
                .iter()
                .map(|i| {
                    if *i < schema.fields().len() {
                        Ok(schema.field(*i).clone())
                    } else {
                        Err(DataFusionError::Internal(format!(
                            "Projection index out of range in ChunksProvider: {}",
                            i
                        )))
                    }
                })
                .collect();

            let projected_schema = Arc::new(Schema::new(projected_columns?));

            let columns = projection
                .iter()
                .map(|i| Arc::clone(batch.column(*i)))
                .collect::<Vec<_>>();

            let projected_batch = RecordBatch::try_new(Arc::clone(&projected_schema), columns)?;
            (projected_schema, projected_batch)
        }
    };

    Ok(Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use chrono::NaiveDateTime;
    use data_types::chunk::ChunkStorage;
    use data_types::partition_metadata::{
        ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary,
    };

    #[test]
    fn test_from_chunk_summaries() {
        let chunks = vec![
            ChunkSummary {
                partition_key: Arc::from("p1"),
                table_name: Arc::from("table1"),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 23754,
                row_count: 11,
                time_of_first_write: Some(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(10, 0),
                    Utc,
                )),
                time_of_last_write: None,
                time_closed: None,
            },
            ChunkSummary {
                partition_key: Arc::from("p1"),
                table_name: Arc::from("table1"),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 23454,
                row_count: 22,
                time_of_first_write: None,
                time_of_last_write: Some(DateTime::from_utc(
                    NaiveDateTime::from_timestamp(80, 0),
                    Utc,
                )),
                time_closed: None,
            },
        ];

        let expected = vec![
            "+----+---------------+------------+-------------------+-----------------+-----------+---------------------+---------------------+-------------+",
            "| id | partition_key | table_name | storage           | estimated_bytes | row_count | time_of_first_write | time_of_last_write  | time_closed |",
            "+----+---------------+------------+-------------------+-----------------+-----------+---------------------+---------------------+-------------+",
            "| 0  | p1            | table1     | OpenMutableBuffer | 23754           | 11        | 1970-01-01 00:00:10 |                     |             |",
            "| 0  | p1            | table1     | OpenMutableBuffer | 23454           | 22        |                     | 1970-01-01 00:01:20 |             |",
            "+----+---------------+------------+-------------------+-----------------+-----------+---------------------+---------------------+-------------+",
        ];

        let schema = chunk_summaries_schema();
        let batch = from_chunk_summaries(schema, chunks).unwrap();
        assert_batches_eq!(&expected, &[batch]);
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
            "+---------------+------------+-------------+-------------+---------------+",
            "| partition_key | table_name | column_name | column_type | influxdb_type |",
            "+---------------+------------+-------------+-------------+---------------+",
            "| p1            | t1         | c1          | I64         | Tag           |",
            "| p1            | t1         | c2          | I64         | Field         |",
            "| p1            | t1         | c3          | String      |               |",
            "| p1            | t1         | time        | I64         | Timestamp     |",
            "+---------------+------------+-------------+-------------+---------------+",
        ];

        let batch = from_partition_summaries(partitions).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }

    fn seq_array(start: u64, end: u64) -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(start..end))
    }

    #[tokio::test]
    async fn test_scan_batch_no_projection() {
        let batch = RecordBatch::try_from_iter(vec![
            ("col1", seq_array(0, 3)),
            ("col2", seq_array(1, 4)),
            ("col3", seq_array(2, 5)),
            ("col4", seq_array(3, 6)),
        ])
        .unwrap();

        let projection = None;
        let scan = scan_batch(batch.clone(), batch.schema(), projection).unwrap();
        let collected = datafusion::physical_plan::collect(scan).await.unwrap();

        let expected = vec![
            "+------+------+------+------+",
            "| col1 | col2 | col3 | col4 |",
            "+------+------+------+------+",
            "| 0    | 1    | 2    | 3    |",
            "| 1    | 2    | 3    | 4    |",
            "| 2    | 3    | 4    | 5    |",
            "+------+------+------+------+",
        ];

        assert_batches_eq!(&expected, &collected);
    }

    #[tokio::test]
    async fn test_scan_batch_good_projection() {
        let batch = RecordBatch::try_from_iter(vec![
            ("col1", seq_array(0, 3)),
            ("col2", seq_array(1, 4)),
            ("col3", seq_array(2, 5)),
            ("col4", seq_array(3, 6)),
        ])
        .unwrap();

        let projection = Some(vec![3, 1]);
        let scan = scan_batch(batch.clone(), batch.schema(), projection.as_ref()).unwrap();
        let collected = datafusion::physical_plan::collect(scan).await.unwrap();

        let expected = vec![
            "+------+------+",
            "| col4 | col2 |",
            "+------+------+",
            "| 3    | 1    |",
            "| 4    | 2    |",
            "| 5    | 3    |",
            "+------+------+",
        ];

        assert_batches_eq!(&expected, &collected);
    }

    #[tokio::test]
    async fn test_scan_batch_bad_projection() {
        let batch = RecordBatch::try_from_iter(vec![
            ("col1", seq_array(0, 3)),
            ("col2", seq_array(1, 4)),
            ("col3", seq_array(2, 5)),
            ("col4", seq_array(3, 6)),
        ])
        .unwrap();

        // no column idex 5
        let projection = Some(vec![3, 1, 5]);
        let result = scan_batch(batch.clone(), batch.schema(), projection.as_ref());
        let err_string = result.unwrap_err().to_string();
        assert!(
            err_string
                .contains("Internal error: Projection index out of range in ChunksProvider: 5"),
            "Actual error: {}",
            err_string
        );
    }
}
