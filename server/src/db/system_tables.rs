//! Contains implementation of IOx name: (), stats: () system table stats: () stats: ()es (aka tables in the `system` schema)
//!
//! For example `SELECT * FROM system.chunks`

use std::convert::AsRef;
use std::iter::FromIterator;
use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use chrono::{DateTime, Utc};

use arrow::{
    array::{
        ArrayRef, StringArray, StringBuilder, Time64NanosecondArray, TimestampNanosecondArray,
        UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};
use data_types::{
    chunk::{ChunkSummary, DetailedChunkSummary},
    error::ErrorLogger,
    job::Job,
    partition_metadata::{PartitionSummary, UnaggregatedPartitionSummary},
};
use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::{datasource::Statistics, TableProvider},
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
const CHUNK_COLUMNS: &str = "chunk_columns";
const OPERATIONS: &str = "operations";

pub struct SystemSchemaProvider {
    chunks: Arc<dyn TableProvider>,
    columns: Arc<dyn TableProvider>,
    chunk_columns: Arc<dyn TableProvider>,
    operations: Arc<dyn TableProvider>,
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemSchemaProvider")
            .field("fields", &"...")
            .finish()
    }
}

impl SystemSchemaProvider {
    pub fn new(db_name: impl Into<String>, catalog: Arc<Catalog>, jobs: Arc<JobRegistry>) -> Self {
        let db_name = db_name.into();
        let chunks = Arc::new(SystemTableProvider {
            inner: ChunksTable::new(Arc::clone(&catalog)),
        });
        let columns = Arc::new(SystemTableProvider {
            inner: ColumnsTable::new(Arc::clone(&catalog)),
        });
        let chunk_columns = Arc::new(SystemTableProvider {
            inner: ChunkColumnsTable::new(catalog),
        });
        let operations = Arc::new(SystemTableProvider {
            inner: OperationsTable::new(db_name, jobs),
        });
        Self {
            chunks,
            columns,
            chunk_columns,
            operations,
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
            CHUNK_COLUMNS.to_string(),
            OPERATIONS.to_string(),
        ]
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match name {
            CHUNKS => Some(Arc::clone(&self.chunks)),
            COLUMNS => Some(Arc::clone(&self.columns)),
            CHUNK_COLUMNS => Some(Arc::clone(&self.chunk_columns)),
            OPERATIONS => Some(Arc::clone(&self.operations)),
            _ => None,
        }
    }
}

/// The minimal thing that a system table needs to implement
trait IoxSystemTable: Send + Sync {
    /// Produce the schema from this system table
    fn schema(&self) -> SchemaRef;

    /// Get the contents of the system table as a single RecordBatch
    fn batch(&self) -> Result<RecordBatch>;
}

/// Adapter that makes any `IoxSystemTable` a DataFusion `TableProvider`
struct SystemTableProvider<T>
where
    T: IoxSystemTable,
{
    inner: T,
}

impl<T> TableProvider for SystemTableProvider<T>
where
    T: IoxSystemTable + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        // It would be cool to push projection and limit down
        _filters: &[datafusion::logical_plan::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        scan_batch(self.inner.batch()?, self.schema(), projection.as_ref())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

// TODO: Use a custom proc macro or serde to reduce the boilerplate
fn time_to_ts(time: Option<DateTime<Utc>>) -> Option<i64> {
    time.map(|ts| ts.timestamp_nanos())
}

/// Implementation of system.chunks table
#[derive(Debug)]
struct ChunksTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ChunksTable {
    fn new(catalog: Arc<Catalog>) -> Self {
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

/// Implementation of `system.columns` system table
#[derive(Debug)]
struct ColumnsTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ColumnsTable {
    fn new(catalog: Arc<Catalog>) -> Self {
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
struct ChunkColumnsTable {
    schema: SchemaRef,
    catalog: Arc<Catalog>,
}

impl ChunkColumnsTable {
    fn new(catalog: Arc<Catalog>) -> Self {
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
        assemble_chunk_columns(
            self.schema(),
            self.catalog.unaggregated_partition_summaries(),
            self.catalog.detailed_chunk_summaries(),
        )
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
        Field::new("count", DataType::UInt64, true),
        Field::new("min_value", DataType::Utf8, true),
        Field::new("max_value", DataType::Utf8, true),
        Field::new("estimated_bytes", DataType::UInt64, true),
    ]))
}

fn assemble_chunk_columns(
    schema: SchemaRef,
    partitions: Vec<UnaggregatedPartitionSummary>,
    chunk_summaries: Vec<DetailedChunkSummary>,
) -> Result<RecordBatch> {
    // maps (partition_key, chunk_id, table_name) --> DetailedChunkSummary
    let chunk_summary_map: HashMap<_, _> = chunk_summaries
        .iter()
        .map(|chunk_summary| {
            let key = (
                chunk_summary.inner.partition_key.as_ref(),
                chunk_summary.inner.id,
                chunk_summary.inner.table_name.as_ref(),
            );
            (key, chunk_summary)
        })
        .collect();

    /// Builds an index from column_name -> size
    fn make_column_index(summary: &DetailedChunkSummary) -> HashMap<&str, u64> {
        summary
            .columns
            .iter()
            .map(|column_summary| {
                (
                    column_summary.name.as_ref(),
                    column_summary.estimated_bytes as u64,
                )
            })
            .collect()
    }

    // Assume each partition has roughly 5 tables with 5 columns
    let row_estimate = partitions.len() * 25;

    let mut partition_key = StringBuilder::new(row_estimate);
    let mut chunk_id = UInt32Builder::new(row_estimate);
    let mut table_name = StringBuilder::new(row_estimate);
    let mut column_name = StringBuilder::new(row_estimate);
    let mut storage = StringBuilder::new(row_estimate);
    let mut count = UInt64Builder::new(row_estimate);
    let mut min_values = StringBuilder::new(row_estimate);
    let mut max_values = StringBuilder::new(row_estimate);
    let mut estimated_bytes = UInt64Builder::new(row_estimate);

    // Note no rows are produced for partitions with no tables, or
    // tables with no columns: There are other tables to list tables
    // and columns
    for partition in partitions {
        for chunk_table in partition.tables {
            let table = &chunk_table.table;
            let key = (
                partition.key.as_str(),
                chunk_table.chunk_id,
                table.name.as_str(),
            );

            let chunk_summary = chunk_summary_map.get(&key).ok_or_else(|| {
                ArrowError::ComputeError(format!(
                    "IOx internal error: Can not find summary for {:?}",
                    key
                ))
            })?;

            let mut column_index = make_column_index(chunk_summary);
            let storage_value = chunk_summary.inner.storage.as_str();

            for column in &table.columns {
                partition_key.append_value(&partition.key)?;
                chunk_id.append_value(chunk_table.chunk_id)?;
                table_name.append_value(&table.name)?;
                column_name.append_value(&column.name)?;
                storage.append_value(storage_value)?;
                count.append_value(column.count())?;
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

                estimated_bytes.append_option(size)?;
            }

            // now, if there are any left over (special columns, like __dictionary), add them too
            for (name, size) in column_index {
                partition_key.append_value(&partition.key)?;
                chunk_id.append_value(chunk_table.chunk_id)?;
                table_name.append_value(&table.name)?;
                column_name.append_value(name)?;
                storage.append_value(storage_value)?;
                count.append_null()?;
                min_values.append(false)?;
                max_values.append(false)?;
                estimated_bytes.append_value(size)?;
            }
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
            Arc::new(count.finish()),
            Arc::new(min_values.finish()),
            Arc::new(max_values.finish()),
            Arc::new(estimated_bytes.finish()),
        ],
    )
}

/// Implementation of system.operations table
#[derive(Debug)]
struct OperationsTable {
    schema: SchemaRef,
    db_name: String,
    jobs: Arc<JobRegistry>,
}

impl OperationsTable {
    fn new(db_name: String, jobs: Arc<JobRegistry>) -> Self {
        Self {
            schema: operations_schema(),
            db_name,
            jobs,
        }
    }
}

impl IoxSystemTable for OperationsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn batch(&self) -> Result<RecordBatch> {
        from_task_trackers(self.schema(), &self.db_name, self.jobs.tracked())
            .log_if_error("system.operations table")
    }
}

fn operations_schema() -> SchemaRef {
    let ts = DataType::Time64(TimeUnit::Nanosecond);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("cpu_time_used", ts.clone(), true),
        Field::new("wall_time_used", ts, true),
        Field::new("partition_key", DataType::Utf8, true),
        Field::new("chunk_id", DataType::UInt32, true),
        Field::new("description", DataType::Utf8, true),
    ]))
}

fn from_task_trackers(
    schema: SchemaRef,
    db_name: &str,
    jobs: Vec<TaskTracker<Job>>,
) -> Result<RecordBatch> {
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

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(statuses),
            Arc::new(cpu_time_used),
            Arc::new(wall_time_used),
            Arc::new(partition_keys),
            Arc::new(chunk_ids),
            Arc::new(descriptions),
        ],
    )
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
    use data_types::{
        chunk::{ChunkColumnSummary, ChunkStorage},
        partition_metadata::{
            ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary,
            UnaggregatedTableSummary,
        },
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

        let batch = from_partition_summaries(partition_summaries_schema(), partitions).unwrap();
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

    #[test]
    fn test_assemble_chunk_columns() {
        let partitions = vec![
            UnaggregatedPartitionSummary {
                key: "p1".to_string(),
                tables: vec![UnaggregatedTableSummary {
                    chunk_id: 42,
                    table: TableSummary {
                        name: "t1".to_string(),
                        columns: vec![
                            ColumnSummary {
                                name: "c1".to_string(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::String(StatValues::new(
                                    "bar".to_string(),
                                    "foo".to_string(),
                                    55,
                                )),
                            },
                            ColumnSummary {
                                name: "c2".to_string(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues::new(11.0, 43.0, 66)),
                            },
                        ],
                    },
                }],
            },
            // other partition has different columns
            UnaggregatedPartitionSummary {
                key: "p2".to_string(),
                tables: vec![
                    UnaggregatedTableSummary {
                        chunk_id: 43,
                        table: TableSummary {
                            name: "t1".to_string(),
                            columns: vec![ColumnSummary {
                                name: "c2".to_string(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues::new(110.0, 430.0, 667)),
                            }],
                        },
                    },
                    UnaggregatedTableSummary {
                        chunk_id: 44,
                        table: TableSummary {
                            name: "t2".to_string(),
                            columns: vec![ColumnSummary {
                                name: "c3".to_string(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues::new(-1.0, 2.0, 4)),
                            }],
                        },
                    },
                ],
            },
        ];

        let chunk_summaries = vec![
            DetailedChunkSummary {
                inner: ChunkSummary {
                    partition_key: "p1".into(),
                    table_name: "t1".into(),
                    id: 42,
                    storage: ChunkStorage::ReadBuffer,
                    estimated_bytes: 23754,
                    row_count: 11,
                    time_of_first_write: None,
                    time_of_last_write: None,
                    time_closed: None,
                },
                columns: vec![
                    ChunkColumnSummary {
                        name: "c1".into(),
                        estimated_bytes: 11,
                    },
                    ChunkColumnSummary {
                        name: "c2".into(),
                        estimated_bytes: 12,
                    },
                    ChunkColumnSummary {
                        name: "__other".into(),
                        estimated_bytes: 13,
                    },
                ],
            },
            DetailedChunkSummary {
                inner: ChunkSummary {
                    partition_key: "p2".into(),
                    table_name: "t1".into(),
                    id: 43,
                    storage: ChunkStorage::OpenMutableBuffer,
                    estimated_bytes: 23754,
                    row_count: 11,
                    time_of_first_write: None,
                    time_of_last_write: None,
                    time_closed: None,
                },
                columns: vec![ChunkColumnSummary {
                    name: "c1".into(),
                    estimated_bytes: 100,
                }],
            },
            DetailedChunkSummary {
                inner: ChunkSummary {
                    partition_key: "p2".into(),
                    table_name: "t2".into(),
                    id: 44,
                    storage: ChunkStorage::OpenMutableBuffer,
                    estimated_bytes: 23754,
                    row_count: 11,
                    time_of_first_write: None,
                    time_of_last_write: None,
                    time_closed: None,
                },
                columns: vec![ChunkColumnSummary {
                    name: "c3".into(),
                    estimated_bytes: 200,
                }],
            },
        ];

        let expected = vec![
            "+---------------+----------+------------+-------------+-------------------+-------+-----------+-----------+-----------------+",
            "| partition_key | chunk_id | table_name | column_name | storage           | count | min_value | max_value | estimated_bytes |",
            "+---------------+----------+------------+-------------+-------------------+-------+-----------+-----------+-----------------+",
            "| p1            | 42       | t1         | c1          | ReadBuffer        | 55    | bar       | foo       | 11              |",
            "| p1            | 42       | t1         | c2          | ReadBuffer        | 66    | 11        | 43        | 12              |",
            "| p1            | 42       | t1         | __other     | ReadBuffer        |       |           |           | 13              |",
            "| p2            | 43       | t1         | c2          | OpenMutableBuffer | 667   | 110       | 430       |                 |",
            "| p2            | 43       | t1         | c1          | OpenMutableBuffer |       |           |           | 100             |",
            "| p2            | 44       | t2         | c3          | OpenMutableBuffer | 4     | -1        | 2         | 200             |",
            "+---------------+----------+------------+-------------+-------------------+-------+-----------+-----------+-----------------+",
        ];

        let batch =
            assemble_chunk_columns(chunk_columns_schema(), partitions, chunk_summaries).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }
}
