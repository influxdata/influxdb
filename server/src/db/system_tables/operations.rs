use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, Time64NanosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::Result;
use arrow::record_batch::RecordBatch;

use data_types::error::ErrorLogger;
use data_types::job::Job;
use tracker::TaskTracker;

use crate::db::system_tables::IoxSystemTable;
use crate::JobRegistry;

/// Implementation of system.operations table
#[derive(Debug)]
pub(super) struct OperationsTable {
    schema: SchemaRef,
    db_name: String,
    jobs: Arc<JobRegistry>,
}

impl OperationsTable {
    pub(super) fn new(db_name: String, jobs: Arc<JobRegistry>) -> Self {
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

    let ids = jobs
        .iter()
        .map(|job| Some(job.id().to_string()))
        .collect::<StringArray>();
    let statuses = jobs
        .iter()
        .map(|job| Some(job.get_status().name()))
        .collect::<StringArray>();
    let cpu_time_used = jobs
        .iter()
        .map(|job| job.get_status().cpu_nanos().map(|n| n as i64))
        .collect::<Time64NanosecondArray>();
    let wall_time_used = jobs
        .iter()
        .map(|job| job.get_status().wall_nanos().map(|n| n as i64))
        .collect::<Time64NanosecondArray>();
    let partition_keys = jobs
        .iter()
        .map(|job| job.metadata().partition_key())
        .collect::<StringArray>();
    let chunk_ids = jobs
        .iter()
        .map(|job| job.metadata().chunk_id())
        .collect::<UInt32Array>();
    let descriptions = jobs
        .iter()
        .map(|job| Some(job.metadata().description()))
        .collect::<StringArray>();

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
