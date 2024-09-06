use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, DurationNanosecondArray, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::{error::DataFusionError, logical_expr::Expr};
use iox_query::query_log::{QueryLog, QueryLogEntryState, QueryPhase};
use iox_system_tables::IoxSystemTable;

pub(super) struct QueriesTable {
    schema: SchemaRef,
    query_log: Arc<QueryLog>,
}

impl QueriesTable {
    pub(super) fn new(query_log: Arc<QueryLog>) -> Self {
        Self {
            schema: queries_schema(),
            query_log,
        }
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for QueriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();

        let entries = self
            .query_log
            .entries()
            .entries
            .into_iter()
            .map(|e| e.state())
            .collect::<Vec<_>>();

        from_query_log_entries(Arc::clone(&schema), &entries)
    }
}

fn queries_schema() -> SchemaRef {
    let columns = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, false),
        Field::new(
            "issue_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("query_type", DataType::Utf8, false),
        Field::new("query_text", DataType::Utf8, false),
        Field::new("partitions", DataType::Int64, true),
        Field::new("parquet_files", DataType::Int64, true),
        Field::new(
            "plan_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "permit_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "execute_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "end2end_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new(
            "compute_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
        Field::new("max_memory", DataType::Int64, true),
        Field::new("success", DataType::Boolean, false),
        Field::new("running", DataType::Boolean, false),
        Field::new("cancelled", DataType::Boolean, false),
        Field::new("trace_id", DataType::Utf8, true),
    ];

    Arc::new(Schema::new(columns))
}

fn from_query_log_entries(
    schema: SchemaRef,
    entries: &[Arc<QueryLogEntryState>],
) -> Result<RecordBatch, DataFusionError> {
    let mut columns: Vec<ArrayRef> = vec![];

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.id.to_string()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.phase.name()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.issue_time)
            .map(|ts| Some(ts.timestamp_nanos()))
            .collect::<TimestampNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(&e.query_type))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.query_text.to_string()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.partitions.map(|p| p as i64))
            .collect::<Int64Array>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.parquet_files.map(|p| p as i64))
            .collect::<Int64Array>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.plan_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.permit_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.execute_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.end2end_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.compute_duration.map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries.iter().map(|e| e.max_memory).collect::<Int64Array>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.success))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.running))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| Some(e.phase == QueryPhase::Cancel))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .map(|e| e.trace_id.map(|x| format!("{:x}", x.0)))
            .collect::<StringArray>(),
    ));

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}
