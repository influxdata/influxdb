use crate::{
    query_log::{QueryLog, QueryLogEntry},
    system_tables::IoxSystemTable,
};
use arrow::{
    array::{DurationNanosecondArray, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::Result,
    record_batch::RecordBatch,
};
use data_types::error::ErrorLogger;
use std::{collections::VecDeque, sync::Arc};

/// Implementation of system.queries table
#[derive(Debug)]
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

impl IoxSystemTable for QueriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn batch(&self) -> Result<RecordBatch> {
        from_query_log_entries(self.schema(), self.query_log.entries())
            .log_if_error("system.chunks table")
    }
}

fn queries_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "issue_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("query_type", DataType::Utf8, false),
        Field::new("query_text", DataType::Utf8, false),
        Field::new(
            "completed_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            false,
        ),
    ]))
}

fn from_query_log_entries(
    schema: SchemaRef,
    entries: VecDeque<Arc<QueryLogEntry>>,
) -> Result<RecordBatch> {
    let issue_time = entries
        .iter()
        .map(|e| e.issue_time)
        .map(|ts| Some(ts.timestamp_nanos()))
        .collect::<TimestampNanosecondArray>();

    let query_type = entries
        .iter()
        .map(|e| Some(&e.query_type))
        .collect::<StringArray>();

    let query_text = entries
        .iter()
        .map(|e| Some(&e.query_text))
        .collect::<StringArray>();

    let query_runtime = entries
        .iter()
        .map(|e| e.query_completed_duration().map(|d| d.as_nanos() as i64))
        .collect::<DurationNanosecondArray>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(issue_time),
            Arc::new(query_type),
            Arc::new(query_text),
            Arc::new(query_runtime),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use time::{Time, TimeProvider};

    #[test]
    fn test_from_query_log() {
        let now = Time::from_rfc3339("1996-12-19T16:39:57+00:00").unwrap();
        let time_provider = Arc::new(time::MockProvider::new(now));
        let query_log = QueryLog::new(10, Arc::clone(&time_provider) as Arc<dyn TimeProvider>);
        query_log.push("sql", "select * from foo");
        time_provider.inc(std::time::Duration::from_secs(24 * 60 * 60));
        query_log.push("sql", "select * from bar");
        let read_filter_entry = query_log.push("read_filter", "json goop");

        let expected = vec![
            "+----------------------+-------------+-------------------+--------------------+",
            "| issue_time           | query_type  | query_text        | completed_duration |",
            "+----------------------+-------------+-------------------+--------------------+",
            "| 1996-12-19T16:39:57Z | sql         | select * from foo |                    |",
            "| 1996-12-20T16:39:57Z | sql         | select * from bar |                    |",
            "| 1996-12-20T16:39:57Z | read_filter | json goop         |                    |",
            "+----------------------+-------------+-------------------+--------------------+",
        ];

        let schema = queries_schema();
        let batch = from_query_log_entries(schema.clone(), query_log.entries()).unwrap();
        assert_batches_eq!(&expected, &[batch]);

        // mark one of the queries completed after 4s
        let now = Time::from_rfc3339("1996-12-20T16:40:01+00:00").unwrap();
        read_filter_entry.set_completed(now);

        let expected = vec![
            "+----------------------+-------------+-------------------+--------------------+",
            "| issue_time           | query_type  | query_text        | completed_duration |",
            "+----------------------+-------------+-------------------+--------------------+",
            "| 1996-12-19T16:39:57Z | sql         | select * from foo |                    |",
            "| 1996-12-20T16:39:57Z | sql         | select * from bar |                    |",
            "| 1996-12-20T16:39:57Z | read_filter | json goop         | 4s                 |",
            "+----------------------+-------------+-------------------+--------------------+",
        ];

        let batch = from_query_log_entries(schema, query_log.entries()).unwrap();
        assert_batches_eq!(&expected, &[batch]);
    }
}
