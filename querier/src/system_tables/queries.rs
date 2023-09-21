use crate::{
    query_log::{QueryLog, QueryLogEntry},
    system_tables::{BatchIterator, IoxSystemTable},
};
use arrow::{
    array::{
        ArrayRef, BooleanArray, DurationNanosecondArray, Int64Array, StringArray,
        TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::Result,
    record_batch::RecordBatch,
};
use data_types::NamespaceId;
use observability_deps::tracing::error;
use std::{collections::VecDeque, sync::Arc};

/// Implementation of system.queries table
#[derive(Debug)]
pub(super) struct QueriesTable {
    schema: SchemaRef,
    query_log: Arc<QueryLog>,
    namespace_id_filter: Option<NamespaceId>,
}

impl QueriesTable {
    pub(super) fn new(query_log: Arc<QueryLog>, namespace_id_filter: Option<NamespaceId>) -> Self {
        Self {
            schema: queries_schema(namespace_id_filter.is_none()),
            query_log,
            namespace_id_filter,
        }
    }
}

impl IoxSystemTable for QueriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn scan(&self, batch_size: usize) -> Result<BatchIterator> {
        let schema = self.schema();

        let mut entries = self.query_log.entries();
        if let Some(namespace_id) = self.namespace_id_filter {
            entries.retain(|entry| entry.namespace_id == namespace_id);
        }

        let mut offset = 0;
        let namespace_id_filter = self.namespace_id_filter;
        Ok(Box::new(std::iter::from_fn(move || {
            if offset >= entries.len() {
                return None;
            }

            let len = batch_size.min(entries.len() - offset);
            match from_query_log_entries(
                Arc::clone(&schema),
                &entries,
                offset,
                len,
                namespace_id_filter.is_none(),
            ) {
                Ok(batch) => {
                    offset += len;
                    Some(Ok(batch))
                }
                Err(e) => {
                    error!("Error system.queries table: {:?}", e);
                    Some(Err(e))
                }
            }
        })))
    }
}

fn queries_schema(include_namespace_id: bool) -> SchemaRef {
    let mut columns = vec![];
    if include_namespace_id {
        columns.push(Field::new("namespace_id", DataType::Int64, false));
    }
    columns.append(&mut vec![
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
            true,
        ),
        Field::new("success", DataType::Boolean, false),
        Field::new("trace_id", DataType::Utf8, true),
    ]);

    Arc::new(Schema::new(columns))
}

fn from_query_log_entries(
    schema: SchemaRef,
    entries: &VecDeque<Arc<QueryLogEntry>>,
    offset: usize,
    len: usize,
    include_namespace_id: bool,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = vec![];

    if include_namespace_id {
        columns.push(Arc::new(
            entries
                .iter()
                .skip(offset)
                .take(len)
                .map(|e| Some(e.namespace_id.get()))
                .collect::<Int64Array>(),
        ));
    }

    columns.push(Arc::new(
        entries
            .iter()
            .skip(offset)
            .take(len)
            .map(|e| e.issue_time)
            .map(|ts| Some(ts.timestamp_nanos()))
            .collect::<TimestampNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .skip(offset)
            .take(len)
            .map(|e| Some(&e.query_type))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .skip(offset)
            .take(len)
            .map(|e| Some(e.query_text.to_string()))
            .collect::<StringArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .skip(offset)
            .take(len)
            .map(|e| e.query_completed_duration().map(|d| d.as_nanos() as i64))
            .collect::<DurationNanosecondArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .skip(offset)
            .take(len)
            .map(|e| Some(e.success()))
            .collect::<BooleanArray>(),
    ));

    columns.push(Arc::new(
        entries
            .iter()
            .skip(offset)
            .take(len)
            .map(|e| e.trace_id.map(|x| format!("{:x}", x.0)))
            .collect::<StringArray>(),
    ));

    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use iox_time::{Time, TimeProvider};
    use trace::ctx::TraceId;

    #[test]
    fn test_from_query_log() {
        let now = Time::from_rfc3339("1996-12-19T16:39:57+00:00").unwrap();
        let time_provider = Arc::new(iox_time::MockProvider::new(now));

        let id1 = NamespaceId::new(1);
        let id2 = NamespaceId::new(2);

        let query_log = Arc::new(QueryLog::new(
            10,
            Arc::clone(&time_provider) as Arc<dyn TimeProvider>,
        ));
        query_log.push(id1, "sql", Box::new("select * from foo"), None);
        time_provider.inc(std::time::Duration::from_secs(24 * 60 * 60));
        let sql2_entry = query_log.push(id1, "sql", Box::new("select * from bar"), None);
        let read_filter_entry = query_log.push(
            id2,
            "read_filter",
            Box::new("json goop"),
            Some(TraceId::new(0x45fe).unwrap()),
        );

        let table = QueriesTable::new(Arc::clone(&query_log), None);

        let expected = vec![
            "+--------------+----------------------+-------------+-------------------+--------------------+---------+----------+",
            "| namespace_id | issue_time           | query_type  | query_text        | completed_duration | success | trace_id |",
            "+--------------+----------------------+-------------+-------------------+--------------------+---------+----------+",
            "| 1            | 1996-12-19T16:39:57Z | sql         | select * from foo |                    | false   |          |",
            "| 1            | 1996-12-20T16:39:57Z | sql         | select * from bar |                    | false   |          |",
            "| 2            | 1996-12-20T16:39:57Z | read_filter | json goop         |                    | false   | 45fe     |",
            "+--------------+----------------------+-------------+-------------------+--------------------+---------+----------+",
        ];

        let entries = table.scan(3).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 1);
        assert_batches_eq!(&expected, &entries);

        // mark the sql query completed after 4s unsuccessfully
        let now = Time::from_rfc3339("1996-12-20T16:40:01+00:00").unwrap();
        sql2_entry.set_completed(now, false);

        // mark the read_filter query completed after 4s successfuly
        read_filter_entry.set_completed(now, true);

        let expected = vec![
            "+--------------+----------------------+-------------+-------------------+--------------------+---------+----------+",
            "| namespace_id | issue_time           | query_type  | query_text        | completed_duration | success | trace_id |",
            "+--------------+----------------------+-------------+-------------------+--------------------+---------+----------+",
            "| 1            | 1996-12-19T16:39:57Z | sql         | select * from foo |                    | false   |          |",
            "| 1            | 1996-12-20T16:39:57Z | sql         | select * from bar | 4s                 | false   |          |",
            "| 2            | 1996-12-20T16:39:57Z | read_filter | json goop         | 4s                 | true    | 45fe     |",
            "+--------------+----------------------+-------------+-------------------+--------------------+---------+----------+",
        ];

        let entries = table.scan(2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 2);
        assert_batches_eq!(&expected, &entries);

        // test namespace scoping
        let table = QueriesTable::new(Arc::clone(&query_log), Some(id1));

        let expected = vec![
            "+----------------------+------------+-------------------+--------------------+---------+----------+",
            "| issue_time           | query_type | query_text        | completed_duration | success | trace_id |",
            "+----------------------+------------+-------------------+--------------------+---------+----------+",
            "| 1996-12-19T16:39:57Z | sql        | select * from foo |                    | false   |          |",
            "| 1996-12-20T16:39:57Z | sql        | select * from bar | 4s                 | false   |          |",
            "+----------------------+------------+-------------------+--------------------+---------+----------+",
        ];

        let entries = table.scan(3).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 1);
        assert_batches_eq!(&expected, &entries);
    }
}
