use arrow::array::RecordBatch;
use serde::{
    Serialize, Serializer,
    ser::{SerializeSeq, SerializeStruct},
};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};

pub(super) mod buffered;
pub(super) mod chunked;
pub(super) mod csv;
pub(super) mod json;
pub(super) mod msgpack;
use buffered::BufferedResponseStream;
use chunked::ChunkedResponseStream;
mod stream;
use stream::{SeriesChunkMergeStream, SeriesChunkStream};

use super::{
    types::Precision,
    value::{Value, ValueSerializer},
};

#[derive(Debug, PartialEq)]
pub(super) struct Series {
    measurement: String,
    tags: BTreeMap<Arc<str>, String>,
}

impl Series {
    fn new(measurement: String, tags: BTreeMap<Arc<str>, String>) -> Self {
        Self { measurement, tags }
    }
}

/// This represents a discrete chunk of data as defined by the V1 query API.
/// This can be a complete Series when:
/// 1. We are not running in chunked mode
/// 2. We are in chunked mode, and the series is smaller than the chunk size
///
/// or this will be a Chunk (a subset of a Series) as defined by the chunk size.
///
/// The intention is that this type can be easily converted to our output formats
/// and returned/streamed by another stream.
#[derive(PartialEq)]
pub(crate) struct SeriesChunk {
    measurement_column: usize,

    tag_columns: Arc<BTreeMap<Arc<str>, usize>>,

    value_columns: Arc<[(Arc<str>, usize)]>,

    /// SeriesChunks may contain one or more record batches.
    data: Vec<RecordBatch>,
    /// If this chunk is a partial chunk.
    partial: bool,
}

impl SeriesChunk {
    /// Create a new SeriesChunk from a RecordBatch.
    fn new(
        measurement_column: usize,
        tag_columns: Arc<BTreeMap<Arc<str>, usize>>,
        value_columns: Arc<[(Arc<str>, usize)]>,
        batch: RecordBatch,
    ) -> Self {
        Self {
            measurement_column,
            tag_columns,
            value_columns,
            data: vec![batch],
            partial: false,
        }
    }

    /// Get the measurement name this series belongs to.
    fn measurement(&self) -> String {
        assert!(
            !self.data.is_empty(),
            "SeriesChunk should have at least one record batch"
        );
        Value::new(self.data[0].column(self.measurement_column), 0).to_string()
    }

    /// Get the tags that define this series.
    fn tags(&self) -> BTreeMap<Arc<str>, String> {
        assert!(
            !self.data.is_empty(),
            "SeriesChunk should have at least one record batch"
        );
        self.tag_columns
            .iter()
            .map(|(name, idx)| (Arc::clone(name), Value::new(self.data[0].column(*idx), 0)))
            .map(|(name, value)| (name, value.to_string()))
            .collect()
    }

    /// Get the definition of the Series this chunk is part of.
    fn series(&self) -> Series {
        Series::new(self.measurement(), self.tags())
    }

    // Get the names of the columns in this SeriesChunk.
    fn columns(&self) -> Vec<Arc<str>> {
        self.value_columns
            .iter()
            .map(|(name, _)| Arc::clone(name))
            .collect()
    }

    /// Get the total number of rows in this SeriesChunk.
    fn num_rows(&self) -> usize {
        self.data.iter().map(|x| x.num_rows()).sum()
    }

    /// Get the values at the given row.
    fn row(&self, row: usize) -> Option<Vec<Value>> {
        if row >= self.num_rows() {
            return None;
        }

        // Calculate the index of the record batch, and the index of the row within that batch.
        //
        // For example, if we have 3 batches that look like this:
        // [
        //   [a, b, c],
        //   [d, e, f, g],
        //   [h, i]
        // ]
        // and we want to get the value of row 5 (rows starting from 0), which is "f",
        // the index of the batch is 1, and the index of the row within that batch is 2.
        let mut batch_idx = 0;
        let mut row_idx = row;
        while row_idx >= self.data[batch_idx].num_rows() {
            row_idx -= self.data[batch_idx].num_rows();
            batch_idx += 1;
        }

        if batch_idx > self.data.len() {
            return None;
        }

        let mut values = Vec::new();
        for (_, idx) in self.value_columns.iter() {
            values.push(Value::new(self.data[batch_idx].column(*idx), row_idx));
        }
        Some(values)
    }

    /// Split this SeriesChunk into two SeriesChunks at the given size.
    fn split_at(self, mut size: usize) -> (Self, Self) {
        let mut left = Self {
            measurement_column: self.measurement_column,
            tag_columns: Arc::clone(&self.tag_columns),
            value_columns: Arc::clone(&self.value_columns),
            data: Vec::new(),
            partial: self.partial,
        };
        let mut right = Self {
            measurement_column: self.measurement_column,
            tag_columns: self.tag_columns,
            value_columns: self.value_columns,
            data: Vec::new(),
            partial: self.partial,
        };
        let it = self.data.into_iter();
        for batch in it {
            if size > 0 {
                if batch.num_rows() > size {
                    left.data.push(batch.slice(0, size));
                    right.data.push(batch.slice(size, batch.num_rows() - size));
                    size = 0;
                } else {
                    size -= batch.num_rows();
                    left.data.push(batch);
                }
            } else {
                right.data.push(batch);
            }
        }
        (left, right)
    }

    /// Merge another SeriesChunk into this one.
    fn merge(&mut self, other: Self) {
        assert_eq!(self.series(), other.series());
        self.data.extend(other.data);
    }
}

struct SeriesChunkSerializer<'a> {
    chunk: &'a SeriesChunk,
    epoch: Option<Precision>,
    /// Allow infinite values
    allow_inf: bool,
}

impl<'a> SeriesChunkSerializer<'a> {
    fn new(chunk: &'a SeriesChunk, epoch: Option<Precision>, allow_inf: bool) -> Self {
        Self {
            chunk,
            epoch,
            allow_inf,
        }
    }
}

impl Serialize for SeriesChunkSerializer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 0;
        let name = self.chunk.measurement();
        if !name.is_empty() {
            fields += 1;
        }
        let tags = self.chunk.tags();
        if !tags.is_empty() {
            fields += 1;
        }
        let columns = self.chunk.columns();
        if !columns.is_empty() {
            fields += 1;
        }
        if self.chunk.num_rows() > 0 {
            fields += 1;
        }
        if self.chunk.partial {
            fields += 1;
        }

        let mut obj = serializer.serialize_struct("", fields)?;
        if !name.is_empty() {
            obj.serialize_field("name", &name)?;
        }
        if !tags.is_empty() {
            obj.serialize_field("tags", &tags)?;
        }
        if !columns.is_empty() {
            obj.serialize_field("columns", &self.chunk.columns())?;
        }
        if self.chunk.num_rows() > 0 {
            obj.serialize_field(
                "values",
                &SeriesValues {
                    chunk: self.chunk,
                    epoch: self.epoch,
                    allow_inf: self.allow_inf,
                },
            )?;
        }
        if self.chunk.partial {
            obj.serialize_field("partial", &self.chunk.partial)?;
        }
        obj.end()
    }
}

impl Debug for SeriesChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunk")
            .field("measurement", &self.measurement())
            .field("tags", &self.tags())
            .field("columns", &self.columns())
            .finish_non_exhaustive()
    }
}

impl<'a> IntoIterator for &'a SeriesChunk {
    type Item = Vec<Value>;
    type IntoIter = SeriesChunkIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SeriesChunkIter {
            chunk: self,
            row: 0,
        }
    }
}

pub(crate) struct SeriesChunkIter<'a> {
    chunk: &'a SeriesChunk,
    row: usize,
}

impl Iterator for SeriesChunkIter<'_> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.chunk.row(self.row);
        if row.is_some() {
            self.row += 1;
        }
        row
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.chunk.num_rows() - self.row;
        (remaining, Some(remaining))
    }
}

/// This is a helper struct to serialize a SeriesChunk into the JSON
/// format
struct SeriesValues<'a> {
    chunk: &'a SeriesChunk,
    epoch: Option<Precision>,
    /// Allow infinite values
    allow_inf: bool,
}

impl Serialize for SeriesValues<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.chunk.num_rows()))?;
        for row in self.chunk.into_iter() {
            let row = row
                .iter()
                .map(|e| ValueSerializer::new(e, self.epoch, self.allow_inf))
                .collect::<Vec<_>>();
            seq.serialize_element(&row)?;
        }
        seq.end()
    }
}

/// The result of a single InfluxQL statement. This is equivalent to
/// [query.Result](https://github.com/influxdata/influxdb/blob/master-1.x/query/result.go#L86)
/// from InfluxDB v1.
///
/// N.B. This doesn't support the messages field, as we have no use for
/// it.
#[derive(Debug, PartialEq)]
pub(crate) struct StatementResult {
    statement_id: usize,
    series: Vec<SeriesChunk>,
    partial: bool,
    error: String,
}

impl StatementResult {
    fn new(statement_id: usize) -> Self {
        Self {
            statement_id,
            series: Vec::new(),
            partial: false,
            error: String::new(),
        }
    }

    fn add_series(&mut self, series: SeriesChunk) {
        self.series.push(series);
    }

    fn set_partial(&mut self, partial: bool) {
        self.partial = partial;
    }

    fn set_error(&mut self, error: String) {
        self.error = error;
    }

    fn is_error(&self) -> bool {
        !self.error.is_empty()
    }
}

struct StatementResultSerializer<'a> {
    result: &'a StatementResult,
    epoch: Option<Precision>,
    /// Allow infinite values
    allow_inf: bool,
}

impl Serialize for StatementResultSerializer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if !self.result.series.is_empty() {
            fields += 1;
        }
        if self.result.partial {
            fields += 1;
        }
        if !self.result.error.is_empty() {
            fields += 1;
        }
        let mut obj = serializer.serialize_struct("", fields)?;
        obj.serialize_field("statement_id", &self.result.statement_id)?;
        if !self.result.series.is_empty() {
            let series = self
                .result
                .series
                .iter()
                .map(|s| SeriesChunkSerializer::new(s, self.epoch, self.allow_inf))
                .collect::<Vec<_>>();
            obj.serialize_field("series", &series)?;
        }
        if self.result.partial {
            obj.serialize_field("partial", &self.result.partial)?;
        }
        if !self.result.error.is_empty() {
            obj.serialize_field("error", &self.result.error)?;
        }
        obj.end()
    }
}

#[derive(Debug, Default, PartialEq)]
pub(crate) struct Response(Vec<StatementResult>);

impl Response {
    pub(crate) fn add_result(&mut self, result: StatementResult) {
        self.0.push(result);
    }
}

struct ResponseSerializer<'a> {
    response: &'a Response,
    epoch: Option<Precision>,
    /// Allow infinite values
    allow_inf: bool,
}

impl<'a> ResponseSerializer<'a> {
    fn new(response: &'a Response, epoch: Option<Precision>, allow_inf: bool) -> Self {
        Self {
            response,
            epoch,
            allow_inf,
        }
    }
}

impl Serialize for ResponseSerializer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let fields = if self.response.0.is_empty() { 0 } else { 1 };
        let mut obj = serializer.serialize_struct("", fields)?;
        if !self.response.0.is_empty() {
            let result = self
                .response
                .0
                .iter()
                .map(|r| StatementResultSerializer {
                    result: r,
                    epoch: self.epoch,
                    allow_inf: self.allow_inf,
                })
                .collect::<Vec<_>>();
            obj.serialize_field("results", &result)?;
        }
        obj.end()
    }
}

#[cfg(test)]
mod tests {
    use crate::StatementFuture;
    use crate::error::Error;
    use crate::types::Statement;
    use arrow::{
        array::{ArrayRef, RecordBatch},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use data_types::NamespaceId;
    use datafusion::physical_plan::{ExecutionPlan, test::exec::MockExec};
    use generated_types::influxdata::iox::querier::v1::{
        InfluxQlMetadata, influx_ql_metadata::TagKeyColumn,
    };
    use iox_query::QueryDatabase;
    use iox_query::exec::IOxSessionContext;
    use iox_query::query_log::{PermitAndToken, QueryLog};
    use iox_query_params::StatementParams;
    use schema::{INFLUXQL_MEASUREMENT_COLUMN_NAME, INFLUXQL_METADATA_KEY, TIME_COLUMN_NAME};
    use std::{collections::HashMap, sync::Arc};

    #[derive(Clone)]
    pub(super) enum Column {
        Measurement,
        Tag {
            name: &'static str,
            group_by: bool,
            projected: bool,
        },
        Time,
        Field {
            name: &'static str,
        },
    }

    pub(super) fn make_statement(
        database: &Arc<dyn QueryDatabase>,
        ctx: &Arc<IOxSessionContext>,
        log: &Arc<QueryLog>,
        columns: impl IntoIterator<Item = Column>,
        data: Vec<ArrayRef>,
    ) -> StatementFuture {
        let (schema, batches) = make_schema_and_batches(columns, vec![data]);
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            MockExec::new(
                batches.into_iter().map(Ok).collect(),
                SchemaRef::clone(&schema),
            )
            .with_use_task(false),
        );

        let database = Arc::clone(database);
        let ctx = Arc::clone(ctx);
        let log = Arc::clone(log);
        let fut = async move {
            let token = log.push(
                NamespaceId::new(0),
                Arc::from("test"),
                "test_query",
                Box::new("test_query".to_string()),
                StatementParams::new(),
                None,
                None,
            );
            let token = token.planned(ctx.as_ref(), Arc::clone(&exec));
            let permit = database.acquire_semaphore(None).await;
            let query_completed_token = token.permit();
            let permit_state = Some(PermitAndToken {
                permit,
                query_completed_token,
            });
            ctx.execute_stream(exec)
                .await
                .map(|stream| Statement {
                    schema,
                    permit_state,
                    stream,
                })
                .map_err(Error::from)
        };

        Box::new(fut)
    }

    pub(super) fn make_schema_and_batches(
        columns: impl IntoIterator<Item = Column>,
        data: Vec<Vec<ArrayRef>>,
    ) -> (SchemaRef, Vec<RecordBatch>) {
        let mut measurement_column_index = None;
        let mut tag_key_columns = vec![];
        let fields = columns
            .into_iter()
            .enumerate()
            .inspect(|(i, column)| {
                if let Column::Tag {
                    name,
                    group_by: true,
                    projected,
                } = column
                {
                    tag_key_columns.push(TagKeyColumn {
                        tag_key: name.to_string(),
                        column_index: *i as u32,
                        is_projected: *projected,
                    });
                }
                if let Column::Measurement = column {
                    measurement_column_index = Some(*i as u32);
                }
            })
            .map(|(i, column)| match column {
                Column::Measurement => Field::new(
                    INFLUXQL_MEASUREMENT_COLUMN_NAME,
                    data.first()
                        .and_then(|batch| batch.get(i))
                        .map(|arr| arr.data_type().clone())
                        .unwrap_or(DataType::Utf8),
                    false,
                ),
                Column::Tag { name, .. } => Field::new(
                    name,
                    data.first()
                        .and_then(|batch| batch.get(i))
                        .map(|arr| arr.data_type().clone())
                        .unwrap_or(DataType::Dictionary(
                            Box::new(DataType::Int32),
                            Box::new(DataType::Utf8),
                        )),
                    true,
                ),
                Column::Time => Field::new(
                    TIME_COLUMN_NAME,
                    data.first()
                        .and_then(|batch| batch.get(i))
                        .map(|arr| arr.data_type().clone())
                        .unwrap_or(DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Nanosecond,
                            None,
                        )),
                    false,
                ),
                Column::Field { name } => Field::new(
                    name,
                    data.first()
                        .and_then(|batch| batch.get(i))
                        .map(|arr| arr.data_type().clone())
                        .unwrap_or(DataType::Float64),
                    true,
                ),
            })
            .collect::<Vec<_>>();
        let md = InfluxQlMetadata {
            measurement_column_index: measurement_column_index.unwrap(),
            tag_key_columns,
        };
        let md = serde_json::to_string(&md).unwrap();
        let schema =
            Schema::new(fields).with_metadata(HashMap::from([(INFLUXQL_METADATA_KEY.into(), md)]));
        let schema = SchemaRef::new(schema);

        let batches = data
            .into_iter()
            .map(|d| RecordBatch::try_new(SchemaRef::clone(&schema), d).unwrap())
            .collect();

        (schema, batches)
    }
}
